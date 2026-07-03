package btypedb

import (
	"fmt"
	"io"
	"maps"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// powerFile simulates a file under power loss: bytes written are only
// guaranteed after Sync promotes them from pending to durable. The
// process sees everything (Read/ReadAt/Stat include pending, as the OS
// page cache would); a simulated power cut keeps only the durable bytes
// — or the durable bytes plus an arbitrary torn prefix of what was
// in flight.
type powerFile struct {
	mu      sync.Mutex
	durable []byte
	pending []byte
	pos     int64
}

func (p *powerFile) combined() []byte {
	all := make([]byte, 0, len(p.durable)+len(p.pending))
	all = append(all, p.durable...)
	return append(all, p.pending...)
}

func (p *powerFile) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pos != int64(len(p.durable)+len(p.pending)) {
		return 0, fmt.Errorf("powerFile: non-append write at %d", p.pos)
	}
	p.pending = append(p.pending, b...)
	p.pos += int64(len(b))
	return len(b), nil
}

func (p *powerFile) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	all := p.combined()
	if p.pos >= int64(len(all)) {
		return 0, io.EOF
	}
	n := copy(b, all[p.pos:])
	p.pos += int64(n)
	return n, nil
}

func (p *powerFile) ReadAt(b []byte, off int64) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	all := p.combined()
	if off >= int64(len(all)) {
		return 0, io.EOF
	}
	n := copy(b, all[off:])
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

func (p *powerFile) Seek(offset int64, whence int) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch whence {
	case io.SeekStart:
		p.pos = offset
	case io.SeekCurrent:
		p.pos += offset
	case io.SeekEnd:
		p.pos = int64(len(p.durable)+len(p.pending)) + offset
	}
	return p.pos, nil
}

func (p *powerFile) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.durable = p.combined()
	p.pending = nil
	return nil
}

func (p *powerFile) Truncate(size int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	all := p.combined()
	if size < int64(len(all)) {
		all = all[:size]
	}
	if size <= int64(len(p.durable)) {
		p.durable, p.pending = all, nil
	} else {
		p.pending = all[len(p.durable):]
	}
	return nil
}

func (p *powerFile) Stat() (os.FileInfo, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return pfInfo{size: int64(len(p.durable) + len(p.pending))}, nil
}

func (p *powerFile) Close() error { return nil }

func (p *powerFile) durableSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.durable)
}

// tornSnapshot returns the durable bytes plus a torn prefix of the
// bytes still in flight — another state a power cut may leave behind.
func (p *powerFile) tornSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append(slices.Clone(p.durable), p.pending[:len(p.pending)/2]...)
}

type pfInfo struct{ size int64 }

func (pfInfo) Name() string       { return "powerfile" }
func (i pfInfo) Size() int64      { return i.size }
func (pfInfo) Mode() os.FileMode  { return 0o644 }
func (pfInfo) ModTime() time.Time { return time.Time{} }
func (pfInfo) IsDir() bool        { return false }
func (pfInfo) Sys() any           { return nil }

// logfileFS injects a fault-simulating log file; every other filesystem
// operation hits the real OS.
type logfileFS struct {
	realFS
	f logfile
}

func (l logfileFS) OpenFile(string) (logfile, error) { return l.f, nil }

// withLogfile injects a fault-simulating log file (test seam).
func withLogfile(f logfile) Option {
	return func(o *options) { o.fs = logfileFS{f: f} }
}

// withFS injects a whole fault-simulating filesystem (test seam),
// covering the compaction temp-file/rename path as well as the log.
func withFS(fs fsys) Option {
	return func(o *options) { o.fs = fs }
}

// TestPowerLossDurability is the durability half of the power-loss
// harness: with SyncAlways, every acknowledged operation must survive a
// power cut, exactly — no more, no less. The workload runs against a
// powerFile; after each acked op we snapshot the durable bytes and the
// expected logical state, then "cut power" at every such boundary (plus
// torn mid-record cuts and garbage tails) and reopen from a real file.
//
// This catches ordering bugs SIGKILL cannot: acking before fsync, or
// applying to memory before the log write, would leave a durable
// snapshot missing its op and fail the exact-state check.
func TestPowerLossDurability(t *testing.T) {
	pf := &powerFile{}
	db, err := Open(filepath.Join(t.TempDir(), "sim.db"), StringCodec, StringCodec,
		withLogfile(pf), WithAutoCompactDisabled(), WithSweepInterval(0)) // SyncAlways default
	if err != nil {
		t.Fatal(err)
	}

	type cutPoint struct {
		durable []byte
		want    map[string]string
	}
	expected := map[string]string{}
	var cuts []cutPoint
	record := func() {
		cuts = append(cuts, cutPoint{pf.durableSnapshot(), maps.Clone(expected)})
	}
	record() // the empty database is a valid cut too

	for i := range 48 {
		switch i % 4 {
		case 0, 1: // direct set (overwrites cycle through 12 keys)
			k, v := fmt.Sprintf("k%02d", i%12), fmt.Sprintf("v%d", i)
			if err := db.Set(k, v); err != nil {
				t.Fatal(err)
			}
			expected[k] = v
		case 2: // multi-op transaction: must be atomic at every cut
			err := db.Update(func(tx *Tx[string, string]) error {
				for j := range 3 {
					if err := tx.Set(fmt.Sprintf("b%02d:%d", i, j), fmt.Sprintf("t%d", i)); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			for j := range 3 {
				expected[fmt.Sprintf("b%02d:%d", i, j)] = fmt.Sprintf("t%d", i)
			}
		case 3: // delete (sometimes of an absent key — writes nothing)
			k := fmt.Sprintf("k%02d", (i+5)%12)
			if _, err := db.Delete(k); err != nil {
				t.Fatal(err)
			}
			delete(expected, k)
		}
		record()
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	for i, c := range cuts {
		// Clean cut at an ack boundary: recovered state must be exact.
		verifyPowerCut(t, dir, c.durable, c.want, fmt.Sprintf("cut %d (clean)", i))

		// Torn cut: power died mid-way through persisting the next op.
		// Recovery must land exactly on the pre-op state.
		if i+1 < len(cuts) {
			delta := cuts[i+1].durable[len(c.durable):]
			if len(delta) > 1 {
				torn := append(slices.Clone(c.durable), delta[:len(delta)/2]...)
				verifyPowerCut(t, dir, torn, c.want, fmt.Sprintf("cut %d (torn next op)", i))
			}
		}

		// Sector junk after the durable prefix must not confuse replay.
		junk := append(slices.Clone(c.durable), 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01)
		verifyPowerCut(t, dir, junk, c.want, fmt.Sprintf("cut %d (garbage tail)", i))
	}
}

// verifyPowerCut materializes surviving bytes as a real log file, opens
// it, and checks the recovered state matches want exactly and the DB
// accepts new writes.
func verifyPowerCut(t *testing.T, dir string, data []byte, want map[string]string, label string) {
	t.Helper()
	path := filepath.Join(dir, "cut.db")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, StringCodec, StringCodec, WithSweepInterval(0))
	if err != nil {
		t.Fatalf("%s: open after power cut: %v", label, err)
	}
	defer db.Close()

	if db.Len() != len(want) {
		t.Fatalf("%s: recovered %d keys; want exactly %d", label, db.Len(), len(want))
	}
	for k, wv := range want {
		if v, ok := db.Get(k); !ok || v != wv {
			t.Fatalf("%s: Get(%q) = %q, %v; want %q (acked op lost or corrupted)", label, k, v, ok, wv)
		}
	}
	if err := db.Set("probe", "ok"); err != nil {
		t.Fatalf("%s: write after recovery: %v", label, err)
	}
}

// TestPowerLossEveryPrefix is the consistency half of the harness: for
// every byte-length prefix of a real log (any of which a power cut
// could leave behind, fsync policy aside), the database must open
// cleanly and every multi-op transaction must be present all-or-nothing
// with uniform values. Periodically a garbage tail is appended to model
// sector junk beyond the surviving prefix.
func TestPowerLossEveryPrefix(t *testing.T) {
	if testing.Short() {
		t.Skip("exhaustive prefix scan skipped in -short mode")
	}

	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	db := openStr(t, src, WithSyncPolicy(SyncNever), WithAutoCompactDisabled(), WithSweepInterval(0))
	for r := range 14 {
		err := db.Update(func(tx *Tx[string, string]) error {
			for j := range 4 {
				if err := tx.Set(fmt.Sprintf("g:%02d:%d", r, j), fmt.Sprintf("%d", r)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if r%3 == 0 {
			if err := db.Set(fmt.Sprintf("s:%02d", r), "x"); err != nil {
				t.Fatal(err)
			}
		}
		if r%5 == 4 {
			if _, err := db.Delete(fmt.Sprintf("s:%02d", r-4)); err != nil {
				t.Fatal(err)
			}
		}
		if r%7 == 6 {
			if err := db.SetTTL(fmt.Sprintf("t:%02d", r), "x", time.Hour); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(42))
	cutPath := filepath.Join(dir, "cut.db")
	for n := 0; n <= len(raw); n++ {
		data := slices.Clone(raw[:n])
		if n%7 == 3 { // sometimes power loss leaves junk past the prefix
			junk := make([]byte, 1+rng.Intn(24))
			rng.Read(junk)
			data = append(data, junk...)
		}
		if err := os.WriteFile(cutPath, data, 0o644); err != nil {
			t.Fatal(err)
		}
		db2, err := Open(cutPath, StringCodec, StringCodec, WithSweepInterval(0))
		if err != nil {
			t.Fatalf("prefix %d/%d: open: %v", n, len(raw), err)
		}
		checkTxGroups(t, db2, n)
		db2.Close()
	}
}

// checkTxGroups asserts every "g:NN:*" transaction group recovered
// all-or-nothing with a single value.
func checkTxGroups(t *testing.T, db *DB[string, string], prefixLen int) {
	t.Helper()
	type group struct {
		members int
		values  map[string]bool
	}
	groups := map[string]*group{}
	for k, v := range db.All() {
		if !strings.HasPrefix(k, "g:") {
			continue
		}
		id := k[:strings.LastIndex(k, ":")]
		g := groups[id]
		if g == nil {
			g = &group{values: map[string]bool{}}
			groups[id] = g
		}
		g.members++
		g.values[v] = true
	}
	for id, g := range groups {
		if g.members != 4 {
			t.Fatalf("prefix %d: group %s recovered %d/4 members: torn transaction applied", prefixLen, id, g.members)
		}
		if len(g.values) != 1 {
			t.Fatalf("prefix %d: group %s has mixed values %v", prefixLen, id, g.values)
		}
	}
}
