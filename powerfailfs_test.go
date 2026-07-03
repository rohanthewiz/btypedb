package btypedb

import (
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// powerFS simulates a whole filesystem under power loss, extending the
// powerFile model to directory metadata: file contents are durable only
// after the file's Sync, and directory operations (create, rename,
// remove) are durable only after SyncDir. A cut snapshot is recorded at
// every operation boundary in two flavors bracketing what a real crash
// could leave behind:
//
//   - conservative: no directory op since the last SyncDir persisted
//     (metadata still in the journal);
//   - eager: every issued directory op persisted (journal flushed on
//     its own).
//
// Either way file contents survive only up to their last Sync.
type powerFS struct {
	mu       sync.Mutex
	cur      map[string]*powerFile // directory as the process sees it
	dur      map[string]*powerFile // directory as of the last SyncDir
	cuts     []fsCut
	onCreate func() // if set, runs once right after the next Create (test hook)
}

// fsCut is one possible post-crash filesystem state: surviving bytes by
// path, under each metadata-durability assumption. The torn flavors
// additionally keep a torn prefix of the compaction temp file's
// unsynced bytes — the crash tearing the very content compaction was
// streaming — while every other file stays at its last Sync.
type fsCut struct {
	label            string
	conservative     map[string][]byte
	eager            map[string][]byte
	tornConservative map[string][]byte
	tornEager        map[string][]byte
}

func newPowerFS() *powerFS {
	return &powerFS{cur: map[string]*powerFile{}, dur: map[string]*powerFile{}}
}

func (fs *powerFS) snapLocked(label string) fsCut {
	view := func(dir map[string]*powerFile, torn bool) map[string][]byte {
		m := make(map[string][]byte, len(dir))
		for p, f := range dir {
			if torn && strings.HasSuffix(p, compactSuffix) {
				m[p] = f.tornSnapshot()
			} else {
				m[p] = f.durableSnapshot()
			}
		}
		return m
	}
	c := fsCut{label: label,
		conservative: view(fs.dur, false), eager: view(fs.cur, false),
		tornConservative: view(fs.dur, true), tornEager: view(fs.cur, true),
	}
	fs.cuts = append(fs.cuts, c)
	return c
}

// cutNow records and returns a cut at an explicit test boundary.
func (fs *powerFS) cutNow(label string) fsCut {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.snapLocked(label)
}

func (fs *powerFS) cutCount() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return len(fs.cuts)
}

func (fs *powerFS) cutList() []fsCut {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return append([]fsCut(nil), fs.cuts...)
}

func (fs *powerFS) OpenFile(path string) (logfile, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f := fs.cur[path]
	if f == nil {
		f = &powerFile{}
		fs.cur[path] = f
		fs.snapLocked("create " + path)
	}
	f.Seek(0, io.SeekStart)
	return &pfsFile{pf: f, fs: fs, name: path}, nil
}

func (fs *powerFS) Create(path string) (logfile, error) {
	fs.mu.Lock()
	f := &powerFile{} // truncate semantics: a fresh inode replaces any old entry
	fs.cur[path] = f
	fs.snapLocked("create " + path)
	hook := fs.onCreate
	fs.onCreate = nil
	fs.mu.Unlock()
	if hook != nil {
		hook()
	}
	return &pfsFile{pf: f, fs: fs, name: path}, nil
}

func (fs *powerFS) Rename(oldpath, newpath string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	f := fs.cur[oldpath]
	if f == nil {
		return os.ErrNotExist
	}
	delete(fs.cur, oldpath)
	fs.cur[newpath] = f
	fs.snapLocked("rename " + oldpath)
	return nil
}

func (fs *powerFS) Remove(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.cur[path] == nil {
		return os.ErrNotExist
	}
	delete(fs.cur, path)
	fs.snapLocked("remove " + path)
	return nil
}

func (fs *powerFS) SyncDir(string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.dur = maps.Clone(fs.cur)
	fs.snapLocked("syncdir")
}

// pfsFile wraps a powerFile inode so every content mutation records a
// cut point in the owning powerFS.
type pfsFile struct {
	pf   *powerFile
	fs   *powerFS
	name string
}

func (f *pfsFile) cut(op string) {
	f.fs.mu.Lock()
	f.fs.snapLocked(op + " " + f.name)
	f.fs.mu.Unlock()
}

func (f *pfsFile) Write(b []byte) (int, error) {
	n, err := f.pf.Write(b)
	f.cut("write")
	return n, err
}

func (f *pfsFile) Sync() error {
	err := f.pf.Sync()
	f.cut("sync")
	return err
}

func (f *pfsFile) Truncate(size int64) error {
	err := f.pf.Truncate(size)
	f.cut("truncate")
	return err
}

func (f *pfsFile) Read(b []byte) (int, error)              { return f.pf.Read(b) }
func (f *pfsFile) ReadAt(b []byte, off int64) (int, error) { return f.pf.ReadAt(b, off) }
func (f *pfsFile) Seek(off int64, whence int) (int64, error) {
	return f.pf.Seek(off, whence)
}
func (f *pfsFile) Stat() (os.FileInfo, error) { return f.pf.Stat() }
func (f *pfsFile) Close() error               { return f.pf.Close() }

// TestPowerLossCompaction cuts power at every operation boundary of a
// compaction — temp-file create, snapshot writes, temp sync, rename,
// directory sync — under both metadata-durability assumptions, plus
// torn variants where the temp file's unsynced content survives only
// partially, and requires every cut to recover exactly the acknowledged
// state: compaction must never be able to lose or corrupt data,
// whichever of the old and new logs survives, however torn the temp
// file. Writes racing the compaction land in the log tail that phase B
// splices into the compacted file, so tail loss is detectable too. It
// then verifies that writes acknowledged after the compaction survive a
// metadata-conservative cut, proving the rename was made durable before
// the log accepted them.
func TestPowerLossCompaction(t *testing.T) {
	const dbName = "sim.db"
	pfs := newPowerFS()
	db, err := Open(dbName, StringCodec, StringCodec,
		withFS(pfs), WithAutoCompactDisabled(), WithSweepInterval(0)) // SyncAlways default
	if err != nil {
		t.Fatal(err)
	}

	// Churn with overwrites and deletes so the compaction has garbage to
	// drop, plus a TTL key and a multi-op transaction.
	expected := map[string]string{}
	for i := range 30 {
		k, v := fmt.Sprintf("k%02d", i%10), fmt.Sprintf("v%d", i)
		if err := db.Set(k, v); err != nil {
			t.Fatal(err)
		}
		expected[k] = v
	}
	for _, k := range []string{"k01", "k07"} {
		if _, err := db.Delete(k); err != nil {
			t.Fatal(err)
		}
		delete(expected, k)
	}
	if err := db.SetTTL("ttl:a", "x", time.Hour); err != nil {
		t.Fatal(err)
	}
	expected["ttl:a"] = "x"
	err = db.Update(func(tx *Tx[string, string]) error {
		for j := range 3 {
			if err := tx.Set(fmt.Sprintf("tx:%d", j), "t"); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for j := range 3 {
		expected[fmt.Sprintf("tx:%d", j)] = "t"
	}

	scratch := t.TempDir()

	// Race writes into the compaction window (right after the temp file
	// is created, before the snapshot streams) so they land in the log
	// tail phase B splices in. Each ack point records the cut index from
	// which that write must be durable; cuts inside a write's own window
	// may legitimately land on either side of its fsync.
	type ackPoint struct {
		atCut int
		want  map[string]string
	}
	cutStart := pfs.cutCount()
	acks := []ackPoint{{cutStart, maps.Clone(expected)}}
	var tailErr error
	pfs.onCreate = func() {
		for i := range 3 {
			k := fmt.Sprintf("tail%d", i)
			if err := db.Set(k, "tv"); err != nil {
				tailErr = err
				return
			}
			expected[k] = "tv"
			acks = append(acks, ackPoint{pfs.cutCount(), maps.Clone(expected)})
		}
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if tailErr != nil {
		t.Fatal(tailErr)
	}
	if len(acks) != 4 {
		t.Fatalf("tail writes did not run during compaction (%d ack points)", len(acks))
	}
	cuts := pfs.cutList()[cutStart:]
	if len(cuts) < 10 { // create, tail write/sync pairs, snapshot+splice writes, sync, rename, syncdir
		t.Fatalf("compaction recorded only %d cut points", len(cuts))
	}
	for i, c := range cuts {
		m := 0
		for m+1 < len(acks) && acks[m+1].atCut <= cutStart+i {
			m++
		}
		wants := []map[string]string{acks[m].want}
		if m+1 < len(acks) {
			wants = append(wants, acks[m+1].want)
		}
		label := fmt.Sprintf("compact cut %d [%s]", i, c.label)
		verifyFSCut(t, scratch, dbName, c.conservative, wants, label+" conservative")
		verifyFSCut(t, scratch, dbName, c.eager, wants, label+" eager")
		verifyFSCut(t, scratch, dbName, c.tornConservative, wants, label+" torn conservative")
		verifyFSCut(t, scratch, dbName, c.tornEager, wants, label+" torn eager")
	}

	// Post-compaction acked writes must be durable even if no directory
	// metadata after the last explicit sync persisted.
	for i := range 3 {
		k := fmt.Sprintf("post%d", i)
		if err := db.Set(k, "v"); err != nil {
			t.Fatal(err)
		}
		expected[k] = "v"
		c := pfs.cutNow("post write acked")
		wants := []map[string]string{expected}
		label := fmt.Sprintf("post-compaction write %d", i)
		verifyFSCut(t, scratch, dbName, c.conservative, wants, label+" conservative")
		verifyFSCut(t, scratch, dbName, c.eager, wants, label+" eager")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// verifyFSCut materializes one surviving filesystem state as real files,
// opens the database, and checks it recovers one of the candidate
// states exactly, discards any leftover compaction temp file, and
// accepts new writes. Multiple candidates arise only for cuts inside a
// concurrent write's own ack window, where recovery may land on either
// side of that write's fsync.
func verifyFSCut(t *testing.T, scratch, dbName string, files map[string][]byte, wants []map[string]string, label string) {
	t.Helper()
	dir, err := os.MkdirTemp(scratch, "cut")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	for p, data := range files {
		if err := os.WriteFile(filepath.Join(dir, filepath.Base(p)), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	path := filepath.Join(dir, dbName)
	db, err := Open(path, StringCodec, StringCodec, WithSweepInterval(0))
	if err != nil {
		t.Fatalf("%s: open after power cut: %v", label, err)
	}
	defer db.Close()
	if _, err := os.Stat(path + compactSuffix); !os.IsNotExist(err) {
		t.Fatalf("%s: leftover compaction temp file not discarded", label)
	}
	got := maps.Collect(db.All())
	if db.Len() != len(got) {
		t.Fatalf("%s: Len %d disagrees with iteration count %d", label, db.Len(), len(got))
	}
	if !slices.ContainsFunc(wants, func(w map[string]string) bool { return maps.Equal(got, w) }) {
		t.Fatalf("%s: recovered %d keys, matching none of the %d candidate states: %v",
			label, len(got), len(wants), got)
	}
	if err := db.Set("probe", "ok"); err != nil {
		t.Fatalf("%s: write after recovery: %v", label, err)
	}
}
