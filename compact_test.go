package btypedb

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestCompactShrinksAndPreservesData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path, WithSyncPolicy(SyncNever), WithAutoCompactDisabled())

	// Generate lots of dead records: overwrites and deletes.
	for round := range 5 {
		for i := range 200 {
			key := fmt.Sprintf("k%03d", i)
			if err := db.Set(key, fmt.Sprintf("v%d-%d", round, i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := 0; i < 200; i += 2 {
		if _, err := db.Delete(fmt.Sprintf("k%03d", i)); err != nil {
			t.Fatal(err)
		}
	}

	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size()/2 {
		t.Fatalf("compaction barely shrank the log: %d -> %d bytes", before.Size(), after.Size())
	}

	// Live view unchanged, and DB still writable through the new file.
	if db.Len() != 100 {
		t.Fatalf("Len after compact = %d; want 100", db.Len())
	}
	if err := db.Set("post-compact", "v"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The compacted file must replay to the identical state.
	db2 := openStr(t, path)
	defer db2.Close()
	if db2.Len() != 101 {
		t.Fatalf("Len after reopen = %d; want 101", db2.Len())
	}
	if v, ok := db2.Get("k001"); !ok || v != "v4-1" {
		t.Fatalf("Get(k001) = %q, %v; want last overwrite v4-1", v, ok)
	}
	if db2.Contains("k000") {
		t.Fatal("deleted key resurrected by compaction")
	}
	if !db2.Contains("post-compact") {
		t.Fatal("write after compaction lost")
	}
}

func TestCompactDuringConcurrentWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(path, Int64Codec, Int64Codec, WithSyncPolicy(SyncNever), WithAutoCompactDisabled())
	if err != nil {
		t.Fatal(err)
	}

	const n = 2000
	var wg sync.WaitGroup
	writeErr := make(chan error, 1)
	wg.Go(func() {
		for i := range int64(n) {
			if err := db.Set(i, i*10); err != nil {
				writeErr <- err
				return
			}
		}
	})
	compactErr := make(chan error, 1)
	wg.Go(func() {
		for range 5 {
			if err := db.Compact(); err != nil {
				compactErr <- err
				return
			}
		}
	})
	wg.Wait()
	select {
	case err := <-writeErr:
		t.Fatal(err)
	case err := <-compactErr:
		t.Fatal(err)
	default:
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// No write may be lost, no matter where the compactions landed.
	db2, err := Open(path, Int64Codec, Int64Codec)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if db2.Len() != n {
		t.Fatalf("Len after reopen = %d; want %d", db2.Len(), n)
	}
	for i := range int64(n) {
		if v, ok := db2.Get(i); !ok || v != i*10 {
			t.Fatalf("Get(%d) = %d, %v; want %d", i, v, ok, i*10)
		}
	}
}

func TestAutoCompact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path, WithSyncPolicy(SyncNever), WithAutoCompact(2048, 20))

	// Hammer a tiny keyspace: live data stays ~10 records while the raw
	// log would be ~40 KB, so background compaction must kick in.
	for i := range 2000 {
		if err := db.Set(fmt.Sprintf("k%d", i%10), "value-payload"); err != nil {
			t.Fatal(err)
		}
	}
	// Writers keep running through a compaction's catch-up loop, so the
	// whole burst can end inside one run and land in its tail. The run
	// then chains another compaction for that growth. Wait for the chain
	// to settle: Close stops new runs from starting, so closing straight
	// away would test the race rather than the policy.
	waitAutoCompactIdle(t, db)
	if err := db.Close(); err != nil { // waits for in-flight compaction
		t.Fatal(err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() > 16<<10 {
		t.Fatalf("auto-compaction never ran: log is %d bytes for 10 live keys", fi.Size())
	}

	db2 := openStr(t, path)
	defer db2.Close()
	if db2.Len() != 10 {
		t.Fatalf("Len after reopen = %d; want 10", db2.Len())
	}
}

// waitAutoCompactIdle polls until no auto-compaction is in flight. An
// in-flight run may chain another before clearing the flag (both happen
// under mu), so a single false reading means the chain is done.
func waitAutoCompactIdle(t *testing.T, db *DB[string, string]) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		db.mu.RLock()
		busy := db.compacting
		db.mu.RUnlock()
		if !busy {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("auto-compaction still running after 10s")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCompactLeftoverTempDiscarded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path)
	if err := db.Set("k", "v"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-compaction: a stale temp file next to the log.
	if err := os.WriteFile(path+compactSuffix, []byte("half-written garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	db2 := openStr(t, path)
	defer db2.Close()
	if v, ok := db2.Get("k"); !ok || v != "v" {
		t.Fatalf("Get(k) = %q, %v; want original data", v, ok)
	}
	if _, err := os.Stat(path + compactSuffix); !os.IsNotExist(err) {
		t.Fatal("stale compaction temp file not discarded on open")
	}
}

func TestCompactOnClosedDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != ErrClosed {
		t.Fatalf("Compact on closed DB = %v; want ErrClosed", err)
	}
}

// syncProbeFS is the real filesystem, except that a compaction temp file
// reports each of its Syncs: how many bytes had been written since the
// previous Sync, and whether db.mu was held at that moment. It lets a test
// see which bytes Compact makes durable while writers are paused.
type syncProbeFS struct {
	realFS
	db     *DB[string, string]
	armed  bool         // record only while the test's Compact runs
	onSync func()       // if set, runs once after the first recorded Sync
	syncs  []syncRecord // in call order
}

type syncRecord struct {
	pending int64 // bytes written since the previous Sync of this file
	locked  bool  // db.mu was held (by Compact; the test has no other lockers)
}

func (fs *syncProbeFS) Create(path string) (logfile, error) {
	f, err := fs.realFS.Create(path)
	if err != nil {
		return nil, err
	}
	return &syncProbeFile{logfile: f, fs: fs}, nil
}

// syncProbeFile wraps the temp file. It hides any io.ReaderFrom on the
// underlying *os.File, so io.Copy's tail splice goes through Write and is
// counted.
type syncProbeFile struct {
	logfile
	fs      *syncProbeFS
	pending int64
}

func (f *syncProbeFile) Write(b []byte) (int, error) {
	n, err := f.logfile.Write(b)
	f.pending += int64(n)
	return n, err
}

func (f *syncProbeFile) Sync() error {
	err := f.logfile.Sync()
	if f.fs.armed {
		// TryLock fails only while someone holds db.mu, and the only
		// goroutines here are the test's and Compact's own.
		locked := !f.fs.db.mu.TryLock()
		if !locked {
			f.fs.db.mu.Unlock()
		}
		f.fs.syncs = append(f.fs.syncs, syncRecord{pending: f.pending, locked: locked})
		if hook := f.fs.onSync; hook != nil {
			f.fs.onSync = nil
			hook()
		}
	}
	f.pending = 0
	return err
}

// TestCompactSyncsSnapshotBeforePausingWriters pins down the fix for a
// writer stall that grew with database size. The snapshot is fsynced
// before phase B takes db.mu. A tail that piles up meanwhile is copied
// and fsynced by the unlocked catch-up loop when it exceeds
// compactCatchUpSlack, and left for phase B when it doesn't. So the sync
// under the lock covers at most a slack's worth of tail, and is skipped
// when there is none.
func TestCompactSyncsSnapshotBeforePausingWriters(t *testing.T) {
	val := string(make([]byte, 100))
	for _, tc := range []struct {
		name     string
		tailKeys int  // writes that land after the snapshot's first sync
		tailVal  int  // bytes per tail value
		caughtUp bool // tail exceeds the slack, so the catch-up loop takes it
	}{
		{"no tail", 0, 0, false},
		{"small tail", 3, 100, false},
		{"tail over slack", 1200, 1024, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "test.db")
			pfs := &syncProbeFS{}
			db := openStr(t, path, withFS(pfs), WithSyncPolicy(SyncNever),
				WithAutoCompactDisabled(), WithSweepInterval(0))
			pfs.db = db
			for i := range 500 {
				if err := db.Set(fmt.Sprintf("k%03d", i), val); err != nil {
					t.Fatal(err)
				}
			}

			// The tail writes run inside the snapshot's Sync, after writeSnapshot
			// and before phase B, so phase B has exactly these bytes to splice.
			var tailBytes int64
			var hookErr error
			if tc.tailKeys > 0 {
				pfs.onSync = func() {
					before, err := os.Stat(path)
					if err != nil {
						hookErr = err
						return
					}
					tv := string(make([]byte, tc.tailVal))
					for i := range tc.tailKeys {
						if err := db.Set(fmt.Sprintf("tail%d", i), tv); err != nil {
							hookErr = err
							return
						}
					}
					after, err := os.Stat(path)
					if err != nil {
						hookErr = err
						return
					}
					tailBytes = after.Size() - before.Size()
				}
			}
			pfs.armed = true
			err := db.Compact()
			pfs.armed = false
			if err != nil {
				t.Fatal(err)
			}
			if hookErr != nil {
				t.Fatal(hookErr)
			}

			if len(pfs.syncs) == 0 {
				t.Fatal("compaction never synced its temp file")
			}
			first := pfs.syncs[0]
			if first.locked {
				t.Fatal("snapshot was synced with db.mu held")
			}
			if first.pending < 500*int64(len(val)) {
				t.Fatalf("first sync covered %d bytes; want the whole snapshot", first.pending)
			}
			var lockedSyncs []syncRecord
			for _, s := range pfs.syncs[1:] {
				if s.locked {
					lockedSyncs = append(lockedSyncs, s)
				}
			}
			switch {
			case tc.tailKeys == 0:
				if len(lockedSyncs) != 0 {
					t.Fatalf("synced under db.mu with no tail: %+v", lockedSyncs)
				}
			case tc.caughtUp:
				if tailBytes <= compactCatchUpSlack {
					t.Fatalf("tail is %d bytes; the case needs more than the %d-byte slack",
						tailBytes, compactCatchUpSlack)
				}
				if len(lockedSyncs) != 0 {
					t.Fatalf("synced under db.mu after catching up: %+v", lockedSyncs)
				}
				if len(pfs.syncs) < 2 || pfs.syncs[1].locked || pfs.syncs[1].pending != tailBytes {
					t.Fatalf("syncs = %+v; want an unlocked second sync covering the %d-byte tail",
						pfs.syncs, tailBytes)
				}
			default:
				if tailBytes <= 0 {
					t.Fatal("tail writes did not grow the log")
				}
				if len(lockedSyncs) != 1 || lockedSyncs[0].pending != tailBytes {
					t.Fatalf("syncs under db.mu = %+v; want one covering the %d-byte tail",
						lockedSyncs, tailBytes)
				}
			}

			// Everything, tail included, survives a reopen of the compacted file.
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db2 := openStr(t, path)
			defer db2.Close()
			if want := 500 + tc.tailKeys; db2.Len() != want {
				t.Fatalf("Len after reopen = %d; want %d", db2.Len(), want)
			}
		})
	}
}
