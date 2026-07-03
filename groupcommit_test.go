package btypedb

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slowSyncFile wraps a real log file, counting fsyncs and making each
// one slow enough that concurrent committers pile up behind it — the
// situation group commit exists for.
type slowSyncFile struct {
	*os.File
	syncs atomic.Int64
}

func (s *slowSyncFile) Sync() error {
	s.syncs.Add(1)
	time.Sleep(500 * time.Microsecond)
	return s.File.Sync()
}

// TestGroupCommit drives many concurrent SyncAlways writers and checks
// that (a) fsyncs coalesce — far fewer syncs than acknowledged writes —
// and (b) every acknowledged write is actually durable: the log replays
// completely after reopening.
func TestGroupCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "group.db")
	raw, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	sf := &slowSyncFile{File: raw}
	db, err := Open(path, StringCodec, StringCodec,
		withLogfile(sf), WithAutoCompactDisabled(), WithSweepInterval(0)) // SyncAlways default
	if err != nil {
		t.Fatal(err)
	}

	const writers, perWriter = 8, 25
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, writers)
	for w := range writers {
		wg.Go(func() {
			<-start
			for i := range perWriter {
				k := fmt.Sprintf("w%d:%03d", w, i)
				var err error
				if i%5 == 4 { // mix in transactional commits
					err = db.Update(func(tx *Tx[string, string]) error {
						return tx.Set(k, k)
					})
				} else {
					err = db.Set(k, k)
				}
				if err != nil {
					errs <- err
					return
				}
			}
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	total := int64(writers * perWriter)
	if got := sf.syncs.Load(); got >= total {
		t.Fatalf("%d fsyncs for %d writes: no group-commit coalescing", got, total)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Every acknowledged write must have been durable at ack time.
	db2 := openStr(t, path, WithSweepInterval(0))
	defer db2.Close()
	if got := db2.Len(); got != int(total) {
		t.Fatalf("recovered %d keys; want %d", got, total)
	}
	for w := range writers {
		for i := range perWriter {
			k := fmt.Sprintf("w%d:%03d", w, i)
			if v, ok := db2.Get(k); !ok || v != k {
				t.Fatalf("Get(%q) = %q, %v after reopen", k, v, ok)
			}
		}
	}
}

// TestGroupCommitWithCompaction exercises the file-swap race: a manual
// compaction retires the log handle while SyncAlways writers are in
// flight. No write may fail, and everything must replay after reopen.
func TestGroupCommitWithCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "groupcompact.db")
	db := openStr(t, path, WithAutoCompactDisabled(), WithSweepInterval(0)) // SyncAlways default

	const writers, perWriter = 4, 30
	var wg sync.WaitGroup
	errs := make(chan error, writers+1)
	for w := range writers {
		wg.Go(func() {
			for i := range perWriter {
				k := fmt.Sprintf("w%d:%03d", w, i)
				if err := db.Set(k, k); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Go(func() {
		for range 5 {
			if err := db.Compact(); err != nil {
				errs <- err
				return
			}
		}
	})
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := openStr(t, path, WithSweepInterval(0))
	defer db2.Close()
	if got := db2.Len(); got != writers*perWriter {
		t.Fatalf("recovered %d keys; want %d", got, writers*perWriter)
	}
}
