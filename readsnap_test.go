package btypedb

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mustBeginRead is Begin(false) with the test failing on error.
func mustBeginRead(t testing.TB, db *DB[string, string]) *Tx[string, string] {
	t.Helper()
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

// TestReadSnapSharedUntilWrite pins the sharing contract: read-only
// transactions begun with no write in between hold the same snapshot,
// and the first one begun after a write holds a new one.
func TestReadSnapSharedUntilWrite(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "s.db"), WithSyncPolicy(SyncNever))
	defer db.Close()
	if err := db.Set("a", "1"); err != nil {
		t.Fatal(err)
	}

	r1 := mustBeginRead(t, db)
	r2 := mustBeginRead(t, db)
	if r1.state != r2.state || r1.shared != r2.shared {
		t.Fatal("two reads with no write between should share one snapshot")
	}
	// Cache slot + two readers.
	if got := r1.shared.refs.Load(); got != 3 {
		t.Fatalf("refs = %d; want 3", got)
	}

	if err := db.Set("a", "2"); err != nil {
		t.Fatal(err)
	}
	r3 := mustBeginRead(t, db)
	if r3.state == r1.state {
		t.Fatal("a read begun after a write reused the stale snapshot")
	}
	if v, _ := r3.Get("a"); v != "2" {
		t.Fatalf("new read sees a = %q; want 2", v)
	}
	// The old readers still see their snapshot, untouched by the write.
	for i, r := range []*Tx[string, string]{r1, r2} {
		if v, _ := r.Get("a"); v != "1" {
			t.Fatalf("old reader %d sees a = %q; want 1", i+1, v)
		}
	}

	// Dropped from the cache by the write, the old snapshot is held by
	// the two readers alone and dies with the last of them.
	old := r1.shared
	if got := old.refs.Load(); got != 2 {
		t.Fatalf("uncached refs = %d; want 2", got)
	}
	r1.Rollback()
	if got := old.refs.Load(); got != 1 {
		t.Fatalf("refs after first rollback = %d; want 1", got)
	}
	r2.Commit() // a read-only Commit releases like Rollback
	if got := old.refs.Load(); got != deadRefs {
		t.Fatalf("refs after last holder = %d; want deadRefs", got)
	}
	if old.acquire() {
		t.Fatal("acquire resurrected a released snapshot")
	}
	r3.Rollback()
}

// TestReadSnapFreshAfterEveryMutation is the invalidation checklist: for
// each way the head can change, a read cached before the change must
// not be served to a Begin that starts after it. Each case primes the
// cache with a read, mutates, and checks a fresh read.
func TestReadSnapFreshAfterEveryMutation(t *testing.T) {
	for _, occ := range []bool{false, true} {
		t.Run(fmt.Sprintf("occ=%v", occ), func(t *testing.T) {
			opts := []Option{WithSyncPolicy(SyncNever)}
			if occ {
				opts = append(opts, WithConcurrentWrites())
			}
			db := openStr(t, filepath.Join(t.TempDir(), "f.db"), opts...)
			defer db.Close()

			byVal := func(ak, av, bk, bv string) int {
				switch {
				case av < bv:
					return -1
				case av > bv:
					return 1
				}
				return 0
			}
			indexLen := func(tx *Tx[string, string], name string) int {
				n := 0
				for range tx.AscendIndex(name) {
					n++
				}
				return n
			}

			cases := []struct {
				name   string
				mutate func() error
				check  func(tx *Tx[string, string]) error
			}{
				{"Set", func() error { return db.Set("k", "set") }, func(tx *Tx[string, string]) error {
					if v, _ := tx.Get("k"); v != "set" {
						return fmt.Errorf("k = %q", v)
					}
					return nil
				}},
				{"SetTTL", func() error { return db.SetTTL("ttl", "x", time.Hour) }, func(tx *Tx[string, string]) error {
					if _, ok := tx.TTL("ttl"); !ok {
						return errors.New("TTL not visible")
					}
					return nil
				}},
				{"Delete", func() error { _, err := db.Delete("k"); return err }, func(tx *Tx[string, string]) error {
					if tx.Contains("k") {
						return errors.New("deleted key still visible")
					}
					return nil
				}},
				{"Update", func() error {
					return db.Update(func(tx *Tx[string, string]) error { return tx.Set("u", "upd") })
				}, func(tx *Tx[string, string]) error {
					if v, _ := tx.Get("u"); v != "upd" {
						return fmt.Errorf("u = %q", v)
					}
					return nil
				}},
				{"UpdateExclusive", func() error {
					return db.UpdateExclusive(func(tx *Tx[string, string]) error { return tx.Set("x", "excl") })
				}, func(tx *Tx[string, string]) error {
					if v, _ := tx.Get("x"); v != "excl" {
						return fmt.Errorf("x = %q", v)
					}
					return nil
				}},
				{"DeleteRange", func() error { _, err := db.DeleteRange("t", "v"); return err }, func(tx *Tx[string, string]) error {
					if tx.Contains("ttl") || tx.Contains("u") {
						return errors.New("range-deleted keys still visible")
					}
					return nil
				}},
				{"CreateIndex", func() error { return db.CreateIndex("by-val", byVal) }, func(tx *Tx[string, string]) error {
					if n := indexLen(tx, "by-val"); n != tx.Len() {
						return fmt.Errorf("new index has %d entries; want %d", n, tx.Len())
					}
					return nil
				}},
				{"DropIndex", func() error { return db.DropIndex("by-val") }, func(tx *Tx[string, string]) error {
					if n := indexLen(tx, "by-val"); n != 0 {
						return fmt.Errorf("dropped index still yields %d entries", n)
					}
					return nil
				}},
			}
			for _, c := range cases {
				// Prime: a read leaves the snapshot cached (no holders
				// but the cache once it rolls back).
				prime := mustBeginRead(t, db)
				prime.Rollback()
				if db.readSnap.Load() == nil {
					t.Fatalf("%s: priming read left nothing cached", c.name)
				}
				if err := c.mutate(); err != nil {
					t.Fatalf("%s: %v", c.name, err)
				}
				if err := db.View(c.check); err != nil {
					t.Fatalf("%s: read after the mutation: %v", c.name, err)
				}
			}
		})
	}
}

// TestReadSnapExpiredKeyBecomesAbsent covers the one change that does not
// mutate the head: a TTL passing. Expiry is evaluated per read against
// the clock, so a cached snapshot taken before the deadline still
// reports the key absent after it.
func TestReadSnapExpiredKeyBecomesAbsent(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "e.db"),
		WithSyncPolicy(SyncNever), WithSweepInterval(time.Hour))
	defer db.Close()
	if err := db.SetTTL("k", "v", 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	r := mustBeginRead(t, db)
	r.Rollback() // cached, holds k with its deadline
	time.Sleep(40 * time.Millisecond)
	if err := db.View(func(tx *Tx[string, string]) error {
		if tx.Contains("k") {
			return errors.New("expired key visible through the cached snapshot")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestReadSnapAfterClose: Close uncaches the snapshot, so a read-only
// Begin after it fails instead of taking the lock-free path, while a
// reader opened before Close finishes normally.
func TestReadSnapAfterClose(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "c.db"), WithSyncPolicy(SyncNever))
	if err := db.Set("a", "1"); err != nil {
		t.Fatal(err)
	}
	open := mustBeginRead(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Begin(false); !errors.Is(err, ErrClosed) {
		t.Fatalf("Begin(false) after Close = %v; want ErrClosed", err)
	}
	if v, _ := open.Get("a"); v != "1" {
		t.Fatalf("reader opened before Close sees a = %q", v)
	}
	if err := open.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := open.shared.refs.Load(); got != deadRefs {
		t.Fatalf("refs after Close and last rollback = %d; want deadRefs", got)
	}
}

// TestReadSnapSavepoints: savepoints in a read-only transaction take no
// copy of the shared state and still behave as marks.
func TestReadSnapSavepoints(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "sp.db"), WithSyncPolicy(SyncNever))
	defer db.Close()
	if err := db.Set("a", "1"); err != nil {
		t.Fatal(err)
	}
	tx := mustBeginRead(t, db)
	sp1, err := tx.Savepoint()
	if err != nil {
		t.Fatal(err)
	}
	sp2, err := tx.Savepoint()
	if err != nil {
		t.Fatal(err)
	}
	if sp1.state != nil || sp2.state != nil {
		t.Fatal("read-only savepoint copied the shared state")
	}
	if err := tx.RollbackTo(sp1); err != nil {
		t.Fatal(err)
	}
	if err := tx.RollbackTo(sp2); !errors.Is(err, ErrSavepointInvalid) {
		t.Fatalf("RollbackTo a destroyed later mark = %v; want ErrSavepointInvalid", err)
	}
	if v, _ := tx.Get("a"); v != "1" {
		t.Fatalf("a = %q after RollbackTo", v)
	}
	sp3, _ := tx.Savepoint()
	if err := tx.Release(sp1); err != nil {
		t.Fatal(err)
	}
	if err := tx.RollbackTo(sp3); !errors.Is(err, ErrSavepointInvalid) {
		t.Fatalf("mark after a released one = %v; want ErrSavepointInvalid", err)
	}
	if _, err := tx.Savepoint(); err != nil {
		t.Fatal(err)
	}
	// Rollback with a mark outstanding: nothing to release, no panic.
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// TestReadSnapRefProtocol hammers acquire/unref directly. Readers take
// and drop references while the cache's reference is dropped midway;
// exactly one unref must win the release, and no acquire may succeed
// after it.
func TestReadSnapRefProtocol(t *testing.T) {
	for round := range 200 {
		s := &sharedRead[string, string]{}
		s.refs.Store(1) // the cache slot
		var released atomic.Int32
		var lateAcquire atomic.Bool
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				for range 50 {
					if !s.acquire() {
						return // dead: a real reader would take the slow path
					}
					if released.Load() != 0 {
						lateAcquire.Store(true)
					}
					if s.unref() {
						released.Add(1)
					}
				}
			})
		}
		wg.Go(func() {
			if s.unref() { // dropReadSnap
				released.Add(1)
			}
		})
		wg.Wait()
		if got := released.Load(); got != 1 {
			t.Fatalf("round %d: released %d times; want exactly 1", round, got)
		}
		if lateAcquire.Load() {
			t.Fatalf("round %d: an acquire succeeded after release", round)
		}
		if s.acquire() {
			t.Fatalf("round %d: acquire after release succeeded", round)
		}
	}
}

// TestReadSnapHammer is the race-detector check of sharing: one-shot
// Views (the pattern sharing exists for), long scans and read-only
// savepoints run against every kind of head mutation, plus compaction.
// A writer bumps a counter and then reads it back through a fresh View:
// a stale cached snapshot would show the old value.
func TestReadSnapHammer(t *testing.T) {
	for _, occ := range []bool{false, true} {
		t.Run(fmt.Sprintf("occ=%v", occ), func(t *testing.T) {
			opts := []Option{WithSyncPolicy(SyncNever), WithSweepInterval(2 * time.Millisecond)}
			if occ {
				opts = append(opts, WithConcurrentWrites())
			}
			db := openStr(t, filepath.Join(t.TempDir(), "h.db"), opts...)
			defer db.Close()
			for i := range 200 {
				if err := db.Set(fmt.Sprintf("k%03d", i), "0"); err != nil {
					t.Fatal(err)
				}
			}

			// writers and readers wait separately: readers loop until
			// every writer is done, so the run covers the whole churn.
			var writers, readers sync.WaitGroup
			stop := make(chan struct{})
			errCh := make(chan error, 32)
			fail := func(err error) {
				select {
				case errCh <- err:
				default:
				}
			}

			// Counter writer with read-your-write through View.
			writers.Go(func() {
				for n := 1; n <= 300; {
					want := strconv.Itoa(n)
					err := db.Update(func(tx *Tx[string, string]) error { return tx.Set("ctr", want) })
					if errors.Is(err, ErrTxConflict) {
						continue // occ: retry the same value
					}
					if err != nil {
						fail(err)
						return
					}
					var got string
					if err := db.View(func(tx *Tx[string, string]) error {
						got, _ = tx.Get("ctr")
						return nil
					}); err != nil {
						fail(err)
						return
					}
					if got != want {
						fail(fmt.Errorf("read after commit: ctr = %q; want %q", got, want))
						return
					}
					n++
				}
			})
			// Direct writes, deletes, TTLs, index churn.
			writers.Go(func() {
				for i := range 400 {
					k := fmt.Sprintf("k%03d", i%200)
					var err error
					switch i % 5 {
					case 0:
						err = db.Set(k, strconv.Itoa(i))
					case 1:
						_, err = db.Delete(k)
					case 2:
						err = db.SetTTL(k, "t", time.Millisecond)
					case 3:
						err = db.CreateIndex("ix", func(ak, av, bk, bv string) int {
							switch {
							case av < bv:
								return -1
							case av > bv:
								return 1
							}
							return 0
						})
					case 4:
						err = db.DropIndex("ix")
					}
					if err != nil {
						fail(fmt.Errorf("churn op %d: %w", i%5, err))
						return
					}
				}
			})
			writers.Go(func() {
				for range 5 {
					if err := db.Compact(); err != nil {
						fail(err)
						return
					}
				}
			})

			// Readers: one-shot Views, scans, savepoints.
			for r := range 6 {
				readers.Go(func() {
					for {
						select {
						case <-stop:
							return
						default:
						}
						err := db.View(func(tx *Tx[string, string]) error {
							switch r % 3 {
							case 0:
								tx.Get(fmt.Sprintf("k%03d", r))
							case 1:
								n := 0
								for range tx.All() {
									n++
								}
								if n > tx.Len() {
									return errors.New("scan saw more pairs than Len")
								}
								for range tx.AscendIndex("ix") {
								}
							case 2:
								sp, err := tx.Savepoint()
								if err != nil {
									return err
								}
								tx.Get("ctr")
								return tx.RollbackTo(sp)
							}
							return nil
						})
						if err != nil {
							fail(err)
							return
						}
					}
				})
			}

			writers.Wait()
			close(stop)
			readers.Wait()
			select {
			case err := <-errCh:
				t.Fatal(err)
			default:
			}
		})
	}
}

// BenchmarkViewGetParallel is one-shot read-only transactions from
// GOMAXPROCS goroutines, each doing a single Get: the access pattern of
// a point lookup API built on View (bytdb's Engine.Get).
func BenchmarkViewGetParallel(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "b.db"), Int64Codec, Int64Codec, WithSyncPolicy(SyncNever))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	const n = 10_000
	if err := db.Update(func(tx *Tx[int64, int64]) error {
		for i := range int64(n) {
			if err := tx.Set(i, i); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	var seed atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := seed.Add(7919)
		for pb.Next() {
			i++
			err := db.View(func(tx *Tx[int64, int64]) error {
				if _, ok := tx.Get(i % n); !ok {
					return errors.New("missing")
				}
				return nil
			})
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}
