package btypedb

import (
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
)

// openOCC opens a fresh concurrent-writes DB. SyncNever keeps the
// high-commit-count tests fast; durability is exercised separately by
// the reopen tests, which Close (and therefore sync) first.
func openOCC(t *testing.T) (*DB[string, int64], string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "occ.db")
	db, err := Open(path, StringCodec, Int64Codec, WithConcurrentWrites(), WithSyncPolicy(SyncNever))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, path
}

// Two transactions writing the same key: first committer wins, the
// second fails with ErrTxConflict and leaves no trace.
func TestOCCWriteWriteConflict(t *testing.T) {
	db, _ := openOCC(t)

	tx1, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := db.Begin(true) // would deadlock here without occ
	if err != nil {
		t.Fatal(err)
	}
	if err := tx1.Set("k", 1); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Set("k", 2); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("first committer: %v", err)
	}
	if err := tx2.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("second committer: %v; want ErrTxConflict", err)
	}
	if v, ok := db.Get("k"); !ok || v != 1 {
		t.Fatalf("Get(k) = %d, %v; want 1, true (loser's write leaked)", v, ok)
	}
}

// Disjoint writers both commit; the loser of the pointer-swap race
// replays onto the winner's head, so neither write is lost.
func TestOCCDisjointWritersReplay(t *testing.T) {
	db, _ := openOCC(t)

	tx1, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx1.Set("a", 1); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Set("b", 2); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("disjoint commit after head moved: %v", err)
	}
	for k, want := range map[string]int64{"a": 1, "b": 2} {
		if v, ok := db.Get(k); !ok || v != want {
			t.Fatalf("Get(%s) = %d, %v; want %d, true", k, v, ok, want)
		}
	}
}

// A direct one-shot DB.Set is a commit too: an open transaction that
// wrote the same key must lose to it.
func TestOCCDirectWriteConflictsWithTx(t *testing.T) {
	db, _ := openOCC(t)

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("k", 10); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("k", 20); err != nil { // no longer blocks behind tx
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("commit over direct write: %v; want ErrTxConflict", err)
	}
	if v, _ := db.Get("k"); v != 20 {
		t.Fatalf("Get(k) = %d; want 20", v)
	}
}

// A DeleteRange claims its whole interval: a concurrent insert into the
// range conflicts in both commit orders, so a key can never silently
// survive a range delete (the TRUNCATE guarantee).
func TestOCCRangeDeleteVsInsert(t *testing.T) {
	t.Run("insert commits first", func(t *testing.T) {
		db, _ := openOCC(t)
		if err := db.Set("x", 1); err != nil { // give the range something to delete
			t.Fatal(err)
		}
		txDel, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		txIns, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := txDel.DeleteRange("a", "z"); err != nil {
			t.Fatal(err)
		}
		if err := txIns.Set("m", 1); err != nil {
			t.Fatal(err)
		}
		if err := txIns.Commit(); err != nil {
			t.Fatal(err)
		}
		// Their key "m" lands inside our claimed [a, z).
		if err := txDel.Commit(); !errors.Is(err, ErrTxConflict) {
			t.Fatalf("range-delete commit: %v; want ErrTxConflict", err)
		}
	})
	t.Run("range delete commits first", func(t *testing.T) {
		db, _ := openOCC(t)
		if err := db.Set("x", 1); err != nil { // give the range something to delete
			t.Fatal(err)
		}
		txDel, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		txIns, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := txDel.DeleteRange("a", "z"); err != nil {
			t.Fatal(err)
		}
		if err := txIns.Set("m", 1); err != nil {
			t.Fatal(err)
		}
		if err := txDel.Commit(); err != nil {
			t.Fatal(err)
		}
		// Our key "m" lands inside their claimed [a, z).
		if err := txIns.Commit(); !errors.Is(err, ErrTxConflict) {
			t.Fatalf("insert commit: %v; want ErrTxConflict", err)
		}
		if db.Len() != 0 {
			t.Fatalf("Len = %d; want 0 (insert survived the range delete)", db.Len())
		}
	})
}

// A transaction whose only action was an empty DeleteRange publishes
// nothing (nops == 0), so its range claim never enters the ring and a
// concurrent insert rightly survives: the no-op transaction serializes
// as if it ran first. This pins that boundary deliberately.
func TestOCCEmptyRangeNoPublishNoClaim(t *testing.T) {
	db, _ := openOCC(t)

	txIns, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	txDel, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := txDel.DeleteRange("a", "z"); err != nil || n != 0 {
		t.Fatalf("DeleteRange = %d, %v; want 0, nil", n, err)
	}
	if err := txDel.Commit(); err != nil { // publishes nothing
		t.Fatal(err)
	}
	if err := txIns.Set("m", 1); err != nil {
		t.Fatal(err)
	}
	if err := txIns.Commit(); err != nil {
		t.Fatalf("insert after no-op range delete: %v; want success", err)
	}
	if v, ok := db.Get("m"); !ok || v != 1 {
		t.Fatalf("Get(m) = %d, %v; want 1, true", v, ok)
	}
}

// The interval claim holds even when the range was empty at execution
// time, as long as the transaction publishes at all.
func TestOCCEmptyRangeStillClaims(t *testing.T) {
	db, _ := openOCC(t)

	txIns, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	txDel, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := txDel.DeleteRange("a", "z"); err != nil || n != 0 {
		t.Fatalf("DeleteRange = %d, %v; want 0, nil", n, err)
	}
	if err := txDel.Set("zz", 1); err != nil { // outside the range; makes the tx publish
		t.Fatal(err)
	}
	if err := txDel.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := txIns.Set("m", 1); err != nil {
		t.Fatal(err)
	}
	if err := txIns.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("insert into claimed-empty range: %v; want ErrTxConflict", err)
	}
}

// A snapshot older than the validation ring's reach cannot prove itself
// conflict-free and must abort, even with genuinely disjoint keys.
func TestOCCRingEvictionAborts(t *testing.T) {
	db, _ := openOCC(t)

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("mine", 1); err != nil {
		t.Fatal(err)
	}
	// Push the ring past capacity with disjoint direct writes.
	for i := range occRingCap + 1 {
		if err := db.Set(fmt.Sprintf("filler-%05d", i), int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("commit past ring reach: %v; want ErrTxConflict", err)
	}
}

// A commit too large to track key-by-key becomes a wildcard entry that
// conflicts with every overlapping-in-time transaction.
func TestOCCWildcardBulkCommit(t *testing.T) {
	db, _ := openOCC(t)

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("mine", 1); err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(bulk *Tx[string, int64]) error {
		for i := range occMaxEntryKeys + 1 {
			if err := bulk.Set(fmt.Sprintf("bulk-%05d", i), int64(i)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("commit over wildcard bulk: %v; want ErrTxConflict", err)
	}
}

// Rolling back to a savepoint shrinks the OCC write set with it: an op
// discarded by RollbackTo must neither conflict nor replay.
func TestOCCSavepointShrinksWriteSet(t *testing.T) {
	db, _ := openOCC(t)

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("keep", 1); err != nil {
		t.Fatal(err)
	}
	sp, err := tx.Savepoint()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("dropped", 2); err != nil {
		t.Fatal(err)
	}
	if err := tx.RollbackTo(sp); err != nil {
		t.Fatal(err)
	}
	// Another commit takes "dropped": would conflict if the rolled-back
	// op lingered in the write set; must not, since it was discarded.
	if err := db.Set("dropped", 99); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit after savepoint rollback: %v; want success", err)
	}
	if v, _ := db.Get("keep"); v != 1 {
		t.Fatalf("Get(keep) = %d; want 1", v)
	}
	// And replay must not resurrect the rolled-back write.
	if v, _ := db.Get("dropped"); v != 99 {
		t.Fatalf("Get(dropped) = %d; want 99 (rolled-back op replayed)", v)
	}
}

// The default mode is untouched: no option, no conflicts, writers
// serialize at Begin exactly as before.
func TestOCCOffByDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := Open(path, StringCodec, Int64Codec, WithSyncPolicy(SyncNever))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for range 50 {
				if err := db.Update(func(tx *Tx[string, int64]) error {
					v, _ := tx.Get("counter")
					return tx.Set("counter", v+1)
				}); err != nil {
					t.Errorf("writer %d: %v", w, err)
					return
				}
			}
		})
	}
	wg.Wait()
	if v, _ := db.Get("counter"); v != 200 {
		t.Fatalf("counter = %d; want 200 (lost update in serialized mode)", v)
	}
}

// The classic snapshot-isolation soak: concurrent transfers between
// accounts with retry-on-conflict, total balance conserved throughout
// and across a reopen (WAL replay must reconstruct the commit order).
func TestOCCBankTransferInvariant(t *testing.T) {
	db, path := openOCC(t)

	const accounts = 8
	const perAccount = 1000
	const writers = 8
	const transfersPerWriter = 200

	for i := range accounts {
		if err := db.Set(fmt.Sprintf("acct-%d", i), perAccount); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	var conflicts int64
	var conflictMu sync.Mutex
	for w := range writers {
		wg.Go(func() {
			rng := rand.New(rand.NewSource(int64(w)))
			for range transfersPerWriter {
				from := fmt.Sprintf("acct-%d", rng.Intn(accounts))
				to := fmt.Sprintf("acct-%d", rng.Intn(accounts))
				if from == to {
					continue
				}
				// Retry loop: ErrTxConflict means "run it again from
				// Begin", the contract pgwire will surface as 40001.
				for {
					err := db.Update(func(tx *Tx[string, int64]) error {
						fv, _ := tx.Get(from)
						tv, _ := tx.Get(to)
						if fv < 1 {
							return nil // insufficient funds; commit empty
						}
						if err := tx.Set(from, fv-1); err != nil {
							return err
						}
						return tx.Set(to, tv+1)
					})
					if errors.Is(err, ErrTxConflict) {
						conflictMu.Lock()
						conflicts++
						conflictMu.Unlock()
						continue
					}
					if err != nil {
						t.Errorf("transfer: %v", err)
					}
					break
				}
			}
		})
	}
	wg.Wait()

	sum := func(d *DB[string, int64]) int64 {
		var s int64
		for _, v := range d.All() {
			s += v
		}
		return s
	}
	if got := sum(db); got != accounts*perAccount {
		t.Fatalf("total = %d; want %d (money created or destroyed)", got, accounts*perAccount)
	}
	t.Logf("conflicts retried: %d", conflicts)

	// Same invariant after replaying the log from disk.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(path, StringCodec, Int64Codec, WithConcurrentWrites())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if got := sum(db2); got != accounts*perAccount {
		t.Fatalf("total after reopen = %d; want %d (WAL order broken)", got, accounts*perAccount)
	}
}

// Many disjoint writers, all overlapping in time, all forced through
// the replay path; every write must survive to disk.
func TestOCCConcurrentDisjointWritersReopen(t *testing.T) {
	db, path := openOCC(t)

	const writers = 8
	const keysEach = 100
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			<-start
			for i := range keysEach {
				err := db.Update(func(tx *Tx[string, int64]) error {
					return tx.Set(fmt.Sprintf("w%d-k%03d", w, i), int64(i))
				})
				if err != nil {
					t.Errorf("writer %d key %d: %v", w, i, err)
					return
				}
			}
		})
	}
	close(start)
	wg.Wait()

	if db.Len() != writers*keysEach {
		t.Fatalf("Len = %d; want %d", db.Len(), writers*keysEach)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(path, StringCodec, Int64Codec, WithConcurrentWrites())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if db2.Len() != writers*keysEach {
		t.Fatalf("Len after reopen = %d; want %d", db2.Len(), writers*keysEach)
	}
}

// A conflicted transaction is fully rolled back: locks free, snapshot
// released, and an immediate retry succeeds.
func TestOCCConflictThenRetrySucceeds(t *testing.T) {
	db, _ := openOCC(t)

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("k", 1); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("k", 2); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("commit: %v; want ErrTxConflict", err)
	}
	// The retry sees the winner's value and lands cleanly.
	err = db.Update(func(tx *Tx[string, int64]) error {
		v, _ := tx.Get("k")
		if v != 2 {
			t.Errorf("retry snapshot Get(k) = %d; want 2", v)
		}
		return tx.Set("k", v+1)
	})
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := db.Get("k"); v != 3 {
		t.Fatalf("Get(k) = %d; want 3", v)
	}
}

// benchTxWork is one transaction: reads across a spread keyspace, then
// a few writes. The read count is the experiment's dial — reads are the
// in-transaction work that legacy mode serializes under the writer lock
// and occ mode runs concurrently. Light transactions expose OCC's
// per-commit overhead (extra snapshot copy, validation, replay); heavy
// ones expose the lock-hold time OCC reclaims.
func benchTxWork(tx *Tx[string, int64], rng *rand.Rand, keyspace, reads int) error {
	var acc int64
	for range reads {
		v, _ := tx.Get(fmt.Sprintf("key-%06d", rng.Intn(keyspace)))
		acc += v
	}
	for range 5 {
		if err := tx.Set(fmt.Sprintf("key-%06d", rng.Intn(keyspace)), acc); err != nil {
			return err
		}
	}
	return nil
}

// benchParallelUpdates drives GOMAXPROCS goroutines of benchTxWork
// through Update, retrying conflicts (occ mode only produces them).
func benchParallelUpdates(b *testing.B, occ bool, reads int) {
	opts := []Option{WithSyncPolicy(SyncNever), WithSweepInterval(0), WithAutoCompactDisabled()}
	if occ {
		opts = append(opts, WithConcurrentWrites())
	}
	db, err := Open(filepath.Join(b.TempDir(), "bench.db"), StringCodec, Int64Codec, opts...)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const keyspace = 100_000
	for i := range keyspace {
		if err := db.Set(fmt.Sprintf("key-%06d", i), int64(i)); err != nil {
			b.Fatal(err)
		}
	}

	var seed int64
	var seedMu sync.Mutex
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		seedMu.Lock()
		seed++
		rng := rand.New(rand.NewSource(seed))
		seedMu.Unlock()
		for pb.Next() {
			for {
				err := db.Update(func(tx *Tx[string, int64]) error {
					return benchTxWork(tx, rng, keyspace, reads)
				})
				if errors.Is(err, ErrTxConflict) {
					continue
				}
				if err != nil {
					b.Error(err)
				}
				break
			}
		}
	})
}

func BenchmarkParallelUpdatesLightSerialized(b *testing.B) { benchParallelUpdates(b, false, 20) }
func BenchmarkParallelUpdatesLightOCC(b *testing.B)        { benchParallelUpdates(b, true, 20) }
func BenchmarkParallelUpdatesHeavySerialized(b *testing.B) { benchParallelUpdates(b, false, 2000) }
func BenchmarkParallelUpdatesHeavyOCC(b *testing.B)        { benchParallelUpdates(b, true, 2000) }
