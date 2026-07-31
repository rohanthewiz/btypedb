package btypedb

// occ_serial_test.go: the serializable upgrade over OCC snapshot
// isolation — read-set validation (TrackReads / MarkRead /
// MarkReadRange) and full serialization for schema-critical work
// (MarkExclusive / UpdateExclusive).

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// A tracked point read conflicts with a later write of that key, even
// though the reader's own writes are disjoint — the read-write overlap
// snapshot isolation admits and serializability must not.
func TestSerialPointReadConflict(t *testing.T) {
	db, _ := openOCC(t)
	if err := db.Set("a", 1); err != nil {
		t.Fatal(err)
	}

	tx1, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	tx1.TrackReads()
	if v, ok := tx1.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a) = %d, %v; want 1, true", v, ok)
	}
	tx1.MarkRead("a")
	if err := tx1.Set("b", 10); err != nil {
		t.Fatal(err)
	}

	if err := db.Set("a", 2); err != nil { // intervening write of the read key
		t.Fatal(err)
	}
	if err := tx1.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("commit after read key changed: %v; want ErrTxConflict", err)
	}

	// Control: the identical dance without TrackReads is plain SI and
	// commits — proving the conflict above came from the read set.
	tx2, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := tx2.Get("a"); !ok {
		t.Fatal("Get(a) miss")
	}
	tx2.MarkRead("a") // no-op: TrackReads was not called
	if err := tx2.Set("b", 20); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("a", 3); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("SI control commit: %v", err)
	}
}

// A tracked absent read (existence probe that missed) conflicts with a
// concurrent insert of that key: absence is information.
func TestSerialAbsentReadConflict(t *testing.T) {
	db, _ := openOCC(t)

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	tx.TrackReads()
	if _, ok := tx.Get("missing"); ok {
		t.Fatal("key should be absent")
	}
	tx.MarkRead("missing")
	if err := tx.Set("elsewhere", 1); err != nil {
		t.Fatal(err)
	}

	if err := db.Set("missing", 99); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("commit after absent key appeared: %v; want ErrTxConflict", err)
	}
}

// A tracked scan range conflicts with an insert INTO the range (a
// phantom) but not with one beyond it.
func TestSerialReadRangePhantom(t *testing.T) {
	for _, tc := range []struct {
		insertKey string
		conflict  bool
	}{
		{"g", true},  // inside [a, m): phantom
		{"z", false}, // outside: unrelated
	} {
		t.Run(tc.insertKey, func(t *testing.T) {
			db, _ := openOCC(t)
			tx, err := db.Begin(true)
			if err != nil {
				t.Fatal(err)
			}
			tx.TrackReads()
			tx.MarkReadRange("a", "m") // "scanned [a, m) and saw nothing"
			if err := tx.Set("result", 0); err != nil {
				t.Fatal(err)
			}
			if err := db.Set(tc.insertKey, 1); err != nil {
				t.Fatal(err)
			}
			err = tx.Commit()
			if tc.conflict && !errors.Is(err, ErrTxConflict) {
				t.Fatalf("phantom insert %q: commit = %v; want ErrTxConflict", tc.insertKey, err)
			}
			if !tc.conflict && err != nil {
				t.Fatalf("unrelated insert %q: commit = %v; want nil", tc.insertKey, err)
			}
		})
	}
}

// A concurrent range delete conflicts with both a tracked point read
// inside its interval and a tracked scan overlapping it.
func TestSerialReadVsRangeDelete(t *testing.T) {
	db, _ := openOCC(t)
	for _, k := range []string{"c", "e", "x"} {
		if err := db.Set(k, 1); err != nil {
			t.Fatal(err)
		}
	}

	byKey, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	byKey.TrackReads()
	byKey.Get("c")
	byKey.MarkRead("c")
	if err := byKey.Set("out1", 1); err != nil {
		t.Fatal(err)
	}

	byRange, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	byRange.TrackReads()
	byRange.MarkReadRange("d", "f")
	if err := byRange.Set("out2", 1); err != nil {
		t.Fatal(err)
	}

	if _, err := db.DeleteRange("a", "m"); err != nil {
		t.Fatal(err)
	}
	if err := byKey.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("point read under deleted range: %v; want ErrTxConflict", err)
	}
	if err := byRange.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("scan overlapping deleted range: %v; want ErrTxConflict", err)
	}
}

// The classic write-skew shape: T1 reads x and writes y, T2 reads y
// and writes x. Snapshot isolation lets both commit (the anomaly);
// with read tracking the second committer must abort.
func TestSerialWriteSkew(t *testing.T) {
	run := func(tracked bool) (commitErrs []error) {
		db, _ := openOCC(t)
		if err := db.Set("x", 1); err != nil {
			t.Fatal(err)
		}
		if err := db.Set("y", 1); err != nil {
			t.Fatal(err)
		}
		t1, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		t2, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		if tracked {
			t1.TrackReads()
			t2.TrackReads()
		}
		t1.Get("x")
		t1.MarkRead("x")
		if err := t1.Set("y", 0); err != nil {
			t.Fatal(err)
		}
		t2.Get("y")
		t2.MarkRead("y")
		if err := t2.Set("x", 0); err != nil {
			t.Fatal(err)
		}
		return []error{t1.Commit(), t2.Commit()}
	}

	// Untracked (SI): both commit — the skew is real, which is exactly
	// what makes the tracked assertion below meaningful.
	for i, err := range run(false) {
		if err != nil {
			t.Fatalf("SI commit %d: %v; want both to commit (write skew)", i+1, err)
		}
	}
	// Tracked: first wins, second conflicts.
	errs := run(true)
	if errs[0] != nil {
		t.Fatalf("tracked first committer: %v", errs[0])
	}
	if !errors.Is(errs[1], ErrTxConflict) {
		t.Fatalf("tracked second committer: %v; want ErrTxConflict", errs[1])
	}
}

// Reads recorded before a savepoint rollback stay in the read set:
// results may already have escaped to the application, so Commit keeps
// vouching for them.
func TestSerialReadsSurviveSavepointRollback(t *testing.T) {
	db, _ := openOCC(t)
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	tx.TrackReads()
	sp, err := tx.Savepoint()
	if err != nil {
		t.Fatal(err)
	}
	tx.MarkRead("a")
	if err := tx.RollbackTo(sp); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("b", 1); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("a", 1); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("commit after rolled-back read's key changed: %v; want ErrTxConflict", err)
	}
}

// An exclusive transaction refuses to commit over ANY intervening
// commit — even one touching completely unrelated keys.
func TestSerialExclusiveAbortsOnAnyCommit(t *testing.T) {
	db, _ := openOCC(t)
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	tx.MarkExclusive()
	if err := tx.Set("schema", 1); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("unrelated", 1); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("exclusive commit over moved head: %v; want ErrTxConflict", err)
	}
}

// A committed exclusive transaction conflicts every transaction that
// overlapped it in time out, whatever keys they touched — the wildcard
// publish that makes schema changes safe against in-flight writers.
func TestSerialExclusiveWildcardAbortsOverlappers(t *testing.T) {
	db, _ := openOCC(t)

	writer, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Set("user-key", 1); err != nil {
		t.Fatal(err)
	}

	excl, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	excl.MarkExclusive()
	if err := excl.Set("schema", 1); err != nil {
		t.Fatal(err)
	}
	if err := excl.Commit(); err != nil {
		t.Fatalf("exclusive commit on unmoved head: %v", err)
	}

	if err := writer.Commit(); !errors.Is(err, ErrTxConflict) {
		t.Fatalf("overlapping writer after exclusive commit: %v; want ErrTxConflict", err)
	}
}

// UpdateExclusive freezes the head for the closure's lifetime, so it
// can never lose — even while concurrent writers hammer the store —
// and its wildcard still aborts writers that overlapped it.
func TestSerialUpdateExclusiveUnderLoad(t *testing.T) {
	db, _ := openOCC(t)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				// Ignore conflicts: the writers exist to move the head.
				db.Update(func(tx *Tx[string, int64]) error {
					return tx.Set(fmt.Sprintf("w%d-%d", w, i), int64(i))
				})
			}
		})
	}

	for i := range 20 {
		err := db.UpdateExclusive(func(tx *Tx[string, int64]) error {
			return tx.Set("schema", int64(i))
		})
		if err != nil {
			t.Fatalf("UpdateExclusive #%d: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()

	if v, ok := db.Get("schema"); !ok || v != 19 {
		t.Fatalf("Get(schema) = %d, %v; want 19, true", v, ok)
	}
}

// In default (serialized) mode the whole serializable surface is a
// no-op: writers already fully serialize, so nothing can conflict.
func TestSerialNoopInDefaultMode(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "plain.db"), StringCodec, Int64Codec, WithSyncPolicy(SyncNever))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	if err := db.Update(func(tx *Tx[string, int64]) error {
		tx.TrackReads()
		tx.MarkRead("a")
		tx.MarkReadRange("a", "z")
		tx.MarkExclusive()
		return tx.Set("k", 1)
	}); err != nil {
		t.Fatalf("tracked update in default mode: %v", err)
	}
	if err := db.UpdateExclusive(func(tx *Tx[string, int64]) error {
		return tx.Set("k", 2)
	}); err != nil {
		t.Fatalf("UpdateExclusive in default mode: %v", err)
	}
	if v, ok := db.Get("k"); !ok || v != 2 {
		t.Fatalf("Get(k) = %d, %v; want 2, true", v, ok)
	}
}

// Read tracking must not manufacture conflicts: tracked transactions
// over disjoint keys and ranges all commit.
func TestSerialDisjointTrackedCommit(t *testing.T) {
	db, _ := openOCC(t)

	t1, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	t2, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	t1.TrackReads()
	t2.TrackReads()
	t1.MarkRead("a")
	t1.MarkReadRange("c", "f")
	if err := t1.Set("b", 1); err != nil {
		t.Fatal(err)
	}
	t2.MarkRead("x")
	t2.MarkReadRange("p", "s")
	if err := t2.Set("y", 2); err != nil {
		t.Fatal(err)
	}
	if err := t1.Commit(); err != nil {
		t.Fatalf("t1: %v", err)
	}
	if err := t2.Commit(); err != nil {
		t.Fatalf("t2 (disjoint reads and writes): %v", err)
	}
}
