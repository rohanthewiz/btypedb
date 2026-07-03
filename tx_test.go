package btypedb

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func openStr(t *testing.T, path string, opts ...Option) *DB[string, string] {
	t.Helper()
	db, err := Open(path, StringCodec, StringCodec, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestTxCommitVisibility(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("a", "1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("b", "2"); err != nil {
		t.Fatal(err)
	}

	// Own writes visible inside the tx, not outside.
	if v, ok := tx.Get("a"); !ok || v != "1" {
		t.Fatalf("tx.Get(a) = %q, %v; want own write", v, ok)
	}
	if _, ok := db.Get("a"); ok {
		t.Fatal("uncommitted write visible via db.Get")
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if v, ok := db.Get("a"); !ok || v != "1" {
		t.Fatalf("Get(a) after commit = %q, %v", v, ok)
	}
	if db.Len() != 2 {
		t.Fatalf("Len after commit = %d; want 2", db.Len())
	}
}

func TestTxRollback(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	if err := db.Set("keep", "v"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("discard", "v"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Delete("keep"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if db.Contains("discard") {
		t.Fatal("rolled-back write survived")
	}
	if !db.Contains("keep") {
		t.Fatal("rolled-back delete removed key")
	}
	// Writer lock must be free again.
	if err := db.Set("after", "v"); err != nil {
		t.Fatal(err)
	}
}

func TestTxSnapshotIsolation(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	if err := db.Set("k", "old"); err != nil {
		t.Fatal(err)
	}

	rtx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Rollback()

	if err := db.Set("k", "new"); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("k2", "v2"); err != nil {
		t.Fatal(err)
	}

	// The read tx still sees the world as of Begin.
	if v, ok := rtx.Get("k"); !ok || v != "old" {
		t.Fatalf("read tx sees %q, %v; want frozen %q", v, ok, "old")
	}
	if rtx.Contains("k2") {
		t.Fatal("read tx sees later write")
	}
	if rtx.Len() != 1 {
		t.Fatalf("read tx Len = %d; want 1", rtx.Len())
	}
	// While the live DB sees the new state.
	if v, _ := db.Get("k"); v != "new" {
		t.Fatalf("db sees %q; want new", v)
	}
}

func TestTxReadOnlyRejectsWrites(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	rtx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Rollback()
	if err := rtx.Set("k", "v"); !errors.Is(err, ErrTxNotWritable) {
		t.Fatalf("read tx Set = %v; want ErrTxNotWritable", err)
	}
	if _, err := rtx.Delete("k"); !errors.Is(err, ErrTxNotWritable) {
		t.Fatalf("read tx Delete = %v; want ErrTxNotWritable", err)
	}
}

func TestTxClosedOps(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("k", "v"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := tx.Set("k2", "v"); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("Set on committed tx = %v; want ErrTxClosed", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("double Commit = %v; want ErrTxClosed", err)
	}
	if err := tx.Rollback(); err != nil { // rollback after commit is a no-op
		t.Fatalf("Rollback after Commit = %v; want nil", err)
	}
	if _, ok := tx.Get("k"); ok {
		t.Fatal("Get on closed tx reported ok")
	}
}

func TestUpdateViewHelpers(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	if err := db.Update(func(tx *Tx[string, string]) error {
		return tx.Set("a", "1")
	}); err != nil {
		t.Fatal(err)
	}

	boom := errors.New("boom")
	if err := db.Update(func(tx *Tx[string, string]) error {
		if err := tx.Set("b", "2"); err != nil {
			return err
		}
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("Update error = %v; want boom", err)
	}
	if db.Contains("b") {
		t.Fatal("failed Update leaked its write")
	}

	var got string
	if err := db.View(func(tx *Tx[string, string]) error {
		got, _ = tx.Get("a")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got != "1" {
		t.Fatalf("View read %q; want 1", got)
	}
}

func TestTxBatchReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path)

	if err := db.Update(func(tx *Tx[string, string]) error {
		for _, k := range []string{"a", "b", "c"} {
			if err := tx.Set(k, "v-"+k); err != nil {
				return err
			}
		}
		_, err := tx.Delete("b")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := openStr(t, path)
	defer db2.Close()
	if db2.Len() != 2 {
		t.Fatalf("Len after batch replay = %d; want 2", db2.Len())
	}
	if v, ok := db2.Get("a"); !ok || v != "v-a" {
		t.Fatalf("Get(a) = %q, %v after replay", v, ok)
	}
	if db2.Contains("b") {
		t.Fatal("in-tx deleted key survived replay")
	}
}

func TestTornBatchAtomicRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path)
	if err := db.Set("committed", "v"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	cleanSize := fi.Size()

	// Hand-craft a torn transaction: a batch header promising 3 records,
	// followed by only 1 — as if the process died mid-commit.
	var cnt [8]byte
	binary.LittleEndian.PutUint64(cnt[:], 3)
	torn := appendRecord(nil, opBatch, nil, cnt[:])
	torn = appendRecord(torn, opSet, []byte("phantom"), []byte("v"))

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(torn); err != nil {
		t.Fatal(err)
	}
	f.Close()

	db2 := openStr(t, path)
	defer db2.Close()
	if db2.Contains("phantom") {
		t.Fatal("partial batch applied: transaction atomicity violated")
	}
	if !db2.Contains("committed") {
		t.Fatal("committed data lost during batch recovery")
	}
	if fi, err = os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if fi.Size() != cleanSize {
		t.Fatalf("file size = %d; want %d (torn batch truncated)", fi.Size(), cleanSize)
	}
}

func TestTxEmptyCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path)
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 0 {
		t.Fatalf("empty commit wrote %d bytes to the log", fi.Size())
	}
	// Writer lock released.
	if err := db.Set("k", "v"); err != nil {
		t.Fatal(err)
	}
}

func TestTxIteration(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "test.db"), Int64Codec, StringCodec)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, n := range []int64{5, 1, 9} {
		if err := db.Set(n, "x"); err != nil {
			t.Fatal(err)
		}
	}

	err = db.Update(func(tx *Tx[int64, string]) error {
		if err := tx.Set(3, "x"); err != nil {
			return err
		}
		var keys []int64
		for k := range tx.All() {
			keys = append(keys, k)
		}
		want := []int64{1, 3, 5, 9} // sees own uncommitted write, sorted
		if len(keys) != len(want) {
			return errors.New("wrong key count in tx.All")
		}
		for i := range want {
			if keys[i] != want[i] {
				t.Errorf("tx.All order = %v; want %v", keys, want)
			}
		}
		var from []int64
		for k := range tx.Ascend(3) {
			from = append(from, k)
		}
		if len(from) != 3 || from[0] != 3 {
			t.Errorf("tx.Ascend(3) = %v; want [3 5 9]", from)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestConcurrentSnapshotsUnderWrites is the race-detector validation of
// the COW concurrency model: many read transactions iterate frozen
// snapshots while writable transactions and direct writes churn the tree.
func TestConcurrentSnapshotsUnderWrites(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "test.db"), Int64Codec, Int64Codec,
		WithSyncPolicy(SyncNever), WithSweepInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.CreateIndex("by-val-desc", func(ak, av, bk, bv int64) int {
		return int(bv - av) // descending by value
	}); err != nil {
		t.Fatal(err)
	}
	for i := range int64(500) {
		if err := db.Set(i, i); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 16)

	// Writers: transactions and direct sets.
	for w := range 2 {
		wg.Go(func() {
			for round := range int64(20) {
				err := db.Update(func(tx *Tx[int64, int64]) error {
					base := int64(w+1) * 10000
					for i := range int64(50) {
						if err := tx.Set(base+round*50+i, i); err != nil {
							return err
						}
					}
					// Race the TTL trees and sweeper too.
					if err := tx.SetTTL(base+round, round, time.Millisecond); err != nil {
						return err
					}
					_, err := tx.Delete(base + round*50)
					return err
				})
				if err != nil {
					errCh <- err
					return
				}
			}
		})
	}
	wg.Go(func() {
		for i := range int64(500) {
			if err := db.Set(i, -i); err != nil {
				errCh <- err
				return
			}
		}
	})

	// Readers: snapshot views must always be internally consistent.
	for range 4 {
		wg.Go(func() {
			for range 30 {
				err := db.View(func(tx *Tx[int64, int64]) error {
					n := 0
					for range tx.All() {
						n++
					}
					// n may trail Len by expired-unswept keys, never exceed it.
					if n > tx.Len() {
						return errors.New("snapshot iterated more pairs than snapshot Len")
					}
					// Iterated later, the index can only have seen more keys
					// expire — never extra entries (which would mean the
					// index holds duplicates or leaked pairs).
					idx := 0
					for range tx.AscendIndex("by-val-desc") {
						idx++
					}
					if idx > n {
						return errors.New("index iterated more pairs than primary tree")
					}
					return nil
				})
				if err != nil {
					errCh <- err
					return
				}
			}
		})
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}
