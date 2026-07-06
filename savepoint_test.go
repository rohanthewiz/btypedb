package btypedb

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestSavepointRollbackTo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path)
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("a", "1"); err != nil {
		t.Fatal(err)
	}
	sp, err := tx.Savepoint()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("a", "2"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("b", "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Delete("a"); err != nil {
		t.Fatal(err)
	}

	if err := tx.RollbackTo(sp); err != nil {
		t.Fatal(err)
	}
	if v, ok := tx.Get("a"); !ok || v != "1" {
		t.Fatalf("after RollbackTo, a = %q, %v; want 1", v, ok)
	}
	if _, ok := tx.Get("b"); ok {
		t.Fatal("b survived RollbackTo")
	}

	// The savepoint stays valid: write past it and roll back again.
	if err := tx.Set("c", "1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.RollbackTo(sp); err != nil {
		t.Fatal(err)
	}
	if _, ok := tx.Get("c"); ok {
		t.Fatal("c survived second RollbackTo")
	}

	if err := tx.Set("d", "1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if v, ok := db.Get("a"); !ok || v != "1" {
		t.Fatalf("a = %q, %v; want 1", v, ok)
	}
	if db.Contains("b") || db.Contains("c") {
		t.Fatal("rolled-back keys committed")
	}
	if !db.Contains("d") {
		t.Fatal("post-rollback write lost")
	}

	// The WAL batch was truncated at the mark: replay agrees.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2 := openStr(t, path)
	defer db2.Close()
	if v, ok := db2.Get("a"); !ok || v != "1" {
		t.Fatalf("replayed a = %q, %v; want 1", v, ok)
	}
	if db2.Contains("b") || db2.Contains("c") {
		t.Fatal("rolled-back keys replayed from the log")
	}
	if !db2.Contains("d") {
		t.Fatal("post-rollback write missing after replay")
	}
}

func TestSavepointNesting(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	sp1, err := tx.Savepoint()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("a", "1"); err != nil {
		t.Fatal(err)
	}
	sp2, err := tx.Savepoint()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("b", "1"); err != nil {
		t.Fatal(err)
	}

	// Rolling back to sp1 destroys sp2.
	if err := tx.RollbackTo(sp1); err != nil {
		t.Fatal(err)
	}
	if tx.Contains("a") || tx.Contains("b") {
		t.Fatal("writes survived RollbackTo(sp1)")
	}
	if err := tx.RollbackTo(sp2); !errors.Is(err, ErrSavepointInvalid) {
		t.Fatalf("RollbackTo(destroyed sp2) = %v; want ErrSavepointInvalid", err)
	}
}

func TestSavepointRelease(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	sp1, err := tx.Savepoint()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("a", "1"); err != nil {
		t.Fatal(err)
	}
	sp2, err := tx.Savepoint()
	if err != nil {
		t.Fatal(err)
	}

	// Release keeps the changes and destroys sp1 and the later sp2.
	if err := tx.Release(sp1); err != nil {
		t.Fatal(err)
	}
	if !tx.Contains("a") {
		t.Fatal("Release discarded changes")
	}
	if err := tx.RollbackTo(sp1); !errors.Is(err, ErrSavepointInvalid) {
		t.Fatalf("RollbackTo(released sp1) = %v; want ErrSavepointInvalid", err)
	}
	if err := tx.Release(sp2); !errors.Is(err, ErrSavepointInvalid) {
		t.Fatalf("Release(destroyed sp2) = %v; want ErrSavepointInvalid", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if !db.Contains("a") {
		t.Fatal("commit lost the released savepoint's changes")
	}
}

func TestSavepointForeignAndClosed(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	tx1, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	sp, err := tx1.Savepoint()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx1.Rollback(); err != nil {
		t.Fatal(err)
	}

	// The savepoint died with its transaction.
	tx2, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()
	if err := tx2.RollbackTo(sp); !errors.Is(err, ErrSavepointInvalid) {
		t.Fatalf("RollbackTo(foreign sp) = %v; want ErrSavepointInvalid", err)
	}
	if _, err := tx1.Savepoint(); !errors.Is(err, ErrTxClosed) {
		t.Fatalf("Savepoint on closed tx = %v; want ErrTxClosed", err)
	}
}

func TestSavepointIndexesAndTTL(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	if err := db.CreateIndex("byval", func(ak string, av string, bk string, bv string) int {
		switch {
		case av < bv:
			return -1
		case av > bv:
			return 1
		}
		return 0
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("a", "2"); err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	sp, err := tx.Savepoint()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set("b", "1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.SetTTL("c", "3", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := tx.RollbackTo(sp); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Index and TTL state rolled back with the data.
	var keys []string
	for k := range db.AscendIndex("byval") {
		keys = append(keys, k)
	}
	if len(keys) != 1 || keys[0] != "a" {
		t.Fatalf("index keys = %v; want [a]", keys)
	}
	if _, ok := db.TTL("c"); ok {
		t.Fatal("TTL entry survived rollback")
	}
}
