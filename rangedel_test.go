package btypedb

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestDeleteRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path)

	for i := range 10 {
		if err := db.Set(fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}

	// [k3, k7) — half-open like btype's DeleteRange.
	n, err := db.DeleteRange("k3", "k7")
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("DeleteRange = %d; want 4", n)
	}
	if db.Len() != 6 {
		t.Fatalf("Len = %d; want 6", db.Len())
	}
	if db.Contains("k3") || db.Contains("k6") {
		t.Fatal("in-range key survived")
	}
	if !db.Contains("k2") || !db.Contains("k7") {
		t.Fatal("boundary key wrongly deleted")
	}

	// Empty range is a no-op, not an error.
	if n, err = db.DeleteRange("x", "z"); err != nil || n != 0 {
		t.Fatalf("empty DeleteRange = %d, %v", n, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The deletions replay as one atomic batch.
	db2 := openStr(t, path)
	defer db2.Close()
	if db2.Len() != 6 {
		t.Fatalf("Len after reopen = %d; want 6", db2.Len())
	}
	if db2.Contains("k5") {
		t.Fatal("range-deleted key resurrected by replay")
	}
}

func TestDeleteRangeInTx(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	for i := range 6 {
		if err := db.Set(fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}

	// Rollback restores the whole range.
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := tx.DeleteRange("k0", "k9"); err != nil || n != 6 {
		t.Fatalf("tx.DeleteRange = %d, %v; want 6", n, err)
	}
	if tx.Len() != 0 {
		t.Fatalf("tx.Len = %d; want 0", tx.Len())
	}
	tx.Rollback()
	if db.Len() != 6 {
		t.Fatalf("Len after rollback = %d; want 6", db.Len())
	}

	// Read-only transactions may not range-delete.
	rtx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Rollback()
	if _, err := rtx.DeleteRange("a", "z"); err != ErrTxNotWritable {
		t.Fatalf("read tx DeleteRange = %v; want ErrTxNotWritable", err)
	}
}

func TestDeleteRangeMaintainsDerivedTrees(t *testing.T) {
	db := openPeople(t, filepath.Join(t.TempDir(), "test.db"), WithSweepInterval(0))
	defer db.Close()

	if err := db.CreateIndex("by-age", byAge); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("a", person{Age: 3}); err != nil {
		t.Fatal(err)
	}
	if err := db.SetTTL("b", person{Age: 1}, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := db.SetTTL("c", person{Age: 2}, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // c expires but is unswept

	// Deleting [a, c] returns only the 2 visible keys, but scrubs all 3
	// from data, TTL bookkeeping, and the index.
	n, err := db.DeleteRange("a", "d")
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("DeleteRange = %d; want 2 visible", n)
	}
	if db.Len() != 0 {
		t.Fatalf("Len = %d; want 0 (expired key physically removed too)", db.Len())
	}
	if got := indexKeys(db.AscendIndex("by-age")); got != nil {
		t.Fatalf("index still has entries after DeleteRange: %v", got)
	}
	if _, ok := db.TTL("b"); ok {
		t.Fatal("TTL bookkeeping survived DeleteRange")
	}
}
