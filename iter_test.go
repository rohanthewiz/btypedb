package btypedb

import (
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestKeysValues(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"), WithSweepInterval(0))
	defer db.Close()

	for _, kv := range [][2]string{{"c", "3"}, {"a", "1"}, {"b", "2"}} {
		if err := db.Set(kv[0], kv[1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.SetTTL("x", "9", 15*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond) // x expires, unswept

	var keys []string
	for k := range db.Keys() {
		keys = append(keys, k)
	}
	if !slices.Equal(keys, []string{"a", "b", "c"}) {
		t.Fatalf("Keys() = %v; want [a b c] sorted, expired skipped", keys)
	}
	var vals []string
	for v := range db.Values() {
		vals = append(vals, v)
	}
	if !slices.Equal(vals, []string{"1", "2", "3"}) {
		t.Fatalf("Values() = %v; want [1 2 3] in key order", vals)
	}

	// Tx variants see own writes, lock-free.
	err := db.Update(func(tx *Tx[string, string]) error {
		if err := tx.Set("aa", "1.5"); err != nil {
			return err
		}
		var txKeys []string
		for k := range tx.Keys() {
			txKeys = append(txKeys, k)
		}
		if !slices.Equal(txKeys, []string{"a", "aa", "b", "c"}) {
			t.Errorf("tx.Keys() = %v; want own write included", txKeys)
		}
		var txVals []string
		for v := range tx.Values() {
			txVals = append(txVals, v)
		}
		if !slices.Equal(txVals, []string{"1", "1.5", "2", "3"}) {
			t.Errorf("tx.Values() = %v", txVals)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Early break must release the lock cleanly.
	for range db.Keys() {
		break
	}
	if err := db.Set("after-break", "v"); err != nil {
		t.Fatal(err)
	}
}
