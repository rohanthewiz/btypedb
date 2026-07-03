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

func TestBackwardDescend(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"), WithSweepInterval(0))
	defer db.Close()

	for _, k := range []string{"b", "d", "a", "c", "e"} {
		if err := db.Set(k, "v:"+k); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.SetTTL("cc", "9", 15*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond) // cc expires, unswept

	var keys []string
	for k, v := range db.Backward() {
		if v != "v:"+k {
			t.Fatalf("Backward: key %q carries value %q", k, v)
		}
		keys = append(keys, k)
	}
	if !slices.Equal(keys, []string{"e", "d", "c", "b", "a"}) {
		t.Fatalf("Backward() keys = %v; want [e d c b a], expired skipped", keys)
	}

	keys = keys[:0]
	for k := range db.Descend("c") {
		keys = append(keys, k)
	}
	if !slices.Equal(keys, []string{"c", "b", "a"}) {
		t.Fatalf("Descend(c) keys = %v; want [c b a]", keys)
	}
	keys = keys[:0]
	for k := range db.Descend("bz") { // between keys: starts at last key <= pivot
		keys = append(keys, k)
	}
	if !slices.Equal(keys, []string{"b", "a"}) {
		t.Fatalf("Descend(bz) keys = %v; want [b a]", keys)
	}

	// Tx variants: lock-free over the snapshot, own writes included.
	err := db.Update(func(tx *Tx[string, string]) error {
		if err := tx.Set("cz", "v:cz"); err != nil {
			return err
		}
		var txKeys []string
		for k := range tx.Backward() {
			txKeys = append(txKeys, k)
		}
		if !slices.Equal(txKeys, []string{"e", "d", "cz", "c", "b", "a"}) {
			t.Errorf("tx.Backward() = %v; want own write included", txKeys)
		}
		txKeys = txKeys[:0]
		for k := range tx.Descend("cz") {
			txKeys = append(txKeys, k)
		}
		if !slices.Equal(txKeys, []string{"cz", "c", "b", "a"}) {
			t.Errorf("tx.Descend(cz) = %v", txKeys)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Early break must release the lock cleanly.
	for range db.Backward() {
		break
	}
	if err := db.Set("after-break", "v:after-break"); err != nil {
		t.Fatal(err)
	}
}

func TestLiveLen(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"), WithSweepInterval(0))
	defer db.Close()

	for _, k := range []string{"a", "b", "c"} {
		if err := db.Set(k, "v"); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.SetTTL("x", "9", 15*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := db.SetTTL("y", "9", time.Hour); err != nil {
		t.Fatal(err)
	}
	if got := db.LiveLen(); got != 5 {
		t.Fatalf("LiveLen() = %d before expiry; want 5", got)
	}
	time.Sleep(30 * time.Millisecond) // x expires; no sweeper to remove it

	if got := db.Len(); got != 5 {
		t.Fatalf("Len() = %d; want 5 (expired key still counted)", got)
	}
	if got := db.LiveLen(); got != 4 {
		t.Fatalf("LiveLen() = %d; want 4 (expired key excluded)", got)
	}

	err := db.View(func(tx *Tx[string, string]) error {
		if got := tx.LiveLen(); got != 4 {
			t.Errorf("tx.LiveLen() = %d; want 4", got)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
