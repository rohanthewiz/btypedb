package btypedb

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSetGetDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(path, StringCodec, StringCodec)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Set("alpha", "1"); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("beta", "2"); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("alpha", "one"); err != nil { // overwrite
		t.Fatal(err)
	}

	if v, ok := db.Get("alpha"); !ok || v != "one" {
		t.Fatalf("Get(alpha) = %q, %v; want %q, true", v, ok, "one")
	}
	if _, ok := db.Get("missing"); ok {
		t.Fatal("Get(missing) reported ok")
	}
	if db.Len() != 2 {
		t.Fatalf("Len = %d; want 2", db.Len())
	}

	existed, err := db.Delete("beta")
	if err != nil || !existed {
		t.Fatalf("Delete(beta) = %v, %v; want true, nil", existed, err)
	}
	existed, err = db.Delete("beta")
	if err != nil || existed {
		t.Fatalf("second Delete(beta) = %v, %v; want false, nil", existed, err)
	}
	if db.Len() != 1 {
		t.Fatalf("Len after delete = %d; want 1", db.Len())
	}
}

func TestReopenReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, StringCodec, Int64Codec)
	if err != nil {
		t.Fatal(err)
	}
	for i, k := range []string{"a", "b", "c", "d"} {
		if err := db.Set(k, int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Delete("b"); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("a", 100); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path, StringCodec, Int64Codec)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	want := map[string]int64{"a": 100, "c": 2, "d": 3}
	if db2.Len() != len(want) {
		t.Fatalf("Len after reopen = %d; want %d", db2.Len(), len(want))
	}
	for k, wv := range want {
		if v, ok := db2.Get(k); !ok || v != wv {
			t.Fatalf("Get(%q) = %d, %v; want %d, true", k, v, ok, wv)
		}
	}
	if db2.Contains("b") {
		t.Fatal("deleted key b survived replay")
	}
}

func TestTornTailRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, StringCodec, StringCodec)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"k1", "k2", "k3"} {
		if err := db.Set(k, "v-"+k); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	cleanSize := fi.Size()

	// Simulate a crash mid-append: valid log followed by a partial record.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{opSet, 0xFF, 0x03}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	db2, err := Open(path, StringCodec, StringCodec)
	if err != nil {
		t.Fatalf("open after torn write: %v", err)
	}
	if db2.Len() != 3 {
		t.Fatalf("Len after recovery = %d; want 3", db2.Len())
	}
	if v, ok := db2.Get("k2"); !ok || v != "v-k2" {
		t.Fatalf("Get(k2) = %q, %v after recovery", v, ok)
	}

	// The torn tail must be gone so new appends land on a valid boundary.
	if fi, err = os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if fi.Size() != cleanSize {
		t.Fatalf("file size after recovery = %d; want %d (torn tail truncated)", fi.Size(), cleanSize)
	}

	// And the DB must still be writable and durable after recovery.
	if err := db2.Set("k4", "v-k4"); err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	db3, err := Open(path, StringCodec, StringCodec)
	if err != nil {
		t.Fatal(err)
	}
	defer db3.Close()
	if db3.Len() != 4 {
		t.Fatalf("Len after post-recovery write = %d; want 4", db3.Len())
	}
}

func TestCorruptTailCRC(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path, StringCodec, StringCodec)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set("good", "data"); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("last", "record"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Flip a byte inside the last record's value to break its CRC.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-recCRCSize-1] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path, StringCodec, StringCodec)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if v, ok := db2.Get("good"); !ok || v != "data" {
		t.Fatalf("Get(good) = %q, %v; want intact record preserved", v, ok)
	}
	if db2.Contains("last") {
		t.Fatal("record with bad CRC survived replay")
	}
}

func TestOrderedIteration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(path, Int64Codec, StringCodec)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, n := range []int64{42, 7, 99, 1, 63} {
		if err := db.Set(n, "x"); err != nil {
			t.Fatal(err)
		}
	}

	var keys []int64
	for k := range db.All() {
		keys = append(keys, k)
	}
	want := []int64{1, 7, 42, 63, 99}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("All() order = %v; want %v", keys, want)
		}
	}

	var fromKeys []int64
	for k := range db.Ascend(42) {
		fromKeys = append(fromKeys, k)
	}
	if len(fromKeys) != 3 || fromKeys[0] != 42 || fromKeys[2] != 99 {
		t.Fatalf("Ascend(42) = %v; want [42 63 99]", fromKeys)
	}
}

func TestJSONValues(t *testing.T) {
	type user struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(path, StringCodec, JSONCodec[user]())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set("u1", user{Name: "Ada", Age: 36}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path, StringCodec, JSONCodec[user]())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if u, ok := db2.Get("u1"); !ok || u.Name != "Ada" || u.Age != 36 {
		t.Fatalf("Get(u1) = %+v, %v after reopen", u, ok)
	}
}

func TestClosedErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(path, StringCodec, StringCodec)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil { // idempotent
		t.Fatalf("second Close = %v; want nil", err)
	}
	if err := db.Set("k", "v"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Set after Close = %v; want ErrClosed", err)
	}
	if _, err := db.Delete("k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Delete after Close = %v; want ErrClosed", err)
	}
	if err := db.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Sync after Close = %v; want ErrClosed", err)
	}
}

func TestSyncEverySecondPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(path, StringCodec, StringCodec, WithSyncPolicy(SyncEverySecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set("k", "v"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil { // must stop the syncer cleanly and flush
		t.Fatal(err)
	}

	db2, err := Open(path, StringCodec, StringCodec)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if v, ok := db2.Get("k"); !ok || v != "v" {
		t.Fatalf("Get(k) = %q, %v after reopen", v, ok)
	}
}

func TestConcurrentAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(path, Int64Codec, Int64Codec, WithSyncPolicy(SyncNever))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	done := make(chan error, 4)
	for w := range 2 {
		go func() {
			for i := range int64(200) {
				if err := db.Set(int64(w)*1000+i, i); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
	}
	for range 2 {
		go func() {
			for i := range int64(200) {
				db.Get(i)
				db.Len()
			}
			done <- nil
		}()
	}
	for range 4 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if db.Len() != 400 {
		t.Fatalf("Len = %d; want 400", db.Len())
	}
}
