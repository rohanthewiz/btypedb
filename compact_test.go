package btypedb

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestCompactShrinksAndPreservesData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path, WithSyncPolicy(SyncNever), WithAutoCompactDisabled())

	// Generate lots of dead records: overwrites and deletes.
	for round := range 5 {
		for i := range 200 {
			key := fmt.Sprintf("k%03d", i)
			if err := db.Set(key, fmt.Sprintf("v%d-%d", round, i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := 0; i < 200; i += 2 {
		if _, err := db.Delete(fmt.Sprintf("k%03d", i)); err != nil {
			t.Fatal(err)
		}
	}

	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size()/2 {
		t.Fatalf("compaction barely shrank the log: %d -> %d bytes", before.Size(), after.Size())
	}

	// Live view unchanged, and DB still writable through the new file.
	if db.Len() != 100 {
		t.Fatalf("Len after compact = %d; want 100", db.Len())
	}
	if err := db.Set("post-compact", "v"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The compacted file must replay to the identical state.
	db2 := openStr(t, path)
	defer db2.Close()
	if db2.Len() != 101 {
		t.Fatalf("Len after reopen = %d; want 101", db2.Len())
	}
	if v, ok := db2.Get("k001"); !ok || v != "v4-1" {
		t.Fatalf("Get(k001) = %q, %v; want last overwrite v4-1", v, ok)
	}
	if db2.Contains("k000") {
		t.Fatal("deleted key resurrected by compaction")
	}
	if !db2.Contains("post-compact") {
		t.Fatal("write after compaction lost")
	}
}

func TestCompactDuringConcurrentWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(path, Int64Codec, Int64Codec, WithSyncPolicy(SyncNever), WithAutoCompactDisabled())
	if err != nil {
		t.Fatal(err)
	}

	const n = 2000
	var wg sync.WaitGroup
	writeErr := make(chan error, 1)
	wg.Go(func() {
		for i := range int64(n) {
			if err := db.Set(i, i*10); err != nil {
				writeErr <- err
				return
			}
		}
	})
	compactErr := make(chan error, 1)
	wg.Go(func() {
		for range 5 {
			if err := db.Compact(); err != nil {
				compactErr <- err
				return
			}
		}
	})
	wg.Wait()
	select {
	case err := <-writeErr:
		t.Fatal(err)
	case err := <-compactErr:
		t.Fatal(err)
	default:
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// No write may be lost, no matter where the compactions landed.
	db2, err := Open(path, Int64Codec, Int64Codec)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if db2.Len() != n {
		t.Fatalf("Len after reopen = %d; want %d", db2.Len(), n)
	}
	for i := range int64(n) {
		if v, ok := db2.Get(i); !ok || v != i*10 {
			t.Fatalf("Get(%d) = %d, %v; want %d", i, v, ok, i*10)
		}
	}
}

func TestAutoCompact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path, WithSyncPolicy(SyncNever), WithAutoCompact(2048, 20))

	// Hammer a tiny keyspace: live data stays ~10 records while the raw
	// log would be ~40 KB, so background compaction must kick in.
	for i := range 2000 {
		if err := db.Set(fmt.Sprintf("k%d", i%10), "value-payload"); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil { // waits for in-flight compaction
		t.Fatal(err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() > 16<<10 {
		t.Fatalf("auto-compaction never ran: log is %d bytes for 10 live keys", fi.Size())
	}

	db2 := openStr(t, path)
	defer db2.Close()
	if db2.Len() != 10 {
		t.Fatalf("Len after reopen = %d; want 10", db2.Len())
	}
}

func TestCompactLeftoverTempDiscarded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path)
	if err := db.Set("k", "v"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-compaction: a stale temp file next to the log.
	if err := os.WriteFile(path+compactSuffix, []byte("half-written garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	db2 := openStr(t, path)
	defer db2.Close()
	if v, ok := db2.Get("k"); !ok || v != "v" {
		t.Fatalf("Get(k) = %q, %v; want original data", v, ok)
	}
	if _, err := os.Stat(path + compactSuffix); !os.IsNotExist(err) {
		t.Fatal("stale compaction temp file not discarded on open")
	}
}

func TestCompactOnClosedDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != ErrClosed {
		t.Fatalf("Compact on closed DB = %v; want ErrClosed", err)
	}
}
