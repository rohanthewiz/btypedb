package btypedb

import (
	"path/filepath"
	"testing"
	"time"
)

// TestUpdatePanicReleasesWriterLock verifies that a panic inside an Update
// closure does not leak the single-writer lock.
//
// Begin(true) takes writerMu for the transaction's lifetime; if a panic
// unwound past Update without Commit/Rollback ever running, the lock would
// stay held and every subsequent write would block forever. The recover in
// Update must roll back (releasing the lock) and re-panic. The assertions
// here are: (1) the original panic value still propagates to the caller,
// (2) the next write completes promptly rather than hanging, and (3) the
// panicked transaction's write was rolled back, not published.
func TestUpdatePanicReleasesWriterLock(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	if err := db.Set("keep", "before"); err != nil {
		t.Fatal(err)
	}

	// Run the panicking Update and confirm the panic value re-surfaces.
	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic to propagate out of Update")
			}
			if s, ok := r.(string); !ok || s != "boom" {
				t.Fatalf("recovered %v (%T); want \"boom\"", r, r)
			}
		}()
		_ = db.Update(func(tx *Tx[string, string]) error {
			// This write must be discarded once the closure panics.
			if err := tx.Set("keep", "mutated"); err != nil {
				t.Fatal(err)
			}
			panic("boom")
		})
	}()

	// The writer lock must be free: the next write should finish quickly.
	// Guard with a timeout so a regression manifests as a clean failure
	// rather than a hung test binary.
	done := make(chan error, 1)
	go func() {
		done <- db.Update(func(tx *Tx[string, string]) error {
			return tx.Set("after", "ok")
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("post-panic Update failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("post-panic Update hung: writer lock was leaked by the panic")
	}

	// The panicked transaction's mutation must have been rolled back.
	if v, ok := db.Get("keep"); !ok || v != "before" {
		t.Fatalf("Get(keep) = %q, %v; panicked write should have rolled back", v, ok)
	}
	if v, ok := db.Get("after"); !ok || v != "ok" {
		t.Fatalf("Get(after) = %q, %v; post-panic write missing", v, ok)
	}
}
