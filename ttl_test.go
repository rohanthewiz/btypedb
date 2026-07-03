package btypedb

import (
	"path/filepath"
	"testing"
	"time"
)

func TestTTLExpiryHidesKey(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"), WithSweepInterval(0))
	defer db.Close()

	if err := db.SetTTL("ephemeral", "v", 40*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("durable", "v"); err != nil {
		t.Fatal(err)
	}

	if v, ok := db.Get("ephemeral"); !ok || v != "v" {
		t.Fatalf("Get before expiry = %q, %v", v, ok)
	}
	if d, ok := db.TTL("ephemeral"); !ok || d <= 0 || d > 40*time.Millisecond {
		t.Fatalf("TTL = %v, %v; want (0, 40ms]", d, ok)
	}
	if _, ok := db.TTL("durable"); ok {
		t.Fatal("TTL reported for key without deadline")
	}

	time.Sleep(60 * time.Millisecond)

	if _, ok := db.Get("ephemeral"); ok {
		t.Fatal("expired key visible via Get")
	}
	if db.Contains("ephemeral") {
		t.Fatal("expired key visible via Contains")
	}
	if _, ok := db.TTL("ephemeral"); ok {
		t.Fatal("TTL reported for expired key")
	}
	for k := range db.All() {
		if k == "ephemeral" {
			t.Fatal("expired key visible via All")
		}
	}
	if !db.Contains("durable") {
		t.Fatal("unrelated key affected by expiry")
	}
}

func TestTTLPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path)
	if err := db.SetTTL("longlived", "v", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := db.SetTTL("shortlived", "v", 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	db2 := openStr(t, path)
	defer db2.Close()
	if d, ok := db2.TTL("longlived"); !ok || d < 55*time.Minute {
		t.Fatalf("TTL after reopen = %v, %v; want ~1h", d, ok)
	}
	if _, ok := db2.Get("shortlived"); ok {
		t.Fatal("key that expired while closed is visible after reopen")
	}
}

func TestSetClearsTTL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path)
	if err := db.SetTTL("k", "temp", 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("k", "permanent"); err != nil { // plain Set removes the deadline
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	if v, ok := db.Get("k"); !ok || v != "permanent" {
		t.Fatalf("Get = %q, %v; want TTL cleared by Set", v, ok)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// And the clearing must survive replay.
	db2 := openStr(t, path)
	defer db2.Close()
	if v, ok := db2.Get("k"); !ok || v != "permanent" {
		t.Fatalf("Get after reopen = %q, %v", v, ok)
	}
}

func TestSweeperPhysicallyRemoves(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"),
		WithSweepInterval(20*time.Millisecond), WithSyncPolicy(SyncNever))
	defer db.Close()

	for _, k := range []string{"a", "b", "c"} {
		if err := db.SetTTL(k, "v", 10*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Set("keeper", "v"); err != nil {
		t.Fatal(err)
	}

	// Len counts physical entries, so it hitting 1 proves the sweeper
	// actually deleted rather than just hiding.
	deadline := time.Now().Add(2 * time.Second)
	for db.Len() > 1 {
		if time.Now().After(deadline) {
			t.Fatalf("sweeper never removed expired keys; Len = %d", db.Len())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !db.Contains("keeper") {
		t.Fatal("sweeper removed an unexpired key")
	}
}

func TestTxTTL(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()

	// Rollback discards TTL writes entirely.
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.SetTTL("gone", "v", time.Hour); err != nil {
		t.Fatal(err)
	}
	if d, ok := tx.TTL("gone"); !ok || d <= 0 {
		t.Fatalf("tx.TTL = %v, %v; want own write visible", d, ok)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if db.Contains("gone") {
		t.Fatal("rolled-back SetTTL leaked")
	}

	// Commit publishes value and deadline atomically.
	if err := db.Update(func(tx *Tx[string, string]) error {
		return tx.SetTTL("kept", "v", time.Hour)
	}); err != nil {
		t.Fatal(err)
	}
	if d, ok := db.TTL("kept"); !ok || d < 55*time.Minute {
		t.Fatalf("TTL after commit = %v, %v", d, ok)
	}

	// A read snapshot's expiry view is fixed by wall clock, not by
	// later writes: replacing the TTL in the live DB must not change
	// what the old snapshot reports.
	rtx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Rollback()
	if err := db.Set("kept", "no-ttl-now"); err != nil {
		t.Fatal(err)
	}
	if d, ok := rtx.TTL("kept"); !ok || d <= 0 {
		t.Fatalf("snapshot TTL = %v, %v; want frozen deadline", d, ok)
	}
	if _, ok := db.TTL("kept"); ok {
		t.Fatal("live TTL should be cleared by Set")
	}
}

func TestCompactDropsExpiredPreservesTTL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db := openStr(t, path, WithSweepInterval(0), WithAutoCompactDisabled())

	if err := db.SetTTL("expired", "v", 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := db.SetTTL("alive", "v", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("plain", "v"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := openStr(t, path)
	defer db2.Close()
	if db2.Len() != 2 {
		t.Fatalf("Len after compact+reopen = %d; want 2 (expired key dropped from file)", db2.Len())
	}
	if d, ok := db2.TTL("alive"); !ok || d < 55*time.Minute {
		t.Fatalf("TTL lost through compaction: %v, %v", d, ok)
	}
	if _, ok := db2.Get("plain"); !ok {
		t.Fatal("plain key lost through compaction")
	}
}

func TestSetTTLRejectsNonPositive(t *testing.T) {
	db := openStr(t, filepath.Join(t.TempDir(), "test.db"))
	defer db.Close()
	if err := db.SetTTL("k", "v", 0); err == nil {
		t.Fatal("SetTTL(0) succeeded; want error")
	}
	if err := db.SetTTL("k", "v", -time.Second); err == nil {
		t.Fatal("SetTTL(negative) succeeded; want error")
	}
}
