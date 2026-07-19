package btypedb

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// shipAll drains everything the log currently has past from, in small
// chunks, the way a real follower would.
func shipAll(t *testing.T, db *DB[string, string], epoch uint64, from int64, dst *bytes.Buffer) int64 {
	t.Helper()
	for {
		_, size, err := db.LogState()
		if err != nil {
			t.Fatal(err)
		}
		if from >= size {
			return from
		}
		n, err := db.ReadLogRange(epoch, from, 7, dst) // deliberately tiny chunks to cross record boundaries
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return from
		}
		from += n
	}
}

// TestReplicationShipAndRestore assembles a replica purely by
// concatenating shipped byte ranges and verifies it opens to the same
// dataset — the core contract the replicate module builds on.
func TestReplicationShipAndRestore(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "src.db"), StringCodec, StringCodec, WithAutoCompactDisabled())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	epoch, size, err := db.LogState()
	if err != nil {
		t.Fatal(err)
	}
	if epoch != 0 {
		t.Fatalf("fresh epoch = %d, want 0", epoch)
	}
	if size != logHeaderSize {
		t.Fatalf("fresh size = %d, want header size %d", size, logHeaderSize)
	}

	var replica bytes.Buffer
	watermark := shipAll(t, db, epoch, 0, &replica) // ships just the header

	if err := db.Set("alpha", "1"); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("beta", "2"); err != nil {
		t.Fatal(err)
	}
	watermark = shipAll(t, db, epoch, watermark, &replica)

	// Writes landing after a ship must ship incrementally, including a
	// delete and a multi-op transaction (batch framing).
	if _, err := db.Delete("alpha"); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx[string, string]) error {
		tx.Set("gamma", "3")
		tx.Set("delta", "4")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	shipAll(t, db, epoch, watermark, &replica)

	restorePath := filepath.Join(dir, "replica.db")
	if err := os.WriteFile(restorePath, replica.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	rdb, err := Open(restorePath, StringCodec, StringCodec)
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()

	want := map[string]string{"beta": "2", "gamma": "3", "delta": "4"}
	if rdb.Len() != len(want) {
		t.Fatalf("replica has %d keys, want %d", rdb.Len(), len(want))
	}
	for k, wantV := range want {
		if v, ok := rdb.Get(k); !ok || v != wantV {
			t.Fatalf("replica[%q] = %q,%v; want %q", k, v, ok, wantV)
		}
	}
	if _, ok := rdb.Get("alpha"); ok {
		t.Fatal("deleted key resurrected in replica")
	}
}

// TestReplicationTornTailReplica verifies the crash-window contract: a
// replica whose last shipped chunk ends mid-record still opens, holding
// everything before the tear — same repair as local crash recovery.
func TestReplicationTornTailReplica(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "src.db"), StringCodec, StringCodec, WithAutoCompactDisabled())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Set("kept", "yes"); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("torn", "value-that-gets-cut"); err != nil {
		t.Fatal(err)
	}

	epoch, size, err := db.LogState()
	if err != nil {
		t.Fatal(err)
	}
	var replica bytes.Buffer
	if _, err := db.ReadLogRange(epoch, 0, size, &replica); err != nil {
		t.Fatal(err)
	}

	restorePath := filepath.Join(dir, "torn.db")
	if err := os.WriteFile(restorePath, replica.Bytes()[:replica.Len()-5], 0o644); err != nil {
		t.Fatal(err)
	}
	rdb, err := Open(restorePath, StringCodec, StringCodec)
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()
	if v, ok := rdb.Get("kept"); !ok || v != "yes" {
		t.Fatalf("pre-tear key lost: %q,%v", v, ok)
	}
	if _, ok := rdb.Get("torn"); ok {
		t.Fatal("torn record should have been discarded on open")
	}
}

// TestReplicationEpochChange verifies that a compaction invalidates
// in-flight follower offsets via ErrEpochChanged, and that shipping the
// new epoch from zero yields a valid replica.
func TestReplicationEpochChange(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "src.db"), StringCodec, StringCodec, WithAutoCompactDisabled())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, k := range []string{"a", "b", "c"} {
		if err := db.Set(k, "v-"+k); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Set("a", "v2-a"); err != nil { // overwrite, so compaction actually shrinks
		t.Fatal(err)
	}

	epoch0, _, err := db.LogState()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	epoch1, size1, err := db.LogState()
	if err != nil {
		t.Fatal(err)
	}
	if epoch1 != epoch0+1 {
		t.Fatalf("epoch after compact = %d, want %d", epoch1, epoch0+1)
	}

	var stale bytes.Buffer
	if _, err := db.ReadLogRange(epoch0, 0, 10, &stale); !errors.Is(err, ErrEpochChanged) {
		t.Fatalf("stale-epoch read: err = %v, want ErrEpochChanged", err)
	}
	if stale.Len() != 0 {
		t.Fatal("stale-epoch read wrote bytes")
	}

	var replica bytes.Buffer
	if _, err := db.ReadLogRange(epoch1, 0, size1, &replica); err != nil {
		t.Fatal(err)
	}
	restorePath := filepath.Join(dir, "postcompact.db")
	if err := os.WriteFile(restorePath, replica.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	rdb, err := Open(restorePath, StringCodec, StringCodec)
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close()
	if v, ok := rdb.Get("a"); !ok || v != "v2-a" {
		t.Fatalf("replica[a] = %q,%v after compaction ship", v, ok)
	}
}

// TestReplicationRangeErrors pins down the misuse surface: reads past
// the end fail loudly, and a closed DB reports ErrClosed.
func TestReplicationRangeErrors(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "src.db"), StringCodec, StringCodec)
	if err != nil {
		t.Fatal(err)
	}

	epoch, size, err := db.LogState()
	if err != nil {
		t.Fatal(err)
	}
	var sink bytes.Buffer
	if _, err := db.ReadLogRange(epoch, size+1, 10, &sink); err == nil {
		t.Fatal("read past end succeeded")
	}
	if _, err := db.ReadLogRange(epoch, -1, 10, &sink); err == nil {
		t.Fatal("negative offset succeeded")
	}
	if n, err := db.ReadLogRange(epoch, size, 10, &sink); err != nil || n != 0 {
		t.Fatalf("read at exact end: n=%d err=%v, want 0,nil", n, err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.LogState(); !errors.Is(err, ErrClosed) {
		t.Fatalf("LogState after close: %v, want ErrClosed", err)
	}
	if _, err := db.ReadLogRange(epoch, 0, 10, &sink); !errors.Is(err, ErrClosed) {
		t.Fatalf("ReadLogRange after close: %v, want ErrClosed", err)
	}
}
