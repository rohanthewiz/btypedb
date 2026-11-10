package btypedb

// bytdb deep-look D-48: replay reads records into one reused buffer when
// both codecs copy what they decode, checks CRCs without a heap digest,
// and appends in-order keys with PushBack. The state must come out the
// same whichever path each record takes.

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"
)

func TestReplayCompactedAndTailMatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r.db")
	db, err := Open(path, StringCodec, BytesCodec, WithAutoCompactDisabled(), WithSyncPolicy(SyncNever))
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(48, 48))
	want := map[string][]byte{}
	put := func(k string, v []byte) {
		if err := db.Set(k, v); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	for i := range 5000 {
		k := fmt.Sprintf("k%06d", rng.IntN(8000))
		put(k, []byte(fmt.Sprintf("v%d-%s", i, bytes.Repeat([]byte{'x'}, rng.IntN(40)))))
	}
	for i := range 500 {
		k := fmt.Sprintf("k%06d", rng.IntN(8000))
		if _, err := db.Delete(k); err != nil {
			t.Fatal(err)
		}
		delete(want, k)
		_ = i
	}
	// A multi-record transaction, so the tail holds a batch too.
	if err := db.Update(func(tx *Tx[string, []byte]) error {
		for i := range 20 {
			k := fmt.Sprintf("b%03d", i)
			if err := tx.Set(k, []byte{byte(i)}); err != nil {
				return err
			}
			want[k] = []byte{byte(i)}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	check := func(stage string, db *DB[string, []byte]) {
		t.Helper()
		if db.Len() != len(want) {
			t.Fatalf("%s: %d keys, want %d", stage, db.Len(), len(want))
		}
		var keys []string
		for k, v := range db.Ascend("") {
			if !bytes.Equal(v, want[k]) {
				t.Fatalf("%s: %q = %q, want %q", stage, k, v, want[k])
			}
			keys = append(keys, k)
		}
		if !slices.IsSorted(keys) || len(keys) != len(want) {
			t.Fatalf("%s: ascend yielded %d keys, sorted=%v", stage, len(keys), slices.IsSorted(keys))
		}
	}
	check("before reopen", db)
	db.Close()

	// Uncompacted log: overwrites, deletes and a batch take the set path.
	if db, err = Open(path, StringCodec, BytesCodec, WithAutoCompactDisabled(), WithSyncPolicy(SyncNever)); err != nil {
		t.Fatal(err)
	}
	check("uncompacted replay", db)
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	// Writes after compaction: keys below and above the compacted range,
	// and an overwrite, so the tail mixes in-order and out-of-order keys.
	put("a-first", []byte("low"))
	put("z-last", []byte("high"))
	put("k000100", []byte("over"))
	db.Close()

	if db, err = Open(path, StringCodec, BytesCodec, WithAutoCompactDisabled(), WithSyncPolicy(SyncNever)); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	check("compacted replay", db)
	// The replayed tree still takes ordinary writes everywhere.
	put("k000050x", []byte("mid"))
	put("zz", []byte("end"))
	check("after writes", db)
}

// A codec whose Decode returns its input must not see the replay buffer
// reused under it: reuse is limited to the built-in codecs.
func TestReplayAliasingCodec(t *testing.T) {
	aliasing := Codec[[]byte]{
		Encode: func(b []byte) ([]byte, error) { return b, nil },
		Decode: func(b []byte) ([]byte, error) { return b, nil }, // keeps the input
	}
	path := filepath.Join(t.TempDir(), "a.db")
	db, err := Open(path, StringCodec, aliasing)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 200 {
		if err := db.Set(fmt.Sprintf("k%03d", i), []byte(fmt.Sprintf("value-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	if db, err = Open(path, StringCodec, aliasing); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := range 200 {
		if v, ok := db.Get(fmt.Sprintf("k%03d", i)); !ok || string(v) != fmt.Sprintf("value-%03d", i) {
			t.Fatalf("k%03d = %q, %v", i, v, ok)
		}
	}
}
