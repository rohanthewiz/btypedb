package btypedb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testKey is a fixed 32-byte master used across the encryption tests. A
// second, different key exercises the wrong-key path.
func testKey() []byte  { return bytes.Repeat([]byte{0xA5}, 32) }
func otherKey() []byte { return bytes.Repeat([]byte{0x5A}, 32) }

// openEnc opens an encrypted string/string database, failing the test on
// error. Auto-compaction and the sweeper are off so on-disk layout is
// predictable for the byte-level assertions.
func openEnc(t *testing.T, path string, key []byte, extra ...Option) *DB[string, string] {
	t.Helper()
	opts := append([]Option{WithEncryptionKey(key), WithAutoCompactDisabled(), WithSweepInterval(0)}, extra...)
	db, err := Open(path, StringCodec, StringCodec, opts...)
	if err != nil {
		t.Fatalf("open encrypted: %v", err)
	}
	return db
}

// TestEncryptRoundTrip is the core guarantee: values written under a key
// survive a close/reopen, the on-disk header is the v2 (encrypted) form, and
// the plaintext value bytes never appear on disk — while the cleartext key
// bytes (value-only scope) do.
func TestEncryptRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enc.db")

	db := openEnc(t, path, testKey())
	rows := map[string]string{
		"user:1": "SECRETpayload-one",
		"user:2": "SECRETpayload-two",
		"empty":  "", // empty value still seals to nonce+tag
	}
	for k, v := range rows {
		if err := db.Set(k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(raw, []byte(logMagic)) {
		t.Fatalf("no magic prefix")
	}
	if v := binary.LittleEndian.Uint32(raw[8:12]); v != encFormatVersion {
		t.Fatalf("on-disk version = %d, want encrypted %d", v, encFormatVersion)
	}
	if raw[16]&byte(hdrFlagEncrypted) == 0 {
		t.Fatalf("encrypted flag not set in header flags word")
	}
	// Value-only scope: plaintext value bytes must be absent, but the
	// cleartext keys are expected on disk.
	if bytes.Contains(raw, []byte("SECRETpayload")) {
		t.Fatalf("plaintext value found on disk — value not encrypted")
	}
	if !bytes.Contains(raw, []byte("user:1")) {
		t.Fatalf("cleartext key not found on disk (value-only scope expects it)")
	}

	db2 := openEnc(t, path, testKey())
	defer db2.Close()
	for k, want := range rows {
		if got, ok := db2.Get(k); !ok || got != want {
			t.Fatalf("row %q: got %q,%v want %q", k, got, ok, want)
		}
	}
}

// TestEncryptWrongKey: reopening with a different key is caught at Open by the
// header key-check value, before any record is read.
func TestEncryptWrongKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enc.db")
	db := openEnc(t, path, testKey())
	if err := db.Set("k", "v"); err != nil {
		t.Fatal(err)
	}
	db.Close()

	_, err := Open(path, StringCodec, StringCodec, WithEncryptionKey(otherKey()))
	if !errors.Is(err, ErrWrongKey) {
		t.Fatalf("got %v, want ErrWrongKey", err)
	}
}

// TestEncryptKeyRequired: an encrypted file cannot be opened without a key.
func TestEncryptKeyRequired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enc.db")
	db := openEnc(t, path, testKey())
	if err := db.Set("k", "v"); err != nil {
		t.Fatal(err)
	}
	db.Close()

	_, err := Open(path, StringCodec, StringCodec)
	if !errors.Is(err, ErrKeyRequired) {
		t.Fatalf("got %v, want ErrKeyRequired", err)
	}
}

// TestEncryptKeyOnPlaintext: passing a key to a plaintext database is refused
// rather than silently appending encrypted records to a cleartext log.
func TestEncryptKeyOnPlaintext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plain.db")
	db := openStr(t, path, WithAutoCompactDisabled(), WithSweepInterval(0))
	if err := db.Set("k", "v"); err != nil {
		t.Fatal(err)
	}
	db.Close()

	_, err := Open(path, StringCodec, StringCodec, WithEncryptionKey(testKey()))
	if !errors.Is(err, ErrNotEncrypted) {
		t.Fatalf("got %v, want ErrNotEncrypted", err)
	}
}

// TestEncryptBadKeyLength: a non-32-byte key fails Open cleanly.
func TestEncryptBadKeyLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enc.db")
	_, err := Open(path, StringCodec, StringCodec, WithEncryptionKey([]byte("too-short")))
	if err == nil {
		t.Fatalf("expected error for short key")
	}
}

// TestEncryptHeaderDeterministic: the header image is stable across calls for
// a fixed key. Torn-header detection and the byte-compare key check both rely
// on this.
func TestEncryptHeaderDeterministic(t *testing.T) {
	wc, err := newWalCipher(testKey())
	if err != nil {
		t.Fatal(err)
	}
	h1, h2 := wc.header(), wc.header()
	if !bytes.Equal(h1, h2) {
		t.Fatalf("header not deterministic:\n%x\n%x", h1, h2)
	}
	if int64(len(h1)) != logHeaderSizeV2 {
		t.Fatalf("header len = %d, want %d", len(h1), logHeaderSizeV2)
	}
	if crc32.ChecksumIEEE(h1[:12]) != binary.LittleEndian.Uint32(h1[12:16]) {
		t.Fatalf("compatibility-prefix checksum wrong")
	}
	// A different key must yield a different KCV region.
	wc2, _ := newWalCipher(otherKey())
	if bytes.Equal(wc2.header()[hdrKCVOffset:], h1[hdrKCVOffset:]) {
		t.Fatalf("KCV did not change with the key")
	}
}

// TestEncryptTTLAndBatch: TTL deadlines (sealed inside the ciphertext),
// deletes (never sealed), and multi-op transactions all round-trip.
func TestEncryptTTLAndBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enc.db")
	db := openEnc(t, path, testKey())

	if err := db.SetTTL("temp", "expires-later", time.Hour); err != nil {
		t.Fatal(err)
	}
	// Multi-op transaction → batch framing, each member sealed.
	err := db.Update(func(tx *Tx[string, string]) error {
		if err := tx.Set("a", "alpha"); err != nil {
			return err
		}
		if err := tx.Set("b", "bravo"); err != nil {
			return err
		}
		if _, err := tx.Delete("a"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	db2 := openEnc(t, path, testKey())
	defer db2.Close()
	if v, ok := db2.Get("temp"); !ok || v != "expires-later" {
		t.Fatalf("ttl row lost: %q,%v", v, ok)
	}
	if _, ok := db2.TTL("temp"); !ok {
		t.Fatalf("ttl deadline not restored")
	}
	if _, ok := db2.Get("a"); ok {
		t.Fatalf("deleted key survived the batch")
	}
	if v, ok := db2.Get("b"); !ok || v != "bravo" {
		t.Fatalf("batch row lost: %q,%v", v, ok)
	}
}

// TestEncryptTamperDetected: a value byte flipped on disk with the record CRC
// fixed up (so it passes framing) is caught by the AEAD tag at replay — a hard
// error, not a silently discarded tail.
func TestEncryptTamperDetected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enc.db")
	db := openEnc(t, path, testKey())
	if err := db.Set("k", "authenticated-payload"); err != nil {
		t.Fatal(err)
	}
	db.Close()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Parse the single record that begins right after the v2 header.
	off := int64(logHeaderSizeV2)
	klen := int64(binary.LittleEndian.Uint32(raw[off+1 : off+5]))
	vlen := int64(binary.LittleEndian.Uint32(raw[off+5 : off+9]))
	valStart := off + recHeaderSize + klen
	valEnd := valStart + vlen
	// Flip the last ciphertext/tag byte, then repair the record CRC so the
	// tamper is invisible to framing and must be caught cryptographically.
	raw[valEnd-1] ^= 0xff
	binary.LittleEndian.PutUint32(raw[valEnd:valEnd+recCRCSize], crc32.ChecksumIEEE(raw[off:valEnd]))
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err = Open(path, StringCodec, StringCodec, WithEncryptionKey(testKey()))
	if err == nil {
		t.Fatalf("tampered ciphertext opened without error")
	}
}

// TestEncryptTornTailRepaired: a crash mid-append (a truncated trailing
// record) is repaired exactly as for a plaintext log — the encryption rides
// on the same CRC framing, so recovery runs before any decryption.
func TestEncryptTornTailRepaired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enc.db")
	db := openEnc(t, path, testKey())
	for i := range 5 {
		if err := db.Set(key2(i), val2(i)); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Drop the final byte: the last record's CRC comes up short → torn tail.
	if err := os.WriteFile(path, raw[:len(raw)-1], 0o644); err != nil {
		t.Fatal(err)
	}

	db2 := openEnc(t, path, testKey())
	defer db2.Close()
	if got := db2.Len(); got != 4 {
		t.Fatalf("torn-tail repair: got %d rows, want 4", got)
	}
	// Still writable after repair.
	if err := db2.Set("after", "ok"); err != nil {
		t.Fatal(err)
	}
}

// TestEncryptCompaction: compaction re-seals live rows (fresh nonces, fresh
// v2 header) and the database still opens and reads correctly afterward.
func TestEncryptCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enc.db")
	db := openEnc(t, path, testKey())
	// Overwrite the same keys so compaction has dead versions to drop.
	for round := range 3 {
		for i := range 10 {
			if err := db.Set(key2(i), val2(i*round+i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	rawBefore := recordCiphertexts(t, path, db)
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	rawAfter, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(rawAfter[8:12]) != encFormatVersion {
		t.Fatalf("compacted header is not encrypted")
	}
	// Fresh nonces mean the compacted ciphertext for a live row differs from
	// its pre-compaction ciphertext.
	afterSet := recordCiphertextSet(t, rawAfter)
	overlap := 0
	for _, ct := range rawBefore {
		if afterSet[string(ct)] {
			overlap++
		}
	}
	if overlap != 0 {
		t.Fatalf("compaction reused %d nonces/ciphertexts; expected fresh", overlap)
	}

	db2 := openEnc(t, path, testKey())
	defer db2.Close()
	for i := range 10 {
		if _, ok := db2.Get(key2(i)); !ok {
			t.Fatalf("row %d lost after compaction", i)
		}
	}
}

// TestEncryptNoncesDistinct: every sealed record carries a unique nonce.
func TestEncryptNoncesDistinct(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enc.db")
	db := openEnc(t, path, testKey())
	const n = 200
	for i := range n {
		if err := db.Set(key2(i), "same-value-every-time"); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, rec := range walkRecords(t, raw) {
		if rec.op != opSet {
			continue
		}
		nonce := string(rec.val[:12]) // GCM nonce size
		if seen[nonce] {
			t.Fatalf("duplicate nonce observed")
		}
		seen[nonce] = true
	}
	if len(seen) != n {
		t.Fatalf("saw %d distinct nonces, want %d", len(seen), n)
	}
}

// walkRecords decodes every framed record after the header, reusing the
// production readRecord so the test sees exactly what replay does.
func walkRecords(t *testing.T, raw []byte) []walRecord {
	t.Helper()
	start := logHeaderSize
	if binary.LittleEndian.Uint32(raw[8:12]) == encFormatVersion {
		start = logHeaderSizeV2
	}
	br := bufio.NewReader(bytes.NewReader(raw[start:]))
	var recs []walRecord
	for {
		rec, _, ok, err := readRecord(br)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		recs = append(recs, rec)
	}
	return recs
}

// recordCiphertexts returns each live record's on-disk value bytes.
func recordCiphertexts(t *testing.T, path string, _ *DB[string, string]) [][]byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out [][]byte
	for _, rec := range walkRecords(t, raw) {
		if rec.op == opSet {
			out = append(out, append([]byte(nil), rec.val...))
		}
	}
	return out
}

func recordCiphertextSet(t *testing.T, raw []byte) map[string]bool {
	t.Helper()
	set := map[string]bool{}
	for _, rec := range walkRecords(t, raw) {
		if rec.op == opSet {
			set[string(rec.val)] = true
		}
	}
	return set
}

// FuzzOpenRecord asserts the decrypt path never panics on arbitrary framing:
// any (op, key, value) triple either opens or returns an error.
func FuzzOpenRecord(f *testing.F) {
	wc, err := newWalCipher(testKey())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(byte(opSet), []byte("k"), []byte("v"))
	f.Add(byte(opDelete), []byte("k"), []byte(nil))
	f.Add(byte(opSetTTL), []byte("k"), bytes.Repeat([]byte{0}, 40))
	f.Fuzz(func(t *testing.T, op byte, key, val []byte) {
		_, _, _ = openRecord(wc, op, key, val) // property: no panic
	})
}

// FuzzOpenEncryptedFile asserts Open never panics on arbitrary on-disk bytes
// presented as an encrypted database — it must always either open or error.
func FuzzOpenEncryptedFile(f *testing.F) {
	// Seed with a real encrypted image so the corpus explores near-valid files.
	seed := filepath.Join(f.TempDir(), "seed.db")
	if db, err := Open(seed, StringCodec, StringCodec,
		WithEncryptionKey(testKey()), WithAutoCompactDisabled(), WithSweepInterval(0)); err == nil {
		_ = db.Set("k", "v")
		db.Close()
		if raw, e := os.ReadFile(seed); e == nil {
			f.Add(raw)
		}
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "fuzz.db")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Skip()
		}
		db, err := Open(path, StringCodec, StringCodec,
			WithEncryptionKey(testKey()), WithAutoCompactDisabled(), WithSweepInterval(0))
		if err == nil {
			db.Close()
		}
	})
}
