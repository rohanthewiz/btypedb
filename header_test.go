package btypedb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// writeStrDB creates a database at path holding n sequential rows and
// closes it, returning the raw on-disk image.
func writeStrDB(t *testing.T, path string, n int) []byte {
	t.Helper()
	db := openStr(t, path, WithAutoCompactDisabled(), WithSweepInterval(0))
	for i := range n {
		if err := db.Set(key2(i), val2(i)); err != nil {
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
	return raw
}

func key2(i int) string { return string(rune('a'+i/10)) + string(rune('0'+i%10)) }
func val2(i int) string { return "value-" + itoa(i) }

// checkRows asserts the database holds exactly rows [0, n).
func checkRows(t *testing.T, db *DB[string, string], n int) {
	t.Helper()
	if got := db.Len(); got != n {
		t.Fatalf("got %d rows, want %d", got, n)
	}
	for i := range n {
		if v, ok := db.Get(key2(i)); !ok || v != val2(i) {
			t.Fatalf("row %d: got %q,%v", i, v, ok)
		}
	}
}

func TestHeaderWrittenAndReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	raw := writeStrDB(t, path, 5)

	if !bytes.HasPrefix(raw, []byte(logMagic)) {
		t.Fatalf("new file does not start with the log magic: %q", raw[:min(len(raw), 16)])
	}
	db := openStr(t, path)
	defer db.Close()
	checkRows(t, db, 5)
}

func TestLegacyHeaderlessFileOpens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	raw := writeStrDB(t, path, 5)

	// Strip the header to reconstruct a pre-header (format 0) image.
	legacy := filepath.Join(dir, "legacy.db")
	if err := os.WriteFile(legacy, raw[logHeaderSize:], 0o644); err != nil {
		t.Fatal(err)
	}
	db := openStr(t, legacy, WithAutoCompactDisabled(), WithSweepInterval(0))
	checkRows(t, db, 5)

	// A legacy file keeps working in place: appends land after its data.
	if err := db.Set("zz", "new"); err != nil {
		t.Fatal(err)
	}

	// Compaction rewrites it into the current format.
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	upgraded, err := os.ReadFile(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(upgraded, []byte(logMagic)) {
		t.Fatal("compaction did not upgrade the legacy file to the current header format")
	}
	db2 := openStr(t, legacy)
	defer db2.Close()
	if db2.Len() != 6 {
		t.Fatalf("got %d rows after upgrade, want 6", db2.Len())
	}
	for i := range 5 {
		if v, ok := db2.Get(key2(i)); !ok || v != val2(i) {
			t.Fatalf("row %d lost in upgrade: %q,%v", i, v, ok)
		}
	}
	if v, ok := db2.Get("zz"); !ok || v != "new" {
		t.Fatalf("post-upgrade row lost: %q,%v", v, ok)
	}
}

func TestNewerFormatVersionRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	writeStrDB(t, path, 3)

	// Bump the version field and fix up the header checksum so only the
	// version is "wrong".
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint32(raw[8:12], logFormatVersion+1)
	binary.LittleEndian.PutUint32(raw[12:16], crc32.ChecksumIEEE(raw[:12]))
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err = Open(path, StringCodec, StringCodec)
	if !errors.Is(err, ErrNewerFormat) {
		t.Fatalf("got %v, want ErrNewerFormat", err)
	}
}

func TestTornHeaderStartsOver(t *testing.T) {
	// A crash during first creation can leave any prefix of the header,
	// optionally followed by sector junk. No record was ever accepted, so
	// every such file must open as an empty database.
	for _, junk := range [][]byte{nil, {0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}} {
		for n := 0; n < logHeaderSize; n++ {
			path := filepath.Join(t.TempDir(), "torn.db")
			img := append(append([]byte{}, logHeader()[:n]...), junk...)
			if err := os.WriteFile(path, img, 0o644); err != nil {
				t.Fatal(err)
			}
			db := openStr(t, path, WithSweepInterval(0))
			if db.Len() != 0 {
				t.Fatalf("prefix %d junk=%v: torn-header file opened with %d rows", n, junk != nil, db.Len())
			}
			// And it must be fully usable afterward.
			if err := db.Set("k", "v"); err != nil {
				t.Fatal(err)
			}
			db.Close()
			db2 := openStr(t, path)
			if v, ok := db2.Get("k"); !ok || v != "v" {
				t.Fatalf("prefix %d: reopen lost the row", n)
			}
			db2.Close()
		}
	}
}

func TestMidFileCorruptionRefused(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	raw := writeStrDB(t, path, 20)

	// Flip one byte in a record roughly a third of the way through the
	// data region — bitrot — leaving intact records after it.
	pos := logHeaderSize + (len(raw)-logHeaderSize)/3
	raw[pos] ^= 0xff
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := Open(path, StringCodec, StringCodec)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("mid-file corruption: got %v, want ErrCorrupt", err)
	}

	// The salvage escape hatch recovers the prefix before the damage.
	db, err := Open(path, StringCodec, StringCodec, WithTruncateAtCorruption(), WithSweepInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.Len() == 0 || db.Len() >= 20 {
		t.Fatalf("salvage open recovered %d rows, want a proper prefix of 20", db.Len())
	}
	for i := range db.Len() {
		if v, ok := db.Get(key2(i)); !ok || v != val2(i) {
			t.Fatalf("salvaged row %d: got %q,%v", i, v, ok)
		}
	}
}

func TestCorruptHeaderWithDataRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	raw := writeStrDB(t, path, 5)

	raw[12] ^= 0xff // break the header checksum; records after it are intact
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(path, StringCodec, StringCodec)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt header over live data: got %v, want ErrCorrupt", err)
	}
}

func TestTornTailStillTruncates(t *testing.T) {
	// The classic crash shape — a torn record at the very end — must keep
	// repairing silently, with all earlier rows intact.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	raw := writeStrDB(t, path, 10)

	cut := filepath.Join(dir, "cut.db")
	if err := os.WriteFile(cut, raw[:len(raw)-3], 0o644); err != nil {
		t.Fatal(err)
	}
	db := openStr(t, cut, WithSweepInterval(0))
	defer db.Close()
	checkRows(t, db, 9)
}
