package btypedb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"strconv"

	"github.com/rohanthewiz/serr"
)

// The write-ahead log opens with a fixed 16-byte header:
//
//	magic    8 bytes  "btydbLOG"
//	version  4 bytes  little-endian format version (currently 1)
//	crc      4 bytes  CRC-32 (IEEE) over magic+version
//
// The header lets Open reject files that are not btypedb logs and files
// written by a newer, incompatible format revision — before the header
// existed, either mistake read as "corrupt tail at offset 0" and
// silently truncated the whole file. Files created before the header
// (magic absent) still open as legacy format-0; their next compaction
// rewrites them with a header. The whole v1 header is a compile-time
// constant, which makes a torn header write (crash during first
// creation) detectable as a strict prefix of that constant.
//
// After the header, the log is an append-only sequence of framed records:
//
//	op    1 byte   (1 = set, 2 = delete, 3 = batch header, 4 = set with TTL)
//	klen  4 bytes  little-endian
//	vlen  4 bytes  little-endian (0 for delete)
//	key   klen bytes
//	val   vlen bytes
//	crc   4 bytes  CRC-32 (IEEE) over op..val
//
// A batch header (klen 0, vlen 8, val = uint64 op count) marks the next
// N records as one atomic transaction: replay applies them all or, if
// any part is torn or corrupt, discards the whole group.
//
// A set-with-TTL record prefixes its value bytes with the absolute
// expiry deadline (8 bytes, unix nanoseconds, little-endian), so replay
// at any later time reconstructs the same expiration.
//
// A record that is truncated or fails its CRC normally marks the end of
// the valid log — the expected shape of a crash mid-append — and the
// tail past it is discarded on open. But Open first scans past the bad
// record: if any intact record follows it, the damage is *mid-file*
// (bitrot, a wrong tool writing into the log), truncating would silently
// discard good committed data, and Open refuses with ErrCorrupt instead
// (override with WithTruncateAtCorruption).

const (
	opSet    byte = 1
	opDelete byte = 2
	opBatch  byte = 3
	opSetTTL byte = 4
)

const (
	recHeaderSize = 9 // op(1) + klen(4) + vlen(4)
	recCRCSize    = 4
	ttlPrefixSize = 8       // deadline prefix on opSetTTL values
	maxPartLen    = 1 << 30 // sanity bound on klen/vlen; larger reads as corruption
)

const (
	logMagic = "btydbLOG" // first byte 'b' is outside the 1..4 op range, so a legacy log can never start with it

	// plainFormatVersion tags an unencrypted log — the original v1 layout
	// (magic|version|crc, 16 bytes). encFormatVersion tags an encrypted log,
	// whose header additionally carries the cipher flags and a key-check
	// value (see walCipher.header in encrypt.go). logFormatVersion is the
	// highest version this build understands; a file numbered above it is
	// refused with ErrNewerFormat — which is precisely how an older,
	// encryption-unaware binary (whose logFormatVersion is still 1) rejects
	// an encrypted v2 file.
	plainFormatVersion = 1
	encFormatVersion   = 2
	logFormatVersion   = 2

	logHeaderSize   = 16 // v1: magic(8) + version(4) + crc(4)
	logHeaderSizeV2 = 44 // v2: magic(8) + version(4) + flags(4) + kcv(24) + crc(4)
)

// logHeader returns the header for a freshly written log file. For the
// current version every byte is deterministic, so callers compare
// against it directly to detect torn header writes.
func logHeader() []byte {
	h := make([]byte, 0, logHeaderSize)
	h = append(h, logMagic...)
	h = binary.LittleEndian.AppendUint32(h, plainFormatVersion)
	return binary.LittleEndian.AppendUint32(h, crc32.ChecksumIEEE(h))
}

// prepareHeader classifies the just-opened log file by its header and
// returns the offset where records begin. wc carries the caller's
// encryption configuration (nil = open as plaintext) and decides which
// header the file is expected to have.
//
// An empty file gets the appropriate header (v1 plaintext, or v2 encrypted
// when a key was supplied) written and synced before any record can follow
// it, so a headerless-but-nonempty new-format file can never exist. A strict
// prefix of the header image we would write means the creating process
// crashed between writing the header and syncing it — no record was ever
// acknowledged, so start the file over. Anything that does not open with the
// magic is a legacy pre-header log whose records start at offset 0.
//
// The header also reconciles the file's encryption state with the caller's
// key, failing fast — before any record is read — with ErrKeyRequired (an
// encrypted file opened without a key), ErrNotEncrypted (a plaintext file
// opened with a key), ErrCipherMismatch (cipher/scope disagreement), or
// ErrWrongKey (the header's key-check value does not match the supplied key).
func prepareHeader(f logfile, wc *walCipher) (dataStart int64, err error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, serr.Wrap(err, "op", "stat log")
	}
	size := fi.Size()
	expected := headerFor(wc) // the image WE would write for this config
	hdrSize := int64(len(expected))

	writeFresh := func() (int64, error) {
		if err := f.Truncate(0); err != nil {
			return 0, serr.Wrap(err, "op", "reset torn log header")
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return 0, serr.Wrap(err, "op", "seek for log header")
		}
		if _, err := f.Write(expected); err != nil {
			return 0, serr.Wrap(err, "op", "write log header")
		}
		if err := f.Sync(); err != nil {
			return 0, serr.Wrap(err, "op", "sync log header")
		}
		return hdrSize, nil
	}

	if size == 0 {
		return writeFresh()
	}

	// Read up to a full v2 header so we can classify either format.
	got := make([]byte, min(size, int64(logHeaderSizeV2)))
	if _, err := f.ReadAt(got, 0); err != nil {
		return 0, serr.Wrap(err, "op", "read log header")
	}

	// A strict prefix of exactly what we would write, with nothing after it:
	// the header write tore during first creation and nothing was ever live.
	if size < hdrSize && bytes.Equal(got, expected[:size]) {
		return writeFresh()
	}

	// Shorter than a v1 prefix, or not opening with the magic → legacy
	// pre-header log, records at offset 0. A key cannot open a plaintext file.
	if size < logHeaderSize || !bytes.Equal(got[:len(logMagic)], []byte(logMagic)) {
		if wc != nil {
			return 0, serr.Wrap(ErrNotEncrypted, "state", "database is not encrypted but an encryption key was supplied")
		}
		return 0, nil
	}

	// The 16-byte compatibility prefix — magic(8) | version(4) | crc(4) over
	// the first 12 — is identical in every format, so the checksum is
	// validated the same way for v1 and v2, and BEFORE the version is trusted.
	// A bad checksum is a torn or junk header (a crash during creation with
	// sector junk past the tear), never a "newer format": no record ever
	// existed (the header is synced before the first record), unless an intact
	// record survives past it, which means mid-file corruption instead.
	if crc32.ChecksumIEEE(got[:12]) != binary.LittleEndian.Uint32(got[12:16]) {
		rest, err := io.ReadAll(io.NewSectionReader(f, logHeaderSize, size-logHeaderSize))
		if err != nil {
			return 0, serr.Wrap(err, "op", "read past corrupt header")
		}
		if _, found := scanForRecord(rest); found {
			return 0, serr.Wrap(ErrCorrupt, "state", "log header failed its checksum")
		}
		return writeFresh()
	}

	// Prefix checksum good: the version field is trustworthy.
	version := binary.LittleEndian.Uint32(got[8:12])
	if version > logFormatVersion {
		return 0, serr.Wrap(ErrNewerFormat,
			"fileVersion", itoa(int(version)), "supported", itoa(logFormatVersion))
	}
	fileEncrypted := version >= encFormatVersion
	switch {
	case fileEncrypted && wc == nil:
		return 0, serr.Wrap(ErrKeyRequired, "state", "database is encrypted but no encryption key was supplied")
	case !fileEncrypted && wc != nil:
		return 0, serr.Wrap(ErrNotEncrypted, "state", "database is not encrypted but an encryption key was supplied")
	}
	if !fileEncrypted {
		return logHeaderSize, nil // v1: records begin right after the prefix
	}

	// Encrypted (v2): flags and the KCV trail the compatibility prefix.
	if size < logHeaderSizeV2 {
		// A valid prefix but the v2 header is incomplete. Records can only
		// begin at logHeaderSizeV2, so nothing was ever live → torn creation.
		return writeFresh()
	}
	// flags and KCV are deterministic in the key, so an exact compare against
	// the image we would write proves both the cipher/scope configuration and
	// the key itself before any record is touched.
	if !bytes.Equal(got[hdrFlagsOffset:hdrKCVOffset], expected[hdrFlagsOffset:hdrKCVOffset]) {
		return 0, serr.Wrap(ErrCipherMismatch, "state", "encryption cipher or scope does not match the database header")
	}
	if !bytes.Equal(got[hdrKCVOffset:logHeaderSizeV2], expected[hdrKCVOffset:logHeaderSizeV2]) {
		return 0, serr.Wrap(ErrWrongKey, "state", "encryption key does not match the database")
	}
	return logHeaderSizeV2, nil
}

// prependDeadline returns val prefixed with the encoded expiry deadline,
// forming the value payload of an opSetTTL record.
func prependDeadline(deadline int64, val []byte) []byte {
	out := make([]byte, ttlPrefixSize+len(val))
	binary.LittleEndian.PutUint64(out, uint64(deadline))
	copy(out[ttlPrefixSize:], val)
	return out
}

// appendRecord appends one framed record to dst and returns the extended slice.
func appendRecord(dst []byte, op byte, key, val []byte) []byte {
	start := len(dst)
	var hdr [recHeaderSize]byte
	hdr[0] = op
	binary.LittleEndian.PutUint32(hdr[1:5], uint32(len(key)))
	binary.LittleEndian.PutUint32(hdr[5:9], uint32(len(val)))
	dst = append(dst, hdr[:]...)
	dst = append(dst, key...)
	dst = append(dst, val...)
	return binary.LittleEndian.AppendUint32(dst, crc32.ChecksumIEEE(dst[start:]))
}

// walRecord is one decoded log entry.
type walRecord struct {
	op  byte
	key []byte
	val []byte
}

// readRecord reads one framed record. ok is false at clean EOF and on a
// torn or corrupt record — the caller treats both as end-of-valid-log.
// err is reserved for real I/O failures.
func readRecord(br *bufio.Reader) (rec walRecord, size int64, ok bool, err error) {
	var hdr [recHeaderSize]byte
	if _, e := io.ReadFull(br, hdr[:]); e != nil {
		if errors.Is(e, io.EOF) || errors.Is(e, io.ErrUnexpectedEOF) {
			return rec, 0, false, nil // clean end, or torn header
		}
		return rec, 0, false, serr.Wrap(e, "phase", "read record header")
	}
	op := hdr[0]
	klen := int64(binary.LittleEndian.Uint32(hdr[1:5]))
	vlen := int64(binary.LittleEndian.Uint32(hdr[5:9]))
	if op < opSet || op > opSetTTL || klen > maxPartLen || vlen > maxPartLen {
		return rec, 0, false, nil // corrupt
	}

	body := make([]byte, klen+vlen+recCRCSize)
	if _, e := io.ReadFull(br, body); e != nil {
		if errors.Is(e, io.EOF) || errors.Is(e, io.ErrUnexpectedEOF) {
			return rec, 0, false, nil // torn body
		}
		return rec, 0, false, serr.Wrap(e, "phase", "read record body")
	}

	wantCRC := binary.LittleEndian.Uint32(body[klen+vlen:])
	h := crc32.NewIEEE()
	h.Write(hdr[:])
	h.Write(body[:klen+vlen])
	if h.Sum32() != wantCRC {
		return rec, 0, false, nil // corrupt
	}

	rec = walRecord{op: op, key: body[:klen], val: body[klen : klen+vlen]}
	return rec, recHeaderSize + klen + vlen + recCRCSize, true, nil
}

// replayLog streams records from r, invoking apply for each valid one.
// Records following a batch header are applied only if the whole batch
// is intact, keeping multi-op transactions atomic across a crash.
//
// validLen is the offset just past the last applied record or batch —
// the point the file may safely be truncated to. parsedLen (>= validLen)
// is the offset just past the last record that framed and CRC-checked
// correctly, even if it was discarded as part of an incomplete trailing
// batch; the caller scans *from there* when deciding whether the
// unparseable remainder is a torn tail or mid-file corruption (scanning
// from validLen would misread a torn batch's own intact members as
// "good data after the corruption"). A torn or corrupt record ends
// replay without error. An apply failure (e.g. codec mismatch) is
// returned as a hard error since the data itself is intact.
func replayLog(r io.Reader, apply func(rec walRecord) error) (validLen, parsedLen int64, err error) {
	br := bufio.NewReaderSize(r, 1<<20)
	var off, parsed int64
	for {
		rec, size, ok, err := readRecord(br)
		if err != nil {
			return off, parsed, err
		}
		if !ok {
			return off, parsed, nil
		}

		if rec.op != opBatch {
			if err := apply(rec); err != nil {
				return off, parsed, serr.Wrap(err, "phase", "apply record", "offset", itoa64(off))
			}
			off += size
			parsed = off
			continue
		}

		// Batch: buffer the group, applying only if fully intact.
		if len(rec.val) != 8 {
			// CRC-valid header with a malformed count: count past it as
			// parsed (its bytes are intact) but stop applying here.
			parsed = off + size
			return off, parsed, nil
		}
		n := binary.LittleEndian.Uint64(rec.val)
		batch := make([]walRecord, 0, min(n, 1024))
		total := size
		parsed = off + size
		intact := true
		for range n {
			brec, bsize, bok, berr := readRecord(br)
			if berr != nil {
				return off, parsed, berr
			}
			if !bok {
				intact = false
				break
			}
			if brec.op == opBatch {
				// A batch header where a member belongs means the group's
				// count never got satisfied yet the log kept going — that
				// cannot come from a crash (writes stop at the tear), so
				// leave parsed *before* this header: the caller's scan will
				// see it as an intact record after the damage and refuse.
				intact = false
				break
			}
			batch = append(batch, brec)
			total += bsize
			parsed += bsize
		}
		if !intact {
			return off, parsed, nil // torn mid-batch: discard the whole group
		}
		for _, brec := range batch {
			if err := apply(brec); err != nil {
				return off, parsed, serr.Wrap(err, "phase", "apply batch record", "offset", itoa64(off))
			}
		}
		off += total
		parsed = off
	}
}

// scanForRecord reports whether an intact framed record begins at any
// byte offset in data, returning the offset of the first one. It is the
// torn-tail vs mid-file-corruption discriminator: a crash tears the
// *end* of the log, so nothing parseable follows the damage; if a whole
// record survives past it, truncating there would discard committed
// data. Trying every byte offset re-synchronizes framing no matter how
// the corruption shifted record boundaries, and a false positive on
// random garbage needs a CRC-32 collision on plausible framing —
// vanishingly unlikely.
func scanForRecord(data []byte) (off int64, found bool) {
	const minRec = int64(recHeaderSize + recCRCSize)
	for i := int64(0); i+minRec <= int64(len(data)); i++ {
		op := data[i]
		if op < opSet || op > opSetTTL {
			continue
		}
		klen := int64(binary.LittleEndian.Uint32(data[i+1 : i+5]))
		vlen := int64(binary.LittleEndian.Uint32(data[i+5 : i+9]))
		if klen > maxPartLen || vlen > maxPartLen {
			continue
		}
		end := i + recHeaderSize + klen + vlen
		if end+recCRCSize > int64(len(data)) {
			continue
		}
		if crc32.ChecksumIEEE(data[i:end]) == binary.LittleEndian.Uint32(data[end:end+recCRCSize]) {
			return i, true
		}
	}
	return 0, false
}

func itoa(n int) string     { return strconv.Itoa(n) }
func itoa64(n int64) string { return strconv.FormatInt(n, 10) }
