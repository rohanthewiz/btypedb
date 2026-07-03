package btypedb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"strconv"

	"github.com/rohanthewiz/serr"
)

// The write-ahead log is a single append-only file of framed records:
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
// A record that is truncated or fails its CRC marks the end of the valid
// log; everything from that offset on is discarded on open (torn-write
// recovery after a crash mid-append).

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
// It returns the offset just past the last valid record or batch; a torn
// or corrupt record ends replay without error. An apply failure (e.g.
// codec mismatch) is returned as a hard error since the data itself is
// intact.
func replayLog(r io.Reader, apply func(rec walRecord) error) (validLen int64, err error) {
	br := bufio.NewReaderSize(r, 1<<20)
	var off int64
	for {
		rec, size, ok, err := readRecord(br)
		if err != nil {
			return off, err
		}
		if !ok {
			return off, nil
		}

		if rec.op != opBatch {
			if err := apply(rec); err != nil {
				return off, serr.Wrap(err, "phase", "apply record", "offset", itoa64(off))
			}
			off += size
			continue
		}

		// Batch: buffer the group, applying only if fully intact.
		if len(rec.val) != 8 {
			return off, nil // malformed header reads as corrupt tail
		}
		n := binary.LittleEndian.Uint64(rec.val)
		batch := make([]walRecord, 0, min(n, 1024))
		total := size
		intact := true
		for range n {
			brec, bsize, bok, berr := readRecord(br)
			if berr != nil {
				return off, berr
			}
			if !bok || brec.op == opBatch {
				intact = false
				break
			}
			batch = append(batch, brec)
			total += bsize
		}
		if !intact {
			return off, nil // torn mid-batch: discard the whole group
		}
		for _, brec := range batch {
			if err := apply(brec); err != nil {
				return off, serr.Wrap(err, "phase", "apply batch record", "offset", itoa64(off))
			}
		}
		off += total
	}
}

func itoa(n int) string     { return strconv.Itoa(n) }
func itoa64(n int64) string { return strconv.FormatInt(n, 10) }
