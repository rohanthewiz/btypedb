package btypedb

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"

	"github.com/rohanthewiz/serr"
)

// Row-level WAL encryption at rest (value-only scope)
// ---------------------------------------------------
// The engine keeps every row plaintext in memory — queries, ordering, and
// range scans run at full speed on decoded values — and encrypts only the
// on-disk record *value* payload. The tuple-encoded key stays cleartext
// (value-only scope), which is why encrypting it never disturbs the
// in-memory B-tree: ordering comes from the decoded key, not on-disk bytes.
//
// Because replication and backup ship raw log bytes (replication.go /
// backup.go do a bare io.Copy over file ranges), sealing the on-disk value
// makes the S3 replica chunks and backup files ciphertext automatically; a
// follower or a restore needs the same key to Open and serve the file.
//
// Threat model: this protects data at rest (a stolen disk, backup, or
// object-store copy). It does NOT protect a running process's memory, and —
// being value-only — it does NOT hide primary-key column values, which ride
// in the cleartext key.

const (
	// hdrFlagEncrypted is set in the v2 header's flags word for any
	// encrypted log. The cipher id occupies the next four bits and the
	// scope the bit above that, leaving room to grow the format without a
	// version bump.
	hdrFlagEncrypted uint32 = 1 << 0

	// cipherAES256GCM is the only cipher shipped today. Reserve the id
	// space (bits 1..4 of flags) so ChaCha20-Poly1305 etc. can be added
	// later behind the same header machinery.
	cipherAES256GCM uint8 = 1

	// scopeValueOnly encrypts the record value only; the key stays
	// cleartext. scopeKeyAndValue is reserved for a future full-row option.
	scopeValueOnly uint8 = 0

	// kcvConst is the constant plaintext sealed into the header as a key
	// check value. Opening it (or, since the seal is deterministic,
	// byte-comparing it) proves the supplied key matches the database
	// before a single record is touched, so a wrong key fails fast with
	// ErrWrongKey instead of surfacing as a storm of per-record auth
	// failures during replay.
	kcvConst = "btydbKCV"

	// v2 header field offsets. The first 16 bytes are byte-for-byte the same
	// compatibility prefix as a v1 header — magic(8) | version(4) | crc(4)
	// over the first 12 — so header classification and torn/junk detection
	// run identically for both formats, and the version is only trusted once
	// that prefix checksum verifies. flags and the KCV trail the prefix and
	// carry their own integrity: the KCV's AEAD tag authenticates flags as
	// additional data, and both are validated by an exact compare against the
	// image we would write (deterministic in the key).
	hdrFlagsOffset = 16 // flags word
	hdrKCVOffset   = 20 // key-check value (24 bytes: 8 ct + 16 tag)
)

// walCipher holds a database's AEAD state for sealing WAL record payloads.
// It is deliberately non-generic so crypto never leaks into DB[K, V]'s type
// parameters. A nil *walCipher means the log is plaintext: every helper
// below degrades to the original framing, so unencrypted databases stay
// byte-for-byte unchanged.
type walCipher struct {
	rec      cipher.AEAD // seals record values, keyed by the derived record subkey
	kcv      cipher.AEAD // seals the header key-check value, keyed by a separate subkey
	cipherID uint8
	scope    uint8
}

// newWalCipher derives the record and KCV subkeys from a 32-byte master and
// builds the AEADs. Two subkeys (via HKDF-SHA256 with distinct info labels)
// are deliberate: the KCV is sealed under a *fixed* all-zero nonce, so it
// must live in a different key space from the random per-record nonces —
// a reused (key, nonce) pair is catastrophic for GCM. Deriving both also
// means the caller's raw master is never used directly as a cipher key.
func newWalCipher(master []byte) (*walCipher, error) {
	if len(master) != 32 {
		return nil, serr.New("encryption key must be 32 bytes", "len", itoa(len(master)))
	}
	kRec, err := hkdf.Key(sha256.New, master, nil, "btypedb/wal/record/v2", 32)
	if err != nil {
		return nil, serr.Wrap(err, "op", "derive record key")
	}
	kKCV, err := hkdf.Key(sha256.New, master, nil, "btypedb/wal/kcv/v2", 32)
	if err != nil {
		return nil, serr.Wrap(err, "op", "derive kcv key")
	}
	recAEAD, err := newGCM(kRec)
	if err != nil {
		return nil, err
	}
	kcvAEAD, err := newGCM(kKCV)
	if err != nil {
		return nil, err
	}
	return &walCipher{
		rec:      recAEAD,
		kcv:      kcvAEAD,
		cipherID: cipherAES256GCM,
		scope:    scopeValueOnly,
	}, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, serr.Wrap(err, "op", "aes cipher")
	}
	g, err := cipher.NewGCM(block)
	if err != nil {
		return nil, serr.Wrap(err, "op", "gcm")
	}
	return g, nil
}

// flags is the header flags word describing this cipher's configuration.
func (wc *walCipher) flags() uint32 {
	return hdrFlagEncrypted | uint32(wc.cipherID)<<1 | uint32(wc.scope)<<5
}

// header builds the 44-byte v2 (encrypted) log header:
//
//	magic(8) | version(4)=2 | crc(4) | flags(4) | kcv(24)
//	\_________ compatibility prefix ________/
//	         crc = crc32(bytes[0:12])
//
// The leading 16 bytes are structurally identical to a v1 header, so an
// older reader validates the prefix checksum and rejects this file cleanly
// with ErrNewerFormat, and torn/junk-header detection is shared with the
// plaintext path. The KCV seals kcvConst under a fixed all-zero nonce, bound
// to the header-so-far (magic|version|crc|flags) as AAD. Because AES-GCM is
// deterministic given (key, nonce, plaintext, AAD), the whole 44-byte image
// is a *key-time constant* — which lets prepareHeader detect a torn partial
// write as a strict prefix of a known image and prove the key by byte
// compare.
func (wc *walCipher) header() []byte {
	h := make([]byte, 0, logHeaderSizeV2)
	h = append(h, logMagic...)                                     // [0:8]
	h = binary.LittleEndian.AppendUint32(h, encFormatVersion)      // [8:12]
	h = binary.LittleEndian.AppendUint32(h, crc32.ChecksumIEEE(h)) // [12:16], over [0:12]
	h = binary.LittleEndian.AppendUint32(h, wc.flags())            // [16:20]
	nonce := make([]byte, wc.kcv.NonceSize())                      // all-zero; safe — see newWalCipher
	kcv := wc.kcv.Seal(nil, nonce, []byte(kcvConst), h)            // AAD = [0:20]; appends [20:44]
	return append(h, kcv...)
}

// headerFor returns the header a fresh (or compacted) log should carry:
// the plaintext v1 header when wc is nil, the encrypted v2 header otherwise.
func headerFor(wc *walCipher) []byte {
	if wc == nil {
		return logHeader()
	}
	return wc.header()
}

// recordAAD binds a record's op and (cleartext) key into the AEAD's
// additional data, so a record that is tampered or relocated to another
// key fails to open even though the ciphertext itself is well-formed.
func recordAAD(op byte, key []byte) []byte {
	aad := make([]byte, 0, 1+len(key))
	aad = append(aad, op)
	return append(aad, key...)
}

// sealValue returns nonce || AEAD-seal(val, AAD = op||key). The random
// nonce is stored inline as the value's prefix.
func (wc *walCipher) sealValue(op byte, key, val []byte) ([]byte, error) {
	ns := wc.rec.NonceSize()
	// One allocation: nonce prefix followed by room for the sealed output.
	out := make([]byte, ns, ns+len(val)+wc.rec.Overhead())
	if _, err := rand.Read(out[:ns]); err != nil {
		return nil, serr.Wrap(err, "op", "generate nonce")
	}
	// Seal appends the ciphertext after the nonce we already laid down.
	return wc.rec.Seal(out, out[:ns], val, recordAAD(op, key)), nil
}

// appendSealedRecord frames one record like appendRecord, but seals the
// value payload when encryption is on. The key, op/klen/vlen framing, and
// the trailing CRC all stay outside the ciphertext, so crash recovery and
// torn-tail detection run before any key is needed and the CRC still catches
// at-rest bit-rot. opDelete carries no value, so it is framed exactly as
// plaintext — there is nothing to protect, and its key is already cleartext.
func appendSealedRecord(dst []byte, wc *walCipher, op byte, key, val []byte) ([]byte, error) {
	if wc == nil || op == opDelete {
		return appendRecord(dst, op, key, val), nil
	}
	sealed, err := wc.sealValue(op, key, val)
	if err != nil {
		return dst, err
	}
	return appendRecord(dst, op, key, sealed), nil
}

// openRecord reverses appendSealedRecord during replay. With wc nil (or an
// opDelete, which was never sealed) it returns the regions unchanged.
// Otherwise it strips the nonce prefix and AEAD-opens the value. A failure
// here means bytes that passed the CRC failed authentication — tampering,
// or a wrong/mismatched key that slipped past the header KCV — so it is a
// hard replay error, never treated as a torn tail.
func openRecord(wc *walCipher, op byte, key, val []byte) (plainKey, plainVal []byte, err error) {
	if wc == nil || op == opDelete {
		return key, val, nil
	}
	ns := wc.rec.NonceSize()
	if len(val) < ns+wc.rec.Overhead() {
		return nil, nil, serr.New("encrypted value too short", "len", itoa(len(val)))
	}
	pt, err := wc.rec.Open(nil, val[:ns], val[ns:], recordAAD(op, key))
	if err != nil {
		return nil, nil, serr.Wrap(err, "op", "decrypt record")
	}
	return key, pt, nil
}
