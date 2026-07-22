package btypedb

import (
	"bufio"
	"errors"
	"io"
	"time"

	"github.com/rohanthewiz/serr"
)

// compactSuffix names the temp file a compaction streams into. A
// leftover file with this suffix (crash mid-compaction) is discarded on
// Open — it only ever becomes live via the atomic rename at the end.
const compactSuffix = ".compact"

// Compact rewrites the log as a minimal snapshot of the live dataset,
// dropping overwritten and deleted records. Writers are only paused
// twice, briefly: once to take the O(1) snapshot and once to splice in
// the ops that committed while the snapshot was being streamed and to
// atomically swap the compacted file into place. A crash at any point
// leaves either the old complete log or the new complete log.
func (db *DB[K, V]) Compact() error {
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	// Phase A: freeze a snapshot and note where the log's tail begins.
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	if db.writeErr != nil {
		db.mu.Unlock()
		return serr.Wrap(db.writeErr, "state", "log is suspect after a failed append; not compacting")
	}
	snap := db.state.copy()
	tailStart := db.walSize
	db.mu.Unlock()

	tmpPath := db.path + compactSuffix
	tmp, err := db.fs.Create(tmpPath)
	if err != nil {
		db.releaseState(snap)
		return serr.Wrap(err, "path", tmpPath)
	}
	discard := func() {
		tmp.Close()
		db.fs.Remove(tmpPath)
	}

	// The compacted file always begins with the current-format header — v1
	// for a plaintext log, v2 (with a fresh key-check value) for an encrypted
	// one. This is also how legacy pre-header files get upgraded in place.
	if _, err := tmp.Write(headerFor(db.cipher)); err != nil {
		db.releaseState(snap)
		discard()
		return serr.Wrap(err, "op", "write compacted log header")
	}

	// Stream the snapshot with no locks held: it is immutable, and
	// writers keep appending to the live log meanwhile.
	snapBytes, err := db.writeSnapshot(tmp, snap)
	db.releaseState(snap)
	if err != nil {
		discard()
		return err
	}

	// Phase B: pause writers, splice in the tail that committed since
	// the snapshot, then swap the compacted file into place.
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		discard()
		return ErrClosed
	}
	if db.writeErr != nil {
		discard()
		return serr.Wrap(db.writeErr, "state", "log append failed during compaction")
	}
	tailLen := db.walSize - tailStart
	if tailLen > 0 {
		if _, err := io.Copy(tmp, io.NewSectionReader(db.file, tailStart, tailLen)); err != nil {
			discard()
			return serr.Wrap(err, "op", "copy log tail")
		}
	}
	if err := tmp.Sync(); err != nil {
		discard()
		return serr.Wrap(err, "op", "sync compacted log")
	}
	if err := db.fs.Rename(tmpPath, db.path); err != nil {
		discard()
		return serr.Wrap(err, "op", "swap compacted log")
	}
	db.fs.SyncDir(db.path)
	db.file.Close() // old inode, now unlinked
	db.file = tmp   // handle followed the rename; positioned at end
	// The header is 16 bytes for a plaintext log, 44 for an encrypted one;
	// size it from the same image just written above.
	db.walSize = int64(len(headerFor(db.cipher))) + snapBytes + tailLen
	db.baseSize = db.walSize
	// The bytes of the new file share nothing with the old one, so any
	// offset a replication follower holds is now meaningless — bump the
	// epoch so its next ReadLogRange fails fast with ErrEpochChanged.
	db.fileEpoch++
	// The new file holds every append so far, synced before the rename:
	// release any group-commit waiters whose handle we just retired.
	db.markDurable()
	return nil
}

// writeSnapshot streams every live pair in snap to f as set records —
// with the deadline preserved for TTL'd keys, and already-expired keys
// dropped entirely — and returns the byte count written.
func (db *DB[K, V]) writeSnapshot(f io.Writer, snap *dbState[K, V]) (int64, error) {
	now := time.Now().UnixNano()
	w := bufio.NewWriterSize(f, 1<<20)
	var n int64
	var rec []byte
	for k, v := range snap.data.All() {
		if snap.expired(k, now) {
			continue
		}
		kb, err := db.keyCodec.Encode(k)
		if err != nil {
			return 0, serr.Wrap(err, "encoding", "key")
		}
		vb, err := db.valCodec.Encode(v)
		if err != nil {
			return 0, serr.Wrap(err, "encoding", "value")
		}
		op := opSet
		if dl, hasTTL := snap.ttl.Get(k); hasTTL {
			op, vb = opSetTTL, prependDeadline(dl, vb)
		}
		// Re-seal from plaintext with a fresh nonce; this is also the
		// re-encrypt seam that a future key rotation would ride on.
		rec, err = appendSealedRecord(rec[:0], db.cipher, op, kb, vb)
		if err != nil {
			return 0, serr.Wrap(err, "op", "seal snapshot record")
		}
		if _, err := w.Write(rec); err != nil {
			return 0, serr.Wrap(err, "op", "write snapshot record")
		}
		n += int64(len(rec))
	}
	if err := w.Flush(); err != nil {
		return 0, serr.Wrap(err, "op", "flush snapshot")
	}
	return n, nil
}

// maybeAutoCompact spawns a background compaction when the log has
// outgrown the live dataset per the configured policy. Callers must
// hold mu.
func (db *DB[K, V]) maybeAutoCompact() {
	if !db.autoCompact || db.compacting || db.closed || db.writeErr != nil {
		return
	}
	if db.walSize < db.compactMinSize {
		return
	}
	if db.walSize-db.baseSize < db.baseSize*int64(db.compactGrowthPct)/100 {
		return
	}
	db.compacting = true
	db.bgWG.Go(func() {
		err := db.Compact()
		db.mu.Lock()
		db.compacting = false
		if err != nil && !errors.Is(err, ErrClosed) && db.compactErr == nil {
			db.compactErr = serr.Wrap(err, "op", "auto compact")
		}
		db.mu.Unlock()
	})
}
