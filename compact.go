package btypedb

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/rohanthewiz/serr"
	"github.com/tidwall/btype"
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
	snap := db.m.Copy()
	tailStart := db.walSize
	db.mu.Unlock()

	tmpPath := db.path + compactSuffix
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		db.releaseSnap(snap)
		return serr.Wrap(err, "path", tmpPath)
	}
	discard := func() {
		tmp.Close()
		os.Remove(tmpPath)
	}

	// Stream the snapshot with no locks held: it is immutable, and
	// writers keep appending to the live log meanwhile.
	snapBytes, err := db.writeSnapshot(tmp, snap)
	db.releaseSnap(snap)
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
	if err := os.Rename(tmpPath, db.path); err != nil {
		discard()
		return serr.Wrap(err, "op", "swap compacted log")
	}
	syncDir(db.path)
	db.file.Close() // old inode, now unlinked
	db.file = tmp   // handle followed the rename; positioned at end
	db.walSize = snapBytes + tailLen
	db.baseSize = db.walSize
	return nil
}

// writeSnapshot streams every pair in snap to f as set records and
// returns the byte count written.
func (db *DB[K, V]) writeSnapshot(f *os.File, snap *btype.Map[K, V]) (int64, error) {
	w := bufio.NewWriterSize(f, 1<<20)
	var n int64
	var rec []byte
	for k, v := range snap.All() {
		kb, err := db.keyCodec.Encode(k)
		if err != nil {
			return 0, serr.Wrap(err, "encoding", "key")
		}
		vb, err := db.valCodec.Encode(v)
		if err != nil {
			return 0, serr.Wrap(err, "encoding", "value")
		}
		rec = appendRecord(rec[:0], opSet, kb, vb)
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

// syncDir persists a rename in path's directory. Best effort: directory
// fsync is not supported everywhere.
func syncDir(path string) {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return
	}
	d.Sync()
	d.Close()
}
