package btypedb

import (
	"errors"
	"io"

	"github.com/rohanthewiz/serr"
)

// Replication primitives.
//
// The log file is strictly append-only between compactions, which makes
// incremental replication a matter of shipping byte ranges: a follower
// remembers how many bytes it has copied and periodically asks for
// whatever appended since. The one event that breaks the append-only
// invariant is a compaction, which atomically swaps in a rewritten file
// whose bytes share nothing with the old one — every previously shipped
// offset becomes meaningless.
//
// The epoch is how a follower detects that swap. Each DB handle counts
// file swaps: epoch N's byte ranges are immutable for the lifetime of
// the handle, and any range read is validated against the epoch it was
// requested for, so a follower can never splice bytes from two
// different files into one copy. On seeing ErrEpochChanged (or a fresh
// process, since epochs restart at zero on Open), a follower starts a
// new replica "generation" from offset zero.
//
// Restarting from zero on every Open is not just simplicity: crash
// recovery may truncate a torn tail, so post-restart appends can
// diverge from bytes a follower shipped moments before the crash.
// Fresh-generation-per-Open sidesteps that whole class of split-brain
// reasoning — an old generation still restores to the valid
// point-in-time state it captured, and the new one supersedes it.
//
//	writer:   |-- epoch 0 (append-only) --| compact |-- epoch 1 --| ...
//	follower: ship [0,a) [a,b) [b,c) ...   restart   ship [0,x) ...
//
// A range may include the newest appends before their fsync completes —
// the same stance as BackupTo: that only ever makes the replica a
// superset of what a simultaneous power cut would have preserved
// locally, and every record is CRC-framed with batch atomicity, so a
// replica assembled from these ranges replays exactly like a crash
// recovery would.

// ErrEpochChanged is returned by ReadLogRange when the log file was
// swapped by a compaction after the caller captured its epoch. The
// caller should fetch the new epoch via LogState and restart its copy
// from offset zero. Test with errors.Is.
var ErrEpochChanged = errors.New("btypedb: log file was replaced; restart replication from offset zero")

// LogState returns the current log epoch and its size in bytes. The
// bytes [0, size) of the returned epoch are immutable: the log only
// grows within an epoch, and any rewrite bumps the epoch. Size counts
// only fully appended records (a torn append never advances it), so
// every byte below it is safe to ship.
func (db *DB[K, V]) LogState() (epoch uint64, size int64, err error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return 0, 0, ErrClosed
	}
	return db.fileEpoch, db.walSize, nil
}

// ReadLogRange copies up to max bytes of the log starting at offset
// from, for the given epoch, into w, returning the count copied. A
// short count (including zero) with a nil error means the log currently
// ends inside the requested range — poll LogState for growth. If a
// compaction replaced the file since epoch was captured, no bytes are
// written and ErrEpochChanged is returned.
//
// Compaction (only) is held off for the duration of the copy, exactly
// as in BackupTo; readers and writers are never blocked. Callers keep
// ranges modest (a few MB) so that hold stays brief.
func (db *DB[K, V]) ReadLogRange(epoch uint64, from, max int64, w io.Writer) (int64, error) {
	if from < 0 || max < 0 {
		return 0, serr.New("negative range", "from", itoa64(from), "max", itoa64(max))
	}

	// compactMu (not mu) is what pins db.file: the copy below runs
	// lock-free against the immutable bytes while writers keep
	// appending past them, but the handle must not be swapped out and
	// closed mid-read. Same order as BackupTo: compactMu, then mu.
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return 0, ErrClosed
	}
	if db.fileEpoch != epoch {
		cur := db.fileEpoch
		db.mu.RUnlock()
		return 0, serr.Wrap(ErrEpochChanged, "wanted", itoa64(int64(epoch)), "current", itoa64(int64(cur)))
	}
	f, size := db.file, db.walSize
	db.mu.RUnlock()

	if from > size {
		// Beyond the end can only be a follower bookkeeping bug — an
		// epoch mismatch would have been caught above — so fail loudly
		// rather than reporting an empty read forever.
		return 0, serr.New("range start beyond log size", "from", itoa64(from), "size", itoa64(size))
	}
	n := min(max, size-from)
	if n == 0 {
		return 0, nil
	}
	copied, err := io.Copy(w, io.NewSectionReader(f, from, n))
	if err != nil {
		return copied, serr.Wrap(err, "op", "read log range", "from", itoa64(from), "want", itoa64(n))
	}
	return copied, nil
}
