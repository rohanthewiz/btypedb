package btypedb

import (
	"bufio"
	"cmp"
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
//
// The second pause doesn't grow with the database. The snapshot, and
// the tail that piled up while it streamed, are copied and fsynced
// before writers are paused, so the lock covers only the last residue:
//
//	phase A   db.mu held   copy state, note tail start       O(1)
//	          unlocked     stream snapshot                   O(live data)
//	          unlocked     copy tail so far, fsync; repeat   O(live data + tail)
//	phase B   db.mu held   copy residue, fsync, rename       O(residue)
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

	// Catch up without the lock: copy the tail that committed while the
	// snapshot streamed, and fsync everything written so far, in rounds,
	// while writers keep running. Phase B then holds db.mu only for the
	// residue that landed during the last round.
	//
	// Both halves used to run under the lock, and both grew with the
	// database. writeSnapshot only flushes its buffer to the OS, so phase
	// B's fsync pushed the whole snapshot to disk; and the tail is every
	// append made during the stream, which is longer for a bigger
	// snapshot. On darwin (Sync is F_FULLFSYNC), a writer looping Set
	// stalled up to ~185 ms behind a ~920 MB compaction; with a writer
	// fast enough, the tail alone reached hundreds of MB.
	//
	// Reading db.file without db.mu is the BackupTo pattern: compactMu
	// (held) pins the handle, and bytes below a walSize read under the
	// lock are immutable while writers append past them. None of these
	// syncs is what makes the rename safe — phase B's final one is — so
	// a crash in here just leaves a temp file that Open discards.
	copied := tailStart
	var streamEnd int64 // where the live log ended when the snapshot finished
	for round := 0; ; round++ {
		db.mu.RLock()
		f, end := db.file, db.walSize
		db.mu.RUnlock()
		if round == 0 {
			streamEnd = end
		}
		if end > copied {
			if _, err := io.Copy(tmp, io.NewSectionReader(f, copied, end-copied)); err != nil {
				return discardCopyErr(db, discard, err)
			}
			copied = end
		}
		if err := tmp.Sync(); err != nil {
			discard()
			return serr.Wrap(err, "op", "sync compacted snapshot")
		}
		// Stop once the residue is small enough to splice under the lock,
		// or after a fixed number of rounds: a writer that outruns the disk
		// would otherwise keep this loop going forever. Phase B then takes
		// whatever is left.
		db.mu.RLock()
		behind := db.walSize - copied
		db.mu.RUnlock()
		if behind <= compactCatchUpSlack || round == compactCatchUpRounds-1 {
			break
		}
	}

	// Phase B: pause writers, splice in the residue of the tail that the
	// catch-up loop didn't copy, then swap the compacted file into place.
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
	if residue := db.walSize - copied; residue > 0 {
		if _, err := io.Copy(tmp, io.NewSectionReader(db.file, copied, residue)); err != nil {
			discard()
			return serr.Wrap(err, "op", "copy log tail")
		}
		// The rename below must never publish bytes that aren't on disk,
		// so the residue is synced before it. Everything before it was
		// synced by the catch-up loop. With no residue there is nothing
		// new to sync, and skipping it saves a full device flush
		// (F_FULLFSYNC on darwin) while writers wait.
		if err := tmp.Sync(); err != nil {
			discard()
			return serr.Wrap(err, "op", "sync compacted log")
		}
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
	hdrLen := int64(len(headerFor(db.cipher)))
	db.walSize = hdrLen + snapBytes + tailLen
	// baseSize leaves out the tail appended after the snapshot finished
	// streaming. Those appends used to wait for phase B and then land in
	// the new file as growth, and they still count toward the next
	// auto-compaction, so the catch-up loop doesn't change the policy:
	//
	//	compacted file:  header │ snapshot │ tail during stream │ tail after
	//	                 └───────── baseSize ─────────────────┘ └─ growth ─┘
	db.baseSize = hdrLen + snapBytes + (streamEnd - tailStart)
	// The bytes of the new file share nothing with the old one, so any
	// offset a replication follower holds is now meaningless — bump the
	// epoch so its next ReadLogRange fails fast with ErrEpochChanged.
	db.fileEpoch++
	// The new file holds every append so far, synced before the rename:
	// release any group-commit waiters whose handle we just retired.
	db.markDurable()
	// Appends that landed during the catch-up loop are already past
	// baseSize, and if they were a burst that has since stopped, no later
	// append will look at them. Look now, as the first append after a
	// stalled phase B used to. A no-op for an auto-compaction run (its
	// compacting flag is still set): its goroutine re-checks instead.
	db.maybeAutoCompact()
	return nil
}

// The catch-up loop in Compact stops once phase B would have at most
// compactCatchUpSlack bytes of tail left to splice under the lock, or
// after compactCatchUpRounds rounds. A MiB copies and fsyncs in a few
// milliseconds. Each round lasts about as long as it takes to copy and
// sync the previous round's growth, so a writer slower than the disk
// converges in a round or two. One faster than the disk never converges,
// and the cap hands phase B whatever is left.
const (
	compactCatchUpSlack  = 1 << 20
	compactCatchUpRounds = 4
)

// discardCopyErr abandons a compaction whose unlocked tail copy failed.
// A manual Compact can race Close, which closes db.file without taking
// compactMu, so a read error after Close is reported as ErrClosed (as
// phase B would have) rather than as an I/O failure.
func discardCopyErr[K cmp.Ordered, V any](db *DB[K, V], discard func(), err error) error {
	discard()
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	return serr.Wrap(err, "op", "copy log tail")
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
		// Only after a success: growth that arrived during this run's
		// catch-up loop may already be past the threshold (see the end of
		// Compact). After a failure, retrying here could spin on a
		// persistent error, so the next append decides instead.
		if err == nil {
			db.maybeAutoCompact()
		}
		db.mu.Unlock()
	})
}
