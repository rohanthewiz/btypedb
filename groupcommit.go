package btypedb

import (
	"sync"

	"github.com/rohanthewiz/serr"
)

// Group commit: under SyncAlways every write must be fsynced before it
// is acknowledged, but concurrent committers need not each pay for
// their own fsync. Every log append takes a sequence number; a
// committer then waits only until the durable watermark passes its
// sequence. Whoever finds no fsync in flight becomes the leader and
// syncs once on behalf of every append that completed before the sync
// started, so N committers queued behind one fsync are all released by
// the next.
type groupSync struct {
	mu      sync.Mutex
	cond    *sync.Cond // signaled when synced or err changes; bound to mu in Open
	synced  uint64     // highest append sequence known durable
	syncing bool       // a leader's fsync is in flight
	err     error      // sticky: a failed group sync poisons the DB
}

// waitDurable blocks until an fsync covering append sequence seq has
// completed, coalescing with other committers. Under policies other
// than SyncAlways it returns immediately: durability is deferred by
// configuration. Called with no DB locks held.
func (db *DB[K, V]) waitDurable(seq uint64) error {
	if db.policy != SyncAlways {
		return nil
	}
	gs := &db.gsync
	gs.mu.Lock()
	for gs.synced < seq && gs.err == nil {
		if gs.syncing {
			gs.cond.Wait()
			continue
		}
		// Become the leader: one fsync covers every append so far.
		gs.syncing = true
		gs.mu.Unlock()

		db.mu.RLock()
		f, cover := db.file, db.appendSeq
		closed := db.closed
		db.mu.RUnlock()
		var err error
		if closed {
			err = ErrClosed
		} else if err = f.Sync(); err != nil {
			// A compaction may have swapped the log while we synced and
			// closed the handle we captured. Everything appended before
			// the swap is durable in the new file (its pre-rename sync),
			// so a retired handle failing is not a durability failure.
			db.mu.RLock()
			swapped := db.file != f
			db.mu.RUnlock()
			if swapped {
				err = nil
			} else {
				db.mu.Lock()
				if db.writeErr == nil {
					db.writeErr = err
				}
				db.mu.Unlock()
			}
		}

		gs.mu.Lock()
		gs.syncing = false
		if err != nil {
			gs.err = err
		} else if cover > gs.synced {
			gs.synced = cover
		}
		gs.cond.Broadcast()
	}
	err := gs.err
	durable := gs.synced >= seq
	gs.mu.Unlock()
	if durable {
		return nil
	}
	return serr.Wrap(err, "op", "group sync")
}

// markDurable records that everything appended so far is on disk —
// called after an fsync performed outside the group-commit path
// (Close's final sync, DB.Sync, a compaction's pre-rename sync).
// Callers must hold mu.
func (db *DB[K, V]) markDurable() {
	gs := &db.gsync
	gs.mu.Lock()
	if db.appendSeq > gs.synced {
		gs.synced = db.appendSeq
	}
	gs.cond.Broadcast()
	gs.mu.Unlock()
}
