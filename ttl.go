package btypedb

import (
	"errors"
	"time"

	"github.com/rohanthewiz/serr"
)

// sweepBatchLimit caps how many expired keys one sweep transaction
// removes, bounding the writer pause and the commit batch size.
const sweepBatchLimit = 512

// sweepLoop periodically removes expired keys. Expiry is already
// enforced lazily at read time; the sweeper reclaims the memory and
// logs the deletions so compaction and replay converge on a clean state.
func (db *DB[K, V]) sweepLoop() {
	defer close(db.sweepDone)
	ticker := time.NewTicker(db.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-db.stopSweep:
			return
		case <-ticker.C:
			if err := db.sweepExpired(); err != nil && !errors.Is(err, ErrClosed) {
				db.mu.Lock()
				if db.sweepErr == nil {
					db.sweepErr = serr.Wrap(err, "op", "expiry sweep")
				}
				db.mu.Unlock()
			}
		}
	}
}

// sweepExpired deletes up to sweepBatchLimit keys whose deadline has
// passed, as one atomic transaction. The candidate scan is lock-light;
// each key's deadline is rechecked inside the transaction in case it
// was rewritten in between.
func (db *DB[K, V]) sweepExpired() error {
	now := time.Now().UnixNano()

	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil
	}
	var keys []K
	for e := range db.state.exp.All() {
		if e.at > now || len(keys) >= sweepBatchLimit {
			break
		}
		keys = append(keys, e.key)
	}
	db.mu.RUnlock()
	if len(keys) == 0 {
		return nil
	}

	return db.Update(func(tx *Tx[K, V]) error {
		for _, k := range keys {
			dl, ok := tx.state.ttl.Get(k)
			if !ok || dl > now {
				continue // deadline moved or cleared since the scan
			}
			if _, err := tx.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}
