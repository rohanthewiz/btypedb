package btypedb

import (
	"cmp"
	"errors"
)

// ErrTxConflict is returned by Commit in concurrent-writes mode when the
// transaction's writes overlap a commit that landed after its snapshot
// was taken. The transaction has been rolled back; the caller should
// re-run it from Begin (first committer wins, the loser retries).
// Test with errors.Is.
var ErrTxConflict = errors.New("btypedb: transaction conflicts with a concurrent commit; retry the transaction")

// WithConcurrentWrites enables optimistic concurrency control (OCC) for
// writable transactions. The single-writer lock shrinks from
// transaction-lifetime to commit-time: transactions execute lock-free
// against their private snapshot, and Commit validates against whatever
// landed in the meantime — publishing when the write sets are disjoint,
// failing with ErrTxConflict when they overlap.
//
// Isolation becomes snapshot isolation: every transaction reads a
// consistent snapshot, and of two transactions writing the same key the
// first committer wins. One deliberate strictness beyond classic SI:
// a DeleteRange claims its whole [min, max) interval, so a concurrent
// insert into that interval conflicts even though the keys differ —
// range deletes back TRUNCATE-style operations upstream, and an insert
// silently surviving one is not acceptable.
//
// A transaction can individually upgrade to serializable by recording
// its reads (TrackReads, MarkRead, MarkReadRange) — Commit then also
// fails on read-write overlap — or fully serialize with MarkExclusive /
// UpdateExclusive, the schema-change contract. See those methods.
//
// Off by default: without this option writable transactions serialize
// exactly as before, holding the writer lock from Begin to Commit, and
// ErrTxConflict is never returned.
func WithConcurrentWrites() Option {
	return func(o *options) { o.concurrentWrites = true }
}

// ConcurrentWrites reports whether the store was opened with
// WithConcurrentWrites. Layers above use it to pick strategies that only
// make sense under OCC — e.g. moving hot counters out of transactional
// write sets so they stop manufacturing write-write conflicts.
func (db *DB[K, V]) ConcurrentWrites() bool { return db.occ }

const (
	// occRingCap bounds how many recent commits keep their changed-key
	// sets for validation. A committer whose snapshot predates the
	// oldest retained entry cannot prove itself conflict-free and
	// aborts with ErrTxConflict — long-running write transactions
	// degrade to a retry, never to silent misvalidation.
	occRingCap = 1024

	// occMaxEntryKeys caps the per-commit changed-key list. A commit
	// touching more keys than this is recorded as a wildcard that
	// conflicts with every overlapping-in-time transaction: a bulk
	// load aborting concurrent writers is acceptable, an unbounded
	// ring is not.
	occMaxEntryKeys = 4096
)

// logicalOp is one replayable write. Ops carry decoded keys and values —
// not WAL bytes — so a commit that lost the fast path can re-apply them
// onto the current head with the same state.set/state.delete calls used
// at execution time. Deadlines are absolute (resolved when the op ran),
// so replay is wall-clock-independent.
type logicalOp[K cmp.Ordered, V any] struct {
	del      bool
	key      K
	val      V     // zero value when del
	deadline int64 // 0 = no expiry; else unix nanos, already absolute
}

// keyRange is a half-open [min, max) interval claimed by a DeleteRange.
type keyRange[K cmp.Ordered] struct {
	min, max K
}

// commitEntry summarizes one published commit for conflict validation:
// which keys it wrote and which ranges it claimed. wildcard marks a
// commit too large to track key-by-key; it conflicts with everything.
type commitEntry[K cmp.Ordered] struct {
	version  uint64
	keys     []K
	ranges   []keyRange[K]
	wildcard bool
}

// commitRing is a fixed-capacity circular buffer of the most recent
// commit entries, guarded by db.mu. Every publishing commit pushes
// exactly one entry and versions are contiguous, so the entries for
// versions (base, current] are simply the last (current-base) pushes —
// if that many are still retained.
type commitRing[K cmp.Ordered] struct {
	entries []commitEntry[K] // circular; allocated on first push
	next    int              // slot the next push lands in
	count   int              // live entries, <= occRingCap
}

func (r *commitRing[K]) push(e commitEntry[K]) {
	if r.entries == nil {
		r.entries = make([]commitEntry[K], occRingCap)
	}
	r.entries[r.next] = e
	r.next = (r.next + 1) % len(r.entries)
	if r.count < len(r.entries) {
		r.count++
	}
}

// since returns the entries for versions (base, current], oldest first.
// ok is false when the ring has already evicted part of that span — the
// caller cannot validate and must treat it as a conflict.
func (r *commitRing[K]) since(base, current uint64) (entries []commitEntry[K], ok bool) {
	needed := int(current - base)
	if needed > r.count {
		return nil, false
	}
	out := make([]commitEntry[K], 0, needed)
	// The newest entry sits at next-1; walk back needed slots, then
	// emit forward so callers see commits in the order they landed.
	start := r.next - needed
	if start < 0 {
		start += len(r.entries)
	}
	for i := range needed {
		out = append(out, r.entries[(start+i)%len(r.entries)])
	}
	return out, true
}

// pushCommit records a published commit in the validation ring and bumps
// the commit version. Callers must hold mu; only called in
// concurrent-writes mode.
func (db *DB[K, V]) pushCommit(e commitEntry[K]) {
	db.commitVersion++
	e.version = db.commitVersion
	db.recent.push(e)
}

// occEntry builds the ring entry for a transaction's write set. The
// transaction is finished when this runs, so its slices are taken over
// rather than copied.
func occEntry[K cmp.Ordered, V any](ops []logicalOp[K, V], wranges []keyRange[K]) commitEntry[K] {
	if len(ops) > occMaxEntryKeys {
		return commitEntry[K]{wildcard: true}
	}
	keys := make([]K, len(ops))
	for i, op := range ops {
		keys[i] = op.key
	}
	return commitEntry[K]{keys: keys, ranges: wranges}
}

// occReadConflicts reports whether any intervening commit wrote a key
// or range this transaction read. This is the upgrade from snapshot
// isolation to serializability (classic backward validation): with
// write-write overlap already excluded by occConflicts, proving that no
// intervening commit touched anything we READ means our reads are still
// true on the current head — so replaying our ops after those commits
// equals a serial execution in commit order. Committed transactions'
// reads need no checking in return: they already serialized before us,
// and nothing they read could have included our unpublished writes.
//
// The read set only exists when the layer above opted in via TrackReads
// and recorded its semantic reads (point looks, scan bounds); an empty
// read set keeps plain snapshot-isolation behavior at zero cost.
func occReadConflicts[K cmp.Ordered](entries []commitEntry[K], readKeys map[K]struct{}, readRanges []keyRange[K]) bool {
	if len(readKeys) == 0 && len(readRanges) == 0 {
		return false
	}
	inReadRanges := func(k K) bool {
		for _, r := range readRanges {
			if k >= r.min && k < r.max {
				return true
			}
		}
		return false
	}
	for _, e := range entries {
		if e.wildcard {
			return true
		}
		for _, k := range e.keys {
			if _, hit := readKeys[k]; hit {
				return true
			}
			// A key they wrote inside a range we scanned is a phantom:
			// re-running our scan on the current head would see it.
			if inReadRanges(k) {
				return true
			}
		}
		for _, r := range e.ranges {
			// Their range delete may have removed keys we point-read or
			// keys inside our scan bounds — either way our reads are
			// stale on the current head.
			for k := range readKeys {
				if k >= r.min && k < r.max {
					return true
				}
			}
			for _, or := range readRanges {
				if or.min < r.max && r.min < or.max {
					return true
				}
			}
		}
	}
	return false
}

// occConflicts reports whether any intervening commit overlaps this
// transaction's write set. Overlap is checked in every combination that
// can break replay:
//
//	their key   vs our keys    — classic write-write conflict
//	their key   vs our ranges  — they touched a key inside a range we
//	                             deleted (e.g. inserted under our TRUNCATE)
//	their range vs our keys    — they range-deleted an interval holding
//	                             a key we wrote
//	their range vs our ranges  — overlapping interval claims; the keys
//	                             each side actually touched are usually
//	                             caught above, but treating claims as
//	                             exclusive keeps the argument local: no
//	                             overlap of any kind means our ops
//	                             commute with theirs, full stop
//
// No overlap means replaying our ops after their commits yields the
// same state as a serial execution, which is what Commit then does.
func occConflicts[K cmp.Ordered, V any](entries []commitEntry[K], ops []logicalOp[K, V], wranges []keyRange[K]) bool {
	wkeys := make(map[K]struct{}, len(ops))
	for _, op := range ops {
		wkeys[op.key] = struct{}{}
	}
	inOurRanges := func(k K) bool {
		for _, r := range wranges {
			if k >= r.min && k < r.max {
				return true
			}
		}
		return false
	}
	for _, e := range entries {
		if e.wildcard {
			return true
		}
		for _, k := range e.keys {
			if _, hit := wkeys[k]; hit {
				return true
			}
			if inOurRanges(k) {
				return true
			}
		}
		for _, r := range e.ranges {
			for k := range wkeys {
				if k >= r.min && k < r.max {
					return true
				}
			}
			for _, or := range wranges {
				if or.min < r.max && r.min < or.max {
					return true
				}
			}
		}
	}
	return false
}
