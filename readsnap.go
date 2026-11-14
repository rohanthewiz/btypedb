package btypedb

import (
	"cmp"
	"math"
	"sync/atomic"
)

// Shared read snapshots.
//
// A read-only transaction needs a frozen view of the head state. Taking
// a private COW copy per transaction is O(1) but not free: Copy and
// Release mutate the trees' shared bookkeeping, so both run under db.mu
// held exclusively, and a burst of one-shot reads (one View per point
// lookup) serializes on that mutex twice per read and allocates the
// copied tree headers each time.
//
// Instead, every read-only transaction begun while the head is
// unchanged shares one copy. The DB caches it in readSnap; a reader
// takes a reference with one atomic add and no lock, and the copy is
// released when its last holder lets go.
//
//	              Begin, slow path (under mu): copy the head, refs = 2
//	         ┌─────────────────────────────────────────────────────┐
//	         │                                                     ▼
//	   [nothing cached] ◄── dropReadSnap ───────────────────── [cached]
//	                        (any head mutation, under mu)      refs = 1 + readers
//	                               │                           Begin/Rollback: ±1,
//	                               ▼                           no lock (fast path)
//	                         [uncached]  refs = readers
//	                               │ last holder: refs 0 → deadRefs
//	                               ▼
//	                         [released]  trees released under mu
//
// Sharing one copy across goroutines is sound because reads never
// modify a tree. btype's read paths (Get, the non-Mut iterators) only
// load nodes, and DB.Get and DB.All already read the head from many
// goroutines at once under mu.RLock. A read-only transaction's state is
// never written: every mutator checks writable first, and a read-only
// savepoint takes no copy (see Savepoint).
//
// Freshness: every change to the head (a commit's pointer swap, a direct
// Set or Delete, CreateIndex, DropIndex, Close) calls dropReadSnap under
// mu before it mutates, and a new cache entry is installed only under
// mu, so a Begin that starts after a write has returned sees that write.
// A Begin that overlaps a write may get the snapshot from just before it,
// which is the same linearizable outcome a private copy gave.
//
// Dropping before mutating, rather than after, keeps direct writes
// cheap. While the cache holds a copy, the head's nodes are shared, so
// a mutation must copy its root-to-leaf path. Dropping the cache's
// reference first, when no reader holds the copy, returns those nodes
// to sole ownership and the write proceeds in place as it did before
// read snapshots were shared.

// deadRefs marks a released snapshot's reference count. It is far
// enough below zero that no number of racing acquire attempts (each
// adds one, sees a non-positive result, and subtracts it back) can
// carry it up to a value that looks live.
const deadRefs = math.MinInt64 / 2

// sharedRead is one read-only snapshot of the head, shared by every
// read-only transaction begun while the head stayed unchanged.
type sharedRead[K cmp.Ordered, V any] struct {
	state *dbState[K, V]

	// refs counts holders: the DB's cache slot while the snapshot is
	// cached, plus each open read-only transaction. It reaches 0 only
	// once the snapshot is uncached and every reader has left; the
	// goroutine that then moves it from 0 to deadRefs releases state.
	refs atomic.Int64
}

// acquire takes a reference, failing if the snapshot is already dead.
//
// A plain Add instead of a compare-and-swap loop keeps the fast path at
// one atomic add that never retries under contention. The only subtle
// case is
// the window in which a releaser has taken refs to 0 but not yet marked
// it dead: an acquire landing there sees 1, keeps its reference, and
// the releaser's CompareAndSwap(0, deadRefs) then fails, so ownership of
// the final release passes to the acquirer. The snapshot cannot have
// been released yet in that window — release happens only after the
// CAS succeeds — so the acquirer's view is intact.
func (s *sharedRead[K, V]) acquire() bool {
	if s.refs.Add(1) > 0 {
		return true
	}
	s.refs.Add(-1) // dead: undo, never resurrect
	return false
}

// unref drops a reference and reports whether the caller became
// responsible for releasing the snapshot's trees. The caller must then
// release them under db.mu (Copy and Release share bookkeeping).
func (s *sharedRead[K, V]) unref() bool {
	return s.refs.Add(-1) == 0 && s.refs.CompareAndSwap(0, deadRefs)
}

// beginRead starts a read-only transaction on the shared snapshot,
// creating it first when no current one is cached.
func (db *DB[K, V]) beginRead() (*Tx[K, V], error) {
	// Fast path: no lock. A snapshot loaded here was current when it was
	// cached; if a writer drops it between this load and the acquire,
	// the Begin linearizes before that write, as it would have had it
	// taken mu a moment earlier.
	if s := db.readSnap.Load(); s != nil && s.acquire() {
		return &Tx[K, V]{db: db, state: s.state, shared: s}, nil
	}

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil, ErrClosed
	}
	// Recheck under mu: another reader may have installed one while we
	// waited. A cached snapshot always holds the cache's own reference
	// while mu is held (only dropReadSnap removes it, under mu), so
	// acquire cannot fail here; the check stays for safety.
	s := db.readSnap.Load()
	if s == nil || !s.acquire() {
		s = &sharedRead[K, V]{state: db.state.copy()}
		s.refs.Store(2) // the cache slot and this transaction
		db.readSnap.Store(s)
	}
	db.mu.Unlock()
	// No baseVersion: only an OCC Commit of a writable transaction reads
	// it, and a read-only Commit just releases.
	return &Tx[K, V]{db: db, state: s.state, shared: s}, nil
}

// dropReadSnap uncaches the shared read snapshot so the next read-only
// Begin copies the head afresh. Every mutation of db.state, in place or
// by pointer swap, must call it first. The caller holds db.mu.
func (db *DB[K, V]) dropReadSnap() {
	if s := db.readSnap.Swap(nil); s != nil && s.unref() {
		s.state.release() // mu already held
	}
}

// releaseRead returns a read-only transaction's reference to the shared
// snapshot, releasing the trees when it was the last one. The lock is
// taken only in that case, once per snapshot, not once per reader.
func (tx *Tx[K, V]) releaseRead() {
	if tx.shared.unref() {
		tx.db.releaseState(tx.state)
	}
}
