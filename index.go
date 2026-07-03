package btypedb

import (
	"cmp"
	"iter"
	"slices"
	"time"

	"github.com/rohanthewiz/serr"
)

// CreateIndex registers a secondary sort order over the dataset. The
// compare function orders (key, value) pairs; ties fall back to the
// primary key, so every pair has a stable position. The index is built
// immediately by scanning current data (O(n log n), writers paused) and
// maintained atomically by every subsequent write and transaction.
//
// Index definitions are not persisted — compare functions cannot be —
// so re-register indexes after each Open. Read transactions begun
// before CreateIndex do not see the new index.
func (db *DB[K, V]) CreateIndex(name string, compare func(ak K, av V, bk K, bv V) int) error {
	if name == "" || compare == nil {
		return serr.New("index name and compare function are required")
	}
	db.writerMu.Lock()
	defer db.writerMu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if _, exists := db.state.idx[name]; exists {
		return serr.New("index already exists", "index", name)
	}
	ix := newIndex(name, compare)
	for k, v := range db.state.data.All() {
		ix.tree.Set(ientry[K, V]{k: k, v: v})
	}
	db.state.idx[name] = ix
	return nil
}

// DropIndex removes a secondary index.
func (db *DB[K, V]) DropIndex(name string) error {
	db.writerMu.Lock()
	defer db.writerMu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	ix, exists := db.state.idx[name]
	if !exists {
		return serr.New("no such index", "index", name)
	}
	delete(db.state.idx, name)
	ix.tree.Release()
	return nil
}

// Indexes returns the names of the registered indexes, sorted.
func (db *DB[K, V]) Indexes() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	names := make([]string, 0, len(db.state.idx))
	for name := range db.state.idx {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// AscendIndex iterates every unexpired pair in the named index's order.
// An unknown index yields nothing — check Indexes if unsure. The read
// lock is held for the duration of the loop (see All).
func (db *DB[K, V]) AscendIndex(name string) iter.Seq2[K, V] {
	return db.iterIndex(name, false, false, *new(K), *new(V))
}

// DescendIndex iterates the named index in reverse order.
func (db *DB[K, V]) DescendIndex(name string) iter.Seq2[K, V] {
	return db.iterIndex(name, true, false, *new(K), *new(V))
}

// AscendIndexFrom iterates the named index in order, starting at the
// first entry >= the pivot pair per the index's comparator.
func (db *DB[K, V]) AscendIndexFrom(name string, pivotKey K, pivotValue V) iter.Seq2[K, V] {
	return db.iterIndex(name, false, true, pivotKey, pivotValue)
}

func (db *DB[K, V]) iterIndex(name string, desc, pivoted bool, pk K, pv V) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		db.mu.RLock()
		defer db.mu.RUnlock()
		ix := db.state.idx[name]
		if ix == nil {
			return
		}
		iterIndexTree(db.state, ix, desc, pivoted, pk, pv, yield)
	}
}

// AscendIndex iterates the transaction's view of the named index. The
// snapshot is immutable, so no lock is held.
func (tx *Tx[K, V]) AscendIndex(name string) iter.Seq2[K, V] {
	return tx.iterIndex(name, false, false, *new(K), *new(V))
}

// DescendIndex iterates the transaction's view of the named index in
// reverse order.
func (tx *Tx[K, V]) DescendIndex(name string) iter.Seq2[K, V] {
	return tx.iterIndex(name, true, false, *new(K), *new(V))
}

// AscendIndexFrom iterates the transaction's view of the named index
// starting at the first entry >= the pivot pair.
func (tx *Tx[K, V]) AscendIndexFrom(name string, pivotKey K, pivotValue V) iter.Seq2[K, V] {
	return tx.iterIndex(name, false, true, pivotKey, pivotValue)
}

func (tx *Tx[K, V]) iterIndex(name string, desc, pivoted bool, pk K, pv V) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if tx.done {
			return
		}
		ix := tx.state.idx[name]
		if ix == nil {
			return
		}
		iterIndexTree(tx.state, ix, desc, pivoted, pk, pv, yield)
	}
}

// iterIndexTree walks one index tree in the requested direction,
// skipping expired keys.
func iterIndexTree[K cmp.Ordered, V any](s *dbState[K, V], ix *index[K, V], desc, pivoted bool, pk K, pv V, yield func(K, V) bool) {
	seq := ix.tree.All()
	if desc {
		seq = ix.tree.Backward()
	} else if pivoted {
		seq = ix.tree.Ascend(ientry[K, V]{k: pk, v: pv})
	}
	now := time.Now().UnixNano()
	for e := range seq {
		if s.expired(e.k, now) {
			continue
		}
		if !yield(e.k, e.v) {
			return
		}
	}
}
