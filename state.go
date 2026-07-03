package btypedb

import (
	"cmp"

	"github.com/tidwall/btype"
)

// expEntry orders TTL'd keys by deadline so the sweeper can pull the
// soonest expirations off the front.
type expEntry[K cmp.Ordered] struct {
	at  int64 // deadline, unix nanoseconds
	key K
}

// ientry is one (key, value) pair stored in a secondary index tree,
// ordered by the index's user comparator with the primary key as
// tiebreak so every pair has exactly one slot.
type ientry[K cmp.Ordered, V any] struct {
	k K
	v V
}

// index is one secondary ordering over the dataset. The compare
// function is registration-time state (not persisted); the tree is
// derived data, rebuilt from the primary tree at CreateIndex and
// maintained by every write afterward.
type index[K cmp.Ordered, V any] struct {
	name    string
	compare func(ak K, av V, bk K, bv V) int
	tree    *btype.Table[ientry[K, V]]
}

func newIndex[K cmp.Ordered, V any](name string, compare func(ak K, av V, bk K, bv V) int) *index[K, V] {
	return &index[K, V]{
		name:    name,
		compare: compare,
		tree: btype.NewTableOptions(btype.TableOptions[ientry[K, V]]{
			Compare: func(a, b ientry[K, V]) int {
				if c := compare(a.k, a.v, b.k, b.v); c != 0 {
					return c
				}
				return cmp.Compare(a.k, b.k)
			},
		}),
	}
}

// dbState is the complete in-memory state: the primary tree plus every
// derived tree (TTL bookkeeping, secondary indexes). Transactions copy
// the whole struct — each tree copy is O(1) — and a commit publishes it
// with a single pointer swap, so data, TTLs, and indexes always change
// atomically together.
type dbState[K cmp.Ordered, V any] struct {
	data *btype.Map[K, V]
	ttl  *btype.Map[K, int64]      // key -> deadline (unix nanos); mirror of exp
	exp  *btype.Table[expEntry[K]] // (deadline, key), earliest first
	idx  map[string]*index[K, V]
}

func newDBState[K cmp.Ordered, V any]() *dbState[K, V] {
	return &dbState[K, V]{
		data: btype.NewMap[K, V](),
		ttl:  btype.NewMap[K, int64](),
		exp: btype.NewTableOptions(btype.TableOptions[expEntry[K]]{
			Compare: func(a, b expEntry[K]) int {
				if c := cmp.Compare(a.at, b.at); c != 0 {
					return c
				}
				return cmp.Compare(a.key, b.key)
			},
		}),
		idx: map[string]*index[K, V]{},
	}
}

func (s *dbState[K, V]) copy() *dbState[K, V] {
	c := &dbState[K, V]{
		data: s.data.Copy(),
		ttl:  s.ttl.Copy(),
		exp:  s.exp.Copy(),
		idx:  make(map[string]*index[K, V], len(s.idx)),
	}
	for name, ix := range s.idx {
		c.idx[name] = &index[K, V]{name: ix.name, compare: ix.compare, tree: ix.tree.Copy()}
	}
	return c
}

func (s *dbState[K, V]) release() {
	s.data.Release()
	s.ttl.Release()
	s.exp.Release()
	for _, ix := range s.idx {
		ix.tree.Release()
	}
}

// set upserts a pair across every tree. A deadline of 0 means no expiry;
// any previous deadline is cleared either way (a plain Set removes TTL).
func (s *dbState[K, V]) set(k K, v V, deadline int64) {
	prev, replaced := s.data.Set(k, v)
	if replaced {
		for _, ix := range s.idx {
			ix.tree.Delete(ientry[K, V]{k: k, v: prev})
		}
	}
	if oldDL, had := s.ttl.Delete(k); had {
		s.exp.Delete(expEntry[K]{at: oldDL, key: k})
	}
	if deadline > 0 {
		s.ttl.Set(k, deadline)
		s.exp.Set(expEntry[K]{at: deadline, key: k})
	}
	for _, ix := range s.idx {
		ix.tree.Set(ientry[K, V]{k: k, v: v})
	}
}

// delete physically removes a pair from every tree, reporting whether it
// was present at all (expired entries included).
func (s *dbState[K, V]) delete(k K) (prev V, present bool) {
	prev, present = s.data.Delete(k)
	if !present {
		return prev, false
	}
	if oldDL, had := s.ttl.Delete(k); had {
		s.exp.Delete(expEntry[K]{at: oldDL, key: k})
	}
	for _, ix := range s.idx {
		ix.tree.Delete(ientry[K, V]{k: k, v: prev})
	}
	return prev, true
}

// liveLen counts keys that are not expired as of now. Every ttl/exp
// entry corresponds to a present data key, so the count is the tree
// size minus the expired prefix of the deadline-ordered exp tree.
func (s *dbState[K, V]) liveLen(now int64) int {
	n := s.data.Len()
	for e := range s.exp.All() {
		if e.at > now {
			break
		}
		n--
	}
	return n
}

// expired reports whether k carries a deadline at or before now.
func (s *dbState[K, V]) expired(k K, now int64) bool {
	if s.ttl.Len() == 0 {
		return false
	}
	dl, ok := s.ttl.Get(k)
	return ok && dl <= now
}

// get returns k's value if present and not expired as of now.
func (s *dbState[K, V]) get(k K, now int64) (V, bool) {
	v, ok := s.data.Get(k)
	if !ok || s.expired(k, now) {
		var zero V
		return zero, false
	}
	return v, true
}
