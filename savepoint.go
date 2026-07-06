package btypedb

import (
	"cmp"
	"errors"
)

// ErrSavepointInvalid is returned when using a savepoint that has been
// released, was destroyed by rolling back to an earlier savepoint, or
// belongs to a different transaction.
var ErrSavepointInvalid = errors.New("btypedb: savepoint is no longer valid for this transaction")

// Savepoint marks a point within a transaction that RollbackTo can
// restore. Like a transaction's own snapshot, the mark is an O(1)
// copy-on-write copy of the entire state — data, TTLs, and secondary
// indexes — plus the length of the write-ahead batch, so rolling back
// is O(1) and the eventual commit logs exactly the surviving writes.
type Savepoint[K cmp.Ordered, V any] struct {
	state      *dbState[K, V] // COW snapshot of the tx state at the mark
	pendingLen int            // tx.pending length at the mark
	nops       uint64
	active     bool
}

// Savepoint captures the transaction's current state. A later
// RollbackTo returns the transaction to this point; Release discards
// the mark while keeping the changes. Savepoints nest: rolling back
// to (or releasing) an earlier savepoint destroys the later ones.
// Any savepoints still outstanding at Commit or Rollback are cleaned
// up with the transaction.
func (tx *Tx[K, V]) Savepoint() (*Savepoint[K, V], error) {
	if tx.done {
		return nil, ErrTxClosed
	}
	tx.db.mu.Lock()
	st := tx.state.copy()
	tx.db.mu.Unlock()
	sp := &Savepoint[K, V]{state: st, pendingLen: len(tx.pending), nops: tx.nops, active: true}
	tx.saves = append(tx.saves, sp)
	return sp, nil
}

// RollbackTo restores the transaction to the state sp captured,
// discarding every change made after it. Savepoints created after sp
// are destroyed; sp itself stays valid, so the transaction can roll
// back to it again.
func (tx *Tx[K, V]) RollbackTo(sp *Savepoint[K, V]) error {
	i, err := tx.findSave(sp)
	if err != nil {
		return err
	}
	tx.db.mu.Lock()
	for _, later := range tx.saves[i+1:] {
		later.state.release()
		later.active = false
	}
	tx.state.release()
	tx.state = sp.state.copy()
	tx.db.mu.Unlock()
	tx.saves = tx.saves[:i+1]
	tx.pending = tx.pending[:sp.pendingLen]
	tx.nops = sp.nops
	return nil
}

// Release discards sp — and every savepoint created after it — while
// keeping all of the transaction's changes.
func (tx *Tx[K, V]) Release(sp *Savepoint[K, V]) error {
	i, err := tx.findSave(sp)
	if err != nil {
		return err
	}
	tx.db.mu.Lock()
	for _, s := range tx.saves[i:] {
		s.state.release()
		s.active = false
	}
	tx.db.mu.Unlock()
	tx.saves = tx.saves[:i]
	return nil
}

// findSave locates sp on the transaction's savepoint stack.
func (tx *Tx[K, V]) findSave(sp *Savepoint[K, V]) (int, error) {
	if tx.done {
		return 0, ErrTxClosed
	}
	if sp == nil || !sp.active {
		return 0, ErrSavepointInvalid
	}
	for i := len(tx.saves) - 1; i >= 0; i-- {
		if tx.saves[i] == sp {
			return i, nil
		}
	}
	return 0, ErrSavepointInvalid
}

// releaseSaves returns every outstanding savepoint snapshot to the COW
// refcounting scheme. The caller must hold db.mu.
func (tx *Tx[K, V]) releaseSaves() {
	for _, sp := range tx.saves {
		sp.state.release()
		sp.active = false
	}
	tx.saves = nil
}
