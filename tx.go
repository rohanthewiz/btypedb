package btypedb

import (
	"cmp"
	"encoding/binary"
	"errors"
	"iter"
	"time"

	"github.com/rohanthewiz/serr"
)

// ErrTxClosed is returned by operations on a committed or rolled-back transaction.
var ErrTxClosed = errors.New("btypedb: transaction has already been committed or rolled back")

// ErrTxNotWritable is returned when writing through a read-only transaction.
var ErrTxNotWritable = errors.New("btypedb: transaction is read-only")

// Tx is a transaction over an O(1) copy-on-write snapshot of the entire
// database state — data, TTLs, and secondary indexes together.
//
// A read-only transaction sees the database exactly as it was at Begin,
// no matter what commits afterward, and reads without taking any locks.
// A writable transaction works on a private copy — its changes are
// invisible to others until Commit atomically publishes them — and holds
// the single-writer lock for its lifetime, so writable transactions
// serialize with each other and with direct DB.Set/Delete calls.
//
// Every transaction must be finished with Commit or Rollback to release
// its snapshot. A Tx is not safe for concurrent use by multiple
// goroutines (the DB itself is).
type Tx[K cmp.Ordered, V any] struct {
	db       *DB[K, V]
	state    *dbState[K, V] // private COW snapshot of all trees
	writable bool
	pending  []byte // framed WAL records accumulated by this tx
	nops     uint64
	done     bool
}

// Begin starts a transaction. A writable transaction blocks until any
// in-flight writable transaction or direct write completes.
func (db *DB[K, V]) Begin(writable bool) (*Tx[K, V], error) {
	if writable {
		db.writerMu.Lock()
	}
	db.mu.Lock()
	var err error
	if writable {
		err = db.canWrite()
	} else if db.closed {
		err = ErrClosed
	}
	if err != nil {
		db.mu.Unlock()
		if writable {
			db.writerMu.Unlock()
		}
		return nil, err
	}
	snap := db.state.copy()
	db.mu.Unlock()
	return &Tx[K, V]{db: db, state: snap, writable: writable}, nil
}

// View runs fn inside a read-only transaction, releasing the snapshot
// when fn returns.
func (db *DB[K, V]) View(fn func(tx *Tx[K, V]) error) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return fn(tx)
}

// Update runs fn inside a writable transaction, committing if fn returns
// nil and rolling back otherwise.
func (db *DB[K, V]) Update(fn func(tx *Tx[K, V]) error) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Commit publishes the transaction's changes: the accumulated records are
// appended to the log in one write (multi-op transactions are framed as an
// atomic batch), fsynced per policy, and the snapshot becomes the live
// state in a single pointer swap. Committing a read-only or empty
// transaction just releases the snapshot.
func (tx *Tx[K, V]) Commit() error {
	if tx.done {
		return ErrTxClosed
	}
	tx.done = true
	db := tx.db

	if !tx.writable {
		db.releaseState(tx.state)
		return nil
	}
	defer db.writerMu.Unlock()

	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.canWrite(); err != nil {
		tx.state.release()
		return err
	}
	if tx.nops == 0 {
		tx.state.release()
		return nil
	}

	// A single op replays atomically on its own; a multi-op tx gets a
	// batch header so replay applies it all-or-nothing.
	buf := tx.pending
	if tx.nops > 1 {
		var cnt [8]byte
		binary.LittleEndian.PutUint64(cnt[:], tx.nops)
		db.wbuf = appendRecord(db.wbuf[:0], opBatch, nil, cnt[:])
		db.wbuf = append(db.wbuf, tx.pending...)
		buf = db.wbuf
	}
	if _, err := db.file.Write(buf); err != nil {
		db.writeErr = err
		tx.state.release()
		return serr.Wrap(err, "op", "tx log append")
	}
	if db.policy == SyncAlways {
		if err := db.file.Sync(); err != nil {
			db.writeErr = err
			tx.state.release()
			return serr.Wrap(err, "op", "tx log sync")
		}
	}

	old := db.state
	db.state = tx.state
	old.release()
	db.walSize += int64(len(buf))
	db.maybeAutoCompact()
	return nil
}

// Rollback discards the transaction's changes and releases its snapshot.
// Rolling back a finished transaction is a no-op.
func (tx *Tx[K, V]) Rollback() error {
	if tx.done {
		return nil
	}
	tx.done = true
	tx.db.releaseState(tx.state)
	if tx.writable {
		tx.db.writerMu.Unlock()
	}
	return nil
}

// Set stores value under key within the transaction, clearing any TTL.
// The change is visible to this transaction immediately and to others
// after Commit.
func (tx *Tx[K, V]) Set(key K, value V) error {
	return tx.setInternal(key, value, 0)
}

// SetTTL stores value under key with a time-to-live within the
// transaction.
func (tx *Tx[K, V]) SetTTL(key K, value V, ttl time.Duration) error {
	if ttl <= 0 {
		return serr.New("ttl must be positive")
	}
	return tx.setInternal(key, value, time.Now().Add(ttl).UnixNano())
}

func (tx *Tx[K, V]) setInternal(key K, value V, deadline int64) error {
	if tx.done {
		return ErrTxClosed
	}
	if !tx.writable {
		return ErrTxNotWritable
	}
	kb, err := tx.db.keyCodec.Encode(key)
	if err != nil {
		return serr.Wrap(err, "encoding", "key")
	}
	vb, err := tx.db.valCodec.Encode(value)
	if err != nil {
		return serr.Wrap(err, "encoding", "value")
	}
	op := opSet
	if deadline > 0 {
		op, vb = opSetTTL, prependDeadline(deadline, vb)
	}
	tx.pending = appendRecord(tx.pending, op, kb, vb)
	tx.nops++
	tx.state.set(key, value, deadline)
	return nil
}

// Delete removes key within the transaction, reporting whether it was
// visible (present and not expired) in the transaction's view.
func (tx *Tx[K, V]) Delete(key K) (existed bool, err error) {
	if tx.done {
		return false, ErrTxClosed
	}
	if !tx.writable {
		return false, ErrTxNotWritable
	}
	if !tx.state.data.Contains(key) {
		return false, nil
	}
	kb, err := tx.db.keyCodec.Encode(key)
	if err != nil {
		return false, serr.Wrap(err, "encoding", "key")
	}
	visible := !tx.state.expired(key, time.Now().UnixNano())
	tx.pending = appendRecord(tx.pending, opDelete, kb, nil)
	tx.nops++
	tx.state.delete(key)
	return visible, nil
}

// DeleteRange removes every key in [min, max) within the transaction,
// returning how many visible keys were deleted. Keys are encoded before
// anything is mutated, so a codec failure leaves the transaction intact.
func (tx *Tx[K, V]) DeleteRange(min, max K) (int, error) {
	if tx.done {
		return 0, ErrTxClosed
	}
	if !tx.writable {
		return 0, ErrTxNotWritable
	}

	var keys []K
	for k := range tx.state.data.Ascend(min) {
		if k >= max {
			break
		}
		keys = append(keys, k)
	}
	encoded := make([][]byte, len(keys))
	for i, k := range keys {
		kb, err := tx.db.keyCodec.Encode(k)
		if err != nil {
			return 0, serr.Wrap(err, "encoding", "key")
		}
		encoded[i] = kb
	}

	now := time.Now().UnixNano()
	visible := 0
	for i, k := range keys {
		if !tx.state.expired(k, now) {
			visible++
		}
		tx.pending = appendRecord(tx.pending, opDelete, encoded[i], nil)
		tx.nops++
		tx.state.delete(k)
	}
	return visible, nil
}

// Get returns the value stored under key in the transaction's view,
// including this transaction's own uncommitted writes. Expired keys
// read as absent.
func (tx *Tx[K, V]) Get(key K) (value V, ok bool) {
	if tx.done {
		return value, false
	}
	return tx.state.get(key, time.Now().UnixNano())
}

// Contains reports whether key exists unexpired in the transaction's view.
func (tx *Tx[K, V]) Contains(key K) bool {
	_, ok := tx.Get(key)
	return ok
}

// TTL returns the remaining time-to-live for key in the transaction's
// view. ok is false when the key is absent, expired, or has no deadline.
func (tx *Tx[K, V]) TTL(key K) (remaining time.Duration, ok bool) {
	if tx.done {
		return 0, false
	}
	now := time.Now().UnixNano()
	dl, ok := tx.state.ttl.Get(key)
	if !ok || dl <= now {
		return 0, false
	}
	return time.Duration(dl - now), true
}

// Len returns the number of keys in the transaction's view, counting
// expired keys not yet swept.
func (tx *Tx[K, V]) Len() int {
	if tx.done {
		return 0
	}
	return tx.state.data.Len()
}

// All iterates the transaction's view in ascending key order, skipping
// expired keys. Unlike DB.All, no lock is held: the snapshot is
// immutable.
func (tx *Tx[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if tx.done {
			return
		}
		now := time.Now().UnixNano()
		for k, v := range tx.state.data.All() {
			if tx.state.expired(k, now) {
				continue
			}
			if !yield(k, v) {
				return
			}
		}
	}
}

// Keys iterates the transaction's unexpired keys in ascending order.
func (tx *Tx[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		if tx.done {
			return
		}
		now := time.Now().UnixNano()
		for k := range tx.state.data.All() {
			if tx.state.expired(k, now) {
				continue
			}
			if !yield(k) {
				return
			}
		}
	}
}

// Values iterates the transaction's unexpired values in ascending key
// order.
func (tx *Tx[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		if tx.done {
			return
		}
		now := time.Now().UnixNano()
		for k, v := range tx.state.data.All() {
			if tx.state.expired(k, now) {
				continue
			}
			if !yield(v) {
				return
			}
		}
	}
}

// Ascend iterates the transaction's view in ascending order starting at
// the first key >= from, skipping expired keys.
func (tx *Tx[K, V]) Ascend(from K) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if tx.done {
			return
		}
		now := time.Now().UnixNano()
		for k, v := range tx.state.data.Ascend(from) {
			if tx.state.expired(k, now) {
				continue
			}
			if !yield(k, v) {
				return
			}
		}
	}
}
