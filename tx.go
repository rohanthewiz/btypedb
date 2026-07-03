package btypedb

import (
	"cmp"
	"encoding/binary"
	"errors"
	"iter"

	"github.com/rohanthewiz/serr"
	"github.com/tidwall/btype"
)

// ErrTxClosed is returned by operations on a committed or rolled-back transaction.
var ErrTxClosed = errors.New("btypedb: transaction has already been committed or rolled back")

// ErrTxNotWritable is returned when writing through a read-only transaction.
var ErrTxNotWritable = errors.New("btypedb: transaction is read-only")

// Tx is a transaction over an O(1) copy-on-write snapshot of the database.
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
	m        *btype.Map[K, V] // private COW snapshot
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
	snap := db.m.Copy()
	db.mu.Unlock()
	return &Tx[K, V]{db: db, m: snap, writable: writable}, nil
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
// tree in a single pointer swap. Committing a read-only or empty
// transaction just releases the snapshot.
func (tx *Tx[K, V]) Commit() error {
	if tx.done {
		return ErrTxClosed
	}
	tx.done = true
	db := tx.db

	if !tx.writable {
		db.releaseSnap(tx.m)
		return nil
	}
	defer db.writerMu.Unlock()

	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.canWrite(); err != nil {
		tx.m.Release()
		return err
	}
	if tx.nops == 0 {
		tx.m.Release()
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
		tx.m.Release()
		return serr.Wrap(err, "op", "tx log append")
	}
	if db.policy == SyncAlways {
		if err := db.file.Sync(); err != nil {
			db.writeErr = err
			tx.m.Release()
			return serr.Wrap(err, "op", "tx log sync")
		}
	}

	old := db.m
	db.m = tx.m
	old.Release()
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
	tx.db.releaseSnap(tx.m)
	if tx.writable {
		tx.db.writerMu.Unlock()
	}
	return nil
}

// Set stores value under key within the transaction. The change is
// visible to this transaction immediately and to others after Commit.
func (tx *Tx[K, V]) Set(key K, value V) error {
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
	tx.pending = appendRecord(tx.pending, opSet, kb, vb)
	tx.nops++
	tx.m.Set(key, value)
	return nil
}

// Delete removes key within the transaction, reporting whether it
// existed in the transaction's view.
func (tx *Tx[K, V]) Delete(key K) (existed bool, err error) {
	if tx.done {
		return false, ErrTxClosed
	}
	if !tx.writable {
		return false, ErrTxNotWritable
	}
	if !tx.m.Contains(key) {
		return false, nil
	}
	kb, err := tx.db.keyCodec.Encode(key)
	if err != nil {
		return false, serr.Wrap(err, "encoding", "key")
	}
	tx.pending = appendRecord(tx.pending, opDelete, kb, nil)
	tx.nops++
	tx.m.Delete(key)
	return true, nil
}

// Get returns the value stored under key in the transaction's view,
// including this transaction's own uncommitted writes.
func (tx *Tx[K, V]) Get(key K) (value V, ok bool) {
	if tx.done {
		return value, false
	}
	return tx.m.Get(key)
}

// Contains reports whether key exists in the transaction's view.
func (tx *Tx[K, V]) Contains(key K) bool {
	if tx.done {
		return false
	}
	return tx.m.Contains(key)
}

// Len returns the number of keys in the transaction's view.
func (tx *Tx[K, V]) Len() int {
	if tx.done {
		return 0
	}
	return tx.m.Len()
}

// All iterates the transaction's view in ascending key order. Unlike
// DB.All, no lock is held: the snapshot is immutable.
func (tx *Tx[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if tx.done {
			return
		}
		for k, v := range tx.m.All() {
			if !yield(k, v) {
				return
			}
		}
	}
}

// Ascend iterates the transaction's view in ascending order starting at
// the first key >= from.
func (tx *Tx[K, V]) Ascend(from K) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if tx.done {
			return
		}
		for k, v := range tx.m.Ascend(from) {
			if !yield(k, v) {
				return
			}
		}
	}
}
