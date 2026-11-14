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
// Read-only transactions begun while nothing has changed share one
// snapshot, so Begin and Rollback on that path are a reference-count
// update with no lock and no copy (see readsnap.go); a burst of
// one-shot Views scales with cores.
// A writable transaction works on a private copy — its changes are
// invisible to others until Commit atomically publishes them.
//
// How writable transactions coordinate depends on the mode. Default:
// Begin takes the single-writer lock and holds it until Commit or
// Rollback, so writers fully serialize with each other and with direct
// DB.Set/Delete calls. Concurrent-writes mode (WithConcurrentWrites):
// writers execute lock-free and only Commit serializes, validating the
// write set against commits that landed since Begin — see occ.go.
//
// Every transaction must be finished with Commit or Rollback to release
// its snapshot. A Tx is not safe for concurrent use by multiple
// goroutines (the DB itself is).
type Tx[K cmp.Ordered, V any] struct {
	db       *DB[K, V]
	state    *dbState[K, V] // COW snapshot of all trees: private if writable, shared if read-only
	writable bool
	pending  []byte // framed WAL records accumulated by this tx
	nops     uint64
	saves    []*Savepoint[K, V] // outstanding savepoints, oldest first
	done     bool

	// shared is set on read-only transactions: state then belongs to
	// a snapshot other readers hold too, so the transaction returns a
	// reference at Commit/Rollback instead of releasing the trees, and
	// must never write to state (see readsnap.go).
	shared *sharedRead[K, V]

	// holdsWriter tracks writerMu ownership so Commit and Rollback
	// release exactly what was taken: the whole-lifetime lock from
	// Begin (default mode) or nothing yet (occ mode, where Commit
	// takes it itself). A flag, not a mode check, so the unlock sites
	// cannot drift from the lock sites.
	holdsWriter bool

	// OCC bookkeeping (occ mode only; unused otherwise). ops mirrors
	// pending as decoded, replayable writes; wranges holds the [min,
	// max) claims of DeleteRange calls; baseVersion is the head's
	// commitVersion at Begin, against which Commit validates.
	baseVersion uint64
	ops         []logicalOp[K, V]
	wranges     []keyRange[K]

	// Read-set bookkeeping for serializable validation (occ mode,
	// opt-in via TrackReads). The kv layer cannot see semantic reads —
	// an iterator does not know where its consumer stops, and recording
	// only the keys actually yielded would miss phantoms (the keys a
	// scan did NOT see because they were absent). So the layer above,
	// which knows each read's meaning, records point reads and scan
	// bounds through MarkRead/MarkReadRange, and Commit validates them
	// against every intervening commit's writes.
	trackReads bool
	readKeys   map[K]struct{}
	readRanges []keyRange[K]

	// exclusive marks a transaction that must fully serialize (DDL):
	// its Commit succeeds only if no other commit landed since Begin,
	// and publishes a wildcard ring entry that conflicts every
	// overlapping-in-time transaction out. See MarkExclusive.
	exclusive bool
}

// Begin starts a transaction. In the default mode a writable
// transaction blocks until any in-flight writable transaction or direct
// write completes; in concurrent-writes mode it starts immediately and
// contention is resolved at Commit instead (ErrTxConflict).
func (db *DB[K, V]) Begin(writable bool) (*Tx[K, V], error) {
	return db.begin(writable, false)
}

// begin is Begin with the exclusive escalation UpdateExclusive needs:
// exclusive takes the writer lock for the transaction's whole lifetime
// even in concurrent-writes mode, freezing the head so Commit cannot
// conflict. In default mode exclusive changes nothing — every writable
// transaction already holds the lock for its lifetime.
func (db *DB[K, V]) begin(writable, exclusive bool) (*Tx[K, V], error) {
	if !writable {
		// Read-only transactions share one snapshot per head version
		// instead of each copying the head under mu (readsnap.go).
		return db.beginRead()
	}
	takeWriter := writable && (!db.occ || exclusive)
	if takeWriter {
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
		if takeWriter {
			db.writerMu.Unlock()
		}
		return nil, err
	}
	snap := db.state.copy()
	base := db.commitVersion
	db.mu.Unlock()
	tx := &Tx[K, V]{db: db, state: snap, writable: writable, holdsWriter: takeWriter, baseVersion: base}
	// The wildcard publish matters even though the held lock froze the
	// head: transactions that began before us can still commit after we
	// release, and they must fail validation against our entry.
	tx.exclusive = exclusive && db.occ
	return tx, nil
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
//
// In the default mode Begin(true) takes the single-writer lock (writerMu)
// and holds it for the transaction's lifetime; the lock is released only
// by Commit or Rollback. An error RETURN from fn is handled below, but a
// PANIC would otherwise unwind straight past this function with neither
// Commit nor Rollback ever running — leaving writerMu held forever and
// wedging every future write process-wide (reads keep working on their
// own snapshots, which masks the hang). The deferred recover closes that
// hole: it rolls back to release the lock, then re-panics so the caller
// still sees the original failure and its stack. In concurrent-writes
// mode no lock is held across fn, but the recover still matters — it
// releases the snapshot's COW pins. Rollback is idempotent on tx.done,
// so this is a harmless no-op on the normal commit/return paths and
// composes safely with any recover a caller layers on top (e.g. bytdb's
// WriteTxn).
func (db *DB[K, V]) Update(fn func(tx *Tx[K, V]) error) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		}
	}()
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// UpdateExclusive runs fn inside a writable transaction that fully
// serializes with every other write, whichever mode the store is in.
// In concurrent-writes mode it takes the writer lock for the
// transaction's whole lifetime — no commit can land while fn runs, so
// the commit can never fail with ErrTxConflict — and publishes a
// wildcard validation entry, so every transaction that overlapped it
// in time conflicts and retries. In default mode it is exactly Update.
//
// This is the guaranteed-progress escalation for work that must
// serialize (schema changes): optimism first via MarkExclusive, and
// when contention keeps winning, one UpdateExclusive run that cannot
// lose. Keep fn short — every writer in the process waits behind it.
func (db *DB[K, V]) UpdateExclusive(fn func(tx *Tx[K, V]) error) error {
	tx, err := db.begin(true, true)
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		}
	}()
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// TrackReads opts this transaction into serializable validation: point
// reads and scan bounds subsequently recorded with MarkRead and
// MarkReadRange are checked at Commit against every commit that landed
// since Begin — any overlap fails with ErrTxConflict, exactly like a
// write-write conflict. Only meaningful for writable transactions in
// concurrent-writes mode; elsewhere it is a no-op (default mode is
// already serializable, and a read-only snapshot needs no validation).
//
// The caller decides what a "read" is; that is the point. Recording a
// scan as its semantic [min, max) bound is what catches phantoms — the
// keys the scan did not see — which per-key instrumentation never can.
// Reads survive savepoint rollback deliberately: results produced
// before a RollbackTo may already have escaped to the application, so
// they stay part of what Commit vouches for.
func (tx *Tx[K, V]) TrackReads() {
	if tx.done || !tx.writable || !tx.db.occ {
		return
	}
	tx.trackReads = true
}

// MarkRead records a point read (a Get or existence probe, hit or
// miss — absence is information too) for serializable validation.
// A no-op unless the transaction called TrackReads.
func (tx *Tx[K, V]) MarkRead(key K) {
	if !tx.trackReads || tx.done {
		return
	}
	if tx.readKeys == nil {
		tx.readKeys = map[K]struct{}{}
	}
	tx.readKeys[key] = struct{}{}
}

// MarkReadRange records a scan over [min, max) for serializable
// validation. Record the bounds actually scanned, not the keys seen:
// the interval claim is what makes a concurrent insert into the gap —
// a phantom — a conflict. A no-op unless the transaction called
// TrackReads.
func (tx *Tx[K, V]) MarkReadRange(min, max K) {
	if !tx.trackReads || tx.done {
		return
	}
	tx.readRanges = append(tx.readRanges, keyRange[K]{min: min, max: max})
}

// MarkExclusive declares that this transaction must fully serialize
// with every other write — the schema-change contract. Its Commit
// succeeds only if NO other commit landed since Begin (any commit at
// all fails it with ErrTxConflict, no validation attempted: work like
// an index backfill depends on every key of its snapshot staying put,
// which per-key write sets cannot express). When it does publish, its
// ring entry is a wildcard, so every transaction that overlapped it in
// time — whatever keys it touched — conflicts and retries against the
// new schema. A no-op outside concurrent-writes mode, where writers
// already fully serialize.
func (tx *Tx[K, V]) MarkExclusive() {
	if tx.done || !tx.writable || !tx.db.occ {
		return
	}
	tx.exclusive = true
}

// Commit publishes the transaction's changes: the accumulated records are
// appended to the log in one write (multi-op transactions are framed as an
// atomic batch), fsynced per policy, and the snapshot becomes the live
// state in a single pointer swap. Committing a read-only or empty
// transaction just releases the snapshot.
//
// In concurrent-writes mode this is where contention resolves. The
// writer lock is taken here (not at Begin) and held only across
// validate → append → swap. If the head moved since Begin, the write
// set is checked against every intervening commit: any overlap fails
// with ErrTxConflict, no overlap replays the buffered ops onto a copy
// of the current head — validation proved the ops commute, so the
// result equals a serial execution after the commits that beat us. The
// WAL append happens inside the lock either way, so the log stays in
// commit order and replay, backup, and replication are unaffected.
// Writable reports whether the transaction accepts writes — the same
// property that decides ErrTxNotWritable. Exposed for layers that route
// around the transaction (e.g. non-transactional sequence allocation)
// and must apply the same read-only guard themselves.
func (tx *Tx[K, V]) Writable() bool { return tx.writable }

func (tx *Tx[K, V]) Commit() error {
	if tx.done {
		return ErrTxClosed
	}
	tx.done = true
	db := tx.db

	if !tx.writable {
		// Read-only savepoints hold no snapshot, so clearing them
		// needs no lock; the shared snapshot's reference goes back
		// without one too, except for its last holder.
		tx.releaseSaves()
		tx.releaseRead()
		return nil
	}

	if !tx.holdsWriter {
		// occ mode: the commit-scoped writer lock. holdsWriter keeps
		// the release paths below identical for both modes.
		db.writerMu.Lock()
		tx.holdsWriter = true
	}
	db.mu.Lock()
	tx.releaseSaves()
	err := db.canWrite()
	if err != nil || tx.nops == 0 {
		tx.state.release()
		db.mu.Unlock()
		db.writerMu.Unlock()
		return err
	}

	if db.occ && db.commitVersion != tx.baseVersion {
		// The head moved while we executed. An exclusive transaction
		// (DDL) gives up immediately — its work assumed a frozen
		// snapshot. Otherwise fetch the commits we missed and validate
		// writes, then reads (serializable opt-in); failure to fetch
		// (ring evicted part of the span) counts as a conflict because
		// we can no longer prove safety.
		entries, ok := db.recent.since(tx.baseVersion, db.commitVersion)
		if tx.exclusive || !ok ||
			occConflicts(entries, tx.ops, tx.wranges) ||
			occReadConflicts(entries, tx.readKeys, tx.readRanges) {
			tx.state.release()
			db.mu.Unlock()
			db.writerMu.Unlock()
			return ErrTxConflict
		}
		// Disjoint: replay our ops onto the current head. tx.state was
		// built from a stale base and would lose the intervening
		// commits if swapped in, so it is discarded, not merged.
		merged := db.state.copy()
		for _, op := range tx.ops {
			if op.del {
				merged.delete(op.key)
			} else {
				merged.set(op.key, op.val, op.deadline)
			}
		}
		tx.state.release()
		tx.state = merged
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
	if err := db.writeLog(buf); err != nil {
		tx.state.release()
		db.mu.Unlock()
		db.writerMu.Unlock()
		return err
	}
	db.dropReadSnap() // before the swap: see readsnap.go on ordering
	old := db.state
	db.state = tx.state
	old.release()
	if db.occ {
		if tx.exclusive {
			// A schema change invalidates every overlapping
			// transaction, not just those touching its keys: they may
			// have read (or be about to write under) the old schema.
			db.pushCommit(commitEntry[K]{wildcard: true})
		} else {
			db.pushCommit(occEntry(tx.ops, tx.wranges))
		}
	}
	seq := db.appendSeq
	db.mu.Unlock()
	db.writerMu.Unlock()

	// Ack only once a group fsync covers this commit (SyncAlways).
	return db.waitDurable(seq)
}

// Rollback discards the transaction's changes and releases its snapshot.
// Rolling back a finished transaction is a no-op.
func (tx *Tx[K, V]) Rollback() error {
	if tx.done {
		return nil
	}
	tx.done = true
	if tx.shared != nil {
		// Read-only: see the matching branch in Commit.
		tx.releaseSaves()
		tx.releaseRead()
		return nil
	}
	tx.db.mu.Lock()
	tx.releaseSaves()
	tx.state.release()
	tx.db.mu.Unlock()
	// holdsWriter, not writable: an occ-mode writable transaction never
	// took writerMu (Commit takes and releases it itself).
	if tx.holdsWriter {
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
	tx.pending, err = appendSealedRecord(tx.pending, tx.db.cipher, op, kb, vb)
	if err != nil {
		return serr.Wrap(err, "op", "seal record")
	}
	tx.nops++
	tx.state.set(key, value, deadline)
	if tx.db.occ {
		// Mirror the write as a decoded, replayable op: pending holds
		// sealed WAL bytes, which cannot be re-applied to trees if the
		// commit has to replay onto a moved head.
		tx.ops = append(tx.ops, logicalOp[K, V]{key: key, val: value, deadline: deadline})
	}
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
	if tx.db.occ {
		tx.ops = append(tx.ops, logicalOp[K, V]{del: true, key: key})
	}
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
		if tx.db.occ {
			tx.ops = append(tx.ops, logicalOp[K, V]{del: true, key: k})
		}
	}
	if tx.db.occ {
		// Claim the whole interval, not just the keys the snapshot
		// held: a key another transaction inserts into [min, max)
		// concurrently must conflict, or it would silently survive a
		// range delete that logically covers it (the TRUNCATE case).
		// Claimed even when the range was empty — a later write in
		// this tx still publishes the entry.
		tx.wranges = append(tx.wranges, keyRange[K]{min: min, max: max})
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

// Backward iterates the transaction's view in descending key order,
// skipping expired keys.
func (tx *Tx[K, V]) Backward() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if tx.done {
			return
		}
		now := time.Now().UnixNano()
		for k, v := range tx.state.data.Backward() {
			if tx.state.expired(k, now) {
				continue
			}
			if !yield(k, v) {
				return
			}
		}
	}
}

// Descend iterates the transaction's view in descending order starting
// at the last key <= from, skipping expired keys.
func (tx *Tx[K, V]) Descend(from K) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if tx.done {
			return
		}
		now := time.Now().UnixNano()
		for k, v := range tx.state.data.Descend(from) {
			if tx.state.expired(k, now) {
				continue
			}
			if !yield(k, v) {
				return
			}
		}
	}
}

// LiveLen returns the number of unexpired keys in the transaction's
// view. Unlike Len it excludes expired keys not yet swept.
func (tx *Tx[K, V]) LiveLen() int {
	if tx.done {
		return 0
	}
	return tx.state.liveLen(time.Now().UnixNano())
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
