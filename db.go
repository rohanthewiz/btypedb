// Package btypedb is an embedded, pure-Go, memory-resident key-value
// database with disk durability, built on the copy-on-write B-tree
// collections of github.com/tidwall/btype.
//
// The entire dataset lives in a btype.Map; durability comes from an
// append-only write-ahead log that is replayed on open. Keys are ordered
// by their natural (cmp.Ordered) ordering, giving sorted iteration and
// range scans over live data.
//
// Transactions (Begin, View, Update) run against O(1) copy-on-write
// snapshots: readers get a frozen, lock-free view of the database, and a
// writable transaction stages changes privately, committing them with a
// single batched log append and an atomic root swap. Multi-op commits
// are framed in the log so crash recovery applies them all-or-nothing.
//
// Keys may carry a TTL (SetTTL); expired keys become invisible to reads
// immediately and are physically removed by a background sweeper.
// Secondary indexes (CreateIndex) provide additional sort orders over
// the same data, maintained atomically with every commit.
package btypedb

import (
	"cmp"
	"encoding/binary"
	"errors"
	"io"
	"iter"
	"os"
	"sync"
	"time"

	"github.com/rohanthewiz/serr"
)

// ErrClosed is returned by operations on a closed database.
var ErrClosed = errors.New("btypedb: database is closed")

// SyncPolicy controls when the write-ahead log is fsynced.
type SyncPolicy int

const (
	// SyncAlways fsyncs before acknowledging every write, coalescing
	// concurrent committers into shared fsyncs (group commit). Durable
	// to the last acknowledged operation. A write becomes visible to
	// readers when it is applied, slightly before its fsync completes,
	// so a reader can briefly observe a committed write that a power cut
	// in that window would lose; the writer itself is never acknowledged
	// until the data is on disk. This is the default.
	SyncAlways SyncPolicy = iota
	// SyncEverySecond fsyncs on a one-second background ticker. A crash
	// may lose up to the last second of writes.
	SyncEverySecond
	// SyncNever leaves syncing to the operating system.
	SyncNever
)

// logfile is what the DB needs from its write-ahead log file. *os.File
// satisfies it directly; tests inject fault-simulating implementations
// to model power loss (unsynced writes vanishing, records tearing).
type logfile interface {
	io.Reader
	io.Writer
	io.ReaderAt
	io.Closer
	Seek(offset int64, whence int) (int64, error)
	Sync() error
	Truncate(size int64) error
	Stat() (os.FileInfo, error)
}

// Option configures a DB at open time.
type Option func(*options)

type options struct {
	syncPolicy       SyncPolicy
	autoCompact      bool
	compactMinSize   int64
	compactGrowthPct int
	sweepInterval    time.Duration
	fs               fsys // test seam; nil = the real filesystem
}

func defaultOptions() options {
	return options{
		autoCompact:      true,
		compactMinSize:   32 << 20, // 32 MB
		compactGrowthPct: 100,
		sweepInterval:    500 * time.Millisecond,
	}
}

// WithSyncPolicy sets the fsync policy for the write-ahead log.
func WithSyncPolicy(p SyncPolicy) Option {
	return func(o *options) { o.syncPolicy = p }
}

// WithAutoCompact tunes background compaction: the log is rewritten in
// the background once it is at least minSize bytes and has grown
// growthPct percent past its size after the previous compaction.
// Defaults: 32 MB and 100%.
func WithAutoCompact(minSize int64, growthPct int) Option {
	return func(o *options) {
		o.autoCompact = true
		o.compactMinSize = minSize
		o.compactGrowthPct = growthPct
	}
}

// WithAutoCompactDisabled turns off background compaction. Compact can
// still be called manually.
func WithAutoCompactDisabled() Option {
	return func(o *options) { o.autoCompact = false }
}

// WithSweepInterval sets how often the background sweeper physically
// removes expired keys (default 500ms). Zero or negative disables the
// sweeper; expired keys then stay invisible but occupy memory and log
// space until overwritten, deleted, or dropped by compaction.
func WithSweepInterval(d time.Duration) Option {
	return func(o *options) { o.sweepInterval = d }
}

// DB is an embedded key-value store. Keys are kept in sorted order.
// All methods are safe for concurrent use.
type DB[K cmp.Ordered, V any] struct {
	path             string
	keyCodec         Codec[K]
	valCodec         Codec[V]
	policy           SyncPolicy
	autoCompact      bool
	compactMinSize   int64
	compactGrowthPct int
	sweepInterval    time.Duration

	// writerMu serializes writable transactions and direct writes with
	// each other. Lock order: writerMu before mu, always.
	writerMu sync.Mutex

	// compactMu allows only one compaction at a time. It is never held
	// together with writerMu; it nests outside mu like writerMu does.
	compactMu sync.Mutex

	mu         sync.RWMutex
	state      *dbState[K, V]
	file       logfile
	fs         fsys   // filesystem ops (real, or a fault-injecting test double)
	wbuf       []byte // reusable record-encoding buffer, guarded by mu
	walSize    int64  // bytes of valid log on disk
	baseSize   int64  // log size just after the last compaction (or open)
	appendSeq  uint64 // count of log appends; the group-commit watermark unit
	closed     bool
	compacting bool  // an auto-compaction goroutine is in flight
	writeErr   error // sticky: after a failed log append the DB refuses writes
	syncErr    error // last error from the background syncer, surfaced on Close
	compactErr error // last error from auto-compaction, surfaced on Close
	sweepErr   error // last error from the expiry sweeper, surfaced on Close

	// gsync coalesces SyncAlways fsyncs across concurrent committers
	// (group commit). It has its own lock, taken after mu when both are
	// needed; it is never held while acquiring mu.
	gsync groupSync

	bgWG      sync.WaitGroup // in-flight auto-compactions
	stopSync  chan struct{}
	syncDone  chan struct{}
	stopSweep chan struct{}
	sweepDone chan struct{}
}

// Open opens (creating if necessary) the database file at path and
// replays its log into memory. A torn record at the tail — the normal
// result of a crash mid-write — is truncated away; valid data before it
// is preserved.
func Open[K cmp.Ordered, V any](path string, keyCodec Codec[K], valCodec Codec[V], opts ...Option) (*DB[K, V], error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	fs := o.fs
	if fs == nil {
		fs = realFS{}
	}

	// A leftover temp file means a compaction died before its atomic
	// rename; it was never live, so discard it.
	fs.Remove(path + compactSuffix)

	f, err := fs.OpenFile(path)
	if err != nil {
		return nil, serr.Wrap(err, "path", path)
	}
	// Persist the directory entry in case the file was just created:
	// without this, a power cut could drop the file itself even though
	// its contents were fsynced.
	fs.SyncDir(path)

	state := newDBState[K, V]()
	validLen, err := replayLog(f, func(rec walRecord) error {
		key, err := keyCodec.Decode(rec.key)
		if err != nil {
			return serr.Wrap(err, "decoding", "key")
		}
		switch rec.op {
		case opSet:
			val, err := valCodec.Decode(rec.val)
			if err != nil {
				return serr.Wrap(err, "decoding", "value")
			}
			state.set(key, val, 0)
		case opSetTTL:
			if len(rec.val) < ttlPrefixSize {
				return serr.New("malformed ttl record")
			}
			deadline := int64(binary.LittleEndian.Uint64(rec.val))
			val, err := valCodec.Decode(rec.val[ttlPrefixSize:])
			if err != nil {
				return serr.Wrap(err, "decoding", "value")
			}
			state.set(key, val, deadline)
		case opDelete:
			state.delete(key)
		}
		return nil
	})
	if err != nil {
		f.Close()
		return nil, serr.Wrap(err, "path", path)
	}

	// Discard any torn tail, then position for appends.
	if fi, err := f.Stat(); err == nil && fi.Size() > validLen {
		if err := f.Truncate(validLen); err != nil {
			f.Close()
			return nil, serr.Wrap(err, "path", path, "op", "truncate torn tail")
		}
	}
	if _, err := f.Seek(validLen, io.SeekStart); err != nil {
		f.Close()
		return nil, serr.Wrap(err, "path", path)
	}

	db := &DB[K, V]{
		path:             path,
		keyCodec:         keyCodec,
		valCodec:         valCodec,
		policy:           o.syncPolicy,
		autoCompact:      o.autoCompact,
		compactMinSize:   o.compactMinSize,
		compactGrowthPct: o.compactGrowthPct,
		sweepInterval:    o.sweepInterval,
		state:            state,
		file:             f,
		fs:               fs,
		walSize:          validLen,
		baseSize:         validLen,
	}
	db.gsync.cond = sync.NewCond(&db.gsync.mu)
	if db.policy == SyncEverySecond {
		db.stopSync = make(chan struct{})
		db.syncDone = make(chan struct{})
		go db.backgroundSync()
	}
	if db.sweepInterval > 0 {
		db.stopSweep = make(chan struct{})
		db.sweepDone = make(chan struct{})
		go db.sweepLoop()
	}
	return db, nil
}

// Set stores value under key, replacing any existing value and clearing
// any TTL. The write is appended to the log before it is visible in
// memory. Set blocks while a writable transaction is open; do not call
// it from inside an Update function — use the Tx methods there.
func (db *DB[K, V]) Set(key K, value V) error {
	return db.setInternal(key, value, 0)
}

// SetTTL stores value under key with a time-to-live: once ttl elapses
// the key becomes invisible to reads and is later removed by the
// background sweeper. Setting a key again replaces any previous TTL.
func (db *DB[K, V]) SetTTL(key K, value V, ttl time.Duration) error {
	if ttl <= 0 {
		return serr.New("ttl must be positive")
	}
	return db.setInternal(key, value, time.Now().Add(ttl).UnixNano())
}

func (db *DB[K, V]) setInternal(key K, value V, deadline int64) error {
	kb, err := db.keyCodec.Encode(key)
	if err != nil {
		return serr.Wrap(err, "encoding", "key")
	}
	vb, err := db.valCodec.Encode(value)
	if err != nil {
		return serr.Wrap(err, "encoding", "value")
	}
	op := opSet
	if deadline > 0 {
		op, vb = opSetTTL, prependDeadline(deadline, vb)
	}

	db.writerMu.Lock()
	db.mu.Lock()
	err = db.canWrite()
	if err == nil {
		err = db.appendToLog(op, kb, vb)
	}
	if err == nil {
		db.state.set(key, value, deadline)
	}
	seq := db.appendSeq
	db.mu.Unlock()
	db.writerMu.Unlock()
	if err != nil {
		return err
	}
	return db.waitDurable(seq)
}

// TTL returns the remaining time-to-live for key. ok is false when the
// key is absent, already expired, or has no deadline.
func (db *DB[K, V]) TTL(key K) (remaining time.Duration, ok bool) {
	now := time.Now().UnixNano()
	db.mu.RLock()
	defer db.mu.RUnlock()
	dl, ok := db.state.ttl.Get(key)
	if !ok || dl <= now {
		return 0, false
	}
	return time.Duration(dl - now), true
}

// Delete removes key, reporting whether it was visible (present and not
// expired). An expired-but-unswept key is physically removed but
// reported as absent. Deleting a missing key writes nothing to the log.
func (db *DB[K, V]) Delete(key K) (existed bool, err error) {
	kb, err := db.keyCodec.Encode(key)
	if err != nil {
		return false, serr.Wrap(err, "encoding", "key")
	}
	now := time.Now().UnixNano()

	db.writerMu.Lock()
	db.mu.Lock()
	err = db.canWrite()
	if err == nil && !db.state.data.Contains(key) {
		db.mu.Unlock()
		db.writerMu.Unlock()
		return false, nil
	}
	var visible bool
	if err == nil {
		visible = !db.state.expired(key, now)
		err = db.appendToLog(opDelete, kb, nil)
	}
	if err == nil {
		db.state.delete(key)
	}
	seq := db.appendSeq
	db.mu.Unlock()
	db.writerMu.Unlock()
	if err != nil {
		return false, err
	}
	if err := db.waitDurable(seq); err != nil {
		return false, err
	}
	return visible, nil
}

// DeleteRange atomically removes every key in [min, max), returning how
// many visible keys were deleted. It runs as one transaction: a single
// batched log append that replays all-or-nothing.
func (db *DB[K, V]) DeleteRange(min, max K) (int, error) {
	var n int
	err := db.Update(func(tx *Tx[K, V]) error {
		var err error
		n, err = tx.DeleteRange(min, max)
		return err
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Get returns the value stored under key. Expired keys read as absent.
func (db *DB[K, V]) Get(key K) (value V, ok bool) {
	now := time.Now().UnixNano()
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.state.get(key, now)
}

// Contains reports whether key exists and has not expired.
func (db *DB[K, V]) Contains(key K) bool {
	_, ok := db.Get(key)
	return ok
}

// Len returns the number of stored keys. Expired keys are counted until
// the sweeper removes them.
func (db *DB[K, V]) Len() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.state.data.Len()
}

// All iterates every unexpired key-value pair in ascending key order.
// The read lock is held for the duration of the loop, so do not call
// Set, Delete, or Close from inside it.
func (db *DB[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		now := time.Now().UnixNano()
		db.mu.RLock()
		defer db.mu.RUnlock()
		for k, v := range db.state.data.All() {
			if db.state.expired(k, now) {
				continue
			}
			if !yield(k, v) {
				return
			}
		}
	}
}

// Keys iterates every unexpired key in ascending order. The same
// locking caveat as All applies.
func (db *DB[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		now := time.Now().UnixNano()
		db.mu.RLock()
		defer db.mu.RUnlock()
		for k := range db.state.data.All() {
			if db.state.expired(k, now) {
				continue
			}
			if !yield(k) {
				return
			}
		}
	}
}

// Values iterates every unexpired value in ascending key order. The
// same locking caveat as All applies.
func (db *DB[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		now := time.Now().UnixNano()
		db.mu.RLock()
		defer db.mu.RUnlock()
		for k, v := range db.state.data.All() {
			if db.state.expired(k, now) {
				continue
			}
			if !yield(v) {
				return
			}
		}
	}
}

// Ascend iterates in ascending order starting at the first key >= from.
// The same locking caveat as All applies.
func (db *DB[K, V]) Ascend(from K) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		now := time.Now().UnixNano()
		db.mu.RLock()
		defer db.mu.RUnlock()
		for k, v := range db.state.data.Ascend(from) {
			if db.state.expired(k, now) {
				continue
			}
			if !yield(k, v) {
				return
			}
		}
	}
}

// Backward iterates every unexpired key-value pair in descending key
// order. The same locking caveat as All applies.
func (db *DB[K, V]) Backward() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		now := time.Now().UnixNano()
		db.mu.RLock()
		defer db.mu.RUnlock()
		for k, v := range db.state.data.Backward() {
			if db.state.expired(k, now) {
				continue
			}
			if !yield(k, v) {
				return
			}
		}
	}
}

// Descend iterates in descending order starting at the last key <= from.
// The same locking caveat as All applies.
func (db *DB[K, V]) Descend(from K) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		now := time.Now().UnixNano()
		db.mu.RLock()
		defer db.mu.RUnlock()
		for k, v := range db.state.data.Descend(from) {
			if db.state.expired(k, now) {
				continue
			}
			if !yield(k, v) {
				return
			}
		}
	}
}

// LiveLen returns the number of unexpired keys. Unlike Len it excludes
// expired keys the sweeper has not removed yet, at a cost proportional
// to the number of such keys.
func (db *DB[K, V]) LiveLen() int {
	now := time.Now().UnixNano()
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.state.liveLen(now)
}

// Sync forces an fsync of the write-ahead log.
func (db *DB[K, V]) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if err := db.file.Sync(); err != nil {
		return serr.Wrap(err, "op", "sync")
	}
	db.markDurable()
	return nil
}

// Close syncs the log and closes the database. Close is idempotent.
func (db *DB[K, V]) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.mu.Unlock()

	if db.stopSync != nil {
		close(db.stopSync)
		<-db.syncDone
	}
	if db.stopSweep != nil {
		close(db.stopSweep)
		<-db.sweepDone
	}
	db.bgWG.Wait() // let any in-flight auto-compaction finish or abort

	// Take mu for the final file ops: a manual Compact racing Close may
	// have swapped db.file up until the moment closed was set.
	db.mu.Lock()
	defer db.mu.Unlock()
	var errs []error
	for _, bgErr := range []error{db.syncErr, db.compactErr, db.sweepErr} {
		if bgErr != nil {
			errs = append(errs, bgErr)
		}
	}
	if err := db.file.Sync(); err != nil {
		errs = append(errs, serr.Wrap(err, "op", "final sync"))
	} else {
		// Release any group-commit waiters still in flight: their data
		// is on disk now.
		db.markDurable()
	}
	if err := db.file.Close(); err != nil {
		errs = append(errs, serr.Wrap(err, "op", "close file"))
	}
	return errors.Join(errs...)
}

// canWrite reports whether the DB can accept writes. Callers must hold mu.
func (db *DB[K, V]) canWrite() error {
	if db.closed {
		return ErrClosed
	}
	if db.writeErr != nil {
		return serr.Wrap(db.writeErr, "state", "log append previously failed; database is read-only")
	}
	return nil
}

// releaseState returns a snapshot's trees to the COW refcounting scheme.
// Copy and Release mutate shared bookkeeping, so both happen under mu.
func (db *DB[K, V]) releaseState(st *dbState[K, V]) {
	db.mu.Lock()
	st.release()
	db.mu.Unlock()
}

// appendToLog frames and writes one record. Callers must hold mu.
// Under SyncAlways the record is acknowledged by a later waitDurable
// call, made after the locks are released, so concurrent committers
// share fsyncs (group commit).
func (db *DB[K, V]) appendToLog(op byte, key, val []byte) error {
	db.wbuf = appendRecord(db.wbuf[:0], op, key, val)
	return db.writeLog(db.wbuf)
}

// writeLog appends framed bytes to the log, bumping the group-commit
// sequence. Callers must hold mu. On failure the record may be torn on
// disk; the DB goes read-only and the tail is repaired on next open.
func (db *DB[K, V]) writeLog(buf []byte) error {
	if _, err := db.file.Write(buf); err != nil {
		db.writeErr = err
		return serr.Wrap(err, "op", "log append")
	}
	db.appendSeq++
	db.walSize += int64(len(buf))
	db.maybeAutoCompact()
	return nil
}

func (db *DB[K, V]) backgroundSync() {
	defer close(db.syncDone)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-db.stopSync:
			return
		case <-ticker.C:
			db.mu.Lock()
			if !db.closed {
				if err := db.file.Sync(); err != nil && db.syncErr == nil {
					db.syncErr = serr.Wrap(err, "op", "background sync")
				}
			}
			db.mu.Unlock()
		}
	}
}
