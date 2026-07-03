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
package btypedb

import (
	"cmp"
	"errors"
	"io"
	"iter"
	"os"
	"sync"
	"time"

	"github.com/rohanthewiz/serr"
	"github.com/tidwall/btype"
)

// ErrClosed is returned by operations on a closed database.
var ErrClosed = errors.New("btypedb: database is closed")

// SyncPolicy controls when the write-ahead log is fsynced.
type SyncPolicy int

const (
	// SyncAlways fsyncs after every write. Durable to the last operation;
	// slowest. This is the default.
	SyncAlways SyncPolicy = iota
	// SyncEverySecond fsyncs on a one-second background ticker. A crash
	// may lose up to the last second of writes.
	SyncEverySecond
	// SyncNever leaves syncing to the operating system.
	SyncNever
)

// Option configures a DB at open time.
type Option func(*options)

type options struct {
	syncPolicy       SyncPolicy
	autoCompact      bool
	compactMinSize   int64
	compactGrowthPct int
}

func defaultOptions() options {
	return options{
		autoCompact:      true,
		compactMinSize:   32 << 20, // 32 MB
		compactGrowthPct: 100,
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

	// writerMu serializes writable transactions and direct writes with
	// each other. Lock order: writerMu before mu, always.
	writerMu sync.Mutex

	// compactMu allows only one compaction at a time. It is never held
	// together with writerMu; it nests outside mu like writerMu does.
	compactMu sync.Mutex

	mu         sync.RWMutex
	m          *btype.Map[K, V]
	file       *os.File
	wbuf       []byte // reusable record-encoding buffer, guarded by mu
	walSize    int64  // bytes of valid log on disk
	baseSize   int64  // log size just after the last compaction (or open)
	closed     bool
	compacting bool  // an auto-compaction goroutine is in flight
	writeErr   error // sticky: after a failed log append the DB refuses writes
	syncErr    error // last error from the background syncer, surfaced on Close
	compactErr error // last error from auto-compaction, surfaced on Close

	bgWG     sync.WaitGroup // in-flight auto-compactions
	stopSync chan struct{}
	syncDone chan struct{}
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

	// A leftover temp file means a compaction died before its atomic
	// rename; it was never live, so discard it.
	os.Remove(path + compactSuffix)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, serr.Wrap(err, "path", path)
	}

	m := btype.NewMap[K, V]()
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
			m.Set(key, val)
		case opDelete:
			m.Delete(key)
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
		m:                m,
		file:             f,
		walSize:          validLen,
		baseSize:         validLen,
	}
	if db.policy == SyncEverySecond {
		db.stopSync = make(chan struct{})
		db.syncDone = make(chan struct{})
		go db.backgroundSync()
	}
	return db, nil
}

// Set stores value under key, replacing any existing value.
// The write is appended to the log before it is visible in memory.
// Set blocks while a writable transaction is open; do not call it from
// inside an Update function — use the Tx methods there.
func (db *DB[K, V]) Set(key K, value V) error {
	kb, err := db.keyCodec.Encode(key)
	if err != nil {
		return serr.Wrap(err, "encoding", "key")
	}
	vb, err := db.valCodec.Encode(value)
	if err != nil {
		return serr.Wrap(err, "encoding", "value")
	}

	db.writerMu.Lock()
	defer db.writerMu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.canWrite(); err != nil {
		return err
	}
	if err := db.appendToLog(opSet, kb, vb); err != nil {
		return err
	}
	db.m.Set(key, value)
	return nil
}

// Delete removes key, reporting whether it existed. Deleting an absent
// key is a no-op and writes nothing to the log.
func (db *DB[K, V]) Delete(key K) (existed bool, err error) {
	kb, err := db.keyCodec.Encode(key)
	if err != nil {
		return false, serr.Wrap(err, "encoding", "key")
	}

	db.writerMu.Lock()
	defer db.writerMu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.canWrite(); err != nil {
		return false, err
	}
	if !db.m.Contains(key) {
		return false, nil
	}
	if err := db.appendToLog(opDelete, kb, nil); err != nil {
		return false, err
	}
	db.m.Delete(key)
	return true, nil
}

// Get returns the value stored under key.
func (db *DB[K, V]) Get(key K) (value V, ok bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.m.Get(key)
}

// Contains reports whether key exists.
func (db *DB[K, V]) Contains(key K) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.m.Contains(key)
}

// Len returns the number of stored keys.
func (db *DB[K, V]) Len() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.m.Len()
}

// All iterates every key-value pair in ascending key order. The read
// lock is held for the duration of the loop, so do not call Set, Delete,
// or Close from inside it.
func (db *DB[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		db.mu.RLock()
		defer db.mu.RUnlock()
		for k, v := range db.m.All() {
			if !yield(k, v) {
				return
			}
		}
	}
}

// Ascend iterates in ascending order starting at the first key >= from.
// The same locking caveat as All applies.
func (db *DB[K, V]) Ascend(from K) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		db.mu.RLock()
		defer db.mu.RUnlock()
		for k, v := range db.m.Ascend(from) {
			if !yield(k, v) {
				return
			}
		}
	}
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
	db.bgWG.Wait() // let any in-flight auto-compaction finish or abort

	// Take mu for the final file ops: a manual Compact racing Close may
	// have swapped db.file up until the moment closed was set.
	db.mu.Lock()
	defer db.mu.Unlock()
	var errs []error
	if db.syncErr != nil {
		errs = append(errs, db.syncErr)
	}
	if db.compactErr != nil {
		errs = append(errs, db.compactErr)
	}
	if err := db.file.Sync(); err != nil {
		errs = append(errs, serr.Wrap(err, "op", "final sync"))
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

// releaseSnap returns a transaction snapshot's nodes to the COW
// refcounting scheme. Copy and Release mutate shared bookkeeping, so
// both happen under mu.
func (db *DB[K, V]) releaseSnap(snap *btype.Map[K, V]) {
	db.mu.Lock()
	snap.Release()
	db.mu.Unlock()
}

// appendToLog frames and writes one record, fsyncing per policy.
// Callers must hold mu. On failure the record may be torn on disk; the
// DB goes read-only and the tail is repaired on next open.
func (db *DB[K, V]) appendToLog(op byte, key, val []byte) error {
	db.wbuf = appendRecord(db.wbuf[:0], op, key, val)
	if _, err := db.file.Write(db.wbuf); err != nil {
		db.writeErr = err
		return serr.Wrap(err, "op", "log append")
	}
	if db.policy == SyncAlways {
		if err := db.file.Sync(); err != nil {
			db.writeErr = err
			return serr.Wrap(err, "op", "log sync")
		}
	}
	db.walSize += int64(len(db.wbuf))
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
