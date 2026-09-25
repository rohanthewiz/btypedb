package btypedb

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/rohanthewiz/serr"
)

// ErrLocked is returned when a database path is already held: by Open
// when another DB has the file open — in another process, or earlier in
// this one without a Close — and by Backup or AcquireLock when the
// destination is a live database.
//
// Two DBs over one file would each replay the log into their own memory
// and then append to it independently, so each would silently miss the
// other's writes and interleave records the next replay has to make
// sense of. Writing over a live file (a backup or restore renamed onto
// its path) is worse: the holder keeps appending to the unlinked old
// inode, and everything it commits afterwards vanishes at the next open.
var ErrLocked = errors.New("database is locked: already open by another DB")

// lockSuffix names the sidecar lock file next to the database file.
//
// The lock lives on a sidecar rather than on the database file because
// compaction writes a fresh file and renames it over the original path.
// A lock held on the old file follows the old inode, so after the first
// compaction a second process opening the path would find the new,
// unlocked inode and get in. The sidecar is never renamed or replaced,
// so its lock stays meaningful for the DB's lifetime.
//
// The sidecar is deliberately left on disk after Close. Deleting it
// would reopen the same race in a new form: a process that opened the
// old sidecar just before the unlink could lock that orphaned inode
// while a third process creates and locks a new file at the path — two
// holders, each believing it is alone.
const lockSuffix = ".lock"

// errLockHeld is the platform layer's signal that the lock is taken;
// acquireFileLock turns it into ErrLocked with context.
var errLockHeld = errors.New("lock held")

// fileLock is an exclusive, advisory, process-lifetime lock on a
// database's sidecar file. The OS drops it when the holding process
// exits for any reason (crash and kill -9 included), so a stale sidecar
// left by a dead process never blocks a later Open — the file's content
// is only a diagnostic, not the lock itself.
type fileLock struct {
	path string
	f    *os.File
	once sync.Once
	err  error
}

// AcquireLock takes the same exclusive lock Open holds on path for the
// life of a DB, failing with ErrLocked if it is taken. It is for code
// that writes a database file without opening it as a DB — a restore
// that assembles a file and renames it into place, say — so that write
// cannot land on a database some process has open. Hold the lock until
// the file is in place, then Close it; the lock is not reentrant, so do
// not Open the path while holding it.
//
// Like Open's, the lock is advisory: it excludes other lock takers
// (every Open, Backup and AcquireLock), not plain file writes.
func AcquireLock(path string) (io.Closer, error) {
	return acquireFileLock(path)
}

// lockPathFor returns the sidecar path guarding dbPath. An existing
// database is resolved through symlinks first so every spelling of it —
// a symlinked file or directory, a relative path — maps to one sidecar;
// otherwise two names for the same file would take two different locks
// and exclude nothing. A path that does not exist yet (first Open) has
// nothing to resolve and is used as given.
func lockPathFor(dbPath string) string {
	if real, err := filepath.EvalSymlinks(dbPath); err == nil {
		dbPath = real
	}
	return dbPath + lockSuffix
}

// acquireFileLock takes the lock guarding dbPath without waiting: if it
// is held, the caller should fail fast with an explanation rather than
// hang behind a program someone left running in another terminal.
func acquireFileLock(dbPath string) (*fileLock, error) {
	lp := lockPathFor(dbPath)
	f, err := tryLockFile(lp)
	if err != nil {
		if errors.Is(err, errLockHeld) {
			flds := []string{"path", dbPath, "lock_file", lp}
			if pid := holderPID(lp); pid != "" {
				flds = append(flds, "holder_pid", pid)
			}
			return nil, serr.Wrap(ErrLocked, flds...)
		}
		return nil, serr.Wrap(err, "op", "lock database", "path", dbPath, "lock_file", lp)
	}

	// Record the holder's PID so the next contender's error can name
	// it. Best effort: the lock is the flock/share-mode state, not these
	// bytes, so a failed write costs only the diagnostic.
	if err := f.Truncate(0); err == nil {
		f.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\n"), 0)
	}
	return &fileLock{path: lp, f: f}, nil
}

// holderPID reads the PID the current holder recorded, or "" if it is
// unreadable (Windows' share mode may refuse the read; the holder may
// not have written it yet).
func holderPID(lockPath string) string {
	b, err := os.ReadFile(lockPath)
	if err != nil {
		return ""
	}
	pid := strings.TrimSpace(string(b))
	if _, err := strconv.Atoi(pid); err != nil {
		return ""
	}
	return pid
}

// Close drops the lock by closing the file (closing releases both a
// flock and a Windows share-mode handle). Idempotent, because DB.Close
// is: a second Close must not fail on an already-closed file.
func (l *fileLock) Close() error {
	l.once.Do(func() {
		if err := l.f.Close(); err != nil {
			l.err = serr.Wrap(err, "op", "release database lock", "lock_file", l.path)
		}
	})
	return l.err
}

// nopLock stands in for the lock where a filesystem double has no real
// paths to lock (the power-loss test FS).
type nopLock struct{}

func (nopLock) Close() error { return nil }
