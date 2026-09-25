package btypedb

import (
	"io"
	"os"
	"path/filepath"
)

// fsys abstracts the filesystem operations the log lifecycle needs —
// locking and opening the log, and the temp-file/rename dance of
// compaction — so tests can inject power-loss faults into any of them.
// realFS is the production implementation.
//
// Lock is on the interface, rather than always hitting the OS, because
// a test double's paths may be purely virtual (powerFS keeps its files
// in a map), and a sidecar created beside them would land in whatever
// directory the test happens to run in.
type fsys interface {
	Lock(path string) (io.Closer, error) // take the exclusive lock guarding the database at path

	OpenFile(path string) (logfile, error) // open the log, creating it if absent
	Create(path string) (logfile, error)   // create or truncate a compaction temp file
	Rename(oldpath, newpath string) error
	Remove(path string) error
	SyncDir(path string) // best-effort fsync of path's parent directory
}

type realFS struct{}

func (realFS) Lock(path string) (io.Closer, error) { return acquireFileLock(path) }

func (realFS) OpenFile(path string) (logfile, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
}

func (realFS) Create(path string) (logfile, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
}

func (realFS) Rename(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }

func (realFS) Remove(path string) error { return os.Remove(path) }

// SyncDir persists a create or rename in path's directory. Best effort:
// directory fsync is not supported everywhere.
func (realFS) SyncDir(path string) {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return
	}
	d.Sync()
	d.Close()
}
