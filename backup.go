package btypedb

import (
	"io"

	"github.com/rohanthewiz/serr"
)

// backupSuffix names the temp file Backup streams into before its
// atomic rename, mirroring the compaction dance so a crash mid-backup
// never leaves a plausible-looking partial backup at the destination.
const backupSuffix = ".backup-tmp"

// BackupTo streams a consistent point-in-time copy of the database file
// to w, returning the byte count written. The copy is a valid database
// file: opening it yields every transaction committed before BackupTo
// was called, each one whole (batch framing keeps multi-op transactions
// atomic in the copy, exactly as in crash recovery).
//
// Writers are never blocked: the log is append-only, so the bytes below
// the captured length are immutable and are read while commits keep
// appending past them. Only compaction is held off for the duration,
// since it swaps the underlying file out. Commits that land after the
// length is captured are simply not in the copy — take the next backup
// for those. The copy may also include the very newest commits before
// their fsync completes, which only ever makes the backup a superset of
// what a simultaneous power cut would have preserved.
//
// BackupTo works even after a failed append has made the database
// read-only (the sticky-error state): the captured length stops short
// of the torn record, so this is precisely the moment a backup is most
// worth taking.
func (db *DB[K, V]) BackupTo(w io.Writer) (int64, error) {
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return 0, ErrClosed
	}
	f := db.file
	n := db.walSize
	db.mu.Unlock()

	copied, err := io.Copy(w, io.NewSectionReader(f, 0, n))
	if err != nil {
		return copied, serr.Wrap(err, "op", "backup copy", "wanted", itoa64(n), "copied", itoa64(copied))
	}
	return copied, nil
}

// Backup writes a consistent point-in-time copy of the database to
// destPath, atomically: the copy streams into destPath + ".backup-tmp",
// is fsynced, and only then renamed into place, so destPath either
// holds a complete backup or whatever it held before. See BackupTo for
// the consistency and concurrency guarantees. Restoring is just opening
// the backup file with Open.
func (db *DB[K, V]) Backup(destPath string) error {
	tmpPath := destPath + backupSuffix
	tmp, err := db.fs.Create(tmpPath)
	if err != nil {
		return serr.Wrap(err, "path", tmpPath)
	}
	discard := func() {
		tmp.Close()
		db.fs.Remove(tmpPath)
	}

	if _, err := db.BackupTo(tmp); err != nil {
		discard()
		return err
	}
	if err := tmp.Sync(); err != nil {
		discard()
		return serr.Wrap(err, "op", "sync backup")
	}
	if err := db.fs.Rename(tmpPath, destPath); err != nil {
		discard()
		return serr.Wrap(err, "op", "swap backup into place")
	}
	db.fs.SyncDir(destPath)
	return tmp.Close()
}
