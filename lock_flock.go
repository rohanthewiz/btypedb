//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package btypedb

import (
	"errors"
	"os"
	"syscall"
)

// tryLockFile opens (creating) the sidecar and takes a non-blocking
// exclusive flock on it.
//
// flock rather than fcntl (POSIX record locks): fcntl locks belong to
// the process, so a second Open of the same path in the same process
// would "succeed" and closing any fd on the file would silently drop the
// lock. flock locks belong to the open file description, so a second
// Open in this process conflicts just like one from another process, and
// only closing this fd releases it. Go opens files O_CLOEXEC, so a child
// the embedder spawns does not inherit (and pin) the lock.
//
// flock is advisory and, on some network filesystems (older NFS), may be
// a no-op or node-local; a database on such a mount is not protected.
func tryLockFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	if err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errLockHeld
		}
		return nil, err
	}
	return f, nil
}
