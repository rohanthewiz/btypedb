//go:build !(darwin || dragonfly || freebsd || linux || netbsd || openbsd || windows)

package btypedb

import "os"

// tryLockFile on platforms with no flock in the standard syscall package
// (solaris, illumos, aix, plan9, wasip1, js) only creates the sidecar and
// takes no lock: Open behaves as it did before locking existed, rather
// than refusing to run. Concurrent opens of one file are unprotected here.
func tryLockFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
}
