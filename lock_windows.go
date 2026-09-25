//go:build windows

package btypedb

import (
	"errors"
	"os"
	"syscall"
)

// errSharingViolation is ERROR_SHARING_VIOLATION (winerror.h), which the
// standard syscall package does not name.
const errSharingViolation syscall.Errno = 32

// tryLockFile opens (creating) the sidecar with a share mode that admits
// readers but no other writer: Windows enforces share modes at open, so
// holding this handle is the lock, and a second opener asking for write
// access fails with ERROR_SHARING_VIOLATION. This avoids LockFileEx,
// which the standard syscall package does not export (it would pull in
// golang.org/x/sys). FILE_SHARE_READ leaves the recorded PID readable
// for the contender's error message.
func tryLockFile(path string) (*os.File, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	h, err := syscall.CreateFile(p,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		syscall.FILE_SHARE_READ,
		nil,
		syscall.OPEN_ALWAYS,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0)
	if err != nil {
		if errors.Is(err, errSharingViolation) {
			return nil, errLockHeld
		}
		return nil, err
	}
	return os.NewFile(uintptr(h), path), nil
}
