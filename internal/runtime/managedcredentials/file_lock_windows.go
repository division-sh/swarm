//go:build windows

package managedcredentials

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func lockManagedCredentialFile(lockPath string) (func(), error) {
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open managed credential lock %s: %w", lockPath, err)
	}
	handle := windows.Handle(lock.Fd())
	overlapped := &windows.Overlapped{}
	flags := uint32(windows.LOCKFILE_EXCLUSIVE_LOCK | windows.LOCKFILE_FAIL_IMMEDIATELY)
	if err := windows.LockFileEx(handle, flags, 0, 1, 0, overlapped); err != nil {
		_ = lock.Close()
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, fmt.Errorf("%w: %s", ErrStoreLocked, lockPath)
		}
		return nil, fmt.Errorf("lock managed credential file %s: %w", lockPath, err)
	}
	return func() {
		_ = windows.UnlockFileEx(handle, 0, 1, 0, overlapped)
		_ = lock.Close()
	}, nil
}
