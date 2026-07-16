//go:build windows

package pythonmodule

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func lockArtifactMutation(lockPath string) (func(), error) {
	if info, err := os.Lstat(lockPath); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("artifact lock must be a regular file")
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("inspect artifact lock: %w", err)
	}
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open artifact lock: %w", err)
	}
	info, err := lock.Stat()
	if err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("stat artifact lock: %w", err)
	}
	if !info.Mode().IsRegular() {
		_ = lock.Close()
		return nil, fmt.Errorf("artifact lock must be a regular file")
	}
	handle := windows.Handle(lock.Fd())
	overlapped := &windows.Overlapped{}
	if err := windows.LockFileEx(handle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, overlapped); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("acquire artifact lock: %w", err)
	}
	return func() {
		_ = windows.UnlockFileEx(handle, 0, 1, 0, overlapped)
		_ = lock.Close()
	}, nil
}
