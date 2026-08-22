//go:build windows

package packartifact

import (
	"os"

	"golang.org/x/sys/windows"
)

func lockProjectPackFile(file *os.File, exclusive bool) error {
	flags := uint32(0)
	if exclusive {
		flags = windows.LOCKFILE_EXCLUSIVE_LOCK
	}
	return windows.LockFileEx(windows.Handle(file.Fd()), flags, 0, 1, 0, &windows.Overlapped{})
}

func unlockProjectPackFile(file *os.File) error {
	return windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, &windows.Overlapped{})
}
