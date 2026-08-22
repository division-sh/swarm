//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package packartifact

import (
	"os"

	"golang.org/x/sys/unix"
)

func lockProjectPackFile(file *os.File, exclusive bool) error {
	mode := unix.LOCK_SH
	if exclusive {
		mode = unix.LOCK_EX
	}
	return unix.Flock(int(file.Fd()), mode)
}

func unlockProjectPackFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}
