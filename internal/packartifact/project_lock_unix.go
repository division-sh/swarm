//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package packartifact

import (
	"os"

	"golang.org/x/sys/unix"
)

func lockProjectPackFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_EX)
}

func unlockProjectPackFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}
