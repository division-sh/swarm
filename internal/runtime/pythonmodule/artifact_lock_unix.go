//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package pythonmodule

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func lockArtifactMutation(lockPath string) (func(), error) {
	fd, err := unix.Open(lockPath, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open artifact lock: %w", err)
	}
	lock := os.NewFile(uintptr(fd), lockPath)
	info, err := lock.Stat()
	if err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("stat artifact lock: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		_ = lock.Close()
		return nil, fmt.Errorf("artifact lock must be a private regular file")
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("acquire artifact lock: %w", err)
	}
	return func() {
		_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
		_ = lock.Close()
	}, nil
}
