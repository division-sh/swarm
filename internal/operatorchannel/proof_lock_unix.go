//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package operatorchannel

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func lockProofFile(path string) (func(), error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open operator channel proof lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, fmt.Errorf("%w: %s", ErrProofStoreLocked, path)
		}
		return nil, fmt.Errorf("lock operator channel proof store: %w", err)
	}
	return func() {
		_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
		_ = file.Close()
	}, nil
}
