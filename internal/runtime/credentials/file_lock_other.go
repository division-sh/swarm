//go:build !aix && !darwin && !dragonfly && !freebsd && !hurd && !illumos && !linux && !netbsd && !openbsd && !solaris

package credentials

import (
	"errors"
	"fmt"
	"os"
)

func lockCredentialFile(lockPath string) (func(), error) {
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("%w: %s", ErrStoreLocked, lockPath)
		}
		return nil, fmt.Errorf("open credential lock %s: %w", lockPath, err)
	}
	return func() {
		_ = lock.Close()
		_ = os.Remove(lockPath)
	}, nil
}
