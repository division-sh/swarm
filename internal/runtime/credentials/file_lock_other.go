//go:build !aix && !darwin && !dragonfly && !freebsd && !hurd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package credentials

import (
	"fmt"
)

func lockCredentialFile(lockPath string) (func(), error) {
	return nil, fmt.Errorf("%w: %s", ErrStoreLockUnsupported, lockPath)
}
