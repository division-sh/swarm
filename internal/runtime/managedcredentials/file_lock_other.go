//go:build !aix && !darwin && !dragonfly && !freebsd && !hurd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package managedcredentials

import "fmt"

func lockManagedCredentialFile(lockPath string) (func(), error) {
	return nil, fmt.Errorf("%w: %s", ErrStoreLockUnsupported, lockPath)
}
