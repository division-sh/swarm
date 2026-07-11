//go:build !aix && !darwin && !dragonfly && !freebsd && !hurd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package testpostgres

import "fmt"

func acquireFileLock(path string, nonblocking bool) (*fileLock, bool, error) {
	return nil, false, fmt.Errorf("Postgres test service locks are unsupported on this platform")
}

func (l *fileLock) Close() error { return nil }
