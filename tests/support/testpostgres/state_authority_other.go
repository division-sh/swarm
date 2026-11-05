//go:build !windows && !aix && !darwin && !dragonfly && !freebsd && !hurd && !illumos && !linux && !netbsd && !openbsd && !solaris

package testpostgres

import (
	"fmt"
	"os"
)

func validateStateAccess(path string, _ os.FileInfo) error {
	return fmt.Errorf("Postgres service state authority validation is unsupported on this platform for %q", path)
}
