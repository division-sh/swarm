//go:build !aix && !darwin && !dragonfly && !freebsd && !hurd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package packartifact

import (
	"fmt"
	"os"
)

func lockProjectPackFile(_ *os.File) error {
	return fmt.Errorf("project pack transactions are unsupported on this platform")
}

func unlockProjectPackFile(_ *os.File) error {
	return nil
}
