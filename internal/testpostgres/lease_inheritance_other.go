//go:build !aix && !darwin && !dragonfly && !freebsd && !hurd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package testpostgres

import (
	"fmt"
	"os/exec"
)

func inheritFileLock(*exec.Cmd, *fileLock) error {
	return fmt.Errorf("test run lease inheritance is unsupported on this platform")
}
