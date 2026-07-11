//go:build !aix && !darwin && !dragonfly && !freebsd && !hurd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package testpostgres

import (
	"fmt"
	"os/exec"
)

func validateCreatorProcessSupport() error {
	return fmt.Errorf("Postgres test service creator handoff is unsupported on this platform")
}

func creatorProcessCommand(string, string, string, *fileLock) (*exec.Cmd, error) {
	return nil, validateCreatorProcessSupport()
}
