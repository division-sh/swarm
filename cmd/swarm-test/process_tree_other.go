//go:build !aix && !darwin && !dragonfly && !freebsd && !hurd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package main

import (
	"fmt"
	"os"
	"os/exec"
)

func prepareChildProcessTree(*exec.Cmd) error {
	return fmt.Errorf("test process-tree supervision is unsupported on this platform")
}

func signalChildProcessTree(*exec.Cmd, os.Signal) error {
	return fmt.Errorf("test process-tree supervision is unsupported on this platform")
}
