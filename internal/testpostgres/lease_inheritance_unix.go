//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package testpostgres

import (
	"fmt"
	"os/exec"
)

func inheritFileLock(cmd *exec.Cmd, lock *fileLock) error {
	if cmd == nil || lock == nil || lock.File() == nil {
		return fmt.Errorf("child command and live lease are required")
	}
	cmd.ExtraFiles = append(cmd.ExtraFiles, lock.File())
	return nil
}
