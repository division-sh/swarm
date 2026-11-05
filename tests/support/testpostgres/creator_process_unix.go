//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package testpostgres

import (
	"context"
	"os"
	"os/exec"
	"syscall"
)

func validateCreatorProcessSupport() error { return nil }

func restrictCreatorProcessFileMode() {
	// The creator is a dedicated subprocess, so its umask cannot affect the runner.
	syscall.Umask(0o077)
}

func creatorProcessCommand(executable, stateRoot, leaseID string, creator *fileLock) (*exec.Cmd, error) {
	cmd := exec.CommandContext(context.Background(), executable, "--internal-create", "--state-root", stateRoot, "--lease-id", leaseID, "--creator-fd", "3")
	cmd.ExtraFiles = []*os.File{creator.File()}
	return cmd, nil
}
