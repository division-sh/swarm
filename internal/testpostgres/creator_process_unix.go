//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package testpostgres

import (
	"context"
	"os"
	"os/exec"
)

func validateCreatorProcessSupport() error { return nil }

func creatorProcessCommand(executable, stateRoot, leaseID string, creator *fileLock) (*exec.Cmd, error) {
	cmd := exec.CommandContext(context.Background(), executable, "--internal-create", "--state-root", stateRoot, "--lease-id", leaseID, "--creator-fd", "3")
	cmd.ExtraFiles = []*os.File{creator.File()}
	return cmd, nil
}
