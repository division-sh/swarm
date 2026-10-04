//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package workspace

import (
	"errors"
	"os"
	"os/exec"
)

func configureAdmissionProbeProcess(cmd *exec.Cmd) {}

func killAdmissionProbeProcess(cmd *exec.Cmd) error {
	err := cmd.Process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}
