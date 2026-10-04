//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package workspace

import (
	"errors"
	"os/exec"
	"syscall"
)

func configureAdmissionProbeProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killAdmissionProbeProcess(cmd *exec.Cmd) error {
	err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}
