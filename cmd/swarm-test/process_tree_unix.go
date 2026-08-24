//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package main

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

func prepareChildProcessTree(cmd *exec.Cmd) error {
	if cmd == nil {
		return fmt.Errorf("child command is required")
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	cmd.SysProcAttr.Pgid = 0
	return nil
}

func signalChildProcessTree(cmd *exec.Cmd, signal os.Signal) error {
	if cmd == nil || cmd.Process == nil {
		return fmt.Errorf("started child process tree is required")
	}
	value, ok := signal.(syscall.Signal)
	if !ok {
		return fmt.Errorf("unsupported child process tree signal %T", signal)
	}
	if err := syscall.Kill(-cmd.Process.Pid, value); err != nil {
		return fmt.Errorf("signal process group %d: %w", cmd.Process.Pid, err)
	}
	return nil
}
