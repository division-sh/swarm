//go:build windows

package main

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/windows"
)

func prepareChildProcessTree(cmd *exec.Cmd) error {
	if cmd == nil {
		return fmt.Errorf("child command is required")
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= windows.CREATE_NEW_PROCESS_GROUP
	return nil
}

func signalChildProcessTree(cmd *exec.Cmd, signal os.Signal) error {
	if cmd == nil || cmd.Process == nil {
		return fmt.Errorf("started child process tree is required")
	}
	if _, ok := signal.(syscall.Signal); !ok {
		return fmt.Errorf("unsupported child process tree signal %T", signal)
	}
	if err := windows.GenerateConsoleCtrlEvent(windows.CTRL_BREAK_EVENT, uint32(cmd.Process.Pid)); err != nil {
		return fmt.Errorf("signal process group %d: %w", cmd.Process.Pid, err)
	}
	return nil
}
