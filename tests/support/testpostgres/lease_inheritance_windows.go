//go:build windows

package testpostgres

import (
	"fmt"
	"os/exec"
	"syscall"

	"golang.org/x/sys/windows"
)

func inheritFileLock(cmd *exec.Cmd, lock *fileLock) error {
	if cmd == nil || lock == nil || lock.File() == nil {
		return fmt.Errorf("child command and live lease are required")
	}
	handle := windows.Handle(lock.File().Fd())
	if err := windows.SetHandleInformation(handle, windows.HANDLE_FLAG_INHERIT, windows.HANDLE_FLAG_INHERIT); err != nil {
		return fmt.Errorf("make child lease handle inheritable: %w", err)
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.AdditionalInheritedHandles = append(cmd.SysProcAttr.AdditionalInheritedHandles, syscall.Handle(handle))
	return nil
}
