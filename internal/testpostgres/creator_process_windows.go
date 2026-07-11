//go:build windows

package testpostgres

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"syscall"

	"golang.org/x/sys/windows"
)

func validateCreatorProcessSupport() error { return nil }

func creatorProcessCommand(executable, stateRoot, leaseID string, creator *fileLock) (*exec.Cmd, error) {
	handle := windows.Handle(creator.File().Fd())
	if err := windows.SetHandleInformation(handle, windows.HANDLE_FLAG_INHERIT, windows.HANDLE_FLAG_INHERIT); err != nil {
		return nil, fmt.Errorf("make creator fence handle inheritable: %w", err)
	}
	value := strconv.FormatUint(uint64(handle), 10)
	cmd := exec.CommandContext(context.Background(), executable, "--internal-create", "--state-root", stateRoot, "--lease-id", leaseID, "--creator-fd", value)
	cmd.SysProcAttr = &syscall.SysProcAttr{AdditionalInheritedHandles: []syscall.Handle{syscall.Handle(handle)}}
	return cmd, nil
}
