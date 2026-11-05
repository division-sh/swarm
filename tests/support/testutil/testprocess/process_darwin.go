package testprocess

import (
	"errors"
	"fmt"
	"os/exec"

	"golang.org/x/sys/unix"
)

func startNative(command *exec.Cmd) error {
	owner, err := Current()
	if err != nil {
		return err
	}
	return startWatched(command, owner)
}

func validateWatched(*exec.Cmd) error { return nil }

func processBirth(pid int) (string, error) {
	info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return "", err
	}
	if info.Proc.P_pid != int32(pid) || info.Proc.P_stat == 5 {
		return "", errors.New("test helper process is not live")
	}
	return fmt.Sprintf("%d:%d", info.Proc.P_starttime.Sec, info.Proc.P_starttime.Usec), nil
}
