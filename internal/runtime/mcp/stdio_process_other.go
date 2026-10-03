//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package mcp

import (
	"errors"
	"os/exec"
)

func configureStdioProcess(cmd *exec.Cmd) {}

func killStdioProcess(cmd *exec.Cmd) error { return cmd.Process.Kill() }

func stdioProcessWasKilled(err error) bool {
	var exit *exec.ExitError
	return errors.As(err, &exit) && exit.ExitCode() == -1
}
