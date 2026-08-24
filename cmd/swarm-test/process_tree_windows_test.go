//go:build windows

package main

import (
	"os/exec"
	"testing"

	"golang.org/x/sys/windows"
)

func TestPrepareChildProcessTreeCreatesWindowsProcessGroup(t *testing.T) {
	command := exec.Command("cmd.exe", "/c", "exit", "0")
	if err := prepareChildProcessTree(command); err != nil {
		t.Fatal(err)
	}
	if command.SysProcAttr == nil || command.SysProcAttr.CreationFlags&windows.CREATE_NEW_PROCESS_GROUP == 0 {
		t.Fatalf("process attributes = %+v, want CREATE_NEW_PROCESS_GROUP", command.SysProcAttr)
	}
}
