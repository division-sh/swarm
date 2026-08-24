//go:build windows

package testpostgres

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestWindowsLeaseInheritanceAddsExactHandle(t *testing.T) {
	lock, acquired, err := acquireFileLock(filepath.Join(t.TempDir(), "lease.lock"), false)
	if err != nil || !acquired {
		t.Fatalf("acquire lease: acquired=%v err=%v", acquired, err)
	}
	defer lock.Close()
	cmd := exec.Command("cmd.exe", "/c", "exit", "0")
	if err := inheritFileLock(cmd, lock); err != nil {
		t.Fatal(err)
	}
	if cmd.SysProcAttr == nil || len(cmd.SysProcAttr.AdditionalInheritedHandles) != 1 {
		t.Fatalf("inherited handles = %+v, want exact lease handle", cmd.SysProcAttr)
	}
	if got, want := uintptr(cmd.SysProcAttr.AdditionalInheritedHandles[0]), lock.File().Fd(); got != want {
		t.Fatalf("inherited handle = %d, want %d", got, want)
	}
}
