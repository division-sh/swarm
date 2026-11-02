package testprocess

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

func TestNativeLauncherCreatingThreadMayExit(t *testing.T) {
	root := t.TempDir()
	command := fixtureCommand(t, fixture{Mode: "writer", Root: root})
	started := make(chan error, 1)
	go func() {
		// Exiting this locked goroutine terminates its OS thread. The helper
		// must belong to the shared launcher thread instead.
		runtime.LockOSThread()
		started <- Start(command)
	}()
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	ready(t, root)
	time.Sleep(100 * time.Millisecond)
	if !alive(command.Process.Pid) {
		t.Fatal("helper died with a short-lived caller thread")
	}
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeParentDeathSignalDoesNotPretendToCoverForkedChildren(t *testing.T) {
	if root := os.Getenv("SWARM_TEST_PROCESS_SIGNAL_ROOT"); root != "" {
		var signal int32
		_, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, syscall.PR_GET_PDEATHSIG, uintptr(unsafe.Pointer(&signal)), 0, 0, 0, 0)
		if errno != 0 {
			t.Fatal(errno)
		}
		name := "native-signal"
		if os.Getenv("SWARM_TEST_PROCESS_SIGNAL_CHILD") == "1" {
			name = "forked-signal"
		} else {
			// A naturally completing synchronous child is intentionally OUT of
			// the long-lived launcher scope. Fork clears its inherited setting.
			child := exec.Command(os.Args[0], "-test.run=^TestNativeParentDeathSignalDoesNotPretendToCoverForkedChildren$")
			child.Env = append(os.Environ(), "SWARM_TEST_PROCESS_SIGNAL_CHILD=1")
			if raw, err := child.CombinedOutput(); err != nil {
				t.Fatalf("finite fork observation: %s %v", raw, err)
			}
		}
		if err := os.WriteFile(filepath.Join(root, name), []byte(strconv.Itoa(int(signal))), 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	root := t.TempDir()
	command := exec.Command(os.Args[0], "-test.run=^TestNativeParentDeathSignalDoesNotPretendToCoverForkedChildren$")
	command.Env = append(os.Environ(), "SWARM_TEST_PROCESS_SIGNAL_ROOT="+root)
	if err := Start(command); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"native-signal": fmt.Sprint(int(syscall.SIGKILL)), "forked-signal": "0"} {
		got, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || string(got) != want {
			t.Fatalf("%s=%s, want %s: %v", name, got, want, err)
		}
	}
}
