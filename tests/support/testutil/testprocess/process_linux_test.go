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

var startupParentDeath struct {
	signal   int32
	errno    syscall.Errno
	pid, tid int
}

func init() {
	if os.Getenv("SWARM_TEST_PROCESS_SIGNAL_ROOT") != "" {
		// Go runs init on the exec/startup thread, before test goroutines migrate.
		startupParentDeath.pid, startupParentDeath.tid = os.Getpid(), syscall.Gettid()
		_, _, startupParentDeath.errno = syscall.Syscall6(syscall.SYS_PRCTL, syscall.PR_GET_PDEATHSIG, uintptr(unsafe.Pointer(&startupParentDeath.signal)), 0, 0, 0, 0)
	}
}

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
		if startupParentDeath.errno != 0 || startupParentDeath.pid != os.Getpid() || startupParentDeath.tid != startupParentDeath.pid {
			t.Fatalf("parent-death observation is not the exec task: %+v", startupParentDeath)
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
		if err := os.WriteFile(filepath.Join(root, name), []byte(strconv.Itoa(int(startupParentDeath.signal))), 0600); err != nil {
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
