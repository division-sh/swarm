//go:build linux || darwin

package testprocess

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const fixtureEnv = "SWARM_TEST_PROCESS_FIXTURE"

type fixture struct {
	Mode, Root string
	Watched    bool
	Owner      Lifetime
	LeaseFD    int
}

func fixtureCommand(t *testing.T, config fixture) *exec.Cmd {
	t.Helper()
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestProcessFixture$")
	command.Env = append(os.Environ(), fixtureEnv+"="+string(raw))
	return command
}

func fixtureStart(command *exec.Cmd, watched bool) error {
	if watched {
		owner, err := Current()
		if err != nil {
			return err
		}
		return StartFor(command, owner)
	}
	return Start(command)
}

func TestProcessFixture(t *testing.T) {
	if os.Getenv(fixtureEnv) == "" {
		return
	}
	var config fixture
	if err := json.Unmarshal([]byte(os.Getenv(fixtureEnv)), &config); err != nil {
		t.Fatal(err)
	}
	switch config.Mode {
	case "outer":
		signal.Ignore(syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
		owner, err := Current()
		if err != nil {
			t.Fatal(err)
		}
		inner := fixtureCommand(t, fixture{Mode: "inner", Root: config.Root, Owner: owner})
		if err := Start(inner); err != nil {
			t.Fatal(err)
		}
		if err := inner.Wait(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(config.Root, "inner-exited"), nil, 0600); err != nil {
			t.Fatal(err)
		}
		for {
			time.Sleep(time.Hour)
		}
	case "inner":
		child := exec.Command("sh", "-c", `test -z "$SWARM_TEST_PROCESS_BOOTSTRAP" || exit 125
trap '' INT TERM HUP
printf '{"PID":%s}' "$$" > "$1/ready"
while :; do printf 'write\n' >> "$1/writes"; sleep .01; done`, "external-payload", config.Root)
		if err := StartFor(child, config.Owner); err != nil {
			t.Fatal(err)
		}
		identity, _ := json.Marshal(struct{ PID int }{child.Process.Pid})
		if err := os.WriteFile(filepath.Join(config.Root, "child-pid"), identity, 0600); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	case "parent", "race":
		child := fixtureCommand(t, fixture{Mode: "writer", Root: config.Root})
		if err := fixtureStart(child, config.Watched); err != nil {
			t.Fatal(err)
		}
		identity, _ := json.Marshal(struct{ PID int }{child.Process.Pid})
		if err := os.WriteFile(filepath.Join(config.Root, "child-pid"), identity, 0600); err != nil {
			t.Fatal(err)
		}
		if config.Mode == "race" {
			os.Exit(0)
		}
		for {
			time.Sleep(time.Hour)
		}
	case "writer", "lease":
		if os.Getenv(bootstrapEnv) != "" {
			t.Fatal("bootstrap protocol leaked into payload environment")
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()
		if config.Mode == "lease" {
			inherited := os.NewFile(uintptr(config.LeaseFD), "inherited-lease")
			if err := inherited.Close(); err != nil {
				t.Fatal(err)
			}
		}
		file, err := os.OpenFile(filepath.Join(config.Root, "writes"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		pid := os.Getpid()
		group, err := syscall.Getpgid(0)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(struct{ PID, Group int }{pid, group})
		if err := os.WriteFile(filepath.Join(config.Root, "ready"), raw, 0600); err != nil {
			t.Fatal(err)
		}
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				if _, err := file.WriteString("write\n"); err != nil {
					t.Fatal(err)
				}
			}
		}
	default:
		t.Fatal("unknown fixture mode")
	}
}

func await(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(description)
}

func ready(t *testing.T, root string) (result struct{ PID, Group int }) {
	t.Helper()
	await(t, func() bool {
		raw, err := os.ReadFile(filepath.Join(root, "ready"))
		return err == nil && json.Unmarshal(raw, &result) == nil && result.PID > 0
	}, "helper did not report its actual process identity")
	return result
}

func alive(pid int) bool {
	_, err := processBirth(pid)
	return err == nil
}

func TestParentDeathTerminatesHelperAndCeasesWritesBeforeRemoval(t *testing.T) {
	for _, watched := range []bool{false, true} {
		for _, ending := range []string{"kill", "timeout", "race", "nested"} {
			t.Run(strconv.FormatBool(watched)+"/"+ending, func(t *testing.T) {
				root := t.TempDir()
				mode := "parent"
				if ending == "race" {
					mode = "race"
				} else if ending == "nested" {
					mode = "outer"
				}
				parent := fixtureCommand(t, fixture{Mode: mode, Root: root, Watched: watched})
				if ending == "nested" {
					parent.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
				}
				var diagnostics bytes.Buffer
				parent.Stdout, parent.Stderr = &diagnostics, &diagnostics
				if ending == "timeout" {
					parent.Args = append(parent.Args, "-test.timeout=1s")
				}
				if err := Start(parent); err != nil {
					t.Fatal(err)
				}
				var pid int
				t.Cleanup(func() {
					_ = parent.Process.Kill()
					if parent.ProcessState == nil {
						_ = parent.Wait()
					}
					if t.Failed() {
						writes, _ := os.ReadFile(filepath.Join(root, "writes"))
						t.Logf("helper alive=%t write_bytes=%d; parent fixture: %s", pid > 0 && alive(pid), len(writes), diagnostics.String())
					}
					if pid > 0 && alive(pid) {
						_ = syscall.Kill(pid, syscall.SIGKILL)
						await(t, func() bool { return !alive(pid) }, "proof observer could not join helper termination")
					}
				})
				await(t, func() bool {
					raw, err := os.ReadFile(filepath.Join(root, "child-pid"))
					var identity struct{ PID int }
					if err == nil && json.Unmarshal(raw, &identity) == nil {
						pid = identity.PID
					}
					return pid > 0
				}, "parent did not publish spawned helper PID")
				if ending != "race" {
					if got := ready(t, root); got.PID != pid {
						t.Fatal("bootstrap changed helper PID", got.PID, pid)
					}
					await(t, func() bool {
						info, err := os.Stat(filepath.Join(root, "writes"))
						return err == nil && info.Size() > 0
					}, "helper never performed real writes")
				}
				if ending == "nested" {
					await(t, func() bool { _, err := os.Stat(filepath.Join(root, "inner-exited")); return err == nil }, "inner parent did not exit while its outer-owned helper survived")
					if !alive(pid) {
						t.Fatal("helper was bound to its deliberately exited inner parent")
					}
					if err := syscall.Kill(-parent.Process.Pid, syscall.SIGTERM); err != nil {
						t.Fatal(err)
					}
					time.Sleep(100 * time.Millisecond)
					if !alive(parent.Process.Pid) || !alive(pid) {
						t.Fatal("group cancellation bypassed intentional survivor fixture")
					}
				}
				if ending == "kill" || ending == "nested" {
					if err := parent.Process.Kill(); err != nil {
						t.Fatal(err)
					}
				}
				if err := parent.Wait(); ending == "race" && err != nil || ending != "race" && err == nil {
					t.Fatal("wrong parent exit", err)
				}
				await(t, func() bool { return !alive(pid) }, "helper survived its lifetime owner")
				before, err := os.ReadFile(filepath.Join(root, "writes"))
				if err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				// Termination and a stopped writer are separate observations made
				// before unlinking anything; unlinkability alone earns no credit.
				time.Sleep(100 * time.Millisecond)
				after, err := os.ReadFile(filepath.Join(root, "writes"))
				if err != nil && !os.IsNotExist(err) || string(before) != string(after) {
					t.Fatal("terminated helper continued writing", err)
				}
				if err := os.RemoveAll(root); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(root); !os.IsNotExist(err) {
					t.Fatal("owned directory was not removed", err)
				}
			})
		}
	}
}

func TestWatchedExecPreservesIdentityGroupLeaseAndErrors(t *testing.T) {
	for _, implicit := range []bool{false, true} {
		t.Run(strconv.FormatBool(implicit), func(t *testing.T) { proveWatchedExec(t, implicit) })
	}
}

func proveWatchedExec(t *testing.T, implicit bool) {
	t.Helper()
	root := t.TempDir()
	held, err := os.Create(filepath.Join(root, "lease"))
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if err := syscall.Flock(int(held.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	leaseFD := 3
	if implicit {
		fd, err := unix.FcntlInt(held.Fd(), unix.F_DUPFD, 64)
		if err != nil {
			t.Fatal(err)
		}
		inherited := os.NewFile(uintptr(fd), "implicit-lease")
		defer inherited.Close()
		leaseFD = fd
		original := held
		held = inherited
		if err := original.Close(); err != nil {
			t.Fatal(err)
		}
	}
	command := fixtureCommand(t, fixture{Mode: "lease", Root: root, LeaseFD: leaseFD})
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if !implicit {
		command.ExtraFiles = []*os.File{held}
	}
	owner, err := Current()
	if err != nil {
		t.Fatal(err)
	}
	if err := StartFor(command, owner); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	got := ready(t, root)
	if got.PID != command.Process.Pid || got.Group != got.PID {
		t.Fatal("PID or requested process group changed", got)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	probe, err := os.Open(filepath.Join(root, "lease"))
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	if err := syscall.Flock(int(probe.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal("guard retained a payload lease descriptor", err)
	}
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal("ordinary graceful helper exit changed", err)
	}
	missing := exec.Command(filepath.Join(root, "absent"))
	if err := StartFor(missing, owner); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("payload exec error was not preserved", err)
	}
	owner.Birth += "-different-occurrence"
	if err := StartFor(exec.Command(os.Args[0]), owner); err == nil {
		t.Fatal("different process occurrence adopted as lifetime owner")
	}
}
