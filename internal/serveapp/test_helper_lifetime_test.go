//go:build linux || darwin

package serveapp

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/tests/support/testutil/testprocess"
)

func TestServedHelperLifetimeAndForcedStop(t *testing.T) {
	const modeEnv = "SWARM_TEST_SERVED_LIFETIME_MODE"
	root := os.Getenv("SWARM_TEST_SERVED_LIFETIME_ROOT")
	switch os.Getenv(modeEnv) {
	case "owner":
		isolateCLIAPIConfigEnv(t)
		start, _, _ := issue2564H2Harness(t, "sqlite", issue2564H2Source(t))
		process, rt := start(false)
		requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "hub.start", "bundle_hash": rt.BundleHash, "payload": map[string]any{"hub_id": "h01"}, "idempotency_key": "helper-lifetime"})
		owner, err := testprocess.Observe(process.cmd.Process.Pid)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(owner)
		if err := os.WriteFile(filepath.Join(root, "helper.json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
		for {
			time.Sleep(time.Hour)
		}
	case "graceful", "ignore":
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
		defer cancel()
		if os.Getenv(modeEnv) == "ignore" {
			signal.Ignore(os.Interrupt)
		}
		fmt.Println("LIFETIME_HELPER_READY")
		<-ctx.Done()
		return
	}
	wait := func(t *testing.T, condition func() bool, limit time.Duration, label string) {
		t.Helper()
		deadline := time.Now().Add(limit)
		for !condition() {
			if time.Now().After(deadline) {
				t.Fatal(label)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	for _, ending := range []string{"kill", "timeout"} {
		t.Run(ending, func(t *testing.T) {
			root := t.TempDir()
			parent := exec.Command(os.Args[0], "-test.run=^TestServedHelperLifetimeAndForcedStop$", "-test.timeout=10s")
			parent.Env = append(os.Environ(), modeEnv+"=owner", "SWARM_TEST_SERVED_LIFETIME_ROOT="+root, "TMPDIR="+root)
			output := &lockedBuffer{}
			parent.Stdout, parent.Stderr = output, output
			if err := testprocess.Start(parent); err != nil {
				t.Fatal(err)
			}
			var helper testprocess.Lifetime
			t.Cleanup(func() {
				if parent.ProcessState == nil {
					_ = parent.Process.Kill()
					_ = parent.Wait()
				}
				if helper.Alive() {
					process, _ := os.FindProcess(helper.PID)
					_ = process.Kill()
					wait(t, func() bool { return !helper.Alive() }, 3*time.Second, "proof cleanup did not join helper termination")
				}
				if t.Failed() {
					t.Log(output.String())
				}
				_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
					if err == nil && entry.IsDir() {
						return os.Chmod(path, 0700)
					}
					return err
				})
			})
			wait(t, func() bool {
				raw, err := os.ReadFile(filepath.Join(root, "helper.json"))
				return err == nil && json.Unmarshal(raw, &helper) == nil && helper.Alive()
			}, serveRuntimeReadyTimeout, "real serve helper did not become ready: "+output.String())
			if ending == "kill" {
				if err := parent.Process.Kill(); err != nil {
					t.Fatal(err)
				}
			}
			if err := parent.Wait(); err == nil {
				t.Fatal("hard-ended parent passed")
			}
			wait(t, func() bool { return !helper.Alive() }, 3*time.Second, "serve helper outlived hard-ended test")
			snapshot := func() map[string][32]byte {
				result := map[string][32]byte{}
				if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
					if err != nil || entry.IsDir() || !strings.Contains(entry.Name(), "h2.sqlite") {
						return err
					}
					raw, err := os.ReadFile(path)
					result[path] = sha256.Sum256(raw)
					return err
				}); err != nil || len(result) == 0 {
					t.Fatal("missing real SQLite write evidence", err)
				}
				return result
			}
			before := snapshot()
			time.Sleep(100 * time.Millisecond)
			if !reflect.DeepEqual(before, snapshot()) {
				t.Fatal("serve helper continued writing after termination")
			}
			// Read-only source projections need unlocking only after termination.
			if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
				if err == nil && entry.IsDir() {
					return os.Chmod(path, 0700)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(root); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, mode := range []string{"graceful", "ignore"} {
		t.Run(mode, func(t *testing.T) {
			process := startServedCrashProcessBeforeReadiness(t, "TestServedHelperLifetimeAndForcedStop", []string{modeEnv + "=" + mode})
			t.Cleanup(func() {
				if err := process.kill(); err != nil {
					t.Error(err)
				}
			})
			wait(t, func() bool { return strings.Contains(process.output.String(), "LIFETIME_HELPER_READY") }, 3*time.Second, "helper did not install signal handling")
			began := time.Now()
			err := process.stop()
			if mode == "graceful" && err != nil || mode == "ignore" && (err == nil || time.Since(began) < serveRuntimeStopTimeout) {
				t.Fatal("wrong graceful/forced shutdown result", err)
			}
			select {
			case <-process.exited:
			default:
				t.Fatal("stop returned without joining helper")
			}
		})
	}
}
