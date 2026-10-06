package construction_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestWorkspaceObserverRejectsAbsentAmbiguousAndInvalidStores(t *testing.T) {
	for _, cut := range []string{"relative", "missing", "empty", "multiple", "uninitialized"} {
		t.Run(cut, func(t *testing.T) {
			root := t.TempDir()
			switch cut {
			case "relative":
				root = "relative-root"
			case "missing":
				root = filepath.Join(root, "absent")
			case "multiple", "uninitialized":
				for _, name := range []string{"one", "two"} {
					path := filepath.Join(root, "swarm-test-session-"+name)
					if err := os.Mkdir(path, 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(path, "state.db"), nil, 0o600); err != nil {
						t.Fatal(err)
					}
					if cut == "uninitialized" {
						break
					}
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			result, err := runWorkspaceObserverChild(ctx, root, json.NewEncoder(io.Discard))
			if err == nil || result != (workspaceObserverResult{}) {
				t.Fatalf("invalid private observation manufactured counts: %+v %v", result, err)
			}
		})
	}
}

func TestWorkspaceObserverActualSnapshotJoinsAndDoesNotMutate(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	path := filepath.Join(root, "swarm-test-session-owned", "state.db")
	diagnosticPath := filepath.Join(root, "snapshot.json")
	t.Setenv("SWARM_TEST_WORKSPACE_OBSERVER_DIAGNOSTICS", diagnosticPath)
	writer, _ := storetest.StartSQLiteRuntimeStoreWithReopen(t, ctx, path)
	before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, writer)
	if err != nil {
		t.Fatal(err)
	}
	reader, output := io.Pipe()
	defer reader.Close()
	defer output.Close()
	observerCtx, stop := context.WithCancel(ctx)
	defer stop()
	result := make(chan workspaceObserverResult, 1)
	failure := make(chan error, 1)
	go func() {
		observed, err := runWorkspaceObserverChild(observerCtx, root, json.NewEncoder(output))
		result <- observed
		failure <- err
	}()
	decoder := json.NewDecoder(reader)
	var ready struct {
		Ready bool `json:"ready"`
	}
	if err := decoder.Decode(&ready); err != nil || !ready.Ready {
		t.Fatalf("observer readiness: %+v %v", ready, err)
	}
	var first workspaceObserverResult
	if err := decoder.Decode(&first); err != nil || !first.Observed || first.Finished || first.AgentDeliveries != 0 || first.Delivered != 0 || first.Emitted != 0 {
		t.Fatalf("real empty-store snapshot was not distinct from no observation: %+v %v", first, err)
	}
	stop()
	if got, err := <-result, <-failure; err != nil || got != first {
		t.Fatalf("observer failed to join with its real snapshot: %+v %v", got, err)
	}
	data, err := os.ReadFile(diagnosticPath)
	if err != nil {
		t.Fatal(err)
	}
	var diagnostic struct {
		Tables map[string]storetest.SelectedForkStorageTableSnapshot
	}
	if err := json.Unmarshal(data, &diagnostic); err != nil || !reflect.DeepEqual(before, diagnostic.Tables) {
		t.Fatalf("observer lost exact diagnostics before any agent delivery: %v", err)
	}
	after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, writer)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("independent observer changed writer storage: %v", err)
	}
}

func TestWorkspaceObserverCompiledChildProtocol(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, observed := range []bool{false, true} {
		name := "absent"
		if observed {
			name = "actual_snapshot"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			if observed {
				storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background(), filepath.Join(root, "swarm-test-session-owned", "state.db"))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "-test.run=^TestWorkspaceInvocationReadOnlyObserverChild$")
			cmd.Env = append(os.Environ(), "SWARM_TEST_WORKSPACE_OBSERVER_CHILD=1", "SWARM_TEST_WORKSPACE_OBSERVER_ROOT="+root)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if cmd.ProcessState == nil {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			}()
			decoder := json.NewDecoder(stdout)
			var ready struct {
				Ready bool `json:"ready"`
			}
			if err := decoder.Decode(&ready); err != nil || !ready.Ready {
				t.Fatalf("compiled observer readiness: %+v %v", ready, err)
			}
			var first workspaceObserverResult
			if observed {
				if err := decoder.Decode(&first); err != nil || !first.Observed || first.Finished {
					t.Fatalf("compiled observer initial snapshot: %+v %v", first, err)
				}
			}
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			var final workspaceObserverResult
			decodeErr := decoder.Decode(&final)
			if observed {
				first.Finished = true
				if decodeErr != nil || final != first {
					t.Fatalf("compiled observer final actual snapshot: %+v want=%+v err=%v", final, first, decodeErr)
				}
				if err := decoder.Decode(&final); !errors.Is(err, io.EOF) {
					t.Fatalf("compiled observer trailing protocol: %v", err)
				}
			} else if !errors.Is(decodeErr, io.EOF) || final != (workspaceObserverResult{}) {
				t.Fatalf("compiled observer manufactured absent-store counts: %+v %v", final, decodeErr)
			}
			if err := cmd.Wait(); (err == nil) != observed || ctx.Err() != nil {
				t.Fatalf("compiled observer join: observed=%v exit=%v context=%v", observed, err, ctx.Err())
			}
		})
	}
}

func TestWorkspaceObserverDiagnosticsRetainExactNativeSnapshot(t *testing.T) {
	ctx := context.Background()
	writer, location, _ := releaseInspectionWriter(t, "sqlite")
	want, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, writer)
	if err != nil {
		t.Fatal(err)
	}
	observer, err := storetest.OpenReleaseProcessReadOnlyInspection("sqlite", location)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := observer.Close(); err != nil {
			t.Error(err)
		}
	})
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := writeWorkspaceObserverSnapshot(ctx, observer, path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		At     time.Time
		Tables map[string]storetest.SelectedForkStorageTableSnapshot
	}
	if err := json.Unmarshal(data, &got); err != nil || got.At.IsZero() || !reflect.DeepEqual(want, got.Tables) {
		t.Fatalf("diagnostics lost original native rows/columns: %v", err)
	}
	after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, writer)
	if err != nil || !reflect.DeepEqual(want, after) {
		t.Fatalf("diagnostics changed the writer's store: %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := writeWorkspaceObserverSnapshot(cancelled, observer, path); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled diagnostics fabricated evidence: %v", err)
	}
	unchanged, err := os.ReadFile(path)
	if err != nil || !reflect.DeepEqual(data, unchanged) {
		t.Fatalf("failed capture overwrote committed evidence: %v", err)
	}
	if err := writeWorkspaceObserverSnapshot(ctx, observer, "relative"); err == nil {
		t.Fatal("relative evidence location accepted")
	}
}
