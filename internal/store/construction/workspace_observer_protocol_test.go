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
	"strings"
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
		Phases storetest.WorkspaceInvocationPhases
	}
	if err := json.Unmarshal(data, &diagnostic); err != nil || len(diagnostic.Phases.Deliveries) != 0 || len(diagnostic.Phases.Effects) != 0 {
		t.Fatalf("observer lost exact diagnostics before any agent delivery: %v", err)
	}
	timing, err := os.ReadFile(diagnosticPath + ".timing.json")
	if err != nil {
		t.Fatal(err)
	}
	var overhead struct {
		Samples      int   `json:"samples"`
		TotalNanos   int64 `json:"total_nanos"`
		MaximumNanos int64 `json:"maximum_nanos"`
		Unavailable  bool  `json:"unavailable"`
	}
	if err := json.Unmarshal(timing, &overhead); err != nil || overhead.Samples != 1 || overhead.TotalNanos <= 0 || overhead.MaximumNanos != overhead.TotalNanos || overhead.Unavailable {
		t.Fatalf("optional diagnostic overhead was not measured: %+v %v", overhead, err)
	}
	t.Logf("optional diagnostic sample: %s", time.Duration(overhead.TotalNanos))
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
		Phases storetest.WorkspaceInvocationPhases
	}
	if err := json.Unmarshal(data, &got); err != nil || got.At.IsZero() || len(got.Phases.Deliveries) != 0 || len(got.Phases.Effects) != 0 {
		t.Fatalf("diagnostics lost original native phase facts: %v", err)
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

func TestWorkspaceObserverOptionalDiagnosticFailureCannotAbortCounts(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	writer, _ := storetest.StartSQLiteRuntimeStoreWithReopen(t, ctx, filepath.Join(root, "swarm-test-session-owned", "state.db"))
	before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, writer)
	if err != nil {
		t.Fatal(err)
	}
	// A missing directory makes evidence/timing publication fail. It must
	// neither clear a real count nor fail the independent correctness reader.
	t.Setenv("SWARM_TEST_WORKSPACE_OBSERVER_DIAGNOSTICS", filepath.Join(root, "missing", "snapshot.json"))
	reader, output := io.Pipe()
	defer reader.Close()
	defer output.Close()
	observerCtx, stop := context.WithCancel(ctx)
	defer stop()
	joined := make(chan error, 1)
	go func() {
		result, err := runWorkspaceObserverChild(observerCtx, root, json.NewEncoder(output))
		if err == nil && !result.Observed {
			err = errors.New("optional failure lost real observation")
		}
		joined <- err
	}()
	decoder := json.NewDecoder(reader)
	var ready struct{ Ready bool }
	var observed workspaceObserverResult
	if err := decoder.Decode(&ready); err != nil || !ready.Ready {
		t.Fatalf("optional failure changed readiness: %v", err)
	}
	if err := decoder.Decode(&observed); err != nil || !observed.Observed {
		t.Fatalf("optional failure changed correctness observation: %v", err)
	}
	stop()
	if err := <-joined; err != nil {
		t.Fatalf("optional diagnostics aborted correctness reader: %v", err)
	}
	after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, writer)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("optional failure changed writer state: %v", err)
	}
}

func TestWorkspaceObserverDiagnosticPresentationExcludesPrivateBytes(t *testing.T) {
	types := reflect.TypeOf(storetest.WorkspaceInvocationPhases{})
	for i := 0; i < types.NumField(); i++ {
		row := types.Field(i).Type.Elem()
		for j := 0; j < row.NumField(); j++ {
			name := strings.ToLower(row.Field(j).Name)
			for _, forbidden := range []string{"payload", "source", "credential", "capability", "claimtoken", "authority", "result", "request", "failure"} {
				if strings.Contains(name, forbidden) {
					t.Fatalf("private bytes can enter failure output through %s.%s", types.Field(i).Name, row.Field(j).Name)
				}
			}
		}
	}
}
