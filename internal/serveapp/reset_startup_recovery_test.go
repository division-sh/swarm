package serveapp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestServedResetPendingStartupUsesAdmittedSourcesBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, phase := range []string{"admitted", "containers_settled"} {
			for _, clear := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/clear=%t", backend, phase, clear), func(t *testing.T) {
					unsetStoreSelectorEnv(t)
					source := writeServedEventPublishFollowUpFixture(t)
					hash := servedEventPublishFixtureBundleHash(t, source)
					cfg := writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "reset.db"))
					opts := cliapp.ServeOptions{
						ConfigPath: cfg, SourceRoot: source, PlatformSpecPath: defaultPlatformSpecPath,
						APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true,
						WorkspaceBackend: "host", WorkspaceBackendSet: true,
					}
					if backend == servedparity.BackendExplicitPostgres {
						installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle { return serveRuntimeWorkspaceStub{} })
						opts.ConfigPath = writeServeRuntimeTestConfig(t)
						opts.StoreMode, opts.StoreModeSet = "postgres", true
					} else {
						stubServeRuntimeWorkspaceLifecycle(t)
					}
					var proof servedControlProofRuntime
					var selected any
					captureSelectedRuntimePersistence(t, func(persistence serveRuntimePersistence) {
						proof.DB, proof.Postgres, proof.SQLite = selectedRuntimeStoreForTest(t, persistence)
						if proof.SQLite != nil {
							selected, proof.Backend = proof.SQLite, "sqlite"
						} else {
							selected, proof.Backend = proof.Postgres, "postgres"
						}
					})
					opts.TestRuntimeContextsReadyHook = func(contexts *runtime.RuntimeContextManager) { proof.Contexts = contexts }
					captured := captureSelectedResetSupervisor(t)
					endpoint, stop := startResetRecoveryServedBoot(t, opts)
					previousSupervisor := <-captured
					initial := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
						"event_name": "item.received", "bundle_hash": hash,
						"payload": map[string]any{"item_id": "predecessor"}, "idempotency_key": uuid.NewString(),
					})
					fault := storetest.SetResetFinalReceiptFault
					if phase == "admitted" {
						fault = storetest.SetResetPlanReceiptFault
					}
					if err := fault(context.Background(), selected, true); err != nil {
						t.Fatal(err)
					}
					params := map[string]any{"include_source_artifacts": clear, "idempotency_key": uuid.NewString()}
					if response := requestServedJSONRPC(t, endpoint, "runtime.nuke", params); response.Error == nil {
						t.Fatal("final receipt fault did not interrupt reset")
					}
					if err := fault(context.Background(), selected, false); err != nil {
						t.Fatal(err)
					}
					var actualPhase string
					if err := proof.DB.QueryRow("SELECT phase FROM runtime_reset_operations").Scan(&actualPhase); err != nil || actualPhase != phase {
						t.Fatalf("interrupted phase=%s, want %s: %v", actualPhase, phase, err)
					}
					previousSupervisor.operationMu.Lock()
					previousSelected := previousSupervisor.selected
					previousProcess := previousSupervisor.selectedProcess
					previousSupervisor.operationMu.Unlock()
					stop()
					if _, _, err := previousSelected.StopSelectedFork(context.Background(), runcontrol.TransitionRequest{RunID: uuid.NewString()}); !errors.Is(err, worklifetime.ErrRetired) {
						t.Fatalf("previous boot selected admission survived shutdown: %v", err)
					}
					// A restart may not replace the admitted reset sources with the
					// current directory, nor re-ingest it after a source-clearing reset.
					if err := os.WriteFile(filepath.Join(source, "schema.yaml"), []byte("invalid: ["), 0o600); err != nil {
						t.Fatal(err)
					}
					captured = captureSelectedResetSupervisor(t)
					endpoint, _ = startResetRecoveryServedBoot(t, opts)
					recoveredSupervisor := <-captured
					recoveredSupervisor.operationMu.Lock()
					recoveredSelected := recoveredSupervisor.selected
					recoveredProcess := recoveredSupervisor.selectedProcess
					recoveredSupervisor.operationMu.Unlock()
					if recoveredSelected == nil || recoveredSelected == previousSelected || recoveredProcess == nil || recoveredProcess == previousProcess {
						t.Fatal("startup adopted a predecessor selected family or process identity")
					}
					_, selectedRun, controlErr := recoveredSelected.StopSelectedFork(context.Background(), runcontrol.TransitionRequest{RunID: uuid.NewString()})
					if clear {
						if selectedRun || !errors.Is(controlErr, worklifetime.ErrRetired) {
							t.Fatalf("cleared startup admitted selected execution: selected=%v err=%v", selectedRun, controlErr)
						}
					} else if selectedRun || controlErr != nil {
						t.Fatalf("retained startup failed to reconcile selected controls: selected=%v err=%v", selectedRun, controlErr)
					}
					if err := proof.DB.QueryRow("SELECT phase FROM runtime_reset_operations").Scan(&actualPhase); err != nil || actualPhase != "completed" {
						t.Fatalf("recovered phase=%s: %v", actualPhase, err)
					}
					var predecessor int
					if err := proof.DB.QueryRow("SELECT COUNT(*) FROM runs WHERE run_id = $1", initial.RunID).Scan(&predecessor); err != nil || predecessor != 0 {
						t.Fatalf("startup retained predecessor history after explicit pending reset: %d, %v", predecessor, err)
					}
					want := 1
					if clear {
						want = 0
					}
					if proof.Contexts.Len() != want {
						t.Fatalf("recovered contexts=%d, want %d", proof.Contexts.Len(), want)
					}
					if !clear {
						use, _, err := proof.Contexts.AcquireBundleHash(context.Background(), hash)
						if err != nil || use == nil {
							t.Fatalf("identical admitted source unavailable: %v", err)
						}
						if err := use.Done(); err != nil {
							t.Fatal(err)
						}
					}
					response := requestServedJSONRPC(t, endpoint, "runtime.nuke", params)
					if response.Error != nil {
						t.Fatalf("recovered outcome replay: %+v", response.Error)
					}
					recoveredSupervisor.operationMu.Lock()
					if recoveredSupervisor.selected != recoveredSelected {
						t.Error("startup outcome replay replaced selected family")
					}
					recoveredSupervisor.operationMu.Unlock()
				})
			}
		}
	}
}

// This proves a real served stop/start, not process death. Crash coverage uses
// the subprocess harness separately.
func startResetRecoveryServedBoot(t *testing.T, opts cliapp.ServeOptions) (string, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var output lockedBuffer
	opts.Output = &output
	done := make(chan int, 1)
	finished := make(chan struct{})
	var exitCode int
	go func() {
		exitCode = runFrom(ctx, repoRootForTest(), opts)
		done <- exitCode
		close(finished)
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-finished:
				if exitCode != 0 {
					t.Errorf("serve exit=%d\n%s", exitCode, output.String())
				}
			case <-time.After(servedProofPollDeadline):
				t.Errorf("serve shutdown timed out\n%s", output.String())
			}
		})
	}
	t.Cleanup(stop)
	waitForServeReadyLine(t, &output, done)
	return "http://" + serveRuntimeAPIListenerFromOutput(t, output.String()) + "/v1/rpc", stop
}
