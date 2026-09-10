package serveapp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/servedparity"
)

func captureSelectedResetSupervisor(t *testing.T) <-chan *processLifecycleSupervisor {
	t.Helper()
	captured := make(chan *processLifecycleSupervisor, 1)
	prior := buildSelectedAPICapabilities
	var once sync.Once
	buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
		once.Do(func() { captured <- req.RuntimeSupervisor })
		return prior(owner, req)
	}
	t.Cleanup(func() { buildSelectedAPICapabilities = prior })
	return captured
}

func TestSelectedResetFailedCandidateRemainsOwnedBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			captured := captureSelectedResetSupervisor(t)
			rt := startServedControlProofRuntime(t, backend)
			supervisor := <-captured
			predecessor := supervisor.selected
			original := supervisor.resetBuildExecution
			injected := errors.New("injected selected successor consumer construction failure")
			supervisor.operationMu.Lock()
			supervisor.resetBuildExecution = func(serveRuntimeBundleContext) (map[string]apiv1.MethodHandler, error) {
				if supervisor.selected == nil || supervisor.selected == predecessor {
					return nil, errors.New("consumer construction did not receive a fresh selected candidate")
				}
				return nil, injected
			}
			supervisor.operationMu.Unlock()
			params := map[string]any{"include_source_artifacts": false, "idempotency_key": "failed-selected-candidate"}
			response := requestServedJSONRPC(t, rt.Endpoint, "runtime.nuke", params)
			if response.Error == nil {
				t.Fatal("failed candidate was published as successful reset")
			}
			supervisor.operationMu.Lock()
			failed := supervisor.selected
			retainedPredecessor := supervisor.selectedResetPredecessor
			converged, fenced := supervisor.resetConverged, supervisor.resetting
			supervisor.resetBuildExecution = original
			supervisor.operationMu.Unlock()
			if failed == nil || failed == predecessor || retainedPredecessor != predecessor || converged || !fenced {
				t.Fatal("failed candidate lost exact shutdown ownership or exposed admission")
			}
			if _, _, err := failed.StopSelectedFork(context.Background(), runcontrol.TransitionRequest{RunID: "failed-candidate"}); !errors.Is(err, worklifetime.ErrRetired) {
				t.Fatalf("failed candidate control admission survived cleanup: %v", err)
			}
			expireServedResetTransportCache(t, rt)
			retry := requestServedJSONRPC(t, rt.Endpoint, "runtime.nuke", params)
			if retry.Error != nil {
				t.Fatalf("exact reset retry failed: %+v", retry.Error)
			}
			supervisor.operationMu.Lock()
			successor := supervisor.selected
			converged = supervisor.resetConverged
			supervisor.operationMu.Unlock()
			if successor == nil || successor == failed || successor == predecessor || !converged {
				t.Fatal("retry reused a retired inventory instead of constructing its own successor")
			}
			for _, retired := range []selectedForkContextRetirement{predecessor, failed} {
				if err := retired.RetireSelectedContexts(context.Background()); err != nil {
					t.Fatalf("joined retired owner changed after successor publication: %v", err)
				}
			}
			var operations int
			if err := rt.DB.QueryRow("SELECT COUNT(*) FROM runtime_reset_operations").Scan(&operations); err != nil || operations != 1 {
				t.Fatalf("retry replaced durable reset identity: operations=%d err=%v", operations, err)
			}
		})
	}
}

func TestSelectedResetCandidateTimeoutRetainsPendingWorkBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			captured := captureSelectedResetSupervisor(t)
			rt := startServedControlProofRuntime(t, backend)
			supervisor := <-captured
			original := supervisor.resetBuildExecution
			grace := supervisor.shutdownOptions.Grace
			probe := &selectedResetPreparationProbe{make(chan struct{}), make(chan struct{}), make(chan struct{})}
			var release sync.Once
			defer release.Do(func() { close(probe.release) })
			done := make(chan error, 1)
			builds := 0
			supervisor.operationMu.Lock()
			supervisor.resetBuildExecution = func(serveRuntimeBundleContext) (map[string]apiv1.MethodHandler, error) {
				builds++
				candidate := supervisor.selected
				go func() {
					ctx := worklifetime.WithProcess(context.Background(), supervisor.selectedProcess)
					prepared, err := candidate.Prepare(ctx, runforkexecution.SelectedContractExecutionRequest{SourceLoader: probe})
					if prepared != nil {
						err = errors.Join(err, prepared.Close())
					}
					done <- err
				}()
				select {
				case <-probe.entered:
				case err := <-done:
					return nil, errors.Join(errors.New("candidate did not accept preparation"), err)
				case <-time.After(10 * time.Second):
					return nil, errors.New("candidate preparation did not start")
				}
				// An already-expired join budget forces the failure ordering;
				// no clock race or sleep controls when the work can finish.
				supervisor.shutdownOptions.Grace = 0
				return nil, errors.New("injected consumer failure with accepted candidate work")
			}
			supervisor.operationMu.Unlock()
			params := map[string]any{"include_source_artifacts": false, "idempotency_key": "pending-selected-candidate"}
			response := requestServedJSONRPC(t, rt.Endpoint, "runtime.nuke", params)
			if response.Error == nil {
				t.Fatal("failed candidate with pending work became public-ready")
			}
			select {
			case <-probe.canceled:
			case <-time.After(10 * time.Second):
				t.Fatal("failed candidate was not fenced")
			}
			supervisor.operationMu.Lock()
			failed := supervisor.selected
			converged, fenced := supervisor.resetConverged, supervisor.resetting
			var projectionIDs []string
			for _, candidate := range supervisor.resetContexts {
				if candidate.loaded.sourceProjection.Identity() == "" {
					t.Error("candidate projection was released before selected work joined")
				}
				projectionIDs = append(projectionIDs, candidate.loaded.sourceProjection.Identity())
			}
			supervisor.operationMu.Unlock()
			if failed == nil || converged || !fenced || len(projectionIDs) == 0 {
				t.Fatal("candidate timeout dropped its owner or projection accounting")
			}
			expireServedResetTransportCache(t, rt)
			blocked := requestServedJSONRPC(t, rt.Endpoint, "runtime.nuke", params)
			if blocked.Error == nil {
				t.Fatal("retry passed unfinished candidate cleanup")
			}
			supervisor.operationMu.Lock()
			if supervisor.selected != failed || builds != 1 {
				t.Error("retry constructed a successor before candidate join")
			}
			if len(supervisor.resetContexts) != len(projectionIDs) {
				t.Error("retry discarded unjoined candidate projections")
			}
			for i, candidate := range supervisor.resetContexts {
				if i >= len(projectionIDs) || candidate.loaded.sourceProjection.Identity() != projectionIDs[i] {
					t.Error("retry replaced unjoined candidate projection")
				}
			}
			supervisor.resetBuildExecution = original
			supervisor.shutdownOptions.Grace = grace
			supervisor.operationMu.Unlock()
			var one int
			if err := rt.DB.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
				t.Fatalf("pending cleanup lost selected store: value=%d err=%v", one, err)
			}
			release.Do(func() { close(probe.release) })
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("pending candidate lost cancellation: %v", err)
			}
			expireServedResetTransportCache(t, rt)
			retry := requestServedJSONRPC(t, rt.Endpoint, "runtime.nuke", params)
			if retry.Error != nil {
				t.Fatalf("settled candidate could not reconstruct: %+v", retry.Error)
			}
			supervisor.operationMu.Lock()
			if supervisor.selected == failed || !supervisor.resetConverged {
				t.Error("settled retry failed to publish a fresh owner")
			}
			supervisor.operationMu.Unlock()
		})
	}
}

// Exercise the exact once-installed final-retirement port independently of
// normal runtime shutdown, which also retires selected work and could mask a
// stale final-close capture. Store-close/process-join ordering has its own test.
func TestSelectedResetFinalRetirementJoinsCurrentOwnerBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			captured := captureSelectedResetSupervisor(t)
			rt := startServedControlProofRuntime(t, backend)
			supervisor := <-captured
			final := &activatedServeLifecycle{}
			if err := final.SetSelectedForkSupervisor(supervisor); err != nil {
				t.Fatal(err)
			}
			if err := final.SetSelectedForkSupervisor(supervisor); err == nil {
				t.Fatal("final-retirement owner could be overwritten")
			}
			predecessor := supervisor.selected
			response := requestServedJSONRPC(t, rt.Endpoint, "runtime.nuke", map[string]any{
				"include_source_artifacts": false, "idempotency_key": "final-current-owner",
			})
			if response.Error != nil {
				t.Fatalf("reset failed: %+v", response.Error)
			}
			supervisor.operationMu.Lock()
			successor := supervisor.selected
			supervisor.operationMu.Unlock()
			if successor == nil || successor == predecessor {
				t.Fatal("reset did not replace selected ownership")
			}
			probe := &selectedResetPreparationProbe{make(chan struct{}), make(chan struct{}), make(chan struct{})}
			var release sync.Once
			done := make(chan error, 1)
			defer release.Do(func() { close(probe.release) })
			go func() {
				ctx := worklifetime.WithProcess(context.Background(), supervisor.selectedProcess)
				prepared, err := successor.Prepare(ctx, runforkexecution.SelectedContractExecutionRequest{SourceLoader: probe})
				if prepared != nil {
					err = errors.Join(err, prepared.Close())
				}
				done <- err
			}()
			select {
			case <-probe.entered:
			case err := <-done:
				t.Fatalf("successor preparation was not admitted: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("successor preparation did not start")
			}
			joinCtx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := final.selected.RetireSelectedContexts(joinCtx); !errors.Is(err, context.Canceled) {
				t.Fatalf("final retirement skipped pending successor work: %v", err)
			}
			select {
			case <-probe.canceled:
			case <-time.After(10 * time.Second):
				t.Fatal("final retirement did not fence successor preparation")
			}
			if supervisor.selected != successor {
				t.Fatal("retirement timeout discarded successor shutdown ownership")
			}
			var one int
			if err := rt.DB.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
				t.Fatalf("pending successor lost its store capability: value=%d err=%v", one, err)
			}
			release.Do(func() { close(probe.release) })
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("accepted successor lost cancellation: %v", err)
			}
			if err := final.selected.RetireSelectedContexts(context.Background()); err != nil {
				t.Fatalf("final owner could not join settled successor: %v", err)
			}
			if _, _, err := successor.StopSelectedFork(context.Background(), runcontrol.TransitionRequest{RunID: "late-successor"}); !errors.Is(err, worklifetime.ErrRetired) {
				t.Fatalf("joined successor still admitted controls: %v", err)
			}
		})
	}
}
