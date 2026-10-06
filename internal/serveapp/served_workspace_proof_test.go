package serveapp

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/sourceartifact"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

// All read roles refer to the original serve projection. This fixture neither
// exposes a pool nor reconstructs a store around another owner's connection.
type servedWorkspaceProofRuntime struct {
	Endpoint, Backend, BundleHash string
	Runtime                       *runtimepkg.Runtime
	Events                        runtimebus.EventStore
	Lifecycle                     manager.AgentLifecycleStateReader
	Observability                 apiv1.ObservabilityReadStore
	ForkRuntime                   selectedForkRuntimeProofOptions
}

func startWorkspaceGatewayProofRuntime(t *testing.T, backend servedparity.Backend, sourceRoot, targetBackend string, factory func(*sourceartifact.RuntimeProjection, semanticview.Source) (cliapp.ServeWorkspaceLifecycle, error), mcpListen string, hooks ...pipeline.WorkflowNodeHandlerStartHook) servedWorkspaceProofRuntime {
	t.Helper()
	if len(hooks) > 1 {
		t.Fatal("at most one handler-start barrier is supported")
	}
	proof := servedWorkspaceProofRuntime{BundleHash: servedEventPublishFixtureBundleHash(t, sourceRoot)}
	forkOptions := captureServedForkRuntimeOptions(t)
	previousProjection := projectRuntimePersistenceForServe
	projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
		projection := previousProjection(owner)
		proof.Events = projection.deps.EventStore
		proof.Lifecycle = projection.deps.ManagerPersistenceRoles.LifecycleState
		proof.Observability = owner.Observability()
		return projection
	}
	t.Cleanup(func() { projectRuntimePersistenceForServe = previousProjection })
	previousWorkspace := cliapp.ConfiguredWorkspaceLifecycleForServe
	root := t.TempDir()
	cliapp.ConfiguredWorkspaceLifecycleForServe = func(_ *config.Config, projection *sourceartifact.RuntimeProjection, source semanticview.Source, _ cliapp.WorkspaceMountSources, _ cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
		if factory != nil {
			return factory(projection, source)
		}
		owner := workspace.NewHostManager()
		cfg := workspace.DefaultHostConfig()
		cfg.WorkspaceRoot, cfg.SourceProjection = root, projection
		owner.SetConfig(cfg)
		owner.SetSemanticSource(source)
		return owner, nil
	}
	t.Cleanup(func() { cliapp.ConfiguredWorkspaceLifecycleForServe = previousWorkspace })
	opts := cliapp.ServeOptions{
		SourceRoot: sourceRoot, PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath),
		WorkspaceBackend: targetBackend, WorkspaceBackendSet: targetBackend != "",
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: mcpListen, SelfCheck: true, Verbose: true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	if len(hooks) == 1 {
		opts.TestWorkflowNodeHandlerStartHook = hooks[0]
	}
	switch backend {
	case servedparity.BackendDefaultSQLite:
		unsetStoreSelectorEnv(t)
		proof.Backend = "sqlite"
		opts.ConfigPath = writeMockAgentRuntimeConfig(t, storebackend.BackendSQLite.String(), filepath.Join(t.TempDir(), ".swarm", "dev.db"))
	case servedparity.BackendExplicitPostgres:
		proof.Backend = "postgres"
		dsn := testutil.StartEmptyPostgresDSN(t)
		previous := buildStoresForServe
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
			if selection.Backend != storebackend.BackendPostgres {
				t.Fatal("native workspace fixture selected another backend")
			}
			return storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{
				Selection: selection, PostgresDSN: dsn, SessionLockTTL: runtimeSessionLockTTL(cfg),
			})
		}
		t.Cleanup(func() { buildStoresForServe = previous })
		opts.ConfigPath = writeMockAgentRuntimeConfig(t, storebackend.BackendPostgres.String(), "")
		opts.StoreMode, opts.StoreModeSet = "postgres", true
	default:
		t.Fatalf("unknown served workspace backend %q", backend)
	}
	proof.Endpoint, proof.Runtime = startOwnedMockLifecycleFollowUpRuntime(t, opts)
	proof.ForkRuntime = *forkOptions
	if proof.Events == nil || proof.Lifecycle == nil || proof.Observability == nil {
		t.Fatal("workspace proof requires the original selected read roles")
	}
	return proof
}

func workspaceProofAuthorActivityContext(t *testing.T, proof servedWorkspaceProofRuntime) context.Context {
	t.Helper()
	return servedRuntimeProofAuthorActivityContext(t, proof.Runtime, proof.BundleHash)
}

func readWorkspaceProofApplication(t *testing.T, owner runtimebus.EventStore) map[string]storetest.SelectedForkStorageTableSnapshot {
	t.Helper()
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func waitWorkspaceProofPipelineHandoff(t *testing.T, proof servedWorkspaceProofRuntime, runID string) {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	for time.Now().Before(deadline) {
		count, err := storetest.ReadServedIncompletePipelineHandoffCount(context.Background(), proof.Events, runID)
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("source pipeline completion did not finish: %s", workspaceProofDebugSummary(t, proof, runID))
}

func workspaceProofDebugSummary(t *testing.T, proof servedWorkspaceProofRuntime, runID string) string {
	t.Helper()
	summary, err := storetest.ReadServedRunDebugSummary(context.Background(), proof.Events, runID)
	if err != nil {
		t.Fatalf("read workspace proof diagnostic: %v", err)
	}
	return summary
}

func waitWorkspaceProofSourceAgentReady(t *testing.T, proof servedWorkspaceProofRuntime, runID, eventID string) {
	t.Helper()
	waitServedSourceAgentReadyFromOwner(t, proof.Events, proof.Backend, runID, eventID)
}

func requireWorkspaceProofMockTurn(t *testing.T, proof servedWorkspaceProofRuntime, runID, agentID, eventID string) storetest.ManagedAgentTurnStorageRow {
	t.Helper()
	matches := workspaceProofMockTurns(t, proof, runID, agentID, eventID)
	if len(matches) != 1 {
		t.Fatalf("exact mock turn for run=%s agent=%s event=%s: count=%d, want one", runID, agentID, eventID, len(matches))
	}
	return matches[0]
}

func workspaceProofMockTurns(t *testing.T, proof servedWorkspaceProofRuntime, runID, agentID, eventID string) []storetest.ManagedAgentTurnStorageRow {
	t.Helper()
	rows, err := storetest.ReadManagedAgentTurnStorage(context.Background(), proof.Events, runID, agentID)
	if err != nil {
		t.Fatal(err)
	}
	var matches []storetest.ManagedAgentTurnStorageRow
	for _, row := range rows {
		if row.ExecutionMode == "mock" && row.TriggerEventID == eventID {
			matches = append(matches, row)
		}
	}
	return matches
}
