package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestSelectedForkDockerGatewayTransportBothStores(t *testing.T) {
	if os.Getenv("SWARM_TEST_WORKSPACE_MCP_DOCKER") != "1" {
		t.Skip("real Docker selected-fork transport proof; a skip earns no credit")
	}
	for _, loss := range []bool{false, true} {
		name := "reachable"
		if loss {
			name = "lost_after_activation"
		}
		t.Run(name, func(t *testing.T) {
			proveSelectedForkPublicChangedTargetExecutionBothStores(t, selectedForkProofOptions{nativeRead: true, docker: true, gatewayLoss: loss})
		})
	}
}

type selectedForkGatewayFault struct {
	mu              sync.Mutex
	armed           bool
	activationRunID string
	runID           string
	container       string
}

func startSelectedForkTransportProofRuntime(t *testing.T, backend servedparity.Backend, root string, options selectedForkProofOptions) (servedControlProofRuntime, *selectedForkGatewayFault) {
	t.Helper()
	if !options.docker {
		return startServedTestSetupEntitiesProofRuntimeWithWorkspace(t, backend, root, true), nil
	}
	fault := &selectedForkGatewayFault{armed: options.gatewayLoss}
	factory := func(projection *sourceartifact.RuntimeProjection, source semanticview.Source) (cliapp.ServeWorkspaceLifecycle, error) {
		manager := workspace.NewDockerManager()
		cfg := workspace.DefaultDockerConfig()
		cfg.SourceProjection = projection
		if network := os.Getenv("SWARM_TEST_WORKSPACE_MCP_NETWORK"); network != "" {
			cfg.WorkspaceNetwork = network
		}
		manager.SetConfig(cfg)
		manager.SetSemanticSource(source)
		return &selectedForkGatewayWorkspace{DockerManager: manager, fault: fault, network: cfg.WorkspaceNetwork}, nil
	}
	return startServedTestSetupEntitiesProofRuntimeWithWorkspaceFactory(t, backend, root, true, workspace.BackendDocker, factory, "0.0.0.0:0"), fault
}

// The fault changes only the exact isolated target, after its successful startup
// probe. The model and HTTP probe themselves remain real, unmodified consumers.
type selectedForkGatewayWorkspace struct {
	*workspace.DockerManager
	fault   *selectedForkGatewayFault
	network string
}

func (w *selectedForkGatewayWorkspace) RebindSourceProjection(projection *sourceartifact.RuntimeProjection, source semanticview.Source) (workspace.Lifecycle, error) {
	lifecycle, err := w.DockerManager.RebindSourceProjection(projection, source)
	if err != nil {
		return nil, err
	}
	return &selectedForkGatewayWorkspace{DockerManager: lifecycle.(*workspace.DockerManager), fault: w.fault, network: w.network}, nil
}

func (w *selectedForkGatewayWorkspace) ResolveWorkspace(ctx context.Context, actor actors.AgentConfig) (*workspace.Target, error) {
	target, err := w.DockerManager.ResolveWorkspace(ctx, actor)
	if err != nil {
		return nil, err
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		return target, nil
	}
	w.fault.mu.Lock()
	defer w.fault.mu.Unlock()
	if !w.fault.armed || surface.Authority.Kind != managedcapabilities.AuthorityProviderTurn || surface.Authority.ExecutionKind != managedcapabilities.ExecutionSelectedContractFork {
		return target, nil
	}
	w.fault.armed = false
	w.fault.runID, w.fault.container = actor.Identity.RunID, target.Container
	if _, err := w.RunDocker(ctx, "network", "disconnect", w.network, target.Container); err != nil {
		return nil, errors.Join(err, target.Release(context.WithoutCancel(ctx)))
	}
	return target, nil
}

func (w *selectedForkGatewayWorkspace) ResolveWorkspaceForCapabilityAdmission(ctx context.Context, actor actors.AgentConfig) (*workspace.Target, error) {
	target, err := w.DockerManager.ResolveWorkspaceForCapabilityAdmission(ctx, actor)
	if err != nil {
		return nil, err
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if ok && surface.Authority.Kind == managedcapabilities.AuthorityStartupProbe && !actor.Identity.IsZero() && surface.Authority.ExecutionKind == managedcapabilities.ExecutionSelectedForkPreparation {
		w.fault.mu.Lock()
		w.fault.activationRunID = actor.Identity.RunID
		w.fault.mu.Unlock()
	}
	return target, nil
}

func requireSelectedForkGatewayLossBeforeModel(t *testing.T, endpoint string, owner runtimebus.EventStore, params map[string]any, fault *selectedForkGatewayFault, sourceRunID string) {
	t.Helper()
	response := requestServedJSONRPCWithTimeout(t, endpoint, "run.fork", params, 30*time.Second)
	if response.Error == nil {
		t.Fatal("selected target executed with its real gateway disconnected")
	}
	fault.mu.Lock()
	activationRunID, runID, container := fault.activationRunID, fault.runID, fault.container
	fault.mu.Unlock()
	if runID == "" || activationRunID != runID || runID == sourceRunID || container == "" {
		t.Fatalf("fault did not reach an activated isolated target: activation_run=%q run=%q source=%q container=%q", activationRunID, runID, sourceRunID, container)
	}
	// run.fork reports aggregate execution refusal. The exact turn failure is
	// authoritative in its settled delivery and public diagnostic projection.
	storage, err := storetest.ReadManagedDeliveryStorage(context.Background(), owner, runID, "same-name")
	if err != nil || len(storage.Failures) != 1 {
		t.Fatalf("exact selected settled failure count=%d error=%v", len(storage.Failures), err)
	}
	deliveryID, eventID, failureRaw := storage.Failures[0].DeliveryID, storage.Failures[0].EventID, storage.Failures[0].Failure
	failure, err := failures.UnmarshalEnvelope(failureRaw)
	if err != nil || failure.Class != failures.ClassDependencyUnavailable || failure.Detail.Code != "workspace_gateway_unreachable" {
		t.Fatalf("selected pre-model refusal lost durable typed gateway evidence: failure=%s err=%v", failureRaw, err)
	}
	turns, err := storetest.ReadManagedAgentTurnStorage(context.Background(), owner, runID, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(turns) != 0 {
		t.Fatalf("unreachable selected target launched %d model turns", len(turns))
	}
	var diagnostics operatorread.OperatorAgentDeliveryDiagnostics
	requireServedJSONRPCResult(t, endpoint, "agent.delivery_diagnostics", map[string]any{
		"agent_id": "same-name", "run_id": runID, "flow_instance": "consumer",
	}, &diagnostics)
	if len(diagnostics.DeadLetters) != 1 {
		t.Fatalf("public selected refusal cardinality: %+v", diagnostics)
	}
	letter := diagnostics.DeadLetters[0]
	if letter.RunID != runID || letter.EventID != eventID || letter.DeliveryID != deliveryID || letter.EventName != "producer/work.ready" || letter.Status != "dead_letter" || letter.Failure == nil || len(letter.DeadLetterRecords) != 1 {
		t.Fatalf("public selected refusal lost its exact turn/delivery/failure: %+v", letter)
	}
	wantFailure, err := json.Marshal(failure)
	if err != nil {
		t.Fatal(err)
	}
	for _, projected := range []*failures.Envelope{letter.Failure, &letter.DeadLetterRecords[0].Failure} {
		got, err := json.Marshal(projected)
		if err != nil || !bytes.Equal(got, wantFailure) {
			t.Fatalf("public selected refusal changed persisted failure bytes: want=%s got=%s err=%v", wantFailure, got, err)
		}
	}
	effects, err := storetest.ReadManagedTurnEffectStorage(context.Background(), owner, runID, "same-name")
	if err != nil || effects.ProviderAttempts != 0 || storage.Delivered != 0 {
		t.Fatalf("unreachable selected target launched/settled model work: effects=%+v delivered=%d err=%v", effects, storage.Delivered, err)
	}
}
