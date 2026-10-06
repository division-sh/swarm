package serveapp

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type activationGatewayFault struct {
	mu        sync.Mutex
	armed     bool
	token     effects.LifecycleToken
	container string
	network   string
	probes    int
}

type activationGatewayDockerWorkspace struct {
	*workspace.DockerManager
	fault   *activationGatewayFault
	network string
}

func (w *activationGatewayDockerWorkspace) ResolveWorkspaceForCapabilityAdmission(ctx context.Context, actor actors.AgentConfig) (*workspace.Target, error) {
	target, err := w.DockerManager.ResolveWorkspaceForCapabilityAdmission(ctx, actor)
	if err != nil {
		return nil, err
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	w.fault.mu.Lock()
	defer w.fault.mu.Unlock()
	if !ok || actor.Identity.IsZero() || surface.Authority.Kind != managedcapabilities.AuthorityStartupProbe || surface.Authority.ExecutionKind != managedcapabilities.ExecutionNormalAgent {
		return target, nil
	}
	w.fault.probes++
	if !w.fault.armed {
		return target, nil
	}
	w.fault.armed = false
	w.fault.token, _ = effects.LifecycleTokenFromContext(ctx)
	w.fault.container, w.fault.network = target.Container, w.network
	if _, err := w.RunDocker(ctx, "network", "disconnect", w.network, target.Container); err != nil {
		return nil, err
	}
	return target, nil
}

func proveDockerActivationRefusalRetry(t *testing.T, rt servedWorkspaceProofRuntime, owner *workspace.DockerManager, fault *activationGatewayFault, actor actors.AgentConfig) {
	t.Helper()
	ctx := workspaceProofAuthorActivityContext(t, rt)
	load := func() manager.AgentLifecycleState {
		t.Helper()
		state, found, err := rt.Lifecycle.LoadAgentLifecycleState(ctx, actor.Identity)
		if err != nil || !found {
			t.Fatalf("exact activation lifecycle read: found=%v err=%v", found, err)
		}
		return state
	}
	before := load()
	fault.mu.Lock()
	beforeProbes := fault.probes
	fault.armed = true
	fault.mu.Unlock()
	params := map[string]any{"run_id": actor.Identity.RunID, "agent_id": actor.ID, "flow_instance": actor.Identity.FlowInstance(), "idempotency_key": "docker-activation-refused"}
	response := requestServedJSONRPCWithTimeout(t, rt.Endpoint, "agent.restart", params, 30*time.Second)
	if response.Error == nil {
		t.Fatal("unreachable activation published a runnable successor")
	}
	raw, err := json.Marshal(response.Error.Data)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Details struct {
			Failure json.RawMessage `json:"failure"`
		} `json:"details"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	refusal, err := failures.UnmarshalEnvelope(wire.Details.Failure)
	if err != nil || refusal.Class != failures.ClassDependencyUnavailable || refusal.Detail.Code != "workspace_gateway_unreachable" {
		t.Fatalf("activation compensation erased its original refusal: rpc=%+v failure=%+v err=%v", response.Error, refusal, err)
	}
	fault.mu.Lock()
	token, container, network := fault.token, fault.container, fault.network
	fault.mu.Unlock()
	after := load()
	if !token.Valid() || token.Identity != actor.Identity || after.RuntimeEpoch != token.RuntimeEpoch || after.Generation != token.Generation || after.Generation != before.Generation+1 || after.Phase != manager.AgentLifecycleRegistered || after.RunMode != manager.AgentRunModeStopped {
		t.Fatalf("refused activation has no exact stopped compensation: before=%+v token=%+v after=%+v", before, token, after)
	}
	if rt.Runtime.Manager.ProveUnpublishedActivation(token) == nil {
		t.Fatal("compensated generation retained executable admission")
	}
	compensation, err := storetest.ReadStartFailedCompensationCount(ctx, rt.Events, token)
	if err != nil || compensation != 1 {
		t.Fatalf("exact merged compensation operation missing: count=%d err=%v", compensation, err)
	}
	top, err := owner.RunDocker(ctx, "top", container, "-eo", "pid,args")
	if err != nil || strings.Contains(top, worker.Argument) {
		t.Fatalf("refusal returned before joining its exact native probe: %q err=%v", top, err)
	}
	if _, err := owner.RunDocker(ctx, "network", "connect", network, container); err != nil {
		t.Fatal(err)
	}
	params["idempotency_key"] = "docker-activation-clean-retry"
	response = requestServedJSONRPCWithTimeout(t, rt.Endpoint, "agent.restart", params, 30*time.Second)
	if response.Error != nil {
		t.Fatalf("clean new activation attempt failed: %+v", response.Error)
	}
	retried := load()
	if retried.Phase != manager.AgentLifecycleRunning || retried.RunMode == manager.AgentRunModeStopped || retried.RuntimeEpoch != after.RuntimeEpoch || retried.Generation != after.Generation+1 {
		t.Fatalf("retry did not issue a fresh executable generation: refused=%+v retried=%+v", after, retried)
	}
	fault.mu.Lock()
	probes := fault.probes
	fault.mu.Unlock()
	if probes != beforeProbes+2 {
		t.Fatalf("refusal/retry did not observe exactly once per new attempt: before=%d after=%d", beforeProbes, probes)
	}
	turns, err := storetest.ReadManagedAgentTurnStorage(ctx, rt.Events, actor.Identity.RunID, actor.ID)
	if err != nil {
		t.Fatal(err)
	}
	completions, err := storetest.ReadLifecycleEventCardinality(ctx, rt.Events, actor.Identity.RunID, "items.processed")
	if err != nil || len(turns) != 1 || completions != 1 {
		t.Fatalf("activation retry replayed committed model/output work: turns=%d events=%d err=%v", len(turns), completions, err)
	}
}
