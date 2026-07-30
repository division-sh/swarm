package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/google/uuid"
)

func managedCompletionTestSurface(t testing.TB, authority runtimeeffects.Authority, adapter string) managedcapabilities.Surface {
	t.Helper()
	executionKind := managedcapabilities.ExecutionNormalAgent
	executionAuthorityID := authority.ID
	runID := authority.Target.RunID
	if authority.Kind == runtimeeffects.AuthoritySelectedContractFork {
		executionKind = managedcapabilities.ExecutionSelectedContractFork
		executionAuthorityID = authority.SelectedFork.ExecutionID
		if runID == "" {
			runID = authority.SelectedFork.ForkRunID
		}
	}
	transport := "api"
	if adapter == "claude_cli" {
		transport = "cli"
	}
	runtimeMode := "task"
	if authority.Target.Memory.Enabled {
		runtimeMode = "session"
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: authority.Target.AgentIdentity, RuntimeMode: runtimeMode,
		Provider: adapter, Transport: transport, ProviderContract: "store-test-provider-contract",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: authority.Target.ID,
			ExecutionKind: executionKind, ExecutionAuthorityID: executionAuthorityID,
			RunID: runID, SessionID: authority.Target.SessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build managed completion test surface: %v", err)
	}
	return surface
}

func managedExecutionStoreTestContext(t testing.TB, ctx context.Context) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		"store-test-authority",
		1,
		"",
		"store-test-actors",
		"bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		nil,
	)
	if err != nil {
		t.Fatalf("build managed execution store test admission: %v", err)
	}
	return managedexecution.WithAdmission(ctx, admission)
}

func managedNormalEffectStoreTestContext(t testing.TB, ctx context.Context, authority runtimeeffects.Authority) context.Context {
	t.Helper()
	ctx = managedExecutionStoreTestContext(t, ctx)
	admission, _ := managedexecution.FromContext(ctx)
	principal, err := authority.Normal.Identity.Fingerprint()
	if err != nil {
		t.Fatalf("fingerprint normal managed-effect identity: %v", err)
	}
	turnID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("managed-effect-turn:"+principal)).String()
	sessionID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("managed-effect-session:"+principal)).String()
	runID := managedNormalEffectStoreTestRunID(authority.Normal.AgentID)
	target := runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: turnID, RunID: runID, AgentID: authority.Normal.AgentID,
		AgentIdentity: authority.Normal.Identity, SessionID: sessionID, Memory: agentmemory.PlatformDefault(),
		FlowInstance: authority.Normal.Identity.FlowInstance(),
	}
	ctx = runtimeeffects.WithUsageTarget(ctx, target)
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: authority.Normal.Identity, RuntimeMode: "task", Provider: "store-test", Transport: "api",
		ProviderContract: "store-test-provider-contract",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: turnID,
			ExecutionKind: managedcapabilities.ExecutionNormalAgent, ExecutionAuthorityID: admission.ExecutionAuthorityID,
			RunID: runID, SessionID: sessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build normal managed-effect test surface: %v", err)
	}
	return managedcapabilities.WithContext(ctx, surface)
}

func managedNormalEffectStoreTestRunID(agentID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("managed-effect-run:"+agentID)).String()
}

func managedSelectedExecutionStoreTestContext(t testing.TB, ctx context.Context, authority runtimeeffects.Authority) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindSelectedContractFork,
		authority.SelectedFork.ExecutionID,
		authority.SelectedFork.Generation,
		authority.SelectedFork.ForkRunID,
		"store-test-selected-actors",
		"bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		nil,
	)
	if err != nil {
		t.Fatalf("build selected managed execution store test admission: %v", err)
	}
	return managedexecution.WithAdmission(ctx, admission)
}

func managedAgentTurnRecordForTest(t testing.TB, rec runtimellm.AgentTurnRecord) runtimellm.AgentTurnRecord {
	t.Helper()
	if rec.Identity == (agentmemory.Identity{}) {
		rec.Identity = testAgentMemoryIdentity(t, rec.RunID, rec.AgentID, rec.FlowInstance)
	}
	authority := runtimeeffects.NormalAgentAuthority(
		runtimeeffects.LifecycleToken{Identity: rec.Identity.Agent, RuntimeEpoch: 1, AgentID: rec.AgentID, Generation: 1},
		"store-test-owner",
		time.Unix(1, 0).UTC().Add(time.Hour),
	)
	authority.Target = runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: uuid.NewString(), RunID: rec.RunID,
		AgentID: rec.AgentID, AgentIdentity: rec.Identity.Agent, SessionID: rec.SessionID,
		Memory: rec.Memory, FlowInstance: rec.FlowInstance, EntityID: rec.EntityID,
	}
	surface := managedCompletionTestSurface(t, authority, "anthropic_api")
	rec.CapabilitySurface = &surface
	return rec
}

type agentTurnAppenderForTest interface {
	AppendAgentTurn(context.Context, runtimellm.AgentTurnRecord) error
}

func appendManagedAgentTurnForTest(t testing.TB, ctx context.Context, store agentTurnAppenderForTest, rec runtimellm.AgentTurnRecord) error {
	t.Helper()
	ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive)
	return store.AppendAgentTurn(ctx, managedAgentTurnRecordForTest(t, rec))
}

func withManagedCompletionTestSurface(t testing.TB, ctx context.Context, authority runtimeeffects.Authority, adapter string) context.Context {
	t.Helper()
	kind := managedexecution.KindNormalRuntime
	executionAuthorityID := authority.ID
	generation := authority.FenceGeneration
	runID := ""
	if authority.Kind == runtimeeffects.AuthoritySelectedContractFork {
		kind = managedexecution.KindSelectedContractFork
		executionAuthorityID = authority.SelectedFork.ExecutionID
		generation = authority.SelectedFork.Generation
		runID = authority.SelectedFork.ForkRunID
	}
	admission, err := managedexecution.New(
		kind,
		executionAuthorityID,
		generation,
		runID,
		"store-test-completion-actors",
		"bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		nil,
	)
	if err != nil {
		t.Fatalf("build managed completion test admission: %v", err)
	}
	ctx = managedexecution.WithAdmission(ctx, admission)
	return managedcapabilities.WithContext(ctx, managedCompletionTestSurface(t, authority, adapter))
}

func applyManagedCompletionTestSurface(t testing.TB, turn *runtimeeffects.CompletionAgentTurn, authority runtimeeffects.Authority, adapter string) {
	t.Helper()
	if turn == nil {
		t.Fatal("completion test turn is missing")
	}
	surface := managedCompletionTestSurface(t, authority, adapter)
	raw, err := json.Marshal(surface)
	if err != nil {
		t.Fatalf("marshal managed completion test surface: %v", err)
	}
	turn.CapabilitySurfaceID = surface.ID
	turn.CapabilitySurface = raw
}

func applyManagedCompletionContextSurface(t testing.TB, ctx context.Context, turn *runtimeeffects.CompletionAgentTurn) {
	t.Helper()
	if turn == nil {
		t.Fatal("completion test turn is missing")
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		t.Fatal("managed completion test surface is missing")
	}
	raw, err := json.Marshal(surface)
	if err != nil {
		t.Fatalf("marshal managed completion context surface: %v", err)
	}
	turn.CapabilitySurfaceID = surface.ID
	turn.CapabilitySurface = raw
}

type managedCapabilityTestStore interface {
	SaveManagedCapabilitySurface(context.Context, managedcapabilities.Surface) error
}

func seedManagedAgentTurnCapabilitySurface(
	t testing.TB,
	store managedCapabilityTestStore,
	runID string,
	identity agentidentity.Identity,
	sessionID, turnID, runtimeMode, scopeKey string,
) string {
	t.Helper()
	now := time.Unix(1, 0).UTC()
	authority := runtimeeffects.NormalAgentAuthority(
		runtimeeffects.LifecycleToken{
			RuntimeEpoch: 1,
			Identity:     identity,
			AgentID:      identity.AgentID(),
			Generation:   1,
		},
		"store-test-owner",
		now.Add(time.Hour),
	)
	authority.Target = runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: turnID, RunID: runID,
		AgentID: identity.AgentID(), AgentIdentity: identity, SessionID: sessionID,
		Memory: agentmemory.PlatformDefault(), FlowInstance: identity.FlowInstance(),
		EntityID: scopeKey,
	}
	if runtimeMode != "task" {
		authority.Target.Memory = agentmemory.Authored(true)
	}
	surface := managedCompletionTestSurface(t, authority, "anthropic_api")
	if err := store.SaveManagedCapabilitySurface(context.Background(), surface); err != nil {
		t.Fatalf("seed managed agent-turn capability surface: %v", err)
	}
	return surface.ID
}

func TestCompletionRecoveryRejectsSameSlugSiblingCapabilityPrincipal(t *testing.T) {
	identityA := testAgentIdentity(t, "recovery-worker", "review/inst-a")
	identityB := testAgentIdentity(t, "recovery-worker", "review/inst-b")
	targetA := runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: uuid.NewString(), RunID: uuid.NewString(),
		AgentID: identityA.AgentID(), AgentIdentity: identityA, SessionID: uuid.NewString(),
		Memory: agentmemory.PlatformDefault(), FlowInstance: identityA.FlowInstance(),
	}
	authorityFor := func(identity agentidentity.Identity) runtimeeffects.Authority {
		target := targetA
		target.AgentIdentity = identity
		target.FlowInstance = identity.FlowInstance()
		token := runtimeeffects.LifecycleToken{
			RuntimeEpoch: 1, Identity: identity, AgentID: identity.AgentID(), Generation: 1,
		}
		authority := runtimeeffects.NormalAgentAuthority(token, "recovery-test-owner", time.Now().UTC().Add(time.Minute))
		authority.Target = target
		return authority
	}
	surfaceA := managedCompletionTestSurface(t, authorityFor(identityA), "anthropic_api")
	surfaceB := managedCompletionTestSurface(t, authorityFor(identityB), "anthropic_api")
	evidence := completionRecoveryAuthorityEvidence{
		ActorTokenID: identityA.AgentID(), ExecutionMode: string(runtimeeffects.ExecutionModeLive),
	}
	evidence.UsageTarget.Kind = string(targetA.Kind)
	evidence.UsageTarget.ID = targetA.ID
	evidence.UsageTarget.RunID = targetA.RunID
	evidence.UsageTarget.AgentID = targetA.AgentID
	evidence.UsageTarget.AgentIdentity = targetA.AgentIdentity
	evidence.UsageTarget.SessionID = targetA.SessionID
	evidence.UsageTarget.MemoryEnabled = targetA.Memory.Enabled
	evidence.UsageTarget.MemorySource = string(targetA.Memory.Source)
	evidence.UsageTarget.FlowInstance = targetA.FlowInstance
	authorityEvidence, err := json.Marshal(evidence)
	if err != nil {
		t.Fatalf("marshal recovery authority evidence: %v", err)
	}
	recovered := completionRecoveryAttempt{
		OperationID: uuid.NewString(), AttemptID: uuid.NewString(),
		AuthorityKind: string(runtimeeffects.AuthorityNormalAgent), AuthorityID: identityA.AgentID(),
		AuthorityEvidence: string(authorityEvidence),
		OperationMode:     string(runtimeeffects.ExecutionModeLive), AttemptMode: string(runtimeeffects.ExecutionModeLive),
		Adapter: "anthropic_api", Transport: "api", State: string(runtimeeffects.StateAuthorized),
		TargetKind: string(targetA.Kind), TargetID: targetA.ID,
	}
	encodeSurface := func(surface managedcapabilities.Surface) string {
		raw, err := json.Marshal(surface)
		if err != nil {
			t.Fatalf("marshal recovery capability surface: %v", err)
		}
		return string(raw)
	}

	recovered.CapabilitySurfaceID = surfaceB.ID
	recovered.CapabilitySurface = encodeSurface(surfaceB)
	if _, _, err := completionRecoverySettlement(
		recovered, runtimeeffects.StateTerminalFailure, nil, time.Now().UTC(),
	); err == nil {
		t.Fatal("completion recovery accepted a same-slug sibling capability principal")
	}

	recovered.CapabilitySurfaceID = surfaceA.ID
	recovered.CapabilitySurface = encodeSurface(surfaceA)
	if _, settlement, err := completionRecoverySettlement(
		recovered, runtimeeffects.StateTerminalFailure, nil, time.Now().UTC(),
	); err != nil {
		t.Fatalf("completion recovery rejected exact capability principal: %v", err)
	} else if settlement.AgentTurn == nil || settlement.AgentTurn.Identity.Agent != identityA {
		t.Fatalf("recovered exact agent turn = %#v", settlement.AgentTurn)
	}
}
