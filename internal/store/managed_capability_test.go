package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
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
		ActorID: authority.Target.AgentID, RuntimeMode: runtimeMode,
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
		"store-test-bundle",
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
	turnID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("managed-effect-turn:"+authority.Normal.AgentID)).String()
	sessionID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("managed-effect-session:"+authority.Normal.AgentID)).String()
	runID := managedNormalEffectStoreTestRunID(authority.Normal.AgentID)
	target := runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: turnID, RunID: runID, AgentID: authority.Normal.AgentID,
		SessionID: sessionID, Memory: agentmemory.PlatformDefault(), FlowInstance: "store-test",
	}
	ctx = runtimeeffects.WithUsageTarget(ctx, target)
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorID: authority.Normal.AgentID, RuntimeMode: "task", Provider: "store-test", Transport: "api",
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
		"store-test-selected-bundle",
		nil,
	)
	if err != nil {
		t.Fatalf("build selected managed execution store test admission: %v", err)
	}
	return managedexecution.WithAdmission(ctx, admission)
}

func managedAgentTurnRecordForTest(t testing.TB, rec runtimellm.AgentTurnRecord) runtimellm.AgentTurnRecord {
	t.Helper()
	authority := runtimeeffects.NormalAgentAuthority(
		runtimeeffects.LifecycleToken{RuntimeEpoch: 1, AgentID: rec.AgentID, Generation: 1},
		"store-test-owner",
		time.Unix(1, 0).UTC().Add(time.Hour),
	)
	authority.Target = runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: uuid.NewString(), RunID: rec.RunID,
		AgentID: rec.AgentID, SessionID: rec.SessionID, Memory: rec.Memory, FlowInstance: rec.FlowInstance, EntityID: rec.EntityID,
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
		"store-test-completion-bundle",
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
	runID, agentID, sessionID, turnID, runtimeMode, scopeKey string,
) string {
	t.Helper()
	now := time.Unix(1, 0).UTC()
	authority := runtimeeffects.NormalAgentAuthority(
		runtimeeffects.LifecycleToken{RuntimeEpoch: 1, AgentID: agentID, Generation: 1},
		"store-test-owner",
		now.Add(time.Hour),
	)
	authority.Target = runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: turnID, RunID: runID,
		AgentID: agentID, SessionID: sessionID, Memory: agentmemory.PlatformDefault(), FlowInstance: scopeKey,
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
