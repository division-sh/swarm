package llm

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

func TestProviderTurnAuthorityRequiresExactActorLifecycleAndSessionIdentity(t *testing.T) {
	identityA := testAgentIdentity("worker", "review/inst-a")
	identityB := testAgentIdentity("worker", "review/inst-b")
	sessionA := &Session{
		ID: uuid.NewString(), AgentID: "worker", Memory: agentmemory.PlatformDefault(),
		MemoryIdentity: agentmemory.Identity{RunID: uuid.NewString(), Agent: identityA},
	}
	sessionB := *sessionA
	sessionB.MemoryIdentity.Agent = identityB
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		"capability-identity-test",
		1,
		"",
		"test-actors",
		"bundle-v2:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		nil,
	)
	if err != nil {
		t.Fatalf("build managed execution admission: %v", err)
	}
	contextFor := func(actorIdentity, lifecycleIdentity agentidentity.Identity) context.Context {
		ctx := runtimeactors.WithActor(context.Background(), runtimeactors.AgentConfig{
			ID: "worker", Identity: actorIdentity, FlowPath: actorIdentity.FlowInstance(),
		})
		ctx = runtimeeffects.WithLifecycleToken(ctx, runtimeeffects.LifecycleToken{
			RuntimeEpoch: 1, Identity: lifecycleIdentity, AgentID: "worker", Generation: 1,
		})
		return managedexecution.WithAdmission(ctx, admission)
	}

	exactCtx := contextFor(identityA, identityA)
	exactCtx = runtimeeffects.WithLogicalOperationIdentity(exactCtx, "event-1\x00provider_turn:1")
	exactCtx, authority, err := withProviderTurnAuthority(exactCtx, sessionA)
	if err != nil {
		t.Fatalf("exact provider-turn authority: %v", err)
	}
	retryCtx := runtimeeffects.WithLogicalOperationIdentity(contextFor(identityA, identityA), "event-1\x00provider_turn:1")
	_, retryAuthority, err := withProviderTurnAuthority(retryCtx, sessionA)
	if err != nil {
		t.Fatalf("same-turn retry provider authority: %v", err)
	}
	if retryAuthority.ID != authority.ID {
		t.Fatalf("same-turn retry authority = %q, want %q", retryAuthority.ID, authority.ID)
	}
	replayCtx := runtimeeffects.WithLogicalOperationIdentity(contextFor(identityA, identityA), "event-2\x00provider_turn:1")
	_, replayAuthority, err := withProviderTurnAuthority(replayCtx, sessionA)
	if err != nil {
		t.Fatalf("fresh replay provider authority: %v", err)
	}
	if replayAuthority.ID == authority.ID {
		t.Fatalf("fresh replay reused provider-turn authority %q", authority.ID)
	}
	surface, err := managedCapabilityPlanForTurn(
		exactCtx,
		&AnthropicAPIRuntime{},
		sessionA,
		nil,
		toolcapabilities.Set{},
	)
	if err != nil {
		t.Fatalf("build exact provider-turn surface: %v", err)
	}
	if !surface.MatchesActor(identityA) {
		t.Fatalf("provider-turn surface actor = %#v, want %#v", surface.ActorIdentity, identityA)
	}

	for _, hostile := range []struct {
		name    string
		ctx     context.Context
		session *Session
	}{
		{
			name:    "actor sibling",
			ctx:     contextFor(identityB, identityA),
			session: sessionA,
		},
		{
			name:    "lifecycle sibling",
			ctx:     contextFor(identityA, identityB),
			session: sessionA,
		},
		{
			name:    "session sibling",
			ctx:     contextFor(identityA, identityA),
			session: &sessionB,
		},
	} {
		t.Run(hostile.name, func(t *testing.T) {
			hostile.ctx = runtimeeffects.WithLogicalOperationIdentity(hostile.ctx, "event-1\x00provider_turn:1")
			if _, _, err := withProviderTurnAuthority(hostile.ctx, hostile.session); err == nil {
				t.Fatal("same-slug sibling provider-turn authority was admitted")
			}
		})
	}
}
