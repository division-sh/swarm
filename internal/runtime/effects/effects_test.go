package effects

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/google/uuid"
)

func TestCompletionAuthorityPreservesExecutionMode(t *testing.T) {
	token := LifecycleToken{RuntimeEpoch: 1, AgentID: "agent-1", Generation: 2}
	ctx := WithExecutionMode(WithLifecycleToken(context.Background(), token), ExecutionModeMock)
	authority, ok := CompletionAuthorityFromContext(ctx)
	if !ok {
		t.Fatal("CompletionAuthorityFromContext returned no authority")
	}
	if authority.ExecutionMode != ExecutionModeMock {
		t.Fatalf("execution mode = %q, want mock", authority.ExecutionMode)
	}
	ctx = WithUsageTarget(ctx, UsageTarget{Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: uuid.NewString(), AgentID: "agent-1", SessionID: uuid.NewString(), Memory: agentmemory.Authored(false)})
	authority, ok = CompletionAuthorityFromContext(ctx)
	if !ok || authority.ExecutionMode != ExecutionModeMock {
		t.Fatalf("targeted authority = %#v, want mock mode preserved", authority)
	}
}

type effectStoreProbe struct {
	authorizations []AuthorizeRequest
}

type completionStoreProbe struct {
	effectStoreProbe
}

func (*completionStoreProbe) SettleCompletion(context.Context, Attempt, CompletionSettlement) (CompletionSettlementResult, error) {
	return CompletionSettlementResult{}, nil
}

type completionProjectionProbe struct{}

func (completionProjectionProbe) ProjectCommittedCompletionSpend(context.Context, CompletionSpendProjection) {
}

func (*effectStoreProbe) IsExternalEffectAuthorityCurrent(context.Context, Authority) (bool, error) {
	return true, nil
}

func (p *effectStoreProbe) AuthorizeExternalAttempt(_ context.Context, authority Authority, req AuthorizeRequest) (Attempt, error) {
	p.authorizations = append(p.authorizations, req)
	return authorizedProbeAttempt(authority, req), nil
}

func (*effectStoreProbe) MarkExternalAttemptLaunched(context.Context, Attempt, time.Time) error {
	return nil
}

func (*effectStoreProbe) MarkExternalAttemptResponseObserved(context.Context, Attempt, map[string]any, time.Time) error {
	return nil
}

func (*effectStoreProbe) HeartbeatCompletionAttempt(context.Context, Attempt, time.Time, time.Duration) error {
	return nil
}

func (*effectStoreProbe) SettleExternalAttempt(context.Context, Settlement) error { return nil }

func authorizedProbeAttempt(authority Authority, req AuthorizeRequest) Attempt {
	return Attempt{
		OperationID: req.OperationID, AttemptID: req.AttemptID, Token: authority.Normal, Authority: authority,
		Kind: req.Kind, Class: req.Class, Adapter: req.Adapter, Transport: req.Transport,
		Ordinal: 1, AuthorizedAt: req.Now,
	}
}

func TestBeginFailsClosedWithoutManagedLifecycleAuthority(t *testing.T) {
	if _, err := Begin(context.Background(), "authored_http_tool", []byte("request"), nil); err == nil {
		t.Fatal("managed effect was admitted without lifecycle authority")
	}

	bypass, err := Begin(WithDifferentOwner(context.Background(), OwnerRuntimeDependency), "authored_http_tool", []byte("request"), nil)
	if err != nil {
		t.Fatalf("explicit different-owner admission: %v", err)
	}
	if bypass == nil {
		t.Fatal("explicit different-owner admission returned a nil handle")
	}
	if err := bypass.MarkLaunched(context.Background()); err != nil {
		t.Fatalf("explicit different-owner launch: %v", err)
	}
	if current, err := ProjectionCurrent(context.Background()); err == nil || current {
		t.Fatalf("missing projection authority = current %v err=%v, want fail closed", current, err)
	}
	if current, err := ProjectionCurrent(WithDifferentOwner(context.Background(), OwnerRuntimeDependency)); err != nil || !current {
		t.Fatalf("different-owner projection = current %v err=%v", current, err)
	}
	if _, err := Begin(WithDifferentOwner(context.Background(), DifferentOwner("ad_hoc_owner")), "authored_http_tool", []byte("request"), nil); err == nil {
		t.Fatal("managed effect was admitted through an unregistered different owner")
	}
}

func TestBeginRequiresControllerAndLogicalIdentity(t *testing.T) {
	token := LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 3}
	withToken := WithLifecycleToken(context.Background(), token)
	if _, err := Begin(withToken, "authored_http_tool", []byte("request"), nil); err == nil {
		t.Fatal("managed effect was admitted without a controller")
	}

	withController := WithController(withToken, NewController(&effectStoreProbe{}))
	if _, err := Begin(withController, "authored_http_tool", []byte("request"), nil); err == nil {
		t.Fatal("managed effect was admitted without logical operation identity")
	}
}

func TestCompletionControllerRequiresSettlementProjectionOwner(t *testing.T) {
	store := &completionStoreProbe{}
	if NewController(store).CompletionEnabled() {
		t.Fatal("generic effect controller enabled completion without a spend projection owner")
	}
	if !NewCompletionController(store, completionProjectionProbe{}).CompletionEnabled() {
		t.Fatal("completion controller with settlement and projection owners is disabled")
	}
}

func TestBeginDerivesStableOperationAndAttemptIdentity(t *testing.T) {
	probe := &effectStoreProbe{}
	token := LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 3}
	ctx := WithLogicalOperationIdentity(
		WithController(WithLifecycleToken(context.Background(), token), NewController(probe)),
		"event-123",
	)
	first, err := Begin(ctx, "authored_http_tool", []byte("request"), map[string]string{"tool": "lookup"})
	if err != nil {
		t.Fatalf("first begin: %v", err)
	}
	second, err := Begin(ctx, "authored_http_tool", []byte("request"), map[string]string{"tool": "lookup"})
	if err != nil {
		t.Fatalf("second begin through probe: %v", err)
	}
	if first.Attempt().OperationID != second.Attempt().OperationID || first.Attempt().AttemptID != second.Attempt().AttemptID {
		t.Fatalf("logical replay identities differ: first=%+v second=%+v", first.Attempt(), second.Attempt())
	}
	if len(probe.authorizations) != 2 || probe.authorizations[0].RequestFingerprint != probe.authorizations[1].RequestFingerprint {
		t.Fatalf("authorizations = %#v, want stable fingerprints", probe.authorizations)
	}
}

func TestCanonicalOperationIdentitySurvivesLifecycleGenerationChange(t *testing.T) {
	ctx := WithLogicalOperationIdentity(context.Background(), "event-123")
	first, err := canonicalOperationID(ctx, testAuthority(LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 3}), "authored_http_tool", map[string]string{"tool": "lookup"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := canonicalOperationID(ctx, testAuthority(LifecycleToken{RuntimeEpoch: 8, AgentID: "agent-a", Generation: 4}), "authored_http_tool", map[string]string{"tool": "lookup"})
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("operation identity changed across lifecycle generation: %s != %s", first, second)
	}
}

func TestLogicalOperationIdentitySegmentsSeparateSiblingEffectsAndRemainStable(t *testing.T) {
	base := WithLogicalOperationIdentity(context.Background(), "event-123")
	first := WithLogicalOperationIdentitySegment(base, "provider_turn:1")
	replay := WithLogicalOperationIdentitySegment(base, "provider_turn:1")
	second := WithLogicalOperationIdentitySegment(base, "provider_turn:2")
	token := LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 3}
	firstID, err := canonicalOperationID(first, testAuthority(token), "anthropic_api", nil)
	if err != nil {
		t.Fatal(err)
	}
	replayID, err := canonicalOperationID(replay, testAuthority(token), "anthropic_api", nil)
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := canonicalOperationID(second, testAuthority(token), "anthropic_api", nil)
	if err != nil {
		t.Fatal(err)
	}
	if firstID != replayID {
		t.Fatalf("same logical child changed identity: %s != %s", firstID, replayID)
	}
	if firstID == secondID {
		t.Fatalf("sibling logical children share identity: %s", firstID)
	}
}

func testAuthority(token LifecycleToken) Authority {
	return NormalAgentAuthority(token, "test-owner", time.Now().UTC().Add(time.Minute))
}
