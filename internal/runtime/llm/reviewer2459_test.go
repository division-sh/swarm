package llm

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/google/uuid"
	"reflect"
	"testing"
	"time"
)

type reviewer2459ProjectionProbe struct {
	*settledContinuationProbe
	fault error
}

func (p *reviewer2459ProjectionProbe) ProjectCompletionConversation(ctx context.Context, a runtimeeffects.Attempt, projection runtimeeffects.CompletionConversationProjection) error {
	p.settledContinuationProbe.ProjectCompletionConversation(ctx, a, projection)
	return runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationProjection, a, p.fault)
}

type reviewer2459ManagedCleanupRuntime struct {
	*managedRoundRuntime
	fault error
}

func (r *reviewer2459ManagedCleanupRuntime) ContinueManagedSession(ctx context.Context, s *Session, call ManagedCall) (*Response, error) {
	response, err := r.managedRoundRuntime.ContinueManagedSession(ctx, s, call)
	if err != nil || response == nil {
		return response, err
	}
	var a runtimeeffects.Attempt
	for _, attempt := range r.harness.Attempts {
		a = attempt
	}
	return response, runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationSettlement, a, r.fault)
}
func TestReviewer2459ManagedCleanupMustNotSkipTools(t *testing.T) {
	harness := effecttest.New()
	fault := errors.New("reviewer acknowledged settlement cleanup")
	runtime := &reviewer2459ManagedCleanupRuntime{managedRoundRuntime: &managedRoundRuntime{harness: harness}, fault: fault}
	tools := &managedEffectToolExecutor{harness: harness}
	c := newTestManagedConversation(t, "effect-test-agent", "effect-test/instance", "analysis", []ToolDefinition{{Name: "echo"}}, testMemory(), 10, runtime)
	c.SetToolExecutor(tools)
	ctx := testManagedConversationContext(t, harness, "effect-test-agent", "effect-test/instance", "analysis")
	response, err := c.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: testManagedEvent("effect-test-agent")})
	if !errors.Is(err, fault) {
		t.Fatalf("diagnostic not retained: %v", err)
	}
	if response == nil || tools.calls != 1 || runtime.calls != 2 {
		t.Fatalf("committed response stalled: provider calls=%d tool calls=%d response=%+v err=%v", runtime.calls, tools.calls, response, err)
	}
}

func TestReviewer2459RecoveryProjectionAcknowledgedErrorKeepsResponse(t *testing.T) {
	harness := effecttest.New()
	ctx := harness.CompletionContext("settled-continuation")
	authority, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok {
		t.Fatal("completion authority missing")
	}
	claim, ok := runtimedelivery.ClaimFromContext(ctx)
	if !ok {
		t.Fatal("completion delivery claim missing")
	}
	origin, err := runtimeeffects.DeliveryCompletionOrigin(claim)
	if err != nil {
		t.Fatal(err)
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		t.Fatal("completion capability surface missing")
	}
	identity := authority.Target.AgentIdentity.Normalize()
	messages := []Message{{Role: "user", Content: "hello"}, {Role: "assistant", Content: "done"}}
	payload, err := json.Marshal(completionContinuationEnvelope{
		Version: completionContinuationVersion,
		Adapter: "anthropic_api",
		Response: Response{
			Message: messages[1], Raw: json.RawMessage(`{"content":"done"}`),
			ToolOutputAuthority: &ToolOutputAuthority{ProviderOperationID: uuid.NewString(), SettledAt: time.Unix(10, 0).UTC()},
		},
		Usage: runtimeeffects.CompletionUsage{ResolvedModel: "test-model", Exactness: runtimeeffects.CompletionUsageUnavailable},
		Projection: completionProjection{
			SessionID: authority.Target.SessionID, ExpectedTurnCount: 0, TurnCount: 1,
			Messages: messages, Identity: identity, Memory: authority.Target.Memory, FrameID: "frame-original",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	evidence := map[string]any{}
	if err := runtimeeffects.AttachCompletionContinuationEvidence(evidence, []byte("exact-original-request"), payload); err != nil {
		t.Fatal(err)
	}
	rawEvidence, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := runtimeeffects.AdmitCompletionContinuation(runtimeeffects.Attempt{
		OperationID: uuid.NewString(), AttemptID: uuid.NewString(), Kind: runtimeeffects.KindProviderTurn,
		Class: runtimeeffects.EffectReadOnly, Adapter: "anthropic_api", Transport: "api",
		Authority: authority, Origin: origin, Ordinal: 1, AuthorizedAt: time.Unix(9, 0).UTC(),
	}, rawEvidence, runtimeeffects.Fingerprint([]byte("exact-original-request")), surface, runtimeeffects.CompletionProjectionConversationProjected, nil)
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("reviewer projection cleanup")
	probe := &reviewer2459ProjectionProbe{settledContinuationProbe: &settledContinuationProbe{Harness: harness, attempt: attempt}, fault: fault}
	controller := runtimeeffects.NewCompletionController(probe, probe, probe, probe)
	session := &Session{
		ID: authority.Target.SessionID, AgentID: authority.Target.AgentID, Memory: authority.Target.Memory,
		MemoryIdentity: identity, Messages: append([]Message(nil), messages...), TurnCount: 1,
	}
	response, found, err := recoverCompletionContinuation(ctx, controller, session, "anthropic_api")
	if !errors.Is(err, fault) {
		t.Fatalf("missing retained diagnostic: %v", err)
	}
	if !found || response == nil || !reflect.DeepEqual(response.Message, messages[1]) {
		t.Fatalf("recovered response=%#v found=%v", response, found)
	}
	if session.TurnCount != 1 || !reflect.DeepEqual(session.Messages, messages) {
		t.Fatalf("recovery duplicated projected conversation: turn=%d messages=%#v", session.TurnCount, session.Messages)
	}
	if len(probe.projections) != 1 || probe.projections[0].ExpectedTurnCount != 0 || probe.projections[0].TurnCount != 1 || probe.projections[0].SessionID != session.ID {
		t.Fatalf("canonical projection calls=%#v", probe.projections)
	}
	if err := consumeCompletionContinuation(ctx, response, nil); err != nil {
		t.Fatal(err)
	}
	if probe.consumed != 1 {
		t.Fatalf("completion consumes=%d, want 1", probe.consumed)
	}
}
