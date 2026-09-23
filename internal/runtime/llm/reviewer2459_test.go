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
	fault              error
	independent        error
	foreign            bool
	phase              runtimeeffects.MutationPhase
	consumeFault       error
	consumeIndependent error
	consumeForeign     bool
	consumePhase       runtimeeffects.MutationPhase
}

func (p *reviewer2459ProjectionProbe) ProjectCompletionConversation(ctx context.Context, a runtimeeffects.Attempt, projection runtimeeffects.CompletionConversationProjection) error {
	p.settledContinuationProbe.ProjectCompletionConversation(ctx, a, projection)
	if p.foreign {
		a.AttemptID = uuid.NewString()
	}
	phase := p.phase
	if phase == 0 {
		phase = runtimeeffects.MutationProjection
	}
	committed := runtimeeffects.NewPostCommitMutationError(phase, a, p.fault)
	if p.independent != nil {
		return errors.Join(committed, p.independent)
	}
	return committed
}

func (p *reviewer2459ProjectionProbe) ConsumeCompletionResponse(ctx context.Context, a runtimeeffects.Attempt, successor *agentframe.ToolContinuation) error {
	p.settledContinuationProbe.ConsumeCompletionResponse(ctx, a, successor)
	if p.consumeFault != nil {
		if p.consumeForeign {
			a.AttemptID = uuid.NewString()
		}
		phase := p.consumePhase
		if phase == 0 {
			phase = runtimeeffects.MutationProjection
		}
		committed := runtimeeffects.NewPostCommitMutationError(phase, a, p.consumeFault)
		if p.consumeIndependent != nil {
			return errors.Join(committed, p.consumeIndependent)
		}
		return committed
	}
	return nil
}

type reviewer2459ManagedCleanupRuntime struct {
	*managedRoundRuntime
	fault       error
	independent error
	cancel      context.CancelFunc
	consumption *reviewer2459ProjectionProbe
}

func (r *reviewer2459ManagedCleanupRuntime) ContinueManagedSession(ctx context.Context, s *Session, call ManagedCall) (*Response, error) {
	response, err := r.managedRoundRuntime.ContinueManagedSession(ctx, s, call)
	if err != nil || response == nil {
		return response, err
	}
	var a runtimeeffects.Attempt
	for attemptID, attempt := range r.harness.Attempts {
		authorization := r.harness.Authorizations[attemptID]
		if authorization.AgentFrame != nil && authorization.AgentFrame.FrameID == call.Frame().FrameID {
			if a.AttemptID != "" {
				return nil, errors.New("test provider frame matched multiple completion attempts")
			}
			a = attempt
		}
	}
	if a.AttemptID == "" {
		return nil, errors.New("test provider frame has no completion attempt")
	}
	response.completionAttempt = &a
	if r.consumption != nil {
		r.consumption.attempt = a
		controller := runtimeeffects.NewCompletionController(r.consumption, r.consumption, r.consumption, r.consumption)
		handle, found, recoverErr := controller.RecoverCompletionContinuation(ctx, s.ID, s.Memory)
		if recoverErr != nil || !found {
			return nil, errors.Join(recoverErr, errors.New("test continuation handle not recovered"))
		}
		response.completionHandle = handle
	}
	if r.cancel != nil {
		r.cancel()
	}
	return response, errors.Join(runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationSettlement, a, r.fault), r.independent)
}
func TestReviewer2459ManagedCleanupMustNotSkipTools(t *testing.T) {
	harness := effecttest.New()
	fault := errors.New("reviewer acknowledged settlement cleanup")
	consumeFault := errors.New("reviewer acknowledged consumption cleanup")
	consumption := &reviewer2459ProjectionProbe{settledContinuationProbe: &settledContinuationProbe{Harness: harness}, consumeFault: consumeFault}
	runtime := &reviewer2459ManagedCleanupRuntime{managedRoundRuntime: &managedRoundRuntime{harness: harness}, fault: fault, consumption: consumption}
	tools := &managedEffectToolExecutor{harness: harness}
	c := newTestManagedConversation(t, "effect-test-agent", "effect-test/instance", "analysis", []ToolDefinition{{Name: "echo"}}, testMemory(), 10, runtime)
	c.SetToolExecutor(tools)
	ctx := testManagedConversationContext(t, harness, "effect-test-agent", "effect-test/instance", "analysis")
	response, err := c.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: testManagedEvent("effect-test-agent")})
	if err != nil || response == nil || !errors.Is(response.completionCleanupDiagnostics, fault) || !errors.Is(response.completionCleanupDiagnostics, consumeFault) {
		t.Fatalf("committed result must succeed with retained diagnostic: response=%+v err=%v", response, err)
	}
	if tools.calls != 1 || runtime.calls != 2 || consumption.consumed != 2 {
		t.Fatalf("committed response stalled: provider calls=%d tool calls=%d consumes=%d response=%+v err=%v", runtime.calls, tools.calls, consumption.consumed, response, err)
	}
}

func TestManagedCompletionCleanupRejectsForeignPhaseAndJoinedFailure(t *testing.T) {
	attempt := runtimeeffects.Attempt{OperationID: uuid.NewString(), AttemptID: uuid.NewString()}
	response := &Response{completionAttempt: &attempt}
	cleanup := errors.New("committed cleanup")
	committed := runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationSettlement, attempt, cleanup)
	other := attempt
	other.AttemptID = uuid.NewString()
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"exact settlement", committed, true},
		{"exact projection and settlement", errors.Join(committed, runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationProjection, attempt, cleanup)), true},
		{"foreign attempt", runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationSettlement, other, cleanup), false},
		{"authorization phase", runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationAuthorization, attempt, cleanup), false},
		{"independent failure", errors.Join(committed, errors.New("independent failure")), false},
		{"independent cancellation", errors.Join(committed, context.Canceled), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := committedCompletionCleanup(response, tc.err); got != tc.want {
				t.Fatalf("committed cleanup admission=%t, want %t", got, tc.want)
			}
		})
	}
	if committedCompletionCleanup(&Response{}, committed) {
		t.Fatal("response without exact attempt borrowed acknowledgement")
	}
}

func TestManagedCompletionCleanupDoesNotAuthorizeIndependentToolWork(t *testing.T) {
	for _, tc := range []struct {
		name        string
		independent error
		cancel      bool
	}{
		{"joined failure", errors.New("independent failure"), false},
		{"canceled caller", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			harness := effecttest.New()
			ctx, cancel := context.WithCancel(testManagedConversationContext(t, harness, "effect-test-agent", "effect-test/instance", "analysis"))
			defer cancel()
			runtime := &reviewer2459ManagedCleanupRuntime{
				managedRoundRuntime: &managedRoundRuntime{harness: harness},
				fault:               errors.New("committed cleanup"), independent: tc.independent,
			}
			if tc.cancel {
				runtime.cancel = cancel
			}
			tools := &managedEffectToolExecutor{harness: harness}
			conversation := newTestManagedConversation(t, "effect-test-agent", "effect-test/instance", "analysis", []ToolDefinition{{Name: "echo"}}, testMemory(), 10, runtime)
			conversation.SetToolExecutor(tools)
			_, err := conversation.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: testManagedEvent("effect-test-agent")})
			if tools.calls != 0 || err == nil {
				t.Fatalf("independent failure admitted tools: calls=%d err=%v", tools.calls, err)
			}
			if tc.independent != nil && !errors.Is(err, tc.independent) {
				t.Fatalf("independent failure lost: %v", err)
			}
			if tc.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("caller cancellation lost: %v", err)
			}
		})
	}
}

type reviewer2459RecoveredCleanupRuntime struct {
	*managedRoundRuntime
	attempt runtimeeffects.Attempt
	fault   error
}

func (r *reviewer2459RecoveredCleanupRuntime) recoverManagedCompletionContinuation(ctx context.Context, session *Session) (*Response, bool, error) {
	response, found, err := r.managedRoundRuntime.recoverManagedCompletionContinuation(ctx, session)
	if err != nil || !found || response == nil {
		return response, found, err
	}
	response.completionAttempt = &r.attempt
	return response, true, runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationProjection, r.attempt, r.fault)
}

func TestManagedRecoveredCleanupResumesExactTerminalOrToolSuccessor(t *testing.T) {
	parentFrame := "agent-frame:v1:00000000-0000-4000-8000-000000000099"
	successor, err := agentframe.NewToolContinuation(parentFrame, json.RawMessage(`[{"name":"echo","ok":true,"result":{"value":"stable"}}]`))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name          string
		successor     *agentframe.ToolContinuation
		consumed      bool
		toolCalls     bool
		providerCalls int
		content       string
	}{
		{"projected no-tool response", nil, false, false, 0, "predecessor"},
		{"consumed terminal", nil, true, true, 0, "predecessor"},
		{"consumed tool successor", &successor, true, true, 1, "resumed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			harness := effecttest.New()
			fault := errors.New("recovered projection cleanup")
			attempt := runtimeeffects.Attempt{OperationID: uuid.NewString(), AttemptID: uuid.NewString()}
			var calls []ToolCall
			if tc.toolCalls {
				calls = []ToolCall{{ID: "call-1", Name: "echo", Arguments: map[string]any{"value": "must-not-repeat"}}}
			}
			runtime := &reviewer2459RecoveredCleanupRuntime{
				managedRoundRuntime: &managedRoundRuntime{
					harness: harness,
					recovered: &Response{
						Message:   Message{Role: "assistant", Content: "predecessor"},
						ToolCalls: calls, completionConsumed: tc.consumed,
						completionFrameID: parentFrame, completionSuccessor: tc.successor,
					},
					recoveredMessages: []Message{{Role: "user", Content: "start"}, {Role: "assistant", Content: "predecessor"}},
					recoveredTurn:     1, resumeTerminal: true,
				},
				attempt: attempt, fault: fault,
			}
			tools := &managedEffectToolExecutor{harness: harness}
			conversation := newTestManagedConversation(t, "effect-test-agent", "effect-test/instance", "analysis", []ToolDefinition{{Name: "echo"}}, testMemory(), 10, runtime)
			conversation.SetToolExecutor(tools)
			ctx := testManagedConversationContext(t, harness, "effect-test-agent", "effect-test/instance", "analysis")
			response, err := conversation.RunManaged(ctx, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: testManagedEvent("effect-test-agent")})
			if err != nil || response == nil || response.Message.Content != tc.content || !errors.Is(response.completionCleanupDiagnostics, fault) ||
				runtime.calls != tc.providerCalls || runtime.preparationCalls != 0 || tools.calls != 0 {
				t.Fatalf("recovered continuation: response=%+v err=%v provider=%d preparations=%d tools=%d", response, err, runtime.calls, runtime.preparationCalls, tools.calls)
			}
			if tc.successor != nil && (len(runtime.frames) != 1 || runtime.frames[0].Turn.Kind != agentframe.TurnToolContinuation ||
				runtime.frames[0].Turn.ParentFrameID != parentFrame) {
				t.Fatalf("wrong successor frame: %+v", runtime.frames)
			}
		})
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
	consumeFault := errors.New("reviewer consumption cleanup")
	probe := &reviewer2459ProjectionProbe{settledContinuationProbe: &settledContinuationProbe{Harness: harness, attempt: attempt}, fault: fault, consumeFault: consumeFault}
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
	if !errors.Is(response.completionCleanupDiagnostics, consumeFault) {
		t.Fatalf("consumption cleanup diagnostic lost: %v", response.completionCleanupDiagnostics)
	}
	for _, tc := range []struct {
		name        string
		independent error
		foreign     bool
		phase       runtimeeffects.MutationPhase
	}{
		{"joined independent consumption failure", errors.New("independent consumption failure"), false, 0},
		{"foreign consumption acknowledgement", nil, true, 0},
		{"wrong consumption phase", nil, false, runtimeeffects.MutationSettlement},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe.consumeIndependent, probe.consumeForeign, probe.consumePhase = tc.independent, tc.foreign, tc.phase
			consumeErr := consumeCompletionContinuation(ctx, response, nil)
			if consumeErr == nil {
				t.Fatal("unacknowledged consumption was accepted")
			}
			if tc.independent != nil && !errors.Is(consumeErr, tc.independent) {
				t.Fatalf("independent consumption failure lost: %v", consumeErr)
			}
		})
	}
	for _, tc := range []struct {
		name        string
		independent error
		foreign     bool
		phase       runtimeeffects.MutationPhase
	}{
		{"joined independent failure", errors.New("independent projection failure"), false, 0},
		{"foreign acknowledgement", nil, true, 0},
		{"wrong phase", nil, false, runtimeeffects.MutationSettlement},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe.independent, probe.foreign, probe.phase = tc.independent, tc.foreign, tc.phase
			unprojected := &Session{ID: session.ID, AgentID: session.AgentID, Memory: session.Memory, MemoryIdentity: session.MemoryIdentity}
			got, found, projectionErr := recoverCompletionContinuation(ctx, controller, unprojected, "anthropic_api")
			if !found || got != nil || projectionErr == nil || unprojected.TurnCount != 0 || len(unprojected.Messages) != 0 {
				t.Fatalf("unacknowledged projection was consumed: response=%+v found=%t session=%+v err=%v", got, found, unprojected, projectionErr)
			}
			if tc.independent != nil && !errors.Is(projectionErr, tc.independent) {
				t.Fatalf("independent projection failure lost: %v", projectionErr)
			}
		})
	}
}
