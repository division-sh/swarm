package manager

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
)

type chatTestAgent struct {
	id              string
	directive       string
	runID           string
	directiveEvent  string
	directiveSource string
	calls           int
	err             error
	started         chan<- struct{}
	release         <-chan struct{}
}

func (a *chatTestAgent) ID() string                        { return a.id }
func (a *chatTestAgent) Type() string                      { return "stub" }
func (a *chatTestAgent) Subscriptions() []events.EventType { return nil }
func (a *chatTestAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}
func (a *chatTestAgent) BoardStep(_ context.Context, directive runtimeagentcontrol.BoardDirective) (string, error) {
	a.calls++
	a.directive = directive.Directive
	a.runID = directive.Event.RunID()
	a.directiveEvent = directive.Event.ID()
	a.directiveSource = string(directive.Event.Type())
	if a.started != nil {
		a.started <- struct{}{}
	}
	if a.release != nil {
		<-a.release
	}
	return "ok", a.err
}

type chatTestStore struct{}

func (s *chatTestStore) UpsertAgent(context.Context, PersistedAgent) error { return nil }
func (s *chatTestStore) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return nil, nil
}
func (s *chatTestStore) MarkAgentTerminated(context.Context, string) error { return nil }
func (s *chatTestStore) EnsureEntitySchema(context.Context, string) error  { return nil }
func (s *chatTestStore) UpsertEventReceipt(context.Context, string, string, ReceiptStatus, string) error {
	return nil
}
func (s *chatTestStore) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (s *chatTestStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

type directiveTargetStore struct {
	chatTestStore
	target runtimeagentcontrol.RunTargetResolution
	err    error
	calls  int
}

func (s *directiveTargetStore) ResolveAgentDirectiveRunTarget(context.Context, string, string) (runtimeagentcontrol.RunTargetResolution, error) {
	s.calls++
	if s.err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, s.err
	}
	return s.target, nil
}

type directiveTestBus struct {
	direct []events.Event
	store  *directiveEventStore
}

func (b *directiveTestBus) Publish(_ context.Context, evt events.Event) error {
	return nil
}
func (b *directiveTestBus) PublishDirect(_ context.Context, evt events.Event, _ []string) error {
	b.direct = append(b.direct, evt)
	return nil
}
func (b *directiveTestBus) PublishPersistedRecipients(context.Context, events.Event, []string) error {
	return nil
}
func (b *directiveTestBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (b *directiveTestBus) Unsubscribe(string) {}
func (b *directiveTestBus) Store() runtimebus.EventStore {
	if b.store == nil {
		b.store = &directiveEventStore{}
	}
	return b.store
}
func (b *directiveTestBus) ResetInMemoryState() error { return nil }
func (b *directiveTestBus) LogRuntime(context.Context, runtimepipeline.RuntimeLogEntry) error {
	return nil
}

type directiveEventStore struct {
	mu                 sync.Mutex
	events             []events.Event
	operations         map[string]runtimeagentcontrol.DirectiveOperation
	recordExecutedErr  error
	finalizeSuccessErr error
}

func (s *directiveEventStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.events = append(s.events, evt)
	return nil
}
func (*directiveEventStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}
func (*directiveEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, runtimereplayclaim.ErrAuthoritativeRecipientManifestUnavailable
}
func (*directiveEventStore) SupportsPersistedReplay() bool { return false }

func (s *directiveEventStore) ReserveDirectiveOperation(_ context.Context, req runtimeagentcontrol.ReserveDirectiveOperationRequest) (runtimeagentcontrol.DirectiveOperationReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.operations == nil {
		s.operations = map[string]runtimeagentcontrol.DirectiveOperation{}
	}
	for _, existing := range s.operations {
		if req.Operation.IdempotencyKey != "" && existing.Method == req.Operation.Method && existing.ActorTokenID == req.Operation.ActorTokenID && existing.IdempotencyKey == req.Operation.IdempotencyKey {
			return runtimeagentcontrol.DirectiveOperationReservation{Operation: existing}, nil
		}
	}
	op := req.Operation
	op.CreatedAt, op.UpdatedAt = req.Now, req.Now
	s.operations[op.OperationID] = op
	s.events = append(s.events, req.Event)
	return runtimeagentcontrol.DirectiveOperationReservation{Operation: op, Created: true}, nil
}

func (s *directiveEventStore) AdmitDirectiveExecution(_ context.Context, operationID, ownerID string, now time.Time, lease time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op := s.operations[operationID]
	if op.State != runtimeagentcontrol.DirectiveOperationPrepared {
		return op, runtimeagentcontrol.ErrorForDirectiveOperation(op)
	}
	op.State = runtimeagentcontrol.DirectiveOperationExecuting
	op.ExecutionOwnerID = ownerID
	op.ExecutionLeaseExpiresAt = now.Add(lease)
	op.ExecutionAdmittedAt, op.UpdatedAt = now, now
	s.operations[operationID] = op
	return op, nil
}

func (*directiveEventStore) RenewDirectiveExecutionLease(context.Context, string, string, time.Time, time.Duration) error {
	return nil
}

func (s *directiveEventStore) RecordDirectiveExecuted(_ context.Context, operationID, ownerID string, response json.RawMessage, now time.Time) (runtimeagentcontrol.DirectiveOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op := s.operations[operationID]
	if s.recordExecutedErr != nil {
		return op, s.recordExecutedErr
	}
	if op.State != runtimeagentcontrol.DirectiveOperationExecuting || op.ExecutionOwnerID != ownerID {
		return op, runtimeagentcontrol.ErrorForDirectiveOperation(op)
	}
	op.State = runtimeagentcontrol.DirectiveOperationExecuted
	op.Response = append(json.RawMessage(nil), response...)
	op.ExecutedAt, op.UpdatedAt = now, now
	s.operations[operationID] = op
	return op, nil
}

func (s *directiveEventStore) FinalizeDirectiveSuccess(_ context.Context, operationID string, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op := s.operations[operationID]
	if s.finalizeSuccessErr != nil {
		return op, s.finalizeSuccessErr
	}
	if op.State != runtimeagentcontrol.DirectiveOperationExecuted && op.State != runtimeagentcontrol.DirectiveOperationSucceeded {
		return op, runtimeagentcontrol.ErrorForDirectiveOperation(op)
	}
	op.State = runtimeagentcontrol.DirectiveOperationSucceeded
	op.CompletedAt, op.UpdatedAt, op.ExpiresAt = now, now, now.Add(ttl)
	s.operations[operationID] = op
	return op, nil
}

func (s *directiveEventStore) FinalizeDirectiveFailure(_ context.Context, operationID, ownerID, code, message string, details json.RawMessage, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op := s.operations[operationID]
	if op.State != runtimeagentcontrol.DirectiveOperationExecuting || op.ExecutionOwnerID != ownerID {
		return op, runtimeagentcontrol.ErrorForDirectiveOperation(op)
	}
	op.State, op.ErrorCode, op.ErrorMessage = runtimeagentcontrol.DirectiveOperationFailed, code, message
	op.ErrorDetails = append(json.RawMessage(nil), details...)
	op.CompletedAt, op.UpdatedAt, op.ExpiresAt = now, now, now.Add(ttl)
	s.operations[operationID] = op
	return op, nil
}

func (s *directiveEventStore) LoadDirectiveOperation(_ context.Context, operationID string) (runtimeagentcontrol.DirectiveOperation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.operations[operationID]
	return op, ok, nil
}

func (s *directiveEventStore) LoadDirectiveOperationByKey(_ context.Context, method, actor, key string) (runtimeagentcontrol.DirectiveOperation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, op := range s.operations {
		if op.Method == method && op.ActorTokenID == actor && op.IdempotencyKey == key {
			return op, true, nil
		}
	}
	return runtimeagentcontrol.DirectiveOperation{}, false, nil
}

func (*directiveEventStore) ReconcileDirectiveOperations(context.Context, time.Time, time.Duration) (runtimeagentcontrol.DirectiveOperationReconcileResult, error) {
	return runtimeagentcontrol.DirectiveOperationReconcileResult{}, nil
}

func (s *directiveEventStore) ReconcileDirectiveOperation(_ context.Context, operationID string, now time.Time, _ time.Duration) (runtimeagentcontrol.DirectiveOperation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.operations[operationID]
	if !ok {
		return runtimeagentcontrol.DirectiveOperation{}, false, nil
	}
	if op.State == runtimeagentcontrol.DirectiveOperationExecuting && !op.ExecutionLeaseExpiresAt.After(now) {
		op.State = runtimeagentcontrol.DirectiveOperationIndeterminate
		op.ErrorCode = "execution_lease_expired"
		op.ErrorMessage = "directive execution lease expired before a durable outcome"
		op.ExecutionLeaseExpiresAt = time.Time{}
		op.UpdatedAt = now
		s.operations[operationID] = op
	}
	return op, true, nil
}

var _ runtimeagentcontrol.DirectiveOperationStore = (*directiveEventStore)(nil)

func TestAgentManager_ChatWithAgentPersistsDirectiveEventBeforeBoardStep(t *testing.T) {
	bus := &directiveTestBus{}
	store := &chatTestStore{}
	agent := &chatTestAgent{id: "campaign-coordinator"}
	am := NewAgentManager(bus, nil, store)
	am.agents[agent.id] = agent

	got, err := am.ChatWithAgent(context.Background(), agent.id, "run corpus")
	if err != nil {
		t.Fatalf("ChatWithAgent: %v", err)
	}
	if got != "ok" {
		t.Fatalf("ChatWithAgent result = %q, want ok", got)
	}
	if agent.calls != 1 || agent.directive != "run corpus" {
		t.Fatalf("board step calls=%d directive=%q", agent.calls, agent.directive)
	}
	if agent.runID == "" || agent.directiveEvent == "" || agent.directiveSource != runtimeagentcontrol.DirectiveEventType {
		t.Fatalf("board directive event = run:%q event:%q type:%q", agent.runID, agent.directiveEvent, agent.directiveSource)
	}
	eventCount := 0
	if bus.store != nil {
		eventCount = len(bus.store.events)
	}
	if eventCount != 1 {
		t.Fatalf("persisted directive events = %d, want 1", eventCount)
	}
	if bus.store.events[0].ID() != agent.directiveEvent || bus.store.events[0].RunID() != agent.runID {
		t.Fatalf("persisted directive event = %#v, board saw event=%q run=%q", bus.store.events[0], agent.directiveEvent, agent.runID)
	}
}

func TestAgentManager_SendDirectivePersistsCanonicalDirectiveEventBeforeBoardStep(t *testing.T) {
	runID := "00000000-0000-0000-0000-000000000701"
	bus := &directiveTestBus{}
	store := &directiveTargetStore{
		target: runtimeagentcontrol.RunTargetResolution{
			RunID: runID,
			Mode:  runtimeagentcontrol.RunResolutionActiveSession,
			ActiveSessions: []runtimeagentcontrol.ActiveSessionTarget{{
				SessionID: "00000000-0000-0000-0000-000000000801",
				RunID:     runID,
			}},
		},
	}
	agent := &chatTestAgent{id: "campaign-coordinator"}
	am := NewAgentManager(bus, nil, store)
	am.agents[agent.id] = agent

	result, err := am.SendDirective(context.Background(), runtimeagentcontrol.SendDirectiveRequest{
		AgentID:    agent.id,
		Directive:  "run corpus",
		Source:     runtimeagentcontrol.DirectiveSourceV1RPC,
		OperatorID: "operator-token",
	})
	if err != nil {
		t.Fatalf("SendDirective: %v", err)
	}
	if result.RunID != runID || result.RunIDResolution != runtimeagentcontrol.RunResolutionActiveSession || result.DirectiveEventID == "" {
		t.Fatalf("directive result = %#v", result)
	}
	if store.calls != 1 {
		t.Fatalf("target resolver calls = %d, want 1", store.calls)
	}
	eventCount := 0
	if bus.store != nil {
		eventCount = len(bus.store.events)
	}
	if eventCount != 1 {
		t.Fatalf("persisted directive events = %d, want 1", eventCount)
	}
	evt := bus.store.events[0]
	if string(evt.Type()) != runtimeagentcontrol.DirectiveEventType || evt.RunID() != runID || evt.ID() == "" {
		t.Fatalf("directive event = %#v", evt)
	}
	if agent.calls != 1 || agent.runID != runID || agent.directiveEvent != evt.ID() {
		t.Fatalf("board step saw calls=%d run=%q event=%q, want event %q", agent.calls, agent.runID, agent.directiveEvent, evt.ID())
	}
}

func TestAgentManager_SendDirectiveTargetErrorFailsBeforeBoardStep(t *testing.T) {
	bus := &directiveTestBus{}
	store := &directiveTargetStore{
		err: &runtimeagentcontrol.StateError{
			Err:     runtimeagentcontrol.ErrRunNotFound,
			AgentID: "campaign-coordinator",
			RunID:   "00000000-0000-0000-0000-000000000404",
		},
	}
	agent := &chatTestAgent{id: "campaign-coordinator"}
	am := NewAgentManager(bus, nil, store)
	am.agents[agent.id] = agent

	_, err := am.SendDirective(context.Background(), runtimeagentcontrol.SendDirectiveRequest{
		AgentID:   agent.id,
		Directive: "run corpus",
		RunID:     "00000000-0000-0000-0000-000000000404",
	})
	if err == nil {
		t.Fatal("SendDirective error = nil")
	}
	eventCount := 0
	if bus.store != nil {
		eventCount = len(bus.store.events)
	}
	if agent.calls != 0 || eventCount != 0 {
		t.Fatalf("side effects after target error: board=%d events=%d", agent.calls, eventCount)
	}
}

func TestAgentManager_SendDirectiveConcurrentSameKeyExecutesBoardStepOnce(t *testing.T) {
	runID := "00000000-0000-0000-0000-000000000711"
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	directiveStore := &directiveEventStore{}
	bus := &directiveTestBus{store: directiveStore}
	store := &directiveTargetStore{target: runtimeagentcontrol.RunTargetResolution{RunID: runID, Mode: runtimeagentcontrol.RunResolutionSpecified}}
	agent := &chatTestAgent{id: "campaign-coordinator", started: started, release: release}
	am := NewAgentManager(bus, nil, store)
	am.agents[agent.id] = agent
	req := runtimeagentcontrol.SendDirectiveRequest{
		AgentID:        agent.id,
		Directive:      "run corpus",
		ActorTokenID:   "operator-token",
		IdempotencyKey: "same-key",
		RequestHash:    "same-hash",
	}

	firstResult := make(chan runtimeagentcontrol.SendDirectiveResult, 1)
	firstErr := make(chan error, 1)
	go func() {
		result, err := am.SendDirective(context.Background(), req)
		firstResult <- result
		firstErr <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first BoardStep did not start")
	}

	if _, err := am.SendDirective(context.Background(), req); !errors.Is(err, runtimeagentcontrol.ErrDirectiveInProgress) {
		t.Fatalf("concurrent same-key error = %v, want in progress", err)
	}
	close(release)
	if err := <-firstErr; err != nil {
		t.Fatalf("first SendDirective: %v", err)
	}
	if result := <-firstResult; !result.OK || result.OperationID == "" {
		t.Fatalf("first result = %#v", result)
	}
	if agent.calls != 1 || len(directiveStore.events) != 1 {
		t.Fatalf("concurrent effects board=%d events=%d, want 1/1", agent.calls, len(directiveStore.events))
	}
}

func TestAgentManager_SendDirectiveCompletionRepairDoesNotRepeatBoardStep(t *testing.T) {
	directiveStore := &directiveEventStore{finalizeSuccessErr: errors.New("injected finalization failure")}
	bus := &directiveTestBus{store: directiveStore}
	store := &directiveTargetStore{target: runtimeagentcontrol.RunTargetResolution{RunID: "00000000-0000-0000-0000-000000000712", Mode: runtimeagentcontrol.RunResolutionSpecified}}
	agent := &chatTestAgent{id: "campaign-coordinator"}
	am := NewAgentManager(bus, nil, store)
	am.agents[agent.id] = agent
	req := runtimeagentcontrol.SendDirectiveRequest{AgentID: agent.id, Directive: "run corpus", ActorTokenID: "operator-token", IdempotencyKey: "completion-key", RequestHash: "completion-hash"}

	if _, err := am.SendDirective(context.Background(), req); !errors.Is(err, runtimeagentcontrol.ErrDirectiveCompletionPending) {
		t.Fatalf("first SendDirective error = %v, want completion pending", err)
	}
	operation, ok, err := directiveStore.LoadDirectiveOperationByKey(context.Background(), runtimeagentcontrol.DirectiveOperationMethod, req.ActorTokenID, req.IdempotencyKey)
	if err != nil || !ok || operation.State != runtimeagentcontrol.DirectiveOperationExecuted {
		t.Fatalf("operation after failed finalization = %#v ok=%v err=%v", operation, ok, err)
	}
	directiveStore.mu.Lock()
	directiveStore.finalizeSuccessErr = nil
	directiveStore.mu.Unlock()
	result, err := am.SendDirective(context.Background(), req)
	if err != nil {
		t.Fatalf("repair SendDirective: %v", err)
	}
	if !result.OK || result.OperationID != operation.OperationID || agent.calls != 1 || len(directiveStore.events) != 1 {
		t.Fatalf("repair result=%#v board=%d events=%d", result, agent.calls, len(directiveStore.events))
	}
}

func TestAgentManager_SendDirectiveResultPersistenceFailureNeverReadmitsBoardStep(t *testing.T) {
	directiveStore := &directiveEventStore{recordExecutedErr: errors.New("injected result persistence failure")}
	bus := &directiveTestBus{store: directiveStore}
	store := &directiveTargetStore{target: runtimeagentcontrol.RunTargetResolution{RunID: "00000000-0000-0000-0000-000000000713", Mode: runtimeagentcontrol.RunResolutionSpecified}}
	agent := &chatTestAgent{id: "campaign-coordinator"}
	am := NewAgentManager(bus, nil, store)
	am.agents[agent.id] = agent
	req := runtimeagentcontrol.SendDirectiveRequest{AgentID: agent.id, Directive: "run corpus", ActorTokenID: "operator-token", IdempotencyKey: "indeterminate-key", RequestHash: "indeterminate-hash"}

	if _, err := am.SendDirective(context.Background(), req); !errors.Is(err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate) {
		t.Fatalf("first SendDirective error = %v, want indeterminate", err)
	}
	operation, ok, err := directiveStore.LoadDirectiveOperationByKey(context.Background(), runtimeagentcontrol.DirectiveOperationMethod, req.ActorTokenID, req.IdempotencyKey)
	if err != nil || !ok || operation.State != runtimeagentcontrol.DirectiveOperationExecuting {
		t.Fatalf("durable operation after result failure = %#v ok=%v err=%v", operation, ok, err)
	}
	directiveStore.mu.Lock()
	operation.ExecutionLeaseExpiresAt = time.Now().UTC().Add(-time.Second)
	directiveStore.operations[operation.OperationID] = operation
	directiveStore.recordExecutedErr = nil
	directiveStore.mu.Unlock()
	if _, err := am.SendDirective(context.Background(), req); !errors.Is(err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate) {
		t.Fatalf("retry error = %v, want indeterminate", err)
	}
	if agent.calls != 1 || len(directiveStore.events) != 1 {
		t.Fatalf("indeterminate retry effects board=%d events=%d, want 1/1", agent.calls, len(directiveStore.events))
	}
}

func TestAgentManager_SendDirectiveExecutionFailureIsDurableAndReplaySafe(t *testing.T) {
	directiveStore := &directiveEventStore{}
	bus := &directiveTestBus{store: directiveStore}
	store := &directiveTargetStore{target: runtimeagentcontrol.RunTargetResolution{RunID: "00000000-0000-0000-0000-000000000714", Mode: runtimeagentcontrol.RunResolutionSpecified}}
	agent := &chatTestAgent{id: "campaign-coordinator", err: errors.New("provider failed")}
	am := NewAgentManager(bus, nil, store)
	am.agents[agent.id] = agent
	req := runtimeagentcontrol.SendDirectiveRequest{AgentID: agent.id, Directive: "run corpus", ActorTokenID: "operator-token", IdempotencyKey: "failure-key", RequestHash: "failure-hash"}

	for attempt := 0; attempt < 2; attempt++ {
		if _, err := am.SendDirective(context.Background(), req); !errors.Is(err, runtimeagentcontrol.ErrDirectiveExecutionFailed) {
			t.Fatalf("attempt %d error = %v, want execution failed", attempt+1, err)
		}
	}
	operation, ok, err := directiveStore.LoadDirectiveOperationByKey(context.Background(), runtimeagentcontrol.DirectiveOperationMethod, req.ActorTokenID, req.IdempotencyKey)
	if err != nil || !ok || operation.State != runtimeagentcontrol.DirectiveOperationFailed || operation.ErrorCode != "board_step_failed" {
		t.Fatalf("failed operation = %#v ok=%v err=%v", operation, ok, err)
	}
	if agent.calls != 1 || len(directiveStore.events) != 1 {
		t.Fatalf("failed replay effects board=%d events=%d, want 1/1", agent.calls, len(directiveStore.events))
	}
}

func TestAgentManager_ChatWithAgent_DeniesWhenRuntimeShutdownAdmissionClosed(t *testing.T) {
	agent := &chatTestAgent{id: "campaign-coordinator"}
	am := NewAgentManagerWithOptions(nil, nil, AgentManagerOptions{
		RuntimeShutdownAdmissionClosed: func() bool { return true },
	})
	am.agents[agent.id] = agent

	if _, err := am.ChatWithAgent(context.Background(), agent.id, "run corpus"); err == nil || err.Error() != "runtime shutting down" {
		t.Fatalf("ChatWithAgent err = %v, want runtime shutting down", err)
	}
	if agent.calls != 0 {
		t.Fatalf("board step calls = %d, want 0", agent.calls)
	}
}
