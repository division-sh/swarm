package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
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
func (s *chatTestStore) EnsureEntitySchema(context.Context, string) error { return nil }
func (s *chatTestStore) ResolveAgentDirectiveRunTarget(_ context.Context, identity runtimeagentidentity.Identity) (runtimeagentcontrol.RunTargetResolution, error) {
	return runtimeagentcontrol.RunTargetResolution{
		RunID: identity.RunID,
		Mode:  runtimeagentcontrol.RunResolutionSpecified,
	}, nil
}

type directiveTargetStore struct {
	chatTestStore
	target runtimeagentcontrol.RunTargetResolution
	err    error
	calls  int
}

func (s *directiveTargetStore) ResolveAgentDirectiveRunTarget(_ context.Context, identity runtimeagentidentity.Identity) (runtimeagentcontrol.RunTargetResolution, error) {
	s.calls++
	if s.err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, s.err
	}
	target := s.target
	if target.RunID == "" {
		target.RunID = identity.RunID
	}
	return target, nil
}

type directiveTestBus struct {
	direct     []events.Event
	store      *directiveEventStore
	admitErr   error
	admitCalls int
}

func (b *directiveTestBus) AdmitBundleSourceFact(ctx context.Context) (context.Context, error) {
	b.admitCalls++
	if b.admitErr != nil {
		return ctx, b.admitErr
	}
	return ctx, nil
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

func (b *directiveTestBus) managerTestDirectiveOperations() runtimeagentcontrol.DirectiveOperationStore {
	if b.store == nil {
		b.store = &directiveEventStore{}
	}
	return b.store
}

func (b *directiveTestBus) ResetInMemoryState() error { return nil }
func (b *directiveTestBus) LogRuntime(context.Context, runtimepipeline.RuntimeLogEntry) error {
	return nil
}

func installDirectiveTestAgent(t *testing.T, am *AgentManager, agent Agent, runID string) {
	t.Helper()
	rec := PersistedAgent{Config: managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live",
		ID:            agent.ID(),
		Identity:      directiveTestAgentIdentityForRun(t, runID, agent.ID()),
	}), Status: "active", HiredBy: "test", Topology: managerTestTopologyAdmission(t)}
	if err := am.lifecycle.registerExecution(testAuthorActivityContext(context.Background()), rec, false, agent, testManagerSubscriptionAdmission(t, rec.Config)); err != nil {
		t.Fatalf("register directive test agent: %v", err)
	}
	identity, err := rec.Config.ConcreteIdentity()
	if err != nil {
		t.Fatalf("resolve directive test agent identity: %v", err)
	}
	am.lifecycle.mu.Lock()
	cell := am.lifecycle.cells[identity.Normalize()]
	if cell == nil {
		am.lifecycle.mu.Unlock()
		t.Fatalf("directive test lifecycle cell %s is missing", identity.Description())
	}
	cell.phase = AgentLifecycleRunning
	cell.runMode = AgentRunModeStandard
	am.lifecycle.mu.Unlock()
}

func directiveTestAgentIdentity(t testing.TB, agentID string) runtimeagentidentity.Identity {
	t.Helper()
	return runtimeagentidentitytest.RootRuntime(t, agentID, "manager.directive_test")
}

func directiveTestAgentIdentityForRun(t testing.TB, runID, agentID string) runtimeagentidentity.Identity {
	t.Helper()
	return runtimeagentidentitytest.RootRuntimeForRun(t, runID, agentID, "manager.directive_test")
}

type directiveEventStore struct {
	mu                   sync.Mutex
	events               []events.Event
	admittedEvents       map[string]events.AdmittedEvent
	operations           map[string]runtimeagentcontrol.DirectiveOperation
	recordExecutedErr    error
	finalizeSuccessErr   error
	renewStarted         chan struct{}
	renewFinished        chan struct{}
	renewRelease         <-chan struct{}
	ignoreRenewContext   bool
	mutationGate         *sync.Mutex
	recordExecutedCalls  int
	finalizeFailureCalls int
	reconcileCalls       int
}

func (s *directiveEventStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return runtimebustest.CommitPublish(ctx, command, nil, func(_ context.Context, req runtimebus.CommitPublishRequest) error {
		s.events = append(s.events, req.Event.Event())
		return nil
	})
}
func (*directiveEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, runtimebus.ErrAuthoritativeRecipientManifestUnavailable
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
	s.events = append(s.events, req.Event.Event())
	if s.admittedEvents == nil {
		s.admittedEvents = map[string]events.AdmittedEvent{}
	}
	s.admittedEvents[req.Event.ID()] = req.Event
	return runtimeagentcontrol.DirectiveOperationReservation{Operation: op, Created: true}, nil
}

func (s *directiveEventStore) AdmitDirectiveExecution(_ context.Context, req runtimeagentcontrol.DirectiveExecutionAdmissionRequest) (runtimeagentcontrol.DirectiveExecutionAdmission, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op := s.operations[req.OperationID]
	if op.State != runtimeagentcontrol.DirectiveOperationPrepared {
		return runtimeagentcontrol.DirectiveExecutionAdmission{Operation: op}, runtimeagentcontrol.ErrorForDirectiveOperation(op)
	}
	event, found := s.admittedEvents[op.DirectiveEventID]
	if !found {
		return runtimeagentcontrol.DirectiveExecutionAdmission{}, fmt.Errorf("directive event %s not found", op.DirectiveEventID)
	}
	if err := req.ExecutionPosture.Admit(event.Event().ExecutionMode(), "prepared directive execution"); err != nil {
		return runtimeagentcontrol.DirectiveExecutionAdmission{}, err
	}
	op.State = runtimeagentcontrol.DirectiveOperationExecuting
	op.ExecutionOwnerID = req.OwnerID
	op.ExecutionLeaseExpiresAt = req.Now.Add(req.Lease)
	op.ExecutionAdmittedAt, op.UpdatedAt = req.Now, req.Now
	s.operations[req.OperationID] = op
	return runtimeagentcontrol.DirectiveExecutionAdmission{Operation: op, Event: event}, nil
}

func (s *directiveEventStore) RenewDirectiveExecutionLease(ctx context.Context, _ string, _ string, _ time.Time, _ time.Duration) error {
	unlock := s.lockMutationGate()
	defer unlock()
	defer func() {
		if s.renewFinished != nil {
			select {
			case s.renewFinished <- struct{}{}:
			default:
			}
		}
	}()
	if s.renewStarted != nil {
		select {
		case s.renewStarted <- struct{}{}:
		default:
		}
	}
	if s.renewRelease == nil {
		return nil
	}
	if s.ignoreRenewContext {
		<-s.renewRelease
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.renewRelease:
		return nil
	}
}

func (s *directiveEventStore) RecordDirectiveExecuted(_ context.Context, operationID, ownerID string, response json.RawMessage, now time.Time) (runtimeagentcontrol.DirectiveOperation, error) {
	unlock := s.lockMutationGate()
	defer unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordExecutedCalls++
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
	unlock := s.lockMutationGate()
	defer unlock()
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

func (s *directiveEventStore) FinalizeDirectiveFailure(_ context.Context, operationID, ownerID string, failure runtimefailures.Envelope, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperation, error) {
	unlock := s.lockMutationGate()
	defer unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finalizeFailureCalls++
	op := s.operations[operationID]
	if op.State != runtimeagentcontrol.DirectiveOperationExecuting || op.ExecutionOwnerID != ownerID {
		return op, runtimeagentcontrol.ErrorForDirectiveOperation(op)
	}
	op.State = runtimeagentcontrol.DirectiveOperationFailed
	op.Failure = runtimefailures.CloneEnvelope(&failure)
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
	unlock := s.lockMutationGate()
	defer unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconcileCalls++
	op, ok := s.operations[operationID]
	if !ok {
		return runtimeagentcontrol.DirectiveOperation{}, false, nil
	}
	if (op.State == runtimeagentcontrol.DirectiveOperationSucceeded || op.State == runtimeagentcontrol.DirectiveOperationFailed) && !op.ExpiresAt.IsZero() && !op.ExpiresAt.After(now) {
		delete(s.operations, operationID)
		return runtimeagentcontrol.DirectiveOperation{}, false, nil
	}
	if op.State == runtimeagentcontrol.DirectiveOperationExecuting && !op.ExecutionLeaseExpiresAt.After(now) {
		op.State = runtimeagentcontrol.DirectiveOperationIndeterminate
		failure := runtimeagentcontrol.DirectiveExecutionLeaseExpiredFailure()
		op.Failure = &failure
		op.ExecutionLeaseExpiresAt = time.Time{}
		op.UpdatedAt = now
		s.operations[operationID] = op
	}
	return op, true, nil
}

func (s *directiveEventStore) reconcileCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reconcileCalls
}

func (s *directiveEventStore) lockMutationGate() func() {
	if s.mutationGate == nil {
		return func() {}
	}
	s.mutationGate.Lock()
	return s.mutationGate.Unlock
}

func (s *directiveEventStore) recordExecutedCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recordExecutedCalls
}

func (s *directiveEventStore) finalizeFailureCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finalizeFailureCalls
}

func TestConsumeProviderSettledDirectiveUsesExactDurableTerminalState(t *testing.T) {
	operationID := "00000000-0000-0000-0000-000000000801"
	ownerID := "00000000-0000-0000-0000-000000000802"
	failure := runtimeagentcontrol.DirectiveExecutionLeaseExpiredFailure()
	store := &directiveEventStore{operations: map[string]runtimeagentcontrol.DirectiveOperation{
		operationID: {
			OperationID: operationID, ExecutionOwnerID: ownerID,
			State: runtimeagentcontrol.DirectiveOperationIndeterminate, Failure: &failure,
		},
	}}
	origin := runtimeagentcontrol.DirectiveExecutionOrigin{OperationID: operationID, ExecutionOwnerID: ownerID}
	completionOrigin, err := runtimeeffects.DirectiveCompletionOrigin(origin)
	if err != nil {
		t.Fatalf("directive completion origin: %v", err)
	}
	terminal, err := consumeProviderSettledDirective(context.Background(), store, store.operations[operationID], origin, runtimeeffects.CompletionSettlementObservation{
		AttemptID: "00000000-0000-0000-0000-000000000803", Disposition: runtimeeffects.CompletionSettlementDrained,
		Origin: completionOrigin, OriginSettled: true,
	})
	if !terminal || !errors.Is(err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate) {
		t.Fatalf("consume provider-settled directive terminal=%v err=%v", terminal, err)
	}
	if got := store.finalizeFailureCallCount(); got != 0 {
		t.Fatalf("provider-settled directive secondary failure calls=%d, want 0", got)
	}
}

var _ runtimeagentcontrol.DirectiveOperationStore = (*directiveEventStore)(nil)

func TestAgentManager_SendDirectivePersistsCanonicalDirectiveEventBeforeBoardStep(t *testing.T) {
	runID := "00000000-0000-0000-0000-000000000701"
	bus := &directiveTestBus{}
	store := &directiveTargetStore{
		target: runtimeagentcontrol.RunTargetResolution{
			RunID: runID,
			Mode:  runtimeagentcontrol.RunResolutionSpecified,
		},
	}
	agent := &chatTestAgent{id: "campaign-coordinator"}
	am := newTestAgentManager(t, bus, nil, store)
	installDirectiveTestAgent(t, am, agent, runID)

	result, err := am.SendDirective(testAuthorActivityContext(context.Background()), runtimeagentcontrol.SendDirectiveRequest{
		AgentID:    agent.id,
		Directive:  "run corpus",
		RunID:      runID,
		Source:     runtimeagentcontrol.DirectiveSourceV1RPC,
		OperatorID: "operator-token",
	})
	if err != nil {
		t.Fatalf("SendDirective: %v", err)
	}
	if result.RunID != runID || result.RunIDResolution != runtimeagentcontrol.RunResolutionSpecified || result.DirectiveEventID == "" {
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
	am := newTestAgentManager(t, bus, nil, store)
	installDirectiveTestAgent(t, am, agent, "00000000-0000-0000-0000-000000000404")

	_, err := am.SendDirective(testAuthorActivityContext(context.Background()), runtimeagentcontrol.SendDirectiveRequest{
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
	am := newTestAgentManager(t, bus, nil, store)
	installDirectiveTestAgent(t, am, agent, store.target.RunID)
	req := runtimeagentcontrol.SendDirectiveRequest{
		AgentID:        agent.id,
		RunID:          store.target.RunID,
		Directive:      "run corpus",
		ActorTokenID:   "operator-token",
		IdempotencyKey: "same-key",
		RequestHash:    "same-hash",
	}

	firstResult := make(chan runtimeagentcontrol.SendDirectiveResult, 1)
	firstErr := make(chan error, 1)
	go func() {
		result, err := am.SendDirective(testAuthorActivityContext(context.Background()), req)
		firstResult <- result
		firstErr <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first BoardStep did not start")
	}

	if _, err := am.SendDirective(testAuthorActivityContext(context.Background()), req); !errors.Is(err, runtimeagentcontrol.ErrDirectiveInProgress) {
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

func TestAgentManager_SendDirectiveHeartbeatTimeoutAvoidsSerializedOutcomeBoundary(t *testing.T) {
	runID := "00000000-0000-0000-0000-000000000716"
	boardStarted := make(chan struct{}, 1)
	releaseBoard := make(chan struct{})
	renewStarted := make(chan struct{}, 1)
	renewFinished := make(chan struct{}, 1)
	releaseRenew := make(chan struct{})
	mutationGate := &sync.Mutex{}
	directiveStore := &directiveEventStore{
		renewStarted:       renewStarted,
		renewFinished:      renewFinished,
		renewRelease:       releaseRenew,
		ignoreRenewContext: true,
		mutationGate:       mutationGate,
	}
	bus := &directiveTestBus{store: directiveStore}
	store := &directiveTargetStore{target: runtimeagentcontrol.RunTargetResolution{RunID: runID, Mode: runtimeagentcontrol.RunResolutionSpecified}}
	agent := &chatTestAgent{id: "campaign-coordinator", started: boardStarted, release: releaseBoard}
	am := newTestAgentManager(t, bus, nil, store)
	am.directiveHeartbeat = directiveHeartbeatConfig{
		interval:        time.Millisecond,
		renewalTimeout:  2 * time.Millisecond,
		shutdownTimeout: 5 * time.Millisecond,
	}
	installDirectiveTestAgent(t, am, agent, store.target.RunID)

	req := runtimeagentcontrol.SendDirectiveRequest{
		AgentID:        agent.id,
		RunID:          store.target.RunID,
		Directive:      "run corpus",
		ActorTokenID:   "operator-token",
		IdempotencyKey: "stalled-heartbeat",
		RequestHash:    "stalled-heartbeat-hash",
	}
	resultCh := make(chan runtimeagentcontrol.SendDirectiveResult, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := am.SendDirective(testAuthorActivityContext(context.Background()), req)
		resultCh <- result
		errCh <- err
	}()

	select {
	case <-boardStarted:
	case <-time.After(time.Second):
		t.Fatal("BoardStep did not start")
	}
	select {
	case <-renewStarted:
	case <-time.After(time.Second):
		t.Fatal("heartbeat renewal did not start")
	}
	close(releaseBoard)

	select {
	case err := <-errCh:
		if !errors.Is(err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate) {
			t.Fatalf("SendDirective error = %v, want indeterminate", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("SendDirective entered the serialized outcome boundary behind stalled renewal")
	}
	if result := <-resultCh; result.OK {
		t.Fatalf("result=%#v, want no success after heartbeat shutdown timeout", result)
	}
	operation, ok, err := directiveStore.LoadDirectiveOperationByKey(testAuthorActivityContext(context.Background()), runtimeagentcontrol.DirectiveOperationMethod, req.ActorTokenID, req.IdempotencyKey)
	if err != nil || !ok || operation.State != runtimeagentcontrol.DirectiveOperationExecuting {
		t.Fatalf("durable operation=%#v ok=%v err=%v", operation, ok, err)
	}
	if directiveStore.recordExecutedCallCount() != 0 || directiveStore.finalizeFailureCallCount() != 0 || agent.calls != 1 {
		t.Fatalf("post-timeout result/failure persistence calls=%d/%d BoardStep calls=%d, want 0/0/1", directiveStore.recordExecutedCallCount(), directiveStore.finalizeFailureCallCount(), agent.calls)
	}
	if _, err := am.SendDirective(testAuthorActivityContext(context.Background()), req); !errors.Is(err, runtimeagentcontrol.ErrDirectiveInProgress) {
		t.Fatalf("same-key retry before lease expiry error = %v, want in progress", err)
	}
	if agent.calls != 1 {
		t.Fatalf("same-key retry BoardStep calls = %d, want 1", agent.calls)
	}

	close(releaseRenew)
	select {
	case <-renewFinished:
	case <-time.After(time.Second):
		t.Fatal("stalled renewal did not finish after release")
	}
	directiveStore.mu.Lock()
	operation = directiveStore.operations[operation.OperationID]
	operation.ExecutionLeaseExpiresAt = time.Now().UTC().Add(-time.Second)
	directiveStore.operations[operation.OperationID] = operation
	directiveStore.mu.Unlock()
	reconciled, ok, err := directiveStore.ReconcileDirectiveOperation(testAuthorActivityContext(context.Background()), operation.OperationID, time.Now().UTC(), directiveOperationTTL)
	if err != nil || !ok || reconciled.State != runtimeagentcontrol.DirectiveOperationIndeterminate {
		t.Fatalf("reconciled operation=%#v ok=%v err=%v", reconciled, ok, err)
	}
	if _, err := am.SendDirective(testAuthorActivityContext(context.Background()), req); !errors.Is(err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate) {
		t.Fatalf("same-key retry after lease reconciliation error = %v, want indeterminate", err)
	}
	if agent.calls != 1 || directiveStore.recordExecutedCallCount() != 0 || directiveStore.finalizeFailureCallCount() != 0 {
		t.Fatalf("final BoardStep/result/failure persistence calls = %d/%d/%d, want 1/0/0", agent.calls, directiveStore.recordExecutedCallCount(), directiveStore.finalizeFailureCallCount())
	}
}

func TestAgentManager_SendDirectiveHeartbeatTimeoutSkipsSerializedFailurePersistence(t *testing.T) {
	boardStarted := make(chan struct{}, 1)
	releaseBoard := make(chan struct{})
	renewStarted := make(chan struct{}, 1)
	renewFinished := make(chan struct{}, 1)
	releaseRenew := make(chan struct{})
	directiveStore := &directiveEventStore{
		renewStarted:       renewStarted,
		renewFinished:      renewFinished,
		renewRelease:       releaseRenew,
		ignoreRenewContext: true,
		mutationGate:       &sync.Mutex{},
	}
	bus := &directiveTestBus{store: directiveStore}
	store := &directiveTargetStore{target: runtimeagentcontrol.RunTargetResolution{RunID: "00000000-0000-0000-0000-000000000718", Mode: runtimeagentcontrol.RunResolutionSpecified}}
	agent := &chatTestAgent{id: "campaign-coordinator", started: boardStarted, release: releaseBoard, err: errors.New("provider failed")}
	am := newTestAgentManager(t, bus, nil, store)
	am.directiveHeartbeat = directiveHeartbeatConfig{
		interval:        time.Millisecond,
		renewalTimeout:  2 * time.Millisecond,
		shutdownTimeout: 5 * time.Millisecond,
	}
	installDirectiveTestAgent(t, am, agent, store.target.RunID)

	errCh := make(chan error, 1)
	go func() {
		_, err := am.SendDirective(testAuthorActivityContext(context.Background()), runtimeagentcontrol.SendDirectiveRequest{
			AgentID:        agent.id,
			RunID:          store.target.RunID,
			Directive:      "run corpus",
			ActorTokenID:   "operator-token",
			IdempotencyKey: "stalled-failure-heartbeat",
			RequestHash:    "stalled-failure-heartbeat-hash",
		})
		errCh <- err
	}()
	select {
	case <-boardStarted:
	case <-time.After(time.Second):
		t.Fatal("BoardStep did not start")
	}
	select {
	case <-renewStarted:
	case <-time.After(time.Second):
		t.Fatal("heartbeat renewal did not start")
	}
	close(releaseBoard)
	select {
	case err := <-errCh:
		if !errors.Is(err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate) {
			t.Fatalf("SendDirective error = %v, want indeterminate", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("SendDirective entered serialized failure persistence behind stalled renewal")
	}
	if directiveStore.finalizeFailureCallCount() != 0 || directiveStore.recordExecutedCallCount() != 0 || agent.calls != 1 {
		t.Fatalf("post-timeout failure/result persistence calls=%d/%d BoardStep calls=%d, want 0/0/1", directiveStore.finalizeFailureCallCount(), directiveStore.recordExecutedCallCount(), agent.calls)
	}
	close(releaseRenew)
	select {
	case <-renewFinished:
	case <-time.After(time.Second):
		t.Fatal("stalled renewal did not finish after release")
	}
}

func TestAgentManager_SendDirectiveCompliantHeartbeatReleasesSerializedOutcomeBoundary(t *testing.T) {
	for _, tc := range []struct {
		name      string
		boardErr  error
		wantState runtimeagentcontrol.DirectiveOperationState
		wantErr   error
	}{
		{name: "success", wantState: runtimeagentcontrol.DirectiveOperationSucceeded},
		{name: "failure", boardErr: errors.New("provider failed"), wantState: runtimeagentcontrol.DirectiveOperationFailed, wantErr: runtimeagentcontrol.ErrDirectiveExecutionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			boardStarted := make(chan struct{}, 1)
			releaseBoard := make(chan struct{})
			renewStarted := make(chan struct{}, 1)
			renewFinished := make(chan struct{}, 1)
			directiveStore := &directiveEventStore{
				renewStarted:  renewStarted,
				renewFinished: renewFinished,
				renewRelease:  make(chan struct{}),
				mutationGate:  &sync.Mutex{},
			}
			bus := &directiveTestBus{store: directiveStore}
			store := &directiveTargetStore{target: runtimeagentcontrol.RunTargetResolution{RunID: "00000000-0000-0000-0000-000000000717", Mode: runtimeagentcontrol.RunResolutionSpecified}}
			agent := &chatTestAgent{id: "campaign-coordinator", started: boardStarted, release: releaseBoard, err: tc.boardErr}
			am := newTestAgentManager(t, bus, nil, store)
			am.directiveHeartbeat = directiveHeartbeatConfig{
				interval:        time.Millisecond,
				renewalTimeout:  100 * time.Millisecond,
				shutdownTimeout: 100 * time.Millisecond,
			}
			installDirectiveTestAgent(t, am, agent, store.target.RunID)
			req := runtimeagentcontrol.SendDirectiveRequest{
				AgentID:        agent.id,
				RunID:          store.target.RunID,
				Directive:      "run corpus",
				ActorTokenID:   "operator-token",
				IdempotencyKey: "compliant-heartbeat-" + tc.name,
				RequestHash:    "compliant-heartbeat-hash-" + tc.name,
			}
			resultCh := make(chan runtimeagentcontrol.SendDirectiveResult, 1)
			errCh := make(chan error, 1)
			go func() {
				result, err := am.SendDirective(testAuthorActivityContext(context.Background()), req)
				resultCh <- result
				errCh <- err
			}()
			select {
			case <-boardStarted:
			case <-time.After(time.Second):
				t.Fatal("BoardStep did not start")
			}
			select {
			case <-renewStarted:
			case <-time.After(time.Second):
				t.Fatal("heartbeat renewal did not start")
			}
			close(releaseBoard)
			err := <-errCh
			result := <-resultCh
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("SendDirective error = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil && (!result.OK || result.OperationID == "") {
				t.Fatalf("success result = %#v", result)
			}
			select {
			case <-renewFinished:
			default:
				t.Fatal("context-compliant renewal did not release before outcome persistence")
			}
			operation, ok, loadErr := directiveStore.LoadDirectiveOperationByKey(testAuthorActivityContext(context.Background()), runtimeagentcontrol.DirectiveOperationMethod, req.ActorTokenID, req.IdempotencyKey)
			if loadErr != nil || !ok || operation.State != tc.wantState {
				t.Fatalf("durable operation=%#v ok=%v err=%v", operation, ok, loadErr)
			}
			if agent.calls != 1 {
				t.Fatalf("BoardStep calls = %d, want 1", agent.calls)
			}
		})
	}
}

func TestAgentManager_SendDirectiveCompletionRepairDoesNotRepeatBoardStep(t *testing.T) {
	directiveStore := &directiveEventStore{finalizeSuccessErr: errors.New("injected finalization failure")}
	bus := &directiveTestBus{store: directiveStore}
	store := &directiveTargetStore{target: runtimeagentcontrol.RunTargetResolution{RunID: "00000000-0000-0000-0000-000000000712", Mode: runtimeagentcontrol.RunResolutionSpecified}}
	agent := &chatTestAgent{id: "campaign-coordinator"}
	am := newTestAgentManager(t, bus, nil, store)
	installDirectiveTestAgent(t, am, agent, store.target.RunID)
	req := runtimeagentcontrol.SendDirectiveRequest{AgentID: agent.id, RunID: store.target.RunID, Directive: "run corpus", ActorTokenID: "operator-token", IdempotencyKey: "completion-key", RequestHash: "completion-hash"}

	if _, err := am.SendDirective(testAuthorActivityContext(context.Background()), req); !errors.Is(err, runtimeagentcontrol.ErrDirectiveCompletionPending) {
		t.Fatalf("first SendDirective error = %v, want completion pending", err)
	}
	operation, ok, err := directiveStore.LoadDirectiveOperationByKey(testAuthorActivityContext(context.Background()), runtimeagentcontrol.DirectiveOperationMethod, req.ActorTokenID, req.IdempotencyKey)
	if err != nil || !ok || operation.State != runtimeagentcontrol.DirectiveOperationExecuted {
		t.Fatalf("operation after failed finalization = %#v ok=%v err=%v", operation, ok, err)
	}
	directiveStore.mu.Lock()
	directiveStore.finalizeSuccessErr = nil
	directiveStore.mu.Unlock()
	result, err := am.SendDirective(testAuthorActivityContext(context.Background()), req)
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
	am := newTestAgentManager(t, bus, nil, store)
	installDirectiveTestAgent(t, am, agent, store.target.RunID)
	req := runtimeagentcontrol.SendDirectiveRequest{AgentID: agent.id, RunID: store.target.RunID, Directive: "run corpus", ActorTokenID: "operator-token", IdempotencyKey: "indeterminate-key", RequestHash: "indeterminate-hash"}

	if _, err := am.SendDirective(testAuthorActivityContext(context.Background()), req); !errors.Is(err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate) {
		t.Fatalf("first SendDirective error = %v, want indeterminate", err)
	}
	operation, ok, err := directiveStore.LoadDirectiveOperationByKey(testAuthorActivityContext(context.Background()), runtimeagentcontrol.DirectiveOperationMethod, req.ActorTokenID, req.IdempotencyKey)
	if err != nil || !ok || operation.State != runtimeagentcontrol.DirectiveOperationExecuting {
		t.Fatalf("durable operation after result failure = %#v ok=%v err=%v", operation, ok, err)
	}
	directiveStore.mu.Lock()
	operation.ExecutionLeaseExpiresAt = time.Now().UTC().Add(-time.Second)
	directiveStore.operations[operation.OperationID] = operation
	directiveStore.recordExecutedErr = nil
	directiveStore.mu.Unlock()
	if _, err := am.SendDirective(testAuthorActivityContext(context.Background()), req); !errors.Is(err, runtimeagentcontrol.ErrDirectiveOutcomeIndeterminate) {
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
	am := newTestAgentManager(t, bus, nil, store)
	installDirectiveTestAgent(t, am, agent, store.target.RunID)
	req := runtimeagentcontrol.SendDirectiveRequest{AgentID: agent.id, RunID: store.target.RunID, Directive: "run corpus", ActorTokenID: "operator-token", IdempotencyKey: "failure-key", RequestHash: "failure-hash"}

	for attempt := 0; attempt < 2; attempt++ {
		if _, err := am.SendDirective(testAuthorActivityContext(context.Background()), req); !errors.Is(err, runtimeagentcontrol.ErrDirectiveExecutionFailed) {
			t.Fatalf("attempt %d error = %v, want execution failed", attempt+1, err)
		}
	}
	operation, ok, err := directiveStore.LoadDirectiveOperationByKey(testAuthorActivityContext(context.Background()), runtimeagentcontrol.DirectiveOperationMethod, req.ActorTokenID, req.IdempotencyKey)
	if err != nil || !ok || operation.State != runtimeagentcontrol.DirectiveOperationFailed || operation.Failure == nil || operation.Failure.Detail.Code != runtimeagentcontrol.DirectiveBoardStepFailedDetail {
		t.Fatalf("failed operation = %#v ok=%v err=%v", operation, ok, err)
	}
	if agent.calls != 1 || len(directiveStore.events) != 1 {
		t.Fatalf("failed replay effects board=%d events=%d, want 1/1", agent.calls, len(directiveStore.events))
	}
}

func TestAgentManager_SendDirectiveExpiredTerminalKeyStartsFreshOperation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		firstErr error
	}{
		{name: "succeeded"},
		{name: "failed", firstErr: errors.New("provider failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			directiveStore := &directiveEventStore{}
			bus := &directiveTestBus{store: directiveStore}
			store := &directiveTargetStore{target: runtimeagentcontrol.RunTargetResolution{RunID: "00000000-0000-0000-0000-000000000715", Mode: runtimeagentcontrol.RunResolutionSpecified}}
			agent := &chatTestAgent{id: "campaign-coordinator", err: tc.firstErr}
			am := newTestAgentManager(t, bus, nil, store)
			installDirectiveTestAgent(t, am, agent, store.target.RunID)
			firstReq := runtimeagentcontrol.SendDirectiveRequest{AgentID: agent.id, RunID: store.target.RunID, Directive: "old directive", ActorTokenID: "operator-token", IdempotencyKey: "expired-key", RequestHash: "old-hash"}

			firstResult, err := am.SendDirective(testAuthorActivityContext(context.Background()), firstReq)
			if tc.firstErr == nil && err != nil {
				t.Fatalf("first SendDirective: %v", err)
			}
			if tc.firstErr != nil && !errors.Is(err, runtimeagentcontrol.ErrDirectiveExecutionFailed) {
				t.Fatalf("first SendDirective error = %v, want execution failed", err)
			}
			operation, ok, err := directiveStore.LoadDirectiveOperationByKey(testAuthorActivityContext(context.Background()), runtimeagentcontrol.DirectiveOperationMethod, firstReq.ActorTokenID, firstReq.IdempotencyKey)
			if err != nil || !ok {
				t.Fatalf("load first operation ok=%v err=%v", ok, err)
			}
			directiveStore.mu.Lock()
			operation.ExpiresAt = time.Now().UTC().Add(-time.Second)
			directiveStore.operations[operation.OperationID] = operation
			directiveStore.mu.Unlock()
			agent.err = nil

			secondResult, err := am.SendDirective(testAuthorActivityContext(context.Background()), runtimeagentcontrol.SendDirectiveRequest{AgentID: agent.id, RunID: store.target.RunID, Directive: "new directive", ActorTokenID: firstReq.ActorTokenID, IdempotencyKey: firstReq.IdempotencyKey, RequestHash: "new-hash"})
			if err != nil {
				t.Fatalf("fresh SendDirective after expiry: %v", err)
			}
			if !secondResult.OK || secondResult.OperationID == "" || secondResult.OperationID == firstResult.OperationID || agent.calls != 2 || len(directiveStore.events) != 2 {
				t.Fatalf("fresh result=%#v first=%#v calls=%d events=%d", secondResult, firstResult, agent.calls, len(directiveStore.events))
			}
		})
	}
}

func TestAgentManager_SendDirectiveRejectsSourceBeforeExpiredReplayReconciliation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state runtimeagentcontrol.DirectiveOperationState
	}{
		{name: "terminal", state: runtimeagentcontrol.DirectiveOperationSucceeded},
		{name: "executing", state: runtimeagentcontrol.DirectiveOperationExecuting},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC()
			operation := runtimeagentcontrol.DirectiveOperation{
				OperationID:    "11111111-1111-4111-8111-111111111199",
				Method:         runtimeagentcontrol.DirectiveOperationMethod,
				ActorTokenID:   "operator-token",
				IdempotencyKey: "expired-source-key",
				RequestHash:    "request-hash",
				AgentIdentity:  directiveTestAgentIdentity(t, "campaign-coordinator"),
				Directive:      "inspect",
				State:          tc.state,
			}
			if tc.state == runtimeagentcontrol.DirectiveOperationSucceeded {
				operation.ExpiresAt = now.Add(-time.Second)
			} else {
				operation.ExecutionLeaseExpiresAt = now.Add(-time.Second)
			}
			directiveStore := &directiveEventStore{
				operations: map[string]runtimeagentcontrol.DirectiveOperation{
					operation.OperationID: operation,
				},
			}
			admissionErr := errors.New("event bus bundle source fact conflicts with manager context")
			bus := &directiveTestBus{store: directiveStore, admitErr: admissionErr}
			store := &directiveTargetStore{target: runtimeagentcontrol.RunTargetResolution{
				RunID: "00000000-0000-0000-0000-000000000799",
				Mode:  runtimeagentcontrol.RunResolutionSpecified,
			}}
			agent := &chatTestAgent{id: operation.AgentID()}
			am := newTestAgentManager(t, bus, nil, store)
			installDirectiveTestAgent(t, am, agent, store.target.RunID)

			_, err := am.SendDirective(testAuthorActivityContext(context.Background()), runtimeagentcontrol.SendDirectiveRequest{
				AgentID:        operation.AgentID(),
				RunID:          store.target.RunID,
				FlowInstance:   operation.FlowInstance(),
				Directive:      operation.Directive,
				ActorTokenID:   operation.ActorTokenID,
				IdempotencyKey: operation.IdempotencyKey,
				RequestHash:    operation.RequestHash,
			})
			if !errors.Is(err, admissionErr) {
				t.Fatalf("SendDirective error = %v, want source admission error", err)
			}
			if bus.admitCalls != 1 {
				t.Fatalf("source admission calls = %d, want 1", bus.admitCalls)
			}
			if got := directiveStore.reconcileCallCount(); got != 0 {
				t.Fatalf("reconciliation calls = %d, want 0", got)
			}
			unchanged, ok, loadErr := directiveStore.LoadDirectiveOperationByKey(
				context.Background(),
				operation.Method,
				operation.ActorTokenID,
				operation.IdempotencyKey,
			)
			if loadErr != nil || !ok {
				t.Fatalf("load unchanged operation ok=%v err=%v", ok, loadErr)
			}
			if unchanged.State != operation.State ||
				!unchanged.ExpiresAt.Equal(operation.ExpiresAt) ||
				!unchanged.ExecutionLeaseExpiresAt.Equal(operation.ExecutionLeaseExpiresAt) {
				t.Fatalf("operation mutated before source admission: got %#v want %#v", unchanged, operation)
			}
		})
	}
}
