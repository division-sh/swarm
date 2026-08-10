package runtime

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type recoveryGuardManagerStore struct {
	agents []runtimemanager.PersistedAgent
}

func (*recoveryGuardManagerStore) CommitAgentLifecycleTransition(_ context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	return startupRecoveryLifecycleResult(req), nil
}

func (s *recoveryGuardManagerStore) UpsertAgent(context.Context, runtimemanager.PersistedAgent) error {
	return nil
}

func (s *recoveryGuardManagerStore) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return append([]runtimemanager.PersistedAgent(nil), s.agents...), nil
}

func (*recoveryGuardManagerStore) EnsureEntitySchema(context.Context, string) error { return nil }

type recoveryGuardEventStore struct {
	runtimeagentcontrol.DirectiveOperationStore
	missing                 []events.PersistedReplayEvent
	routes                  []runtimeflowidentity.Route
	directiveReconcileCalls atomic.Int32
	directiveReconcileErr   error
}

type recoveryGuardEventLease struct{}

func (recoveryGuardEventLease) Release(context.Context) error { return nil }

func (*recoveryGuardEventStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return runtimebustest.CommitPublishNoop(ctx, command)
}
func (*recoveryGuardEventStore) UpsertCommittedReplayScope(context.Context, string, runtimepipelineobligation.CommittedScope) error {
	return nil
}
func (*recoveryGuardEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}
func (*recoveryGuardEventStore) SupportsPersistedReplay() bool { return true }
func (*recoveryGuardEventStore) UpsertFlowInstanceRoute(context.Context, runtimebus.FlowInstanceRouteRecord) error {
	return nil
}
func (*recoveryGuardEventStore) DeleteFlowInstanceRoute(context.Context, runtimeflowidentity.Route) error {
	return nil
}
func (s *recoveryGuardEventStore) ListFlowInstanceRoutes(context.Context) ([]runtimeflowidentity.Route, error) {
	return append([]runtimeflowidentity.Route(nil), s.routes...), nil
}
func (s *recoveryGuardEventStore) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error) {
	return append([]events.PersistedReplayEvent(nil), s.missing...), nil
}
func (*recoveryGuardEventStore) ClaimPipelineReplay(context.Context, string) (runtimeownership.Lease, bool, error) {
	return recoveryGuardEventLease{}, true, nil
}
func (*recoveryGuardEventStore) ClaimPipelinePublication(context.Context, string) (runtimeownership.Lease, bool, error) {
	return recoveryGuardEventLease{}, true, nil
}
func (s *recoveryGuardEventStore) ReconcileDirectiveOperations(context.Context, time.Time, time.Duration) (runtimeagentcontrol.DirectiveOperationReconcileResult, error) {
	s.directiveReconcileCalls.Add(1)
	return runtimeagentcontrol.DirectiveOperationReconcileResult{}, s.directiveReconcileErr
}

type minimalRuntimeEventStore struct{}

func (*minimalRuntimeEventStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return runtimebustest.CommitPublishNoop(ctx, command)
}
func (*minimalRuntimeEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}
func (*minimalRuntimeEventStore) SupportsPersistedReplay() bool { return false }

type recoveryDisabledScheduleStore struct {
	active      []runtimegenericschedule.Activation
	obligations *runtimetimerobligation.Snapshot
	loadCalls   atomic.Int32
}

func (*recoveryDisabledScheduleStore) AdmitGenericSchedule(context.Context, runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.AdmissionResult, error) {
	return runtimegenericschedule.AdmissionResult{}, errors.New("unexpected generic schedule admission")
}
func (s *recoveryDisabledScheduleStore) LoadGenericScheduleActivation(_ context.Context, activationID string) (runtimegenericschedule.Activation, bool, error) {
	for _, activation := range s.active {
		if activation.ID == activationID {
			return activation, true, nil
		}
	}
	return runtimegenericschedule.Activation{}, false, nil
}
func (s *recoveryDisabledScheduleStore) ListActiveGenericScheduleActivations(context.Context) ([]runtimegenericschedule.Activation, error) {
	s.loadCalls.Add(1)
	return append([]runtimegenericschedule.Activation(nil), s.active...), nil
}
func (*recoveryDisabledScheduleStore) PrepareGenericScheduleOccurrence(context.Context, runtimegenericschedule.Wakeup) (runtimegenericschedule.PreparedOccurrence, error) {
	return runtimegenericschedule.PreparedOccurrence{}, errors.New("unexpected generic schedule occurrence")
}
func (*recoveryDisabledScheduleStore) CommitGenericScheduleOccurrence(context.Context, runtimegenericschedule.CommitCommand) (runtimegenericschedule.CommitResult, error) {
	return runtimegenericschedule.CommitResult{}, errors.New("unexpected generic schedule commit")
}
func (*recoveryDisabledScheduleStore) CancelGenericSchedule(context.Context, runtimegenericschedule.CancelCommand) (runtimegenericschedule.CancelResult, error) {
	return runtimegenericschedule.CancelResult{}, errors.New("unexpected generic schedule cancellation")
}
func (*recoveryDisabledScheduleStore) ClaimGenericScheduleWakeup(context.Context, runtimegenericschedule.Wakeup) (bool, error) {
	return true, nil
}
func (*recoveryDisabledScheduleStore) ReleaseGenericScheduleWakeup(context.Context, runtimegenericschedule.Wakeup) error {
	return nil
}
func (*recoveryDisabledScheduleStore) ReleaseGenericScheduleClaims(context.Context) error { return nil }
func (s *recoveryDisabledScheduleStore) ReadTimerObligations(_ context.Context, _ runtimetimerobligation.Scope, observedAt time.Time) (runtimetimerobligation.Snapshot, error) {
	if s.obligations != nil {
		snapshot := *s.obligations
		snapshot.ObservedAt = observedAt
		return snapshot, nil
	}
	snapshot := runtimetimerobligation.Snapshot{ObservedAt: observedAt, GlobalFamilies: runtimetimerobligation.ZeroFamilies()}
	for _, activation := range s.active {
		family := runtimetimerobligation.FamilyTimer
		if activation.Command.Due.Recurring() {
			family = runtimetimerobligation.FamilyGlobalRecurring
		}
		for index := range snapshot.GlobalFamilies {
			if snapshot.GlobalFamilies[index].Family == family {
				snapshot.GlobalFamilies[index].ActiveCount++
				snapshot.GlobalFamilies[index].RecoverableCount++
			}
		}
		snapshot.Activations = append(snapshot.Activations, runtimetimerobligation.Activation{
			ActivationID: activation.ID, Family: family, Status: string(activation.Status),
			DueAt: activation.CurrentDueAt, InitialDueAt: activation.InitialDueAt,
		})
	}
	return snapshot, nil
}

type recoveryDisabledManagerStore struct {
	recoveryGuardManagerStore
	loadCalls atomic.Int32
}

func (s *recoveryDisabledManagerStore) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	s.loadCalls.Add(1)
	return s.recoveryGuardManagerStore.LoadAgents(ctx)
}

func testOperationalRuntimeConfig() *config.Config {
	return &config.Config{
		Runtime: config.RuntimeConfig{
			RecoveryOnStartup: false,
		},
		LLM: config.LLMConfig{
			Backend: "anthropic",
		},
	}
}

func recoveryGuardActivation(t *testing.T, key string) runtimegenericschedule.Activation {
	t.Helper()
	command := runtimegenericschedule.AdmissionCommand{
		ScheduleKey: key, OwnerKind: runtimegenericschedule.OwnerSystem, OwnerID: "runtime",
		EventType: "timer.check", Payload: semanticvalue.EmptyObject(),
		RoutingSource: events.NewPlatformControlRoutingSource(),
		ExecutionMode: executionmode.Live,
		Due:           runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(time.Minute)),
		TaskID:        key,
	}
	hash, err := command.ImmutableHash()
	if err != nil {
		t.Fatalf("hash recovery guard activation: %v", err)
	}
	admittedAt := time.Now().UTC()
	dueAt, err := command.Due.FirstDue(admittedAt)
	if err != nil {
		t.Fatalf("derive recovery guard due time: %v", err)
	}
	activation := runtimegenericschedule.Activation{
		ID: uuid.NewString(), Command: command, ImmutableHash: hash, AdmittedAt: admittedAt,
		InitialDueAt: dueAt, CurrentDueAt: dueAt, Status: runtimegenericschedule.StatusActive,
	}
	if err := activation.Validate(); err != nil {
		t.Fatalf("validate recovery guard activation: %v", err)
	}
	return activation
}

func TestNewRuntimeValidatesInboundPublicationIntegrityBeforeWiringGateway(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	catalog, err := providertriggers.NewCatalogSnapshot()
	if err != nil {
		t.Fatalf("NewCatalogSnapshot: %v", err)
	}
	sentinel := errors.New("inbound publication corruption")
	corrupt := &recordingInboundStore{integrityErr: sentinel}
	_, err = newScopedTestRuntime(t, context.Background(), RuntimeDeps{
		Config: testOperationalRuntimeConfig(), InboundStore: corrupt,
		Options: RuntimeOptions{WorkflowModule: module, LLMRuntime: noopLLMRuntime{}, ProviderTriggerCatalog: catalog},
	})

	if !errors.Is(err, sentinel) || !strings.Contains(err.Error(), "validate inbound publication integrity at startup") {
		t.Fatalf("NewRuntime corruption error = %v", err)
	}
	if corrupt.integrityCalls.Load() != 1 {
		t.Fatalf("integrity calls = %d, want 1", corrupt.integrityCalls.Load())
	}

	healthy := &recordingInboundStore{}
	rt, err := newScopedTestRuntime(t, context.Background(), RuntimeDeps{
		Config: testOperationalRuntimeConfig(), InboundStore: healthy,
		Options: RuntimeOptions{WorkflowModule: module, LLMRuntime: noopLLMRuntime{}, ProviderTriggerCatalog: catalog},
	})

	if err != nil {
		t.Fatalf("NewRuntime healthy store: %v", err)
	}
	if healthy.integrityCalls.Load() != 1 || rt.InboundGateway == nil || rt.InboundGateway.store != healthy {
		t.Fatal("runtime did not validate and bind the selected inbound publication owner")
	}
}

func TestNewRuntimeBuildsRunLifecycleExecutorFromTypedOwnerWithoutRawSQLCapability(t *testing.T) {
	rt, err := newScopedTestRuntime(
		t,
		testAuthorActivityContext(context.Background()),
		RuntimeDeps{
			Config: testOperationalRuntimeConfig(),

			RunLifecycleCandidates: runtimeTestCandidateOwner{},

			Options: RuntimeOptions{
				WorkflowModule: loadRuntimeOwnershipWorkflowModule(t),
				LLMRuntime:     noopLLMRuntime{},
			},
		},
	)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if rt.runLifecycleExecutor == nil {
		t.Fatal("typed run lifecycle candidate owner did not construct the runtime executor")
	}
}

func TestNewRuntimeRejectsInvalidArtifactRootEnv(t *testing.T) {
	t.Setenv("SWARM_ARTIFACT_ROOT", "/data/swarm/artifacts")
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)
	deliveryStore := newRuntimeShutdownDeliveryStore(t)

	_, err := newScopedTestRuntime(t, testAuthorActivityContext(context.Background()), RuntimeDeps{Config: testOperationalRuntimeConfig(),
		WorkflowPersistence: startupRecoveryWorkflowPersistence(db, nil),
		EventStore:          &minimalRuntimeEventStore{},
		EventBusDurable:     runtimeTestSyntheticDurableDependencies(deliveryStore),
		DeliveryStore:       deliveryStore,
		PipelineObligations: newStartupRecoveryPipelineOwner(nil, nil),
		Options: RuntimeOptions{
			WorkflowModule: module,
			LLMRuntime:     noopLLMRuntime{},
		}})

	if err == nil || !strings.Contains(err.Error(), "artifact repo root validation failed") || !strings.Contains(err.Error(), "agent-visible mount /data") {
		t.Fatalf("NewRuntime error = %v, want invalid artifact root", err)
	}
}

func TestRuntimeStart_FailsWhenRecoveryDisabledAndActiveSchedulesExist(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	store := &recoveryDisabledScheduleStore{active: []runtimegenericschedule.Activation{recoveryGuardActivation(t, "recover-me")}}
	rt, err := newScopedTestRuntime(t, testAuthorActivityContext(context.Background()), RuntimeDeps{Config: testOperationalRuntimeConfig(), GenericScheduleStore: store, TimerObligationReader: store, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	err = rt.Start(testAuthorActivityContext(context.Background()))
	if err == nil || !strings.Contains(err.Error(), "runtime.recovery_on_startup=false") || !strings.Contains(err.Error(), "timer obligations") {
		t.Fatalf("Start error = %v, want explicit timer-obligation denial", err)
	}
}

func TestRuntimeStart_AllowsRecoveryDisabledWithManagerSnapshotWork(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	eventStore := &recoveryGuardEventStore{
		missing: []events.PersistedReplayEvent{{
			Event: eventtest.RunCreatingRootIngress("evt-1",
				"support.item_created", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		}},
		routes: []runtimeflowidentity.Route{
			runtimeflowidentity.DeriveRoute("child", "inst-1"),
		},
	}
	managerStore := &recoveryGuardManagerStore{
		agents: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: "persisted-agent"},
		}},
	}
	rt, err := newScopedTestRuntime(t, testAuthorActivityContext(context.Background()), RuntimeDeps{Config: testOperationalRuntimeConfig(),
		EventStore:              eventStore,
		ManagerStore:            managerStore,
		ManagerPersistenceRoles: runtimemanager.PersistenceRoles{DirectiveOperations: eventStore},
		Options: RuntimeOptions{
			SelfCheck:      false,
			WorkflowModule: module,
			LLMRuntime:     noopLLMRuntime{},
		}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(testAuthorActivityContext(context.Background())); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := eventStore.directiveReconcileCalls.Load(); got != 1 {
		t.Fatalf("directive reconcile calls = %d, want 1 with runtime.recovery_on_startup=false", got)
	}
	if err := rt.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestRuntimeStart_AllowsRecoveryDisabledWhenNoRecoverableWorkExists(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	scheduleStore := &recoveryDisabledScheduleStore{}
	eventStore := &recoveryGuardEventStore{}
	rt, err := newScopedTestRuntime(t, testAuthorActivityContext(context.Background()), RuntimeDeps{Config: testOperationalRuntimeConfig(),
		GenericScheduleStore:    scheduleStore,
		TimerObligationReader:   scheduleStore,
		EventStore:              eventStore,
		ManagerStore:            &recoveryGuardManagerStore{},
		ManagerPersistenceRoles: runtimemanager.PersistenceRoles{DirectiveOperations: eventStore},
		Options: RuntimeOptions{
			SelfCheck:      false,
			WorkflowModule: module,
			LLMRuntime:     noopLLMRuntime{},
		}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(testAuthorActivityContext(context.Background())); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := rt.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestRuntimeStart_AllowsRecoveryDisabledWithNonReplayEventStore(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	scheduleStore := &recoveryDisabledScheduleStore{}
	rt, err := newScopedTestRuntime(t, testAuthorActivityContext(context.Background()), RuntimeDeps{Config: testOperationalRuntimeConfig(),
		EventStore:            &minimalRuntimeEventStore{},
		GenericScheduleStore:  scheduleStore,
		TimerObligationReader: scheduleStore,
		ManagerStore:          &recoveryGuardManagerStore{},
		Options: RuntimeOptions{
			SelfCheck:      false,
			WorkflowModule: module,
			LLMRuntime:     noopLLMRuntime{},
		}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(testAuthorActivityContext(context.Background())); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := rt.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestRuntimeStart_DisablePersistentStartupRecoverySkipsUnscopedStoreReads(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	cfg := testOperationalRuntimeConfig()
	cfg.Runtime.RecoveryOnStartup = true
	scheduleStore := &recoveryDisabledScheduleStore{active: []runtimegenericschedule.Activation{recoveryGuardActivation(t, "other-bundle")}}
	managerStore := &recoveryDisabledManagerStore{
		recoveryGuardManagerStore: recoveryGuardManagerStore{
			agents: []runtimemanager.PersistedAgent{{
				Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: "persisted-agent"},
			}},
		},
	}
	eventStore := &recoveryGuardEventStore{}
	rt, err := newScopedTestRuntime(t, testAuthorActivityContext(context.Background()), RuntimeDeps{Config: cfg,
		EventStore:              eventStore,
		ManagerStore:            managerStore,
		ManagerPersistenceRoles: runtimemanager.PersistenceRoles{DirectiveOperations: eventStore},
		GenericScheduleStore:    scheduleStore,
		TimerObligationReader:   scheduleStore,
		Options: RuntimeOptions{
			SelfCheck:                        false,
			WorkflowModule:                   module,
			LLMRuntime:                       noopLLMRuntime{},
			DisablePersistentStartupRecovery: true,
		}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(testAuthorActivityContext(context.Background())); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := scheduleStore.loadCalls.Load(); got != 0 {
		t.Fatalf("ListActiveGenericScheduleActivations calls = %d, want 0", got)
	}
	if got := managerStore.loadCalls.Load(); got != 0 {
		t.Fatalf("LoadAgents calls = %d, want 0", got)
	}
	if got := eventStore.directiveReconcileCalls.Load(); got != 1 {
		t.Fatalf("directive reconcile calls = %d, want 1 with persistent startup recovery disabled", got)
	}
	if err := rt.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestRuntimeStart_FailsClosedWhenRequiredDirectiveReconciliationFails(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	eventStore := &recoveryGuardEventStore{directiveReconcileErr: errors.New("injected directive reconciliation failure")}
	rt, err := newScopedTestRuntime(t, testAuthorActivityContext(context.Background()), RuntimeDeps{Config: testOperationalRuntimeConfig(),
		EventStore:              eventStore,
		ManagerPersistenceRoles: runtimemanager.PersistenceRoles{DirectiveOperations: eventStore},
		Options: RuntimeOptions{
			SelfCheck:      false,
			WorkflowModule: module,
			LLMRuntime:     noopLLMRuntime{},
		}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	err = rt.Start(testAuthorActivityContext(context.Background()))
	if err == nil || !strings.Contains(err.Error(), "required directive operation reconciliation failed") || !strings.Contains(err.Error(), "injected directive reconciliation failure") {
		t.Fatalf("Start error = %v, want required reconciliation failure", err)
	}
	if got := eventStore.directiveReconcileCalls.Load(); got != 1 {
		t.Fatalf("directive reconcile calls = %d, want 1", got)
	}
}

func TestRuntimeStart_ReconcilesEverySelectedRuntimeContext(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	stores := []*recoveryGuardEventStore{{}, {}}
	runtimes := make([]*Runtime, 0, len(stores))
	for _, eventStore := range stores {
		rt, err := newScopedTestRuntime(t, testAuthorActivityContext(context.Background()), RuntimeDeps{Config: testOperationalRuntimeConfig(),
			EventStore:              eventStore,
			ManagerPersistenceRoles: runtimemanager.PersistenceRoles{DirectiveOperations: eventStore},
			Options: RuntimeOptions{
				SelfCheck:      false,
				WorkflowModule: module,
				LLMRuntime:     noopLLMRuntime{},
			}})

		if err != nil {
			t.Fatalf("NewRuntime: %v", err)
		}
		if err := rt.Start(testAuthorActivityContext(context.Background())); err != nil {
			t.Fatalf("Start: %v", err)
		}
		runtimes = append(runtimes, rt)
	}
	for i, eventStore := range stores {
		if got := eventStore.directiveReconcileCalls.Load(); got != 1 {
			t.Fatalf("context %d directive reconcile calls = %d, want 1", i, got)
		}
	}
	for _, rt := range runtimes {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}
}

func TestNewRuntime_FailsClosedOnMalformedExtensionConfig(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	rt, err := newScopedTestRuntime(t, testAuthorActivityContext(context.Background()), RuntimeDeps{Config: &config.Config{
		Runtime: config.RuntimeConfig{RecoveryOnStartup: false},
		LLM:     config.LLMConfig{Backend: "anthropic"},
		Extensions: map[string]any{
			"budget": map[string]any{
				"human_tasks": "oops",
			},
		},
	},
		EventStore: runtimebus.InMemoryEventStore{},
		Options: RuntimeOptions{
			SelfCheck:      false,
			WorkflowModule: module,
			LLMRuntime:     noopLLMRuntime{},
		}})

	if err == nil || !strings.Contains(err.Error(), "runtime config validation failed") || !strings.Contains(err.Error(), "decode extensions") {
		t.Fatalf("NewRuntime error = %v, want explicit extension validation failure", err)
	}
	if rt != nil {
		t.Fatalf("NewRuntime returned %#v, want nil runtime on malformed config", rt)
	}
}
