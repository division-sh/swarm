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
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/testutil"
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
func (*recoveryGuardManagerStore) UpsertEventReceipt(context.Context, string, string, runtimemanager.ReceiptStatus, *runtimefailures.Envelope) error {
	return nil
}
func (*recoveryGuardManagerStore) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (*recoveryGuardManagerStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

type recoveryGuardEventStore struct {
	runtimeagentcontrol.DirectiveOperationStore
	missing                 []events.PersistedReplayEvent
	routes                  []runtimeflowidentity.Route
	directiveReconcileCalls atomic.Int32
	directiveReconcileErr   error
}

func (*recoveryGuardEventStore) AppendEvent(context.Context, events.Event) error { return nil }
func (*recoveryGuardEventStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}
func (*recoveryGuardEventStore) UpsertCommittedReplayScope(context.Context, string, runtimereplayclaim.CommittedReplayScope) error {
	return nil
}
func (*recoveryGuardEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}
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
	return nil, true, nil
}
func (*recoveryGuardEventStore) SupportsPersistedReplay() bool { return false }

func (s *recoveryGuardEventStore) ReconcileDirectiveOperations(context.Context, time.Time, time.Duration) (runtimeagentcontrol.DirectiveOperationReconcileResult, error) {
	s.directiveReconcileCalls.Add(1)
	return runtimeagentcontrol.DirectiveOperationReconcileResult{}, s.directiveReconcileErr
}

type minimalRuntimeEventStore struct{}

func (*minimalRuntimeEventStore) AppendEvent(context.Context, events.Event) error { return nil }
func (*minimalRuntimeEventStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}
func (*minimalRuntimeEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}

type recoveryDisabledScheduleStore struct {
	recordingRuntimeScheduleStore
	loadCalls atomic.Int32
}

func (s *recoveryDisabledScheduleStore) LoadActiveSchedules(ctx context.Context) ([]runtimepipeline.Schedule, error) {
	s.loadCalls.Add(1)
	return s.recordingRuntimeScheduleStore.LoadActiveSchedules(ctx)
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

func TestNewRuntimeValidatesInboundPublicationIntegrityBeforeWiringGateway(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	catalog, err := providertriggers.NewCatalogSnapshot()
	if err != nil {
		t.Fatalf("NewCatalogSnapshot: %v", err)
	}
	sentinel := errors.New("inbound publication corruption")
	corrupt := &recordingInboundStore{integrityErr: sentinel}
	_, err = NewRuntime(context.Background(), RuntimeDeps{
		Config: testOperationalRuntimeConfig(), Stores: Stores{InboundStore: corrupt},
		Options: RuntimeOptions{WorkflowModule: module, LLMRuntime: noopLLMRuntime{}, ProviderTriggerCatalog: catalog},
	})
	if !errors.Is(err, sentinel) || !strings.Contains(err.Error(), "validate inbound publication integrity at startup") {
		t.Fatalf("NewRuntime corruption error = %v", err)
	}
	if corrupt.integrityCalls.Load() != 1 {
		t.Fatalf("integrity calls = %d, want 1", corrupt.integrityCalls.Load())
	}

	healthy := &recordingInboundStore{}
	rt, err := NewRuntime(context.Background(), RuntimeDeps{
		Config: testOperationalRuntimeConfig(), Stores: Stores{InboundStore: healthy},
		Options: RuntimeOptions{WorkflowModule: module, LLMRuntime: noopLLMRuntime{}, ProviderTriggerCatalog: catalog},
	})
	if err != nil {
		t.Fatalf("NewRuntime healthy store: %v", err)
	}
	if healthy.integrityCalls.Load() != 1 || rt.InboundGateway == nil || rt.InboundGateway.store != healthy {
		t.Fatal("runtime did not validate and bind the selected inbound publication owner")
	}
}

func TestNewRuntimeRejectsInvalidArtifactRootEnv(t *testing.T) {
	t.Setenv("SWARM_ARTIFACT_ROOT", "/data/swarm/artifacts")
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)

	_, err := NewRuntime(context.Background(), RuntimeDeps{Config: testOperationalRuntimeConfig(), Stores: Stores{
		SQLDB:         db,
		PipelineStore: runtimepipeline.NewWorkflowInstanceStore(db),
		EventStore:    &minimalRuntimeEventStore{},
	}, Options: RuntimeOptions{
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err == nil || !strings.Contains(err.Error(), "artifact repo root validation failed") || !strings.Contains(err.Error(), "agent-visible mount /data") {
		t.Fatalf("NewRuntime error = %v, want invalid artifact root", err)
	}
}

func TestRuntimeStart_FailsWhenRecoveryDisabledAndActiveSchedulesExist(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	store := &recordingRuntimeScheduleStore{
		active: []runtimepipeline.Schedule{{
			AgentID:   "runtime",
			EventType: "timer.check",
			Mode:      "once",
			At:        time.Now().Add(time.Minute),
			TaskID:    "recover-me",
		}},
	}
	rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: testOperationalRuntimeConfig(), Stores: Stores{ScheduleStore: store}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	err = rt.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "runtime.recovery_on_startup=false") || !strings.Contains(err.Error(), "active schedules") {
		t.Fatalf("Start error = %v, want explicit active schedule denial", err)
	}
}

func TestRuntimeStart_AllowsRecoveryDisabledWithManagerSnapshotWork(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	eventStore := &recoveryGuardEventStore{
		missing: []events.PersistedReplayEvent{{
			Event: eventtest.RootIngress("evt-1",
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
	rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: testOperationalRuntimeConfig(), Stores: Stores{
		EventStore:   eventStore,
		ManagerStore: managerStore,
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(context.Background()); err != nil {
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
	rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: testOperationalRuntimeConfig(), Stores: Stores{
		ScheduleStore: &recordingRuntimeScheduleStore{},
		EventStore:    &recoveryGuardEventStore{},
		ManagerStore:  &recoveryGuardManagerStore{},
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := rt.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestRuntimeStart_AllowsRecoveryDisabledWithNonReplayEventStore(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: testOperationalRuntimeConfig(), Stores: Stores{
		EventStore:    &minimalRuntimeEventStore{},
		ScheduleStore: &recordingRuntimeScheduleStore{},
		ManagerStore:  &recoveryGuardManagerStore{},
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(context.Background()); err != nil {
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
	scheduleStore := &recoveryDisabledScheduleStore{
		recordingRuntimeScheduleStore: recordingRuntimeScheduleStore{
			active: []runtimepipeline.Schedule{{
				AgentID:   "runtime",
				EventType: "timer.check",
				Mode:      "once",
				At:        time.Now().Add(time.Minute),
				TaskID:    "other-bundle",
			}},
		},
	}
	managerStore := &recoveryDisabledManagerStore{
		recoveryGuardManagerStore: recoveryGuardManagerStore{
			agents: []runtimemanager.PersistedAgent{{
				Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: "persisted-agent"},
			}},
		},
	}
	eventStore := &recoveryGuardEventStore{}
	rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: cfg, Stores: Stores{
		EventStore:    eventStore,
		ManagerStore:  managerStore,
		ScheduleStore: scheduleStore,
	}, Options: RuntimeOptions{
		SelfCheck:                        false,
		WorkflowModule:                   module,
		LLMRuntime:                       noopLLMRuntime{},
		DisablePersistentStartupRecovery: true,
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := scheduleStore.loadCalls.Load(); got != 0 {
		t.Fatalf("LoadActiveSchedules calls = %d, want 0", got)
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
	rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: testOperationalRuntimeConfig(), Stores: Stores{
		EventStore: eventStore,
	}, Options: RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	err = rt.Start(context.Background())
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
		rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: testOperationalRuntimeConfig(), Stores: Stores{
			EventStore: eventStore,
		}, Options: RuntimeOptions{
			SelfCheck:      false,
			WorkflowModule: module,
			LLMRuntime:     noopLLMRuntime{},
		}})
		if err != nil {
			t.Fatalf("NewRuntime: %v", err)
		}
		if err := rt.Start(context.Background()); err != nil {
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
	rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: &config.Config{
		Runtime: config.RuntimeConfig{RecoveryOnStartup: false},
		LLM:     config.LLMConfig{Backend: "anthropic"},
		Extensions: map[string]any{
			"budget": map[string]any{
				"human_tasks": "oops",
			},
		},
	}, Stores: Stores{
		EventStore: runtimebus.InMemoryEventStore{},
	}, Options: RuntimeOptions{
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
