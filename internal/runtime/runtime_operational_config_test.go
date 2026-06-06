package runtime

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/testutil"
)

type recoveryGuardManagerStore struct {
	agents []runtimemanager.PersistedAgent
}

func (s *recoveryGuardManagerStore) UpsertAgent(context.Context, runtimemanager.PersistedAgent) error {
	return nil
}

func (s *recoveryGuardManagerStore) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return append([]runtimemanager.PersistedAgent(nil), s.agents...), nil
}

func (*recoveryGuardManagerStore) MarkAgentTerminated(context.Context, string) error { return nil }
func (*recoveryGuardManagerStore) EnsureEntitySchema(context.Context, string) error  { return nil }
func (*recoveryGuardManagerStore) UpsertEventReceipt(context.Context, string, string, runtimemanager.ReceiptStatus, string) error {
	return nil
}
func (*recoveryGuardManagerStore) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (*recoveryGuardManagerStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

type recoveryGuardEventStore struct {
	missing []events.PersistedReplayEvent
	routes  []runtimeflowidentity.Route
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

func TestNewRuntimeRejectsInvalidArtifactRootEnv(t *testing.T) {
	t.Setenv("SWARM_ARTIFACT_ROOT", "/data/swarm/artifacts")
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	module := loadRuntimeOwnershipWorkflowModule(t)

	_, err := NewRuntime(context.Background(), RuntimeDeps{Config: testOperationalRuntimeConfig(), Stores: Stores{
		SQLDB:      db,
		EventStore: &minimalRuntimeEventStore{},
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
			Event: events.NewProjectionEvent("evt-1",
				"support.item_created", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		}},
		routes: []runtimeflowidentity.Route{
			runtimeflowidentity.DeriveRoute("child", "inst-1"),
		},
	}
	managerStore := &recoveryGuardManagerStore{
		agents: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{ID: "persisted-agent"},
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
				Config: runtimeactors.AgentConfig{ID: "persisted-agent"},
			}},
		},
	}
	rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: cfg, Stores: Stores{
		EventStore:    &recoveryGuardEventStore{},
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
	if err := rt.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
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
