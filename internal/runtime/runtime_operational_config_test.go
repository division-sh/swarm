package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"swarm/internal/config"
	"swarm/internal/events"
	runtimeownership "swarm/internal/runtime/core/ownership"
	runtimemanager "swarm/internal/runtime/manager"
	runtimepipeline "swarm/internal/runtime/pipeline"
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
}

func (*recoveryGuardEventStore) AppendEvent(context.Context, events.Event) error { return nil }
func (*recoveryGuardEventStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}
func (*recoveryGuardEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}
func (s *recoveryGuardEventStore) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error) {
	return append([]events.PersistedReplayEvent(nil), s.missing...), nil
}
func (*recoveryGuardEventStore) ClaimPipelineReplay(context.Context, string) (runtimeownership.Lease, bool, error) {
	return nil, true, nil
}

func testOperationalRuntimeConfig() *config.Config {
	return &config.Config{
		Runtime: config.RuntimeConfig{
			RecoveryOnStartup: false,
		},
		LLM: config.LLMConfig{
			RuntimeMode: "api",
		},
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
	rt, err := NewRuntime(context.Background(), testOperationalRuntimeConfig(), Stores{ScheduleStore: store}, RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	err = rt.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "runtime.recovery_on_startup=false") || !strings.Contains(err.Error(), "active schedules") {
		t.Fatalf("Start error = %v, want explicit active schedule denial", err)
	}
}

func TestRuntimeStart_FailsWhenRecoveryDisabledAndPipelineRecoverableWorkExists(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	eventStore := &recoveryGuardEventStore{
		missing: []events.PersistedReplayEvent{{
			Event: events.Event{
				ID:   "evt-1",
				Type: "support.item_created",
			},
		}},
	}
	rt, err := NewRuntime(context.Background(), testOperationalRuntimeConfig(), Stores{
		EventStore:   eventStore,
		ManagerStore: &recoveryGuardManagerStore{},
	}, RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	err = rt.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "runtime.recovery_on_startup=false") || !strings.Contains(err.Error(), "events missing pipeline receipts") {
		t.Fatalf("Start error = %v, want explicit pipeline recovery denial", err)
	}
}

func TestRuntimeStart_AllowsRecoveryDisabledWhenNoRecoverableWorkExists(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	rt, err := NewRuntime(context.Background(), testOperationalRuntimeConfig(), Stores{
		ScheduleStore: &recordingRuntimeScheduleStore{},
		EventStore:    &recoveryGuardEventStore{},
		ManagerStore:  &recoveryGuardManagerStore{},
	}, RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	})
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
