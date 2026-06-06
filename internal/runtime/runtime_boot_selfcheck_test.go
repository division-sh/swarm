package runtime

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
)

type bootSelfCheckDescriptorStore struct {
	mu          sync.Mutex
	descriptors []runtimebus.ActiveAgentDescriptor
	deliveries  []string
	events      []events.Event
}

func (s *bootSelfCheckDescriptorStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, evt)
	return nil
}

func (s *bootSelfCheckDescriptorStore) InsertEventDeliveries(_ context.Context, _ string, recipients []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deliveries = append([]string(nil), recipients...)
	return nil
}

func (s *bootSelfCheckDescriptorStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deliveries...), nil
}

func (s *bootSelfCheckDescriptorStore) ListActiveAgentDescriptors(context.Context) ([]runtimebus.ActiveAgentDescriptor, error) {
	return append([]runtimebus.ActiveAgentDescriptor(nil), s.descriptors...), nil
}

func (s *bootSelfCheckDescriptorStore) persistedDeliveries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deliveries...)
}

func (s *bootSelfCheckDescriptorStore) appendedEvents() []events.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.Event(nil), s.events...)
}

func TestRuntimeStart_SelfCheckUsesInternalSubscriberVisibility(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	store := &bootSelfCheckDescriptorStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{{AgentID: "agent-a"}},
	}
	rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: testOperationalRuntimeConfig(), Stores: Stores{
		EventStore: store,
	}, Options: RuntimeOptions{
		SelfCheck:      true,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	})
	if got := store.persistedDeliveries(); len(got) != 0 {
		t.Fatalf("persisted deliveries = %#v, want none for bootstrap self-check", got)
	}
}

func TestRuntimeStart_PlatformBootPayloadCarriesBootDecisionSummary(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	store := &bootSelfCheckDescriptorStore{}
	progress := []BootProgressEvent{}
	rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: testOperationalRuntimeConfig(), Stores: Stores{
		EventStore: store,
	}, Options: RuntimeOptions{
		SelfCheck:         true,
		WorkflowModule:    module,
		LLMRuntime:        noopLLMRuntime{},
		BundleFingerprint: "sha256:boot-test",
		BootStartedAt:     time.Now().UTC().Add(-1500 * time.Millisecond),
		SystemContainers:  []string{"swarm-system", "swarm-scaffold"},
		BootProgress: func(evt BootProgressEvent) {
			progress = append(progress, evt)
		},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := rt.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	})

	var boot events.Event
	for _, evt := range store.appendedEvents() {
		if evt.Type() == events.EventType("platform.boot") {
			boot = evt
			break
		}
	}
	if boot.ID() == "" {
		t.Fatalf("platform.boot event not appended: %#v", store.appendedEvents())
	}
	var payload map[string]any
	if err := json.Unmarshal(boot.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal platform.boot payload: %v", err)
	}
	for _, key := range []string{
		"boot_started_at",
		"boot_completed_at",
		"duration_ms",
		"bundle_fingerprint",
		"recovery_decision",
		"static_agents_started",
		"flow_required_agents_started",
		"system_containers_started",
		"self_check_required",
		"self_check_passed",
	} {
		if _, ok := payload[key]; !ok {
			t.Fatalf("platform.boot payload missing %q: %#v", key, payload)
		}
	}
	if got := payload["bundle_fingerprint"]; got != "sha256:boot-test" {
		t.Fatalf("bundle_fingerprint = %#v", got)
	}
	recovery, ok := payload["recovery_decision"].(map[string]any)
	if !ok {
		t.Fatalf("recovery_decision = %#v", payload["recovery_decision"])
	}
	if got := recovery["reason_code"]; got != "recovery_disabled_no_persisted_work" {
		t.Fatalf("recovery reason = %#v", got)
	}
	if got := payload["self_check_required"]; got != true {
		t.Fatalf("self_check_required = %#v", got)
	}
	if got := payload["self_check_passed"]; got != nil {
		t.Fatalf("self_check_passed = %#v", got)
	}
	if !bootProgressContains(progress, 19, "platform_boot_event_published") {
		t.Fatalf("boot progress missing platform boot publication: %#v", progress)
	}
}

func bootProgressContains(events []BootProgressEvent, step int, name string) bool {
	for _, evt := range events {
		if evt.Step == step && evt.Name == name {
			return true
		}
	}
	return false
}
