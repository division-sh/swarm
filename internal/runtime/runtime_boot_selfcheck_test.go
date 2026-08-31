package runtime

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
)

type bootSelfCheckDescriptorStore struct {
	mu          sync.Mutex
	descriptors []runtimebus.ActiveAgentDescriptor
	deliveries  []string
	events      []events.Event
}

func (s *bootSelfCheckDescriptorStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return runtimebustest.CommitPublish(ctx, command, nil, func(_ context.Context, req runtimebus.CommitPublishRequest) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.events = append(s.events, req.Event.Event())
		s.deliveries = s.deliveries[:0]
		for _, route := range req.DeliveryRoutes {
			if route.Recipient.IsAgent() {
				s.deliveries = append(s.deliveries, route.Recipient.ID())
			}
		}
		return nil
	})
}

func (*bootSelfCheckDescriptorStore) SupportsPersistedReplay() bool { return false }

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
		descriptors: []runtimebus.ActiveAgentDescriptor{{
			Identity: runtimebustest.Identity(t, "agent-a", ""),
		}},
	}
	rt, err := newScopedTestRuntime(t, testAuthorActivityContext(context.Background()), RuntimeDeps{Config: testOperationalRuntimeConfig(),
		EventStore: store,
		Options: RuntimeOptions{
			SelfCheck:      true,
			WorkflowModule: module,
			LLMRuntime:     noopLLMRuntime{},
		}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(testAuthorActivityContext(context.Background())); err != nil {
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
	for _, subscriberID := range rt.Bus.ResolveSubscribedRecipients("platform.boot") {
		if subscriberID == bootstrapSelfCheckSubscriberID {
			t.Fatalf("bootstrap self-check subscriber remains after startup: %#v", rt.Bus.ResolveSubscribedRecipients("platform.boot"))
		}
	}
}

func TestRuntimeStart_PlatformBootPayloadCarriesBootDecisionSummary(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	store := &bootSelfCheckDescriptorStore{}
	progress := []BootProgressEvent{}
	rt, err := newScopedTestRuntime(t, testAuthorActivityContext(context.Background()), RuntimeDeps{Config: testOperationalRuntimeConfig(),
		EventStore: store,
		Options: RuntimeOptions{
			SelfCheck:        true,
			WorkflowModule:   module,
			LLMRuntime:       noopLLMRuntime{},
			BundleSourceFact: testBundleSourceFact(t, runtimeContextTestHashA),
			BootStartedAt:    time.Now().UTC().Add(-1500 * time.Millisecond),
			SystemContainers: []string{"swarm-system", "swarm-scaffold"},
			BootProgress: func(evt BootProgressEvent) {
				progress = append(progress, evt)
			},
		}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.Start(testAuthorActivityContext(context.Background())); err != nil {
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
		"bundle_hash",
		"recovery_decision",
		"static_agents_started",
		"flow_required_agents_started",
		"system_containers_started",
		"self_check_required",
	} {
		if _, ok := payload[key]; !ok {
			t.Fatalf("platform.boot payload missing %q: %#v", key, payload)
		}
	}
	if got := payload["bundle_hash"]; got != runtimeContextTestHashA {
		t.Fatalf("bundle_hash = %#v", got)
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
	if _, present := payload["self_check_passed"]; present {
		t.Fatalf("self_check_passed must be omitted before the self-check completes: %#v", payload)
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
