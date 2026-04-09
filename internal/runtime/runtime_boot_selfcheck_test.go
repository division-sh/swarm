package runtime

import (
	"context"
	"sync"
	"testing"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
)

type bootSelfCheckDescriptorStore struct {
	mu          sync.Mutex
	descriptors []runtimebus.ActiveAgentDescriptor
	deliveries  []string
}

func (*bootSelfCheckDescriptorStore) AppendEvent(context.Context, events.Event) error { return nil }

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

func TestRuntimeStart_SelfCheckUsesInternalSubscriberVisibility(t *testing.T) {
	module := loadRuntimeOwnershipWorkflowModule(t)
	store := &bootSelfCheckDescriptorStore{
		descriptors: []runtimebus.ActiveAgentDescriptor{{AgentID: "agent-a"}},
	}
	rt, err := NewRuntime(context.Background(), testOperationalRuntimeConfig(), Stores{
		EventStore: store,
	}, RuntimeOptions{
		SelfCheck:      true,
		WorkflowModule: module,
		LLMRuntime:     noopLLMRuntime{},
	})
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
