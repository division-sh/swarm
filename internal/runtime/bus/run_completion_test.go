package bus

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"time"
)

type normalRunCompletionTestStore struct {
	InMemoryEventStore
	pipelineReceipts []string
	standaloneEvents []string
	normalEvents     []string
}

func (s *normalRunCompletionTestStore) UpsertPipelineReceipt(_ context.Context, eventID, _, _ string) error {
	s.pipelineReceipts = append(s.pipelineReceipts, eventID)
	return nil
}

func (s *normalRunCompletionTestStore) ConvergeStandaloneRuntimePlatformRun(_ context.Context, evt events.Event) error {
	s.standaloneEvents = append(s.standaloneEvents, evt.ID())
	return nil
}

func (s *normalRunCompletionTestStore) ConvergeNormalRunCompletion(_ context.Context, eventID string, _ []string, _ map[string][]string) error {
	s.normalEvents = append(s.normalEvents, eventID)
	return nil
}

func TestEventBusMarkPipelineReceiptConvergesNormalRunCompletion(t *testing.T) {
	store := &normalRunCompletionTestStore{}
	eb, err := NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.markPipelineReceipt(context.Background(), "event-1", "processed", ""); err != nil {
		t.Fatalf("markPipelineReceipt: %v", err)
	}
	if len(store.pipelineReceipts) != 1 || store.pipelineReceipts[0] != "event-1" {
		t.Fatalf("pipeline receipts = %#v, want event-1", store.pipelineReceipts)
	}
	if len(store.normalEvents) != 1 || store.normalEvents[0] != "event-1" {
		t.Fatalf("normal completion events = %#v, want event-1", store.normalEvents)
	}
}

func TestEventBusStandalonePlatformConvergenceAlsoProbesNormalRunCompletion(t *testing.T) {
	store := &normalRunCompletionTestStore{}
	eb, err := NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	evt := eventtest.RootIngress("event-2", events.EventType("platform.boot"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})
	if err := eb.convergeStandaloneRuntimePlatformRun(context.Background(), evt); err != nil {
		t.Fatalf("convergeStandaloneRuntimePlatformRun: %v", err)
	}
	if len(store.standaloneEvents) != 1 || store.standaloneEvents[0] != "event-2" {
		t.Fatalf("standalone events = %#v, want event-2", store.standaloneEvents)
	}
	if len(store.normalEvents) != 1 || store.normalEvents[0] != "event-2" {
		t.Fatalf("normal completion events = %#v, want event-2", store.normalEvents)
	}
}
