package runtime

import (
	"context"

	"empireai/internal/events"
)

type captureStore struct {
	events     []events.Event
	deliveries map[string][]string
}

func (s *captureStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.events = append(s.events, evt)
	return nil
}

func (s *captureStore) InsertEventDeliveries(_ context.Context, eventID string, agentIDs []string) error {
	if s.deliveries == nil {
		s.deliveries = make(map[string][]string)
	}
	s.deliveries[eventID] = append([]string(nil), agentIDs...)
	return nil
}

type failingDeliveryStore struct{}

func (failingDeliveryStore) AppendEvent(_ context.Context, _ events.Event) error { return nil }
func (failingDeliveryStore) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	return nil
}
