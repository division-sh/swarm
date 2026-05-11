package runtime

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimeingress "swarm/internal/runtime/ingress"
)

type failingInboundEventStore struct{}

func (failingInboundEventStore) AppendEvent(context.Context, events.Event) error {
	return errors.New("append failed")
}

func (failingInboundEventStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}

func (failingInboundEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return []string{}, nil
}

type rollbackTrackingInboundStore struct {
	recorded bool
	rolled   bool
}

func (s *rollbackTrackingInboundStore) RecordInboundEvent(context.Context, string, string, string) (bool, error) {
	s.recorded = true
	return true, nil
}

func (s *rollbackTrackingInboundStore) ResolveInboundTarget(context.Context, string, string) (InboundTarget, error) {
	return InboundTarget{EntityID: "entity-1", EntitySlug: "entity-1"}, nil
}

func (s *rollbackTrackingInboundStore) PurgeInboundEventsBefore(context.Context, time.Time, int) (int, error) {
	return 0, nil
}

func (s *rollbackTrackingInboundStore) DeleteInboundEvent(context.Context, string, string, string) error {
	s.rolled = true
	return nil
}

func TestInboundGateway_Returns503AndRollsBackMarkerWhenPublishFails(t *testing.T) {
	bus, err := runtimebus.NewEventBus(failingInboundEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &rollbackTrackingInboundStore{}
	g := NewInboundGateway(bus, nil, nil, store)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/entity-1/github", strings.NewReader(`{"id":"evt-1","type":"push"}`))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if !store.recorded {
		t.Fatal("expected inbound event to be recorded before publish attempt")
	}
	if !store.rolled {
		t.Fatal("expected inbound event marker rollback on publish failure")
	}
}

func TestInboundGateway_Returns503WhenRuntimeShutdownAdmissionClosed(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &rollbackTrackingInboundStore{}
	g := NewInboundGateway(bus, nil, func() bool { return true }, store)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/entity-1/github", strings.NewReader(`{"id":"evt-1","type":"push"}`))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "runtime shutting down") {
		t.Fatalf("body = %q, want runtime shutting down", rec.Body.String())
	}
	if store.recorded {
		t.Fatal("did not expect inbound event recording after shutdown admission closed")
	}
}

func TestInboundGateway_PausedRuntimeUsesIngressOwnerAndAcceptsQueueableWebhook(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeingress.NewController(nil, bus, runtimeingress.Options{})
	bus.SetRuntimeIngressDispatchGate(controller)
	if _, err := controller.Pause(context.Background(), runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	store := &rollbackTrackingInboundStore{}
	g := NewInboundGateway(bus, nil, nil, store)
	g.SetRuntimeIngress(controller)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/entity-1/github", strings.NewReader(`{"id":"evt-1","type":"push"}`))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if !store.recorded {
		t.Fatal("expected inbound event to be recorded while paused")
	}
	if store.rolled {
		t.Fatal("did not expect inbound marker rollback for queued paused ingress")
	}
}
