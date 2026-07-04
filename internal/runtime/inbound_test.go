package runtime

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
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

type capturingInboundEventStore struct {
	events []events.Event
}

func (s *capturingInboundEventStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.events = append(s.events, evt)
	return nil
}

func (*capturingInboundEventStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}

func (*capturingInboundEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
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

type recordingInboundStore struct {
	target          InboundTarget
	inserted        bool
	recorded        bool
	providerEventID string
	entityID        string
	provider        string
}

func (s *recordingInboundStore) RecordInboundEvent(_ context.Context, providerEventID, entityID, provider string) (bool, error) {
	s.recorded = true
	s.providerEventID = providerEventID
	s.entityID = entityID
	s.provider = provider
	return s.inserted, nil
}

func (s *recordingInboundStore) ResolveInboundTarget(context.Context, string, string) (InboundTarget, error) {
	return s.target, nil
}

func (*recordingInboundStore) PurgeInboundEventsBefore(context.Context, time.Time, int) (int, error) {
	return 0, nil
}

func TestInboundGateway_Returns503AndRollsBackMarkerWhenPublishFails(t *testing.T) {
	bus, err := runtimebus.NewEventBus(failingInboundEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &rollbackTrackingInboundStore{}
	g := NewInboundGateway(bus, nil, nil, store)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/entity-1/custom", strings.NewReader(`{"id":"evt-1","type":"push"}`))
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

	req := httptest.NewRequest(http.MethodPost, "/webhooks/entity-1/custom", strings.NewReader(`{"id":"evt-1","type":"push"}`))
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

	req := httptest.NewRequest(http.MethodPost, "/webhooks/entity-1/custom", strings.NewReader(`{"id":"evt-1","type":"push"}`))
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

func TestInboundGateway_GitHubPausedRuntimeUsesIngressOwnerAndAcceptsQueueableWebhook(t *testing.T) {
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
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			WebhookSecret: "github-secret",
		},
		inserted: true,
	}
	g := NewInboundGateway(bus, nil, nil, store)
	g.SetRuntimeIngress(controller)

	body := []byte(`{"zen":"Keep it logically awesome."}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/github", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", githubWebhookSignature("github-secret", body))
	req.Header.Set("X-GitHub-Delivery", "delivery-123")
	req.Header.Set("X-GitHub-Event", "push")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if !store.recorded {
		t.Fatal("expected GitHub delivery to record inbound marker while paused")
	}
	if store.providerEventID != "delivery-123" {
		t.Fatalf("providerEventID = %q, want delivery-123", store.providerEventID)
	}
}

func TestInboundGateway_GitHubAdapterOwnsSignatureDeliveryIDAndEventMapping(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			WebhookSecret: "github-secret",
		},
		inserted: true,
	}
	g := NewInboundGateway(bus, nil, nil, store)

	body := []byte(`{"zen":"Keep it logically awesome."}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/github", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", githubWebhookSignature("github-secret", body))
	req.Header.Set("X-GitHub-Delivery", "delivery-123")
	req.Header.Set("X-GitHub-Event", "push")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if !store.recorded {
		t.Fatal("expected GitHub delivery to record inbound marker")
	}
	if store.providerEventID != "delivery-123" {
		t.Fatalf("providerEventID = %q, want delivery-123", store.providerEventID)
	}
	if len(eventStore.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(eventStore.events))
	}
	evt := eventStore.events[0]
	if evt.Type() != events.EventType("inbound.github.push") {
		t.Fatalf("event type = %q, want inbound.github.push", evt.Type())
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["provider_event_id"] != "delivery-123" || payload["event_type"] != "push" || payload["provider"] != "github" {
		t.Fatalf("payload = %+v, want GitHub delivery identity", payload)
	}
	if strings.Contains(rec.Body.String(), "github-secret") || strings.Contains(string(evt.Payload()), "github-secret") {
		t.Fatal("GitHub signing secret leaked into readback or event payload")
	}
}

func TestInboundGateway_GitHubAdapterRejectsInvalidSignatureBeforeMarkerAndPublish(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			WebhookSecret: "github-secret",
		},
		inserted: true,
	}
	g := NewInboundGateway(bus, nil, nil, store)

	body := []byte(`{"zen":"Keep it logically awesome."}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/github", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", githubWebhookSignature("wrong-secret", body))
	req.Header.Set("X-GitHub-Delivery", "delivery-123")
	req.Header.Set("X-GitHub-Event", "push")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("invalid GitHub signature recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_GitHubAdapterDuplicateDeliveryDoesNotPublishAgain(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			WebhookSecret: "github-secret",
		},
		inserted: false,
	}
	g := NewInboundGateway(bus, nil, nil, store)

	body := []byte(`{"zen":"Keep it logically awesome."}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/github", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", githubWebhookSignature("github-secret", body))
	req.Header.Set("X-GitHub-Delivery", "delivery-123")
	req.Header.Set("X-GitHub-Event", "push")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"duplicate"`) {
		t.Fatalf("duplicate response = %s", rec.Body.String())
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func githubWebhookSignature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
