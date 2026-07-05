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
	"strconv"
	"strings"
	"sync"
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

func TestInboundGateway_SlackURLVerificationReturnsChallengeWithoutMarkerOrPublish(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			WebhookSecret: "slack-secret",
		},
		inserted: true,
	}
	g := NewInboundGateway(bus, nil, nil, store)

	body := []byte(`{"type":"url_verification","challenge":"challenge-value","token":"deprecated-token"}`)
	req := newSignedSlackRequest("/webhooks/customer-a/slack", "slack-secret", body, strconv.FormatInt(time.Now().UTC().Unix(), 10))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "challenge-value" {
		t.Fatalf("body = %q, want challenge-value", rec.Body.String())
	}
	if store.recorded {
		t.Fatal("Slack url_verification recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
	if strings.Contains(rec.Body.String(), "slack-secret") || strings.Contains(rec.Body.String(), "deprecated-token") {
		t.Fatal("Slack secret material leaked into challenge response")
	}
}

func TestInboundGateway_SlackURLVerificationRequiresChallengeBeforeMarkerAndPublish(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			WebhookSecret: "slack-secret",
		},
		inserted: true,
	}
	g := NewInboundGateway(bus, nil, nil, store)

	body := []byte(`{"type":"url_verification","token":"deprecated-token"}`)
	req := newSignedSlackRequest("/webhooks/customer-a/slack", "slack-secret", body, strconv.FormatInt(time.Now().UTC().Unix(), 10))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("Slack url_verification without challenge recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_SlackRejectsMissingSecretBeforeMarkerAndPublish(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:   "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug: "customer-a",
		},
		inserted: true,
	}
	g := NewInboundGateway(bus, nil, nil, store)

	body := []byte(`{"type":"event_callback","event_id":"Ev123","event":{"type":"message"}}`)
	req := newSignedSlackRequest("/webhooks/customer-a/slack", "slack-secret", body, strconv.FormatInt(time.Now().UTC().Unix(), 10))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("Slack request without configured signing secret recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_SlackRejectsMissingOrInvalidSignatureBeforeMarkerAndPublish(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*http.Request, []byte)
	}{
		{
			name: "missing signature",
			configure: func(req *http.Request, body []byte) {
				req.Header.Set("X-Slack-Request-Timestamp", strconv.FormatInt(time.Now().UTC().Unix(), 10))
			},
		},
		{
			name: "invalid signature",
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("X-Slack-Request-Timestamp", timestamp)
				req.Header.Set("X-Slack-Signature", slackWebhookSignature("wrong-secret", timestamp, body))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventStore := &capturingInboundEventStore{}
			bus, err := runtimebus.NewEventBus(eventStore)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			store := &recordingInboundStore{
				target: InboundTarget{
					EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
					EntitySlug:    "customer-a",
					WebhookSecret: "slack-secret",
				},
				inserted: true,
			}
			g := NewInboundGateway(bus, nil, nil, store)

			body := []byte(`{"type":"event_callback","event_id":"Ev123","event":{"type":"message"}}`)
			req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/slack", strings.NewReader(string(body)))
			tc.configure(req, body)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401 body=%s", rec.Code, rec.Body.String())
			}
			if store.recorded {
				t.Fatal("Slack request with invalid signature recorded inbound marker")
			}
			if len(eventStore.events) != 0 {
				t.Fatalf("published events = %d, want 0", len(eventStore.events))
			}
		})
	}
}

func TestInboundGateway_SlackRejectsStaleTimestampBeforeMarkerAndPublish(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			WebhookSecret: "slack-secret",
		},
		inserted: true,
	}
	g := NewInboundGateway(bus, nil, nil, store)

	body := []byte(`{"type":"event_callback","event_id":"Ev123","event":{"type":"message"}}`)
	req := newSignedSlackRequest("/webhooks/customer-a/slack", "slack-secret", body, "1")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("Slack request with stale timestamp recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_SlackEventCallbackOwnsEventIDAndInnerEventMapping(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			WebhookSecret: "slack-secret",
		},
		inserted: true,
	}
	g := NewInboundGateway(bus, nil, nil, store)

	body := []byte(`{"type":"event_callback","token":"deprecated-token","event_id":"Ev123ABC456","event":{"type":"message.channels","text":"hello"}}`)
	req := newSignedSlackRequest("/webhooks/customer-a/slack", "slack-secret", body, strconv.FormatInt(time.Now().UTC().Unix(), 10))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if !store.recorded {
		t.Fatal("expected Slack callback to record inbound marker")
	}
	if store.providerEventID != "Ev123ABC456" {
		t.Fatalf("providerEventID = %q, want Ev123ABC456", store.providerEventID)
	}
	if store.provider != "slack" {
		t.Fatalf("provider = %q, want slack", store.provider)
	}
	if len(eventStore.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(eventStore.events))
	}
	evt := eventStore.events[0]
	if evt.Type() != events.EventType("inbound.slack.message_channels") {
		t.Fatalf("event type = %q, want inbound.slack.message_channels", evt.Type())
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["provider_event_id"] != "Ev123ABC456" || payload["event_type"] != "message_channels" || payload["provider"] != "slack" {
		t.Fatalf("payload = %+v, want Slack delivery identity", payload)
	}
	payloadJSON := string(evt.Payload())
	if strings.Contains(rec.Body.String(), "slack-secret") || strings.Contains(payloadJSON, "slack-secret") {
		t.Fatal("Slack signing secret leaked into readback or event payload")
	}
	if strings.Contains(payloadJSON, "deprecated-token") {
		t.Fatal("Slack deprecated verification token leaked into event payload")
	}
}

func TestInboundGateway_SlackEventCallbackAcknowledgesBeforePostCommitDispatchCompletes(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseDispatch := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	t.Cleanup(releaseDispatch)
	bus, err := runtimebus.NewEventBusWithOptions(eventStore, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{
			blockingInboundInterceptor{started: started, release: release},
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			WebhookSecret: "slack-secret",
		},
		inserted: true,
	}
	g := NewInboundGateway(bus, nil, nil, store)

	body := []byte(`{"type":"event_callback","event_id":"Ev123ABC456","event":{"type":"message","text":"hello"}}`)
	req := newSignedSlackRequest("/webhooks/customer-a/slack", "slack-secret", body, strconv.FormatInt(time.Now().UTC().Unix(), 10))
	responseDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		g.Handler().ServeHTTP(rec, req)
		responseDone <- rec
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("Slack callback post-commit dispatch did not start")
	}
	select {
	case rec := <-responseDone:
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Slack callback response waited for post-commit dispatch completion")
	}
	if !store.recorded {
		t.Fatal("expected Slack callback to record inbound marker before acknowledgement")
	}
	if len(eventStore.events) != 1 {
		t.Fatalf("published events = %d, want 1 before dispatch release", len(eventStore.events))
	}

	releaseDispatch()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence after dispatch release: %v", err)
	}
}

func TestInboundGateway_SlackEventCallbackRequiresEventIDBeforeMarkerAndPublish(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			WebhookSecret: "slack-secret",
		},
		inserted: true,
	}
	g := NewInboundGateway(bus, nil, nil, store)

	body := []byte(`{"type":"event_callback","event":{"type":"message"}}`)
	req := newSignedSlackRequest("/webhooks/customer-a/slack", "slack-secret", body, strconv.FormatInt(time.Now().UTC().Unix(), 10))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("Slack callback without event_id recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_SlackDuplicateEventDoesNotPublishAgain(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			WebhookSecret: "slack-secret",
		},
		inserted: false,
	}
	g := NewInboundGateway(bus, nil, nil, store)

	body := []byte(`{"type":"event_callback","event_id":"Ev123ABC456","event":{"type":"message"}}`)
	req := newSignedSlackRequest("/webhooks/customer-a/slack", "slack-secret", body, strconv.FormatInt(time.Now().UTC().Unix(), 10))
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

func TestInboundGateway_StripeManifestOwnsSignatureReplayIDTypeAndAck(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseDispatch := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	t.Cleanup(releaseDispatch)
	bus, err := runtimebus.NewEventBusWithOptions(eventStore, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{
			blockingInboundInterceptor{started: started, release: release},
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			WebhookSecret: "stripe-secret",
		},
		inserted: true,
	}
	g := NewInboundGateway(bus, nil, nil, store)

	body := []byte(`{"id":"evt_123","type":"invoice.paid","data":{"object":{"id":"in_123"}}}`)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/stripe", strings.NewReader(string(body)))
	req.Header.Set("Stripe-Signature", stripeWebhookSignature("stripe-secret", timestamp, body))
	responseDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		g.Handler().ServeHTTP(rec, req)
		responseDone <- rec
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("Stripe callback post-commit dispatch did not start")
	}
	select {
	case rec := <-responseDone:
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Stripe callback response waited for post-commit dispatch completion")
	}
	if !store.recorded {
		t.Fatal("expected Stripe callback to record inbound marker")
	}
	if store.providerEventID != "evt_123" {
		t.Fatalf("providerEventID = %q, want evt_123", store.providerEventID)
	}
	if len(eventStore.events) != 1 {
		t.Fatalf("published events = %d, want 1 before dispatch release", len(eventStore.events))
	}
	evt := eventStore.events[0]
	if evt.Type() != events.EventType("inbound.stripe") {
		t.Fatalf("event type = %q, want inbound.stripe", evt.Type())
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["provider_event_id"] != "evt_123" || payload["provider_event_type"] != "invoice_paid" || payload["provider"] != "stripe" {
		t.Fatalf("payload = %+v, want Stripe delivery identity", payload)
	}
	if strings.Contains(string(evt.Payload()), "stripe-secret") {
		t.Fatal("Stripe signing secret leaked into event payload")
	}

	releaseDispatch()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence after dispatch release: %v", err)
	}
}

func TestInboundGateway_StripeRejectsInvalidInputsBeforeMarkerAndPublish(t *testing.T) {
	for _, tc := range []struct {
		name       string
		body       []byte
		configure  func(*http.Request, []byte)
		wantStatus int
	}{
		{
			name:       "missing signature",
			body:       []byte(`{"id":"evt_123","type":"invoice.paid"}`),
			configure:  func(*http.Request, []byte) {},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "malformed signature params",
			body: []byte(`{"id":"evt_123","type":"invoice.paid"}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("Stripe-Signature", "t="+timestamp+",v0="+stripeSignatureHex("stripe-secret", timestamp, body))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "stale timestamp",
			body: []byte(`{"id":"evt_123","type":"invoice.paid"}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Add(-10*time.Minute).Unix(), 10)
				req.Header.Set("Stripe-Signature", stripeWebhookSignature("stripe-secret", timestamp, body))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "missing event id",
			body: []byte(`{"type":"invoice.paid"}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("Stripe-Signature", stripeWebhookSignature("stripe-secret", timestamp, body))
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing event type",
			body: []byte(`{"id":"evt_123"}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("Stripe-Signature", stripeWebhookSignature("stripe-secret", timestamp, body))
			},
			wantStatus: http.StatusBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventStore := &capturingInboundEventStore{}
			bus, err := runtimebus.NewEventBus(eventStore)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			store := &recordingInboundStore{
				target: InboundTarget{
					EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
					EntitySlug:    "customer-a",
					WebhookSecret: "stripe-secret",
				},
				inserted: true,
			}
			g := NewInboundGateway(bus, nil, nil, store)

			req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/stripe", strings.NewReader(string(tc.body)))
			tc.configure(req, tc.body)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if store.recorded {
				t.Fatal("invalid Stripe request recorded inbound marker")
			}
			if len(eventStore.events) != 0 {
				t.Fatalf("published events = %d, want 0", len(eventStore.events))
			}
		})
	}
}

func TestInboundGateway_StripeDuplicateEventDoesNotPublishAgain(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			WebhookSecret: "stripe-secret",
		},
		inserted: false,
	}
	g := NewInboundGateway(bus, nil, nil, store)

	body := []byte(`{"id":"evt_123","type":"invoice.paid"}`)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/stripe", strings.NewReader(string(body)))
	req.Header.Set("Stripe-Signature", stripeWebhookSignature("stripe-secret", timestamp, body))
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

func TestInboundGateway_RawFallbackDoesNotInterpretStripeSignature(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			WebhookSecret: "raw-secret",
		},
		inserted: true,
	}
	g := NewInboundGateway(bus, nil, nil, store)

	body := []byte(`{"id":"evt_123","type":"invoice.paid"}`)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/custom", strings.NewReader(string(body)))
	req.Header.Set("Stripe-Signature", stripeWebhookSignature("raw-secret", timestamp, body))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("raw fallback accepted Stripe-Signature and recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_RejectsOversizedBodyBeforeMarkerAndPublish(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/github", strings.NewReader(strings.Repeat("a", inboundWebhookMaxBodyBytes+1)))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("oversized webhook body recorded inbound marker")
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

func newSignedSlackRequest(path string, secret string, body []byte, timestamp string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("X-Slack-Request-Timestamp", timestamp)
	req.Header.Set("X-Slack-Signature", slackWebhookSignature(secret, timestamp, body))
	return req
}

func slackWebhookSignature(secret string, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("v0:" + timestamp + ":" + string(body)))
	return "v0=" + hex.EncodeToString(mac.Sum(nil))
}

func stripeWebhookSignature(secret string, timestamp string, body []byte) string {
	return "t=" + timestamp + ",v1=" + stripeSignatureHex(secret, timestamp, body)
}

func stripeSignatureHex(secret string, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp + "." + string(body)))
	return hex.EncodeToString(mac.Sum(nil))
}

type blockingInboundInterceptor struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (i blockingInboundInterceptor) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	select {
	case i.started <- struct{}{}:
	default:
	}
	select {
	case <-i.release:
		return true, nil, nil
	case <-ctx.Done():
		return false, nil, ctx.Err()
	}
}
