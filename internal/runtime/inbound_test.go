package runtime

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
)

type testInboundTargetResolver interface {
	ResolveInboundTarget(context.Context, string, string) (InboundTarget, error)
}

type testInboundGateway struct {
	*InboundGateway
	resolver testInboundTargetResolver
}

func (g *testInboundGateway) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		alias, provider, ok := parseWebhookPath(r.URL.Path)
		if !ok {
			http.Error(w, "expected /webhooks/{alias}/{provider}", http.StatusBadRequest)
			return
		}
		if g == nil || g.resolver == nil {
			http.Error(w, fmt.Sprintf("no ingress target %q is declared; add ingress to a standing singleton flow", alias), http.StatusNotFound)
			return
		}
		target, err := g.resolver.ResolveInboundTarget(r.Context(), alias, provider)
		if err != nil {
			http.Error(w, fmt.Sprintf("no ingress target %q is declared; add ingress to a standing singleton flow", alias), http.StatusNotFound)
			return
		}
		g.HandleResolvedWebhook(w, r, target, nil)
	})
}

func newTestInboundGateway(t *testing.T, bus *runtimebus.EventBus, logger *RuntimeLogger, shutdownAdmissionClosed func() bool, stores ...InboundPersistence) *testInboundGateway {
	t.Helper()
	root := filepath.Join("..", "..", "packs", "provider-triggers")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read provider trigger pack root: %v", err)
	}
	dirs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, filepath.Join(root, entry.Name()))
		}
	}
	sort.Strings(dirs)
	registry, _, err := providertriggers.NewRegistryFromPackDirs("0.7.0", dirs, nil)
	if err != nil {
		t.Fatalf("load provider trigger registry: %v", err)
	}
	gateway := NewInboundGatewayWithProviderRegistry(bus, logger, shutdownAdmissionClosed, registry, stores...)
	gateway.SetCredentialStore(identityInboundCredentialStore{})
	var resolver testInboundTargetResolver
	if len(stores) > 0 {
		resolver, _ = any(stores[0]).(testInboundTargetResolver)
	}
	return &testInboundGateway{InboundGateway: gateway, resolver: resolver}
}

type identityInboundCredentialStore struct{}

func (identityInboundCredentialStore) Get(_ context.Context, key string) (string, bool, error) {
	return key, strings.TrimSpace(key) != "", nil
}
func (identityInboundCredentialStore) Set(context.Context, string, string) error { return nil }
func (identityInboundCredentialStore) List(context.Context) ([]string, error)    { return nil, nil }
func (identityInboundCredentialStore) Delete(context.Context, string) error      { return nil }

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

func TestInboundGatewayResolvedTargetPreservesStandingAuthority(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{inserted: true}
	gateway := newTestInboundGateway(t, bus, nil, nil, store)
	body := []byte(`{"update_id":123,"message":{"chat":{"id":42},"text":"hello"}}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/chat/telegram", strings.NewReader(string(body)))
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
	rec := httptest.NewRecorder()
	gateway.HandleResolvedWebhook(rec, req, InboundTarget{
		BundleHash: "bundle-v1:sha256:" + strings.Repeat("a", 64), FlowID: "chat-flow",
		RunID: "41000000-0000-0000-0000-000000000001", FlowInstance: "chat-flow/@standing/a",
		EntityID: "41000000-0000-0000-0000-000000000002", Alias: "chat", Provider: "telegram",
		SigningSecret: "telegram-secret",
	}, nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s, want 202", rec.Code, rec.Body.String())
	}
	if len(eventStore.events) != 1 {
		t.Fatalf("persisted events = %d, want 1", len(eventStore.events))
	}
	evt := eventStore.events[0]
	if evt.RunID() != "41000000-0000-0000-0000-000000000001" || evt.FlowInstance() != "chat-flow/@standing/a" || evt.EntityID() != "41000000-0000-0000-0000-000000000002" {
		t.Fatalf("event authority = run=%s flow_instance=%s entity=%s", evt.RunID(), evt.FlowInstance(), evt.EntityID())
	}
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
	resolveErr      error
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
	return s.target, s.resolveErr
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
	g := newTestInboundGateway(t, bus, nil, nil, store)

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
	g := newTestInboundGateway(t, bus, nil, func() bool { return true }, store)

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

func TestInboundGateway_UnknownTargetFailsBeforeProviderAdmission(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{resolveErr: errors.New("target not found")}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	// A GitHub request without a signature would fail provider admission with 401
	// if target resolution did not gate the provider interpreter first.
	req := httptest.NewRequest(http.MethodPost, "/webhooks/unknown/github", strings.NewReader(`{"id":"evt-1"}`))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "no ingress target \"unknown\" is declared") {
		t.Fatalf("response = %d %q, want target-gate 404", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("unknown target reached provider marker persistence")
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
	g := newTestInboundGateway(t, bus, nil, nil, store)
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
			SigningSecret: "github-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)
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
			SigningSecret: "github-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

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
			SigningSecret: "github-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

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
			SigningSecret: "github-secret",
		},
		inserted: false,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

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
			SigningSecret: "slack-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

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
			SigningSecret: "slack-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

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
	g := newTestInboundGateway(t, bus, nil, nil, store)

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
					SigningSecret: "slack-secret",
				},
				inserted: true,
			}
			g := newTestInboundGateway(t, bus, nil, nil, store)

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
			SigningSecret: "slack-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

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
			SigningSecret: "slack-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

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
			SigningSecret: "slack-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

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
			SigningSecret: "slack-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

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
			SigningSecret: "slack-secret",
		},
		inserted: false,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

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
			SigningSecret: "stripe-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

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
			name: "wrong signature version param",
			body: []byte(`{"id":"evt_123","type":"invoice.paid"}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("Stripe-Signature", "t="+timestamp+",v0="+stripeSignatureHex("stripe-secret", timestamp, body))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "malformed signature component with otherwise valid signature",
			body: []byte(`{"id":"evt_123","type":"invoice.paid"}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("Stripe-Signature", "t="+timestamp+",broken,v1="+stripeSignatureHex("stripe-secret", timestamp, body))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "duplicate timestamp params with otherwise valid signature",
			body: []byte(`{"id":"evt_123","type":"invoice.paid"}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("Stripe-Signature", "t="+timestamp+",t="+timestamp+",v1="+stripeSignatureHex("stripe-secret", timestamp, body))
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
			name: "object event id",
			body: []byte(`{"id":{"nested":"evt_123"},"type":"invoice.paid"}`),
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
		{
			name: "bool event type",
			body: []byte(`{"id":"evt_123","type":true}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("Stripe-Signature", stripeWebhookSignature("stripe-secret", timestamp, body))
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "list event type",
			body: []byte(`{"id":"evt_123","type":["invoice.paid"]}`),
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
					SigningSecret: "stripe-secret",
				},
				inserted: true,
			}
			g := newTestInboundGateway(t, bus, nil, nil, store)

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
			SigningSecret: "stripe-secret",
		},
		inserted: false,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

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

func TestInboundGateway_TwilioManifestOwnsURLFormSignatureAndLiteralEvent(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "twilio-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	requestURL := "https://example.com/webhooks/customer-a/twilio?tenant=alpha"
	form := url.Values{
		"Body":          {"hello from twilio"},
		"From":          {"+15551234567"},
		"MessageSid":    {"SM1234567890abcdef"},
		"To":            {"+15557654321"},
		"UnexpectedNew": {"still signed"},
	}
	req := newSignedTwilioRequest(requestURL, "twilio-secret", form)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if !store.recorded {
		t.Fatal("expected Twilio delivery to record inbound marker")
	}
	if store.providerEventID != "SM1234567890abcdef" {
		t.Fatalf("providerEventID = %q, want MessageSid", store.providerEventID)
	}
	if store.provider != "twilio" {
		t.Fatalf("provider = %q, want twilio", store.provider)
	}
	if len(eventStore.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(eventStore.events))
	}
	evt := eventStore.events[0]
	if evt.Type() != events.EventType("inbound.twilio") {
		t.Fatalf("event type = %q, want inbound.twilio", evt.Type())
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["provider_event_id"] != "SM1234567890abcdef" ||
		payload["provider_event_type"] != "message_received" ||
		payload["provider"] != "twilio" {
		t.Fatalf("payload = %+v, want Twilio manifest delivery identity", payload)
	}
	formPayload, ok := payload["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload.payload = %T, want form payload map", payload["payload"])
	}
	if formPayload["Body"] != "hello from twilio" || formPayload["UnexpectedNew"] != "still signed" {
		t.Fatalf("form payload = %+v, want evolving Twilio form parameters preserved", formPayload)
	}
	if strings.Contains(rec.Body.String(), "twilio-secret") || strings.Contains(string(evt.Payload()), "twilio-secret") {
		t.Fatal("Twilio signing secret leaked into readback or event payload")
	}
}

func TestInboundGateway_TwilioRejectsInvalidInputsBeforeMarkerAndPublish(t *testing.T) {
	for _, tc := range []struct {
		name       string
		requestURL string
		form       url.Values
		configure  func(*http.Request, url.Values, string)
		wantStatus int
	}{
		{
			name:       "missing signature",
			requestURL: "https://example.com/webhooks/customer-a/twilio?tenant=alpha",
			form:       url.Values{"MessageSid": {"SM1234567890abcdef"}, "Body": {"hello"}},
			configure: func(req *http.Request, form url.Values, requestURL string) {
				req.Header.Del("X-Twilio-Signature")
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "url mismatch",
			requestURL: "https://example.com/webhooks/customer-a/twilio?tenant=beta",
			form:       url.Values{"MessageSid": {"SM1234567890abcdef"}, "Body": {"hello"}},
			configure: func(req *http.Request, form url.Values, requestURL string) {
				req.Header.Set("X-Twilio-Signature", twilioWebhookSignature("twilio-secret", "https://example.com/webhooks/customer-a/twilio?tenant=alpha", form))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "duplicate query params",
			requestURL: "https://example.com/webhooks/customer-a/twilio?tenant=alpha&tenant=beta",
			form:       url.Values{"MessageSid": {"SM1234567890abcdef"}, "Body": {"hello"}},
			configure:  func(*http.Request, url.Values, string) {},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "duplicate form params",
			requestURL: "https://example.com/webhooks/customer-a/twilio?tenant=alpha",
			form:       url.Values{"MessageSid": {"SM1234567890abcdef"}, "Body": {"hello", "tampered"}},
			configure:  func(*http.Request, url.Values, string) {},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "missing message sid",
			requestURL: "https://example.com/webhooks/customer-a/twilio?tenant=alpha",
			form:       url.Values{"Body": {"hello"}},
			configure:  func(*http.Request, url.Values, string) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "json body sha256 mode unsupported in this slice",
			requestURL: "https://example.com/webhooks/customer-a/twilio?bodySHA256=abc123",
			form:       url.Values{"MessageSid": {"SM1234567890abcdef"}, "Body": {"hello"}},
			configure: func(req *http.Request, form url.Values, requestURL string) {
				req.Header.Set("Content-Type", "application/json")
				req.Body = io.NopCloser(strings.NewReader(`{"MessageSid":"SM1234567890abcdef"}`))
				req.ContentLength = int64(len(`{"MessageSid":"SM1234567890abcdef"}`))
			},
			wantStatus: http.StatusUnauthorized,
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
					SigningSecret: "twilio-secret",
				},
				inserted: true,
			}
			g := newTestInboundGateway(t, bus, nil, nil, store)
			req := newSignedTwilioRequest(tc.requestURL, "twilio-secret", tc.form)
			tc.configure(req, tc.form, tc.requestURL)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if store.recorded {
				t.Fatal("invalid Twilio request recorded inbound marker")
			}
			if len(eventStore.events) != 0 {
				t.Fatalf("published events = %d, want 0", len(eventStore.events))
			}
		})
	}
}

func TestInboundGateway_TwilioDuplicateDeliveryDoesNotPublishAgain(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "twilio-secret",
		},
		inserted: false,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	requestURL := "https://example.com/webhooks/customer-a/twilio?tenant=alpha"
	form := url.Values{"MessageSid": {"SM1234567890abcdef"}, "Body": {"hello"}}
	req := newSignedTwilioRequest(requestURL, "twilio-secret", form)
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

func TestInboundGateway_ShopifyManifestOwnsRawBodySignatureDeliveryIDAndTopic(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "shopify-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`)
	req := newSignedShopifyRequest("/webhooks/customer-a/shopify", "shopify-secret", body)
	req.Header.Set("X-Shopify-Webhook-Id", "webhook-123")
	req.Header.Set("X-Shopify-Topic", "orders/create")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if !store.recorded {
		t.Fatal("expected Shopify delivery to record inbound marker")
	}
	if store.providerEventID != "webhook-123" {
		t.Fatalf("providerEventID = %q, want webhook-123", store.providerEventID)
	}
	if store.provider != "shopify" {
		t.Fatalf("provider = %q, want shopify", store.provider)
	}
	if len(eventStore.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(eventStore.events))
	}
	evt := eventStore.events[0]
	if evt.Type() != events.EventType("inbound.shopify") {
		t.Fatalf("event type = %q, want inbound.shopify", evt.Type())
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["provider_event_id"] != "webhook-123" ||
		payload["provider_event_type"] != "orders_create" ||
		payload["provider"] != "shopify" {
		t.Fatalf("payload = %+v, want Shopify manifest delivery identity", payload)
	}
	if strings.Contains(rec.Body.String(), "shopify-secret") || strings.Contains(string(evt.Payload()), "shopify-secret") {
		t.Fatal("Shopify signing secret leaked into readback or event payload")
	}
}

func TestInboundGateway_ShopifyRejectsInvalidInputsBeforeMarkerAndPublish(t *testing.T) {
	for _, tc := range []struct {
		name       string
		body       []byte
		configure  func(*http.Request, []byte)
		wantStatus int
	}{
		{
			name:       "missing signature",
			body:       []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`),
			configure:  func(*http.Request, []byte) {},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "invalid signature",
			body: []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`),
			configure: func(req *http.Request, body []byte) {
				req.Header.Set("X-Shopify-Hmac-Sha256", shopifyWebhookSignature("wrong-secret", body))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "raw body mutation",
			body: []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`),
			configure: func(req *http.Request, body []byte) {
				signedBody := []byte(`{"line_items":[{"sku":"abc"}],"id":123}`)
				req.Header.Set("X-Shopify-Hmac-Sha256", shopifyWebhookSignature("shopify-secret", signedBody))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "missing webhook id",
			body: []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`),
			configure: func(req *http.Request, body []byte) {
				req.Header.Set("X-Shopify-Hmac-Sha256", shopifyWebhookSignature("shopify-secret", body))
				req.Header.Del("X-Shopify-Webhook-Id")
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing topic",
			body: []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`),
			configure: func(req *http.Request, body []byte) {
				req.Header.Set("X-Shopify-Hmac-Sha256", shopifyWebhookSignature("shopify-secret", body))
				req.Header.Del("X-Shopify-Topic")
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "non object payload",
			body: []byte(`[{"id":123}]`),
			configure: func(req *http.Request, body []byte) {
				req.Header.Set("X-Shopify-Hmac-Sha256", shopifyWebhookSignature("shopify-secret", body))
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
					SigningSecret: "shopify-secret",
				},
				inserted: true,
			}
			g := newTestInboundGateway(t, bus, nil, nil, store)
			req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/shopify", strings.NewReader(string(tc.body)))
			req.Header.Set("X-Shopify-Webhook-Id", "webhook-123")
			req.Header.Set("X-Shopify-Topic", "orders/create")
			tc.configure(req, tc.body)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if store.recorded {
				t.Fatal("invalid Shopify request recorded inbound marker")
			}
			if len(eventStore.events) != 0 {
				t.Fatalf("published events = %d, want 0", len(eventStore.events))
			}
		})
	}
}

func TestInboundGateway_ShopifyDuplicateDeliveryDoesNotPublishAgain(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "shopify-secret",
		},
		inserted: false,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`)
	req := newSignedShopifyRequest("/webhooks/customer-a/shopify", "shopify-secret", body)
	req.Header.Set("X-Shopify-Webhook-Id", "webhook-123")
	req.Header.Set("X-Shopify-Topic", "orders/create")
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

func TestInboundGateway_TypeformAndIntercomManifestsOwnRawBodySignatureDeliveryIDAndEventType(t *testing.T) {
	for _, tc := range []struct {
		name              string
		provider          string
		secret            string
		body              []byte
		newRequest        func(path string, secret string, body []byte) *http.Request
		wantProviderID    string
		wantProviderType  string
		wantEventName     events.EventType
		wantMetadataKeyID string
		wantMetadataKeyTy string
	}{
		{
			name:              "typeform",
			provider:          "typeform",
			secret:            "typeform-secret",
			body:              []byte(`{"event_id":"tf-evt-123","event_type":"form_response","form_response":{"token":"abc"}}`),
			newRequest:        newSignedTypeformRequest,
			wantProviderID:    "tf-evt-123",
			wantProviderType:  "form_response",
			wantEventName:     events.EventType("inbound.typeform"),
			wantMetadataKeyID: "typeform_event_id",
			wantMetadataKeyTy: "typeform_event_type",
		},
		{
			name:              "intercom",
			provider:          "intercom",
			secret:            "intercom-secret",
			body:              []byte(`{"id":"notif_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
			newRequest:        newSignedIntercomRequest,
			wantProviderID:    "notif_123",
			wantProviderType:  "conversation_user_created",
			wantEventName:     events.EventType("inbound.intercom"),
			wantMetadataKeyID: "intercom_notification_id",
			wantMetadataKeyTy: "intercom_topic",
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
					SigningSecret: tc.secret,
				},
				inserted: true,
			}
			g := newTestInboundGateway(t, bus, nil, nil, store)

			req := tc.newRequest("/webhooks/customer-a/"+tc.provider, tc.secret, tc.body)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
			}
			if !store.recorded {
				t.Fatalf("expected %s delivery to record inbound marker", tc.provider)
			}
			if store.providerEventID != tc.wantProviderID {
				t.Fatalf("providerEventID = %q, want %s", store.providerEventID, tc.wantProviderID)
			}
			if store.provider != tc.provider {
				t.Fatalf("provider = %q, want %s", store.provider, tc.provider)
			}
			if len(eventStore.events) != 1 {
				t.Fatalf("published events = %d, want 1", len(eventStore.events))
			}
			evt := eventStore.events[0]
			if evt.Type() != tc.wantEventName {
				t.Fatalf("event type = %q, want %s", evt.Type(), tc.wantEventName)
			}
			var payload map[string]any
			if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
				t.Fatalf("unmarshal payload: %v", err)
			}
			headers, ok := payload["headers"].(map[string]any)
			if !ok {
				t.Fatalf("headers = %T, want metadata map", payload["headers"])
			}
			if payload["provider_event_id"] != tc.wantProviderID ||
				payload["provider_event_type"] != tc.wantProviderType ||
				payload["provider"] != tc.provider ||
				headers[tc.wantMetadataKeyID] != tc.wantProviderID ||
				headers[tc.wantMetadataKeyTy] != tc.wantProviderType {
				t.Fatalf("payload = %+v, want %s manifest delivery identity", payload, tc.provider)
			}
			if strings.Contains(rec.Body.String(), tc.secret) || strings.Contains(string(evt.Payload()), tc.secret) {
				t.Fatalf("%s signing secret leaked into readback or event payload", tc.provider)
			}
		})
	}
}

func TestInboundGateway_TypeformAndIntercomRejectInvalidInputsBeforeMarkerAndPublish(t *testing.T) {
	for _, providerCase := range []struct {
		provider   string
		secret     string
		validBody  []byte
		newRequest func(path string, secret string, body []byte) *http.Request
		cases      []struct {
			name       string
			body       []byte
			configure  func(*http.Request, []byte)
			wantStatus int
		}
	}{
		{
			provider:   "typeform",
			secret:     "typeform-secret",
			validBody:  []byte(`{"event_id":"tf-evt-123","event_type":"form_response","form_response":{"token":"abc"}}`),
			newRequest: newSignedTypeformRequest,
			cases: []struct {
				name       string
				body       []byte
				configure  func(*http.Request, []byte)
				wantStatus int
			}{
				{
					name:       "missing signature",
					body:       []byte(`{"event_id":"tf-evt-123","event_type":"form_response","form_response":{"token":"abc"}}`),
					configure:  func(req *http.Request, _ []byte) { req.Header.Del("Typeform-Signature") },
					wantStatus: http.StatusUnauthorized,
				},
				{
					name: "invalid signature",
					body: []byte(`{"event_id":"tf-evt-123","event_type":"form_response","form_response":{"token":"abc"}}`),
					configure: func(req *http.Request, body []byte) {
						req.Header.Set("Typeform-Signature", typeformWebhookSignature("wrong-secret", body))
					},
					wantStatus: http.StatusUnauthorized,
				},
				{
					name: "raw body mutation",
					body: []byte(`{"event_id":"tf-evt-123","event_type":"form_response","form_response":{"token":"abc"}}`),
					configure: func(req *http.Request, _ []byte) {
						signedBody := []byte(`{"event_type":"form_response","event_id":"tf-evt-123","form_response":{"token":"abc"}}`)
						req.Header.Set("Typeform-Signature", typeformWebhookSignature("typeform-secret", signedBody))
					},
					wantStatus: http.StatusUnauthorized,
				},
				{
					name:       "missing delivery id",
					body:       []byte(`{"event_type":"form_response","form_response":{"token":"abc"}}`),
					configure:  func(*http.Request, []byte) {},
					wantStatus: http.StatusBadRequest,
				},
				{
					name:       "missing event type",
					body:       []byte(`{"event_id":"tf-evt-123","form_response":{"token":"abc"}}`),
					configure:  func(*http.Request, []byte) {},
					wantStatus: http.StatusBadRequest,
				},
				{
					name:       "non object payload",
					body:       []byte(`[{"event_id":"tf-evt-123","event_type":"form_response"}]`),
					configure:  func(*http.Request, []byte) {},
					wantStatus: http.StatusBadRequest,
				},
			},
		},
		{
			provider:   "intercom",
			secret:     "intercom-secret",
			validBody:  []byte(`{"id":"notif_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
			newRequest: newSignedIntercomRequest,
			cases: []struct {
				name       string
				body       []byte
				configure  func(*http.Request, []byte)
				wantStatus int
			}{
				{
					name:       "missing signature",
					body:       []byte(`{"id":"notif_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
					configure:  func(req *http.Request, _ []byte) { req.Header.Del("X-Hub-Signature") },
					wantStatus: http.StatusUnauthorized,
				},
				{
					name: "invalid signature",
					body: []byte(`{"id":"notif_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
					configure: func(req *http.Request, body []byte) {
						req.Header.Set("X-Hub-Signature", intercomWebhookSignature("wrong-secret", body))
					},
					wantStatus: http.StatusUnauthorized,
				},
				{
					name: "raw body mutation",
					body: []byte(`{"id":"notif_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
					configure: func(req *http.Request, _ []byte) {
						signedBody := []byte(`{"topic":"conversation.user.created","id":"notif_123","data":{"item":{"id":"conv_1"}}}`)
						req.Header.Set("X-Hub-Signature", intercomWebhookSignature("intercom-secret", signedBody))
					},
					wantStatus: http.StatusUnauthorized,
				},
				{
					name:       "missing delivery id",
					body:       []byte(`{"topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
					configure:  func(*http.Request, []byte) {},
					wantStatus: http.StatusBadRequest,
				},
				{
					name:       "missing event type",
					body:       []byte(`{"id":"notif_123","data":{"item":{"id":"conv_1"}}}`),
					configure:  func(*http.Request, []byte) {},
					wantStatus: http.StatusBadRequest,
				},
				{
					name:       "non object payload",
					body:       []byte(`[{"id":"notif_123","topic":"conversation.user.created"}]`),
					configure:  func(*http.Request, []byte) {},
					wantStatus: http.StatusBadRequest,
				},
			},
		},
	} {
		for _, tc := range providerCase.cases {
			t.Run(providerCase.provider+"/"+tc.name, func(t *testing.T) {
				eventStore := &capturingInboundEventStore{}
				bus, err := runtimebus.NewEventBus(eventStore)
				if err != nil {
					t.Fatalf("NewEventBus: %v", err)
				}
				store := &recordingInboundStore{
					target: InboundTarget{
						EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
						EntitySlug:    "customer-a",
						SigningSecret: providerCase.secret,
					},
					inserted: true,
				}
				g := newTestInboundGateway(t, bus, nil, nil, store)
				body := tc.body
				if len(body) == 0 {
					body = providerCase.validBody
				}
				req := providerCase.newRequest("/webhooks/customer-a/"+providerCase.provider, providerCase.secret, body)
				tc.configure(req, body)
				rec := httptest.NewRecorder()
				g.Handler().ServeHTTP(rec, req)

				if rec.Code != tc.wantStatus {
					t.Fatalf("status = %d, want %d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
				}
				if store.recorded {
					t.Fatalf("invalid %s request recorded inbound marker", providerCase.provider)
				}
				if len(eventStore.events) != 0 {
					t.Fatalf("published events = %d, want 0", len(eventStore.events))
				}
			})
		}
	}
}

func TestInboundGateway_TypeformAndIntercomDuplicateDeliveryDoesNotPublishAgain(t *testing.T) {
	for _, tc := range []struct {
		provider   string
		secret     string
		body       []byte
		newRequest func(path string, secret string, body []byte) *http.Request
	}{
		{
			provider:   "typeform",
			secret:     "typeform-secret",
			body:       []byte(`{"event_id":"tf-evt-123","event_type":"form_response","form_response":{"token":"abc"}}`),
			newRequest: newSignedTypeformRequest,
		},
		{
			provider:   "intercom",
			secret:     "intercom-secret",
			body:       []byte(`{"id":"notif_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
			newRequest: newSignedIntercomRequest,
		},
	} {
		t.Run(tc.provider, func(t *testing.T) {
			eventStore := &capturingInboundEventStore{}
			bus, err := runtimebus.NewEventBus(eventStore)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			store := &recordingInboundStore{
				target: InboundTarget{
					EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
					EntitySlug:    "customer-a",
					SigningSecret: tc.secret,
				},
				inserted: false,
			}
			g := newTestInboundGateway(t, bus, nil, nil, store)

			req := tc.newRequest("/webhooks/customer-a/"+tc.provider, tc.secret, tc.body)
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
		})
	}
}

func TestInboundGateway_TelegramManifestOwnsTokenDeliveryIDLiteralEventAndAck(t *testing.T) {
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
			SigningSecret: "telegram-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"update_id":123456789,"message":{"message_id":7,"chat":{"id":42},"text":"hello"}}`)
	req := newSignedTelegramRequest("/webhooks/customer-a/telegram", "telegram-secret", body)
	responseDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		g.Handler().ServeHTTP(rec, req)
		responseDone <- rec
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("Telegram callback post-commit dispatch did not start")
	}
	select {
	case rec := <-responseDone:
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "telegram-secret") {
			t.Fatal("Telegram secret token leaked into readback")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Telegram callback response waited for post-commit dispatch completion")
	}
	if !store.recorded {
		t.Fatal("expected Telegram delivery to record inbound marker")
	}
	if store.providerEventID != "123456789" {
		t.Fatalf("providerEventID = %q, want 123456789", store.providerEventID)
	}
	if store.provider != "telegram" {
		t.Fatalf("provider = %q, want telegram", store.provider)
	}
	if len(eventStore.events) != 1 {
		t.Fatalf("published events = %d, want 1 before dispatch release", len(eventStore.events))
	}
	evt := eventStore.events[0]
	if evt.Type() != events.EventType("inbound.telegram") {
		t.Fatalf("event type = %q, want inbound.telegram", evt.Type())
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	headers, ok := payload["headers"].(map[string]any)
	if !ok {
		t.Fatalf("headers = %T, want metadata map", payload["headers"])
	}
	if payload["provider_event_id"] != "123456789" ||
		payload["provider_event_type"] != "update" ||
		payload["provider"] != "telegram" ||
		headers["telegram_update_id"] != "123456789" ||
		headers["telegram_update_type"] != "update" {
		t.Fatalf("payload = %+v, want Telegram manifest delivery identity", payload)
	}
	if strings.Contains(string(evt.Payload()), "telegram-secret") {
		t.Fatal("Telegram secret token leaked into event payload")
	}

	releaseDispatch()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence after dispatch release: %v", err)
	}
}

func TestInboundGateway_TelegramRejectsInvalidInputsBeforeMarkerAndPublish(t *testing.T) {
	for _, tc := range []struct {
		name       string
		body       []byte
		target     InboundTarget
		configure  func(*http.Request, []byte)
		wantStatus int
	}{
		{
			name:       "missing configured secret",
			body:       []byte(`{"update_id":123456789,"message":{"message_id":7,"text":"hello"}}`),
			target:     InboundTarget{EntityID: "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a", EntitySlug: "customer-a"},
			configure:  func(*http.Request, []byte) {},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "missing token header",
			body:       []byte(`{"update_id":123456789,"message":{"message_id":7,"text":"hello"}}`),
			configure:  func(req *http.Request, _ []byte) { req.Header.Del("X-Telegram-Bot-Api-Secret-Token") },
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "duplicate token header",
			body: []byte(`{"update_id":123456789,"message":{"message_id":7,"text":"hello"}}`),
			configure: func(req *http.Request, _ []byte) {
				req.Header.Add("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "invalid token header",
			body: []byte(`{"update_id":123456789,"message":{"message_id":7,"text":"hello"}}`),
			configure: func(req *http.Request, _ []byte) {
				req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "wrong-secret")
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "missing update id",
			body:       []byte(`{"message":{"message_id":7,"text":"hello"}}`),
			configure:  func(*http.Request, []byte) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non object payload",
			body:       []byte(`[{"update_id":123456789}]`),
			configure:  func(*http.Request, []byte) {},
			wantStatus: http.StatusBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventStore := &capturingInboundEventStore{}
			bus, err := runtimebus.NewEventBus(eventStore)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			target := tc.target
			if target.EntityID == "" {
				target = InboundTarget{
					EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
					EntitySlug:    "customer-a",
					SigningSecret: "telegram-secret",
				}
			}
			store := &recordingInboundStore{
				target:   target,
				inserted: true,
			}
			g := newTestInboundGateway(t, bus, nil, nil, store)

			req := newSignedTelegramRequest("/webhooks/customer-a/telegram", "telegram-secret", tc.body)
			tc.configure(req, tc.body)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if store.recorded {
				t.Fatal("invalid Telegram request recorded inbound marker")
			}
			if len(eventStore.events) != 0 {
				t.Fatalf("published events = %d, want 0", len(eventStore.events))
			}
		})
	}
}

func TestInboundGateway_TelegramDuplicateDeliveryDoesNotPublishAgain(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "telegram-secret",
		},
		inserted: false,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"update_id":123456789,"message":{"message_id":7,"text":"hello"}}`)
	req := newSignedTelegramRequest("/webhooks/customer-a/telegram", "telegram-secret", body)
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

func TestInboundGateway_RawFallbackDoesNotInterpretTypeformOrIntercomSignatures(t *testing.T) {
	for _, tc := range []struct {
		name      string
		body      []byte
		configure func(*http.Request, []byte)
	}{
		{
			name: "typeform",
			body: []byte(`{"event_id":"tf-evt-123","event_type":"form_response"}`),
			configure: func(req *http.Request, body []byte) {
				req.Header.Set("Typeform-Signature", typeformWebhookSignature("raw-secret", body))
			},
		},
		{
			name: "intercom",
			body: []byte(`{"id":"notif_123","topic":"conversation.user.created"}`),
			configure: func(req *http.Request, body []byte) {
				req.Header.Set("X-Hub-Signature", intercomWebhookSignature("raw-secret", body))
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
					SigningSecret: "raw-secret",
				},
				inserted: true,
			}
			g := newTestInboundGateway(t, bus, nil, nil, store)

			req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/custom", strings.NewReader(string(tc.body)))
			tc.configure(req, tc.body)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401 body=%s", rec.Code, rec.Body.String())
			}
			if store.recorded {
				t.Fatalf("raw fallback accepted %s signature and recorded inbound marker", tc.name)
			}
			if len(eventStore.events) != 0 {
				t.Fatalf("published events = %d, want 0", len(eventStore.events))
			}
		})
	}
}

func TestInboundGateway_RawFallbackDoesNotInterpretTelegramSecretToken(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "raw-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"update_id":123456789}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/custom", strings.NewReader(string(body)))
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "raw-secret")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("raw fallback accepted Telegram secret token and recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_RawFallbackDoesNotInterpretShopifySignature(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "raw-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/custom", strings.NewReader(string(body)))
	req.Header.Set("X-Shopify-Hmac-Sha256", shopifyWebhookSignature("raw-secret", body))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("raw fallback accepted Shopify signature and recorded inbound marker")
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
			SigningSecret: "raw-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

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
			SigningSecret: "github-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

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

func newSignedTwilioRequest(requestURL string, secret string, form url.Values) *http.Request {
	req := httptest.NewRequest(http.MethodPost, requestURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Twilio-Signature", twilioWebhookSignature(secret, requestURL, form))
	return req
}

func twilioWebhookSignature(secret, requestURL string, form url.Values) string {
	mac := hmac.New(sha1.New, []byte(secret))
	_, _ = mac.Write([]byte(twilioSignedPayload(requestURL, form)))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func twilioSignedPayload(requestURL string, form url.Values) string {
	keys := make([]string, 0, len(form))
	for key := range form {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(requestURL)
	for _, key := range keys {
		b.WriteString(key)
		b.WriteString(form.Get(key))
	}
	return b.String()
}

func newSignedShopifyRequest(path string, secret string, body []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("X-Shopify-Hmac-Sha256", shopifyWebhookSignature(secret, body))
	return req
}

func shopifyWebhookSignature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func newSignedTelegramRequest(path string, secret string, body []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)
	return req
}

func newSignedTypeformRequest(path string, secret string, body []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("Typeform-Signature", typeformWebhookSignature(secret, body))
	return req
}

func typeformWebhookSignature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func newSignedIntercomRequest(path string, secret string, body []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature", intercomWebhookSignature(secret, body))
	return req
}

func intercomWebhookSignature(secret string, body []byte) string {
	mac := hmac.New(sha1.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha1=" + hex.EncodeToString(mac.Sum(nil))
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
