package runtime_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestInboundGateway_GitHubPausedRuntimePersistsAndReleasesSubscribedDispatch(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	const (
		runID             = "41000000-0000-0000-0000-000000000001"
		entityID          = "41000000-0000-0000-0000-000000000002"
		flowInstance      = "github-provider-trigger-instance"
		entitySlug        = "customer-a"
		provider          = "github"
		webhookSecret     = "github-secret"
		providerEventID   = "delivery-123"
		agentID           = "github-webhook-subscriber"
		providerEventName = "inbound.github.push"
	)
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	pg := &store.PostgresStore{DB: db}
	seedPostgresInboundGatewayRuntime(t, ctx, db, pg, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeingress.NewController(pg, bus, runtimeingress.Options{})
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	bus.SetRuntimeIngressDispatchGate(controller)

	eventType := events.EventType(providerEventName)
	ch := bus.Subscribe(agentID, eventType)
	defer bus.Unsubscribe(agentID)

	if _, err := controller.Pause(ctx, runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	g := runtimepkg.NewInboundGateway(bus, nil, nil, pg)
	g.SetRuntimeIngress(controller)

	body := []byte(`{"zen":"Keep it logically awesome."}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/github", strings.NewReader(string(body)))
	req = req.WithContext(ctx)
	req.Header.Set("X-Hub-Signature-256", githubWebhookSignature(webhookSecret, body))
	req.Header.Set("X-GitHub-Delivery", providerEventID)
	req.Header.Set("X-GitHub-Event", "push")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if got := countPostgresInboundMarkers(t, ctx, db, providerEventID, entityID, provider); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadPostgresInboundProviderEventID(t, ctx, db, runID, entityID, providerEventName, providerEventID)
	if got := countPostgresInboundProviderEvents(t, ctx, db, runID, entityID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	requireNoInboundBusEvent(t, ch, "paused GitHub webhook before resume")
	if got := countPostgresAgentDeliveriesForEvent(t, ctx, db, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows while paused = %d, want 1", got)
	}
	if got := loadPostgresAgentDeliveryStatus(t, ctx, db, eventID, agentID); got != "pending" {
		t.Fatalf("agent delivery status while paused = %q, want pending", got)
	}
	if got := countPostgresPipelineReceiptsForEvent(t, ctx, db, eventID); got != 0 {
		t.Fatalf("pipeline receipts while paused = %d, want 0", got)
	}
	if got := countPostgresAgentReceiptsForEvent(t, ctx, db, eventID, agentID); got != 0 {
		t.Fatalf("agent receipts while paused = %d, want 0", got)
	}

	resumed, err := controller.Resume(ctx, runtimeingress.TransitionRequest{
		Reason:       "test_resume",
		ControlledBy: "test",
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.ReleasedCount != 1 {
		t.Fatalf("released count = %d, want 1", resumed.ReleasedCount)
	}
	got := requireInboundBusEvent(t, ch, "paused GitHub webhook release after resume")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}
	if got.Type() != eventType {
		t.Fatalf("delivered event type = %s, want %s", got.Type(), eventType)
	}
	requireNoInboundBusEvent(t, ch, "paused GitHub webhook releases exactly once")
	if got := countPostgresPipelineReceiptsForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("pipeline receipts after resume = %d, want 1", got)
	}
	if got := countPostgresInboundMarkers(t, ctx, db, providerEventID, entityID, provider); got != 1 {
		t.Fatalf("inbound marker rows after resume = %d, want 1", got)
	}
	if got := countPostgresInboundProviderEvents(t, ctx, db, runID, entityID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows after resume = %d, want 1", got)
	}
}

func TestInboundGateway_SlackPausedRuntimePersistsAndReleasesSubscribedDispatch(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	const (
		runID             = "42000000-0000-0000-0000-000000000001"
		entityID          = "42000000-0000-0000-0000-000000000002"
		flowInstance      = "slack-provider-trigger-instance"
		entitySlug        = "customer-a"
		provider          = "slack"
		webhookSecret     = "slack-secret"
		providerEventID   = "Ev123ABC456"
		agentID           = "slack-webhook-subscriber"
		providerEventName = "inbound.slack.message"
	)
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	pg := &store.PostgresStore{DB: db}
	seedPostgresInboundGatewayRuntime(t, ctx, db, pg, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeingress.NewController(pg, bus, runtimeingress.Options{})
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	bus.SetRuntimeIngressDispatchGate(controller)

	eventType := events.EventType(providerEventName)
	ch := bus.Subscribe(agentID, eventType)
	defer bus.Unsubscribe(agentID)

	if _, err := controller.Pause(ctx, runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	g := runtimepkg.NewInboundGateway(bus, nil, nil, pg)
	g.SetRuntimeIngress(controller)

	body := []byte(`{"type":"event_callback","event_id":"Ev123ABC456","event":{"type":"message","text":"hello"}}`)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/slack", strings.NewReader(string(body)))
	req = req.WithContext(ctx)
	req.Header.Set("X-Slack-Request-Timestamp", timestamp)
	req.Header.Set("X-Slack-Signature", slackWebhookSignature(webhookSecret, timestamp, body))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if got := countPostgresInboundMarkers(t, ctx, db, providerEventID, entityID, provider); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadPostgresInboundProviderEventID(t, ctx, db, runID, entityID, providerEventName, providerEventID)
	if got := countPostgresInboundProviderEvents(t, ctx, db, runID, entityID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	requireNoInboundBusEvent(t, ch, "paused Slack webhook before resume")
	if got := countPostgresAgentDeliveriesForEvent(t, ctx, db, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows while paused = %d, want 1", got)
	}
	if got := loadPostgresAgentDeliveryStatus(t, ctx, db, eventID, agentID); got != "pending" {
		t.Fatalf("agent delivery status while paused = %q, want pending", got)
	}
	if got := countPostgresPipelineReceiptsForEvent(t, ctx, db, eventID); got != 0 {
		t.Fatalf("pipeline receipts while paused = %d, want 0", got)
	}
	if got := countPostgresAgentReceiptsForEvent(t, ctx, db, eventID, agentID); got != 0 {
		t.Fatalf("agent receipts while paused = %d, want 0", got)
	}

	resumed, err := controller.Resume(ctx, runtimeingress.TransitionRequest{
		Reason:       "test_resume",
		ControlledBy: "test",
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.ReleasedCount != 1 {
		t.Fatalf("released count = %d, want 1", resumed.ReleasedCount)
	}
	got := requireInboundBusEvent(t, ch, "paused Slack webhook release after resume")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}
	if got.Type() != eventType {
		t.Fatalf("delivered event type = %s, want %s", got.Type(), eventType)
	}
	requireNoInboundBusEvent(t, ch, "paused Slack webhook releases exactly once")
	if got := countPostgresPipelineReceiptsForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("pipeline receipts after resume = %d, want 1", got)
	}
	if got := countPostgresInboundMarkers(t, ctx, db, providerEventID, entityID, provider); got != 1 {
		t.Fatalf("inbound marker rows after resume = %d, want 1", got)
	}
	if got := countPostgresInboundProviderEvents(t, ctx, db, runID, entityID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows after resume = %d, want 1", got)
	}
}

func TestInboundGateway_StripePausedRuntimePersistsAndReleasesSubscribedDispatch(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	const (
		runID             = "43000000-0000-0000-0000-000000000001"
		entityID          = "43000000-0000-0000-0000-000000000002"
		flowInstance      = "stripe-provider-trigger-instance"
		entitySlug        = "customer-a"
		provider          = "stripe"
		webhookSecret     = "stripe-secret"
		providerEventID   = "evt_123"
		agentID           = "stripe-webhook-subscriber"
		providerEventName = "inbound.stripe"
	)
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	pg := &store.PostgresStore{DB: db}
	seedPostgresInboundGatewayRuntime(t, ctx, db, pg, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeingress.NewController(pg, bus, runtimeingress.Options{})
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	bus.SetRuntimeIngressDispatchGate(controller)

	eventType := events.EventType(providerEventName)
	ch := bus.Subscribe(agentID, eventType)
	defer bus.Unsubscribe(agentID)

	if _, err := controller.Pause(ctx, runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	g := runtimepkg.NewInboundGateway(bus, nil, nil, pg)
	g.SetRuntimeIngress(controller)

	body := []byte(`{"id":"evt_123","type":"invoice.paid","data":{"object":{"id":"in_123"}}}`)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/stripe", strings.NewReader(string(body)))
	req = req.WithContext(ctx)
	req.Header.Set("Stripe-Signature", stripeWebhookSignature(webhookSecret, timestamp, body))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if got := countPostgresInboundMarkers(t, ctx, db, providerEventID, entityID, provider); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadPostgresInboundProviderEventID(t, ctx, db, runID, entityID, providerEventName, providerEventID)
	if got := countPostgresInboundProviderEvents(t, ctx, db, runID, entityID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	if got := loadPostgresInboundProviderEventPayloadField(t, ctx, db, eventID, "provider_event_type"); got != "invoice_paid" {
		t.Fatalf("provider_event_type = %q, want invoice_paid", got)
	}
	requireNoInboundBusEvent(t, ch, "paused Stripe webhook before resume")
	if got := countPostgresAgentDeliveriesForEvent(t, ctx, db, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows while paused = %d, want 1", got)
	}
	if got := loadPostgresAgentDeliveryStatus(t, ctx, db, eventID, agentID); got != "pending" {
		t.Fatalf("agent delivery status while paused = %q, want pending", got)
	}
	if got := countPostgresPipelineReceiptsForEvent(t, ctx, db, eventID); got != 0 {
		t.Fatalf("pipeline receipts while paused = %d, want 0", got)
	}
	if got := countPostgresAgentReceiptsForEvent(t, ctx, db, eventID, agentID); got != 0 {
		t.Fatalf("agent receipts while paused = %d, want 0", got)
	}

	resumed, err := controller.Resume(ctx, runtimeingress.TransitionRequest{
		Reason:       "test_resume",
		ControlledBy: "test",
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.ReleasedCount != 1 {
		t.Fatalf("released count = %d, want 1", resumed.ReleasedCount)
	}
	got := requireInboundBusEvent(t, ch, "paused Stripe webhook release after resume")
	if got.ID() != eventID {
		t.Fatalf("delivered event = %s, want %s", got.ID(), eventID)
	}
	if got.Type() != eventType {
		t.Fatalf("delivered event type = %s, want %s", got.Type(), eventType)
	}
	requireNoInboundBusEvent(t, ch, "paused Stripe webhook releases exactly once")
	if got := countPostgresPipelineReceiptsForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("pipeline receipts after resume = %d, want 1", got)
	}
	if got := countPostgresInboundMarkers(t, ctx, db, providerEventID, entityID, provider); got != 1 {
		t.Fatalf("inbound marker rows after resume = %d, want 1", got)
	}
	if got := countPostgresInboundProviderEvents(t, ctx, db, runID, entityID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows after resume = %d, want 1", got)
	}
}

func TestInboundGateway_StripeSQLitePersistsConfiguredManifestDelivery(t *testing.T) {
	const (
		runID             = "44000000-0000-0000-0000-000000000001"
		entityID          = "44000000-0000-0000-0000-000000000002"
		flowInstance      = "stripe-sqlite-provider-trigger-instance"
		entitySlug        = "customer-a"
		provider          = "stripe"
		webhookSecret     = "stripe-secret"
		providerEventID   = "evt_456"
		agentID           = "stripe-sqlite-webhook-subscriber"
		providerEventName = "inbound.stripe"
	)
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := runtimebus.NewEventBus(sqliteStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := bus.Subscribe(agentID, events.EventType(providerEventName))
	defer bus.Unsubscribe(agentID)

	g := runtimepkg.NewInboundGateway(bus, nil, nil, sqliteStore)

	body := []byte(`{"id":"evt_456","type":"customer.created","data":{"object":{"id":"cus_123"}}}`)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/stripe", strings.NewReader(string(body)))
	req = req.WithContext(ctx)
	req.Header.Set("Stripe-Signature", stripeWebhookSignature(webhookSecret, timestamp, body))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if got := countSQLiteInboundMarkers(t, ctx, sqliteStore, providerEventID, entityID, provider); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadSQLiteInboundProviderEventID(t, ctx, sqliteStore, runID, entityID, providerEventName, providerEventID)
	if got := countSQLiteInboundProviderEvents(t, ctx, sqliteStore, runID, entityID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	if got := loadSQLiteInboundProviderEventPayloadField(t, ctx, sqliteStore, eventID, "provider_event_type"); got != "customer_created" {
		t.Fatalf("provider_event_type = %q, want customer_created", got)
	}
	if got := countSQLiteAgentDeliveriesForEvent(t, ctx, sqliteStore, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows = %d, want 1", got)
	}
	select {
	case got := <-ch:
		if got.ID() != eventID || got.Type() != events.EventType(providerEventName) {
			t.Fatalf("delivered event = %s/%s, want %s/%s", got.ID(), got.Type(), eventID, providerEventName)
		}
	default:
		// The Stripe manifest uses durable_before_dispatch; this proof is about
		// durable persisted evidence, not immediate post-ack handler scheduling.
	}
}

func TestInboundGateway_TwilioPostgresPersistsConfiguredManifestDelivery(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	const (
		runID             = "45000000-0000-0000-0000-000000000001"
		entityID          = "45000000-0000-0000-0000-000000000002"
		flowInstance      = "twilio-provider-trigger-instance"
		entitySlug        = "customer-a"
		provider          = "twilio"
		webhookSecret     = "twilio-secret"
		providerEventID   = "SM1234567890abcdef"
		agentID           = "twilio-webhook-subscriber"
		providerEventName = "inbound.twilio"
	)
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	pg := &store.PostgresStore{DB: db}
	seedPostgresInboundGatewayRuntime(t, ctx, db, pg, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := bus.Subscribe(agentID, events.EventType(providerEventName))
	defer bus.Unsubscribe(agentID)

	g := runtimepkg.NewInboundGateway(bus, nil, nil, pg)

	requestURL := "https://example.com/webhooks/customer-a/twilio?tenant=alpha"
	form := url.Values{
		"Body":       {"hello from twilio"},
		"From":       {"+15551234567"},
		"MessageSid": {providerEventID},
		"To":         {"+15557654321"},
	}
	req := newSignedTwilioRequest(requestURL, webhookSecret, form)
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if got := countPostgresInboundMarkers(t, ctx, db, providerEventID, entityID, provider); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadPostgresInboundProviderEventID(t, ctx, db, runID, entityID, providerEventName, providerEventID)
	if got := countPostgresInboundProviderEvents(t, ctx, db, runID, entityID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	if got := loadPostgresInboundProviderEventPayloadField(t, ctx, db, eventID, "provider_event_type"); got != "message_received" {
		t.Fatalf("provider_event_type = %q, want message_received", got)
	}
	if got := countPostgresAgentDeliveriesForEvent(t, ctx, db, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows = %d, want 1", got)
	}
	select {
	case got := <-ch:
		if got.ID() != eventID || got.Type() != events.EventType(providerEventName) {
			t.Fatalf("delivered event = %s/%s, want %s/%s", got.ID(), got.Type(), eventID, providerEventName)
		}
	default:
		// Twilio uses durable_before_dispatch; this proof is about persisted
		// evidence and supported store admission, not handler completion.
	}
}

func TestInboundGateway_TwilioSQLitePersistsConfiguredManifestDelivery(t *testing.T) {
	const (
		runID             = "46000000-0000-0000-0000-000000000001"
		entityID          = "46000000-0000-0000-0000-000000000002"
		flowInstance      = "twilio-sqlite-provider-trigger-instance"
		entitySlug        = "customer-a"
		provider          = "twilio"
		webhookSecret     = "twilio-secret"
		providerEventID   = "SMabcdef1234567890"
		agentID           = "twilio-sqlite-webhook-subscriber"
		providerEventName = "inbound.twilio"
	)
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := runtimebus.NewEventBus(sqliteStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := bus.Subscribe(agentID, events.EventType(providerEventName))
	defer bus.Unsubscribe(agentID)

	g := runtimepkg.NewInboundGateway(bus, nil, nil, sqliteStore)

	requestURL := "https://example.com/webhooks/customer-a/twilio?tenant=alpha"
	form := url.Values{
		"Body":       {"hello from sqlite"},
		"From":       {"+15551234567"},
		"MessageSid": {providerEventID},
		"To":         {"+15557654321"},
	}
	req := newSignedTwilioRequest(requestURL, webhookSecret, form)
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if got := countSQLiteInboundMarkers(t, ctx, sqliteStore, providerEventID, entityID, provider); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadSQLiteInboundProviderEventID(t, ctx, sqliteStore, runID, entityID, providerEventName, providerEventID)
	if got := countSQLiteInboundProviderEvents(t, ctx, sqliteStore, runID, entityID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	if got := loadSQLiteInboundProviderEventPayloadField(t, ctx, sqliteStore, eventID, "provider_event_type"); got != "message_received" {
		t.Fatalf("provider_event_type = %q, want message_received", got)
	}
	if got := countSQLiteAgentDeliveriesForEvent(t, ctx, sqliteStore, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows = %d, want 1", got)
	}
	select {
	case got := <-ch:
		if got.ID() != eventID || got.Type() != events.EventType(providerEventName) {
			t.Fatalf("delivered event = %s/%s, want %s/%s", got.ID(), got.Type(), eventID, providerEventName)
		}
	default:
		// Twilio uses durable_before_dispatch; this proof is about persisted
		// evidence and supported store admission, not handler completion.
	}
}

func seedPostgresInboundGatewayRuntime(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	pg *store.PostgresStore,
	runID string,
	entityID string,
	flowInstance string,
	entitySlug string,
	provider string,
	webhookSecret string,
	agentID string,
) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	configBytes, err := json.Marshal(map[string]any{
		"secrets": map[string]any{
			"webhook_signing": map[string]string{
				provider: webhookSecret,
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal flow config: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ($1, 'test', 'static', $2::jsonb, 'active', now())
		ON CONFLICT (instance_id) DO UPDATE SET config = EXCLUDED.config, status = EXCLUDED.status
	`, flowInstance, string(configBytes)); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3, 'default', $4, 'Customer A', 'active',
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
		)
		ON CONFLICT (run_id, entity_id) DO NOTHING
	`, runID, entityID, flowInstance, entitySlug); err != nil {
		t.Fatalf("seed entity state: %v", err)
	}
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:     agentID,
			Role:   "observer",
			Mode:   "global",
			Type:   "stub",
			Model:  "regular",
			Config: []byte(`{}`),
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent(%s): %v", agentID, err)
	}
}

func seedSQLiteInboundGatewayRuntime(
	t *testing.T,
	ctx context.Context,
	sqliteStore *store.SQLiteRuntimeStore,
	runID string,
	entityID string,
	flowInstance string,
	entitySlug string,
	provider string,
	webhookSecret string,
	agentID string,
) {
	t.Helper()
	now := time.Now().UTC()
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES (?, 'running', ?)
	`, runID, now); err != nil {
		t.Fatalf("seed sqlite run: %v", err)
	}
	configBytes, err := json.Marshal(map[string]any{
		"secrets": map[string]any{
			"webhook_signing": map[string]string{
				provider: webhookSecret,
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal sqlite flow config: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES (?, 'test', 'static', ?, 'active', ?)
	`, flowInstance, string(configBytes), now); err != nil {
		t.Fatalf("seed sqlite flow instance: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (?, ?, ?, 'default', ?, 'Customer A', 'active',
			'{}', '{}', '{}', 1, ?, ?, ?)
	`, runID, entityID, flowInstance, entitySlug, now, now, now); err != nil {
		t.Fatalf("seed sqlite entity state: %v", err)
	}
	if err := sqliteStore.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            agentID,
			Role:          "observer",
			Mode:          "global",
			Type:          "stub",
			Model:         "regular",
			Config:        []byte(`{}`),
			Subscriptions: []string{"inbound.stripe"},
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: now,
	}); err != nil {
		t.Fatalf("UpsertAgent(%s): %v", agentID, err)
	}
}

func loadPostgresInboundProviderEventID(t *testing.T, ctx context.Context, db *sql.DB, runID string, entityID string, eventName string, providerEventID string) string {
	t.Helper()
	var eventID string
	if err := db.QueryRowContext(ctx, `
		SELECT event_id::text
		FROM events
		WHERE run_id = $1::uuid
		  AND entity_id = $2::uuid
		  AND event_name = $3
		  AND payload->>'provider_event_id' = $4
		ORDER BY created_at DESC
		LIMIT 1
	`, runID, entityID, eventName, providerEventID).Scan(&eventID); err != nil {
		t.Fatalf("load inbound provider event id: %v", err)
	}
	return eventID
}

func loadPostgresInboundProviderEventPayloadField(t *testing.T, ctx context.Context, db *sql.DB, eventID string, field string) string {
	t.Helper()
	var value string
	if err := db.QueryRowContext(ctx, `
		SELECT payload->>$2
		FROM events
		WHERE event_id = $1::uuid
	`, eventID, field).Scan(&value); err != nil {
		t.Fatalf("load postgres inbound provider payload field %s: %v", field, err)
	}
	return value
}

func countPostgresInboundProviderEvents(t *testing.T, ctx context.Context, db *sql.DB, runID string, entityID string, eventName string, providerEventID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND entity_id = $2::uuid
		  AND event_name = $3
		  AND payload->>'provider_event_id' = $4
	`, runID, entityID, eventName, providerEventID).Scan(&count); err != nil {
		t.Fatalf("count inbound provider events: %v", err)
	}
	return count
}

func loadSQLiteInboundProviderEventID(t *testing.T, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, runID string, entityID string, eventName string, providerEventID string) string {
	t.Helper()
	var eventID string
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT event_id
		FROM events
		WHERE run_id = ?
		  AND entity_id = ?
		  AND event_name = ?
		  AND json_extract(payload, '$.provider_event_id') = ?
		ORDER BY created_at DESC
		LIMIT 1
	`, runID, entityID, eventName, providerEventID).Scan(&eventID); err != nil {
		t.Fatalf("load sqlite inbound provider event id: %v", err)
	}
	return eventID
}

func countSQLiteInboundProviderEvents(t *testing.T, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, runID string, entityID string, eventName string, providerEventID string) int {
	t.Helper()
	var count int
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = ?
		  AND entity_id = ?
		  AND event_name = ?
		  AND json_extract(payload, '$.provider_event_id') = ?
	`, runID, entityID, eventName, providerEventID).Scan(&count); err != nil {
		t.Fatalf("count sqlite inbound provider events: %v", err)
	}
	return count
}

func loadSQLiteInboundProviderEventPayloadField(t *testing.T, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, eventID string, field string) string {
	t.Helper()
	var value string
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT json_extract(payload, ?)
		FROM events
		WHERE event_id = ?
	`, "$."+field, eventID).Scan(&value); err != nil {
		t.Fatalf("load sqlite inbound provider payload field %s: %v", field, err)
	}
	return value
}

func countSQLiteInboundMarkers(t *testing.T, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, providerEventID string, entityID string, provider string) int {
	t.Helper()
	var count int
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE event_name = 'platform.inbound_recorded'
		  AND entity_id = ?
		  AND json_extract(payload, '$.provider_event_id') = ?
		  AND json_extract(payload, '$.provider') = ?
	`, entityID, providerEventID, provider).Scan(&count); err != nil {
		t.Fatalf("count sqlite inbound marker events: %v", err)
	}
	return count
}

func countSQLiteAgentDeliveriesForEvent(t *testing.T, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, eventID string, agentID string) int {
	t.Helper()
	var count int
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = ?
		  AND subscriber_type = 'agent'
		  AND subscriber_id = ?
	`, eventID, agentID).Scan(&count); err != nil {
		t.Fatalf("count sqlite agent deliveries for %s: %v", eventID, err)
	}
	return count
}

func countPostgresInboundMarkers(t *testing.T, ctx context.Context, db *sql.DB, providerEventID string, entityID string, provider string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE event_name = 'platform.inbound_recorded'
		  AND entity_id = $1::uuid
		  AND payload->>'provider_event_id' = $2
		  AND payload->>'provider' = $3
	`, entityID, providerEventID, provider).Scan(&count); err != nil {
		t.Fatalf("count inbound marker events: %v", err)
	}
	return count
}

func countPostgresAgentDeliveriesForEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID string, agentID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, eventID, agentID).Scan(&count); err != nil {
		t.Fatalf("count agent deliveries for %s: %v", eventID, err)
	}
	return count
}

func loadPostgresAgentDeliveryStatus(t *testing.T, ctx context.Context, db *sql.DB, eventID string, agentID string) string {
	t.Helper()
	var status string
	if err := db.QueryRowContext(ctx, `
		SELECT status
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, eventID, agentID).Scan(&status); err != nil {
		t.Fatalf("load agent delivery status for %s: %v", eventID, err)
	}
	return status
}

func countPostgresPipelineReceiptsForEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&count); err != nil {
		t.Fatalf("count pipeline receipts for %s: %v", eventID, err)
	}
	return count
}

func countPostgresAgentReceiptsForEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID string, agentID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, eventID, agentID).Scan(&count); err != nil {
		t.Fatalf("count agent receipts for %s: %v", eventID, err)
	}
	return count
}

func requireInboundBusEvent(t testing.TB, ch <-chan events.Event, context string) events.Event {
	t.Helper()
	select {
	case evt := <-ch:
		return evt
	default:
		t.Fatalf("%s: expected queued bus event", context)
		return events.EmptyEvent()
	}
}

func requireNoInboundBusEvent(t testing.TB, ch <-chan events.Event, context string) {
	t.Helper()
	select {
	case evt := <-ch:
		t.Fatalf("%s: unexpected bus event: %#v", context, evt)
	default:
	}
}

func githubWebhookSignature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func slackWebhookSignature(secret string, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("v0:" + timestamp + ":" + string(body)))
	return "v0=" + hex.EncodeToString(mac.Sum(nil))
}

func stripeWebhookSignature(secret string, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp + "." + string(body)))
	return "t=" + timestamp + ",v1=" + hex.EncodeToString(mac.Sum(nil))
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
