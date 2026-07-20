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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	eventtestsql "github.com/division-sh/swarm/internal/store/testsql"
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
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	seedPostgresInboundGatewayRuntime(t, ctx, db, pg, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{}, providerEventName)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeingress.NewController(pg, bus, runtimeingress.Options{})
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	bus.SetRuntimeIngressDispatchGate(controller)

	eventType := events.EventType(providerEventName)
	ch := bus.Subscribe(agentID, eventType)
	defer bus.Unsubscribe(agentID)

	if _, err := controller.Pause(context.Background(), runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	g := newTestInboundGateway(t, bus, nil, nil, pg)
	g.SetRuntimeIngress(controller)

	body := []byte(`{"zen":"Keep it logically awesome."}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/github", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", githubWebhookSignature(webhookSecret, body))
	req.Header.Set("X-GitHub-Delivery", providerEventID)
	req.Header.Set("X-GitHub-Event", "push")
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, pg, rec, req, runID, entityID, provider, webhookSecret)

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

	resumed, err := controller.Resume(context.Background(), runtimeingress.TransitionRequest{
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
	unsubscribeAndWaitForInboundBusQuiescence(t, bus, agentID)
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
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	seedPostgresInboundGatewayRuntime(t, ctx, db, pg, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{}, providerEventName)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeingress.NewController(pg, bus, runtimeingress.Options{})
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	bus.SetRuntimeIngressDispatchGate(controller)

	eventType := events.EventType(providerEventName)
	ch := bus.Subscribe(agentID, eventType)
	defer bus.Unsubscribe(agentID)

	if _, err := controller.Pause(context.Background(), runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	g := newTestInboundGateway(t, bus, nil, nil, pg)
	g.SetRuntimeIngress(controller)

	body := []byte(`{"type":"event_callback","event_id":"Ev123ABC456","event":{"type":"message","text":"hello"}}`)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/slack", strings.NewReader(string(body)))
	req.Header.Set("X-Slack-Request-Timestamp", timestamp)
	req.Header.Set("X-Slack-Signature", slackWebhookSignature(webhookSecret, timestamp, body))
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, pg, rec, req, runID, entityID, provider, webhookSecret)

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

	resumed, err := controller.Resume(context.Background(), runtimeingress.TransitionRequest{
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
	unsubscribeAndWaitForInboundBusQuiescence(t, bus, agentID)
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
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	seedPostgresInboundGatewayRuntime(t, ctx, db, pg, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{}, providerEventName)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeingress.NewController(pg, bus, runtimeingress.Options{})
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	bus.SetRuntimeIngressDispatchGate(controller)

	eventType := events.EventType(providerEventName)
	ch := bus.Subscribe(agentID, eventType)
	defer bus.Unsubscribe(agentID)

	if _, err := controller.Pause(context.Background(), runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	g := newTestInboundGateway(t, bus, nil, nil, pg)
	g.SetRuntimeIngress(controller)

	body := []byte(`{"id":"evt_123","type":"invoice.paid","data":{"object":{"id":"in_123"}}}`)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/stripe", strings.NewReader(string(body)))
	req.Header.Set("Stripe-Signature", stripeWebhookSignature(webhookSecret, timestamp, body))
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, pg, rec, req, runID, entityID, provider, webhookSecret)

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

	resumed, err := controller.Resume(context.Background(), runtimeingress.TransitionRequest{
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
	unsubscribeAndWaitForInboundBusQuiescence(t, bus, agentID)
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
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newScopedTestEventBus(t, sqliteStore, runtimebus.EventBusOptions{}, providerEventName)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := bus.Subscribe(agentID, events.EventType(providerEventName))
	defer bus.Unsubscribe(agentID)

	g := newTestInboundGateway(t, bus, nil, nil, sqliteStore)

	body := []byte(`{"id":"evt_456","type":"customer.created","data":{"object":{"id":"cus_123"}}}`)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/stripe", strings.NewReader(string(body)))
	req.Header.Set("Stripe-Signature", stripeWebhookSignature(webhookSecret, timestamp, body))
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, sqliteStore, rec, req, runID, entityID, provider, webhookSecret)

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
		_ = got.Complete()
	case <-time.After(5 * time.Second):
		t.Fatal("Stripe SQLite post-commit dispatch did not arrive")
	}
	unsubscribeAndWaitForInboundBusQuiescence(t, bus, agentID)
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
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	seedPostgresInboundGatewayRuntime(t, ctx, db, pg, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{}, providerEventName)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := bus.Subscribe(agentID, events.EventType(providerEventName))
	defer bus.Unsubscribe(agentID)

	g := newTestInboundGateway(t, bus, nil, nil, pg)

	requestURL := "https://example.com/webhooks/customer-a/twilio?tenant=alpha"
	form := url.Values{
		"Body":       {"hello from twilio"},
		"From":       {"+15551234567"},
		"MessageSid": {providerEventID},
		"To":         {"+15557654321"},
	}
	req := newSignedTwilioRequest(requestURL, webhookSecret, form)
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, pg, rec, req, runID, entityID, provider, webhookSecret)

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
		_ = got.Complete()
	case <-time.After(5 * time.Second):
		t.Fatal("Twilio PostgreSQL post-commit dispatch did not arrive")
	}
	unsubscribeAndWaitForInboundBusQuiescence(t, bus, agentID)
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
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newScopedTestEventBus(t, sqliteStore, runtimebus.EventBusOptions{}, providerEventName)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := bus.Subscribe(agentID, events.EventType(providerEventName))
	defer bus.Unsubscribe(agentID)

	g := newTestInboundGateway(t, bus, nil, nil, sqliteStore)

	requestURL := "https://example.com/webhooks/customer-a/twilio?tenant=alpha"
	form := url.Values{
		"Body":       {"hello from sqlite"},
		"From":       {"+15551234567"},
		"MessageSid": {providerEventID},
		"To":         {"+15557654321"},
	}
	req := newSignedTwilioRequest(requestURL, webhookSecret, form)
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, sqliteStore, rec, req, runID, entityID, provider, webhookSecret)

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
		_ = got.Complete()
	case <-time.After(5 * time.Second):
		t.Fatal("Twilio SQLite post-commit dispatch did not arrive")
	}
	unsubscribeAndWaitForInboundBusQuiescence(t, bus, agentID)
}

func TestInboundGateway_ShopifyPostgresPersistsConfiguredManifestDelivery(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	const (
		runID             = "47000000-0000-0000-0000-000000000001"
		entityID          = "47000000-0000-0000-0000-000000000002"
		flowInstance      = "shopify-provider-trigger-instance"
		entitySlug        = "customer-a"
		provider          = "shopify"
		webhookSecret     = "shopify-secret"
		providerEventID   = "webhook-123"
		agentID           = "shopify-webhook-subscriber"
		providerEventName = "inbound.shopify"
	)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	seedPostgresInboundGatewayRuntime(t, ctx, db, pg, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{}, providerEventName)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := bus.Subscribe(agentID, events.EventType(providerEventName))
	defer bus.Unsubscribe(agentID)

	g := newTestInboundGateway(t, bus, nil, nil, pg)

	body := []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`)
	req := newSignedShopifyRequest("/webhooks/customer-a/shopify", webhookSecret, body)
	req.Header.Set("X-Shopify-Webhook-Id", providerEventID)
	req.Header.Set("X-Shopify-Topic", "orders/create")
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, pg, rec, req, runID, entityID, provider, webhookSecret)

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
	if got := loadPostgresInboundProviderEventPayloadField(t, ctx, db, eventID, "provider_event_type"); got != "orders_create" {
		t.Fatalf("provider_event_type = %q, want orders_create", got)
	}
	if got := countPostgresAgentDeliveriesForEvent(t, ctx, db, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows = %d, want 1", got)
	}
	select {
	case got := <-ch:
		if got.ID() != eventID || got.Type() != events.EventType(providerEventName) {
			t.Fatalf("delivered event = %s/%s, want %s/%s", got.ID(), got.Type(), eventID, providerEventName)
		}
		_ = got.Complete()
	case <-time.After(5 * time.Second):
		t.Fatal("Shopify PostgreSQL post-commit dispatch did not arrive")
	}
	unsubscribeAndWaitForInboundBusQuiescence(t, bus, agentID)
}

func TestInboundGateway_ShopifySQLitePersistsConfiguredManifestDelivery(t *testing.T) {
	const (
		runID             = "48000000-0000-0000-0000-000000000001"
		entityID          = "48000000-0000-0000-0000-000000000002"
		flowInstance      = "shopify-sqlite-provider-trigger-instance"
		entitySlug        = "customer-a"
		provider          = "shopify"
		webhookSecret     = "shopify-secret"
		providerEventID   = "webhook-456"
		agentID           = "shopify-sqlite-webhook-subscriber"
		providerEventName = "inbound.shopify"
	)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newScopedTestEventBus(t, sqliteStore, runtimebus.EventBusOptions{}, providerEventName)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := bus.Subscribe(agentID, events.EventType(providerEventName))
	defer bus.Unsubscribe(agentID)

	g := newTestInboundGateway(t, bus, nil, nil, sqliteStore)

	body := []byte(`{"id":456,"line_items":[{"sku":"xyz"}]}`)
	req := newSignedShopifyRequest("/webhooks/customer-a/shopify", webhookSecret, body)
	req.Header.Set("X-Shopify-Webhook-Id", providerEventID)
	req.Header.Set("X-Shopify-Topic", "orders/updated")
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, sqliteStore, rec, req, runID, entityID, provider, webhookSecret)

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
	if got := loadSQLiteInboundProviderEventPayloadField(t, ctx, sqliteStore, eventID, "provider_event_type"); got != "orders_updated" {
		t.Fatalf("provider_event_type = %q, want orders_updated", got)
	}
	if got := countSQLiteAgentDeliveriesForEvent(t, ctx, sqliteStore, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows = %d, want 1", got)
	}
	select {
	case got := <-ch:
		if got.ID() != eventID || got.Type() != events.EventType(providerEventName) {
			t.Fatalf("delivered event = %s/%s, want %s/%s", got.ID(), got.Type(), eventID, providerEventName)
		}
		_ = got.Complete()
	case <-time.After(5 * time.Second):
		t.Fatal("Shopify SQLite post-commit dispatch did not arrive")
	}
	unsubscribeAndWaitForInboundBusQuiescence(t, bus, agentID)
}

func TestInboundGateway_TelegramPostgresPersistsConfiguredManifestDelivery(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	const (
		runID             = "4d000000-0000-0000-0000-000000000001"
		entityID          = "4d000000-0000-0000-0000-000000000002"
		flowInstance      = "telegram-provider-trigger-instance"
		entitySlug        = "customer-a"
		provider          = "telegram"
		webhookSecret     = "telegram-secret"
		providerEventID   = "123456789"
		agentID           = "telegram-webhook-subscriber"
		providerEventName = "inbound.telegram"
	)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	seedPostgresInboundGatewayRuntime(t, ctx, db, pg, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{}, providerEventName, "inbound.telegram.text_message")
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := bus.Subscribe(agentID, events.EventType(providerEventName))
	defer bus.Unsubscribe(agentID)

	g := newTestInboundGateway(t, bus, nil, nil, pg)

	body := []byte(`{"update_id":123456789,"undeclared_root":"root-must-not-enter-author-story","message":{"message_id":7,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"hello","undeclared_private":"private-must-not-enter-author-story"}}`)
	req := newSignedTelegramRequest("/webhooks/customer-a/telegram", webhookSecret, body)
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, pg, rec, req, runID, entityID, provider, webhookSecret)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), webhookSecret) {
		t.Fatal("Telegram secret token leaked into response")
	}
	if got := countPostgresInboundMarkers(t, ctx, db, providerEventID, entityID, provider); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadPostgresInboundProviderEventID(t, ctx, db, runID, entityID, providerEventName, providerEventID)
	if got := countPostgresInboundProviderEvents(t, ctx, db, runID, entityID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	if got := loadPostgresInboundProviderEventPayloadField(t, ctx, db, eventID, "provider_event_type"); got != "update" {
		t.Fatalf("provider_event_type = %q, want update", got)
	}
	if got := countPostgresAgentDeliveriesForEvent(t, ctx, db, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows = %d, want 1", got)
	}
	requireInboundGatewayAuthorProjection(t, ctx, pg, runID, entityID, "hello", "chat", "42")
	record, found, err := pg.LoadInboundPublicationByIdentity(ctx, provider, entityID, providerEventID)
	if err != nil || !found {
		t.Fatalf("LoadInboundPublicationByIdentity = found:%v err:%v", found, err)
	}
	requireInboundPostCommitSnapshot(t, requireInboundBusEvent(t, ch, "Telegram PostgreSQL post-commit dispatch"), inboundPublicationEvent(t, record, eventID))
	waitForInboundBusQuiescence(t, bus)

	eventtestsql.CorruptEventStore(t, ctx, db, runtimeauthoractivity.DialectPostgres, eventtestsql.EventCorruptionClaim{
		Invariant: "store.event_record.duplicate_integrity",
		Reason:    "prove inbound duplicate comparison rejects a schema-valid durable payload conflict",
	}, "", `UPDATE events SET payload = '{"corrupt":true}'::jsonb WHERE event_id = $1::uuid`, eventID)
	duplicate := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, pg, duplicate, newSignedTelegramRequest("/webhooks/customer-a/telegram", webhookSecret, body), runID, entityID, provider, webhookSecret)
	if duplicate.Code != http.StatusServiceUnavailable {
		t.Fatalf("corrupt duplicate status = %d, want 503 body=%s", duplicate.Code, duplicate.Body.String())
	}
	requireNoInboundBusEvent(t, ch, "corrupt Telegram PostgreSQL duplicate")
	eventtestsql.RequireEventRowCount(t, ctx, db, runtimeauthoractivity.DialectPostgres, eventID, 1)
}

func TestInboundGateway_TelegramSQLitePersistsConfiguredManifestDelivery(t *testing.T) {
	const (
		runID             = "4e000000-0000-0000-0000-000000000001"
		entityID          = "4e000000-0000-0000-0000-000000000002"
		flowInstance      = "telegram-sqlite-provider-trigger-instance"
		entitySlug        = "customer-a"
		provider          = "telegram"
		webhookSecret     = "telegram-secret"
		providerEventID   = "987654321"
		agentID           = "telegram-sqlite-webhook-subscriber"
		providerEventName = "inbound.telegram"
	)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := newScopedTestEventBus(t, sqliteStore, runtimebus.EventBusOptions{}, providerEventName, "inbound.telegram.text_message")
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := bus.Subscribe(agentID, events.EventType(providerEventName))
	defer bus.Unsubscribe(agentID)

	g := newTestInboundGateway(t, bus, nil, nil, sqliteStore)

	body := []byte(`{"update_id":987654321,"undeclared_root":"root-must-not-enter-author-story","message":{"message_id":8,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"hello sqlite","undeclared_private":"private-must-not-enter-author-story"}}`)
	req := newSignedTelegramRequest("/webhooks/customer-a/telegram", webhookSecret, body)
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, sqliteStore, rec, req, runID, entityID, provider, webhookSecret)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), webhookSecret) {
		t.Fatal("Telegram secret token leaked into response")
	}
	if got := countSQLiteInboundMarkers(t, ctx, sqliteStore, providerEventID, entityID, provider); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	eventID := loadSQLiteInboundProviderEventID(t, ctx, sqliteStore, runID, entityID, providerEventName, providerEventID)
	if got := countSQLiteInboundProviderEvents(t, ctx, sqliteStore, runID, entityID, providerEventName, providerEventID); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	if got := loadSQLiteInboundProviderEventPayloadField(t, ctx, sqliteStore, eventID, "provider_event_type"); got != "update" {
		t.Fatalf("provider_event_type = %q, want update", got)
	}
	if got := countSQLiteAgentDeliveriesForEvent(t, ctx, sqliteStore, eventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows = %d, want 1", got)
	}
	requireInboundGatewayAuthorProjection(t, ctx, sqliteStore, runID, entityID, "hello sqlite", "chat", "42")
	record, found, err := sqliteStore.LoadInboundPublicationByIdentity(ctx, provider, entityID, providerEventID)
	if err != nil || !found {
		t.Fatalf("LoadInboundPublicationByIdentity = found:%v err:%v", found, err)
	}
	requireInboundPostCommitSnapshot(t, requireInboundBusEvent(t, ch, "Telegram SQLite post-commit dispatch"), inboundPublicationEvent(t, record, eventID))
	waitForInboundBusQuiescence(t, bus)

	eventtestsql.CorruptEventStore(t, ctx, sqliteStore.DB, runtimeauthoractivity.DialectSQLite, eventtestsql.EventCorruptionClaim{
		Invariant: "store.event_record.duplicate_integrity",
		Reason:    "prove inbound duplicate comparison rejects a schema-valid durable payload conflict",
	}, `UPDATE events SET payload = '{"corrupt":true}' WHERE event_id = ?`, "", eventID)
	duplicate := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, g, bus, sqliteStore, duplicate, newSignedTelegramRequest("/webhooks/customer-a/telegram", webhookSecret, body), runID, entityID, provider, webhookSecret)
	if duplicate.Code != http.StatusServiceUnavailable {
		t.Fatalf("corrupt duplicate status = %d, want 503 body=%s", duplicate.Code, duplicate.Body.String())
	}
	requireNoInboundBusEvent(t, ch, "corrupt Telegram SQLite duplicate")
	eventtestsql.RequireEventRowCount(t, ctx, sqliteStore.DB, runtimeauthoractivity.DialectSQLite, eventID, 1)
}

func inboundPublicationEvent(t testing.TB, record runtimeinbound.Record, eventID string) events.Event {
	t.Helper()
	for _, child := range record.Events {
		if child.EventID == eventID {
			return child.Event
		}
	}
	t.Fatalf("inbound publication does not contain event %s: %#v", eventID, record.Events)
	return events.Event{}
}

func requireInboundPostCommitSnapshot(t testing.TB, got, want events.Event) {
	t.Helper()
	var gotPayload, wantPayload any
	if err := json.Unmarshal(got.Payload(), &gotPayload); err != nil {
		t.Fatalf("decode dispatched inbound payload: %v", err)
	}
	if err := json.Unmarshal(want.Payload(), &wantPayload); err != nil {
		t.Fatalf("decode persisted inbound payload: %v", err)
	}
	if got.ID() != want.ID() || got.Type() != want.Type() || !got.Producer().Equal(want.Producer()) ||
		got.TaskID() != want.TaskID() || got.ChainDepth() != want.ChainDepth() || got.RunID() != want.RunID() ||
		got.ParentEventID() != want.ParentEventID() || got.ExecutionMode() != want.ExecutionMode() ||
		!got.CreatedAt().Truncate(time.Microsecond).Equal(want.CreatedAt().Truncate(time.Microsecond)) ||
		!reflect.DeepEqual(gotPayload, wantPayload) || !reflect.DeepEqual(got.Envelope(), want.Envelope()) {
		t.Fatalf("post-commit inbound snapshot changed\n got: id=%s type=%s producer=%s/%s task=%s depth=%d run=%s parent=%s mode=%s at=%s payload=%s envelope=%#v\nwant: id=%s type=%s producer=%s/%s task=%s depth=%d run=%s parent=%s mode=%s at=%s payload=%s envelope=%#v",
			got.ID(), got.Type(), got.ProducerType(), got.SourceAgent(), got.TaskID(), got.ChainDepth(), got.RunID(), got.ParentEventID(), got.ExecutionMode(), got.CreatedAt(), got.Payload(), got.Envelope(),
			want.ID(), want.Type(), want.ProducerType(), want.SourceAgent(), want.TaskID(), want.ChainDepth(), want.RunID(), want.ParentEventID(), want.ExecutionMode(), want.CreatedAt(), want.Payload(), want.Envelope())
	}
}

type inboundAuthorActivityReader interface {
	ListAuthorActivity(context.Context, runtimeauthoractivity.ListOptions) (runtimeauthoractivity.ListResult, error)
}

func requireInboundGatewayAuthorProjection(
	t *testing.T,
	ctx context.Context,
	reader inboundAuthorActivityReader,
	runID string,
	entityID string,
	wantSummary string,
	wantSubjectType string,
	wantSubjectID string,
) {
	t.Helper()
	result, err := reader.ListAuthorActivity(ctx, runtimeauthoractivity.ListOptions{RunID: runID, Limit: 100})
	if err != nil {
		t.Fatalf("ListAuthorActivity: %v", err)
	}
	var matches []runtimeauthoractivity.Occurrence
	for _, occurrence := range result.Occurrences {
		if occurrence.Kind == runtimeauthoractivity.KindInboundReceived && occurrence.Transition == "received" {
			matches = append(matches, occurrence)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("inbound author occurrences = %d, want one: %#v", len(matches), result.Occurrences)
	}
	occurrence := matches[0]
	if occurrence.EntityID != entityID {
		t.Fatalf("inbound author entity_id = %q, want %q", occurrence.EntityID, entityID)
	}
	if occurrence.AuthorSafeSummary != wantSummary {
		t.Fatalf("inbound author summary = %q, want declared %q", occurrence.AuthorSafeSummary, wantSummary)
	}
	if occurrence.Projection.AuthorSubjectType != wantSubjectType || occurrence.Projection.AuthorSubjectID != wantSubjectID {
		t.Fatalf(
			"inbound author subject = %q/%q, want declared %q/%q",
			occurrence.Projection.AuthorSubjectType,
			occurrence.Projection.AuthorSubjectID,
			wantSubjectType,
			wantSubjectID,
		)
	}
	encoded, err := json.Marshal(occurrence)
	if err != nil {
		t.Fatalf("marshal inbound author occurrence: %v", err)
	}
	for _, forbidden := range []string{"root-must-not-enter-author-story", "private-must-not-enter-author-story", "undeclared_root", "undeclared_private"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("inbound author occurrence leaked undeclared payload marker %q: %s", forbidden, encoded)
		}
	}
}

func TestInboundGateway_TypeformAndIntercomPostgresPersistsConfiguredManifestDelivery(t *testing.T) {
	for _, tc := range []struct {
		name              string
		runID             string
		entityID          string
		flowInstance      string
		provider          string
		webhookSecret     string
		providerEventID   string
		providerEventType string
		agentID           string
		providerEventName string
		body              []byte
		newRequest        func(path string, secret string, body []byte) *http.Request
	}{
		{
			name:              "typeform",
			runID:             "49000000-0000-0000-0000-000000000001",
			entityID:          "49000000-0000-0000-0000-000000000002",
			flowInstance:      "typeform-provider-trigger-instance",
			provider:          "typeform",
			webhookSecret:     "typeform-secret",
			providerEventID:   "tf-evt-pg-123",
			providerEventType: "form_response",
			agentID:           "typeform-webhook-subscriber",
			providerEventName: "inbound.typeform",
			body:              []byte(`{"event_id":"tf-evt-pg-123","event_type":"form_response","form_response":{"token":"abc"}}`),
			newRequest:        newSignedTypeformRequest,
		},
		{
			name:              "intercom",
			runID:             "4a000000-0000-0000-0000-000000000001",
			entityID:          "4a000000-0000-0000-0000-000000000002",
			flowInstance:      "intercom-provider-trigger-instance",
			provider:          "intercom",
			webhookSecret:     "intercom-secret",
			providerEventID:   "notif_pg_123",
			providerEventType: "conversation_user_created",
			agentID:           "intercom-webhook-subscriber",
			providerEventName: "inbound.intercom",
			body:              []byte(`{"id":"notif_pg_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
			newRequest:        newSignedIntercomRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)

			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), tc.runID)
			pg := storetest.AdmitPostgresRuntimeStore(t, db)
			seedPostgresInboundGatewayRuntime(t, ctx, db, pg, tc.runID, tc.entityID, tc.flowInstance, "customer-a", tc.provider, tc.webhookSecret, tc.agentID)

			bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{}, tc.providerEventName)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			ch := bus.Subscribe(tc.agentID, events.EventType(tc.providerEventName))
			defer bus.Unsubscribe(tc.agentID)

			g := newTestInboundGateway(t, bus, nil, nil, pg)

			req := tc.newRequest("/webhooks/customer-a/"+tc.provider, tc.webhookSecret, tc.body)
			rec := httptest.NewRecorder()
			handleBoundedProviderDelivery(t, g, bus, pg, rec, req, tc.runID, tc.entityID, tc.provider, tc.webhookSecret)

			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
			}
			if got := countPostgresInboundMarkers(t, ctx, db, tc.providerEventID, tc.entityID, tc.provider); got != 1 {
				t.Fatalf("inbound marker rows = %d, want 1", got)
			}
			eventID := loadPostgresInboundProviderEventID(t, ctx, db, tc.runID, tc.entityID, tc.providerEventName, tc.providerEventID)
			if got := countPostgresInboundProviderEvents(t, ctx, db, tc.runID, tc.entityID, tc.providerEventName, tc.providerEventID); got != 1 {
				t.Fatalf("provider event rows = %d, want 1", got)
			}
			if got := loadPostgresInboundProviderEventPayloadField(t, ctx, db, eventID, "provider_event_type"); got != tc.providerEventType {
				t.Fatalf("provider_event_type = %q, want %s", got, tc.providerEventType)
			}
			if got := countPostgresAgentDeliveriesForEvent(t, ctx, db, eventID, tc.agentID); got != 1 {
				t.Fatalf("agent delivery rows = %d, want 1", got)
			}
			select {
			case got := <-ch:
				if got.ID() != eventID || got.Type() != events.EventType(tc.providerEventName) {
					t.Fatalf("delivered event = %s/%s, want %s/%s", got.ID(), got.Type(), eventID, tc.providerEventName)
				}
				_ = got.Complete()
			case <-time.After(5 * time.Second):
				t.Fatalf("%s PostgreSQL post-commit dispatch did not arrive", tc.provider)
			}
			unsubscribeAndWaitForInboundBusQuiescence(t, bus, tc.agentID)
		})
	}
}

func TestInboundGateway_TypeformAndIntercomSQLitePersistsConfiguredManifestDelivery(t *testing.T) {
	for _, tc := range []struct {
		name              string
		runID             string
		entityID          string
		flowInstance      string
		provider          string
		webhookSecret     string
		providerEventID   string
		providerEventType string
		agentID           string
		providerEventName string
		body              []byte
		newRequest        func(path string, secret string, body []byte) *http.Request
	}{
		{
			name:              "typeform",
			runID:             "4b000000-0000-0000-0000-000000000001",
			entityID:          "4b000000-0000-0000-0000-000000000002",
			flowInstance:      "typeform-sqlite-provider-trigger-instance",
			provider:          "typeform",
			webhookSecret:     "typeform-secret",
			providerEventID:   "tf-evt-sqlite-123",
			providerEventType: "form_response",
			agentID:           "typeform-sqlite-webhook-subscriber",
			providerEventName: "inbound.typeform",
			body:              []byte(`{"event_id":"tf-evt-sqlite-123","event_type":"form_response","form_response":{"token":"abc"}}`),
			newRequest:        newSignedTypeformRequest,
		},
		{
			name:              "intercom",
			runID:             "4c000000-0000-0000-0000-000000000001",
			entityID:          "4c000000-0000-0000-0000-000000000002",
			flowInstance:      "intercom-sqlite-provider-trigger-instance",
			provider:          "intercom",
			webhookSecret:     "intercom-secret",
			providerEventID:   "notif_sqlite_123",
			providerEventType: "conversation_user_created",
			agentID:           "intercom-sqlite-webhook-subscriber",
			providerEventName: "inbound.intercom",
			body:              []byte(`{"id":"notif_sqlite_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
			newRequest:        newSignedIntercomRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), tc.runID)
			sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
			seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, tc.runID, tc.entityID, tc.flowInstance, "customer-a", tc.provider, tc.webhookSecret, tc.agentID)

			bus, err := newScopedTestEventBus(t, sqliteStore, runtimebus.EventBusOptions{}, tc.providerEventName)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			ch := bus.Subscribe(tc.agentID, events.EventType(tc.providerEventName))
			defer bus.Unsubscribe(tc.agentID)

			g := newTestInboundGateway(t, bus, nil, nil, sqliteStore)

			req := tc.newRequest("/webhooks/customer-a/"+tc.provider, tc.webhookSecret, tc.body)
			rec := httptest.NewRecorder()
			handleBoundedProviderDelivery(t, g, bus, sqliteStore, rec, req, tc.runID, tc.entityID, tc.provider, tc.webhookSecret)

			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
			}
			if got := countSQLiteInboundMarkers(t, ctx, sqliteStore, tc.providerEventID, tc.entityID, tc.provider); got != 1 {
				t.Fatalf("inbound marker rows = %d, want 1", got)
			}
			eventID := loadSQLiteInboundProviderEventID(t, ctx, sqliteStore, tc.runID, tc.entityID, tc.providerEventName, tc.providerEventID)
			if got := countSQLiteInboundProviderEvents(t, ctx, sqliteStore, tc.runID, tc.entityID, tc.providerEventName, tc.providerEventID); got != 1 {
				t.Fatalf("provider event rows = %d, want 1", got)
			}
			if got := loadSQLiteInboundProviderEventPayloadField(t, ctx, sqliteStore, eventID, "provider_event_type"); got != tc.providerEventType {
				t.Fatalf("provider_event_type = %q, want %s", got, tc.providerEventType)
			}
			if got := countSQLiteAgentDeliveriesForEvent(t, ctx, sqliteStore, eventID, tc.agentID); got != 1 {
				t.Fatalf("agent delivery rows = %d, want 1", got)
			}
			select {
			case got := <-ch:
				if got.ID() != eventID || got.Type() != events.EventType(tc.providerEventName) {
					t.Fatalf("delivered event = %s/%s, want %s/%s", got.ID(), got.Type(), eventID, tc.providerEventName)
				}
				_ = got.Complete()
			case <-time.After(5 * time.Second):
				t.Fatalf("%s SQLite post-commit dispatch did not arrive", tc.provider)
			}
			unsubscribeAndWaitForInboundBusQuiescence(t, bus, tc.agentID)
		})
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
		VALUES ($1, $2, 'static', $3::jsonb, 'active', now())
		ON CONFLICT (instance_id) DO UPDATE SET config = EXCLUDED.config, status = EXCLUDED.status
	`, flowInstance, boundedProviderFlowID, string(configBytes)); err != nil {
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
			ExecutionMode: "live",
			ID:            agentID,
			Role:          "observer",
			FlowID:        "global",
			Type:          "stub",
			Model:         "regular",
			FlowPath:      flowInstance,
			EntityID:      entityID,
			Subscriptions: []string{"inbound." + provider},
			Config:        []byte(`{}`),
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent(%s): %v", agentID, err)
	}
	ensureBoundedStandingTarget(t, ctx, pg, runID, entityID, provider)
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
		VALUES (?, ?, 'static', ?, 'active', ?)
	`, flowInstance, boundedProviderFlowID, string(configBytes), now); err != nil {
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
			ExecutionMode: "live",
			ID:            agentID,
			Role:          "observer",
			FlowID:        "global",
			Type:          "stub",
			Model:         "regular",
			FlowPath:      flowInstance,
			EntityID:      entityID,
			Config:        []byte(`{}`),
			Subscriptions: []string{"inbound." + provider},
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: now,
	}); err != nil {
		t.Fatalf("UpsertAgent(%s): %v", agentID, err)
	}
	ensureBoundedStandingTarget(t, ctx, sqliteStore, runID, entityID, provider)
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

func requireInboundBusEvent(t testing.TB, ch <-chan *runtimebus.LocalDelivery, context string) events.Event {
	t.Helper()
	select {
	case delivery := <-ch:
		evt := delivery.Event()
		_ = delivery.Complete()
		return evt
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: expected queued bus event", context)
		return events.Event{}
	}
}

func requireNoInboundBusEvent(t testing.TB, ch <-chan *runtimebus.LocalDelivery, context string) {
	t.Helper()
	select {
	case delivery := <-ch:
		_ = delivery.Complete()
		t.Fatalf("%s: unexpected bus event: %#v", context, delivery.Event())
	default:
	}
}

func unsubscribeAndWaitForInboundBusQuiescence(t testing.TB, bus *runtimebus.EventBus, agentID string) {
	t.Helper()
	bus.Unsubscribe(agentID)
	waitForInboundBusQuiescence(t, bus)
}

func waitForInboundBusQuiescence(t testing.TB, bus *runtimebus.EventBus) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(testAuthorActivityContext(context.Background()), 5*time.Second)
	defer cancel()
	if err := bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
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
