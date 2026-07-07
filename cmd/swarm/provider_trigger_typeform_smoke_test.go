package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestTypeformManualLiveHTTPSWebhookSmoke(t *testing.T) {
	if os.Getenv("SWARM_TYPEFORM_LIVE_SMOKE") != "1" {
		t.Skip("set SWARM_TYPEFORM_LIVE_SMOKE=1 to run the optional Typeform manual-live HTTPS smoke")
	}
	const (
		runID             = "74700000-0000-0000-0000-000000000001"
		entityID          = "74700000-0000-0000-0000-000000000002"
		flowInstance      = "typeform-live-smoke-instance"
		entitySlug        = "customer-a"
		provider          = "typeform"
		agentID           = "typeform-live-smoke-subscriber"
		providerEventName = "inbound.typeform"
		webhookPath       = "/webhooks/customer-a/typeform"
	)
	publicWebhookURL := typeformLiveSmokePublicURL(t, webhookPath)
	webhookSecret := os.Getenv("TYPEFORM_WEBHOOK_SECRET")
	if strings.TrimSpace(webhookSecret) == "" {
		t.Skip("TYPEFORM_WEBHOOK_SECRET is required for the optional Typeform manual-live HTTPS smoke")
	}
	listenAddr := typeformLiveSmokeListenAddr(t)
	timeout := typeformLiveSmokeTimeout(t)

	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	seedProviderTriggerSmokeRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, entitySlug, provider, webhookSecret, agentID)

	bus, err := runtimebus.NewEventBus(sqliteStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := bus.Subscribe(agentID, events.EventType(providerEventName))
	defer bus.Unsubscribe(agentID)

	responseCapture := newProviderTriggerSmokeResponseCapture()
	baseURL := startProviderTriggerSmokeServer(t, ctx, listenAddr, bus, sqliteStore, responseCapture)
	localAddress := baseURL + webhookPath
	t.Logf("waiting up to %s for Typeform to POST to %s through a tunnel that forwards to %s", timeout, publicWebhookURL, localAddress)
	t.Log("trigger a Typeform UI Send test request or submit the configured form; this smoke does not call the Typeform API")

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	response, err := responseCapture.wait(waitCtx)
	if err != nil {
		t.Fatalf("timed out waiting for Typeform live webhook delivery at %s: %v", publicWebhookURL, err)
	}
	if response.status != http.StatusAccepted {
		t.Fatalf("Typeform live smoke response status = %d, want 202 body=%s", response.status, response.body)
	}
	if strings.Contains(response.body, webhookSecret) {
		t.Fatal("Typeform signing secret leaked into accepted webhook response")
	}

	event := loadProviderTriggerSmokeEvent(t, ctx, sqliteStore, runID, entityID, providerEventName)
	if event.ProviderEventID == "" {
		t.Fatal("Typeform provider event id is empty")
	}
	if event.ProviderEventType == "" {
		t.Fatal("Typeform provider event type is empty")
	}
	if event.Provider != provider {
		t.Fatalf("provider = %q, want %q", event.Provider, provider)
	}
	assertProviderTriggerSmokeAcceptedResponse(t, response.body, provider, providerEventName, event.ProviderEventID, event.ProviderEventType)
	if got := countProviderTriggerSmokeInboundMarkers(t, ctx, sqliteStore, event.ProviderEventID, entityID, provider); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	if strings.Contains(event.Payload, webhookSecret) {
		t.Fatal("Typeform signing secret leaked into persisted event payload")
	}
	if got := countProviderTriggerSmokeProviderEvents(t, ctx, sqliteStore, runID, entityID, providerEventName); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	if got := countProviderTriggerSmokeAgentDeliveriesForEvent(t, ctx, sqliteStore, event.EventID, agentID); got != 1 {
		t.Fatalf("agent delivery rows = %d, want 1", got)
	}
	select {
	case got := <-ch:
		if got.ID() != event.EventID || got.Type() != events.EventType(providerEventName) {
			t.Fatalf("delivered event = %s/%s, want %s/%s", got.ID(), got.Type(), event.EventID, providerEventName)
		}
	default:
		// The smoke asserts durable_before_dispatch evidence. Handler execution
		// is intentionally not a prerequisite for the HTTP acknowledgement.
	}

	assertTypeformLiveSmokeBadSignatureFailsClosed(t, ctx, localAddress, sqliteStore, runID, entityID, providerEventName, provider)
}

func typeformLiveSmokePublicURL(t *testing.T, webhookPath string) string {
	t.Helper()
	rawURL := strings.TrimSpace(os.Getenv("SWARM_TYPEFORM_LIVE_SMOKE_WEBHOOK_URL"))
	if rawURL == "" {
		t.Skip("SWARM_TYPEFORM_LIVE_SMOKE_WEBHOOK_URL is required for the optional Typeform manual-live HTTPS smoke")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse SWARM_TYPEFORM_LIVE_SMOKE_WEBHOOK_URL: %v", err)
	}
	if parsed.Scheme != "https" {
		t.Fatalf("SWARM_TYPEFORM_LIVE_SMOKE_WEBHOOK_URL must use https, got %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		t.Fatal("SWARM_TYPEFORM_LIVE_SMOKE_WEBHOOK_URL must include a host")
	}
	if parsed.Path != webhookPath {
		t.Fatalf("SWARM_TYPEFORM_LIVE_SMOKE_WEBHOOK_URL path = %q, want %q", parsed.Path, webhookPath)
	}
	return rawURL
}

func typeformLiveSmokeListenAddr(t *testing.T) string {
	t.Helper()
	listenAddr := strings.TrimSpace(os.Getenv("SWARM_TYPEFORM_LIVE_SMOKE_LISTEN_ADDR"))
	if listenAddr == "" {
		listenAddr = "127.0.0.1:17470"
	}
	_, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		t.Fatalf("parse SWARM_TYPEFORM_LIVE_SMOKE_LISTEN_ADDR: %v", err)
	}
	if port == "0" {
		t.Fatal("SWARM_TYPEFORM_LIVE_SMOKE_LISTEN_ADDR must use a stable port so the HTTPS tunnel can forward to it")
	}
	return listenAddr
}

func typeformLiveSmokeTimeout(t *testing.T) time.Duration {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("SWARM_TYPEFORM_LIVE_SMOKE_TIMEOUT"))
	if raw == "" {
		return 5 * time.Minute
	}
	timeout, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("parse SWARM_TYPEFORM_LIVE_SMOKE_TIMEOUT: %v", err)
	}
	if timeout <= 0 {
		t.Fatalf("SWARM_TYPEFORM_LIVE_SMOKE_TIMEOUT must be positive, got %s", timeout)
	}
	return timeout
}

func assertTypeformLiveSmokeBadSignatureFailsClosed(t *testing.T, ctx context.Context, address string, sqliteStore *store.SQLiteRuntimeStore, runID string, entityID string, eventName string, provider string) {
	t.Helper()
	body := []byte(`{"event_id":"typeform-live-smoke-invalid","event_type":"form_response","form_response":{"token":"invalid"}}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, address, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new Typeform bad-signature request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Typeform-Signature", "sha256=invalid")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send Typeform bad-signature request: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("Typeform bad-signature status = %d, want 401 body=%s", resp.StatusCode, string(respBody))
	}
	if got := countProviderTriggerSmokeInboundMarkers(t, ctx, sqliteStore, "typeform-live-smoke-invalid", entityID, provider); got != 0 {
		t.Fatalf("Typeform bad-signature inbound marker rows = %d, want 0", got)
	}
	if got := countProviderTriggerSmokeProviderEventsByProviderEventID(t, ctx, sqliteStore, runID, entityID, eventName, "typeform-live-smoke-invalid"); got != 0 {
		t.Fatalf("Typeform bad-signature provider event rows = %d, want 0", got)
	}
}
