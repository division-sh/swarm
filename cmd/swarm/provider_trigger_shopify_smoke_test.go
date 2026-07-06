package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestShopifyLocalProviderToolSmoke(t *testing.T) {
	if os.Getenv("SWARM_SHOPIFY_LOCAL_SMOKE") != "1" {
		t.Skip("set SWARM_SHOPIFY_LOCAL_SMOKE=1 to run the optional Shopify provider-tool smoke")
	}
	shopifyPath, err := exec.LookPath("shopify")
	if err != nil {
		t.Skip("Shopify CLI not found in PATH; install `shopify` to run the optional Shopify provider-tool smoke")
	}
	clientID := strings.TrimSpace(os.Getenv("SHOPIFY_FLAG_CLIENT_ID"))
	if clientID == "" {
		t.Skip("SHOPIFY_FLAG_CLIENT_ID is required for the optional Shopify provider-tool smoke")
	}
	clientSecret := os.Getenv("SHOPIFY_FLAG_CLIENT_SECRET")
	if strings.TrimSpace(clientSecret) == "" {
		t.Skip("SHOPIFY_FLAG_CLIENT_SECRET is required for the optional Shopify provider-tool smoke")
	}
	topic := strings.TrimSpace(os.Getenv("SHOPIFY_FLAG_TOPIC"))
	if topic == "" {
		topic = "orders/create"
	}
	apiVersion := strings.TrimSpace(os.Getenv("SHOPIFY_FLAG_API_VERSION"))
	if apiVersion == "" {
		apiVersion = "2026-04"
	}
	appPath := shopifyLocalSmokeAppPath(t, clientID, apiVersion)

	const (
		runID             = "74000000-0000-0000-0000-000000000001"
		entityID          = "74000000-0000-0000-0000-000000000002"
		flowInstance      = "shopify-local-smoke-instance"
		entitySlug        = "customer-a"
		provider          = "shopify"
		agentID           = "shopify-local-smoke-subscriber"
		providerEventName = "inbound.shopify"
	)
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	seedShopifyLocalSmokeRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, entitySlug, provider, clientSecret, agentID)

	bus, err := runtimebus.NewEventBus(sqliteStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := bus.Subscribe(agentID, events.EventType(providerEventName))
	defer bus.Unsubscribe(agentID)

	gateway := runtimepkg.NewInboundGateway(bus, nil, nil, sqliteStore)
	responseCapture := &shopifyLocalSmokeResponseCapture{}
	inboundHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &shopifyLocalSmokeCaptureWriter{ResponseWriter: w, status: http.StatusOK}
		gateway.Handler().ServeHTTP(rec, r.WithContext(ctx))
		responseCapture.record(rec.status, rec.body.String())
	})
	apiHandler := http.NotFoundHandler()
	var ready atomic.Bool
	ready.Store(true)
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen on localhost for Shopify smoke: %v", err)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("parse Shopify smoke listener address %q: %v", listener.Addr().String(), err)
	}
	server := &httptest.Server{
		Listener: listener,
		Config: &http.Server{
			Handler: newAPIServer(&ready, apiHandler, inboundHandler).Handler,
		},
	}
	server.Start()
	defer server.Close()

	address := "http://localhost:" + port + "/webhooks/" + entitySlug + "/" + provider
	output := runShopifyWebhookTrigger(t, shopifyPath, address, topic, apiVersion, clientID, clientSecret, appPath)
	if strings.Contains(output, clientSecret) {
		t.Fatal("Shopify CLI output contained the client secret")
	}
	responseStatus, responseBody := responseCapture.last()
	if responseStatus != http.StatusAccepted {
		t.Fatalf("Shopify local smoke response status = %d, want 202 body=%s", responseStatus, responseBody)
	}
	if strings.Contains(responseBody, clientSecret) {
		t.Fatal("Shopify signing secret leaked into accepted webhook response")
	}

	event := loadShopifyLocalSmokeEvent(t, ctx, sqliteStore, runID, entityID, providerEventName)
	if event.ProviderEventID == "" {
		t.Fatal("Shopify provider event id is empty")
	}
	if event.ProviderEventType == "" {
		t.Fatal("Shopify provider event type is empty")
	}
	if event.Provider != provider {
		t.Fatalf("provider = %q, want %q", event.Provider, provider)
	}
	assertShopifyLocalSmokeAcceptedResponse(t, responseBody, provider, providerEventName, event.ProviderEventID, event.ProviderEventType)
	if got := countShopifyLocalSmokeInboundMarkers(t, ctx, sqliteStore, event.ProviderEventID, entityID, provider); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	if strings.Contains(event.Payload, clientSecret) {
		t.Fatal("Shopify signing secret leaked into persisted event payload")
	}
	if got := countShopifyLocalSmokeProviderEvents(t, ctx, sqliteStore, runID, entityID, providerEventName); got != 1 {
		t.Fatalf("provider event rows = %d, want 1", got)
	}
	if got := countShopifyLocalSmokeAgentDeliveriesForEvent(t, ctx, sqliteStore, event.EventID, agentID); got != 1 {
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

	assertShopifyLocalSmokeBadSignatureFailsClosed(t, ctx, address, sqliteStore, runID, entityID, providerEventName, provider)
}

func shopifyLocalSmokeAppPath(t *testing.T, clientID string, apiVersion string) string {
	t.Helper()
	if appPath := strings.TrimSpace(os.Getenv("SHOPIFY_FLAG_PATH")); appPath != "" {
		return appPath
	}
	dir := t.TempDir()
	config := fmt.Sprintf(`name = "Swarm Provider Trigger Smoke"
client_id = %q
application_url = "https://example.com/"
embedded = false

[auth]
redirect_urls = ["https://example.com/auth/callback"]

[webhooks]
api_version = %q
`, clientID, apiVersion)
	if err := os.WriteFile(filepath.Join(dir, "shopify.app.toml"), []byte(config), 0o600); err != nil {
		t.Fatalf("write throwaway Shopify app config: %v", err)
	}
	return dir
}

type shopifyLocalSmokeResponseCapture struct {
	mu     sync.Mutex
	status int
	body   string
}

func (c *shopifyLocalSmokeResponseCapture) record(status int, body string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status = status
	c.body = body
}

func (c *shopifyLocalSmokeResponseCapture) last() (int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status, c.body
}

type shopifyLocalSmokeCaptureWriter struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (w *shopifyLocalSmokeCaptureWriter) WriteHeader(statusCode int) {
	w.status = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *shopifyLocalSmokeCaptureWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.body.Write(data)
	return w.ResponseWriter.Write(data)
}

func runShopifyWebhookTrigger(t *testing.T, shopifyPath string, address string, topic string, apiVersion string, clientID string, clientSecret string, appPath string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	args := []string{
		"app", "webhook", "trigger",
		"--address", address,
		"--topic", topic,
		"--api-version", apiVersion,
		"--client-id", clientID,
		"--delivery-method", "http",
	}
	if appPath != "" {
		args = append(args, "--path", appPath)
	}
	cmd := exec.CommandContext(ctx, shopifyPath, args...)
	cmd.Env = append(os.Environ(),
		"SHOPIFY_FLAG_CLIENT_SECRET="+clientSecret,
		"SHOPIFY_FLAG_ADDRESS="+address,
		"SHOPIFY_FLAG_TOPIC="+topic,
		"SHOPIFY_FLAG_API_VERSION="+apiVersion,
		"SHOPIFY_FLAG_CLIENT_ID="+clientID,
		"SHOPIFY_CLI_NO_ANALYTICS=1",
		"CI=1",
	)
	if appPath != "" {
		cmd.Env = append(cmd.Env, "SHOPIFY_FLAG_PATH="+appPath)
	}
	cmd.Stdin = bytes.NewReader(nil)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	err := cmd.Run()
	rawOutput := output.String()
	if strings.Contains(rawOutput, clientSecret) {
		t.Fatal("Shopify CLI output contained the client secret")
	}
	if err != nil {
		t.Fatalf("shopify app webhook trigger failed: %v\n%s", err, sanitizeShopifyLocalSmokeOutput(rawOutput, clientSecret))
	}
	return rawOutput
}

func assertShopifyLocalSmokeAcceptedResponse(t *testing.T, body string, provider string, eventName string, providerEventID string, providerEventType string) {
	t.Helper()
	var response map[string]any
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		t.Fatalf("unmarshal accepted webhook response: %v body=%s", err, body)
	}
	for field, want := range map[string]string{
		"status":              "accepted",
		"provider":            provider,
		"provider_event_id":   providerEventID,
		"provider_event_type": providerEventType,
		"event_name":          eventName,
	} {
		if got, _ := response[field].(string); got != want {
			t.Fatalf("accepted response %s = %q, want %q body=%s", field, got, want, body)
		}
	}
}

func assertShopifyLocalSmokeBadSignatureFailsClosed(t *testing.T, ctx context.Context, address string, sqliteStore *store.SQLiteRuntimeStore, runID string, entityID string, eventName string, provider string) {
	t.Helper()
	body := []byte(`{"id":999,"line_items":[{"sku":"bad"}]}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, address, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new bad-signature request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Shopify-Hmac-Sha256", "invalid")
	req.Header.Set("X-Shopify-Webhook-Id", "shopify-local-smoke-invalid")
	req.Header.Set("X-Shopify-Topic", "orders/create")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send bad-signature request: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad-signature status = %d, want 401 body=%s", resp.StatusCode, string(respBody))
	}
	if got := countShopifyLocalSmokeInboundMarkers(t, ctx, sqliteStore, "shopify-local-smoke-invalid", entityID, provider); got != 0 {
		t.Fatalf("bad-signature inbound marker rows = %d, want 0", got)
	}
	if got := countShopifyLocalSmokeProviderEventsByProviderEventID(t, ctx, sqliteStore, runID, entityID, eventName, "shopify-local-smoke-invalid"); got != 0 {
		t.Fatalf("bad-signature provider event rows = %d, want 0", got)
	}
}

func seedShopifyLocalSmokeRuntime(
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
			Subscriptions: []string{"inbound." + provider},
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: now,
	}); err != nil {
		t.Fatalf("UpsertAgent(%s): %v", agentID, err)
	}
}

type shopifyLocalSmokeEvent struct {
	EventID           string
	Provider          string
	ProviderEventID   string
	ProviderEventType string
	Payload           string
}

func loadShopifyLocalSmokeEvent(t *testing.T, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, runID string, entityID string, eventName string) shopifyLocalSmokeEvent {
	t.Helper()
	var event shopifyLocalSmokeEvent
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT
			event_id,
			json_extract(payload, '$.provider'),
			json_extract(payload, '$.provider_event_id'),
			json_extract(payload, '$.provider_event_type'),
			payload
		FROM events
		WHERE run_id = ?
		  AND entity_id = ?
		  AND event_name = ?
		ORDER BY created_at DESC
		LIMIT 1
	`, runID, entityID, eventName).Scan(&event.EventID, &event.Provider, &event.ProviderEventID, &event.ProviderEventType, &event.Payload); err != nil {
		t.Fatalf("load Shopify local smoke event: %v", err)
	}
	return event
}

func countShopifyLocalSmokeProviderEvents(t *testing.T, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, runID string, entityID string, eventName string) int {
	t.Helper()
	var count int
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = ?
		  AND entity_id = ?
		  AND event_name = ?
	`, runID, entityID, eventName).Scan(&count); err != nil {
		t.Fatalf("count Shopify local smoke provider events: %v", err)
	}
	return count
}

func countShopifyLocalSmokeProviderEventsByProviderEventID(t *testing.T, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, runID string, entityID string, eventName string, providerEventID string) int {
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
		t.Fatalf("count Shopify local smoke provider events for %q: %v", providerEventID, err)
	}
	return count
}

func countShopifyLocalSmokeInboundMarkers(t *testing.T, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, providerEventID string, entityID string, provider string) int {
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
		t.Fatalf("count Shopify local smoke inbound marker events: %v", err)
	}
	return count
}

func countShopifyLocalSmokeAgentDeliveriesForEvent(t *testing.T, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, eventID string, agentID string) int {
	t.Helper()
	var count int
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = ?
		  AND subscriber_type = 'agent'
		  AND subscriber_id = ?
	`, eventID, agentID).Scan(&count); err != nil {
		t.Fatalf("count Shopify local smoke agent deliveries for %s: %v", eventID, err)
	}
	return count
}

func sanitizeShopifyLocalSmokeOutput(output string, secret string) string {
	if secret == "" {
		return output
	}
	return strings.ReplaceAll(output, secret, fmt.Sprintf("<redacted:%d>", len(secret)))
}
