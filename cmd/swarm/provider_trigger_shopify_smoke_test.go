package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
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
	seedProviderTriggerSmokeRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, entitySlug, provider, clientSecret, agentID)

	bus, err := runtimebus.NewEventBus(sqliteStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := bus.Subscribe(agentID, events.EventType(providerEventName))
	defer bus.Unsubscribe(agentID)

	responseCapture := newProviderTriggerSmokeResponseCapture()
	baseURL := startProviderTriggerSmokeServer(t, ctx, "localhost:0", bus, sqliteStore, responseCapture, runtimepkg.InboundTarget{
		RunID: runID, FlowInstance: flowInstance, EntityID: entityID, EntitySlug: entitySlug,
		Alias: entitySlug, Provider: provider, SigningSecret: "bounded_smoke.shopify",
	}, clientSecret)

	address := baseURL + "/webhooks/" + entitySlug + "/" + provider
	output := runShopifyWebhookTrigger(t, shopifyPath, address, topic, apiVersion, clientID, clientSecret, appPath)
	if strings.Contains(output, clientSecret) {
		t.Fatal("Shopify CLI output contained the client secret")
	}
	responseStatus, responseBody := responseCapture.lastResponse()
	if responseStatus != http.StatusAccepted {
		t.Fatalf("Shopify local smoke response status = %d, want 202 body=%s", responseStatus, responseBody)
	}
	if strings.Contains(responseBody, clientSecret) {
		t.Fatal("Shopify signing secret leaked into accepted webhook response")
	}

	event := loadProviderTriggerSmokeEvent(t, ctx, sqliteStore, runID, entityID, providerEventName)
	if event.ProviderEventID == "" {
		t.Fatal("Shopify provider event id is empty")
	}
	if event.ProviderEventType == "" {
		t.Fatal("Shopify provider event type is empty")
	}
	if event.Provider != provider {
		t.Fatalf("provider = %q, want %q", event.Provider, provider)
	}
	assertProviderTriggerSmokeAcceptedResponse(t, responseBody, provider, providerEventName, event.ProviderEventID, event.ProviderEventType)
	if got := countProviderTriggerSmokeInboundMarkers(t, ctx, sqliteStore, event.ProviderEventID, entityID, provider); got != 1 {
		t.Fatalf("inbound marker rows = %d, want 1", got)
	}
	if strings.Contains(event.Payload, clientSecret) {
		t.Fatal("Shopify signing secret leaked into persisted event payload")
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
		t.Fatalf("shopify app webhook trigger failed: %v\n%s", err, sanitizeProviderTriggerSmokeOutput(rawOutput, clientSecret))
	}
	return rawOutput
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
	if got := countProviderTriggerSmokeInboundMarkers(t, ctx, sqliteStore, "shopify-local-smoke-invalid", entityID, provider); got != 0 {
		t.Fatalf("bad-signature inbound marker rows = %d, want 0", got)
	}
	if got := countProviderTriggerSmokeProviderEventsByProviderEventID(t, ctx, sqliteStore, runID, entityID, eventName, "shopify-local-smoke-invalid"); got != 0 {
		t.Fatalf("bad-signature provider event rows = %d, want 0", got)
	}
}
