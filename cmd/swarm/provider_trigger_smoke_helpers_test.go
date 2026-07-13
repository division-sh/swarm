package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store"
)

type providerTriggerSmokeResponse struct {
	status int
	body   string
}

type providerTriggerSmokeResponseCapture struct {
	mu   sync.Mutex
	last providerTriggerSmokeResponse
	ch   chan providerTriggerSmokeResponse
}

func newProviderTriggerSmokeResponseCapture() *providerTriggerSmokeResponseCapture {
	return &providerTriggerSmokeResponseCapture{ch: make(chan providerTriggerSmokeResponse, 16)}
}

func (c *providerTriggerSmokeResponseCapture) record(status int, body string) {
	response := providerTriggerSmokeResponse{status: status, body: body}
	c.mu.Lock()
	c.last = response
	c.mu.Unlock()
	select {
	case c.ch <- response:
	default:
	}
}

func (c *providerTriggerSmokeResponseCapture) lastResponse() (int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last.status, c.last.body
}

func (c *providerTriggerSmokeResponseCapture) wait(ctx context.Context) (providerTriggerSmokeResponse, error) {
	select {
	case response := <-c.ch:
		return response, nil
	case <-ctx.Done():
		return providerTriggerSmokeResponse{}, ctx.Err()
	}
}

type providerTriggerSmokeCaptureWriter struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

type providerTriggerSmokeCredentialStore map[string]string

func (s providerTriggerSmokeCredentialStore) Get(_ context.Context, key string) (string, bool, error) {
	value, ok := s[key]
	return value, ok, nil
}
func (providerTriggerSmokeCredentialStore) Set(context.Context, string, string) error { return nil }
func (providerTriggerSmokeCredentialStore) List(context.Context) ([]string, error)    { return nil, nil }
func (providerTriggerSmokeCredentialStore) Delete(context.Context, string) error      { return nil }

func (w *providerTriggerSmokeCaptureWriter) WriteHeader(statusCode int) {
	w.status = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *providerTriggerSmokeCaptureWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.body.Write(data)
	return w.ResponseWriter.Write(data)
}

func startProviderTriggerSmokeServer(
	t *testing.T,
	ctx context.Context,
	listenAddr string,
	bus *runtimebus.EventBus,
	sqliteStore *store.SQLiteRuntimeStore,
	responseCapture *providerTriggerSmokeResponseCapture,
	target runtimepkg.InboundTarget,
	signingSecret string,
) string {
	t.Helper()
	if strings.TrimSpace(listenAddr) == "" {
		listenAddr = "localhost:0"
	}
	gateway := runtimepkg.NewInboundGateway(bus, nil, nil)
	gateway.SetCredentialStore(providerTriggerSmokeCredentialStore{target.SigningSecret: signingSecret})
	inboundHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &providerTriggerSmokeCaptureWriter{ResponseWriter: w, status: http.StatusOK}
		gateway.HandleResolvedWebhook(rec, r.WithContext(ctx), target, nil)
		if responseCapture != nil && r.Method == http.MethodPost {
			responseCapture.record(rec.status, rec.body.String())
		}
	})
	apiHandler := http.NotFoundHandler()
	var ready atomic.Bool
	ready.Store(true)
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		t.Fatalf("listen on %s for provider trigger smoke: %v", listenAddr, err)
	}
	server := &httptest.Server{
		Listener: listener,
		Config: &http.Server{
			Handler: newAPIServer(&ready, apiHandler, inboundHandler).Handler,
		},
	}
	server.Start()
	t.Cleanup(server.Close)
	return "http://" + providerTriggerSmokeHTTPHostPort(t, listener.Addr(), listenAddr)
}

func providerTriggerSmokeHTTPHostPort(t *testing.T, addr net.Addr, requestedListenAddr string) string {
	t.Helper()
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		t.Fatalf("parse provider trigger smoke listener address %q: %v", addr.String(), err)
	}
	requestedHost, _, requestedErr := net.SplitHostPort(requestedListenAddr)
	if requestedErr == nil && requestedHost == "localhost" {
		host = "localhost"
	}
	if host == "" || host == "::" || host == "0.0.0.0" || host == "[::]" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

func assertProviderTriggerSmokeAcceptedResponse(t *testing.T, body string, provider string, eventName string, providerEventID string, providerEventType string) {
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

func seedProviderTriggerSmokeRuntime(
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

type providerTriggerSmokeEvent struct {
	EventID           string
	Provider          string
	ProviderEventID   string
	ProviderEventType string
	Payload           string
}

func loadProviderTriggerSmokeEvent(t *testing.T, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, runID string, entityID string, eventName string) providerTriggerSmokeEvent {
	t.Helper()
	var event providerTriggerSmokeEvent
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
		t.Fatalf("load provider trigger smoke event: %v", err)
	}
	return event
}

func countProviderTriggerSmokeProviderEvents(t *testing.T, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, runID string, entityID string, eventName string) int {
	t.Helper()
	var count int
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = ?
		  AND entity_id = ?
		  AND event_name = ?
	`, runID, entityID, eventName).Scan(&count); err != nil {
		t.Fatalf("count provider trigger smoke events: %v", err)
	}
	return count
}

func countProviderTriggerSmokeProviderEventsByProviderEventID(t *testing.T, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, runID string, entityID string, eventName string, providerEventID string) int {
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
		t.Fatalf("count provider trigger smoke events for %q: %v", providerEventID, err)
	}
	return count
}

func countProviderTriggerSmokeInboundMarkers(t *testing.T, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, providerEventID string, entityID string, provider string) int {
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
		t.Fatalf("count provider trigger smoke inbound marker events: %v", err)
	}
	return count
}

func countProviderTriggerSmokeAgentDeliveriesForEvent(t *testing.T, ctx context.Context, sqliteStore *store.SQLiteRuntimeStore, eventID string, agentID string) int {
	t.Helper()
	var count int
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = ?
		  AND subscriber_type = 'agent'
		  AND subscriber_id = ?
	`, eventID, agentID).Scan(&count); err != nil {
		t.Fatalf("count provider trigger smoke agent deliveries for %s: %v", eventID, err)
	}
	return count
}

func sanitizeProviderTriggerSmokeOutput(output string, secret string) string {
	if secret == "" {
		return output
	}
	return strings.ReplaceAll(output, secret, fmt.Sprintf("<redacted:%d>", len(secret)))
}
