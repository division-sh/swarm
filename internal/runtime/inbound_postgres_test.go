package runtime_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
