package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/connectorapi"
)

func TestSlackManagedCredentialConnectorPackRoundTripThroughActivityJournal(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)

		const flowInstance = boundedProviderFlowID
		runID, entityID := boundedInboundTestCoordinates()
		ctx := testAuthorActivityContext(runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID))
		pg := storetest.AdmitPostgresRuntimeStore(t, db)

		runSlackManagedCredentialConnectorSurface(t, slackManagedConnectorBackend{
			name:          "postgres",
			ctx:           ctx,
			db:            db,
			eventStore:    pg,
			deliveryStore: pg,
			inboundStore:  pg,
			persistence:   runtimepipeline.NewWorkflowPersistence(pg),
			runLifecycle:  pg,
			obligations:   pg.PipelineObligations(),
			runID:         runID,
			entityID:      entityID,
			flowInstance:  flowInstance,
			sqlite:        false,
		})
	})

	t.Run("sqlite", func(t *testing.T) {
		const flowInstance = boundedProviderFlowID
		runID, entityID := boundedInboundTestCoordinates()
		ctx := testAuthorActivityContext(runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID))
		sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)

		runSlackManagedCredentialConnectorSurface(t, slackManagedConnectorBackend{
			name:          "sqlite",
			ctx:           ctx,
			db:            storetest.Database(sqliteStore),
			eventStore:    sqliteStore,
			deliveryStore: sqliteStore,
			inboundStore:  sqliteStore,
			persistence:   runtimepipeline.NewWorkflowPersistence(sqliteStore),
			runLifecycle:  sqliteStore,
			obligations:   sqliteStore.PipelineObligations(),
			runID:         runID,
			entityID:      entityID,
			flowInstance:  flowInstance,
			sqlite:        true,
		})
	})
}

type slackManagedConnectorBackend struct {
	source           semanticview.Source
	name             string
	ctx              context.Context
	db               *sql.DB
	eventStore       runtimebus.EventStore
	deliveryStore    runtimedelivery.Store
	inboundStore     runtimepkg.InboundPersistence
	inboundTarget    runtimepkg.InboundTarget
	activityAttempts runtimeTestActivityAttemptReader
	persistence      runtimepipeline.WorkflowPersistence
	runLifecycle     runtimerunlifecycle.OperationOwner
	obligations      runtimepipelineobligation.Store
	runID            string
	entityID         string
	flowInstance     string
	sqlite           bool
}

type slackManagedConnectorCall = connectorapi.Call

type runtimeTestActivityAttemptReader interface {
	LoadActivityAttempt(context.Context, string) (runtimepipeline.ActivityAttemptRecord, bool, error)
}

func runSlackManagedCredentialConnectorSurface(t *testing.T, backend slackManagedConnectorBackend) {
	t.Helper()
	fake := newFakeSlackManagedConnectorServer(t)
	defer fake.Server.Close()

	managedStore := runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
		Key:          "slack.slack_oauth",
		Provider:     "slack",
		GrantType:    runtimemanagedcredentials.GrantAuthorizationCodePKCE,
		TokenURL:     fake.Server.URL + "/oauth.v2.access",
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		Scopes:       []string{"chat:write"},
		AccessToken:  "expired-token",
		RefreshToken: "refresh-secret",
		Status:       runtimemanagedcredentials.StatusConnected,
		ExpiresAt:    time.Now().Add(-time.Hour),
	})
	source := slackManagedConnectorSource(t, fake.Server.URL, backend.flowInstance)
	backend.source = source
	backend.ctx, backend.inboundTarget = seedCapturedProviderTarget(t, backend.ctx, backend.inboundStore, source, backend.runID, backend.entityID, backend.flowInstance, "customer-a", "telegram", "telegram-secret", "slack-managed-credential-observer")
	bus, pc := startSlackManagedConnectorBusAndCoordinator(t, backend, source, managedStore)
	backend.activityAttempts = pc
	gateway := newCapturedProviderGateway(t, bus, source, backend.inboundStore)
	webhookPath := fmt.Sprintf("/webhooks/%s/telegram", backend.entityID)

	publishTelegramMessageToSlack(t, backend, bus, gateway, webhookPath, "123456789", "hello from telegram")
	firstInboundEventID := loadSlackManagedConnectorInboundEventID(t, backend, "123456789")
	firstAttempt := waitForSlackManagedConnectorTerminalActivityAttempt(t, backend, firstInboundEventID)
	if firstAttempt.Status != runtimepipeline.ActivityAttemptStatusSucceeded {
		t.Fatalf("%s first activity attempt status = %q failure=%#v, want succeeded", backend.name, firstAttempt.Status, firstAttempt.Failure)
	}
	firstCall := fake.requireSideEffectCall(t, backend.name, "refresh-before-use")
	if firstCall.Auth != "Bearer fresh-token" {
		t.Fatalf("%s first Slack auth = %q, want Bearer fresh-token", backend.name, firstCall.Auth)
	}
	if got := slackManagedConnectorString(firstCall.Body["channel"]); got != "C123" {
		t.Fatalf("%s first Slack channel = %#v, want C123", backend.name, firstCall.Body["channel"])
	}
	if got := slackManagedConnectorString(firstCall.Body["text"]); got != "hello from telegram" {
		t.Fatalf("%s first Slack text = %#v, want inbound text", backend.name, firstCall.Body["text"])
	}
	requireSlackManagedConnectorResultEventEventually(t, backend, firstAttempt.ResultEventID, firstAttempt.ResultEventType)
	if got := fake.refreshCount(); got != 1 {
		t.Fatalf("%s refresh-before-use token refreshes = %d, want 1", backend.name, got)
	}

	publishTelegramMessageToSlack(t, backend, bus, gateway, webhookPath, "123456790", "needs 401 refresh")
	secondInboundEventID := loadSlackManagedConnectorInboundEventID(t, backend, "123456790")
	secondAttempt := waitForSlackManagedConnectorTerminalActivityAttempt(t, backend, secondInboundEventID)
	if secondAttempt.Status != runtimepipeline.ActivityAttemptStatusSucceeded {
		t.Fatalf("%s second activity attempt status = %q failure=%#v, want succeeded", backend.name, secondAttempt.Status, secondAttempt.Failure)
	}
	secondCall := fake.requireSideEffectCall(t, backend.name, "refresh-on-401")
	if secondCall.Auth != "Bearer after-401-token" {
		t.Fatalf("%s second Slack auth = %q, want Bearer after-401-token", backend.name, secondCall.Auth)
	}
	if got := fake.refreshCount(); got != 2 {
		t.Fatalf("%s token refreshes after 401 = %d, want 2", backend.name, got)
	}
	if got := fake.providerHTTPRequestCount(); got != 3 {
		t.Fatalf("%s Slack HTTP requests = %d, want 3 (success, 401, retry success)", backend.name, got)
	}

	requestEvent := loadSlackManagedConnectorActivityRequestEvent(t, backend, secondAttempt.RequestEventID)
	if err := bus.EngineDispatcher().DispatchPostCommit(backend.ctx, []runtimeengine.EmitIntent{{Event: requestEvent}}); err != nil {
		t.Fatalf("%s duplicate activity request dispatch: %v", backend.name, err)
	}
	waitForInboundBusQuiescence(t, bus)
	fake.requireNoSideEffectCall(t, backend.name, "duplicate activity request")
	if got := fake.refreshCount(); got != 2 {
		t.Fatalf("%s token refreshes after duplicate = %d, want still 2", backend.name, got)
	}
	if got := countSlackManagedConnectorActivityAttempts(t, backend); got != 2 {
		t.Fatalf("%s activity attempts after duplicate = %d, want 2", backend.name, got)
	}

	publishTelegramMessageToSlack(t, backend, bus, gateway, webhookPath, "123456792", "provider ok false")
	okFalseInboundEventID := loadSlackManagedConnectorInboundEventID(t, backend, "123456792")
	okFalseAttempt := waitForSlackManagedConnectorTerminalActivityAttempt(t, backend, okFalseInboundEventID)
	fake.requireNoSideEffectCall(t, backend.name, "response_success ok false")
	if okFalseAttempt.Status != runtimepipeline.ActivityAttemptStatusFailed {
		t.Fatalf("%s ok:false activity attempt status = %q, want failed", backend.name, okFalseAttempt.Status)
	}
	requireSlackManagedConnectorResultEventEventually(t, backend, okFalseAttempt.ResultEventID, okFalseAttempt.ResultEventType)
	if got := countSlackManagedConnectorFailureEventsForSource(t, backend, okFalseInboundEventID); got != 1 {
		t.Fatalf("%s ok:false failure events = %d, want 1", backend.name, got)
	}
	if got := fake.providerHTTPRequestCount(); got != 4 {
		t.Fatalf("%s Slack HTTP requests after ok:false = %d, want 4", backend.name, got)
	}

	assertSlackManagedConnectorMissingCredential(t, backend, fake.Server.URL)
	_ = pc
	for _, secret := range []string{"expired-token", "fresh-token", "after-401-token", "refresh-secret", "client-secret"} {
		assertSlackManagedConnectorNoStoredSecret(t, backend, secret)
	}
}

type fakeSlackManagedConnectorServer struct{ *connectorapi.Slack }

func newFakeSlackManagedConnectorServer(t *testing.T) *fakeSlackManagedConnectorServer {
	t.Helper()
	return &fakeSlackManagedConnectorServer{connectorapi.NewSlack(t)}
}

func (f *fakeSlackManagedConnectorServer) requireSideEffectCall(t *testing.T, backend, context string) slackManagedConnectorCall {
	t.Helper()
	timer := time.NewTimer(connectorSupportedSurfaceAsyncTimeout)
	defer timer.Stop()
	select {
	case call := <-f.SideEffects:
		return call
	case <-timer.C:
		t.Fatalf("%s %s: timed out waiting for fake Slack side effect", backend, context)
		return slackManagedConnectorCall{}
	}
}

func (f *fakeSlackManagedConnectorServer) requireNoSideEffectCall(t *testing.T, backend, context string) {
	t.Helper()
	select {
	case call := <-f.SideEffects:
		t.Fatalf("%s %s: unexpected fake Slack side effect: auth=%s body=%s", backend, context, call.Auth, call.Raw)
	default:
	}
}

func (f *fakeSlackManagedConnectorServer) refreshCount() int {
	return int(f.TokenCalls.Load())
}

func (f *fakeSlackManagedConnectorServer) providerHTTPRequestCount() int {
	return int(f.Calls.Load())
}

func publishTelegramMessageToSlack(t *testing.T, backend slackManagedConnectorBackend, bus *runtimebus.EventBus, gateway *runtimepkg.InboundGateway, webhookPath, updateID, text string) {
	t.Helper()
	body := []byte(fmt.Sprintf(`{"update_id":%s,"message":{"message_id":7,"from":{"id":7},"chat":{"id":42,"type":"private"},"text":%q}}`, updateID, text))
	req := newSignedTelegramRequest(webhookPath, "telegram-secret", body).WithContext(backend.ctx)
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, gateway, bus, backend.inboundTarget, rec, req, "telegram", "telegram-secret")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("%s gateway status for update %s = %d, want 202 body=%s", backend.name, updateID, rec.Code, rec.Body.String())
	}
}

type slackManagedConnectorModule struct {
	source semanticview.Source
	nodes  []runtimepipeline.WorkflowNode
	guards runtimepipeline.GuardRegistry
}

func (m slackManagedConnectorModule) SemanticSource() semanticview.Source {
	return m.source
}

func (m slackManagedConnectorModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.nodes...)
}

func (m slackManagedConnectorModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return m.guards
}

func startSlackManagedConnectorBusAndCoordinator(t *testing.T, backend slackManagedConnectorBackend, source semanticview.Source, managedStore runtimemanagedcredentials.Store) (*runtimebus.EventBus, *runtimepipeline.PipelineCoordinator) {
	t.Helper()
	var pc *runtimepipeline.PipelineCoordinator
	bus, err := newExternalRuntimeTestActivityBus(t, backend.eventStore, runtimebus.EventBusOptions{
		ContractBundle: source, SourceArtifactFact: runtimeTestSourceArtifactFact(t, source),
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	}, "inbound.github.raw.issue_comment", "inbound.github.raw.issues", "inbound.telegram.raw")
	if err != nil {
		t.Fatalf("%s NewEventBusWithOptions: %v", backend.name, err)
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("%s LoadWorkflowNodes: %v", backend.name, err)
	}
	module := slackManagedConnectorModule{
		source: source,
		nodes:  nodes,
		guards: runtimepipeline.NewContractGuardRegistry(source),
	}
	pc = newExternalRuntimeTestActivityCoordinator(t, bus, backend.eventStore, runtimepipeline.PipelineCoordinatorOptions{
		SourceArtifactFact:  runtimeTestSourceArtifactFact(t, source),
		WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
		Module:              module,
		Persistence:         backend.persistence,
		RunLifecycle:        backend.runLifecycle,
		PipelineObligations: backend.obligations,
		DeliveryStore:       backend.deliveryStore,
		ManagedCredentials:  managedStore,
	})

	startConfiguredChannelActivityNode(t, backend.ctx, pc, bus, backend.db)
	return bus, pc
}

func assertSlackManagedConnectorMissingCredential(t *testing.T, backend slackManagedConnectorBackend, baseURL string) {
	t.Helper()
	source := backend.source
	bus, pc := startSlackManagedConnectorBusAndCoordinator(t, backend, source, runtimemanagedcredentials.NewMemoryStore())
	backend.activityAttempts = pc
	gateway := newCapturedProviderGateway(t, bus, source, backend.inboundStore)
	webhookPath := fmt.Sprintf("/webhooks/%s/telegram", backend.entityID)
	publishTelegramMessageToSlack(t, backend, bus, gateway, webhookPath, "123456791", "missing credential")
	inboundEventID := loadSlackManagedConnectorInboundEventID(t, backend, "123456791")
	if attempt := waitForSlackManagedConnectorTerminalActivityAttempt(t, backend, inboundEventID); attempt.Status != runtimepipeline.ActivityAttemptStatusFailed {
		t.Fatalf("%s missing managed credential activity status = %q, want failed", backend.name, attempt.Status)
	}
	if got := countSlackManagedConnectorActivityAttemptsForSource(t, backend, inboundEventID); got != 1 {
		t.Fatalf("%s missing managed credential activity attempts = %d, want one failed claim", backend.name, got)
	}
	requireManagedConnectorFailureEventCountEventually(t, backend, "missing managed credential", inboundEventID, countSlackManagedConnectorFailureEventsForSource)
}

func loadSlackManagedConnectorInboundEventID(t *testing.T, backend slackManagedConnectorBackend, providerEventID string) string {
	return loadNormalizedProviderEventID(t, backend, "inbound.telegram.text_message", providerEventID)
}

func loadNormalizedProviderEventID(t *testing.T, backend slackManagedConnectorBackend, eventName, providerEventID string) string {
	t.Helper()
	var eventID string
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT normalized.event_id
			FROM events normalized
			JOIN events evidence
			  ON evidence.run_id = normalized.run_id
			 AND evidence.event_name = 'platform.inbound_recorded'
			WHERE normalized.run_id = ?
			  AND normalized.entity_id = ?
			  AND normalized.event_name = ?
			  AND json_extract(evidence.payload, '$.provider_event_id') = ?
			  AND EXISTS (
				SELECT 1 FROM json_each(evidence.payload, '$.event_ids') ids
				WHERE ids.value = normalized.event_id
			  )
			ORDER BY normalized.created_at DESC
			LIMIT 1
		`, backend.runID, backend.entityID, eventName, providerEventID).Scan(&eventID)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT normalized.event_id::text
			FROM events normalized
			JOIN events evidence
			  ON evidence.run_id = normalized.run_id
			 AND evidence.event_name = 'platform.inbound_recorded'
			WHERE normalized.run_id = $1::uuid
			  AND normalized.entity_id = $2::uuid
			  AND normalized.event_name = $3
			  AND evidence.payload->>'provider_event_id' = $4
			  AND EXISTS (
				SELECT 1 FROM jsonb_array_elements_text(evidence.payload->'event_ids') ids(event_id)
				WHERE ids.event_id = normalized.event_id::text
			  )
			ORDER BY normalized.created_at DESC
			LIMIT 1
		`, backend.runID, backend.entityID, eventName, providerEventID).Scan(&eventID)
	}
	if err != nil {
		t.Fatalf("%s load normalized event id for %s/%s: %v", backend.name, eventName, providerEventID, err)
	}
	return eventID
}

func waitForSlackManagedConnectorTerminalActivityAttempt(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) runtimepipeline.ActivityAttemptRecord {
	t.Helper()
	deadline := time.Now().Add(connectorSupportedSurfaceAsyncTimeout)
	var last runtimepipeline.ActivityAttemptRecord
	var saw bool
	for {
		rec, ok, err := tryLoadSlackManagedConnectorActivityAttempt(backend, sourceEventID)
		if err != nil {
			t.Fatalf("%s load activity attempt while waiting: %v", backend.name, err)
		}
		if ok {
			last = rec
			saw = true
			if rec.Status != runtimepipeline.ActivityAttemptStatusStarted {
				return rec
			}
		}
		if time.Now().After(deadline) {
			if saw {
				t.Fatalf("%s activity attempt did not reach terminal status; last=%q", backend.name, last.Status)
			}
			t.Fatalf("%s activity attempt for source event %s was not created", backend.name, sourceEventID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func tryLoadSlackManagedConnectorActivityAttempt(backend slackManagedConnectorBackend, sourceEventID string) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	rows, err := runtimeConnectorActivityRowsForSource(backend.ctx, backend.eventStore, backend.runID, "slack.post_message", sourceEventID)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	if len(rows) == 0 {
		return runtimepipeline.ActivityAttemptRecord{}, false, nil
	}
	return backend.activityAttempts.LoadActivityAttempt(backend.ctx, rows[0].RequestEventID)
}

func countSlackManagedConnectorActivityAttempts(t *testing.T, backend slackManagedConnectorBackend) int {
	t.Helper()
	rows, err := runtimeConnectorActivityRows(backend.ctx, backend.eventStore, backend.runID, "slack.post_message")
	if err != nil {
		t.Fatalf("%s read exact activity storage: %v", backend.name, err)
	}
	return len(rows)
}

func countSlackManagedConnectorActivityAttemptsForSource(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) int {
	t.Helper()
	rows, err := runtimeConnectorActivityRowsForSource(backend.ctx, backend.eventStore, backend.runID, "slack.post_message", sourceEventID)
	if err != nil {
		t.Fatalf("%s read exact activity storage: %v", backend.name, err)
	}
	return len(rows)
}

func countSlackManagedConnectorFailureEventsForSource(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) int {
	t.Helper()
	failureEventType := boundedProviderFlowID + "/slack_post_message.failed"
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE run_id = ?
			  AND event_name = ?
			  AND source_event_id = ?
		`, backend.runID, failureEventType, sourceEventID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE run_id = $1::uuid
			  AND event_name = $2
			  AND source_event_id = $3::uuid
		`, backend.runID, failureEventType, sourceEventID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count failure events for source event %s: %v", backend.name, sourceEventID, err)
	}
	return count
}

func requireSlackManagedConnectorResultEventEventually(t *testing.T, backend slackManagedConnectorBackend, eventID, eventType string) {
	t.Helper()
	deadline := time.Now().Add(connectorSupportedSurfaceAsyncTimeout)
	for {
		if slackManagedConnectorEventExists(t, backend, eventID, eventType) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s event row for %s/%s not found", backend.name, eventID, eventType)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func requireManagedConnectorFailureEventCountEventually(t *testing.T, backend slackManagedConnectorBackend, label, sourceEventID string, countFn func(*testing.T, slackManagedConnectorBackend, string) int) {
	t.Helper()
	deadline := time.Now().Add(connectorSupportedSurfaceAsyncTimeout)
	var got int
	for {
		got = countFn(t, backend, sourceEventID)
		if got == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s %s failure events = %d, want 1", backend.name, label, got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func slackManagedConnectorEventExists(t *testing.T, backend slackManagedConnectorBackend, eventID, eventType string) bool {
	t.Helper()
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE event_id = ?
			  AND event_name = ?
		`, eventID, eventType).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE event_id = $1::uuid
			  AND event_name = $2
		`, eventID, eventType).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s require event %s/%s: %v", backend.name, eventID, eventType, err)
	}
	return count == 1
}

func loadSlackManagedConnectorActivityRequestEvent(t *testing.T, backend slackManagedConnectorBackend, eventID string) events.Event {
	t.Helper()
	var raw []byte
	var created time.Time
	var err error
	if backend.sqlite {
		var payload string
		var createdRaw string
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT payload, created_at
			FROM events
			WHERE event_id = ?
			  AND event_name = 'platform.activity_requested'
		`, eventID).Scan(&payload, &createdRaw)
		raw = []byte(payload)
		created, _ = time.Parse(time.RFC3339Nano, createdRaw)
		if created.IsZero() {
			created = time.Now().UTC()
		}
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT payload, created_at
			FROM events
			WHERE event_id = $1::uuid
			  AND event_name = 'platform.activity_requested'
		`, eventID).Scan(&raw, &created)
	}
	if err != nil {
		t.Fatalf("%s load activity request event %s: %v", backend.name, eventID, err)
	}
	return eventtest.PersistedProjection(
		eventID,
		events.EventType("platform.activity_requested"),
		"workflow-runtime",
		"",
		raw,
		2,
		backend.runID,
		"",
		events.EventEnvelope{EntityID: backend.entityID},
		created,
	)
}

func assertSlackManagedConnectorNoStoredSecret(t *testing.T, backend slackManagedConnectorBackend, secret string) {
	t.Helper()
	evidence, err := storetest.ReadConnectorCredentialLeakStorage(backend.ctx, backend.eventStore, backend.runID, secret)
	if err != nil {
		t.Fatalf("%s no-secret storage evidence: %v", backend.name, err)
	}
	if evidence.EventPayloads != 0 || evidence.ActivityResultsOrFailures != 0 {
		t.Fatalf("%s stored secret leaks: events=%d activity_attempts=%d", backend.name, evidence.EventPayloads, evidence.ActivityResultsOrFailures)
	}
}

func slackManagedConnectorString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case json.Number:
		return v.String()
	case float64:
		return fmt.Sprintf("%.0f", v)
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	default:
		return fmt.Sprint(v)
	}
}
