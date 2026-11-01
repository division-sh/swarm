package runtime_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/connectorapi"
)

func TestMicrosoftGraphClientCredentialsConnectorPackRoundTripThroughActivityJournal(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)

		const flowInstance = boundedProviderFlowID
		runID, entityID := boundedInboundTestCoordinates()
		ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)

		runMicrosoftGraphClientCredentialsConnectorSurface(t, slackManagedConnectorBackend{
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
		ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
		sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)

		runMicrosoftGraphClientCredentialsConnectorSurface(t, slackManagedConnectorBackend{
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

type microsoftGraphConnectorCall = connectorapi.Call

func runMicrosoftGraphClientCredentialsConnectorSurface(t *testing.T, backend slackManagedConnectorBackend) {
	t.Helper()
	fake := newFakeMicrosoftGraphConnectorServer(t)
	defer fake.Server.Close()

	managedStore := runtimemanagedcredentials.NewMemoryStore(microsoftGraphClientCredentialsRecord(fake.Server.URL, "expired-token", time.Now().Add(-time.Hour)))
	source := microsoftGraphConnectorSource(t, fake.Server.URL, backend.flowInstance)
	backend.source = source
	backend.ctx, backend.inboundTarget = seedCapturedProviderTarget(t, backend.ctx, backend.inboundStore, source, backend.runID, backend.entityID, backend.flowInstance, "customer-a", "telegram", "telegram-secret", "microsoft-graph-client-credentials-observer")
	bus, pc := startSlackManagedConnectorBusAndCoordinator(t, backend, source, managedStore)
	backend.activityAttempts = pc
	gateway := newCapturedProviderGateway(t, bus, source, backend.inboundStore)
	webhookPath := fmt.Sprintf("/webhooks/%s/telegram", backend.entityID)

	publishTelegramMessageToSlack(t, backend, bus, gateway, webhookPath, "323456789", "send first mail")
	firstInboundEventID := loadSlackManagedConnectorInboundEventID(t, backend, "323456789")
	firstAttempt := waitForMicrosoftGraphTerminalActivityAttempt(t, backend, firstInboundEventID)
	if firstAttempt.Status != runtimepipeline.ActivityAttemptStatusSucceeded {
		t.Fatalf("%s first activity attempt status = %q failure=%#v, want succeeded", backend.name, firstAttempt.Status, firstAttempt.Failure)
	}
	firstCall := fake.requireSideEffectCall(t, backend.name, "client_credentials refresh-before-use")
	if firstCall.Auth != "Bearer graph-fresh-token" {
		t.Fatalf("%s first Graph auth = %q, want Bearer graph-fresh-token", backend.name, firstCall.Auth)
	}
	if got := microsoftGraphConnectorString(firstCall.Body["message"]); !strings.Contains(got, "send first mail") || !strings.Contains(got, "recipient@example.com") {
		t.Fatalf("%s first Graph message = %#v, want subject/body/recipient from activity input", backend.name, firstCall.Body["message"])
	}
	if recipients := microsoftGraphMessageRecipients(firstCall.Body); len(recipients) != 1 {
		t.Fatalf("%s first Graph toRecipients = %#v, want one JSON array recipient", backend.name, microsoftGraphMessageValue(firstCall.Body, "toRecipients"))
	}
	requireSlackManagedConnectorResultEventEventually(t, backend, firstAttempt.ResultEventID, firstAttempt.ResultEventType)
	if got := fake.tokenRequestCount(); got != 1 {
		t.Fatalf("%s client_credentials token requests after refresh-before-use = %d, want 1", backend.name, got)
	}
	if got := fake.refreshTokenValues(); len(got) != 0 {
		t.Fatalf("%s token endpoint received refresh_token values = %#v, want none for client_credentials", backend.name, got)
	}

	publishTelegramMessageToSlack(t, backend, bus, gateway, webhookPath, "323456790", "needs 401 refresh")
	secondInboundEventID := loadSlackManagedConnectorInboundEventID(t, backend, "323456790")
	secondAttempt := waitForMicrosoftGraphTerminalActivityAttempt(t, backend, secondInboundEventID)
	if secondAttempt.Status != runtimepipeline.ActivityAttemptStatusSucceeded {
		t.Fatalf("%s second activity attempt status = %q failure=%#v, want succeeded", backend.name, secondAttempt.Status, secondAttempt.Failure)
	}
	secondCall := fake.requireSideEffectCall(t, backend.name, "client_credentials re-acquire-on-401")
	if secondCall.Auth != "Bearer graph-after-401-token" {
		t.Fatalf("%s second Graph auth = %q, want Bearer graph-after-401-token", backend.name, secondCall.Auth)
	}
	if got := fake.tokenRequestCount(); got != 2 {
		t.Fatalf("%s client_credentials token requests after 401 = %d, want 2", backend.name, got)
	}
	if got := fake.providerHTTPRequestCount(); got != 3 {
		t.Fatalf("%s Graph HTTP requests = %d, want 3 (success, 401, retry success)", backend.name, got)
	}

	requestEvent := loadSlackManagedConnectorActivityRequestEvent(t, backend, secondAttempt.RequestEventID)
	if err := bus.EngineDispatcher().DispatchPostCommit(backend.ctx, []runtimeengine.EmitIntent{{Event: requestEvent}}); err != nil {
		t.Fatalf("%s duplicate activity request dispatch: %v", backend.name, err)
	}
	waitForInboundBusQuiescence(t, bus)
	fake.requireNoSideEffectCall(t, backend.name, "duplicate activity request")
	if got := fake.tokenRequestCount(); got != 2 {
		t.Fatalf("%s client_credentials token requests after duplicate = %d, want still 2", backend.name, got)
	}
	if got := countMicrosoftGraphActivityAttempts(t, backend); got != 2 {
		t.Fatalf("%s activity attempts after duplicate = %d, want 2", backend.name, got)
	}

	publishTelegramMessageToSlack(t, backend, bus, gateway, webhookPath, "323456791", "provider 429 fixture")
	rateLimitInboundEventID := loadSlackManagedConnectorInboundEventID(t, backend, "323456791")
	rateLimitAttempt := waitForMicrosoftGraphTerminalActivityAttempt(t, backend, rateLimitInboundEventID)
	fake.requireNoSideEffectCall(t, backend.name, "429 fixture")
	if rateLimitAttempt.Status != runtimepipeline.ActivityAttemptStatusFailed {
		t.Fatalf("%s 429 activity attempt status = %q, want failed", backend.name, rateLimitAttempt.Status)
	}
	if rateLimitAttempt.Failure == nil || rateLimitAttempt.Failure.Detail.Code != "provider_http_status" || fmt.Sprint(rateLimitAttempt.Failure.Detail.Attributes["status"]) != "429" {
		t.Fatalf("%s 429 attempt failure = %#v, want provider_http_status/429", backend.name, rateLimitAttempt.Failure)
	}
	requireSlackManagedConnectorResultEventEventually(t, backend, rateLimitAttempt.ResultEventID, rateLimitAttempt.ResultEventType)
	waitForMicrosoftGraphFailureEventsForSource(t, backend, rateLimitInboundEventID, 1, "429")

	assertMicrosoftGraphManagedCredentialFailureBeforeDispatch(t, backend, fake, "missing credential", "323456792", runtimemanagedcredentials.NewMemoryStore())
	unconnected := microsoftGraphClientCredentialsRecord(fake.Server.URL, "", time.Now().Add(time.Hour))
	unconnected.Status = runtimemanagedcredentials.StatusUnconnected
	assertMicrosoftGraphManagedCredentialFailureBeforeDispatch(t, backend, fake, "unconnected credential", "323456793", runtimemanagedcredentials.NewMemoryStore(unconnected))
	scopeInsufficient := microsoftGraphClientCredentialsRecord(fake.Server.URL, "scope-mismatch-token", time.Now().Add(time.Hour))
	scopeInsufficient.Scopes = []string{"Mail.Send"}
	assertMicrosoftGraphManagedCredentialFailureBeforeDispatch(t, backend, fake, "scope-insufficient credential", "323456794", runtimemanagedcredentials.NewMemoryStore(scopeInsufficient))

	_ = pc
	for _, secret := range []string{"expired-token", "graph-fresh-token", "graph-after-401-token", "graph-client-secret", "scope-mismatch-token"} {
		assertSlackManagedConnectorNoStoredSecret(t, backend, secret)
	}
}

type fakeMicrosoftGraphConnectorServer struct{ *connectorapi.MicrosoftGraph }

func newFakeMicrosoftGraphConnectorServer(t *testing.T) *fakeMicrosoftGraphConnectorServer {
	t.Helper()
	return &fakeMicrosoftGraphConnectorServer{connectorapi.NewMicrosoftGraph(t)}
}

func (f *fakeMicrosoftGraphConnectorServer) requireSideEffectCall(t *testing.T, backend, context string) microsoftGraphConnectorCall {
	t.Helper()
	select {
	case call := <-f.SideEffects:
		return call
	case <-time.After(20 * time.Second):
		t.Fatalf("%s %s: timed out waiting for fake Microsoft Graph side effect (token_requests=%d provider_requests=%d)", backend, context, f.tokenRequestCount(), f.providerHTTPRequestCount())
		return microsoftGraphConnectorCall{}
	}
}

func (f *fakeMicrosoftGraphConnectorServer) requireNoSideEffectCall(t *testing.T, backend, context string) {
	t.Helper()
	select {
	case call := <-f.SideEffects:
		t.Fatalf("%s %s: unexpected fake Microsoft Graph side effect: auth=%s body=%s", backend, context, call.Auth, call.Raw)
	default:
	}
}

func (f *fakeMicrosoftGraphConnectorServer) tokenRequestCount() int {
	return int(f.TokenCalls.Load())
}

func (f *fakeMicrosoftGraphConnectorServer) providerHTTPRequestCount() int {
	return int(f.Calls.Load())
}

func (f *fakeMicrosoftGraphConnectorServer) refreshTokenValues() []string {
	return append([]string(nil), f.RefreshTokens...)
}

func microsoftGraphClientCredentialsRecord(baseURL, accessToken string, expiresAt time.Time) runtimemanagedcredentials.Record {
	return runtimemanagedcredentials.Record{
		Key:          "microsoft_graph.microsoft_graph_app",
		Provider:     "microsoft_graph",
		GrantType:    runtimemanagedcredentials.GrantClientCredentials,
		TokenURL:     strings.TrimRight(baseURL, "/") + "/tenant/oauth2/v2.0/token",
		ClientID:     "graph-client",
		ClientSecret: "graph-client-secret",
		Scopes:       []string{"https://graph.microsoft.com/.default"},
		GrantModel:   managedcredentialmodel.GrantModelScope,
		TokenRequest: managedcredentialmodel.DefaultTokenRequestProfile(),
		AccessToken:  accessToken,
		Status:       runtimemanagedcredentials.StatusConnected,
		ExpiresAt:    expiresAt,
	}
}

func assertMicrosoftGraphManagedCredentialFailureBeforeDispatch(t *testing.T, backend slackManagedConnectorBackend, fake *fakeMicrosoftGraphConnectorServer, label, updateID string, managedStore runtimemanagedcredentials.Store) {
	t.Helper()
	tokenRequestsBefore := fake.tokenRequestCount()
	graphRequestsBefore := fake.providerHTTPRequestCount()
	source := backend.source
	bus, pc := startSlackManagedConnectorBusAndCoordinator(t, backend, source, managedStore)
	backend.activityAttempts = pc
	gateway := newCapturedProviderGateway(t, bus, source, backend.inboundStore)
	webhookPath := fmt.Sprintf("/webhooks/%s/telegram", backend.entityID)
	publishTelegramMessageToSlack(t, backend, bus, gateway, webhookPath, updateID, label)
	inboundEventID := loadSlackManagedConnectorInboundEventID(t, backend, updateID)
	fake.requireNoSideEffectCall(t, backend.name, label)
	if got := fake.tokenRequestCount(); got != tokenRequestsBefore {
		t.Fatalf("%s %s token requests = %d, want unchanged %d", backend.name, label, got, tokenRequestsBefore)
	}
	if got := fake.providerHTTPRequestCount(); got != graphRequestsBefore {
		t.Fatalf("%s %s Graph HTTP requests = %d, want unchanged %d", backend.name, label, got, graphRequestsBefore)
	}
	if attempt := waitForMicrosoftGraphTerminalActivityAttempt(t, backend, inboundEventID); attempt.Status != runtimepipeline.ActivityAttemptStatusFailed {
		t.Fatalf("%s %s activity status = %q, want failed", backend.name, label, attempt.Status)
	}
	if got := countMicrosoftGraphActivityAttemptsForSource(t, backend, inboundEventID); got != 1 {
		t.Fatalf("%s %s activity attempts = %d, want one failed claim", backend.name, label, got)
	}
	waitForMicrosoftGraphFailureEventsForSource(t, backend, inboundEventID, 1, label)
}

func waitForMicrosoftGraphTerminalActivityAttempt(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) runtimepipeline.ActivityAttemptRecord {
	t.Helper()
	deadline := time.Now().Add(connectorSupportedSurfaceAsyncTimeout)
	var last runtimepipeline.ActivityAttemptRecord
	var saw bool
	for {
		rec, ok, err := tryLoadMicrosoftGraphActivityAttempt(backend, sourceEventID)
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

func tryLoadMicrosoftGraphActivityAttempt(backend slackManagedConnectorBackend, sourceEventID string) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	rows, err := runtimeConnectorActivityRowsForSource(backend.ctx, backend.eventStore, backend.runID, "microsoft_graph.send_mail", sourceEventID)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	if len(rows) == 0 {
		return runtimepipeline.ActivityAttemptRecord{}, false, nil
	}
	return backend.activityAttempts.LoadActivityAttempt(backend.ctx, rows[0].RequestEventID)
}

func countMicrosoftGraphActivityAttempts(t *testing.T, backend slackManagedConnectorBackend) int {
	t.Helper()
	rows, err := runtimeConnectorActivityRows(backend.ctx, backend.eventStore, backend.runID, "microsoft_graph.send_mail")
	if err != nil {
		t.Fatalf("%s read exact activity storage: %v", backend.name, err)
	}
	return len(rows)
}

func countMicrosoftGraphActivityAttemptsForSource(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) int {
	t.Helper()
	rows, err := runtimeConnectorActivityRowsForSource(backend.ctx, backend.eventStore, backend.runID, "microsoft_graph.send_mail", sourceEventID)
	if err != nil {
		t.Fatalf("%s read exact activity storage: %v", backend.name, err)
	}
	return len(rows)
}

func countMicrosoftGraphFailureEventsForSource(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) int {
	t.Helper()
	failureEventType := boundedProviderFlowID + "/microsoft_graph_send_mail.failed"
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

func waitForMicrosoftGraphFailureEventsForSource(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string, want int, label string) {
	t.Helper()
	deadline := time.Now().Add(connectorSupportedSurfaceAsyncTimeout)
	var got int
	for {
		got = countMicrosoftGraphFailureEventsForSource(t, backend, sourceEventID)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s %s failure events for source %s = %d, want %d", backend.name, label, sourceEventID, got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func microsoftGraphMessageRecipients(body map[string]any) []any {
	recipients, _ := microsoftGraphMessageValue(body, "toRecipients").([]any)
	return recipients
}

func microsoftGraphMessageValue(body map[string]any, key string) any {
	message, _ := body["message"].(map[string]any)
	if message == nil {
		return nil
	}
	return message[key]
}

func microsoftGraphConnectorString(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(raw)
}
