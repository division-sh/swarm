package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestMicrosoftGraphClientCredentialsConnectorPackRoundTripThroughActivityJournal(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)

		const (
			runID        = "8c000000-0000-0000-0000-000000000001"
			entityID     = "8c000000-0000-0000-0000-000000000002"
			flowInstance = "microsoft-graph-connector-client-credentials-pg"
		)
		ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
		seedPostgresInboundGatewayRuntime(t, ctx, db, pg, runID, entityID, flowInstance, "customer-a", "telegram", "telegram-secret", "microsoft-graph-client-credentials-observer")
		seedTelegramConnectorSupportedSurfaceWorkflowVersion(t, ctx, db, flowInstance, false)

		runMicrosoftGraphClientCredentialsConnectorSurface(t, slackManagedConnectorBackend{
			name:          "postgres",
			ctx:           ctx,
			db:            db,
			eventStore:    pg,
			deliveryStore: pg,
			inboundStore:  pg,
			workflowStore: workflowStore,
			runID:         runID,
			entityID:      entityID,
			flowInstance:  flowInstance,
			sqlite:        false,
		})
	})

	t.Run("sqlite", func(t *testing.T) {
		const (
			runID        = "8d000000-0000-0000-0000-000000000001"
			entityID     = "8d000000-0000-0000-0000-000000000002"
			flowInstance = "microsoft-graph-connector-client-credentials-sqlite"
		)
		ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
		sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
		workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore)
		seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, "customer-a", "telegram", "telegram-secret", "microsoft-graph-client-credentials-observer")
		seedTelegramConnectorSupportedSurfaceWorkflowVersion(t, ctx, sqliteStore.DB, flowInstance, true)

		runMicrosoftGraphClientCredentialsConnectorSurface(t, slackManagedConnectorBackend{
			name:          "sqlite",
			ctx:           ctx,
			db:            sqliteStore.DB,
			eventStore:    sqliteStore,
			deliveryStore: sqliteStore,
			inboundStore:  sqliteStore,
			workflowStore: workflowStore,
			runID:         runID,
			entityID:      entityID,
			flowInstance:  flowInstance,
			sqlite:        true,
		})
	})
}

type microsoftGraphConnectorCall struct {
	auth string
	body map[string]any
	raw  string
}

func runMicrosoftGraphClientCredentialsConnectorSurface(t *testing.T, backend slackManagedConnectorBackend) {
	t.Helper()
	fake := newFakeMicrosoftGraphConnectorServer(t)
	defer fake.server.Close()

	managedStore := runtimemanagedcredentials.NewMemoryStore(microsoftGraphClientCredentialsRecord(fake.server.URL, "expired-token", time.Now().Add(-time.Hour)))
	source := microsoftGraphConnectorSource(t, fake.server.URL, backend.flowInstance)
	bus, pc := startSlackManagedConnectorBusAndCoordinator(t, backend, source, managedStore)
	gateway := newTestInboundGateway(t, bus, nil, nil, backend.inboundStore)
	webhookPath := fmt.Sprintf("/webhooks/%s/telegram", backend.entityID)

	publishTelegramMessageToSlack(t, backend, bus, gateway, webhookPath, "323456789", "send first mail")
	firstInboundEventID := loadSlackManagedConnectorInboundEventID(t, backend, "323456789")
	firstAttempt := waitForMicrosoftGraphTerminalActivityAttempt(t, backend, firstInboundEventID)
	if firstAttempt.Status != runtimepipeline.ActivityAttemptStatusSucceeded {
		t.Fatalf("%s first activity attempt status = %q failure=%#v, want succeeded", backend.name, firstAttempt.Status, firstAttempt.Failure)
	}
	firstCall := fake.requireSideEffectCall(t, backend.name, "client_credentials refresh-before-use")
	if firstCall.auth != "Bearer graph-fresh-token" {
		t.Fatalf("%s first Graph auth = %q, want Bearer graph-fresh-token", backend.name, firstCall.auth)
	}
	if got := microsoftGraphConnectorString(firstCall.body["message"]); !strings.Contains(got, "send first mail") || !strings.Contains(got, "recipient@example.com") {
		t.Fatalf("%s first Graph message = %#v, want subject/body/recipient from activity input", backend.name, firstCall.body["message"])
	}
	if recipients := microsoftGraphMessageRecipients(firstCall.body); len(recipients) != 1 {
		t.Fatalf("%s first Graph toRecipients = %#v, want one JSON array recipient", backend.name, microsoftGraphMessageValue(firstCall.body, "toRecipients"))
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
	if secondCall.auth != "Bearer graph-after-401-token" {
		t.Fatalf("%s second Graph auth = %q, want Bearer graph-after-401-token", backend.name, secondCall.auth)
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
	unconnected := microsoftGraphClientCredentialsRecord(fake.server.URL, "", time.Now().Add(time.Hour))
	unconnected.Status = runtimemanagedcredentials.StatusUnconnected
	assertMicrosoftGraphManagedCredentialFailureBeforeDispatch(t, backend, fake, "unconnected credential", "323456793", runtimemanagedcredentials.NewMemoryStore(unconnected))
	scopeInsufficient := microsoftGraphClientCredentialsRecord(fake.server.URL, "scope-mismatch-token", time.Now().Add(time.Hour))
	scopeInsufficient.Scopes = []string{"Mail.Send"}
	assertMicrosoftGraphManagedCredentialFailureBeforeDispatch(t, backend, fake, "scope-insufficient credential", "323456794", runtimemanagedcredentials.NewMemoryStore(scopeInsufficient))

	_ = pc
	for _, secret := range []string{"expired-token", "graph-fresh-token", "graph-after-401-token", "graph-client-secret", "scope-mismatch-token"} {
		assertSlackManagedConnectorNoStoredSecret(t, backend, secret)
	}
}

type fakeMicrosoftGraphConnectorServer struct {
	server        *httptest.Server
	tokenCalls    atomic.Int64
	graphCalls    atomic.Int64
	refreshTokens []string
	sideEffects   chan microsoftGraphConnectorCall
}

func newFakeMicrosoftGraphConnectorServer(t *testing.T) *fakeMicrosoftGraphConnectorServer {
	t.Helper()
	fake := &fakeMicrosoftGraphConnectorServer{sideEffects: make(chan microsoftGraphConnectorCall, 8)}
	mux := http.NewServeMux()
	mux.HandleFunc("/tenant/oauth2/v2.0/token", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		if got := r.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/x-www-form-urlencoded") {
			http.Error(w, "bad content type", http.StatusBadRequest)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if got := r.Form.Get("grant_type"); got != "client_credentials" {
			http.Error(w, "unexpected grant_type "+got, http.StatusBadRequest)
			return
		}
		if got := r.Form.Get("client_id"); got != "graph-client" {
			http.Error(w, "unexpected client_id", http.StatusBadRequest)
			return
		}
		if got := r.Form.Get("client_secret"); got != "graph-client-secret" {
			http.Error(w, "unexpected client_secret", http.StatusBadRequest)
			return
		}
		if got := r.Form.Get("scope"); got != "https://graph.microsoft.com/.default" {
			http.Error(w, "unexpected scope "+got, http.StatusBadRequest)
			return
		}
		if refresh := strings.TrimSpace(r.Form.Get("refresh_token")); refresh != "" {
			fake.refreshTokens = append(fake.refreshTokens, refresh)
			http.Error(w, "client_credentials must not send refresh_token", http.StatusBadRequest)
			return
		}
		call := fake.tokenCalls.Add(1)
		access := "graph-fresh-token"
		if call >= 2 {
			access = "graph-after-401-token"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": access,
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	})
	mux.HandleFunc("/v1.0/users/user@example.com/sendMail", func(w http.ResponseWriter, r *http.Request) {
		call := fake.graphCalls.Add(1)
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if recipients := microsoftGraphMessageRecipients(body); len(recipients) != 1 {
			http.Error(w, "toRecipients must be a JSON array", http.StatusBadRequest)
			return
		}
		auth := r.Header.Get("Authorization")
		subject := microsoftGraphMessageSubject(body)
		if auth == "Bearer graph-fresh-token" && subject == "needs 401 refresh" {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "InvalidAuthenticationToken", "message": "token expired"}})
			return
		}
		if auth != "Bearer graph-fresh-token" && auth != "Bearer graph-after-401-token" {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "InvalidAuthenticationToken", "message": "bad token"}})
			return
		}
		if subject == "provider 429 fixture" {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "TooManyRequests", "message": "slow down"}})
			return
		}
		if call < 1 {
			http.Error(w, "unreachable", http.StatusInternalServerError)
			return
		}
		fake.sideEffects <- microsoftGraphConnectorCall{auth: auth, body: body, raw: string(raw)}
		w.WriteHeader(http.StatusAccepted)
	})
	fake.server = httptest.NewServer(mux)
	return fake
}

func (f *fakeMicrosoftGraphConnectorServer) requireSideEffectCall(t *testing.T, backend, context string) microsoftGraphConnectorCall {
	t.Helper()
	select {
	case call := <-f.sideEffects:
		return call
	case <-time.After(20 * time.Second):
		t.Fatalf("%s %s: timed out waiting for fake Microsoft Graph side effect (token_requests=%d provider_requests=%d)", backend, context, f.tokenRequestCount(), f.providerHTTPRequestCount())
		return microsoftGraphConnectorCall{}
	}
}

func (f *fakeMicrosoftGraphConnectorServer) requireNoSideEffectCall(t *testing.T, backend, context string) {
	t.Helper()
	select {
	case call := <-f.sideEffects:
		t.Fatalf("%s %s: unexpected fake Microsoft Graph side effect: auth=%s body=%s", backend, context, call.auth, call.raw)
	default:
	}
}

func (f *fakeMicrosoftGraphConnectorServer) tokenRequestCount() int {
	return int(f.tokenCalls.Load())
}

func (f *fakeMicrosoftGraphConnectorServer) providerHTTPRequestCount() int {
	return int(f.graphCalls.Load())
}

func (f *fakeMicrosoftGraphConnectorServer) refreshTokenValues() []string {
	return append([]string(nil), f.refreshTokens...)
}

func microsoftGraphClientCredentialsRecord(baseURL, accessToken string, expiresAt time.Time) runtimemanagedcredentials.Record {
	return runtimemanagedcredentials.Record{
		Key:          "microsoft_graph_app",
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

func microsoftGraphConnectorSource(t *testing.T, baseURL, flowInstance string) semanticview.Source {
	t.Helper()
	handler := runtimecontracts.SystemNodeEventHandler{
		Activity: runtimecontracts.ActivitySpec{
			ID:   "microsoft_graph_send_mail",
			Tool: "microsoft_graph.send_mail",
			Input: map[string]runtimecontracts.ExpressionValue{
				"user_id": runtimecontracts.LiteralExpression("user@example.com"),
				"to_recipients": runtimecontracts.LiteralExpression([]map[string]any{
					{
						"emailAddress": map[string]any{
							"address": "recipient@example.com",
						},
					},
				}),
				"subject": runtimecontracts.CELExpression("payload.payload.message.text"),
				"content": runtimecontracts.CELExpression("payload.payload.message.text"),
			},
		},
	}
	const nodeID = "microsoft-graph-responder"
	node := runtimecontracts.SystemNodeContract{
		ID:            nodeID,
		ExecutionType: runtimecontracts.SystemNodeExecutionType,
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			"inbound.telegram": handler,
		},
	}
	base := semanticview.Wrap(boundedStandingConnectorBundle(flowInstance, &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{
					Events: []string{"inbound.telegram"},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{nodeID: node},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "microsoft_graph_connector_client_credentials_supported_surface",
			Version: "1.0.0",
			EffectiveNodes: map[string]runtimecontracts.SystemNodeEffectiveSemantics{
				nodeID: {
					ID:                   nodeID,
					ExecutionType:        runtimecontracts.SystemNodeExecutionType,
					RuntimeSubscriptions: []string{"inbound.telegram"},
				},
			},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				nodeID: {"inbound.telegram": handler},
			},
			EventOwners: map[string][]string{
				"inbound.telegram": {nodeID},
			},
		},
	}))
	importSource := slackManagedConnectorPackImportSource{
		Source: base,
		projectScopes: []semanticview.ProjectScope{
			{
				Key: ".",
				Manifest: runtimecontracts.ProjectPackageDocument{
					ConnectorPacks: runtimecontracts.ConnectorPackImports{
						Imports: []runtimecontracts.ConnectorPackImport{{Provider: "microsoft_graph", Tool: "microsoft_graph.send_mail"}},
					},
				},
			},
		},
	}
	source, err := providerconnectors.SourceWithConnectorPackImportsFromRegistry(importSource, microsoftGraphConnectorPackRegistry(t, baseURL))
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImportsFromRegistry: %v", err)
	}
	return source
}

func microsoftGraphConnectorPackRegistry(t *testing.T, baseURL string) *providerconnectors.PackRegistry {
	t.Helper()
	tool, ok := providerconnectors.BuiltinTool("microsoft_graph", "microsoft_graph.send_mail")
	if !ok {
		t.Fatal("provider connector pack microsoft_graph.send_mail not found")
	}
	if tool.HTTP == nil {
		t.Fatal("provider connector pack microsoft_graph.send_mail missing http block")
	}
	httpSpec := *tool.HTTP
	tool.HTTP = &httpSpec
	tool.HTTP.URL = strings.TrimRight(baseURL, "/") + "/v1.0/users/{{input.user_id}}/sendMail"
	registry, err := providerconnectors.NewPackRegistry(providerconnectors.LoadedPack{
		Envelope: packs.Envelope{
			ID: "provider.microsoft_graph.connector",
			Provenance: packs.Provenance{
				Source: packs.ProvenancePlatform,
			},
		},
		Manifest: providerconnectors.ConnectorManifest{
			Provider: "microsoft_graph",
			Tools: map[string]runtimecontracts.ToolSchemaEntry{
				"microsoft_graph.send_mail": tool,
			},
		},
		Source: "test:provider.microsoft_graph.connector",
	})
	if err != nil {
		t.Fatalf("NewPackRegistry: %v", err)
	}
	return registry
}

func assertMicrosoftGraphManagedCredentialFailureBeforeDispatch(t *testing.T, backend slackManagedConnectorBackend, fake *fakeMicrosoftGraphConnectorServer, label, updateID string, managedStore runtimemanagedcredentials.Store) {
	t.Helper()
	tokenRequestsBefore := fake.tokenRequestCount()
	graphRequestsBefore := fake.providerHTTPRequestCount()
	source := microsoftGraphConnectorSource(t, fake.server.URL, backend.flowInstance)
	bus, _ := startSlackManagedConnectorBusAndCoordinator(t, backend, source, managedStore)
	gateway := newTestInboundGateway(t, bus, nil, nil, backend.inboundStore)
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
	var requestEventID string
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT request_event_id
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = 'microsoft_graph.send_mail'
			  AND source_event_id = ?
			ORDER BY started_at ASC
			LIMIT 1
		`, backend.runID, sourceEventID).Scan(&requestEventID)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT request_event_id::text
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = 'microsoft_graph.send_mail'
			  AND source_event_id = $2::uuid
			ORDER BY started_at ASC
			LIMIT 1
		`, backend.runID, sourceEventID).Scan(&requestEventID)
	}
	if err == sql.ErrNoRows {
		return runtimepipeline.ActivityAttemptRecord{}, false, nil
	}
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	rec, ok, err := backend.workflowStore.LoadActivityAttempt(backend.ctx, requestEventID)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	return rec, ok, nil
}

func countMicrosoftGraphActivityAttempts(t *testing.T, backend slackManagedConnectorBackend) int {
	t.Helper()
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = 'microsoft_graph.send_mail'
		`, backend.runID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = 'microsoft_graph.send_mail'
		`, backend.runID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count activity attempts: %v", backend.name, err)
	}
	return count
}

func countMicrosoftGraphActivityAttemptsForSource(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) int {
	t.Helper()
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = 'microsoft_graph.send_mail'
			  AND source_event_id = ?
		`, backend.runID, sourceEventID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = 'microsoft_graph.send_mail'
			  AND source_event_id = $2::uuid
		`, backend.runID, sourceEventID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count activity attempts for source event %s: %v", backend.name, sourceEventID, err)
	}
	return count
}

func countMicrosoftGraphFailureEventsForSource(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) int {
	t.Helper()
	failureEventType := boundedProviderFlowID + ".microsoft_graph_send_mail.failed"
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

func microsoftGraphMessageSubject(body map[string]any) string {
	return slackManagedConnectorString(microsoftGraphMessageValue(body, "subject"))
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
