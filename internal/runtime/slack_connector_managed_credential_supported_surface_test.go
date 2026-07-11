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

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestSlackManagedCredentialConnectorPackRoundTripThroughActivityJournal(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)

		const (
			runID        = "7a000000-0000-0000-0000-000000000001"
			entityID     = "7a000000-0000-0000-0000-000000000002"
			flowInstance = "slack-connector-managed-credential-pg"
		)
		ctx := runtimecorrelation.WithRunID(context.Background(), runID)
		pg := &store.PostgresStore{DB: db}
		workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
		seedPostgresInboundGatewayRuntime(t, ctx, db, pg, runID, entityID, flowInstance, "customer-a", "telegram", "telegram-secret", "slack-managed-credential-observer")
		seedTelegramConnectorSupportedSurfaceWorkflowVersion(t, ctx, db, flowInstance, false)

		runSlackManagedCredentialConnectorSurface(t, slackManagedConnectorBackend{
			name:          "postgres",
			ctx:           ctx,
			db:            db,
			eventStore:    pg,
			inboundStore:  pg,
			workflowStore: workflowStore,
			runID:         runID,
			entityID:      entityID,
			sqlite:        false,
		})
	})

	t.Run("sqlite", func(t *testing.T) {
		const (
			runID        = "7b000000-0000-0000-0000-000000000001"
			entityID     = "7b000000-0000-0000-0000-000000000002"
			flowInstance = "slack-connector-managed-credential-sqlite"
		)
		ctx := runtimecorrelation.WithRunID(context.Background(), runID)
		sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
		workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore)
		seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, "customer-a", "telegram", "telegram-secret", "slack-managed-credential-observer")
		seedTelegramConnectorSupportedSurfaceWorkflowVersion(t, ctx, sqliteStore.DB, flowInstance, true)

		runSlackManagedCredentialConnectorSurface(t, slackManagedConnectorBackend{
			name:          "sqlite",
			ctx:           ctx,
			db:            sqliteStore.DB,
			eventStore:    sqliteStore,
			inboundStore:  sqliteStore,
			workflowStore: workflowStore,
			runID:         runID,
			entityID:      entityID,
			sqlite:        true,
		})
	})
}

type slackManagedConnectorBackend struct {
	name          string
	ctx           context.Context
	db            *sql.DB
	eventStore    runtimebus.EventStore
	inboundStore  runtimepkg.InboundPersistence
	workflowStore *runtimepipeline.WorkflowInstanceStore
	runID         string
	entityID      string
	sqlite        bool
}

type slackManagedConnectorCall struct {
	auth string
	body map[string]any
	raw  string
}

func runSlackManagedCredentialConnectorSurface(t *testing.T, backend slackManagedConnectorBackend) {
	t.Helper()
	fake := newFakeSlackManagedConnectorServer(t)
	defer fake.server.Close()

	managedStore := runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
		Key:          "slack_oauth",
		Provider:     "slack",
		GrantType:    runtimemanagedcredentials.GrantAuthorizationCodePKCE,
		TokenURL:     fake.server.URL + "/oauth.v2.access",
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		Scopes:       []string{"chat:write"},
		AccessToken:  "expired-token",
		RefreshToken: "refresh-secret",
		Status:       runtimemanagedcredentials.StatusConnected,
		ExpiresAt:    time.Now().Add(-time.Hour),
	})
	source := slackManagedConnectorSource(t, fake.server.URL)
	bus, pc := startSlackManagedConnectorBusAndCoordinator(t, backend, source, managedStore)
	gateway := newTestInboundGateway(t, bus, nil, nil, backend.inboundStore)
	webhookPath := fmt.Sprintf("/webhooks/%s/telegram", backend.entityID)

	publishTelegramMessageToSlack(t, backend, bus, gateway, webhookPath, "123456789", "hello from telegram")
	firstCall := fake.requireSideEffectCall(t, backend.name, "refresh-before-use")
	if firstCall.auth != "Bearer fresh-token" {
		t.Fatalf("%s first Slack auth = %q, want Bearer fresh-token", backend.name, firstCall.auth)
	}
	if got := slackManagedConnectorString(firstCall.body["channel"]); got != "C123" {
		t.Fatalf("%s first Slack channel = %#v, want C123", backend.name, firstCall.body["channel"])
	}
	if got := slackManagedConnectorString(firstCall.body["text"]); got != "hello from telegram" {
		t.Fatalf("%s first Slack text = %#v, want inbound text", backend.name, firstCall.body["text"])
	}
	firstInboundEventID := loadSlackManagedConnectorInboundEventID(t, backend, "123456789")
	firstAttempt := waitForSlackManagedConnectorTerminalActivityAttempt(t, backend, firstInboundEventID)
	if firstAttempt.Status != runtimepipeline.ActivityAttemptStatusSucceeded {
		t.Fatalf("%s first activity attempt status = %q, want succeeded", backend.name, firstAttempt.Status)
	}
	requireSlackManagedConnectorResultEventEventually(t, backend, firstAttempt.ResultEventID, firstAttempt.ResultEventType)
	if got := fake.refreshCount(); got != 1 {
		t.Fatalf("%s refresh-before-use token refreshes = %d, want 1", backend.name, got)
	}

	publishTelegramMessageToSlack(t, backend, bus, gateway, webhookPath, "123456790", "needs 401 refresh")
	secondCall := fake.requireSideEffectCall(t, backend.name, "refresh-on-401")
	if secondCall.auth != "Bearer after-401-token" {
		t.Fatalf("%s second Slack auth = %q, want Bearer after-401-token", backend.name, secondCall.auth)
	}
	secondInboundEventID := loadSlackManagedConnectorInboundEventID(t, backend, "123456790")
	secondAttempt := waitForSlackManagedConnectorTerminalActivityAttempt(t, backend, secondInboundEventID)
	if secondAttempt.Status != runtimepipeline.ActivityAttemptStatusSucceeded {
		t.Fatalf("%s second activity attempt status = %q, want succeeded", backend.name, secondAttempt.Status)
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
	fake.requireNoSideEffectCall(t, backend.name, "response_success ok false")
	okFalseInboundEventID := loadSlackManagedConnectorInboundEventID(t, backend, "123456792")
	okFalseAttempt := waitForSlackManagedConnectorTerminalActivityAttempt(t, backend, okFalseInboundEventID)
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

	assertSlackManagedConnectorMissingCredential(t, backend, fake.server.URL)
	_ = pc
	for _, secret := range []string{"expired-token", "fresh-token", "after-401-token", "refresh-secret", "client-secret"} {
		assertSlackManagedConnectorNoStoredSecret(t, backend, secret)
	}
}

type fakeSlackManagedConnectorServer struct {
	server      *httptest.Server
	tokenCalls  atomic.Int64
	slackCalls  atomic.Int64
	sideEffects chan slackManagedConnectorCall
}

func newFakeSlackManagedConnectorServer(t *testing.T) *fakeSlackManagedConnectorServer {
	t.Helper()
	fake := &fakeSlackManagedConnectorServer{sideEffects: make(chan slackManagedConnectorCall, 8)}
	mux := http.NewServeMux()
	mux.HandleFunc("/oauth.v2.access", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if got := r.Form.Get("grant_type"); got != "refresh_token" {
			http.Error(w, "unexpected grant_type "+got, http.StatusBadRequest)
			return
		}
		if got := r.Form.Get("refresh_token"); got != "refresh-secret" {
			http.Error(w, "unexpected refresh token", http.StatusBadRequest)
			return
		}
		call := fake.tokenCalls.Add(1)
		token := "fresh-token"
		if call >= 2 {
			token = "after-401-token"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  token,
			"refresh_token": "refresh-secret",
			"token_type":    "Bearer",
			"expires_in":    3600,
			"scope":         "chat:write",
		})
	})
	mux.HandleFunc("/api/chat.postMessage", func(w http.ResponseWriter, r *http.Request) {
		fake.slackCalls.Add(1)
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		auth := r.Header.Get("Authorization")
		text := slackManagedConnectorString(body["text"])
		if auth == "Bearer fresh-token" && text == "needs 401 refresh" {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "token_expired"})
			return
		}
		if auth != "Bearer fresh-token" && auth != "Bearer after-401-token" {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "bad_token"})
			return
		}
		if text == "provider ok false" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "channel_not_found"})
			return
		}
		call := slackManagedConnectorCall{auth: auth, body: body, raw: string(raw)}
		fake.sideEffects <- call
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":      true,
			"channel": body["channel"],
			"message": map[string]any{
				"text": body["text"],
			},
		})
	})
	fake.server = httptest.NewServer(mux)
	return fake
}

func (f *fakeSlackManagedConnectorServer) requireSideEffectCall(t *testing.T, backend, context string) slackManagedConnectorCall {
	t.Helper()
	select {
	case call := <-f.sideEffects:
		return call
	case <-time.After(5 * time.Second):
		t.Fatalf("%s %s: timed out waiting for fake Slack side effect", backend, context)
		return slackManagedConnectorCall{}
	}
}

func (f *fakeSlackManagedConnectorServer) requireNoSideEffectCall(t *testing.T, backend, context string) {
	t.Helper()
	select {
	case call := <-f.sideEffects:
		t.Fatalf("%s %s: unexpected fake Slack side effect: auth=%s body=%s", backend, context, call.auth, call.raw)
	default:
	}
}

func (f *fakeSlackManagedConnectorServer) refreshCount() int {
	return int(f.tokenCalls.Load())
}

func (f *fakeSlackManagedConnectorServer) providerHTTPRequestCount() int {
	return int(f.slackCalls.Load())
}

func publishTelegramMessageToSlack(t *testing.T, backend slackManagedConnectorBackend, bus *runtimebus.EventBus, gateway *runtimepkg.InboundGateway, webhookPath, updateID, text string) {
	t.Helper()
	body := []byte(fmt.Sprintf(`{"update_id":%s,"message":{"message_id":7,"chat":{"id":42},"text":%q}}`, updateID, text))
	req := newSignedTelegramRequest(webhookPath, "telegram-secret", body).WithContext(backend.ctx)
	rec := httptest.NewRecorder()
	gateway.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("%s gateway status for update %s = %d, want 202 body=%s", backend.name, updateID, rec.Code, rec.Body.String())
	}
	waitForInboundBusQuiescence(t, bus)
}

func slackManagedConnectorSource(t *testing.T, baseURL string) semanticview.Source {
	t.Helper()
	handler := runtimecontracts.SystemNodeEventHandler{
		Activity: runtimecontracts.ActivitySpec{
			ID:   "slack_post_message",
			Tool: "slack.post_message",
			Input: map[string]runtimecontracts.ExpressionValue{
				"channel": runtimecontracts.CELExpression(`"C123"`),
				"text":    runtimecontracts.CELExpression("payload.payload.message.text"),
			},
		},
	}
	const nodeID = "slack-responder"
	node := runtimecontracts.SystemNodeContract{
		ID:            nodeID,
		ExecutionType: runtimecontracts.SystemNodeExecutionType,
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			"inbound.telegram": handler,
		},
	}
	base := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{
					Events: []string{"inbound.telegram"},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{nodeID: node},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "slack_connector_managed_credential_supported_surface",
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
	})
	importSource := slackManagedConnectorPackImportSource{
		Source: base,
		projectScopes: []semanticview.ProjectScope{
			{
				Key: ".",
				Manifest: runtimecontracts.ProjectPackageDocument{
					ConnectorPacks: runtimecontracts.ConnectorPackImports{
						Imports: []runtimecontracts.ConnectorPackImport{{Provider: "slack", Tool: "slack.post_message"}},
					},
				},
			},
		},
	}
	source, err := providerconnectors.SourceWithConnectorPackImportsFromRegistry(importSource, slackManagedConnectorPackRegistry(t, baseURL))
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImportsFromRegistry: %v", err)
	}
	return source
}

func slackManagedConnectorPackRegistry(t *testing.T, baseURL string) *providerconnectors.PackRegistry {
	t.Helper()
	tool, ok := providerconnectors.BuiltinTool("slack", "slack.post_message")
	if !ok {
		t.Fatal("provider connector pack slack.post_message not found")
	}
	if tool.HTTP == nil {
		t.Fatal("provider connector pack slack.post_message missing http block")
	}
	httpSpec := *tool.HTTP
	tool.HTTP = &httpSpec
	tool.HTTP.URL = strings.TrimRight(baseURL, "/") + "/api/chat.postMessage"
	registry, err := providerconnectors.NewPackRegistry(providerconnectors.LoadedPack{
		Envelope: packs.Envelope{
			ID: "provider.slack.connector",
			Provenance: packs.Provenance{
				Source: packs.ProvenancePlatform,
			},
		},
		Manifest: providerconnectors.ConnectorManifest{
			Provider: "slack",
			Tools: map[string]runtimecontracts.ToolSchemaEntry{
				"slack.post_message": tool,
			},
		},
		Source: "test:provider.slack.connector",
	})
	if err != nil {
		t.Fatalf("NewPackRegistry: %v", err)
	}
	return registry
}

type slackManagedConnectorPackImportSource struct {
	semanticview.Source
	projectScopes []semanticview.ProjectScope
}

func (s slackManagedConnectorPackImportSource) BaseSemanticSource() semanticview.Source {
	return s.Source
}

func (s slackManagedConnectorPackImportSource) ProjectScopes() []semanticview.ProjectScope {
	return append([]semanticview.ProjectScope(nil), s.projectScopes...)
}

type slackManagedConnectorModule struct {
	source  semanticview.Source
	nodes   []runtimepipeline.WorkflowNode
	guards  runtimepipeline.GuardRegistry
	actions runtimepipeline.ActionRegistry
}

func (m slackManagedConnectorModule) SemanticSource() semanticview.Source {
	return m.source
}

func (m slackManagedConnectorModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return nil
}

func (m slackManagedConnectorModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.nodes...)
}

func (m slackManagedConnectorModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return m.guards
}

func (m slackManagedConnectorModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return m.actions
}

func startSlackManagedConnectorBusAndCoordinator(t *testing.T, backend slackManagedConnectorBackend, source semanticview.Source, managedStore runtimemanagedcredentials.Store) (*runtimebus.EventBus, *runtimepipeline.PipelineCoordinator) {
	t.Helper()
	var pc *runtimepipeline.PipelineCoordinator
	bus, err := runtimebus.NewEventBusWithOptions(backend.eventStore, runtimebus.EventBusOptions{
		ContractBundle: source,
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
	})
	if err != nil {
		t.Fatalf("%s NewEventBusWithOptions: %v", backend.name, err)
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("%s LoadWorkflowNodes: %v", backend.name, err)
	}
	module := slackManagedConnectorModule{
		source:  source,
		nodes:   nodes,
		guards:  runtimepipeline.NewContractGuardRegistry(source),
		actions: runtimepipeline.NewContractActionRegistry(source),
	}
	pc = runtimepipeline.NewPipelineCoordinatorWithOptions(bus, backend.db, runtimepipeline.PipelineCoordinatorOptions{
		Module:             module,
		WorkflowStore:      backend.workflowStore,
		ManagedCredentials: managedStore,
		EventReceiptsCapability: func(context.Context) (bool, error) {
			return true, nil
		},
	})
	subscribed := make(chan struct{}, 1)
	pc.SetTestSubscribeHook(func() {
		select {
		case subscribed <- struct{}{}:
		default:
		}
	})
	runCtx, cancel := context.WithCancel(backend.ctx)
	t.Cleanup(cancel)
	go pc.Run(runCtx)
	select {
	case <-subscribed:
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline coordinator did not subscribe")
	}
	return bus, pc
}

func assertSlackManagedConnectorMissingCredential(t *testing.T, backend slackManagedConnectorBackend, baseURL string) {
	t.Helper()
	source := slackManagedConnectorSource(t, baseURL)
	bus, _ := startSlackManagedConnectorBusAndCoordinator(t, backend, source, runtimemanagedcredentials.NewMemoryStore())
	gateway := newTestInboundGateway(t, bus, nil, nil, backend.inboundStore)
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
	t.Helper()
	var eventID string
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT event_id
			FROM events
			WHERE run_id = ?
			  AND entity_id = ?
			  AND event_name = 'inbound.telegram'
			  AND json_extract(payload, '$.provider_event_id') = ?
			ORDER BY created_at DESC
			LIMIT 1
		`, backend.runID, backend.entityID, providerEventID).Scan(&eventID)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT event_id::text
			FROM events
			WHERE run_id = $1::uuid
			  AND entity_id = $2::uuid
			  AND event_name = 'inbound.telegram'
			  AND payload->>'provider_event_id' = $3
			ORDER BY created_at DESC
			LIMIT 1
		`, backend.runID, backend.entityID, providerEventID).Scan(&eventID)
	}
	if err != nil {
		t.Fatalf("%s load inbound event id for %s: %v", backend.name, providerEventID, err)
	}
	return eventID
}

func waitForSlackManagedConnectorTerminalActivityAttempt(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) runtimepipeline.ActivityAttemptRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
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
	var requestEventID string
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT request_event_id
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = 'slack.post_message'
			  AND source_event_id = ?
			ORDER BY started_at ASC
			LIMIT 1
		`, backend.runID, sourceEventID).Scan(&requestEventID)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT request_event_id::text
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = 'slack.post_message'
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

func countSlackManagedConnectorActivityAttempts(t *testing.T, backend slackManagedConnectorBackend) int {
	t.Helper()
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = 'slack.post_message'
		`, backend.runID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = 'slack.post_message'
		`, backend.runID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count activity attempts: %v", backend.name, err)
	}
	return count
}

func countSlackManagedConnectorActivityAttemptsForSource(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) int {
	t.Helper()
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = 'slack.post_message'
			  AND source_event_id = ?
		`, backend.runID, sourceEventID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = 'slack.post_message'
			  AND source_event_id = $2::uuid
		`, backend.runID, sourceEventID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count activity attempts for source event %s: %v", backend.name, sourceEventID, err)
	}
	return count
}

func countSlackManagedConnectorFailureEventsForSource(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) int {
	t.Helper()
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE run_id = ?
			  AND event_name = 'slack_post_message.failed'
			  AND source_event_id = ?
		`, backend.runID, sourceEventID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE run_id = $1::uuid
			  AND event_name = 'slack_post_message.failed'
			  AND source_event_id = $2::uuid
		`, backend.runID, sourceEventID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count failure events for source event %s: %v", backend.name, sourceEventID, err)
	}
	return count
}

func requireSlackManagedConnectorResultEventEventually(t *testing.T, backend slackManagedConnectorBackend, eventID, eventType string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
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
	deadline := time.Now().Add(5 * time.Second)
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
	var eventLeaks int
	var attemptLeaks int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE run_id = ?
			  AND payload LIKE '%' || ? || '%'
		`, backend.runID, secret).Scan(&eventLeaks)
		if err == nil {
			err = backend.db.QueryRowContext(backend.ctx, `
				SELECT COUNT(*)
				FROM activity_attempts
				WHERE run_id = ?
				  AND (
					COALESCE(result_payload, '') LIKE '%' || ? || '%'
					OR COALESCE(failure, '') LIKE '%' || ? || '%'
				  )
			`, backend.runID, secret, secret).Scan(&attemptLeaks)
		}
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE run_id = $1::uuid
			  AND payload::text LIKE '%' || $2 || '%'
		`, backend.runID, secret).Scan(&eventLeaks)
		if err == nil {
			err = backend.db.QueryRowContext(backend.ctx, `
				SELECT COUNT(*)
				FROM activity_attempts
				WHERE run_id = $1::uuid
				  AND (
					COALESCE(result_payload::text, '') LIKE '%' || $2 || '%'
					OR COALESCE(failure::text, '') LIKE '%' || $2 || '%'
				  )
			`, backend.runID, secret).Scan(&attemptLeaks)
		}
	}
	if err != nil {
		t.Fatalf("%s no-secret query for %q: %v", backend.name, secret, err)
	}
	if eventLeaks != 0 || attemptLeaks != 0 {
		t.Fatalf("%s stored secret %q leaks: events=%d activity_attempts=%d", backend.name, secret, eventLeaks, attemptLeaks)
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
