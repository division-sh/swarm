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
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestNotionManagedCredentialConnectorPackRoundTripThroughActivityJournal(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)

		const (
			runID        = "8a000000-0000-0000-0000-000000000001"
			entityID     = "8a000000-0000-0000-0000-000000000002"
			flowInstance = "notion-connector-managed-credential-pg"
		)
		ctx := runtimecorrelation.WithRunID(context.Background(), runID)
		pg := &store.PostgresStore{DB: db}
		workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
		seedPostgresInboundGatewayRuntime(t, ctx, db, pg, runID, entityID, flowInstance, "customer-a", "telegram", "telegram-secret", "notion-managed-credential-observer")
		seedTelegramConnectorSupportedSurfaceWorkflowVersion(t, ctx, db, flowInstance, false)

		runNotionManagedCredentialConnectorSurface(t, slackManagedConnectorBackend{
			name:          "postgres",
			ctx:           ctx,
			db:            db,
			eventStore:    pg,
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
			runID        = "8b000000-0000-0000-0000-000000000001"
			entityID     = "8b000000-0000-0000-0000-000000000002"
			flowInstance = "notion-connector-managed-credential-sqlite"
		)
		ctx := runtimecorrelation.WithRunID(context.Background(), runID)
		sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
		workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore)
		seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, "customer-a", "telegram", "telegram-secret", "notion-managed-credential-observer")
		seedTelegramConnectorSupportedSurfaceWorkflowVersion(t, ctx, sqliteStore.DB, flowInstance, true)

		runNotionManagedCredentialConnectorSurface(t, slackManagedConnectorBackend{
			name:          "sqlite",
			ctx:           ctx,
			db:            sqliteStore.DB,
			eventStore:    sqliteStore,
			inboundStore:  sqliteStore,
			workflowStore: workflowStore,
			runID:         runID,
			entityID:      entityID,
			flowInstance:  flowInstance,
			sqlite:        true,
		})
	})
}

type notionManagedConnectorCall struct {
	auth string
	body map[string]any
	raw  string
}

func runNotionManagedCredentialConnectorSurface(t *testing.T, backend slackManagedConnectorBackend) {
	t.Helper()
	fake := newFakeNotionManagedConnectorServer(t)
	defer fake.server.Close()

	managedStore := runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
		Key:          "notion_oauth",
		Provider:     "notion",
		GrantType:    runtimemanagedcredentials.GrantAuthorizationCode,
		TokenURL:     fake.server.URL + "/v1/oauth/token",
		ClientID:     "notion-client",
		ClientSecret: "notion-secret",
		GrantModel:   managedcredentialmodel.GrantModelWorkspace,
		TokenRequest: managedcredentialmodel.TokenRequestProfile{
			ClientAuth: managedcredentialmodel.TokenClientAuthBasic,
			Body:       managedcredentialmodel.TokenBodyJSON,
			StaticHeaders: map[string]string{
				"Notion-Version": "2026-03-11",
			},
		},
		AccessToken:  "expired-token",
		RefreshToken: "refresh-secret",
		Status:       runtimemanagedcredentials.StatusConnected,
		ExpiresAt:    time.Now().Add(-time.Hour),
	})
	source := notionManagedConnectorSource(t, fake.server.URL, backend.flowInstance)
	bus, pc := startSlackManagedConnectorBusAndCoordinator(t, backend, source, managedStore)
	gateway := newTestInboundGateway(t, bus, nil, nil, backend.inboundStore)
	webhookPath := fmt.Sprintf("/webhooks/%s/telegram", backend.entityID)

	publishTelegramMessageToSlack(t, backend, bus, gateway, webhookPath, "223456789", "append first block")
	firstInboundEventID := loadSlackManagedConnectorInboundEventID(t, backend, "223456789")
	firstAttempt := waitForNotionManagedConnectorTerminalActivityAttempt(t, backend, firstInboundEventID)
	if firstAttempt.Status != runtimepipeline.ActivityAttemptStatusSucceeded {
		t.Fatalf("%s first activity attempt status = %q failure=%#v, want succeeded", backend.name, firstAttempt.Status, firstAttempt.Failure)
	}
	firstCall := fake.requireSideEffectCall(t, backend.name, "refresh-before-use")
	if firstCall.auth != "Bearer fresh-token" {
		t.Fatalf("%s first Notion auth = %q, want Bearer fresh-token", backend.name, firstCall.auth)
	}
	if got := notionManagedConnectorString(firstCall.body["children"]); !strings.Contains(got, "hello from swarm") {
		t.Fatalf("%s first Notion children = %#v, want static block body", backend.name, firstCall.body["children"])
	}
	requireSlackManagedConnectorResultEventEventually(t, backend, firstAttempt.ResultEventID, firstAttempt.ResultEventType)
	if got := fake.refreshCount(); got != 1 {
		t.Fatalf("%s refresh-before-use token refreshes = %d, want 1", backend.name, got)
	}

	publishTelegramMessageToSlack(t, backend, bus, gateway, webhookPath, "223456790", "needs 401 refresh")
	secondInboundEventID := loadSlackManagedConnectorInboundEventID(t, backend, "223456790")
	secondAttempt := waitForNotionManagedConnectorTerminalActivityAttempt(t, backend, secondInboundEventID)
	if secondAttempt.Status != runtimepipeline.ActivityAttemptStatusSucceeded {
		t.Fatalf("%s second activity attempt status = %q failure=%#v, want succeeded", backend.name, secondAttempt.Status, secondAttempt.Failure)
	}
	secondCall := fake.requireSideEffectCall(t, backend.name, "refresh-on-401")
	if secondCall.auth != "Bearer after-401-token" {
		t.Fatalf("%s second Notion auth = %q, want Bearer after-401-token", backend.name, secondCall.auth)
	}
	if got := fake.refreshCount(); got != 2 {
		t.Fatalf("%s token refreshes after 401 = %d, want 2", backend.name, got)
	}
	if got := fake.providerHTTPRequestCount(); got != 3 {
		t.Fatalf("%s Notion HTTP requests = %d, want 3 (success, 401, retry success)", backend.name, got)
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
	if got := countNotionManagedConnectorActivityAttempts(t, backend); got != 2 {
		t.Fatalf("%s activity attempts after duplicate = %d, want 2", backend.name, got)
	}

	publishTelegramMessageToSlack(t, backend, bus, gateway, webhookPath, "223456791", "provider 429 fixture")
	rateLimitInboundEventID := loadSlackManagedConnectorInboundEventID(t, backend, "223456791")
	rateLimitAttempt := waitForNotionManagedConnectorTerminalActivityAttempt(t, backend, rateLimitInboundEventID)
	fake.requireNoSideEffectCall(t, backend.name, "429 fixture")
	if rateLimitAttempt.Status != runtimepipeline.ActivityAttemptStatusFailed {
		t.Fatalf("%s 429 activity attempt status = %q, want failed", backend.name, rateLimitAttempt.Status)
	}
	if rateLimitAttempt.Failure == nil || rateLimitAttempt.Failure.Detail.Code != "provider_http_status" || fmt.Sprint(rateLimitAttempt.Failure.Detail.Attributes["status"]) != "429" {
		t.Fatalf("%s 429 attempt failure = %#v, want provider_http_status/429", backend.name, rateLimitAttempt.Failure)
	}
	requireSlackManagedConnectorResultEventEventually(t, backend, rateLimitAttempt.ResultEventID, rateLimitAttempt.ResultEventType)
	if got := countNotionManagedConnectorFailureEventsForSource(t, backend, rateLimitInboundEventID); got != 1 {
		t.Fatalf("%s 429 failure events = %d, want 1", backend.name, got)
	}

	assertNotionManagedConnectorMissingCredential(t, backend, fake.server.URL)
	_ = pc
	for _, secret := range []string{"expired-token", "fresh-token", "after-401-token", "refresh-secret", "rotated-refresh", "after-401-refresh", "notion-secret"} {
		assertSlackManagedConnectorNoStoredSecret(t, backend, secret)
	}
}

type fakeNotionManagedConnectorServer struct {
	server      *httptest.Server
	tokenCalls  atomic.Int64
	notionCalls atomic.Int64
	sideEffects chan notionManagedConnectorCall
}

func newFakeNotionManagedConnectorServer(t *testing.T) *fakeNotionManagedConnectorServer {
	t.Helper()
	fake := &fakeNotionManagedConnectorServer{sideEffects: make(chan notionManagedConnectorCall, 8)}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "notion-client" || pass != "notion-secret" {
			http.Error(w, "bad basic auth", http.StatusUnauthorized)
			return
		}
		if got := r.Header.Get("Notion-Version"); got != "2026-03-11" {
			http.Error(w, "missing Notion-Version", http.StatusBadRequest)
			return
		}
		if got := r.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
			http.Error(w, "bad content type", http.StatusBadRequest)
			return
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if body["grant_type"] != "refresh_token" {
			http.Error(w, "unexpected grant_type "+body["grant_type"], http.StatusBadRequest)
			return
		}
		call := fake.tokenCalls.Add(1)
		wantRefresh := "refresh-secret"
		access := "fresh-token"
		refresh := "rotated-refresh"
		if call >= 2 {
			wantRefresh = "rotated-refresh"
			access = "after-401-token"
			refresh = "after-401-refresh"
		}
		if body["refresh_token"] != wantRefresh {
			http.Error(w, "unexpected refresh token", http.StatusBadRequest)
			return
		}
		for _, forbidden := range []string{"client_id", "client_secret"} {
			if body[forbidden] != "" {
				http.Error(w, "body carried "+forbidden, http.StatusBadRequest)
				return
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  access,
			"refresh_token": refresh,
			"token_type":    "Bearer",
			"expires_in":    3600,
		})
	})
	mux.HandleFunc("/v1/blocks/block-123/children", func(w http.ResponseWriter, r *http.Request) {
		call := fake.notionCalls.Add(1)
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if r.Method != http.MethodPatch {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		if got := r.Header.Get("Notion-Version"); got != "2026-03-11" {
			http.Error(w, "missing Notion-Version", http.StatusBadRequest)
			return
		}
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		auth := r.Header.Get("Authorization")
		if call == 2 && auth == "Bearer fresh-token" {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{"object": "error", "status": 401, "code": "unauthorized", "message": "token expired"})
			return
		}
		if call == 4 {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Header().Set("Retry-After", "1")
			_ = json.NewEncoder(w).Encode(map[string]any{"object": "error", "status": 429, "code": "rate_limited", "message": "slow down"})
			return
		}
		if auth != "Bearer fresh-token" && auth != "Bearer after-401-token" {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{"object": "error", "status": 401, "code": "unauthorized", "message": "bad token"})
			return
		}
		fake.sideEffects <- notionManagedConnectorCall{auth: auth, body: body, raw: string(raw)}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"object": "list",
			"results": []map[string]any{
				{"object": "block", "id": "child-1"},
			},
		})
	})
	fake.server = httptest.NewServer(mux)
	return fake
}

func (f *fakeNotionManagedConnectorServer) requireSideEffectCall(t *testing.T, backend, context string) notionManagedConnectorCall {
	t.Helper()
	select {
	case call := <-f.sideEffects:
		return call
	case <-time.After(connectorSupportedSurfaceAsyncTimeout):
		t.Fatalf("%s %s: timed out waiting for fake Notion side effect", backend, context)
		return notionManagedConnectorCall{}
	}
}

func (f *fakeNotionManagedConnectorServer) requireNoSideEffectCall(t *testing.T, backend, context string) {
	t.Helper()
	select {
	case call := <-f.sideEffects:
		t.Fatalf("%s %s: unexpected fake Notion side effect: auth=%s body=%s", backend, context, call.auth, call.raw)
	default:
	}
}

func (f *fakeNotionManagedConnectorServer) refreshCount() int {
	return int(f.tokenCalls.Load())
}

func (f *fakeNotionManagedConnectorServer) providerHTTPRequestCount() int {
	return int(f.notionCalls.Load())
}

func notionManagedConnectorSource(t *testing.T, baseURL, flowInstance string) semanticview.Source {
	t.Helper()
	handler := runtimecontracts.SystemNodeEventHandler{
		Activity: runtimecontracts.ActivitySpec{
			ID:   "notion_append_block_children",
			Tool: "notion.append_block_children",
			Input: map[string]runtimecontracts.ExpressionValue{
				"block_id": runtimecontracts.LiteralExpression("block-123"),
				"children": runtimecontracts.LiteralExpression([]map[string]any{
					{
						"object": "block",
						"type":   "paragraph",
						"paragraph": map[string]any{
							"rich_text": []map[string]any{
								{
									"type": "text",
									"text": map[string]any{"content": "hello from swarm"},
								},
							},
						},
					},
				}),
			},
		},
	}
	const nodeID = "notion-responder"
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
			Name:    "notion_connector_managed_credential_supported_surface",
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
						Imports: []runtimecontracts.ConnectorPackImport{{Provider: "notion", Tool: "notion.append_block_children"}},
					},
				},
			},
		},
	}
	source, err := providerconnectors.SourceWithConnectorPackImportsFromRegistry(importSource, notionManagedConnectorPackRegistry(t, baseURL))
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImportsFromRegistry: %v", err)
	}
	return source
}

func notionManagedConnectorPackRegistry(t *testing.T, baseURL string) *providerconnectors.PackRegistry {
	t.Helper()
	tool, ok := providerconnectors.BuiltinTool("notion", "notion.append_block_children")
	if !ok {
		t.Fatal("provider connector pack notion.append_block_children not found")
	}
	if tool.HTTP == nil {
		t.Fatal("provider connector pack notion.append_block_children missing http block")
	}
	httpSpec := *tool.HTTP
	tool.HTTP = &httpSpec
	tool.HTTP.URL = strings.TrimRight(baseURL, "/") + "/v1/blocks/{{input.block_id}}/children"
	registry, err := providerconnectors.NewPackRegistry(providerconnectors.LoadedPack{
		Envelope: packs.Envelope{
			ID: "provider.notion.connector",
			Provenance: packs.Provenance{
				Source: packs.ProvenancePlatform,
			},
		},
		Manifest: providerconnectors.ConnectorManifest{
			Provider: "notion",
			Tools: map[string]runtimecontracts.ToolSchemaEntry{
				"notion.append_block_children": tool,
			},
		},
		Source: "test:provider.notion.connector",
	})
	if err != nil {
		t.Fatalf("NewPackRegistry: %v", err)
	}
	return registry
}

func assertNotionManagedConnectorMissingCredential(t *testing.T, backend slackManagedConnectorBackend, baseURL string) {
	t.Helper()
	source := notionManagedConnectorSource(t, baseURL, backend.flowInstance)
	bus, _ := startSlackManagedConnectorBusAndCoordinator(t, backend, source, runtimemanagedcredentials.NewMemoryStore())
	gateway := newTestInboundGateway(t, bus, nil, nil, backend.inboundStore)
	webhookPath := fmt.Sprintf("/webhooks/%s/telegram", backend.entityID)
	publishTelegramMessageToSlack(t, backend, bus, gateway, webhookPath, "223456792", "missing credential")
	inboundEventID := loadSlackManagedConnectorInboundEventID(t, backend, "223456792")
	if attempt := waitForNotionManagedConnectorTerminalActivityAttempt(t, backend, inboundEventID); attempt.Status != runtimepipeline.ActivityAttemptStatusFailed {
		t.Fatalf("%s missing managed credential activity status = %q, want failed", backend.name, attempt.Status)
	}
	if got := countNotionManagedConnectorActivityAttemptsForSource(t, backend, inboundEventID); got != 1 {
		t.Fatalf("%s missing managed credential activity attempts = %d, want one failed claim", backend.name, got)
	}
	requireManagedConnectorFailureEventCountEventually(t, backend, "missing managed credential", inboundEventID, countNotionManagedConnectorFailureEventsForSource)
}

func waitForNotionManagedConnectorTerminalActivityAttempt(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) runtimepipeline.ActivityAttemptRecord {
	t.Helper()
	deadline := time.Now().Add(connectorSupportedSurfaceAsyncTimeout)
	var last runtimepipeline.ActivityAttemptRecord
	var saw bool
	for {
		rec, ok, err := tryLoadNotionManagedConnectorActivityAttempt(backend, sourceEventID)
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

func tryLoadNotionManagedConnectorActivityAttempt(backend slackManagedConnectorBackend, sourceEventID string) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	var requestEventID string
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT request_event_id
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = 'notion.append_block_children'
			  AND source_event_id = ?
			ORDER BY started_at ASC
			LIMIT 1
		`, backend.runID, sourceEventID).Scan(&requestEventID)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT request_event_id::text
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = 'notion.append_block_children'
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

func countNotionManagedConnectorActivityAttempts(t *testing.T, backend slackManagedConnectorBackend) int {
	t.Helper()
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = 'notion.append_block_children'
		`, backend.runID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = 'notion.append_block_children'
		`, backend.runID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count activity attempts: %v", backend.name, err)
	}
	return count
}

func countNotionManagedConnectorActivityAttemptsForSource(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) int {
	t.Helper()
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = 'notion.append_block_children'
			  AND source_event_id = ?
		`, backend.runID, sourceEventID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = 'notion.append_block_children'
			  AND source_event_id = $2::uuid
		`, backend.runID, sourceEventID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count activity attempts for source event %s: %v", backend.name, sourceEventID, err)
	}
	return count
}

func countNotionManagedConnectorFailureEventsForSource(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) int {
	t.Helper()
	failureEventType := boundedProviderFlowID + ".notion_append_block_children.failed"
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

func notionManagedConnectorString(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(raw)
}
