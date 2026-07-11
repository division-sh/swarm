package runtime_test

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
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
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
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

func TestGitHubAppIssueCommentConnectorPackRoundTripThroughActivityJournal(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)

		const (
			runID        = "9a000000-0000-0000-0000-000000000001"
			entityID     = "9a000000-0000-0000-0000-000000000002"
			flowInstance = "github-app-issue-comment-pg"
		)
		ctx := runtimecorrelation.WithRunID(context.Background(), runID)
		pg := &store.PostgresStore{DB: db}
		workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
		seedPostgresInboundGatewayRuntime(t, ctx, db, pg, runID, entityID, flowInstance, "customer-a", "github", "github-webhook-secret", "github-app-issue-comment-observer")
		seedTelegramConnectorSupportedSurfaceWorkflowVersion(t, ctx, db, flowInstance, false)

		runGitHubAppIssueCommentSurface(t, slackManagedConnectorBackend{
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
			runID        = "9b000000-0000-0000-0000-000000000001"
			entityID     = "9b000000-0000-0000-0000-000000000002"
			flowInstance = "github-app-issue-comment-sqlite"
		)
		ctx := runtimecorrelation.WithRunID(context.Background(), runID)
		sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
		workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore)
		seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, "customer-a", "github", "github-webhook-secret", "github-app-issue-comment-observer")
		seedTelegramConnectorSupportedSurfaceWorkflowVersion(t, ctx, sqliteStore.DB, flowInstance, true)

		runGitHubAppIssueCommentSurface(t, slackManagedConnectorBackend{
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

type githubAppIssueCommentCall struct {
	auth string
	body map[string]any
	path string
	raw  string
}

func runGitHubAppIssueCommentSurface(t *testing.T, backend slackManagedConnectorBackend) {
	t.Helper()
	privateKeyPEM, publicKey := githubAppIssueCommentPrivateKey(t)
	fake := newFakeGitHubAppIssueCommentServer(t, publicKey)
	defer fake.server.Close()

	managedStore := runtimemanagedcredentials.NewMemoryStore(runtimemanagedcredentials.Record{
		Key:            "github_app",
		Provider:       "github",
		GrantType:      runtimemanagedcredentials.GrantGitHubAppInstallation,
		APIBaseURL:     fake.server.URL,
		ClientID:       "github-app-client-id",
		InstallationID: "1001",
		PrivateKey:     privateKeyPEM,
		GrantModel:     managedcredentialmodel.GrantModelInstallation,
		AccessToken:    "expired-install-token",
		Status:         runtimemanagedcredentials.StatusConnected,
		ExpiresAt:      time.Now().Add(-time.Hour),
	})
	source := githubAppIssueCommentSource(t, fake.server.URL)
	bus, pc := startSlackManagedConnectorBusAndCoordinator(t, backend, source, managedStore)
	gateway := newTestInboundGateway(t, bus, nil, nil, backend.inboundStore)
	webhookPath := fmt.Sprintf("/webhooks/%s/github", backend.entityID)

	publishGitHubIssueComment(t, backend, bus, gateway, webhookPath, "gh-delivery-1", "1001", "please respond")
	firstCall := fake.requireSideEffectCall(t, backend.name, "issue comment reply")
	if firstCall.path != "/repos/octo-org/octo-repo/issues/42/comments" {
		t.Fatalf("%s GitHub comment path = %q, want issue comments endpoint", backend.name, firstCall.path)
	}
	if firstCall.auth != "Bearer github-install-token-1" {
		t.Fatalf("%s GitHub comment auth = %q, want installation token", backend.name, firstCall.auth)
	}
	if got := slackManagedConnectorString(firstCall.body["body"]); got != "please respond" {
		t.Fatalf("%s GitHub comment body = %#v, want inbound comment text", backend.name, firstCall.body["body"])
	}
	firstInboundEventID := loadGitHubAppIssueCommentInboundEventID(t, backend, "gh-delivery-1")
	firstAttempt := waitForGitHubAppIssueCommentTerminalActivityAttempt(t, backend, firstInboundEventID)
	if firstAttempt.Status != runtimepipeline.ActivityAttemptStatusSucceeded {
		t.Fatalf("%s first activity attempt status = %q, want succeeded", backend.name, firstAttempt.Status)
	}
	requireSlackManagedConnectorResultEventEventually(t, backend, firstAttempt.ResultEventID, firstAttempt.ResultEventType)
	if got := fake.tokenRequestCount(); got != 1 {
		t.Fatalf("%s installation token requests = %d, want 1", backend.name, got)
	}
	if got := fake.commentRequestCount(); got != 1 {
		t.Fatalf("%s GitHub issue comment requests = %d, want 1", backend.name, got)
	}

	publishGitHubIssueCommentExpectStatus(t, backend, bus, gateway, webhookPath, "gh-delivery-1", "1001", "please respond", http.StatusOK)
	fake.requireNoSideEffectCall(t, backend.name, "duplicate webhook delivery")
	if got := countGitHubAppIssueCommentActivityAttempts(t, backend); got != 1 {
		t.Fatalf("%s activity attempts after duplicate webhook = %d, want 1", backend.name, got)
	}
	if got := fake.tokenRequestCount(); got != 1 {
		t.Fatalf("%s token requests after duplicate webhook = %d, want still 1", backend.name, got)
	}
	if got := fake.commentRequestCount(); got != 1 {
		t.Fatalf("%s comment requests after duplicate webhook = %d, want still 1", backend.name, got)
	}

	requestEvent := loadSlackManagedConnectorActivityRequestEvent(t, backend, firstAttempt.RequestEventID)
	if err := bus.EngineDispatcher().DispatchPostCommit(backend.ctx, []runtimeengine.EmitIntent{{Event: requestEvent}}); err != nil {
		t.Fatalf("%s duplicate activity request dispatch: %v", backend.name, err)
	}
	waitForInboundBusQuiescence(t, bus)
	fake.requireNoSideEffectCall(t, backend.name, "duplicate activity request")
	if got := countGitHubAppIssueCommentActivityAttempts(t, backend); got != 1 {
		t.Fatalf("%s activity attempts after duplicate activity request = %d, want 1", backend.name, got)
	}
	if got := fake.tokenRequestCount(); got != 1 {
		t.Fatalf("%s token requests after duplicate activity = %d, want still 1", backend.name, got)
	}
	if got := fake.commentRequestCount(); got != 1 {
		t.Fatalf("%s comment requests after duplicate activity = %d, want still 1", backend.name, got)
	}

	publishGitHubIssueCommentFromSender(t, backend, bus, gateway, webhookPath, "gh-delivery-bot-1", "1001", "issue comment reply", "github-app[bot]", "Bot", http.StatusAccepted)
	botInboundEventID := loadGitHubAppIssueCommentInboundEventID(t, backend, "gh-delivery-bot-1")
	if got := countGitHubAppIssueCommentActivityAttemptsForSource(t, backend, botInboundEventID); got != 0 {
		t.Fatalf("%s bot-authored inbound activity attempts = %d, want 0", backend.name, got)
	}
	fake.requireNoSideEffectCall(t, backend.name, "bot-authored issue_comment")
	if got := countGitHubAppIssueCommentActivityAttempts(t, backend); got != 1 {
		t.Fatalf("%s activity attempts after bot-authored issue_comment = %d, want still 1", backend.name, got)
	}
	if got := fake.tokenRequestCount(); got != 1 {
		t.Fatalf("%s token requests after bot-authored issue_comment = %d, want still 1", backend.name, got)
	}
	if got := fake.commentRequestCount(); got != 1 {
		t.Fatalf("%s comment requests after bot-authored issue_comment = %d, want still 1", backend.name, got)
	}

	assertGitHubAppIssueCommentManagedCredentialFailureBeforeDispatch(t, backend, fake, "missing credential", "gh-delivery-2", "1001", runtimemanagedcredentials.NewMemoryStore())
	mismatched := managedStoreRecordWithInstallation(privateKeyPEM, fake.server.URL, "1001")
	assertGitHubAppIssueCommentManagedCredentialFailureBeforeDispatch(t, backend, fake, "installation mismatch", "gh-delivery-3", "2002", runtimemanagedcredentials.NewMemoryStore(mismatched))
	_ = pc
	for _, secret := range []string{"expired-install-token", "github-install-token-1", "github-webhook-secret"} {
		assertSlackManagedConnectorNoStoredSecret(t, backend, secret)
	}
}

func newFakeGitHubAppIssueCommentServer(t *testing.T, publicKey *rsa.PublicKey) *fakeGitHubAppIssueCommentServer {
	t.Helper()
	fake := &fakeGitHubAppIssueCommentServer{
		publicKey:   publicKey,
		sideEffects: make(chan githubAppIssueCommentCall, 8),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/app/installations/1001/access_tokens", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		if got := r.Header.Get("Accept"); got != "application/vnd.github+json" {
			http.Error(w, "bad accept", http.StatusBadRequest)
			return
		}
		if got := r.Header.Get("X-GitHub-Api-Version"); got != "2022-11-28" {
			http.Error(w, "bad api version", http.StatusBadRequest)
			return
		}
		jwt := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if jwt == "" || jwt == r.Header.Get("Authorization") {
			http.Error(w, "missing app jwt", http.StatusUnauthorized)
			return
		}
		verifyGitHubAppIssueCommentJWT(t, jwt, fake.publicKey, "github-app-client-id", time.Now().UTC())
		call := fake.tokenCalls.Add(1)
		if call != 1 {
			http.Error(w, "unexpected token reacquisition", http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token":      "github-install-token-1",
			"expires_at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
		})
	})
	mux.HandleFunc("/repos/octo-org/octo-repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		fake.commentCalls.Add(1)
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer github-install-token-1" {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{"message": "bad token"})
			return
		}
		if got := r.Header.Get("Accept"); got != "application/vnd.github+json" {
			http.Error(w, "bad accept", http.StatusBadRequest)
			return
		}
		if got := r.Header.Get("X-GitHub-Api-Version"); got != "2022-11-28" {
			http.Error(w, "bad api version", http.StatusBadRequest)
			return
		}
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fake.sideEffects <- githubAppIssueCommentCall{
			auth: r.Header.Get("Authorization"),
			body: body,
			path: r.URL.Path,
			raw:  string(raw),
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":   123,
			"body": body["body"],
		})
	})
	fake.server = httptest.NewServer(mux)
	return fake
}

type fakeGitHubAppIssueCommentServer struct {
	server       *httptest.Server
	publicKey    *rsa.PublicKey
	tokenCalls   atomic.Int64
	commentCalls atomic.Int64
	sideEffects  chan githubAppIssueCommentCall
}

func (f *fakeGitHubAppIssueCommentServer) requireSideEffectCall(t *testing.T, backend, context string) githubAppIssueCommentCall {
	t.Helper()
	select {
	case call := <-f.sideEffects:
		return call
	case <-time.After(5 * time.Second):
		t.Fatalf("%s %s: timed out waiting for fake GitHub issue comment side effect", backend, context)
		return githubAppIssueCommentCall{}
	}
}

func (f *fakeGitHubAppIssueCommentServer) requireNoSideEffectCall(t *testing.T, backend, context string) {
	t.Helper()
	select {
	case call := <-f.sideEffects:
		t.Fatalf("%s %s: unexpected fake GitHub issue comment side effect: auth=%s body=%s", backend, context, call.auth, call.raw)
	default:
	}
}

func (f *fakeGitHubAppIssueCommentServer) tokenRequestCount() int {
	return int(f.tokenCalls.Load())
}

func (f *fakeGitHubAppIssueCommentServer) commentRequestCount() int {
	return int(f.commentCalls.Load())
}

func publishGitHubIssueComment(t *testing.T, backend slackManagedConnectorBackend, bus *runtimebus.EventBus, gateway *runtimepkg.InboundGateway, webhookPath, deliveryID, installationID, body string) {
	t.Helper()
	publishGitHubIssueCommentExpectStatus(t, backend, bus, gateway, webhookPath, deliveryID, installationID, body, http.StatusAccepted)
}

func publishGitHubIssueCommentExpectStatus(t *testing.T, backend slackManagedConnectorBackend, bus *runtimebus.EventBus, gateway *runtimepkg.InboundGateway, webhookPath, deliveryID, installationID, body string, wantStatus int) {
	t.Helper()
	publishGitHubIssueCommentFromSender(t, backend, bus, gateway, webhookPath, deliveryID, installationID, body, "octocat", "User", wantStatus)
}

func publishGitHubIssueCommentFromSender(t *testing.T, backend slackManagedConnectorBackend, bus *runtimebus.EventBus, gateway *runtimepkg.InboundGateway, webhookPath, deliveryID, installationID, body, senderLogin, senderType string, wantStatus int) {
	t.Helper()
	payload := []byte(fmt.Sprintf(`{"action":"created","installation":{"id":%s},"repository":{"name":"octo-repo","owner":{"login":"octo-org"}},"issue":{"number":42},"comment":{"id":9001,"body":%q,"user":{"login":%q,"type":%q}},"sender":{"login":%q,"type":%q}}`, installationID, body, senderLogin, senderType, senderLogin, senderType))
	req := httptest.NewRequest(http.MethodPost, webhookPath, strings.NewReader(string(payload))).WithContext(backend.ctx)
	req.Header.Set("X-Hub-Signature-256", githubWebhookSignature("github-webhook-secret", payload))
	req.Header.Set("X-GitHub-Delivery", deliveryID)
	req.Header.Set("X-GitHub-Event", "issue_comment")
	rec := httptest.NewRecorder()
	gateway.Handler().ServeHTTP(rec, req)
	if rec.Code != wantStatus {
		t.Fatalf("%s gateway status for delivery %s = %d, want %d body=%s", backend.name, deliveryID, rec.Code, wantStatus, rec.Body.String())
	}
	waitForInboundBusQuiescence(t, bus)
}

func githubAppIssueCommentSource(t *testing.T, baseURL string) semanticview.Source {
	t.Helper()
	handler := runtimecontracts.SystemNodeEventHandler{
		Guard: &runtimecontracts.GuardSpec{
			Check:  `payload.payload.comment.user.type != "Bot" && payload.payload.sender.type != "Bot"`,
			OnFail: "discard",
		},
		Activity: runtimecontracts.ActivitySpec{
			ID:   "github_create_issue_comment",
			Tool: "github.create_issue_comment",
			Input: map[string]runtimecontracts.ExpressionValue{
				"installation_id": runtimecontracts.CELExpression("payload.payload.installation.id"),
				"owner":           runtimecontracts.CELExpression("payload.payload.repository.owner.login"),
				"repo":            runtimecontracts.CELExpression("payload.payload.repository.name"),
				"issue_number":    runtimecontracts.CELExpression("payload.payload.issue.number"),
				"body":            runtimecontracts.CELExpression("payload.payload.comment.body"),
			},
		},
	}
	const nodeID = "github-issue-comment-responder"
	node := runtimecontracts.SystemNodeContract{
		ID:            nodeID,
		ExecutionType: runtimecontracts.SystemNodeExecutionType,
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			"inbound.github.issue_comment": handler,
		},
	}
	base := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{
					Events: []string{"inbound.github.issue_comment"},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{nodeID: node},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "github_app_issue_comment_supported_surface",
			Version: "1.0.0",
			EffectiveNodes: map[string]runtimecontracts.SystemNodeEffectiveSemantics{
				nodeID: {
					ID:                   nodeID,
					ExecutionType:        runtimecontracts.SystemNodeExecutionType,
					RuntimeSubscriptions: []string{"inbound.github.issue_comment"},
				},
			},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				nodeID: {"inbound.github.issue_comment": handler},
			},
			EventOwners: map[string][]string{
				"inbound.github.issue_comment": {nodeID},
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
						Imports: []runtimecontracts.ConnectorPackImport{{Provider: "github", Tool: "github.create_issue_comment"}},
					},
				},
			},
		},
	}
	source, err := providerconnectors.SourceWithConnectorPackImportsFromRegistry(importSource, githubAppIssueCommentPackRegistry(t, baseURL))
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImportsFromRegistry: %v", err)
	}
	return source
}

func githubAppIssueCommentPackRegistry(t *testing.T, baseURL string) *providerconnectors.PackRegistry {
	t.Helper()
	tool, ok := providerconnectors.BuiltinTool("github", "github.create_issue_comment")
	if !ok {
		t.Fatal("provider connector pack github.create_issue_comment not found")
	}
	if tool.HTTP == nil {
		t.Fatal("provider connector pack github.create_issue_comment missing http block")
	}
	httpSpec := *tool.HTTP
	tool.HTTP = &httpSpec
	tool.HTTP.URL = strings.TrimRight(baseURL, "/") + "/repos/{{input.owner}}/{{input.repo}}/issues/{{input.issue_number}}/comments"
	registry, err := providerconnectors.NewPackRegistry(providerconnectors.LoadedPack{
		Envelope: packs.Envelope{
			ID: "provider.github.connector",
			Provenance: packs.Provenance{
				Source: packs.ProvenancePlatform,
			},
		},
		Manifest: providerconnectors.ConnectorManifest{
			Provider: "github",
			Tools: map[string]runtimecontracts.ToolSchemaEntry{
				"github.create_issue_comment": tool,
			},
		},
		Source: "test:provider.github.connector",
	})
	if err != nil {
		t.Fatalf("NewPackRegistry: %v", err)
	}
	return registry
}

func assertGitHubAppIssueCommentManagedCredentialFailureBeforeDispatch(t *testing.T, backend slackManagedConnectorBackend, fake *fakeGitHubAppIssueCommentServer, label, deliveryID, installationID string, managedStore runtimemanagedcredentials.Store) {
	t.Helper()
	beforeTokens := fake.tokenRequestCount()
	beforeComments := fake.commentRequestCount()
	source := githubAppIssueCommentSource(t, fake.server.URL)
	bus, _ := startSlackManagedConnectorBusAndCoordinator(t, backend, source, managedStore)
	gateway := newTestInboundGateway(t, bus, nil, nil, backend.inboundStore)
	webhookPath := fmt.Sprintf("/webhooks/%s/github", backend.entityID)
	publishGitHubIssueComment(t, backend, bus, gateway, webhookPath, deliveryID, installationID, label)
	inboundEventID := loadGitHubAppIssueCommentInboundEventID(t, backend, deliveryID)
	if attempt := waitForGitHubAppIssueCommentTerminalActivityAttempt(t, backend, inboundEventID); attempt.Status != runtimepipeline.ActivityAttemptStatusFailed {
		t.Fatalf("%s %s activity status = %q, want failed", backend.name, label, attempt.Status)
	}
	if got := countGitHubAppIssueCommentActivityAttemptsForSource(t, backend, inboundEventID); got != 1 {
		t.Fatalf("%s %s activity attempts = %d, want one failed claim", backend.name, label, got)
	}
	requireGitHubAppIssueCommentFailureEventEventually(t, backend, label, inboundEventID)
	if got := fake.tokenRequestCount(); got != beforeTokens {
		t.Fatalf("%s %s token requests = %d, want still %d", backend.name, label, got, beforeTokens)
	}
	if got := fake.commentRequestCount(); got != beforeComments {
		t.Fatalf("%s %s comment requests = %d, want still %d", backend.name, label, got, beforeComments)
	}
	fake.requireNoSideEffectCall(t, backend.name, label)
}

func requireGitHubAppIssueCommentFailureEventEventually(t *testing.T, backend slackManagedConnectorBackend, label, sourceEventID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if got := countGitHubAppIssueCommentFailureEventsForSource(t, backend, sourceEventID); got == 1 {
			return
		} else if got > 1 {
			t.Fatalf("%s %s failure events = %d, want 1", backend.name, label, got)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s %s failure event for source event %s was not created", backend.name, label, sourceEventID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func loadGitHubAppIssueCommentInboundEventID(t *testing.T, backend slackManagedConnectorBackend, providerEventID string) string {
	t.Helper()
	var eventID string
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT event_id
			FROM events
			WHERE run_id = ?
			  AND entity_id = ?
			  AND event_name = 'inbound.github.issue_comment'
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
			  AND event_name = 'inbound.github.issue_comment'
			  AND payload->>'provider_event_id' = $3
			ORDER BY created_at DESC
			LIMIT 1
		`, backend.runID, backend.entityID, providerEventID).Scan(&eventID)
	}
	if err != nil {
		t.Fatalf("%s load GitHub inbound event id for %s: %v", backend.name, providerEventID, err)
	}
	return eventID
}

func waitForGitHubAppIssueCommentTerminalActivityAttempt(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) runtimepipeline.ActivityAttemptRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last runtimepipeline.ActivityAttemptRecord
	var saw bool
	for {
		rec, ok, err := tryLoadGitHubAppIssueCommentActivityAttempt(backend, sourceEventID)
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

func tryLoadGitHubAppIssueCommentActivityAttempt(backend slackManagedConnectorBackend, sourceEventID string) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	var requestEventID string
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT request_event_id
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = 'github.create_issue_comment'
			  AND source_event_id = ?
			ORDER BY started_at ASC
			LIMIT 1
		`, backend.runID, sourceEventID).Scan(&requestEventID)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT request_event_id::text
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = 'github.create_issue_comment'
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

func countGitHubAppIssueCommentActivityAttempts(t *testing.T, backend slackManagedConnectorBackend) int {
	t.Helper()
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = 'github.create_issue_comment'
		`, backend.runID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = 'github.create_issue_comment'
		`, backend.runID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count GitHub activity attempts: %v", backend.name, err)
	}
	return count
}

func countGitHubAppIssueCommentActivityAttemptsForSource(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) int {
	t.Helper()
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = 'github.create_issue_comment'
			  AND source_event_id = ?
		`, backend.runID, sourceEventID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = 'github.create_issue_comment'
			  AND source_event_id = $2::uuid
		`, backend.runID, sourceEventID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count GitHub activity attempts for source event %s: %v", backend.name, sourceEventID, err)
	}
	return count
}

func countGitHubAppIssueCommentFailureEventsForSource(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) int {
	t.Helper()
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE run_id = ?
			  AND event_name = 'github_create_issue_comment.failed'
			  AND source_event_id = ?
		`, backend.runID, sourceEventID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE run_id = $1::uuid
			  AND event_name = 'github_create_issue_comment.failed'
			  AND source_event_id = $2::uuid
		`, backend.runID, sourceEventID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count GitHub failure events for source event %s: %v", backend.name, sourceEventID, err)
	}
	return count
}

func managedStoreRecordWithInstallation(privateKeyPEM, apiBaseURL, installationID string) runtimemanagedcredentials.Record {
	return runtimemanagedcredentials.Record{
		Key:            "github_app",
		Provider:       "github",
		GrantType:      runtimemanagedcredentials.GrantGitHubAppInstallation,
		APIBaseURL:     apiBaseURL,
		ClientID:       "github-app-client-id",
		InstallationID: installationID,
		PrivateKey:     privateKeyPEM,
		GrantModel:     managedcredentialmodel.GrantModelInstallation,
		AccessToken:    "expired-install-token",
		Status:         runtimemanagedcredentials.StatusConnected,
		ExpiresAt:      time.Now().Add(-time.Hour),
	}
}

func githubAppIssueCommentPrivateKey(t *testing.T) (string, *rsa.PublicKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	raw := x509.MarshalPKCS1PrivateKey(key)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: raw})
	if len(pemBytes) == 0 {
		t.Fatal("EncodeToMemory returned empty PEM")
	}
	return string(pemBytes), &key.PublicKey
}

func verifyGitHubAppIssueCommentJWT(t *testing.T, token string, publicKey *rsa.PublicKey, wantIss string, now time.Time) {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("JWT parts = %d, want 3", len(parts))
	}
	decode := func(name, raw string, target any) {
		t.Helper()
		decoded, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil {
			t.Fatalf("decode JWT %s: %v", name, err)
		}
		if err := json.Unmarshal(decoded, target); err != nil {
			t.Fatalf("unmarshal JWT %s: %v", name, err)
		}
	}
	var header struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}
	decode("header", parts[0], &header)
	if header.Alg != "RS256" || header.Typ != "JWT" {
		t.Fatalf("JWT header = %#v, want RS256 JWT", header)
	}
	var claims struct {
		Iss string `json:"iss"`
		Iat int64  `json:"iat"`
		Exp int64  `json:"exp"`
	}
	decode("claims", parts[1], &claims)
	if claims.Iss != wantIss {
		t.Fatalf("JWT iss = %q, want %q", claims.Iss, wantIss)
	}
	nowUnix := now.Unix()
	if claims.Iat > nowUnix || claims.Exp <= nowUnix || claims.Exp-claims.Iat > 11*60 {
		t.Fatalf("JWT time claims = %#v, want active short-lived app JWT around %s", claims, now)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decode JWT signature: %v", err)
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, sum[:], signature); err != nil {
		t.Fatalf("VerifyPKCS1v15: %v", err)
	}
}
