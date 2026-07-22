package runtime_test

import (
	"context"
	"crypto/rsa"
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
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
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

func TestGitHubAppIssueWorkflowConnectorPackRoundTripThroughActivityJournal(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)

		const (
			runID        = "9c000000-0000-0000-0000-000000000001"
			entityID     = "9c000000-0000-0000-0000-000000000002"
			flowInstance = "github-app-issue-workflow-pg"
		)
		ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
		pg := storetest.AdmitPostgresRuntimeStore(t, db)
		workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
		seedPostgresInboundGatewayRuntime(t, ctx, db, pg, runID, entityID, flowInstance, "customer-a", "github", "github-webhook-secret", "github-app-issue-workflow-observer")
		seedTelegramConnectorSupportedSurfaceWorkflowVersion(t, ctx, db, flowInstance, false)

		runGitHubAppIssueWorkflowSurface(t, slackManagedConnectorBackend{
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
			runID        = "9d000000-0000-0000-0000-000000000001"
			entityID     = "9d000000-0000-0000-0000-000000000002"
			flowInstance = "github-app-issue-workflow-sqlite"
		)
		ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
		sqliteStore := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
		workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore)
		seedSQLiteInboundGatewayRuntime(t, ctx, sqliteStore, runID, entityID, flowInstance, "customer-a", "github", "github-webhook-secret", "github-app-issue-workflow-observer")
		seedTelegramConnectorSupportedSurfaceWorkflowVersion(t, ctx, sqliteStore.DB, flowInstance, true)

		runGitHubAppIssueWorkflowSurface(t, slackManagedConnectorBackend{
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

type githubAppIssueWorkflowCall struct {
	action string
	auth   string
	body   map[string]any
	path   string
	raw    string
}

func runGitHubAppIssueWorkflowSurface(t *testing.T, backend slackManagedConnectorBackend) {
	t.Helper()
	privateKeyPEM, publicKey := githubAppIssueCommentPrivateKey(t)
	fake := newFakeGitHubAppIssueWorkflowServer(t, publicKey)
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
	source := githubAppIssueWorkflowSource(t, fake.server.URL, backend.flowInstance)
	bus, pc := startSlackManagedConnectorBusAndCoordinator(t, backend, source, managedStore)
	gateway := newTestInboundGateway(t, bus, nil, nil, backend.inboundStore)
	webhookPath := fmt.Sprintf("/webhooks/%s/github", backend.entityID)

	publishGitHubIssueOpenedWithSignature(t, backend, bus, gateway, webhookPath, "gh-issue-bad-signature", "1001", 42, "Bad signature issue", "sha256=bad", http.StatusUnauthorized)
	fake.requireNoSideEffectCall(t, backend.name, "invalid issues signature")
	if got := fake.tokenRequestCount(); got != 0 {
		t.Fatalf("%s token requests after invalid signature = %d, want 0", backend.name, got)
	}

	publishGitHubIssueComment(t, backend, bus, gateway, webhookPath, "gh-comment-1", "1001", "please respond")
	commentCall := fake.requireSideEffectCall(t, backend.name, "issue_comment reply")
	if commentCall.action != "create_issue_comment" || commentCall.path != "/repos/octo-org/octo-repo/issues/42/comments" {
		t.Fatalf("%s GitHub comment call = %#v, want issue comment endpoint", backend.name, commentCall)
	}
	if got := slackManagedConnectorString(commentCall.body["body"]); got != "please respond" {
		t.Fatalf("%s GitHub comment body = %#v, want inbound comment text", backend.name, commentCall.body["body"])
	}
	commentEventID := loadGitHubInboundEventID(t, backend, "inbound.github.issue_comment", "gh-comment-1")
	commentAttempt := waitForGitHubTerminalActivityAttempt(t, backend, "github.create_issue_comment", commentEventID)
	if commentAttempt.Status != runtimepipeline.ActivityAttemptStatusSucceeded {
		t.Fatalf("%s comment activity status = %q, want succeeded", backend.name, commentAttempt.Status)
	}
	requireSlackManagedConnectorResultEventEventually(t, backend, commentAttempt.ResultEventID, commentAttempt.ResultEventType)
	if got := fake.tokenRequestCount(); got != 1 {
		t.Fatalf("%s installation token requests after comment = %d, want 1", backend.name, got)
	}
	if got := fake.commentRequestCount(); got != 1 {
		t.Fatalf("%s GitHub issue comment requests = %d, want 1", backend.name, got)
	}

	publishGitHubIssueOpened(t, backend, bus, gateway, webhookPath, "gh-issue-1", "1001", 42, "Incoming support issue")
	issueCalls := fake.requireSideEffectCalls(t, backend.name, "issues opened actions", 2)
	issueCreateCall := requireGitHubWorkflowCall(t, backend.name, issueCalls, "create_issue")
	if issueCreateCall.auth != "Bearer github-install-token-1" || issueCreateCall.path != "/repos/octo-org/octo-repo/issues" {
		t.Fatalf("%s GitHub create issue call = %#v, want create issue endpoint with installation token", backend.name, issueCreateCall)
	}
	if got := slackManagedConnectorString(issueCreateCall.body["title"]); got != "Incoming support issue" {
		t.Fatalf("%s GitHub create issue title = %#v, want inbound issue title", backend.name, issueCreateCall.body["title"])
	}
	if got := slackManagedConnectorString(issueCreateCall.body["body"]); got != "Created by Swarm follow-up" {
		t.Fatalf("%s GitHub create issue body = %#v, want literal follow-up body", backend.name, issueCreateCall.body["body"])
	}
	labelCall := requireGitHubWorkflowCall(t, backend.name, issueCalls, "add_labels_to_issue")
	if labelCall.auth != "Bearer github-install-token-1" || labelCall.path != "/repos/octo-org/octo-repo/issues/42/labels" {
		t.Fatalf("%s GitHub label call = %#v, want issue labels endpoint with installation token", backend.name, labelCall)
	}
	requireGitHubLabelsBody(t, backend.name, labelCall.body["labels"], []string{"triage", "swarm"})
	issueEventID := loadGitHubInboundEventID(t, backend, "inbound.github.issues", "gh-issue-1")
	createIssueAttempt := waitForGitHubTerminalActivityAttempt(t, backend, "github.create_issue", issueEventID)
	addLabelsAttempt := waitForGitHubTerminalActivityAttempt(t, backend, "github.add_labels_to_issue", issueEventID)
	for _, attempt := range []runtimepipeline.ActivityAttemptRecord{createIssueAttempt, addLabelsAttempt} {
		if attempt.Status != runtimepipeline.ActivityAttemptStatusSucceeded {
			t.Fatalf("%s issues activity %s status = %q, want succeeded", backend.name, attempt.Tool, attempt.Status)
		}
		requireSlackManagedConnectorResultEventEventually(t, backend, attempt.ResultEventID, attempt.ResultEventType)
	}
	if got := fake.tokenRequestCount(); got != 1 {
		t.Fatalf("%s installation token requests after issues actions = %d, want still 1", backend.name, got)
	}
	if got := fake.issueRequestCount(); got != 1 {
		t.Fatalf("%s GitHub create issue requests = %d, want 1", backend.name, got)
	}
	if got := fake.labelRequestCount(); got != 1 {
		t.Fatalf("%s GitHub add labels requests = %d, want 1", backend.name, got)
	}

	publishGitHubIssueOpenedExpectStatus(t, backend, bus, gateway, webhookPath, "gh-issue-1", "1001", 42, "Incoming support issue", http.StatusOK)
	fake.requireNoSideEffectCall(t, backend.name, "duplicate issues webhook delivery")
	for _, toolID := range []string{"github.create_issue", "github.add_labels_to_issue"} {
		if got := countGitHubActivityAttemptsForSource(t, backend, toolID, issueEventID); got != 1 {
			t.Fatalf("%s %s attempts after duplicate webhook = %d, want 1", backend.name, toolID, got)
		}
	}
	if got := fake.tokenRequestCount(); got != 1 {
		t.Fatalf("%s token requests after duplicate issues webhook = %d, want still 1", backend.name, got)
	}

	duplicateRequests := []runtimeengine.EmitIntent{
		{Event: loadSlackManagedConnectorActivityRequestEvent(t, backend, createIssueAttempt.RequestEventID)},
		{Event: loadSlackManagedConnectorActivityRequestEvent(t, backend, addLabelsAttempt.RequestEventID)},
	}
	if err := bus.EngineDispatcher().DispatchPostCommit(backend.ctx, duplicateRequests); err != nil {
		t.Fatalf("%s duplicate issue activity request dispatch: %v", backend.name, err)
	}
	waitForInboundBusQuiescence(t, bus)
	fake.requireNoSideEffectCall(t, backend.name, "duplicate issues activity requests")
	if got := fake.tokenRequestCount(); got != 1 {
		t.Fatalf("%s token requests after duplicate issues activities = %d, want still 1", backend.name, got)
	}
	if got := fake.issueRequestCount(); got != 1 {
		t.Fatalf("%s create issue requests after duplicate activities = %d, want still 1", backend.name, got)
	}
	if got := fake.labelRequestCount(); got != 1 {
		t.Fatalf("%s add labels requests after duplicate activities = %d, want still 1", backend.name, got)
	}

	publishGitHubIssueCommentFromSender(t, backend, bus, gateway, webhookPath, "gh-comment-bot-1", "1001", "bot echo", "github-app[bot]", "Bot", http.StatusAccepted)
	botCommentEventID := loadGitHubInboundEventID(t, backend, "inbound.github.issue_comment", "gh-comment-bot-1")
	if got := countGitHubActivityAttemptsForSource(t, backend, "github.create_issue_comment", botCommentEventID); got != 0 {
		t.Fatalf("%s bot-authored comment attempts = %d, want 0", backend.name, got)
	}
	fake.requireNoSideEffectCall(t, backend.name, "bot-authored issue_comment")
	if got := fake.commentRequestCount(); got != 1 {
		t.Fatalf("%s comment requests after bot-authored comment = %d, want still 1", backend.name, got)
	}

	assertGitHubAppIssueWorkflowManagedCredentialFailureBeforeDispatch(t, backend, fake, "missing credential", "gh-comment-missing-credential", "1001", runtimemanagedcredentials.NewMemoryStore())
	mismatched := managedStoreRecordWithInstallation(privateKeyPEM, fake.server.URL, "1001")
	assertGitHubAppIssueWorkflowManagedCredentialFailureBeforeDispatch(t, backend, fake, "installation mismatch", "gh-comment-installation-mismatch", "2002", runtimemanagedcredentials.NewMemoryStore(mismatched))
	_ = pc
	for _, secret := range []string{"expired-install-token", "github-install-token-1", "github-webhook-secret"} {
		assertSlackManagedConnectorNoStoredSecret(t, backend, secret)
	}
}

func newFakeGitHubAppIssueWorkflowServer(t *testing.T, publicKey *rsa.PublicKey) *fakeGitHubAppIssueWorkflowServer {
	t.Helper()
	fake := &fakeGitHubAppIssueWorkflowServer{
		publicKey:   publicKey,
		sideEffects: make(chan githubAppIssueWorkflowCall, 8),
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
	mux.HandleFunc("/repos/octo-org/octo-repo/issues", func(w http.ResponseWriter, r *http.Request) {
		body, ok := fake.acceptSideEffectRequest(t, w, r, "create_issue")
		if !ok {
			return
		}
		fake.issueCalls.Add(1)
		fake.sideEffects <- githubAppIssueWorkflowCall{
			action: "create_issue",
			auth:   r.Header.Get("Authorization"),
			body:   body,
			path:   r.URL.Path,
			raw:    mustMarshalGitHubWorkflowBody(t, body),
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":     456,
			"number": 101,
			"title":  body["title"],
			"body":   body["body"],
		})
	})
	mux.HandleFunc("/repos/octo-org/octo-repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		body, ok := fake.acceptSideEffectRequest(t, w, r, "create_issue_comment")
		if !ok {
			return
		}
		fake.commentCalls.Add(1)
		fake.sideEffects <- githubAppIssueWorkflowCall{
			action: "create_issue_comment",
			auth:   r.Header.Get("Authorization"),
			body:   body,
			path:   r.URL.Path,
			raw:    mustMarshalGitHubWorkflowBody(t, body),
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":   123,
			"body": body["body"],
		})
	})
	mux.HandleFunc("/repos/octo-org/octo-repo/issues/42/labels", func(w http.ResponseWriter, r *http.Request) {
		body, ok := fake.acceptSideEffectRequest(t, w, r, "add_labels_to_issue")
		if !ok {
			return
		}
		fake.labelCalls.Add(1)
		fake.sideEffects <- githubAppIssueWorkflowCall{
			action: "add_labels_to_issue",
			auth:   r.Header.Get("Authorization"),
			body:   body,
			path:   r.URL.Path,
			raw:    mustMarshalGitHubWorkflowBody(t, body),
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"name": "triage"},
			{"name": "swarm"},
		})
	})
	fake.server = httptest.NewServer(mux)
	return fake
}

type fakeGitHubAppIssueWorkflowServer struct {
	server       *httptest.Server
	publicKey    *rsa.PublicKey
	tokenCalls   atomic.Int64
	commentCalls atomic.Int64
	issueCalls   atomic.Int64
	labelCalls   atomic.Int64
	sideEffects  chan githubAppIssueWorkflowCall
}

func (f *fakeGitHubAppIssueWorkflowServer) acceptSideEffectRequest(t *testing.T, w http.ResponseWriter, r *http.Request, action string) (map[string]any, bool) {
	t.Helper()
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return nil, false
	}
	if r.Method != http.MethodPost {
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
		return nil, false
	}
	if got := r.Header.Get("Authorization"); got != "Bearer github-install-token-1" {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{"message": "bad token"})
		return nil, false
	}
	if got := r.Header.Get("Accept"); got != "application/vnd.github+json" {
		http.Error(w, "bad accept", http.StatusBadRequest)
		return nil, false
	}
	if got := r.Header.Get("X-GitHub-Api-Version"); got != "2022-11-28" {
		http.Error(w, "bad api version", http.StatusBadRequest)
		return nil, false
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		http.Error(w, action+": "+err.Error(), http.StatusBadRequest)
		return nil, false
	}
	return body, true
}

func (f *fakeGitHubAppIssueWorkflowServer) requireSideEffectCall(t *testing.T, backend, context string) githubAppIssueWorkflowCall {
	t.Helper()
	calls := f.requireSideEffectCalls(t, backend, context, 1)
	return calls[0]
}

func (f *fakeGitHubAppIssueWorkflowServer) requireSideEffectCalls(t *testing.T, backend, context string, want int) []githubAppIssueWorkflowCall {
	t.Helper()
	calls := make([]githubAppIssueWorkflowCall, 0, want)
	deadline := time.After(connectorSupportedSurfaceAsyncTimeout)
	for len(calls) < want {
		select {
		case call := <-f.sideEffects:
			calls = append(calls, call)
		case <-deadline:
			t.Fatalf("%s %s: timed out waiting for %d fake GitHub side effects; got %d", backend, context, want, len(calls))
		}
	}
	return calls
}

func (f *fakeGitHubAppIssueWorkflowServer) requireNoSideEffectCall(t *testing.T, backend, context string) {
	t.Helper()
	select {
	case call := <-f.sideEffects:
		t.Fatalf("%s %s: unexpected fake GitHub side effect: action=%s auth=%s body=%s", backend, context, call.action, call.auth, call.raw)
	default:
	}
}

func (f *fakeGitHubAppIssueWorkflowServer) tokenRequestCount() int {
	return int(f.tokenCalls.Load())
}

func (f *fakeGitHubAppIssueWorkflowServer) commentRequestCount() int {
	return int(f.commentCalls.Load())
}

func (f *fakeGitHubAppIssueWorkflowServer) issueRequestCount() int {
	return int(f.issueCalls.Load())
}

func (f *fakeGitHubAppIssueWorkflowServer) labelRequestCount() int {
	return int(f.labelCalls.Load())
}

func publishGitHubIssueOpened(t *testing.T, backend slackManagedConnectorBackend, bus *runtimebus.EventBus, gateway *runtimepkg.InboundGateway, webhookPath, deliveryID, installationID string, issueNumber int, title string) {
	t.Helper()
	publishGitHubIssueOpenedExpectStatus(t, backend, bus, gateway, webhookPath, deliveryID, installationID, issueNumber, title, http.StatusAccepted)
}

func publishGitHubIssueOpenedExpectStatus(t *testing.T, backend slackManagedConnectorBackend, bus *runtimebus.EventBus, gateway *runtimepkg.InboundGateway, webhookPath, deliveryID, installationID string, issueNumber int, title string, wantStatus int) {
	t.Helper()
	payload := githubIssueOpenedPayload(installationID, issueNumber, title)
	publishGitHubIssueOpenedWithSignature(t, backend, bus, gateway, webhookPath, deliveryID, installationID, issueNumber, title, githubWebhookSignature("github-webhook-secret", payload), wantStatus)
}

func publishGitHubIssueOpenedWithSignature(t *testing.T, backend slackManagedConnectorBackend, bus *runtimebus.EventBus, gateway *runtimepkg.InboundGateway, webhookPath, deliveryID, installationID string, issueNumber int, title, signature string, wantStatus int) {
	t.Helper()
	payload := githubIssueOpenedPayload(installationID, issueNumber, title)
	req := httptest.NewRequest(http.MethodPost, webhookPath, strings.NewReader(string(payload))).WithContext(backend.ctx)
	if strings.TrimSpace(signature) != "" {
		req.Header.Set("X-Hub-Signature-256", signature)
	}
	req.Header.Set("X-GitHub-Delivery", deliveryID)
	req.Header.Set("X-GitHub-Event", "issues")
	rec := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, gateway, bus, backend.inboundStore, rec, req, backend.runID, backend.entityID, "github", "github-webhook-secret")
	if rec.Code != wantStatus {
		t.Fatalf("%s gateway status for issues delivery %s = %d, want %d body=%s", backend.name, deliveryID, rec.Code, wantStatus, rec.Body.String())
	}
	waitForInboundBusQuiescence(t, bus)
}

func githubIssueOpenedPayload(installationID string, issueNumber int, title string) []byte {
	return []byte(fmt.Sprintf(`{"action":"opened","installation":{"id":%s},"repository":{"name":"octo-repo","owner":{"login":"octo-org"}},"issue":{"number":%d,"title":%q},"sender":{"login":"octocat","type":"User"}}`, installationID, issueNumber, title))
}

func githubAppIssueWorkflowSource(t *testing.T, baseURL, flowInstance string) semanticview.Source {
	t.Helper()
	commentHandler := runtimecontracts.SystemNodeEventHandler{
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
	createIssueHandler := runtimecontracts.SystemNodeEventHandler{
		Guard: &runtimecontracts.GuardSpec{
			Check:  `payload.payload.action == "opened"`,
			OnFail: "discard",
		},
		Activity: runtimecontracts.ActivitySpec{
			ID:   "github_create_issue",
			Tool: "github.create_issue",
			Input: map[string]runtimecontracts.ExpressionValue{
				"installation_id": runtimecontracts.CELExpression("payload.payload.installation.id"),
				"owner":           runtimecontracts.CELExpression("payload.payload.repository.owner.login"),
				"repo":            runtimecontracts.CELExpression("payload.payload.repository.name"),
				"title":           runtimecontracts.CELExpression("payload.payload.issue.title"),
				"body":            runtimecontracts.LiteralExpression("Created by Swarm follow-up"),
			},
		},
	}
	addLabelsHandler := runtimecontracts.SystemNodeEventHandler{
		Guard: &runtimecontracts.GuardSpec{
			Check:  `payload.payload.action == "opened"`,
			OnFail: "discard",
		},
		Activity: runtimecontracts.ActivitySpec{
			ID:   "github_add_labels_to_issue",
			Tool: "github.add_labels_to_issue",
			Input: map[string]runtimecontracts.ExpressionValue{
				"installation_id": runtimecontracts.CELExpression("payload.payload.installation.id"),
				"owner":           runtimecontracts.CELExpression("payload.payload.repository.owner.login"),
				"repo":            runtimecontracts.CELExpression("payload.payload.repository.name"),
				"issue_number":    runtimecontracts.CELExpression("payload.payload.issue.number"),
				"labels":          runtimecontracts.LiteralExpression([]any{"triage", "swarm"}),
			},
		},
	}
	const (
		commentNodeID = "github-issue-comment-responder"
		createNodeID  = "github-issue-creator"
		labelNodeID   = "github-issue-labeler"
	)
	nodes := map[string]runtimecontracts.SystemNodeContract{
		commentNodeID: {
			ID:            commentNodeID,
			ExecutionType: runtimecontracts.SystemNodeExecutionType,
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"inbound.github.issue_comment": commentHandler},
		},
		createNodeID: {
			ID:            createNodeID,
			ExecutionType: runtimecontracts.SystemNodeExecutionType,
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"inbound.github.issues": createIssueHandler},
		},
		labelNodeID: {
			ID:            labelNodeID,
			ExecutionType: runtimecontracts.SystemNodeExecutionType,
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"inbound.github.issues": addLabelsHandler},
		},
	}
	base := semanticview.Wrap(boundedStandingConnectorBundle(flowInstance, &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{
					Events: []string{"inbound.github.issue_comment", "inbound.github.issues"},
				},
			},
		},
		Nodes: nodes,
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "github_app_issue_workflow_supported_surface",
			Version: "1.0.0",
			EffectiveNodes: map[string]runtimecontracts.SystemNodeEffectiveSemantics{
				commentNodeID: {
					ID:                   commentNodeID,
					ExecutionType:        runtimecontracts.SystemNodeExecutionType,
					RuntimeSubscriptions: []string{"inbound.github.issue_comment"},
				},
				createNodeID: {
					ID:                   createNodeID,
					ExecutionType:        runtimecontracts.SystemNodeExecutionType,
					RuntimeSubscriptions: []string{"inbound.github.issues"},
				},
				labelNodeID: {
					ID:                   labelNodeID,
					ExecutionType:        runtimecontracts.SystemNodeExecutionType,
					RuntimeSubscriptions: []string{"inbound.github.issues"},
				},
			},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				commentNodeID: {"inbound.github.issue_comment": commentHandler},
				createNodeID:  {"inbound.github.issues": createIssueHandler},
				labelNodeID:   {"inbound.github.issues": addLabelsHandler},
			},
			EventOwners: map[string][]string{
				"inbound.github.issue_comment": {commentNodeID},
				"inbound.github.issues":        {createNodeID, labelNodeID},
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
						Imports: []runtimecontracts.ConnectorPackImport{
							{Provider: "github", Tool: "github.add_labels_to_issue"},
							{Provider: "github", Tool: "github.create_issue"},
							{Provider: "github", Tool: "github.create_issue_comment"},
						},
					},
				},
			},
		},
	}
	source, err := providerconnectors.SourceWithConnectorPackImportsFromRegistry(importSource, githubAppIssueWorkflowPackRegistry(t, baseURL))
	if err != nil {
		t.Fatalf("SourceWithConnectorPackImportsFromRegistry: %v", err)
	}
	return source
}

func githubAppIssueWorkflowPackRegistry(t *testing.T, baseURL string) *providerconnectors.PackRegistry {
	t.Helper()
	tools := map[string]runtimecontracts.ToolSchemaEntry{}
	for _, toolID := range []string{"github.add_labels_to_issue", "github.create_issue", "github.create_issue_comment"} {
		tool, ok := providerconnectors.BuiltinTool("github", toolID)
		if !ok {
			t.Fatalf("provider connector pack %s not found", toolID)
		}
		if tool.HTTP == nil {
			t.Fatalf("provider connector pack %s missing http block", toolID)
		}
		httpSpec := *tool.HTTP
		tool.HTTP = &httpSpec
		switch toolID {
		case "github.add_labels_to_issue":
			tool.HTTP.URL = strings.TrimRight(baseURL, "/") + "/repos/{{input.owner}}/{{input.repo}}/issues/{{input.issue_number}}/labels"
		case "github.create_issue":
			tool.HTTP.URL = strings.TrimRight(baseURL, "/") + "/repos/{{input.owner}}/{{input.repo}}/issues"
		case "github.create_issue_comment":
			tool.HTTP.URL = strings.TrimRight(baseURL, "/") + "/repos/{{input.owner}}/{{input.repo}}/issues/{{input.issue_number}}/comments"
		}
		tools[toolID] = tool
	}
	registry, err := providerconnectors.NewPackRegistry(providerconnectors.LoadedPack{
		Envelope: packs.Envelope{
			ID: "provider.github.connector",
			Provenance: packs.Provenance{
				Source: packs.ProvenancePlatform,
			},
		},
		Manifest: providerconnectors.ConnectorManifest{
			Provider: "github",
			Tools:    tools,
		},
		Source: "test:provider.github.connector",
	})
	if err != nil {
		t.Fatalf("NewPackRegistry: %v", err)
	}
	return registry
}

func assertGitHubAppIssueWorkflowManagedCredentialFailureBeforeDispatch(t *testing.T, backend slackManagedConnectorBackend, fake *fakeGitHubAppIssueWorkflowServer, label, deliveryID, installationID string, managedStore runtimemanagedcredentials.Store) {
	t.Helper()
	beforeTokens := fake.tokenRequestCount()
	beforeComments := fake.commentRequestCount()
	beforeIssues := fake.issueRequestCount()
	beforeLabels := fake.labelRequestCount()
	source := githubAppIssueWorkflowSource(t, fake.server.URL, backend.flowInstance)
	bus, _ := startSlackManagedConnectorBusAndCoordinator(t, backend, source, managedStore)
	gateway := newTestInboundGateway(t, bus, nil, nil, backend.inboundStore)
	webhookPath := fmt.Sprintf("/webhooks/%s/github", backend.entityID)
	publishGitHubIssueComment(t, backend, bus, gateway, webhookPath, deliveryID, installationID, label)
	inboundEventID := loadGitHubInboundEventID(t, backend, "inbound.github.issue_comment", deliveryID)
	if attempt := waitForGitHubTerminalActivityAttempt(t, backend, "github.create_issue_comment", inboundEventID); attempt.Status != runtimepipeline.ActivityAttemptStatusFailed {
		t.Fatalf("%s %s activity status = %q, want failed", backend.name, label, attempt.Status)
	}
	if got := countGitHubActivityAttemptsForSource(t, backend, "github.create_issue_comment", inboundEventID); got != 1 {
		t.Fatalf("%s %s activity attempts = %d, want one failed claim", backend.name, label, got)
	}
	requireGitHubFailureEventEventually(t, backend, label, boundedProviderFlowID+".github_create_issue_comment.failed", inboundEventID)
	if got := fake.tokenRequestCount(); got != beforeTokens {
		t.Fatalf("%s %s token requests = %d, want still %d", backend.name, label, got, beforeTokens)
	}
	if got := fake.commentRequestCount(); got != beforeComments {
		t.Fatalf("%s %s comment requests = %d, want still %d", backend.name, label, got, beforeComments)
	}
	if got := fake.issueRequestCount(); got != beforeIssues {
		t.Fatalf("%s %s issue requests = %d, want still %d", backend.name, label, got, beforeIssues)
	}
	if got := fake.labelRequestCount(); got != beforeLabels {
		t.Fatalf("%s %s label requests = %d, want still %d", backend.name, label, got, beforeLabels)
	}
	fake.requireNoSideEffectCall(t, backend.name, label)
}

func loadGitHubInboundEventID(t *testing.T, backend slackManagedConnectorBackend, eventName, providerEventID string) string {
	t.Helper()
	var eventID string
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT event_id
			FROM events
			WHERE run_id = ?
			  AND entity_id = ?
			  AND event_name = ?
			  AND json_extract(payload, '$.provider_event_id') = ?
			ORDER BY created_at DESC
			LIMIT 1
		`, backend.runID, backend.entityID, eventName, providerEventID).Scan(&eventID)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT event_id::text
			FROM events
			WHERE run_id = $1::uuid
			  AND entity_id = $2::uuid
			  AND event_name = $3
			  AND payload->>'provider_event_id' = $4
			ORDER BY created_at DESC
			LIMIT 1
		`, backend.runID, backend.entityID, eventName, providerEventID).Scan(&eventID)
	}
	if err != nil {
		t.Fatalf("%s load GitHub inbound event id for %s/%s: %v", backend.name, eventName, providerEventID, err)
	}
	return eventID
}

func waitForGitHubTerminalActivityAttempt(t *testing.T, backend slackManagedConnectorBackend, toolID, sourceEventID string) runtimepipeline.ActivityAttemptRecord {
	t.Helper()
	deadline := time.Now().Add(connectorSupportedSurfaceAsyncTimeout)
	var last runtimepipeline.ActivityAttemptRecord
	var saw bool
	for {
		rec, ok, err := tryLoadGitHubActivityAttempt(backend, toolID, sourceEventID)
		if err != nil {
			t.Fatalf("%s load %s activity attempt while waiting: %v", backend.name, toolID, err)
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
				t.Fatalf("%s %s activity attempt did not reach terminal status; last=%q", backend.name, toolID, last.Status)
			}
			t.Fatalf("%s %s activity attempt for source event %s was not created", backend.name, toolID, sourceEventID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func tryLoadGitHubActivityAttempt(backend slackManagedConnectorBackend, toolID, sourceEventID string) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	var requestEventID string
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT request_event_id
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = ?
			  AND source_event_id = ?
			ORDER BY started_at ASC
			LIMIT 1
		`, backend.runID, toolID, sourceEventID).Scan(&requestEventID)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT request_event_id::text
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = $2
			  AND source_event_id = $3::uuid
			ORDER BY started_at ASC
			LIMIT 1
		`, backend.runID, toolID, sourceEventID).Scan(&requestEventID)
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

func countGitHubActivityAttemptsForSource(t *testing.T, backend slackManagedConnectorBackend, toolID, sourceEventID string) int {
	t.Helper()
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = ?
			  AND tool = ?
			  AND source_event_id = ?
		`, backend.runID, toolID, sourceEventID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM activity_attempts
			WHERE run_id = $1::uuid
			  AND tool = $2
			  AND source_event_id = $3::uuid
		`, backend.runID, toolID, sourceEventID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count %s activity attempts for source event %s: %v", backend.name, toolID, sourceEventID, err)
	}
	return count
}

func requireGitHubFailureEventEventually(t *testing.T, backend slackManagedConnectorBackend, label, eventName, sourceEventID string) {
	t.Helper()
	deadline := time.Now().Add(connectorSupportedSurfaceAsyncTimeout)
	for {
		if got := countGitHubFailureEventsForSource(t, backend, eventName, sourceEventID); got == 1 {
			return
		} else if got > 1 {
			t.Fatalf("%s %s failure events = %d, want 1", backend.name, label, got)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s %s failure event %s for source event %s was not created", backend.name, label, eventName, sourceEventID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func countGitHubFailureEventsForSource(t *testing.T, backend slackManagedConnectorBackend, eventName, sourceEventID string) int {
	t.Helper()
	var count int
	var err error
	if backend.sqlite {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE run_id = ?
			  AND event_name = ?
			  AND source_event_id = ?
		`, backend.runID, eventName, sourceEventID).Scan(&count)
	} else {
		err = backend.db.QueryRowContext(backend.ctx, `
			SELECT COUNT(*)
			FROM events
			WHERE run_id = $1::uuid
			  AND event_name = $2
			  AND source_event_id = $3::uuid
		`, backend.runID, eventName, sourceEventID).Scan(&count)
	}
	if err != nil {
		t.Fatalf("%s count failure events for source event %s: %v", backend.name, sourceEventID, err)
	}
	return count
}

func requireGitHubWorkflowCall(t *testing.T, backend string, calls []githubAppIssueWorkflowCall, action string) githubAppIssueWorkflowCall {
	t.Helper()
	for _, call := range calls {
		if call.action == action {
			return call
		}
	}
	t.Fatalf("%s GitHub side effects = %#v, want action %s", backend, calls, action)
	return githubAppIssueWorkflowCall{}
}

func requireGitHubLabelsBody(t *testing.T, backend string, raw any, want []string) {
	t.Helper()
	values, ok := raw.([]any)
	if !ok {
		t.Fatalf("%s labels body = %#v, want JSON array", backend, raw)
	}
	if len(values) != len(want) {
		t.Fatalf("%s labels body = %#v, want %#v", backend, raw, want)
	}
	for i, value := range values {
		if got := slackManagedConnectorString(value); got != want[i] {
			t.Fatalf("%s labels[%d] = %#v, want %q", backend, i, value, want[i])
		}
	}
}

func mustMarshalGitHubWorkflowBody(t *testing.T, body map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal GitHub workflow body: %v", err)
	}
	return string(raw)
}
