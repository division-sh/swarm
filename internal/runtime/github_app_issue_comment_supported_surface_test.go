package runtime_test

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

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
	handleBoundedProviderDelivery(t, gateway, bus, backend.inboundTarget, rec, req, "github", "github-webhook-secret")
	if rec.Code != wantStatus {
		t.Fatalf("%s gateway status for delivery %s = %d, want %d body=%s", backend.name, deliveryID, rec.Code, wantStatus, rec.Body.String())
	}
	waitForInboundBusQuiescence(t, bus)
}

func requireGitHubAppIssueCommentFailureEventEventually(t *testing.T, backend slackManagedConnectorBackend, label, sourceEventID string) {
	t.Helper()
	deadline := time.Now().Add(connectorSupportedSurfaceAsyncTimeout)
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
	return loadNormalizedProviderEventID(t, backend, "inbound.github.issue_comment_created", providerEventID)
}

func waitForGitHubAppIssueCommentTerminalActivityAttempt(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) runtimepipeline.ActivityAttemptRecord {
	t.Helper()
	deadline := time.Now().Add(connectorSupportedSurfaceAsyncTimeout)
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
	rows, err := runtimeConnectorActivityRowsForSource(backend.ctx, backend.eventStore, backend.runID, "github.create_issue_comment", sourceEventID)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	if len(rows) == 0 {
		return runtimepipeline.ActivityAttemptRecord{}, false, nil
	}
	return backend.activityAttempts.LoadActivityAttempt(backend.ctx, rows[0].RequestEventID)
}

func countGitHubAppIssueCommentActivityAttempts(t *testing.T, backend slackManagedConnectorBackend) int {
	t.Helper()
	rows, err := runtimeConnectorActivityRows(backend.ctx, backend.eventStore, backend.runID, "github.create_issue_comment")
	if err != nil {
		t.Fatalf("%s read exact activity storage: %v", backend.name, err)
	}
	return len(rows)
}

func countGitHubAppIssueCommentActivityAttemptsForSource(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) int {
	t.Helper()
	rows, err := runtimeConnectorActivityRowsForSource(backend.ctx, backend.eventStore, backend.runID, "github.create_issue_comment", sourceEventID)
	if err != nil {
		t.Fatalf("%s read exact activity storage: %v", backend.name, err)
	}
	return len(rows)
}

func countGitHubAppIssueCommentFailureEventsForSource(t *testing.T, backend slackManagedConnectorBackend, sourceEventID string) int {
	t.Helper()
	failureEventType := boundedProviderFlowID + "/github_create_issue_comment.failed"
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
