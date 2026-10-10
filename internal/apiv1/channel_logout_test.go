package apiv1

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// This adapter isolates API admission. It does not claim SDK launch, journal
// settlement or served logout coverage.
type logoutAPIAdmissionProbe struct {
	directOperatorChannelDestructiveTestAdapter
	result  channelonboarding.SessionLogoutReadback
	failure error
	keys    []string
	hashes  []string
}

func (p *logoutAPIAdmissionProbe) Logout(_ context.Context, id string, revision int64, key, hash string) (channelonboarding.SessionLogoutReadback, error) {
	p.keys = append(p.keys, key)
	p.hashes = append(p.hashes, hash)
	if id != p.result.OperationID || revision != p.result.ExpectedRevision {
		return channelonboarding.SessionLogoutReadback{}, channelonboarding.ErrRevisionConflict
	}
	return p.result, p.failure
}

func TestChannelLogoutAPIPrincipalAndRevisionAdmission(t *testing.T) {
	selected := storetest.StartSQLiteRuntimeStore(t)
	proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	channels, err := operatorchannel.NewService(selected, proofs, operatorChannelAPICredentialCurrentness{}, nil, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	principal, _, err := channels.Bootstrap(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	identity := operatorchannel.InterfaceIdentity{InterfaceRef: operatorchannel.InterfaceHITLChannelV2,
		ChannelPackID: "provider.whatsapp.hitl_channel", ChannelPackVersion: "0.1.0",
		ChannelManifestHash: "sha256:logout-api-proof", SemanticGeneration: "logout-api-proof"}.Normalized()
	operationID, teardownID := uuid.NewString(), uuid.NewString()
	effectID, err := channelonboarding.SessionLogoutEffectOperationID(teardownID)
	if err != nil {
		t.Fatal(err)
	}
	probe := &logoutAPIAdmissionProbe{result: channelonboarding.SessionLogoutReadback{
		OperationID: operationID, ExpectedRevision: 7, EffectOperationID: effectID,
		Teardown: channelonboarding.TeardownOperation{TeardownID: teardownID, Kind: channelonboarding.TeardownLogout,
			PrincipalID: principal.ID, Scope: channelonboarding.TeardownScope{Interface: identity},
			Phase: channelonboarding.TeardownAuthorityRetired, Revision: 1, RequestedAt: now, UpdatedAt: now},
	}}
	opts := OperatorChannelHandlerOptions{Channels: channels, Destructive: probe, Idempotency: selected, Now: func() time.Time { return now }}
	handlers := OperatorChannelHandlers(opts)
	const rotated = "logout-api-rotated-token"
	handler := testHandler(t, Options{AuthTokens: []string{testToken, rotated}, OperatorPrincipalID: principal.ID, Handlers: handlers})
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"logout","method":"channel.logout","params":{"operation_id":%q,"expected_revision":7,"idempotency_key":"exact-logout"}}`, operationID)
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(body)))
	if unauthorized.Code != http.StatusUnauthorized || len(probe.keys) != 0 {
		t.Fatal("unauthenticated request reached destruction")
	}
	for _, params := range []string{
		`{}`, fmt.Sprintf(`{"operation_id":%q}`, operationID),
		fmt.Sprintf(`{"operation_id":%q,"expected_revision":0}`, operationID),
		fmt.Sprintf(`{"operation_id":%q,"expected_revision":7,"account":"invented"}`, operationID),
		fmt.Sprintf(`{"operation_id":%q,"expected_revision":7,"session_path":"/invented"}`, operationID),
	} {
		response := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"invalid","method":"channel.logout","params":`+params+`}`)
		if response.Error == nil || len(probe.keys) != 0 {
			t.Fatalf("invalid request reached destruction: %+v", response)
		}
	}
	for _, token := range []string{testToken, testToken, rotated} {
		response := operatorChannelRPCCallWithToken(t, handler, token, body)
		if response.Error != nil {
			t.Fatalf("exact authenticated admission: %+v", response.Error)
		}
		result := asMap(t, response.Result)
		if result["operation_id"] != operationID || result["effect_operation_id"] != effectID || asMap(t, result["teardown"])["phase"] != "authority_retired" {
			t.Fatalf("durable pending readback changed: %+v", result)
		}
		openRPC, _ := loadComplianceOpenRPC(t, complianceOpenRPCPath(repoRoot(t)))
		newOpenRPCResultSchemaValidator(t, openRPC).validateMethodResult(t, "channel.logout", result)
	}
	if len(probe.keys) != 2 || probe.keys[0] != probe.keys[1] || probe.hashes[0] != probe.hashes[1] {
		t.Fatalf("API replay or principal identity diverged across token rotation: %+v %+v", probe.keys, probe.hashes)
	}
	changed := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"changed","method":"channel.logout","params":{"operation_id":%q,"expected_revision":8,"idempotency_key":"exact-logout"}}`, operationID))
	requireOperatorChannelAPIErrorCode(t, changed, "IDEMPOTENCY_CONFLICT")
	if _, err := handlers["channel.logout"](context.Background(), Request{Method: "channel.logout", OperatorPrincipalID: uuid.NewString(),
		Params: map[string]any{"operation_id": operationID, "expected_revision": 7}}); err == nil || len(probe.keys) != 2 {
		t.Fatal("foreign principal reached destruction")
	}
	for i, tc := range []struct {
		err  error
		code string
	}{
		{channelonboarding.ErrNotFound, ChannelOperationNotFoundCode},
		{channelonboarding.ErrRevisionConflict, ChannelRevisionConflictCode},
		{channelonboarding.ErrConflict, ChannelBindingConflictCode},
	} {
		probe.failure = tc.err
		response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"failure","method":"channel.logout","params":{"operation_id":%q,"expected_revision":7,"idempotency_key":"failure-%d"}}`, operationID, i))
		requireOperatorChannelAPIErrorCode(t, response, tc.code)
	}
}
