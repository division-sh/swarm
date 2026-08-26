package apiv1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type recordingOperatorChannelIdempotency struct {
	delegate APIIdempotencyStore
	actors   []string
}

type directOperatorChannelDestructiveTestAdapter struct {
	service *operatorchannel.Service
	now     time.Time
}

type operatorChannelIdentityReadbackAdapter struct {
	service *operatorchannel.Service
}

func (a operatorChannelIdentityReadbackAdapter) ReadbackConnectedChannels(ctx context.Context) ([]channelonboarding.ConnectedChannelReadback, error) {
	identities, err := a.service.Readback(ctx)
	if err != nil {
		return nil, err
	}
	rows := make([]channelonboarding.ConnectedChannelReadback, 0, len(identities))
	for _, identity := range identities {
		rows = append(rows, channelonboarding.ConnectedChannelReadback{Identity: identity})
	}
	return rows, nil
}

func (a directOperatorChannelDestructiveTestAdapter) Unbind(ctx context.Context, selector string, revision int64, requestKey, requestHash string) (operatorchannel.Operation, operatorchannel.Binding, error) {
	return a.service.Unbind(ctx, selector, revision, requestKey, requestHash, a.now)
}

func (a directOperatorChannelDestructiveTestAdapter) RevokeProof(ctx context.Context, selector string, revision int64, _, _ string) (operatorchannel.VerifiedProof, error) {
	return a.service.RevokeProof(ctx, selector, revision, a.now)
}

func (s *recordingOperatorChannelIdempotency) WithAPIIdempotency(ctx context.Context, req apiidempotency.Request, execute func(context.Context) (apiidempotency.Completion, error)) (apiidempotency.Completion, bool, error) {
	s.actors = append(s.actors, req.ActorTokenID)
	return s.delegate.WithAPIIdempotency(ctx, req, execute)
}

func TestOperatorChannelAPIContractEvidence(t *testing.T) {
	selected := storetest.StartSQLiteRuntimeStore(t)
	proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	identity := operatorchannel.InterfaceIdentity{
		InterfaceRef: operatorchannel.InterfaceHITLChannelV2, ChannelPackID: "provider.telegram.hitl_channel",
		ChannelPackVersion: "1.0.0", ChannelManifestHash: "sha256:" + strings.Repeat("a", 64),
		SemanticGeneration: "operator-channel-api-contract",
	}.Normalized()
	service, err := operatorchannel.NewService(selected, proofs, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	principal, _, err := service.Bootstrap(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	idempotency := &recordingOperatorChannelIdempotency{delegate: selected}
	handlers := OperatorChannelHandlers(OperatorChannelHandlerOptions{
		Channels: service, Destructive: directOperatorChannelDestructiveTestAdapter{service: service, now: now},
		Readback: operatorChannelIdentityReadbackAdapter{service: service}, Idempotency: idempotency, Now: func() time.Time { return now },
	})
	for _, method := range []string{"channel.connect", "channel.reconnect", "channel.rebind", "channel.confirm", "channel.unbind", "channel.proof_revoke", "channel.list"} {
		if handlers[method] == nil {
			t.Fatalf("operator channel handler %s is missing", method)
		}
	}
	const rotatedToken = "operator-channel-rotated-token"
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken, rotatedToken}, OperatorPrincipalID: principal.ID, Handlers: handlers,
	})

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"auth","method":"channel.list","params":{}}`)))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized channel.list status = %d, want 401", unauthorized.Code)
	}

	list := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"list","method":"channel.list","params":{}}`)
	if list.Error != nil {
		t.Fatalf("channel.list error = %#v", list.Error)
	}
	listed := asMap(t, list.Result)
	if listed["principal_id"] != principal.ID || len(listed["channels"].([]any)) != 1 {
		t.Fatalf("channel.list result = %#v", listed)
	}

	for _, body := range []string{
		`{"jsonrpc":"2.0","id":"missing","method":"channel.connect","params":{"interface":"` + identity.Selector + `"}}`,
		`{"jsonrpc":"2.0","id":"unknown","method":"channel.connect","params":{"interface":"` + identity.Selector + `","expected_revision":0,"unexpected":true}}`,
	} {
		response := rpcCall(t, handler, body)
		if response.Error == nil || response.Error.Code != -32602 {
			t.Fatalf("channel.connect admission response = %#v, want invalid params", response)
		}
	}

	shortSelector := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"short","method":"channel.connect","params":{"interface":"provider.telegram.hitl_channel","expected_revision":0}}`)
	requireOperatorChannelAPIErrorCode(t, shortSelector, ChannelInterfaceNotFoundCode)

	connectBody := fmt.Sprintf(`{"jsonrpc":"2.0","id":"connect","method":"channel.connect","params":{"interface":%q,"expected_revision":0,"save_proof":true,"idempotency_key":"connect-key"}}`, identity.Selector)
	connected := rpcCall(t, handler, connectBody)
	if connected.Error != nil {
		t.Fatalf("channel.connect error = %#v", connected.Error)
	}
	operation := asMap(t, asMap(t, connected.Result)["operation"])
	if operation["state"] != string(operatorchannel.StateAwaitingClaim) || operation["challenge"] == "" {
		t.Fatalf("channel.connect operation = %#v", operation)
	}
	for _, absent := range []string{"claimed_at", "completed_at"} {
		if _, present := operation[absent]; present {
			t.Fatalf("channel.connect operation exposed zero %s: %#v", absent, operation)
		}
	}
	if _, present := operation["expires_at"]; !present {
		t.Fatalf("channel.connect operation omitted challenge expiry: %#v", operation)
	}
	replayed := rpcCall(t, handler, connectBody)
	if replayed.Error != nil || asMap(t, asMap(t, replayed.Result)["operation"])["operation_id"] != operation["operation_id"] {
		t.Fatalf("channel.connect replay = %#v", replayed)
	}
	changed := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"changed","method":"channel.connect","params":{"interface":%q,"expected_revision":0,"save_proof":false,"idempotency_key":"connect-key"}}`, identity.Selector))
	requireOperatorChannelAPIErrorCode(t, changed, IdempotencyConflictCode)

	missingProof := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"missing-proof","method":"channel.proof_revoke","params":{"interface":%q,"expected_revision":1,"idempotency_key":"missing-proof"}}`, identity.Selector))
	requireOperatorChannelAPIErrorCode(t, missingProof, ChannelProofUnavailableCode)

	idempotency.actors = nil
	missingOperationID := uuid.NewString()
	for index, token := range []string{testToken, rotatedToken} {
		response := operatorChannelRPCCallWithToken(t, handler, token, fmt.Sprintf(`{"jsonrpc":"2.0","id":"token-%d","method":"channel.confirm","params":{"operation_id":%q,"expected_revision":1,"approve":true,"idempotency_key":"token-scope-%d"}}`, index, missingOperationID, index))
		requireOperatorChannelAPIErrorCode(t, response, ChannelOperationNotFoundCode)
	}
	if len(idempotency.actors) != 2 || idempotency.actors[0] != actorTokenID(testToken) || idempotency.actors[1] != actorTokenID(rotatedToken) || idempotency.actors[0] == idempotency.actors[1] {
		t.Fatalf("operator channel idempotency actors = %#v, want distinct bearer-token occurrences", idempotency.actors)
	}
}

func operatorChannelRPCCallWithToken(t *testing.T, handler *Handler, token, body string) rpcResponse {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(recorder, request)
	var response rpcResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode operator channel RPC response: %v", err)
	}
	return response
}

func requireOperatorChannelAPIErrorCode(t *testing.T, response rpcResponse, want string) {
	t.Helper()
	if response.Error == nil {
		t.Fatalf("operator channel response = %#v, want %s", response, want)
	}
	data := asMap(t, response.Error.Data)
	if data["code"] != want {
		t.Fatalf("operator channel error code = %#v, want %s", data["code"], want)
	}
}
