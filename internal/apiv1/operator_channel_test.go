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
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type recordingOperatorChannelIdempotency struct {
	delegate APIIdempotencyStore
	actors   []string
}

type operatorChannelAPICredentialCurrentness struct{}

func (operatorChannelAPICredentialCurrentness) CurrentValueMatchesSeal(_ context.Context, evidence runtimecredentials.ValueEvidence) (bool, error) {
	return evidence.Validate() == nil, nil
}

type directOperatorChannelDestructiveTestAdapter struct {
	service *operatorchannel.Service
	now     time.Time
}

type directOperatorChannelConfirmationTestAdapter struct {
	service *operatorchannel.Service
}

type credentialRequiredOperatorChannelConfirmation struct {
	operationID string
}

func (c credentialRequiredOperatorChannelConfirmation) ConfirmIdentity(context.Context, string, int64, bool, time.Time) (operatorchannel.Operation, operatorchannel.Binding, error) {
	return operatorchannel.Operation{OperationID: uuid.NewString(), State: operatorchannel.StateCredentialStale}, operatorchannel.Binding{}, &channelonboarding.CredentialRequiredError{
		OperationID: c.operationID, Role: "bot_token", StoreKey: "channel.telegram.provider",
	}
}

func (a directOperatorChannelConfirmationTestAdapter) ConfirmIdentity(ctx context.Context, operationID string, revision int64, approve bool, now time.Time) (operatorchannel.Operation, operatorchannel.Binding, error) {
	return a.service.Confirm(ctx, operationID, revision, approve, now)
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
	service, err := operatorchannel.NewService(selected, proofs, operatorChannelAPICredentialCurrentness{}, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
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
		Channels: service, Confirmation: directOperatorChannelConfirmationTestAdapter{service: service}, Destructive: directOperatorChannelDestructiveTestAdapter{service: service, now: now},
		Readback: operatorChannelIdentityReadbackAdapter{service: service}, Idempotency: idempotency, Now: func() time.Time { return now },
	})
	for _, method := range []string{"channel.confirm", "channel.unbind", "channel.proof_revoke", "channel.list"} {
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

func TestOperatorChannelConfirmationCredentialRotationReturnsParentResumeContract(t *testing.T) {
	selected := storetest.StartSQLiteRuntimeStore(t)
	proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service, err := operatorchannel.NewService(selected, proofs, operatorChannelAPICredentialCurrentness{}, nil, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC)
	principal, _, err := service.Bootstrap(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	parentID := uuid.NewString()
	handlers := OperatorChannelHandlers(OperatorChannelHandlerOptions{
		Channels: service, Confirmation: credentialRequiredOperatorChannelConfirmation{operationID: parentID},
		Idempotency: selected, Now: func() time.Time { return now },
	})
	handler := testHandler(t, Options{AuthTokens: []string{testToken}, OperatorPrincipalID: principal.ID, Handlers: handlers})
	response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"stale","method":"channel.confirm","params":{"operation_id":%q,"expected_revision":2,"approve":true}}`, uuid.NewString()))
	requireOperatorChannelAPIErrorCode(t, response, ChannelCredentialRequiredCode)
	details := asMap(t, response.Error.Data)["details"].(map[string]any)
	wantCommand := "swarm channel resume " + parentID + " --credential-stdin"
	if details["operation_id"] != parentID || details["role"] != "bot_token" || details["store_key"] != "channel.telegram.provider" || details["remediation"] != wantCommand {
		t.Fatalf("credential rotation details = %#v", details)
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
