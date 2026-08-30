package apiv1

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type recordingChannelOnboardingLifecycle struct {
	start    channelonboarding.StartInput
	retry    channelonboarding.RetryInput
	get      string
	result   channelonboarding.Result
	startErr error
	retryErr error
}

func (l *recordingChannelOnboardingLifecycle) Start(_ context.Context, input channelonboarding.StartInput) (channelonboarding.Result, error) {
	l.start = input
	return l.result, l.startErr
}

func (l *recordingChannelOnboardingLifecycle) Get(_ context.Context, operationID string) (channelonboarding.Result, error) {
	l.get = operationID
	return l.result, nil
}

func (l *recordingChannelOnboardingLifecycle) Retry(_ context.Context, input channelonboarding.RetryInput) (channelonboarding.Result, error) {
	l.retry = input
	return l.result, l.retryErr
}

func TestChannelOnboardingAPIContractEvidence(t *testing.T) {
	selected := storetest.StartSQLiteRuntimeStore(t)
	proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	identity := operatorchannel.InterfaceIdentity{
		InterfaceRef: operatorchannel.InterfaceHITLChannelV2, ChannelPackID: "provider.telegram.hitl_channel",
		ChannelPackVersion: "1.0.0", ChannelManifestHash: "sha256:" + strings.Repeat("a", 64),
		SemanticGeneration: "channel-onboarding-api-contract",
	}.Normalized()
	channels, err := operatorchannel.NewService(selected, proofs, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	principal, _, err := channels.Bootstrap(context.Background(), time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	operationID := uuid.NewString()
	planGeneration, err := plangeneration.FromCanonicalValue(map[string]string{"test": "channel-onboarding-api"})
	if err != nil {
		t.Fatal(err)
	}
	coordinate := channelonboarding.ChannelRuntimeContextCoordinate{
		BundleHash: "bundle-v2:sha256:" + strings.Repeat("b", 64), BundleIdentity: "support@1.0.0#bundle",
		PackInventoryGeneration: "inventory", RuntimeInstanceID: "11111111-1111-4111-8111-111111111111",
		ContextPublicationGeneration: 1, PlanGeneration: planGeneration, TargetGeneration: 1,
	}
	lifecycle := &recordingChannelOnboardingLifecycle{result: channelonboarding.Result{
		Operation: channelonboarding.Operation{OperationID: operationID, Verb: channelonboarding.VerbConnect, Provider: "telegram", Interface: identity, Coordinate: coordinate},
		Candidate: channelonboarding.Candidate{
			Provider: "telegram", Interface: identity, Coordinate: coordinate,
			Target: channelonboarding.CandidateTarget{Selector: "ingress:support/telegram:telegram", ServiceID: uuid.NewString(), FlowPath: "support/telegram", Alias: "telegram", Provider: "telegram", Generation: 1, PublicationSequence: 1, AdmissionGeneration: triggergeneration.FromCanonicalBytes([]byte("catalog"))},
		},
	}}
	handlers := ChannelOnboardingHandlers(ChannelOnboardingHandlerOptions{Onboarding: lifecycle, Channels: channels})
	for _, method := range []string{"channel.onboarding_start", "channel.onboarding_get", "channel.onboarding_retry"} {
		if handlers[method] == nil {
			t.Fatalf("channel onboarding handler %s is missing", method)
		}
	}
	handler := testHandler(t, Options{AuthTokens: []string{testToken}, OperatorPrincipalID: principal.ID, Handlers: handlers})

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"auth","method":"channel.onboarding_get","params":{"operation_id":"`+operationID+`"}}`)))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized onboarding status = %d, want 401", unauthorized.Code)
	}

	start := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"start","method":"channel.onboarding_start","params":{"verb":"connect","provider":"telegram","bundle":"bundle-v2:sha256:`+strings.Repeat("b", 64)+`","interface":"`+identity.Selector+`","target":"ingress:support:telegram","provider_credential":"private-token","save_proof":false,"idempotency_key":"journey"}}`)
	if start.Error != nil {
		t.Fatalf("channel.onboarding_start error = %#v", start.Error)
	}
	if lifecycle.start.Verb != channelonboarding.VerbConnect || lifecycle.start.Selection.Provider != "telegram" || lifecycle.start.ProviderCredential != "private-token" || lifecycle.start.SaveProof {
		t.Fatalf("channel.onboarding_start input = %#v", lifecycle.start)
	}

	get := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"get","method":"channel.onboarding_get","params":{"operation_id":"`+operationID+`"}}`)
	if get.Error != nil || lifecycle.get != operationID {
		t.Fatalf("channel.onboarding_get response=%#v operation=%q", get, lifecycle.get)
	}
	retry := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"retry","method":"channel.onboarding_retry","params":{"operation_id":"`+operationID+`","provider_credential":"retry-token","idempotency_key":"retry"}}`)
	if retry.Error != nil || lifecycle.retry.OperationID != operationID || lifecycle.retry.ProviderCredential != "retry-token" {
		t.Fatalf("channel.onboarding_retry response=%#v input=%#v", retry, lifecycle.retry)
	}

	for _, body := range []string{
		`{"jsonrpc":"2.0","id":"missing","method":"channel.onboarding_start","params":{"provider":"telegram"}}`,
		`{"jsonrpc":"2.0","id":"unknown","method":"channel.onboarding_get","params":{"operation_id":"` + operationID + `","unexpected":true}}`,
	} {
		response := rpcCall(t, handler, body)
		if response.Error == nil || response.Error.Code != -32602 {
			t.Fatalf("onboarding invalid params response = %#v", response)
		}
	}

	lifecycle.startErr = &channelonboarding.CredentialRequiredError{OperationID: operationID, Role: "telegram_bot_token", StoreKey: "telegram_bot_token"}
	credentialRequired := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"credential","method":"channel.onboarding_start","params":{"verb":"connect","provider":"telegram"}}`)
	requireOperatorChannelAPIErrorCode(t, credentialRequired, ChannelCredentialRequiredCode)
	details := asMap(t, asMap(t, credentialRequired.Error.Data)["details"])
	if details["operation_id"] != operationID || details["role"] != "telegram_bot_token" || details["store_key"] != "telegram_bot_token" || details["remediation"] != "swarm channel resume "+operationID+" --credential-stdin" {
		t.Fatalf("credential-required details = %#v", details)
	}
}
