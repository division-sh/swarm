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

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type recordingChannelOnboardingLifecycle struct {
	selection channelonboarding.StartInput
	start     channelonboarding.StartInput
	retry     channelonboarding.RetryInput
	get       string
	result    channelonboarding.Result
	startErr  error
	retryErr  error
}

func (l *recordingChannelOnboardingLifecycle) Select(_ context.Context, input channelonboarding.StartInput) (channelonboarding.StartSelection, error) {
	l.selection = input
	return channelonboarding.StartSelection{Candidate: *l.result.Candidate, SelectionFence: "selected-declaration"}, l.startErr
}

func TestChannelClientLanguageParameterDoesNotGuessOrNormalize(t *testing.T) {
	for _, value := range []any{nil, false, 123, "", " fr", "fr ", "\tfr"} {
		if language, err := channelClientLanguageParam(map[string]any{"client_language": value}); err == nil {
			t.Fatalf("invalid declaration %v became %q", value, language)
		}
	}
	for _, language := range []string{"en", "fr", "de", "FR"} {
		actual, err := channelClientLanguageParam(map[string]any{"client_language": language})
		if err != nil || actual != language {
			t.Fatalf("parameter boundary changed provider-owned language %q: %q %v", language, actual, err)
		}
	}
	if language, err := channelClientLanguageParam(nil); err != nil || language != "" {
		t.Fatalf("absent declaration invented language %q: %v", language, err)
	}
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
	ctx := apiTestOriginalProcessContext(t, context.Background(), selected)
	proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	identity := operatorchannel.InterfaceIdentity{
		InterfaceRef: operatorchannel.InterfaceHITLChannelV2, ChannelPackID: "provider.telegram.hitl_channel",
		ChannelPackVersion: "1.0.0", ChannelManifestHash: "sha256:" + strings.Repeat("a", 64),
		SemanticGeneration: "channel-onboarding-api-contract",
	}.Normalized()
	channels, err := operatorchannel.NewService(selected, proofs, operatorChannelAPICredentialCurrentness{}, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	principal, _, err := channels.Bootstrap(ctx, time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC))
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
		ContextPublicationGeneration: 1, PlanGeneration: planGeneration,
	}
	lifecycle := &recordingChannelOnboardingLifecycle{result: channelonboarding.Result{
		Operation: channelonboarding.Operation{OperationID: operationID, Verb: channelonboarding.VerbConnect, Provider: "telegram", Interface: identity, Coordinate: coordinate},
		Candidate: &channelonboarding.Candidate{
			Provider: "telegram", Interface: identity, Coordinate: coordinate,
			Target: channelonboarding.CandidateTarget{Selector: "ingress:support/telegram:telegram", FlowPath: "support/telegram", Alias: "telegram", Provider: "telegram", AdmissionGeneration: triggergeneration.FromCanonicalBytes([]byte("catalog"))},
		},
	}}
	deploymentCalls := 0
	handlers := ChannelOnboardingHandlers(ChannelOnboardingHandlerOptions{Onboarding: lifecycle, Channels: channels,
		Deployment: func(ctx context.Context, req Request) (any, error) {
			deploymentCalls++
			revision, ok := req.Params["expected_locale_revision"].(int64)
			if !ok {
				return nil, fmt.Errorf("locale revision was not normalized")
			}
			return lifecycle.Retry(ctx, channelonboarding.RetryInput{OperationID: operationID,
				ClientLanguage: req.Params["client_language"].(string), ExpectedLocaleRevision: revision})
		}})
	for method, handler := range OperatorChannelHandlers(OperatorChannelHandlerOptions{
		Channels: channels, Confirmation: directOperatorChannelConfirmationTestAdapter{service: channels},
		Destructive: directOperatorChannelDestructiveTestAdapter{service: channels},
		Readback:    operatorChannelIdentityReadbackAdapter{service: channels}, Idempotency: selected,
		Deployment: func(context.Context, Request) (any, error) {
			deploymentCalls++
			return nil, fmt.Errorf("unexpected deployment dispatch")
		},
	}) {
		handlers[method] = handler
	}
	for _, method := range []string{"channel.onboarding_select", "channel.onboarding_start", "channel.onboarding_get", "channel.onboarding_retry"} {
		if handlers[method] == nil {
			t.Fatalf("channel onboarding handler %s is missing", method)
		}
	}
	handler := testHandler(t, Options{AuthTokens: []string{testToken}, OperatorPrincipalID: principal.ID, Handlers: handlers})
	bindAPITestHandlerOriginalProcess(t, handler, ctx)
	for _, cell := range []struct{ method, params string }{
		{"channel.onboarding_select", `"verb":"connect","provider":"telegram"`},
		{"channel.onboarding_start", `"verb":"connect","provider":"telegram"`},
		{"channel.onboarding_get", `"operation_id":"` + operationID + `"`},
		{"channel.onboarding_retry", `"operation_id":"` + operationID + `"`},
		{"channel.confirm", `"operation_id":"` + operationID + `","expected_revision":1,"approve":true`},
		{"channel.logout", `"operation_id":"` + operationID + `","expected_revision":1`},
		{"channel.list", `"provider":"telegram"`},
	} {
		if handlers[cell.method] == nil {
			t.Fatalf("missing channel handler: %s", cell.method)
		}
		response := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"obsolete-holder","method":"`+cell.method+`","params":{"scope":"deployment","holder":{},`+cell.params+`}}`)
		if response.Error == nil || response.Error.Code != -32602 || deploymentCalls != 0 {
			t.Fatal("obsolete holder reached deployment dispatch", cell.method, response, deploymentCalls)
		}
		raw, err := json.Marshal(response.Error)
		if err != nil || !strings.Contains(string(raw), `"field":"holder"`) || !strings.Contains(string(raw), `"reason":"unknown parameter"`) {
			t.Fatal("holder was not refused by ordinary descriptor admission", cell.method, string(raw), err)
		}
	}

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"auth","method":"channel.onboarding_get","params":{"operation_id":"`+operationID+`"}}`)))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized onboarding status = %d, want 401", unauthorized.Code)
	}
	selection := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"selection","method":"channel.onboarding_select","params":{"verb":"reconnect","provider":"telegram"}}`)
	if selection.Error != nil || lifecycle.selection.Verb != channelonboarding.VerbReconnect || lifecycle.start.Verb != "" {
		t.Fatal("selection invoked Start or lost its request", selection.Error, lifecycle.selection)
	}
	for _, cell := range []struct {
		err  error
		code string
	}{
		{fmt.Errorf("%w: no matching candidate", channelonboarding.ErrNotFound), ChannelInterfaceNotFoundCode},
		{fmt.Errorf("%w: ambiguous candidate", channelonboarding.ErrConflict), ChannelInterfaceAmbiguousCode},
		{fmt.Errorf("%w: changed selection", channelonboarding.ErrConflict), ChannelBindingConflictCode},
	} {
		lifecycle.startErr = cell.err
		refused := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"selection-refusal","method":"channel.onboarding_select","params":{"verb":"reconnect","provider":"telegram"}}`)
		requireOperatorChannelAPIErrorCode(t, refused, cell.code)
	}
	lifecycle.startErr = nil

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
	if lifecycle.retry.ApproveAccount {
		t.Fatal("absent account answer became approval")
	}
	localeRequest := func(revision string) rpcResponse {
		return rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"deployment-locale","method":"channel.onboarding_retry","params":{"scope":"deployment","operation_id":%q,"client_language":"fr","expected_locale_revision":%s}}`, operationID, revision))
	}
	locale := localeRequest("3")
	if locale.Error != nil || deploymentCalls != 1 || lifecycle.retry.ExpectedLocaleRevision != 3 || lifecycle.retry.ClientLanguage != "fr" {
		t.Fatal("served deployment locale lost its JSON-number revision", locale, lifecycle.retry)
	}
	for _, test := range []struct {
		revision string
		code     int
	}{{"-1", -32602}, {"1.5", -32602}, {"true", -32602}, {`"3"`, -32602}, {"1e30", -32600}} {
		invalid := localeRequest(test.revision)
		if invalid.Error == nil || invalid.Error.Code != test.code || deploymentCalls != 1 {
			t.Fatal("invalid deployment locale revision reached its owner", test.revision, invalid)
		}
	}
	lifecycle.retryErr = channelonboarding.ErrRevisionConflict
	staleLocale := localeRequest("2")
	requireOperatorChannelAPIErrorCode(t, staleLocale, ChannelRevisionConflictCode)
	if lifecycle.retry.ExpectedLocaleRevision != 2 || deploymentCalls != 2 {
		t.Fatal("deployment locale changed its stale revision before owner refusal")
	}
	lifecycle.retryErr = nil
	for _, answer := range []bool{false, true} {
		response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"answer","method":"channel.onboarding_retry","params":{"operation_id":%q,"approve_account":%t}}`, operationID, answer))
		if response.Error != nil || lifecycle.retry.ApproveAccount != answer {
			t.Fatal("retry changed the supplied account answer", answer, response.Error)
		}
	}
	unset := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"unset-answer","method":"channel.onboarding_retry","params":{"operation_id":%q,"approve_account":null}}`, operationID))
	if unset.Error != nil || lifecycle.retry.ApproveAccount {
		t.Fatal("optional null answer became approval", unset.Error)
	}
	for _, value := range []string{`"true"`, `1`} {
		response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"invalid-answer","method":"channel.onboarding_retry","params":{"operation_id":%q,"approve_account":%s}}`, operationID, value))
		if response.Error == nil || response.Error.Code != -32602 {
			t.Fatal("retry accepted a malformed account answer", value, response)
		}
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
