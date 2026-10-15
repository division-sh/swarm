//go:build linux || darwin

package sessionprovider

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types/events"
)

func TestWhatsAppHeldFirstQRPublicCLIProgressBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, key := range []string{"SWARM_CONFIG", "SWARM_API_SERVER", "SWARM_API_TOKEN", "SWARM_API_TOKEN_FILE"} {
				t.Setenv(key, "")
			}
			t.Setenv("HOME", t.TempDir())
			t.Setenv("XDG_CONFIG_HOME", t.TempDir())
			f := newRuntimeBootstrapFixture(t, backend)
			const token = "held-first-qr-test-token"
			handler, err := apiv1.NewHandler(apiv1.Options{PlatformSpecPath: filepath.Join("..", "..", "platform-spec.yaml"),
				AuthTokens: []string{token}, OperatorPrincipalID: f.principal.ID,
				Handlers: apiv1.ChannelOnboardingHandlers(apiv1.ChannelOnboardingHandlerOptions{Onboarding: f.service, Channels: f.channels})})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			_, started := requestHeldQRPublicResult(t, server, token, "channel.onboarding_start", map[string]any{
				"provider": "whatsapp", "verb": "connect", "save_proof": false, "idempotency_key": "held-first-qr"})
			if f.connection == nil || f.connection.CheckBootstrap(f.ctx) != nil {
				t.Fatal("held-first-batch control did not finish its genuine SDK connect")
			}
			occurrence := f.connection.state.currentOccurrence()
			observed := make(chan struct{}, 1)
			occurrence.client.AddEventHandler(func(event any) {
				if _, ok := event.(*events.QR); ok {
					observed <- struct{}{}
				}
			})
			// This real peer emits no QR until the explicit wire send below.
			// Connect is complete; neither public response can race a first batch.
			params := map[string]any{"operation_id": started.Operation.OperationID}
			for _, method := range []string{"channel.onboarding_get", "channel.onboarding_retry"} {
				raw, result := requestHeldQRPublicResult(t, server, token, method, params)
				code, calls := validateHeldQRResponseViaCLI(t, raw, result.Operation.OperationID, token)
				if code != 0 {
					logHeldQRProgressFailure(t, method, started.Operation, result, code, calls)
					t.Errorf("%s rejected genuine owned pre-first-batch progress with CLI exit %d", method, code)
				}
				if result.Operation.OperationID != started.Operation.OperationID || result.Operation.Revision != started.Operation.Revision ||
					result.Pairing == nil || result.Pairing.Status != channelonboarding.PairingStatusPreparing ||
					result.Pairing.Code != "" || !result.Pairing.ExpiresAt.IsZero() || result.Pairing.Paired || result.Pairing.Connected ||
					result.Operation.SessionAccount != (operatorchannel.SessionAccountAdmission{}) || result.IdentityOperation != nil ||
					result.Readiness != nil && result.Readiness.Ready {
					t.Errorf("%s did not retain exact non-executable preparation", method)
				}
			}
			private, err := occurrence.pairing.read(occurrence.pairing.scope)
			if err != nil || private.Status != pairingAwaiting || private.Code != "" || !private.ExpiresAt.IsZero() {
				t.Fatal("public progress changed the private held-first-batch state")
			}
			socket := pairingSocketFixture(t, f.peer)
			if err := socket.send(f.ctx, pairingIQFixture(waBinary.Node{Tag: "pair-device", Content: []waBinary.Node{
				{Tag: "ref", Content: []byte("released-first-qr")}}})); err != nil {
				t.Fatal("release first SDK QR batch", err)
			}
			select {
			case <-observed:
			case <-f.peer.ctx.Done():
				t.Fatal("released SDK QR callback did not complete")
			}
			raw, current := requestHeldQRPublicResult(t, server, token, "channel.onboarding_get", params)
			code, calls := validateHeldQRResponseViaCLI(t, raw, current.Operation.OperationID, token)
			if code != 0 {
				logHeldQRProgressFailure(t, "released_get", started.Operation, current, code, calls)
				t.Error("ordinary released current-QR progress was refused")
			}
			if current.Operation.OperationID != started.Operation.OperationID || current.Operation.Revision != started.Operation.Revision ||
				current.Pairing == nil || current.Pairing.Status != "awaiting_qr" || current.Pairing.Code == "" ||
				!current.Pairing.ExpiresAt.After(time.Now()) || current.Pairing.Paired || current.Pairing.Connected ||
				current.IdentityOperation != nil || current.Readiness != nil && current.Readiness.Ready {
				t.Fatal("released first QR changed responsibility or granted authority")
			}
		})
	}
}

func requestHeldQRPublicResult(t *testing.T, server *httptest.Server, token, method string, params map[string]any) (json.RawMessage, channelonboarding.Result) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "first-qr-cut", "method": method, "params": params})
	if err != nil {
		t.Fatal("encode controlled public request")
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/v1/rpc", bytes.NewReader(body))
	if err != nil {
		t.Fatal("construct controlled public request")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal("controlled public request failed")
	}
	defer response.Body.Close()
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if json.NewDecoder(response.Body).Decode(&envelope) != nil || len(envelope.Error) != 0 || len(envelope.Result) == 0 {
		t.Fatal("controlled public response refused or malformed")
	}
	var result channelonboarding.Result
	if json.Unmarshal(envelope.Result, &result) != nil {
		t.Fatal("decode controlled public result")
	}
	return envelope.Result, result
}

// Each captured real public result is separately fed through the CLI's private
// validateCLIChannelProgress boundary. Replay is only an in-memory consumer
// measurement, not another live onboarding retry or a transport qualification.
func validateHeldQRResponseViaCLI(t *testing.T, raw json.RawMessage, operationID, token string) (int, int64) {
	t.Helper()
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || r.Header.Get("Authorization") != "Bearer "+token ||
			(request.Method != "channel.onboarding_get" && request.Method != "channel.onboarding_retry") {
			http.Error(w, "invalid controlled CLI request", http.StatusBadRequest)
			return
		}
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": raw})
	}))
	defer server.Close()
	file := filepath.Join(t.TempDir(), "api-token")
	if os.WriteFile(file, []byte(token+"\n"), 0o600) != nil {
		t.Fatal("write controlled CLI token")
	}
	var stdout, stderr bytes.Buffer
	code := cliapp.Execute(t.Context(), []string{"channel", "resume", operationID, "--api-server", server.URL,
		"--api-token-file", file, "--json"}, &stdout, &stderr, nil, nil)
	return code, calls.Load()
}

func logHeldQRProgressFailure(t *testing.T, response string, original channelonboarding.Operation, result channelonboarding.Result, code int, calls int64) {
	t.Helper()
	c, o, p := result.Candidate, result.Operation, result.Pairing
	fields := map[string]any{"response": response, "cli_exit": code, "cli_calls": calls,
		"phase": o.Phase, "revision": o.Revision, "candidate_present": c != nil,
		"same_operation": o.OperationID == original.OperationID, "same_coordinate": o.Coordinate == original.Coordinate,
		"account_present":  o.SessionAccount != (operatorchannel.SessionAccountAdmission{}),
		"identity_present": result.IdentityOperation != nil || o.IdentityOperationID != "", "ready": result.Readiness != nil && result.Readiness.Ready}
	if c != nil {
		fields["candidate_valid"] = c.ValidateDeclaration() == nil
		fields["provider_match"], fields["interface_match"] = c.Provider == o.Provider, c.Interface == o.Interface
		fields["coordinate_match"], fields["target_match"] = c.Coordinate.MatchesDeclaration(o.Coordinate), c.Target.Selector == o.TargetSelector
		fields["posture_match"], fields["ceremony_match"] = c.Posture == o.Posture, c.Ceremony == o.Ceremony
	}
	if p != nil {
		fields["status"], fields["has_code"], fields["has_expiry"] = p.Status, p.Code != "", !p.ExpiresAt.IsZero()
		fields["paired"], fields["connected"] = p.Paired, p.Connected
	}
	_, declarationErr := result.ProviderCredentialRequirement()
	fields["selected_declaration_valid"] = declarationErr == nil
	raw, _ := json.Marshal(fields)
	t.Logf("first-QR failure-only discriminator %s", raw)
}
