package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
)

type channelCredentialInput struct {
	reads int
	value string
}

func (in *channelCredentialInput) Read(p []byte) (int, error) {
	in.reads++
	if in.value == "" {
		return 0, errors.New("secret-input-must-not-be-read")
	}
	n := copy(p, in.value)
	in.value = in.value[n:]
	return n, io.EOF
}

func runChannelProgressCLI(t *testing.T, server *httptest.Server, in io.Reader, args ...string) (string, string, int) {
	t.Helper()
	setCLIAPITestToken(t, operatorChannelCLIToken)
	opts := testRootCommandOptions(server)
	opts.input = in
	opts.stdinIsTerminal = func() bool { return false }
	var out, errOut bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &out, &errOut, opts)
	return out.String(), errOut.String(), code
}

func sessionProgressCLIResult(provider, role, phase string, ready bool) map[string]any {
	result := channelOnboardingCLIResult(phase, "", ready)
	op := result["operation"].(map[string]any)
	c := result["candidate"].(map[string]any)
	identity := op["interface"].(map[string]any)
	identity["channel_pack_id"] = "provider." + provider + ".hitl_channel"
	identity["selector"] = (operatorchannel.InterfaceIdentity{
		InterfaceRef: operatorchannel.InterfaceHITLChannelV2, ChannelPackID: identity["channel_pack_id"].(string),
		ChannelPackVersion: "1.0.0", ChannelManifestHash: "sha256:" + strings.Repeat("a", 64),
		SemanticGeneration: "operator-channel-cli-generation",
	}).Normalized().Selector
	coordinate := op["coordinate"].(map[string]any)
	if !ready {
		coordinate["target_generation"] = 0
	}
	target := c["target"].(map[string]any)
	target["selector"], target["provider"] = "ingress:support:"+provider, provider
	if !ready {
		target["generation"], target["publication_sequence"] = 0, 0
	}
	c["provider"], c["activation_posture"] = provider, "session_connection"
	c["provider_credential_role"], c["connection_health"] = role, "provider_connection"
	delete(c, "signing_credential_role")
	op["provider"], op["activation_posture"], op["target_selector"] = provider, "session_connection", target["selector"]
	op["binding_revision"] = 0
	op["credential_reservations"] = []any{}
	if role != "" {
		op["credential_reservations"] = []any{map[string]any{"role": role, "store_key": "channel.telegram.bot_token"}}
	}
	if phase == "activating_provider" {
		result["pairing"] = map[string]any{"status": channelonboarding.PairingStatusPreparing, "paired": false, "connected": false}
	}
	return result
}

func TestChannelCLIProgressFixturesRetainExactDeclarations(t *testing.T) {
	for _, fixture := range []map[string]any{channelOnboardingCLIResult("preparing", "", false), sessionProgressCLIResult("whatsapp", "", "activating_provider", false)} {
		body, err := json.Marshal(fixture)
		if err != nil {
			t.Fatal(err)
		}
		var result channelOnboardingResult
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatal(err)
		}
		if err := result.Candidate.ValidateDeclaration(); err != nil {
			t.Fatal("invalid selected declaration", result.Candidate, err)
		}
		if _, err := result.ProviderCredentialRequirement(); err != nil {
			t.Fatal("declaration does not match retained operation", result, err)
		}
	}
}

func requireChannelPendingCLIOutput(t *testing.T, out, errOut string, code int, mode string) {
	t.Helper()
	if code != 0 || errOut != "" || strings.Contains(out+errOut, "Connected ") || strings.Contains(out+errOut, "input-must-not") {
		t.Fatal("pending CLI reported an error, secret or READY", code, out, errOut)
	}
	switch mode {
	case "--json":
		var result channelOnboardingResult
		if err := json.Unmarshal([]byte(out), &result); err != nil {
			t.Fatal(err)
		}
		pending, err := result.SessionPreparationPending()
		if err != nil || !pending || result.Operation.OperationID != operatorChannelCLIOperation || result.Operation.Revision != 6 {
			t.Fatal("JSON lost exact pending evidence", result, err)
		}
	case "--quiet":
		if strings.TrimSpace(out) != operatorChannelCLIOperation {
			t.Fatal("quiet pending output does not identify the retained operation", out)
		}
	default:
		if !strings.Contains(out, "pending (not READY)") || !strings.Contains(out, "swarm channel resume "+operatorChannelCLIOperation) {
			t.Fatal("human output lost pending disposition or exact continuation", out)
		}
	}
}

func TestChannelCLIExplicitSessionPreparationProgress(t *testing.T) {
	for _, verb := range []string{"connect", "resume"} {
		for _, mode := range []string{"human", "--json", "--quiet"} {
			t.Run(verb+"/"+mode, func(t *testing.T) {
				var methods []string
				server := newOperatorChannelCLIServer(t, func(t *testing.T, req jsonRPCRequest, _ int) map[string]any {
					methods = append(methods, req.Method)
					if req.Params["provider_credential"] != nil || req.Method == "channel.onboarding_retry" && req.Params["operation_id"] != operatorChannelCLIOperation {
						t.Fatal("pending request submitted a secret or changed responsibility", req)
					}
					return sessionProgressCLIResult("whatsapp", "", "activating_provider", false)
				})
				in := &channelCredentialInput{}
				args := []string{"channel", verb, "whatsapp", "--yes"}
				if verb == "resume" {
					args[2] = operatorChannelCLIOperation
				}
				if mode != "human" {
					args = append(args, mode)
				}
				out, errOut, code := runChannelProgressCLI(t, server, in, args...)
				requireChannelPendingCLIOutput(t, out, errOut, code, mode)
				want := "channel.onboarding_start"
				if verb == "resume" {
					want = "channel.onboarding_get,channel.onboarding_retry"
				}
				if in.reads != 0 || strings.Join(methods, ",") != want {
					t.Fatal("pending CLI read stdin or expanded the retry policy", in.reads, methods)
				}
			})
		}
	}
}

func TestChannelCLIPendingResumeCanReachGenuineReady(t *testing.T) {
	server := newOperatorChannelCLIServer(t, func(t *testing.T, req jsonRPCRequest, call int) map[string]any {
		if call == 0 && req.Method == "channel.onboarding_get" {
			return sessionProgressCLIResult("whatsapp", "", "activating_provider", false)
		}
		if req.Method != "channel.onboarding_retry" || req.Params["operation_id"] != operatorChannelCLIOperation {
			t.Fatal("explicit continuation changed responsibility", req)
		}
		return sessionProgressCLIResult("whatsapp", "", "succeeded", true)
	})
	out, errOut, code := runChannelProgressCLI(t, server, &channelCredentialInput{}, "channel", "resume", operatorChannelCLIOperation)
	if code != 0 || errOut != "" || !strings.Contains(out, "Connected whatsapp channel READY") {
		t.Fatal("genuine READY was not preserved", code, out, errOut)
	}
}

func TestChannelCLISelectedCredentialAdmissionRetainsOperation(t *testing.T) {
	for _, posture := range []string{"webhook", "session"} {
		t.Run(posture, func(t *testing.T) {
			var methods []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Fatal(err)
				}
				methods = append(methods, req.Method)
				response := selectedCredentialAdmissionResponse(t, req, posture)
				if err := json.NewEncoder(w).Encode(response); err != nil {
					t.Fatal(err)
				}
			}))
			t.Cleanup(server.Close)
			in := &channelCredentialInput{value: "private-token\n"}
			out, errOut, code := runChannelProgressCLI(t, server, in, "channel", "connect", "telegram", "--yes", "--json", "--target", "ingress:support:telegram", "--idempotency-key", "retained-key")
			if code != 0 || errOut != "" || in.reads != 1 || strings.Contains(out+errOut, "private-token") ||
				strings.Join(methods, ",") != "channel.onboarding_start,channel.onboarding_get,channel.onboarding_retry" {
				t.Fatal("credential admission leaked, repeated or changed responsibility", code, out, errOut, methods, in.reads)
			}
		})
	}
}

func selectedCredentialAdmissionResponse(t *testing.T, req jsonRPCRequest, posture string) map[string]any {
	t.Helper()
	response := map[string]any{"jsonrpc": "2.0", "id": req.ID}
	switch req.Method {
	case "channel.onboarding_start":
		if req.Params["provider_credential"] != nil || req.Params["idempotency_key"] != "retained-key" || req.Params["target"] != "ingress:support:telegram" {
			t.Fatal("initial selection read/sent a secret or lost selectors", req)
		}
		response["error"] = map[string]any{"code": -32000, "message": "credential required", "data": map[string]any{
			"code": "CHANNEL_CREDENTIAL_REQUIRED", "details": map[string]any{"operation_id": operatorChannelCLIOperation, "role": "bot_token", "store_key": "channel.telegram.bot_token"}}}
	case "channel.onboarding_get":
		if req.Params["operation_id"] != operatorChannelCLIOperation {
			t.Fatal("credential readback changed operation", req)
		}
		response["result"] = channelOnboardingCLIResult("preparing", "", false)
		if posture == "session" {
			response["result"] = sessionProgressCLIResult("telegram", "bot_token", "preparing", false)
		}
	case "channel.onboarding_retry":
		if req.Params["operation_id"] != operatorChannelCLIOperation || req.Params["provider_credential"] != "private-token" {
			t.Fatal("hidden input missed exact credential admission", req)
		}
		response["result"] = channelOnboardingCLIResult("succeeded", "", true)
		if posture == "session" {
			response["result"] = sessionProgressCLIResult("telegram", "bot_token", "activating_provider", false)
		}
	default:
		t.Fatal("unexpected credential method", req.Method)
	}
	return response
}

func TestChannelCLIRejectsInapplicableCredentialBeforeInput(t *testing.T) {
	for _, verb := range []string{"reconnect", "rebind", "resume"} {
		t.Run(verb, func(t *testing.T) {
			server := newOperatorChannelCLIServer(t, func(t *testing.T, req jsonRPCRequest, _ int) map[string]any {
				if req.Params["provider_credential"] != nil || req.Method == "channel.onboarding_retry" {
					t.Fatal("inapplicable replacement reached credential admission", req)
				}
				result := sessionProgressCLIResult("whatsapp", "", "activating_provider", false)
				if verb != "resume" {
					if req.Method != "channel.onboarding_select" {
						t.Fatal("inapplicable replacement started work", req.Method)
					}
					return channelCLISelectionResult(result)
				}
				return result
			})
			in := &channelCredentialInput{}
			args := []string{"channel", verb, "whatsapp", "--yes", "--credential-stdin"}
			if verb == "resume" {
				args[2] = operatorChannelCLIOperation
			}
			out, errOut, code := runChannelProgressCLI(t, server, in, args...)
			if code != CLIExitValidation || in.reads != 0 || !strings.Contains(errOut, "no provider credential") || strings.Contains(out, "READY") {
				t.Fatal("inapplicable credential was read or ignored", code, out, errOut, in.reads)
			}
		})
	}
}

func TestChannelCLIRefusesMalformedPendingAndGenuineBlockage(t *testing.T) {
	for _, cell := range []string{"absent_pairing", "missing_paired", "missing_connected", "null_role", "code", "paired", "connected", "ready", "identity", "candidate", "missing_role", "foreign_role", "foreign_target", "failed", "retired", "historical", "unknown_phase"} {
		t.Run(cell, func(t *testing.T) {
			result := sessionProgressCLIResult("whatsapp", "", "activating_provider", false)
			op := result["operation"].(map[string]any)
			c := result["candidate"].(map[string]any)
			p := result["pairing"].(map[string]any)
			switch cell {
			case "absent_pairing":
				delete(result, "pairing")
			case "missing_paired":
				delete(p, "paired")
			case "missing_connected":
				delete(p, "connected")
			case "null_role":
				c["provider_credential_role"] = nil
			case "code":
				p["code"] = "must-not-disclose"
			case "paired", "connected":
				p[cell] = true
			case "ready":
				result["readiness"] = operatorChannelCLIReadiness(true)
			case "identity":
				result["identity_operation"] = operatorChannelCLIOperationResult("awaiting_claim", 1, 0)
			case "candidate":
				delete(result, "candidate")
			case "missing_role":
				delete(c, "provider_credential_role")
			case "foreign_role":
				c["provider_credential_role"] = " secret "
			case "foreign_target":
				op["target_selector"] = "ingress:foreign:whatsapp"
			case "failed", "retired", "unknown_phase":
				op["phase"] = cell
			case "historical":
				op["phase"] = "succeeded"
				delete(result, "pairing")
			}
			server := newOperatorChannelCLIServer(t, func(t *testing.T, _ jsonRPCRequest, _ int) map[string]any { return result })
			in := &channelCredentialInput{}
			out, errOut, code := runChannelProgressCLI(t, server, in, "channel", "connect", "whatsapp", "--yes", "--json")
			if code == 0 || in.reads != 0 || strings.Contains(out, "READY") || strings.Contains(out+errOut, "must-not-disclose") {
				t.Fatal("invalid or genuinely blocked state gained progress/READY", code, out, errOut, in.reads)
			}
		})
	}
}

func TestChannelCLISelectionRefusalDoesNotReadCredential(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.Method != "channel.onboarding_start" || req.Params["provider_credential"] != nil {
			t.Fatal("ambiguous selection gained credential authority", req)
		}
		if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{
			"code": -32000, "message": "ambiguous", "data": map[string]any{"code": "CHANNEL_INTERFACE_AMBIGUOUS"}}}); err != nil {
			t.Fatal(err)
		}
	}))
	t.Cleanup(server.Close)
	in := &channelCredentialInput{}
	_, errOut, code := runChannelProgressCLI(t, server, in, "channel", "connect", "whatsapp", "--yes")
	if code == 0 || in.reads != 0 || !strings.Contains(errOut, "AMBIGUOUS") {
		t.Fatal("ambiguous selection was ignored or read stdin", code, errOut, in.reads)
	}
}
