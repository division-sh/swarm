package cliapp

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func channelCLISelectionResult(result map[string]any) map[string]any {
	return map[string]any{"candidate": result["candidate"], "selection_fence": "selected-declaration"}
}

func TestChannelCLIExplicitReplacementPrecedesStartEffects(t *testing.T) {
	for _, verb := range []string{"reconnect", "rebind"} {
		t.Run(verb, func(t *testing.T) {
			in := &channelCredentialInput{value: "replacement-token\n"}
			var methods []string
			server := newOperatorChannelCLIServer(t, func(t *testing.T, req jsonRPCRequest, call int) map[string]any {
				methods = append(methods, req.Method)
				if call == 0 {
					if req.Method != "channel.onboarding_select" || req.Params["provider_credential"] != nil || in.reads != 0 {
						t.Fatal("selection performed credential work", req, in.reads)
					}
					return channelCLISelectionResult(channelOnboardingCLIResult("preparing", "", false))
				}
				if call != 1 || req.Method != "channel.onboarding_start" || in.reads != 1 ||
					req.Params["provider_credential"] != "replacement-token" || req.Params["expected_selection"] != "selected-declaration" {
					t.Fatal("Start launched before replacement input or lost its exact fence", req, in.reads)
				}
				return channelOnboardingCLIResult("succeeded", "", true)
			})
			out, errOut, code := runChannelProgressCLI(t, server, in, "channel", verb, "telegram", "--yes", "--credential-stdin", "--json")
			if code != 0 || errOut != "" || strings.Join(methods, ",") != "channel.onboarding_select,channel.onboarding_start" || strings.Contains(out, "replacement-token") {
				t.Fatal("explicit replacement launched old-credential work or exposed input", code, out, errOut, methods)
			}
		})
	}
}

func TestChannelCLIPendingResumeReachesQRWithoutRevisionChange(t *testing.T) {
	for _, mode := range []string{"human", "--json", "--quiet"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			server := newOperatorChannelCLIServer(t, func(t *testing.T, req jsonRPCRequest, call int) map[string]any {
				calls++
				result := sessionProgressCLIResult("whatsapp", "", "activating_provider", false)
				if call == 0 {
					if req.Method != "channel.onboarding_get" {
						t.Fatal(req.Method)
					}
					return result
				}
				if req.Method != "channel.onboarding_retry" || call != 1 {
					t.Fatal("QR progress initiated automatic polling or another continuation", req.Method, call)
				}
				result["pairing"] = map[string]any{"status": "awaiting_qr", "code": "test-qr-material", "expires_at": time.Now().Add(time.Minute), "paired": false, "connected": true}
				return result
			})
			args := []string{"channel", "resume", operatorChannelCLIOperation}
			if mode != "human" {
				args = append(args, mode)
			}
			in := &channelCredentialInput{}
			out, errOut, code := runChannelProgressCLI(t, server, in, args...)
			if code != 0 || errOut != "" || calls != 2 || in.reads != 0 || strings.Contains(out, "Connected ") {
				t.Fatal("valid QR progress became terminal failure or READY", code, out, errOut, calls, in.reads)
			}
			if mode == "--quiet" {
				if strings.TrimSpace(out) != operatorChannelCLIOperation || strings.Contains(out, "test-qr-material") {
					t.Fatal("quiet mode disclosed QR instead of operation identity", out)
				}
			} else if !strings.Contains(out, "test-qr-material") {
				t.Fatal("actionable QR was not presented", out)
			}
		})
	}
}

func TestChannelCLIPairingProgressRefusesContradictoryOrExpiredEvidence(t *testing.T) {
	for _, cell := range []string{"empty", "expired", "missing_expiry", "control", "paired", "foreign_target", "terminal", "ready", "identity"} {
		t.Run(cell, func(t *testing.T) {
			result := sessionProgressCLIResult("whatsapp", "", "activating_provider", false)
			pairing := map[string]any{"status": "awaiting_qr", "code": "private-material", "expires_at": time.Now().Add(time.Minute), "paired": false, "connected": true}
			result["pairing"] = pairing
			switch cell {
			case "empty":
				pairing["code"] = ""
			case "expired":
				pairing["expires_at"] = time.Now().Add(-time.Minute)
			case "missing_expiry":
				delete(pairing, "expires_at")
			case "control":
				pairing["code"] = "private-material\x1b[31m"
			case "paired":
				pairing["paired"] = true
			case "foreign_target":
				result["operation"].(map[string]any)["target_selector"] = "ingress:foreign:whatsapp"
			case "terminal":
				result["operation"].(map[string]any)["phase"] = "succeeded"
			case "ready":
				result["readiness"] = operatorChannelCLIReadiness(true)
			case "identity":
				result["identity_operation"] = operatorChannelCLIOperationResult("awaiting_claim", 1, 0)
			}
			calls := 0
			server := newOperatorChannelCLIServer(t, func(t *testing.T, req jsonRPCRequest, _ int) map[string]any {
				calls++
				if req.Method != "channel.onboarding_get" {
					t.Fatal("invalid QR gained continuation", req.Method)
				}
				return result
			})
			out, errOut, code := runChannelProgressCLI(t, server, &channelCredentialInput{}, "channel", "resume", operatorChannelCLIOperation, "--json")
			if code == 0 || calls != 1 || strings.Contains(out+errOut, "private-material") {
				t.Fatal("invalid QR gained progress or disclosure", code, out, errOut, calls)
			}
		})
	}
}

func TestChannelCLIExplicitPairingClaimConfirmationJourney(t *testing.T) {
	for _, mode := range []string{"human", "--json", "--quiet"} {
		t.Run(mode, func(t *testing.T) {
			stage := 0
			var methods []string
			progress := func() map[string]any {
				result := sessionProgressCLIResult("whatsapp", "", "activating_provider", false)
				switch stage {
				case 1:
					result["pairing"] = map[string]any{"status": "awaiting_qr", "code": "journey-material", "expires_at": time.Now().Add(time.Minute), "paired": false, "connected": false}
				case 2:
					result["pairing"] = map[string]any{"status": "paired", "paired": true, "connected": false}
				case 3:
					result = sessionProgressCLIResult("whatsapp", "", "awaiting_external_identity", false)
					result["identity_operation"] = operatorChannelCLIOperationResult("awaiting_claim", 1, 0)
					result["identity_operation"].(map[string]any)["expires_at"] = time.Now().Add(time.Minute)
				case 4:
					result = sessionProgressCLIResult("whatsapp", "", "awaiting_operator_confirmation", false)
					result["identity_operation"] = operatorChannelCLIOperationResult("awaiting_confirmation", 2, 0)
					result["identity_operation"].(map[string]any)["expires_at"] = time.Now().Add(time.Minute)
				case 5:
					result = sessionProgressCLIResult("whatsapp", "", "succeeded", true)
				}
				return result
			}
			server := newOperatorChannelCLIServer(t, func(t *testing.T, req jsonRPCRequest, _ int) map[string]any {
				methods = append(methods, req.Method)
				switch req.Method {
				case "channel.onboarding_start":
					if stage != 0 {
						t.Fatal("pairing created a replacement responsibility")
					}
				case "channel.onboarding_get":
					if stage == 3 {
						stage = 4
					}
				case "channel.onboarding_retry":
					if stage == 4 || stage > 5 || req.Params["operation_id"] != operatorChannelCLIOperation {
						t.Fatal("pairing skipped explicit confirmation or changed responsibility", req.Params, stage)
					}
					if stage < 3 {
						stage++
					}
				case "channel.confirm":
					if stage != 4 || req.Params["approve"] != true || req.Params["expected_revision"] != float64(2) {
						t.Fatal("claim confirmation lost its exact evidence", req.Params, stage)
					}
					stage = 5
					return map[string]any{"operation": operatorChannelCLIOperationResult("bound", 3, 1), "binding": map[string]any{"revision": 1}}
				default:
					t.Fatal("unsupported automatic pairing work", req.Method)
				}
				return progress()
			})
			for call := 0; call < 4; call++ {
				args := []string{"channel", "resume", operatorChannelCLIOperation, "--yes"}
				if call == 0 {
					args = []string{"channel", "connect", "whatsapp", "--yes"}
				}
				if mode != "human" {
					args = append(args, mode)
				}
				out, errOut, code := runChannelProgressCLI(t, server, &channelCredentialInput{}, args...)
				if code != 0 || errOut != "" {
					t.Fatal("genuine ceremony failed", stage, code, out, errOut)
				}
				if call < 3 && strings.Contains(out, "Connected ") {
					t.Fatal("pairing was reported READY before principal confirmation", stage, out)
				}
			}
			if stage != 5 || strings.Count(strings.Join(methods, ","), "channel.confirm") != 1 {
				t.Fatal("ceremony failed to reach genuine confirmation", stage, methods)
			}
		})
	}
}

func TestChannelCLICredentialRequiredAcceptsCanonicalEffectiveKey(t *testing.T) {
	server := newOperatorChannelCLIServer(t, func(t *testing.T, req jsonRPCRequest, _ int) map[string]any { return nil })
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		response := map[string]any{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "channel.onboarding_start":
			response["error"] = map[string]any{"code": -32000, "message": "credential required", "data": map[string]any{"code": "CHANNEL_CREDENTIAL_REQUIRED", "details": map[string]any{"operation_id": operatorChannelCLIOperation, "role": "bot_token", "store_key": "current-operation-specific-token-key"}}}
		case "channel.onboarding_get":
			response["result"] = channelOnboardingCLIResult("preparing", "", false)
		case "channel.onboarding_retry":
			if req.Params["provider_credential"] != "replacement-token" || req.Params["store_key"] != nil {
				t.Fatal("effective key became caller write authority", req.Params)
			}
			response["result"] = channelOnboardingCLIResult("succeeded", "", true)
		default:
			t.Fatal(req.Method)
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Fatal(err)
		}
	})
	in := &channelCredentialInput{value: "replacement-token\n"}
	out, errOut, code := runChannelProgressCLI(t, server, in, "channel", "connect", "telegram", "--yes", "--json")
	if code != 0 || errOut != "" || in.reads != 1 || strings.Contains(out, "replacement-token") {
		t.Fatal("canonical effective key rejected against declaration base key", in.reads, code, out, errOut)
	}
}
