package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
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
	for _, deployment := range []bool{false, true} {
		for _, mode := range []string{"human", "--no-color", "--json", "--quiet"} {
			t.Run(fmt.Sprintf("%s/deployment=%t", mode, deployment), func(t *testing.T) {
				calls := 0
				server := newOperatorChannelCLIServer(t, func(t *testing.T, req jsonRPCRequest, call int) map[string]any {
					calls++
					result := sessionProgressCLIResult("whatsapp", "", "activating_provider", false)
					if deployment {
						operation := result["operation"].(map[string]any)
						operation["deployment_registration"] = true
						operation["identity_ceremony"] = "account_link"
						delete(operation, "coordinate")
						delete(operation, "target_selector")
						candidate := result["candidate"].(map[string]any)
						candidate["deployment_registration"] = true
						candidate["identity_ceremony"] = "account_link"
						delete(candidate, "confirmation_operation")
						delete(candidate, "coordinate")
						delete(candidate, "target")
						delete(candidate, "capabilities")
					}
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
				} else if mode == "--json" {
					if !strings.Contains(out, "test-qr-material") {
						t.Fatal("JSON lost the current QR material", out)
					}
				} else if !strings.Contains(out, "Linked Devices") || !strings.Contains(out, "\u2588") || strings.Contains(out, "test-qr-material") {
					t.Fatal("public command did not render a QR without exposing raw pairing text", out)
				} else if mode == "--no-color" && strings.Contains(out, "\x1b[") {
					t.Fatal("plain QR output contained ANSI styling", out)
				}
				if (mode == "human" || mode == "--no-color") && (strings.Contains(out, "Waiting for") || !strings.Contains(out, "Next: swarm channel connect whatsapp")) {
					t.Fatal("single-step resume claimed to wait or omitted its continuation command", out)
				}
			})
		}
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
	for _, mode := range []string{"human", "human-receptionist", "human-login-handoff", "human-login-disconnected", "human-expired-disconnect", "human-cached-expiry", "human-expired-banned", "human-network-failure", "human-reentry", "human-reentry-receptionist", "--json", "--quiet"} {
		t.Run(mode, func(t *testing.T) {
			human := strings.HasPrefix(mode, "human")
			loginHandoff := strings.HasPrefix(mode, "human-login-")
			reentry := strings.Contains(mode, "reentry")
			receptionist := strings.Contains(mode, "receptionist")
			stage := 0
			if reentry {
				stage = 2
			}
			pairingReads := 0
			checkpoints := 0
			loginReads := 0
			var methods []string
			var out, errOut bytes.Buffer
			progress := func() map[string]any {
				result := sessionProgressCLIResult("whatsapp", "", "activating_provider", false)
				switch stage {
				case 1:
					result["pairing"] = map[string]any{"status": "awaiting_qr", "code": "journey-material", "expires_at": time.Now().Add(time.Minute), "paired": false, "connected": false}
					if human && pairingReads == 2 {
						result["pairing"].(map[string]any)["code"] = "replacement-material"
					} else if human && pairingReads >= 3 {
						result["pairing"] = map[string]any{"status": "qr_expired", "paired": false, "connected": false}
						if mode == "human-cached-expiry" {
							result["pairing"] = map[string]any{"status": "awaiting_qr", "code": "expired-material", "expires_at": time.Now().Add(-time.Second), "paired": false, "connected": false}
						}
						if mode == "human-expired-disconnect" || mode == "human-expired-banned" {
							reason := "network unreachable"
							if mode == "human-expired-banned" {
								reason = "account temporarily banned"
							}
							result["connection"] = map[string]any{"state": "disconnected", "linked": false, "reason": reason}
						}
					}
					if mode == "human-network-failure" {
						result["connection"] = map[string]any{"state": "disconnected", "linked": false, "reason": "network unreachable"}
					}
				case 2:
					result["pairing"] = map[string]any{"status": "paired", "paired": true, "connected": false}
					if human && !loginHandoff {
						result["pairing"].(map[string]any)["connected"] = true
						result["pairing"].(map[string]any)["account_presentation"] = "+15550000001"
					}
					if reentry {
						result = sessionProgressCLIResult("whatsapp", "", "awaiting_external_identity", false)
						result["connection"] = map[string]any{"state": "connected", "linked": true, "account_presentation": "+15550000001"}
					}
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
					if !receptionist {
						result["readiness"].(map[string]any)["native_inbox"] = map[string]any{"state": "qualified"}
					}
				}
				if loginHandoff || reentry {
					result["operation"].(map[string]any)["revision"] = stage + checkpoints + 1
				}
				return result
			}
			server := newOperatorChannelCLIServer(t, func(t *testing.T, req jsonRPCRequest, _ int) map[string]any {
				methods = append(methods, req.Method)
				switch req.Method {
				case "channel.onboarding_start":
					if stage != 0 && !(reentry && stage == 2) {
						t.Fatal("pairing created a replacement responsibility")
					}
				case "channel.onboarding_get":
					if loginHandoff && stage == 2 && checkpoints > 0 {
						loginReads++
					}
					if human && stage <= 1 {
						stage = 1
						pairingReads++
					}
					if stage == 3 {
						stage = 4
					}
				case "channel.onboarding_retry":
					if mode == "human-expired-banned" || mode == "human-network-failure" {
						t.Fatal("unrelated SDK failure authorized automatic renewal")
					}
					if req.Params["checkpoint_only"] == true {
						if !loginHandoff || stage != 2 {
							t.Fatal("unexpected pairing-only login handoff", stage, req.Params)
						}
						checkpoints++
						if checkpoints == 1 {
							result := progress()
							if mode == "human-login-disconnected" {
								result["connection"] = map[string]any{"state": "disconnected", "linked": false, "reason": "network unreachable"}
							}
							return result
						}
						stage = 3
						result := sessionProgressCLIResult("whatsapp", "", "awaiting_external_identity", false)
						result["operation"].(map[string]any)["revision"] = stage + 1
						result["connection"] = map[string]any{"state": "connected", "linked": true, "account_presentation": "+15550000001"}
						return result
					}
					if stage == 4 || stage > 5 || req.Params["operation_id"] != operatorChannelCLIOperation {
						t.Fatal("pairing skipped explicit confirmation or changed responsibility", req.Params, stage)
					}
					if reentry && stage == 2 && !strings.Contains(errOut.String(), "Is this the right account? [y/N]") {
						t.Fatal("retained account advanced without its confirmation prompt")
					}
					if stage < 3 {
						stage++
					}
					if receptionist && stage == 3 {
						stage = 5
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
			if human {
				isolateCLIAPIConfigEnv(t)
				project := t.TempDir()
				if err := os.Mkdir(filepath.Join(project, ".swarm"), 0o700); err != nil {
					t.Fatal(err)
				}
				token := writeCLIAPITokenFile(t, operatorChannelCLIToken)
				config := "connection:\n  api_server: " + server.URL + "\n  api_token_file: " + token + "\n"
				if err := os.WriteFile(filepath.Join(project, ".swarm", "swarm.yaml"), []byte(config), 0o600); err != nil {
					t.Fatal(err)
				}
				opts := defaultRootCommandOptions()
				opts.httpClient, opts.input = server.Client(), strings.NewReader("y\ny\n")
				opts.stdinIsTerminal = func() bool { return true }
				opts.channelConnectPoll = time.Millisecond
				if reentry {
					opts.input = iotest.ErrReader(context.Canceled)
					code := executeRootCommandWithOptions(context.Background(), project, []string{"channel", "connect", "whatsapp"}, &out, &errOut, opts)
					if code == 0 || stage != 2 || !strings.Contains(errOut.String(), "Is this the right account? [y/N]") {
						t.Fatal("cancelled account confirmation advanced the retained preview", code, stage)
					}
					out.Reset()
					errOut.Reset()
					opts.input = strings.NewReader("y\ny\n")
				}
				code := executeRootCommandWithOptions(context.Background(), project, []string{"channel", "connect", "whatsapp"}, &out, &errOut, opts)
				if mode == "human-expired-banned" || mode == "human-network-failure" {
					if code != 2 || strings.Contains(out.String(), "READY") || strings.Contains(errOut.String(), "[y/N]") ||
						!strings.Contains(errOut.String(), "WhatsApp disconnected:") {
						t.Fatal("unrelated SDK failure did not refuse", code, out.String(), errOut.String())
					}
					return
				}
				if mode == "human-login-disconnected" {
					if code != 2 || checkpoints != 1 || loginReads != 0 ||
						!strings.Contains(errOut.String(), "WhatsApp disconnected: network unreachable") ||
						strings.Contains(out.String(), "READY") || strings.Contains(errOut.String(), "[y/N]") ||
						strings.Count(out.String(), "Connecting to whatsapp...") != 1 ||
						// One exhausted QR refresh precedes the one login handoff.
						strings.Count(strings.Join(methods, ","), "channel.onboarding_retry") != 2 {
						t.Fatal("disconnected retained pairing kept driving, prompted, or lost its plain reason", code, checkpoints, loginReads, methods)
					}
					return
				}
				if code != 0 || !strings.Contains(out.String(), "READY") || !reentry && (!strings.Contains(out.String(), "\x1b[J") ||
					!strings.Contains(out.String(), "Linked Devices")) ||
					strings.Contains(out.String(), "Native inbox") ||
					!strings.Contains(errOut.String(), "[y/N]") || strings.Contains(out.String()+errOut.String(), operatorChannelCLIOperation) ||
					strings.Contains(out.String(), "journey-material") || strings.Contains(out.String(), "replacement-material") ||
					strings.Contains(out.String(), "expired-material") || strings.Contains(out.String(), "QR expired, new QR:") {
					t.Fatal("single public connect lost pairing, refresh, claimant confirmation or config targeting", code, out.String(), errOut.String())
				}
				if receptionist {
					if strings.Contains(out.String(), "operator") || strings.Count(errOut.String(), "[y/N]") != 1 {
						t.Fatal("receptionist asked for operator authority", out.String(), errOut.String())
					}
				} else if !strings.Contains(out.String(), "Code received from") || strings.Count(errOut.String(), "[y/N]") != 2 {
					t.Fatal("HITL connect lost either account or claimant confirmation", out.String(), errOut.String())
				}
			} else {
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
			}
			confirmations := 1
			if receptionist {
				confirmations = 0
			}
			if stage != 5 || strings.Count(strings.Join(methods, ","), "channel.confirm") != confirmations {
				t.Fatal("ceremony failed to reach genuine confirmation", stage, methods)
			}
			if mode == "human-login-handoff" && (checkpoints != 2 || loginReads != 0 || strings.Count(out.String(), "Connecting to whatsapp...") != 1) {
				t.Fatal("pending login relied on GET or repeated its connection announcement", checkpoints, loginReads)
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
