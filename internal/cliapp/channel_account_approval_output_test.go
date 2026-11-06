package cliapp

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestChannelCLIExplicitDeploymentAccountApprovalOutputModes(t *testing.T) {
	for _, provider := range []string{"telegram", "whatsapp"} {
		for _, mode := range []string{"human", "--json", "--quiet"} {
			for _, approve := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/yes=%t", provider, mode, approve), func(t *testing.T) {
					calls := 0
					server := newOperatorChannelCLIServer(t, func(t *testing.T, req jsonRPCRequest, call int) map[string]any {
						calls++
						if call == 0 {
							if req.Method != "channel.onboarding_start" {
								t.Fatal("preview did not start through the public command", req.Method)
							}
							return deploymentAccountPreviewCLIResult(t, provider, false)
						}
						if call != 1 || req.Method != "channel.onboarding_retry" || req.Params["operation_id"] != operatorChannelCLIOperation {
							t.Fatal("preview answer changed its selected operation", call, req)
						}
						if approve {
							if req.Params["approve_account"] != true {
								t.Error("explicit --yes was lost before deployment retry", req.Params)
							}
							return deploymentAccountPreviewCLIResult(t, provider, true)
						}
						if req.Params["approve_account"] != nil {
							t.Error("preview state became an affirmative answer", req.Params)
						}
						return deploymentAccountPreviewCLIResult(t, provider, false)
					})
					args := []string{"channel", "connect", provider}
					if mode != "human" {
						args = append(args, mode)
					}
					if approve {
						args = append(args, "--yes")
					}
					in := &channelCredentialInput{}
					out, errOut, code := runChannelProgressCLI(t, server, in, args...)
					if !approve {
						if code == 0 || strings.Contains(out, "READY") || strings.Contains(out, whatsAppReadyTeaching) || in.reads != 0 {
							t.Fatal("absent answer approved or read nonterminal stdin", code, out, errOut, in.reads)
						}
						return
					}
					if code != 0 || errOut != "" || in.reads != 0 || calls != 2 {
						t.Fatal("explicit account approval failed", code, out, errOut, calls, in.reads)
					}
					account := deploymentAccountPreviewCLIResult(t, provider, true)["operation"].(map[string]any)["operator_account"]
					if mode == "--json" && !json.Valid([]byte(out)) ||
						mode == "--quiet" && strings.TrimSpace(out) != account {
						t.Fatal("account preview polluted machine output", out)
					}
				})
			}
		}
	}
}

func deploymentAccountPreviewCLIResult(t *testing.T, provider string, confirmed bool) map[string]any {
	t.Helper()
	phase := "awaiting_external_identity"
	if confirmed {
		phase = "succeeded"
	}
	result := sessionProgressCLIResult(provider, "", phase, false)
	if provider == "telegram" {
		result = channelOnboardingCLIResult(phase, "", false)
	}
	op := result["operation"].(map[string]any)
	op["deployment_registration"], op["account_confirmed"] = true, confirmed
	op["principal_id"] = "00000000-0000-4000-8000-000000000278"
	op["identity_ceremony"], op["operator_account"] = "account_link", "420079"
	op["binding_revision"] = 0
	if provider == "whatsapp" {
		op["operator_account"] = "+15551234567"
	}
	delete(op, "coordinate")
	delete(op, "target_selector")
	candidate := result["candidate"].(map[string]any)
	candidate["deployment_registration"], candidate["identity_ceremony"] = true, "account_link"
	delete(candidate, "coordinate")
	delete(candidate, "target")
	delete(candidate, "confirmation_operation")
	delete(candidate, "capabilities")
	delete(candidate, "connection_health")
	result["connection"] = map[string]any{"linked": true, "state": "connected", "account_presentation": op["operator_account"]}
	delete(result, "readiness")
	if confirmed {
		op["revision"] = 7
	}
	raw, err := json.Marshal(result)
	var decoded channelOnboardingResult
	if err != nil || json.Unmarshal(raw, &decoded) != nil {
		t.Fatal("account preview fixture failed its public encoding", err)
	}
	if _, err := decoded.ProviderCredentialRequirement(); err != nil || decoded.Operation.ValidateDeploymentRegistration() != nil {
		t.Fatal("account preview fixture contradicts its provider/deployment declaration", err)
	}
	return result
}
