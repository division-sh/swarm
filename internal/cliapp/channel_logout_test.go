package cliapp

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

func TestChannelLogoutCLIUsesExactRetainedOperation(t *testing.T) {
	for _, alreadyReserved := range []bool{false, true} {
		for _, output := range []string{"--json", "--quiet"} {
			t.Run(output+"/"+map[bool]string{false: "fresh", true: "replay"}[alreadyReserved], func(t *testing.T) {
				now := time.Now().UTC()
				teardownID := uuid.NewString()
				effectID, err := effects.ChannelLogoutOperationID(teardownID)
				if err != nil {
					t.Fatal(err)
				}
				result := channelonboarding.SessionLogoutReadback{OperationID: operatorChannelCLIOperation,
					ExpectedRevision: 7, EffectOperationID: effectID,
					Teardown: channelonboarding.TeardownOperation{TeardownID: teardownID, Kind: channelonboarding.TeardownLogout,
						PrincipalID: uuid.NewString(), Scope: channelonboarding.TeardownScope{Interface: operatorchannel.InterfaceIdentity{
							InterfaceRef: operatorchannel.InterfaceHITLChannelV2, ChannelPackID: "provider.whatsapp.hitl_channel",
							ChannelPackVersion: "0.1.0", ChannelManifestHash: "sha256:logout-cli", SemanticGeneration: "logout-cli"}.Normalized()},
						Phase: channelonboarding.TeardownAuthorityRetired, Revision: 1, RequestedAt: now, UpdatedAt: now},
				}
				var methods []string
				server := newOperatorChannelCLIServer(t, func(t *testing.T, request jsonRPCRequest, _ int) map[string]any {
					methods = append(methods, request.Method)
					switch request.Method {
					case "channel.onboarding_get":
						if request.Params["operation_id"] != operatorChannelCLIOperation {
							t.Fatal("readback guessed another operation")
						}
						readback := map[string]any{"operation": map[string]any{"operation_id": operatorChannelCLIOperation, "revision": 7}}
						if alreadyReserved {
							readback["operation"].(map[string]any)["revision"] = 8
							readback["logout"] = result
						}
						return readback
					case "channel.logout":
						if len(request.Params) != 3 || request.Params["operation_id"] != operatorChannelCLIOperation || request.Params["expected_revision"] != float64(7) || request.Params["idempotency_key"] != "logout-key" {
							t.Fatalf("logout leaked or changed its exact selector: %+v", request.Params)
						}
						return map[string]any{"operation_id": result.OperationID, "expected_revision": result.ExpectedRevision, "effect_operation_id": effectID, "teardown": result.Teardown}
					default:
						t.Fatalf("unexpected method %q", request.Method)
					}
					return nil
				})
				stdout, stderr, code := runOperatorChannelCLI(t, server, "channel", "logout", operatorChannelCLIOperation, "--idempotency-key", "logout-key", output)
				if code != 0 || stderr != "" || strings.Join(methods, ",") != "channel.onboarding_get,channel.logout" {
					t.Fatalf("code=%d methods=%v stdout=%q stderr=%q", code, methods, stdout, stderr)
				}
				if output == "--quiet" && stdout != teardownID+"\n" {
					t.Fatalf("quiet output=%q", stdout)
				}
				if output == "--json" && !strings.Contains(stdout, effectID) {
					t.Fatalf("JSON lost exact effect identity: %q", stdout)
				}
			})
		}
	}
}
