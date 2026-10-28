//go:build linux || darwin

package serveapp

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil"
	"go.mau.fi/whatsmeow/types"
)

// A comms-only connection carries customer traffic, not operator authority.
func TestRunServeWhatsAppReceptionistWithoutOperatorBothStores(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.ArtifactID("internal/cliapp/archetypes/whatsapp-receptionist"))
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, rollover := range []bool{false, true} {
			name := backend
			if rollover {
				name += "/source_rollover"
			}
			t.Run(name, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				peer := newServeNativeProtocolPeer(t)
				peer.mu.Lock()
				peer.loginHandoff = true
				peer.mu.Unlock()
				source := t.TempDir()
				copyReleaseFixtureTree(t, filepath.Join(repoRootForTest(), "internal/cliapp/archetypes/whatsapp-receptionist"), source)
				if err := os.WriteFile(filepath.Join(source, "policy.yaml"), []byte("operator_account: '+15551234567'\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				opts := cliapp.ServeOptions{
					SourceRoot:    source,
					APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
					SwarmDir: t.TempDir(), SwarmDirSet: true, SelfCheck: true,
					WorkspaceBackend: "host", WorkspaceBackendSet: true, StoreMode: backend, StoreModeSet: true,
				}
				if err := os.Chmod(opts.SwarmDir, 0o700); err != nil {
					t.Fatal(err)
				}
				var location string
				if backend == "sqlite" {
					location = filepath.Join(t.TempDir(), "receptionist.sqlite")
					opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend,
						location, channelOnboardingHostWorkspaceFields())
				} else {
					location = testutil.StartEmptyPostgresDSN(t)
					opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, location)
				}
				const pendingText = "Message during account activation"
				const pendingID = "PENDING_ACCOUNT_ACTIVATION"
				pendingCustomer := types.NewJID("15551234579", types.DefaultUserServer)
				if !rollover {
					var injected atomic.Bool
					opts.TestChannelOnboardingBarrier = func(boundary channelonboarding.TestLifecycleBoundary, _ string) error {
						if boundary != channelonboarding.TestAfterActivationCommitBeforePublication || !injected.CompareAndSwap(false, true) {
							return nil
						}
						peer.mu.Lock()
						before := peer.disconnected
						peer.mu.Unlock()
						sender, _, _ := peer.senderFrom(pendingCustomer, types.NewJID("100000000007", types.HiddenUserServer))
						peer.sendEncryptedFrame(sender.EncryptText(t, pendingText, pendingID))
						for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
							peer.mu.Lock()
							disconnected := peer.disconnected > before
							peer.mu.Unlock()
							if disconnected {
								t.Error("pending account activation fenced the native peer")
								return nil
							}
							time.Sleep(10 * time.Millisecond)
						}
						select {
						case <-peer.receipts:
							t.Error("pending account activation acknowledged unadmitted input")
						default:
						}
						select {
						case <-peer.sent:
							t.Error("pending account activation published customer input")
						default:
						}
						return nil
					}
				}
				process := startServeRuntimeTestProcess(t, opts)
				process.waitForReadyLine()
				endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
				var result channelonboarding.Result
				requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", map[string]any{"provider": "whatsapp", "verb": "connect", "save_proof": false}, &result)
				if result.Operation.Ceremony != channelonboarding.CeremonyAccountLink {
					t.Fatal("receptionist required operator authority", result.Operation.Ceremony)
				}
				result = requireServedNativePairingProgress(t, endpoint, result, time.Now().Add(5*time.Second))
				result = requireServedRevokedPairingRecovery(t, endpoint, peer, opts.SwarmDir, backend, location, result)
				peer.pair(result.Pairing.Code)
				id := result.Operation.OperationID
				for deadline := time.Now().Add(5 * time.Second); result.Operation.Phase == channelonboarding.PhaseActivatingProvider && time.Now().Before(deadline); {
					requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id, "checkpoint_only": true}, &result)
					if result.Operation.Phase == channelonboarding.PhaseActivatingProvider {
						time.Sleep(10 * time.Millisecond)
					}
				}
				peer.awaitPostLogin()
				if result.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity || result.Operation.ActivationRevision != 0 ||
					result.IdentityOperation != nil || result.Connection == nil || !result.Connection.Linked || result.Readiness != nil && result.Readiness.Ready {
					t.Fatal("account-preview checkpoint published customer traffic or operator authority", result)
				}
				preview := result.Operation
				requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", map[string]any{"provider": "whatsapp", "verb": "connect", "save_proof": false}, &result)
				if !reflect.DeepEqual(result.Operation, preview) || result.IdentityOperation != nil || result.Binding != nil ||
					result.Readiness != nil && result.Readiness.Ready {
					t.Fatal("connecting again approved the retained account preview", result)
				}
				rejected := requireServedNativeCLILogout(t, endpoint, id, "reject-preview")
				if rejected.Teardown.Phase != channelonboarding.TeardownSucceeded {
					t.Fatal("rejected account was not unlinked before replacement", rejected)
				}
				requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", map[string]any{"provider": "whatsapp", "verb": "connect", "save_proof": false}, &result)
				if result.Operation.OperationID == id || result.Operation.ActivationRevision != 0 || result.IdentityOperation != nil {
					t.Fatal("account rejection did not start fresh non-executable pairing", result)
				}
				result = requireServedNativePairingProgress(t, endpoint, result, time.Now().Add(5*time.Second))
				peer.pair(result.Pairing.Code)
				id = result.Operation.OperationID
				for deadline := time.Now().Add(5 * time.Second); result.Operation.Phase == channelonboarding.PhaseActivatingProvider && time.Now().Before(deadline); {
					requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id, "checkpoint_only": true}, &result)
					if result.Operation.Phase == channelonboarding.PhaseActivatingProvider {
						time.Sleep(10 * time.Millisecond)
					}
				}
				if result.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity || result.Operation.ActivationRevision != 0 || result.IdentityOperation != nil {
					t.Fatal("login handoff did not retain the non-executable account preview", result.Operation.Phase)
				}
				response := requestServedJSONRPC(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id})
				peer.awaitPostLogin()
				requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
				if response.Error != nil {
					t.Fatal("receptionist continuation refused", response.Error, "phase", result.Operation.Phase, "target", result.Operation.Coordinate.ContextPublicationGeneration)
				}
				if result.Operation.Phase != channelonboarding.PhaseSucceeded || result.Operation.Ceremony != channelonboarding.CeremonyAccountLink ||
					result.IdentityOperation != nil || result.Operation.BindingRevision != 0 || result.Readiness == nil || !result.Readiness.Ready {
					t.Fatalf("customer channel was not independently READY: %+v", result)
				}
				requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", map[string]any{"provider": "whatsapp", "verb": "connect", "save_proof": false}, &result)
				if result.Operation.OperationID != id || result.Readiness == nil || !result.Readiness.Ready {
					t.Fatal("second connect did not attach to the current connection", result)
				}
				originalReply := requireServedNativeAuthoredCustomerReply(t, endpoint, peer, false)
				customer, retained := peer.receiver(originalReply.To)
				if !retained {
					t.Fatal("original customer sender was not retained")
				}
				if !rollover {
					sender, found := peer.receiver(pendingCustomer)
					if !found {
						t.Fatal("pending input lost its original encrypted sender")
					}
					peer.sendEncrypted(sender.sender.EncryptText(t, pendingText, pendingID), pendingID)
					if reply := awaitServedNativeProcessSend(t, peer, pendingText); reply.To.ToNonAD() != pendingCustomer && reply.To.ToNonAD() != sender.lid {
						t.Fatal("admitted redelivery changed the exact customer recipient")
					}
				}
				if code := process.stop(); code != 0 {
					t.Fatal("receptionist shutdown failed", code, process.outputString())
				}
				process = startServeRuntimeTestProcess(t, opts)
				process.waitForReadyLine()
				peer.awaitPostLogin()
				endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
				requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
				if result.Readiness == nil || !result.Readiness.Ready || result.IdentityOperation != nil ||
					result.Connection == nil || result.Connection.State != "connected" {
					t.Fatal("ordinary restart did not reconnect the receptionist", result)
				}
				deadline := time.Now().Add(5 * time.Second)
				for !strings.Contains(process.outputString(), "WhatsApp: reconnected (+") && time.Now().Before(deadline) {
					time.Sleep(10 * time.Millisecond)
				}
				if !strings.Contains(process.outputString(), "WhatsApp: reconnected (+") {
					t.Fatal("restart did not show the linked account")
				}
				if rollover {
					previous := result.Operation
					previousAccount := result.Connection.AccountPresentation
					previousConnection := readServedNativeConnectionID(t, backend, location, id)
					if code := process.stop(); code != 0 {
						t.Fatal("shutdown before source rollover failed", code)
					}
					schemaPath := filepath.Join(source, "schema.yaml")
					schema, err := os.ReadFile(schemaPath)
					if err != nil {
						t.Fatal(err)
					}
					withOperator := strings.Replace(string(schema), "stages: []", "stages:\n  pending:\n    gate:\n      decision: retire_service\n      outcomes:\n        retire:\n          advances_to: done\n  done:\n    final: true", 1)
					if withOperator == string(schema) {
						t.Fatal("receptionist control did not add the admitted human-authority construct")
					}
					if err := os.WriteFile(schemaPath, []byte(withOperator), 0o600); err != nil {
						t.Fatal(err)
					}
					process = startServeRuntimeTestProcess(t, opts)
					process.waitForReadyLine()
					peer.awaitPostLogin()
					endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
					requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
					if result.Operation.OperationID != id || readServedNativeConnectionID(t, backend, location, id) != previousConnection ||
						result.Operation.Coordinate.BundleHash == previous.Coordinate.BundleHash ||
						result.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity || result.IdentityOperation == nil ||
						result.Operation.BindingRevision != 0 || result.Readiness == nil || !result.Readiness.Ready ||
						result.Connection == nil || result.Connection.State != "connected" || result.Connection.AccountPresentation != previousAccount {
						t.Fatal("source rollover did not retain customer service with separate operator enrollment", result)
					}
					if previous.Coordinate.MatchesDurableIdentity(result.Operation.Coordinate) {
						t.Fatal("source rollover treated the previous source as current authority")
					}
					for i, text := range []string{"Customer service continues during operator enrollment", "SWARM-AAAAAAAAAAAAAAAA", "A quoted customer reply is not operator input"} {
						messageID := []string{"ROLLOVER_CUSTOMER", "ROLLOVER_NOT_OPERATOR", "ROLLOVER_CUSTOMER_QUOTE"}[i]
						message := customer.sender.EncryptText(t, text, messageID)
						if i == 2 {
							message = customer.sender.EncryptReply(t, text, messageID, originalReply.ID, originalReply.Body.GetConversation())
						}
						peer.sendEncrypted(message, messageID)
						sent := awaitServedNativeProcessSend(t, peer, text)
						if sent.Body.GetConversation() != text || sent.To.ToNonAD() != originalReply.To.ToNonAD() && sent.To.ToNonAD() != customer.lid {
							t.Fatal("pending operator enrollment changed the exact customer reply", sent)
						}
						requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
						if result.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity || result.IdentityOperation == nil || result.IdentityOperation.State != operatorchannel.StateAwaitingClaim {
							t.Fatal("ordinary customer traffic was consumed as an operator claim", result)
						}
					}
					peer.claimMessage(result.IdentityOperation.Challenge, "ROLLOVER_OPERATOR")
					deadline = time.Now().Add(5 * time.Second)
					for result.Operation.Phase != channelonboarding.PhaseAwaitingOperatorConfirmation && time.Now().Before(deadline) {
						time.Sleep(10 * time.Millisecond)
						requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id}, &result)
					}
					if result.Operation.Phase != channelonboarding.PhaseAwaitingOperatorConfirmation || result.IdentityOperation == nil || result.IdentityOperation.State != operatorchannel.StateAwaitingConfirmation {
						t.Fatal("exact operator claim did not reach separate confirmation", result)
					}
					const awaitingConfirmation = "Customer service continues before the operator is confirmed"
					peer.sendEncrypted(customer.sender.EncryptText(t, awaitingConfirmation, "ROLLOVER_BEFORE_CONFIRM"), "ROLLOVER_BEFORE_CONFIRM")
					pendingReply := awaitServedNativeProcessSend(t, peer, awaitingConfirmation)
					if pendingReply.To.ToNonAD() != originalReply.To.ToNonAD() && pendingReply.To.ToNonAD() != customer.lid {
						t.Fatal("unconfirmed operator replaced the exact customer recipient", pendingReply)
					}
					var confirmed struct {
						Binding operatorchannel.Binding `json:"binding"`
					}
					requireServedJSONRPCResult(t, endpoint, "channel.confirm", map[string]any{
						"operation_id": result.IdentityOperation.OperationID, "expected_revision": result.IdentityOperation.Revision, "approve": true}, &confirmed)
					if confirmed.Binding.Status != operatorchannel.BindingCurrent || confirmed.Binding.Revision == 0 {
						t.Fatal("operator confirmation did not produce its exact current binding", confirmed)
					}
					requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id}, &result)
					if result.Operation.Phase != channelonboarding.PhaseSucceeded || result.Operation.BindingRevision == 0 || result.Readiness == nil || !result.Readiness.Ready {
						t.Fatal("confirmed operator did not gain its separately admitted authority", result)
					}
				}
				logout := requireServedNativeCLILogout(t, endpoint, "whatsapp", "receptionist-logout")
				if logout.OperationID != id || logout.Teardown.Phase != channelonboarding.TeardownSucceeded {
					t.Fatal("provider logout did not unlink the current receptionist", logout)
				}
				if code := process.stop(); code != 0 {
					t.Fatal("logged-out receptionist shutdown failed", code)
				}
			})
		}
	}
}
