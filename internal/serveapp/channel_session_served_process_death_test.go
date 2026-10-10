//go:build linux || darwin

package serveapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestServedWhatsAppLaunchedProcessDeathBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, activity := range []bool{false, true} {
			kind := "inbox_delivery"
			if activity {
				kind = "authored_activity"
			}
			t.Run(backend+"/"+kind, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
				peer := newServeNativeProtocolPeer(t)
				opts, location := servedNativeProcessOptions(t, backend)
				process := startServedNativeSDKProcess(t, opts, peer)
				endpoint := process.endpoint(t) + "/v1/rpc"
				t.Cleanup(func() {
					if t.Failed() {
						t.Logf("original SDK process output:\n%s", process.output.String())
					}
				})
				ready := requireServedNativeProcessPairing(t, endpoint, peer)
				reader, err := storetest.OpenChannelObservation(backend, location)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := reader.Close(); err != nil {
						t.Error(err)
					}
				})
				text := "Inbox\n"
				if activity {
					text = "Native reply interrupted after genuine provider acceptance"
				}
				peer.mu.Lock()
				peer.withholdAck = text
				peer.mu.Unlock()
				if activity {
					message, _, _ := peer.customerMessage(text, "PROCESS_DEATH_CUSTOMER")
					peer.sendEncryptedFrame(message)
				} else {
					peer.text("/inbox", "PROCESS_DEATH_INBOX")
				}
				message := awaitServedNativeProcessSend(t, peer, text)
				if err := process.kill(); err != nil {
					t.Fatal("owned launched SDK process did not die and join", err)
				}
				requireServedNativeDisconnect(t, peer)
				enableChannelOnboardingRecoveryOnStartup(t, opts.ConfigPath)
				process = startServedNativeSDKProcess(t, opts, peer)
				endpoint = process.endpoint(t) + "/v1/rpc"
				peer.awaitPostLogin()
				var restored channelonboarding.Result
				requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": ready.Operation.OperationID}, &restored)
				if restored.Readiness == nil || !restored.Readiness.Ready || restored.Operation.BindingRevision != ready.Operation.BindingRevision ||
					restored.Operation.Coordinate.TargetGeneration != ready.Operation.Coordinate.TargetGeneration ||
					restored.Operation.Coordinate.RuntimeInstanceID == ready.Operation.Coordinate.RuntimeInstanceID {
					t.Fatal("process death replaced original enabled authority or lost readiness", restored)
				}
				if activity {
					requireServedNativeActivityOutcome(t, endpoint, message.ID, "15551234571@s.whatsapp.net", text, "effect_recovery_outcome_unconfirmed")
				} else {
					requireServedNativeUncertainInbox(t, reader, message)
				}
				select {
				case resent := <-peer.sent:
					t.Fatal("ordinary restart replayed an unconfirmed native effect", resent.ID, resent.Body.GetConversation())
				default:
				}
				if err := process.stop(); err != nil {
					t.Fatal("recovered SDK process did not join", err, process.output.String())
				}
			})
		}
	}
}

func servedNativeProcessOptions(t *testing.T, backend string) (cliapp.ServeOptions, string) {
	t.Helper()
	opts := cliapp.ServeOptions{SourceRoot: filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session-reply"),
		PlatformSpecPath: defaultPlatformSpecPath, SwarmDir: t.TempDir(), StoreMode: backend}
	if err := os.Chmod(opts.SwarmDir, 0o700); err != nil {
		t.Fatal(err)
	}
	location := filepath.Join(t.TempDir(), "native-process.sqlite")
	if backend == "sqlite" {
		opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, location, channelOnboardingHostWorkspaceFields())
	} else {
		location = testutil.StartEmptyPostgresDSN(t)
		opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, location)
	}
	return opts, location
}

func requireServedNativeProcessPairing(t *testing.T, endpoint string, peer *serveNativeProtocolPeer) channelonboarding.Result {
	t.Helper()
	var result channelonboarding.Result
	requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", map[string]any{
		"provider": "whatsapp", "verb": "connect", "save_proof": false, "idempotency_key": "native-process-pairing"}, &result)
	id := result.Operation.OperationID
	deadline := time.Now().Add(5 * time.Second)
	for result.Pairing == nil || result.Pairing.Code == "" {
		if time.Now().After(deadline) {
			t.Fatal("SDK process has no authorized pairing disclosure", result)
		}
		time.Sleep(10 * time.Millisecond)
		requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
	}
	peer.pair(result.Pairing.Code)
	peer.awaitPostLogin()
	requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id}, &result)
	if result.IdentityOperation == nil || result.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity {
		t.Fatal("pairing was substituted for principal claim", result)
	}
	from := peer.claim(result.IdentityOperation.Challenge)
	deadline = time.Now().Add(5 * time.Second)
	for result.Operation.Phase != channelonboarding.PhaseAwaitingOperatorConfirmation {
		if time.Now().After(deadline) {
			t.Fatal("genuine SDK claim did not reach public confirmation", result)
		}
		time.Sleep(10 * time.Millisecond)
		requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id}, &result)
	}
	var confirmed struct {
		Binding operatorchannel.Binding `json:"binding"`
	}
	requireServedJSONRPCResult(t, endpoint, "channel.confirm", map[string]any{
		"operation_id": result.IdentityOperation.OperationID, "expected_revision": result.IdentityOperation.Revision, "approve": true}, &confirmed)
	if confirmed.Binding.ExternalAccountRef != from.String() || confirmed.Binding.Status != operatorchannel.BindingCurrent {
		t.Fatal("confirmation did not bind the exact genuine claimant", confirmed)
	}
	requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id}, &result)
	if result.Operation.Phase != channelonboarding.PhaseSucceeded || result.Readiness == nil || !result.Readiness.Ready {
		t.Fatal("SDK process did not establish enabled authority", result)
	}
	message := awaitServedNativeProcessSend(t, peer, "Swarm channel connected.")
	if message.ID != strings.ReplaceAll(result.Operation.ConfirmationOperationID, "-", "") {
		t.Fatal("confirmation send lost its original journal identity", message.ID, result.Operation)
	}
	return result
}

func awaitServedNativeProcessSend(t *testing.T, peer *serveNativeProtocolPeer, prefix string) servedNativeMessage {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case sent := <-peer.sent:
			if strings.HasPrefix(sent.Body.GetConversation(), prefix) {
				return sent
			}
		case <-deadline:
			t.Fatal("real SDK process did not send the selected encrypted output", prefix)
		}
	}
}

func requireServedNativeUncertainInbox(t *testing.T, reader storetest.ChannelObservation, message servedNativeMessage) {
	t.Helper()
	_, sourceID, found := strings.Cut(message.Body.GetConversation(), "\nReference: ")
	operationID, parseErr := uuid.Parse(message.ID)
	if !found || uuid.Validate(sourceID) != nil || parseErr != nil {
		t.Fatal("original encrypted Inbox has no exact source/effect identity", message)
	}
	plans, err := reader.ListCurrentChannelDeliveryPlans(context.Background(), "", 200)
	if err != nil {
		t.Fatal(err)
	}
	for _, plan := range plans {
		if plan.SourceKind == "response" && plan.SourceID == sourceID {
			if plan.State != "uncertain" || plan.CurrentReceiptID != "" {
				t.Fatal("startup lost unconfirmed Inbox evidence or fabricated success", plan)
			}
			receipt, found, err := reader.GetCurrentChannelSentReceipt(context.Background(), plan.DeliveryID, operationID.String())
			if err != nil || found || receipt != (render.SentReceipt{}) {
				t.Fatal("unacknowledged Inbox acquired a sent receipt", receipt, found, err)
			}
			return
		}
	}
	t.Fatal("startup lost the original launched Inbox responsibility", message.ID, sourceID, plans)
}
