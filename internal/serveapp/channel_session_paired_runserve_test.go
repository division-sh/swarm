//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/store/sessionstate"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestRunServeWhatsAppSignedPairingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			peer := newServeNativeProtocolPeer(t)
			opts := cliapp.ServeOptions{SourceRoot: filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session"),
				PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
				SwarmDir: t.TempDir(), SwarmDirSet: true, SelfCheck: true,
				WorkspaceBackend: "host", WorkspaceBackendSet: true, StoreMode: backend, StoreModeSet: true}
			if err := os.Chmod(opts.SwarmDir, 0o700); err != nil {
				t.Fatal(err)
			}
			var activationCommitted atomic.Bool
			opts.TestChannelOnboardingBarrier = func(boundary channelonboarding.TestLifecycleBoundary, _ string) error {
				if boundary == channelonboarding.TestAfterActivationCommitBeforePublication {
					activationCommitted.Store(true)
					return errors.New("interrupt served native activation before process publication")
				}
				return nil
			}
			storeDSN := ""
			if backend == "sqlite" {
				storeDSN = filepath.Join(t.TempDir(), "paired-serve.sqlite")
				opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend,
					storeDSN, channelOnboardingHostWorkspaceFields())
			} else {
				storeDSN = testutil.StartEmptyPostgresDSN(t)
				opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, storeDSN)
			}
			process := startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			var result channelonboarding.Result
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", map[string]any{
				"provider": "whatsapp", "verb": "connect", "save_proof": false, "idempotency_key": "native-pairing"}, &result)
			id := result.Operation.OperationID
			deadline := time.Now().Add(5 * time.Second)
			for result.Pairing == nil || result.Pairing.Code == "" {
				if time.Now().After(deadline) {
					t.Fatal("served owner did not disclose authorized QR", result.Operation.Phase, result.Pairing)
				}
				time.Sleep(10 * time.Millisecond)
				requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
			}
			peer.pair(result.Pairing.Code)
			result = channelonboarding.Result{}
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id}, &result)
			if result.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity || result.IdentityOperation == nil ||
				result.Pairing != nil || result.Operation.Coordinate.TargetGeneration != 0 {
				t.Fatal("signed pairing did not reach the original runless operator claim", result)
			}
			identityID := result.IdentityOperation.OperationID
			from := peer.claim(result.IdentityOperation.Challenge)
			previous := result.Operation.Coordinate
			if code := process.stop(); code != 0 {
				t.Fatal("paired RunServe did not join before recovery", code, process.outputString())
			}
			process = startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			result = channelonboarding.Result{}
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id}, &result)
			if result.Operation.Phase != channelonboarding.PhaseAwaitingOperatorConfirmation || result.IdentityOperation == nil ||
				result.IdentityOperation.OperationID != identityID ||
				result.IdentityOperation.State != operatorchannel.StateAwaitingConfirmation ||
				result.IdentityOperation.ExternalAccountRef != operatorchannel.MaskPresentation(from.String()) || result.Operation.Coordinate.TargetGeneration != 0 ||
				result.Operation.Coordinate.RuntimeInstanceID == previous.RuntimeInstanceID || result.Operation.Coordinate.BundleHash != previous.BundleHash {
				t.Fatal("encrypted claim did not reach authenticated confirmation without a business target", result.Operation, result.IdentityOperation, "want sender", from.String())
			}
			peer.mu.Lock()
			connections := peer.connections
			peer.mu.Unlock()
			if connections < 2 {
				t.Fatal("ordinary restart did not restore a genuine SDK connection", connections)
			}
			var confirmed struct {
				Operation operatorchannel.Operation `json:"operation"`
				Binding   operatorchannel.Binding   `json:"binding"`
			}
			requireServedJSONRPCResult(t, endpoint, "channel.confirm", map[string]any{
				"operation_id": result.IdentityOperation.OperationID, "expected_revision": result.IdentityOperation.Revision, "approve": true}, &confirmed)
			if confirmed.Operation.State != operatorchannel.StateBound || confirmed.Binding.Status != operatorchannel.BindingCurrent {
				t.Fatal("public confirmation did not commit the exact encrypted claimant", confirmed)
			}
			if confirmed.Binding.ExternalAccountRef != from.String() || confirmed.Binding.ConversationRef != from.String() {
				t.Fatal("confirmed binding differs from the genuine encrypted claimant", confirmed.Binding)
			}
			interrupted := requestServedJSONRPC(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id})
			if interrupted.Error == nil || !activationCommitted.Load() {
				t.Fatal("native target/activation did not reach the deliberate publication interruption", interrupted)
			}
			result = channelonboarding.Result{}
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
			if result.Operation.Phase != channelonboarding.PhasePublishingProcessActivation || result.Operation.Coordinate.TargetGeneration < 1 ||
				result.Operation.BindingRevision != confirmed.Binding.Revision || result.Operation.ActivationRevision < 1 {
				t.Fatal("served native target and activation were not committed through their canonical owners", result.Operation)
			}
			if code := process.stop(); code != 0 {
				t.Fatal("paired RunServe shutdown failed", code, process.outputString())
			}
			opts.TestChannelOnboardingBarrier = nil
			process = startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			result = channelonboarding.Result{}
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
			if result.Operation.Phase != channelonboarding.PhaseSucceeded || result.Readiness == nil || !result.Readiness.Ready {
				t.Fatal("interrupted native activation did not recover to executable readiness", result)
			}
			select {
			case sent := <-peer.sent:
				if sent.ID != strings.ReplaceAll(result.Operation.ConfirmationOperationID, "-", "") || sent.Body.GetConversation() != "Swarm channel connected." {
					t.Fatal("served confirmation journal differs from decrypted provider output", sent)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("ready native channel had no genuine decrypted confirmation send")
			}
			if code := process.stop(); code != 0 {
				t.Fatal("enabled RunServe shutdown failed", code, process.outputString())
			}
			completed := result.Operation
			process = startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			result = channelonboarding.Result{}
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
			if result.Operation.Phase != channelonboarding.PhaseSucceeded || result.Readiness == nil || !result.Readiness.Ready ||
				result.Operation.IdentityOperationID != identityID || result.Operation.BindingRevision != completed.BindingRevision ||
				result.Operation.Coordinate.TargetGeneration != completed.Coordinate.TargetGeneration ||
				result.Operation.Coordinate.RuntimeInstanceID == completed.Coordinate.RuntimeInstanceID {
				t.Fatal("completed native restart lost its original responsibility", result.Operation, result.Readiness)
			}
			select {
			case sent := <-peer.sent:
				t.Fatal("completed confirmation was resent during ordinary restart", sent.ID)
			default:
			}
			reader, err := storetest.OpenChannelObservation(backend, storeDSN)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := reader.Close(); err != nil {
					t.Error(err)
				}
			})
			peer.text("/inbox", "SERVED_INBOX")
			requireServedNativeInboxReply(t, endpoint, peer, reader)
			pairingPath := filepath.Join(opts.SwarmDir, result.Operation.SessionConnectionID, "session.json")
			pairingHeader, err := os.ReadFile(pairingPath)
			if err != nil {
				t.Fatal(err)
			}
			var unbound struct {
				Binding operatorchannel.Binding `json:"binding"`
			}
			for range 2 {
				requireServedJSONRPCResult(t, endpoint, "channel.unbind", map[string]any{
					"interface": result.Operation.Interface.Selector, "expected_revision": confirmed.Binding.Revision,
					"idempotency_key": "served-native-unbind"}, &unbound)
				if unbound.Binding.Status == operatorchannel.BindingCurrent || unbound.Binding.Revision <= confirmed.Binding.Revision {
					t.Fatal("public unbind retained executable principal authority", unbound.Binding)
				}
			}
			result = channelonboarding.Result{}
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
			if result.Operation.Phase != channelonboarding.PhaseRetired || result.Readiness != nil && result.Readiness.Ready || result.Pairing != nil {
				t.Fatal("public native unbind remained ready or disclosed new pairing material", result)
			}
			connected := requireServedNativeDisconnect(t, peer)
			if code := process.stop(); code != 0 {
				t.Fatal("retained enabled RunServe shutdown failed", code, process.outputString())
			}
			process = startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			result = channelonboarding.Result{}
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
			if result.Operation.Phase != channelonboarding.PhaseRetired || result.Readiness != nil && result.Readiness.Ready {
				t.Fatal("ordinary restart adopted the unbound native responsibility", result)
			}
			peer.mu.Lock()
			unchanged := peer.connections == connected
			peer.mu.Unlock()
			if !unchanged {
				t.Fatal("ordinary restart reconnected an explicitly retired parent")
			}
			if code := process.stop(); code != 0 {
				t.Fatal("post-unbind dormant RunServe shutdown failed", code, process.outputString())
			}
			after, err := os.ReadFile(pairingPath)
			if err != nil || !bytes.Equal(pairingHeader, after) {
				t.Fatal("unbind/restart deleted or rewrote retained v1 pairing identity", err)
			}
			fixture, providerState, err := sessionstate.OpenSDKFixture(context.Background(), filepath.Join(opts.SwarmDir, completed.SessionConnectionID, "provider.db"))
			if err != nil {
				t.Fatal(err)
			}
			device, err := providerState.CurrentDevice(context.Background(), completed.SessionAccount.AccountRef)
			closeErr := fixture.Close()
			if err != nil || closeErr != nil || device == nil || device.ID == nil || device.ID.ToNonAD().String() != completed.SessionAccount.AccountRef {
				t.Fatal("public unbind deleted or adopted the SDK-owned paired account", err, closeErr)
			}
			select {
			case sent := <-peer.sent:
				t.Fatal("served Inbox or confirmation was resent without another request", sent.ID)
			default:
			}
		})
	}
}

func requireServedNativeDisconnect(t *testing.T, peer *serveNativeProtocolPeer) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		peer.mu.Lock()
		connected, disconnected := peer.connections, peer.disconnected
		peer.mu.Unlock()
		if connected == disconnected {
			return connected
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("public unbind did not close its original SDK wire")
	return 0
}

func requireServedNativeInboxReply(t *testing.T, endpoint string, peer *serveNativeProtocolPeer, reader storetest.ChannelObservation) {
	t.Helper()
	var sent servedNativeMessage
	select {
	case sent = <-peer.sent:
	case <-time.After(5 * time.Second):
		t.Fatal("served encrypted Inbox request did not produce a decrypted reply")
	}
	var page struct {
		Items []struct {
			Kind string `json:"kind"`
			Card struct {
				Title string `json:"title"`
			} `json:"decision_card"`
			Notice struct {
				Type string `json:"type"`
			} `json:"notice"`
		} `json:"items"`
		Unread     int    `json:"unread_informational_notices"`
		NextCursor string `json:"next_cursor"`
	}
	requireServedJSONRPCResult(t, endpoint, "mailbox.list", map[string]any{"status": "pending", "limit": 200}, &page)
	if page.NextCursor != "" || page.Unread != 0 {
		t.Fatal("served baseline Inbox has unexpected notices or pages", page)
	}
	lines := []string{"Inbox", "Unread notices: 0", "No open items"}
	if len(page.Items) > 0 {
		lines[2] = "Open items:"
		for _, item := range page.Items {
			label := item.Card.Title
			if item.Kind == "notice" {
				label = item.Notice.Type
			}
			if label == "" {
				t.Fatal("public Inbox entry lost its canonical label", item)
			}
			lines = append(lines, "- "+label)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		plans, err := reader.ListCurrentChannelDeliveryPlans(context.Background(), "", 200)
		if err != nil {
			t.Fatal(err)
		}
		for _, plan := range plans {
			if plan.SourceKind != "response" || plan.State != "sent" {
				continue
			}
			receipt, found, err := reader.GetCurrentChannelSentReceipt(context.Background(), plan.DeliveryID, plan.CurrentReceiptID)
			if err != nil {
				t.Fatal(err)
			}
			if !found || receipt.DeliveryReference != sent.ID {
				continue
			}
			want := strings.Join(lines, "\n") + "\nReference: " + plan.SourceID
			if sent.Body.GetConversation() != want || sent.ID != strings.ReplaceAll(receipt.OperationID, "-", "") {
				t.Fatal("decrypted native Inbox differs from canonical public list/exact journal", sent.Body.GetConversation(), want, receipt)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("decrypted native Inbox had no exact settled delivery receipt", sent.ID)
}
