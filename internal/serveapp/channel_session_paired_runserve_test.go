//go:build linux || darwin

package serveapp

import (
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
			if backend == "sqlite" {
				opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend,
					filepath.Join(t.TempDir(), "paired-serve.sqlite"), channelOnboardingHostWorkspaceFields())
			} else {
				opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
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
			if code := process.stop(); code != 0 {
				t.Fatal("retained enabled RunServe shutdown failed", code, process.outputString())
			}
			select {
			case sent := <-peer.sent:
				t.Fatal("completed confirmation was resent during ordinary restart", sent.ID)
			default:
			}
		})
	}
}
