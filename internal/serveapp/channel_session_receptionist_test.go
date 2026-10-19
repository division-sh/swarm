//go:build linux || darwin

package serveapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/testutil"
)

// A comms-only connection carries customer traffic, not operator authority.
func TestRunServeWhatsAppReceptionistWithoutOperatorBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			peer := newServeNativeProtocolPeer(t)
			opts := cliapp.ServeOptions{
				SourceRoot:    filepath.Join(repoRootForTest(), "internal/cliapp/archetypes/whatsapp-receptionist"),
				APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
				SwarmDir: t.TempDir(), SwarmDirSet: true, SelfCheck: true,
				WorkspaceBackend: "host", WorkspaceBackendSet: true, StoreMode: backend, StoreModeSet: true,
			}
			if err := os.Chmod(opts.SwarmDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if backend == "sqlite" {
				opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend,
					filepath.Join(t.TempDir(), "receptionist.sqlite"), channelOnboardingHostWorkspaceFields())
			} else {
				opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
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
			peer.pair(result.Pairing.Code)
			peer.awaitPostLogin()
			id := result.Operation.OperationID
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id, "checkpoint_only": true}, &result)
			if result.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity || result.Operation.ActivationRevision != 0 ||
				result.IdentityOperation != nil || result.Connection == nil || !result.Connection.Linked || result.Readiness != nil && result.Readiness.Ready {
				t.Fatal("account-preview checkpoint published customer traffic or operator authority", result)
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
			peer.awaitPostLogin()
			id = result.Operation.OperationID
			response := requestServedJSONRPC(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id})
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
			if response.Error != nil {
				t.Fatal("receptionist continuation refused", response.Error, "phase", result.Operation.Phase, "target", result.Operation.Coordinate.TargetGeneration)
			}
			if result.Operation.Phase != channelonboarding.PhaseSucceeded || result.Operation.Ceremony != channelonboarding.CeremonyAccountLink ||
				result.IdentityOperation != nil || result.Operation.BindingRevision != 0 || result.Readiness == nil || !result.Readiness.Ready {
				t.Fatalf("customer channel was not independently READY: %+v", result)
			}
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", map[string]any{"provider": "whatsapp", "verb": "connect", "save_proof": false}, &result)
			if result.Operation.OperationID != id || result.Readiness == nil || !result.Readiness.Ready {
				t.Fatal("second connect did not attach to the current connection", result)
			}
			requireServedNativeAuthoredCustomerReply(t, endpoint, peer, false)
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
			if !strings.Contains(process.outputString(), "WhatsApp: reconnected (+") {
				t.Fatal("restart did not show the linked account")
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
