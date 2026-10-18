//go:build linux || darwin

package serveapp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/testutil"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

func TestServedExpiredQRExplicitRetryReplacesOriginalOccurrenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			peer := newServeNativeProtocolPeer(t)
			prior := newServeSessionBootstrap
			owners := make(chan *serveSessionBootstrap, 1)
			newServeSessionBootstrap = func(selectRuntime serveSessionBootstrapRuntime, store channelonboarding.Store,
				credentials operatorchannel.CredentialCurrentness, directory string, invocation *sessionprovider.InvocationState,
			) (serveSessionBootstrapOwner, error) {
				owner, err := prior(selectRuntime, store, credentials, directory, invocation)
				if err == nil {
					owners <- owner.(*serveSessionBootstrap)
				}
				return owner, err
			}
			t.Cleanup(func() { newServeSessionBootstrap = prior })
			opts := cliapp.DefaultServeOptions()
			opts.SourceRoot = filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session-reply")
			opts.SwarmDir, opts.SwarmDirSet = t.TempDir(), true
			if err := os.Chmod(opts.SwarmDir, 0o700); err != nil {
				t.Fatal(err)
			}
			opts.StoreMode, opts.StoreModeSet = backend, true
			opts.WorkspaceBackend, opts.WorkspaceBackendSet = "host", true
			opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
			if backend == "sqlite" {
				opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend,
					filepath.Join(t.TempDir(), "qr-replacement.sqlite"), channelOnboardingHostWorkspaceFields())
			} else {
				opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
			}
			process := startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			owner := <-owners
			endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			var original channelonboarding.Result
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", map[string]any{
				"provider": "whatsapp", "verb": "connect", "save_proof": false}, &original)
			original = requireServedNativePairingProgress(t, endpoint, original, time.Now().Add(5*time.Second))
			previous := owner.connection(original.Operation.OperationID)
			if previous == nil || previous.CheckBootstrap(t.Context()) != nil {
				t.Fatal("original QR lacks its live SDK socket")
			}
			var healthy channelonboarding.Result
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{
				"operation_id": original.Operation.OperationID}, &healthy)
			if owner.connection(original.Operation.OperationID) != previous || healthy.Pairing == nil ||
				healthy.Pairing.Code != original.Pairing.Code || healthy.Operation.Revision < original.Operation.Revision {
				t.Fatal("healthy unpaired retry replaced its current QR or occurrence")
			}
			peer.mu.Lock()
			wire := peer.wire
			peer.mu.Unlock()
			if err := wire.Send(peer.ctx, waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{
				"id": "QR_EXHAUSTED", "type": "set", "from": types.ServerJID},
				Content: []waBinary.Node{{Tag: "pair-device"}}}); err != nil {
				t.Fatal("send deterministic QR exhaustion", err)
			}
			wait, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			var expired channelonboarding.Result
			for {
				response := requestServedJSONRPC(t, endpoint, "channel.onboarding_get", map[string]any{
					"operation_id": original.Operation.OperationID})
				if response.Error != nil || json.Unmarshal(response.Result, &expired) != nil {
					t.Fatal("public expiry readback failed")
				}
				if expired.Pairing != nil && expired.Pairing.Status == "qr_expired" {
					break
				}
				select {
				case <-tick.C:
				case <-wait.Done():
					t.Fatal("SDK did not consume the deterministic empty batch")
				}
			}
			if expired.Pairing.Code != "" || expired.Pairing.Paired || expired.Pairing.Connected ||
				previous.CheckBootstrap(t.Context()) != nil {
				t.Fatal("counterexample requires an expired unpaired QR and a still-live socket")
			}
			owner.mu.Lock()
			attempt := owner.connections[original.Operation.OperationID]
			owner.mu.Unlock()
			attempt.mu.Lock()
			finished := attempt.connected && !attempt.connecting && attempt.connectErr == nil
			attempt.mu.Unlock()
			if !finished {
				t.Fatal("counterexample unexpectedly takes the unfinished-connect branch")
			}
			var renewed channelonboarding.Result
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{
				"operation_id": original.Operation.OperationID}, &renewed)
			if owner.connection(original.Operation.OperationID) == previous {
				t.Fatal("explicit retry reused the live occurrence with an exhausted unpaired QR")
			}
			if previous.CheckBootstrap(t.Context()) == nil {
				t.Fatal("successor constructed before the original transport retired")
			}
			renewed = requireServedNativePairingProgress(t, endpoint, renewed, time.Now().Add(5*time.Second))
			if renewed.Operation.OperationID != original.Operation.OperationID || renewed.Operation.PrincipalID != original.Operation.PrincipalID ||
				renewed.Operation.Coordinate != original.Operation.Coordinate || renewed.Operation.Interface != original.Operation.Interface ||
				renewed.Operation.TargetSelector != original.Operation.TargetSelector || renewed.Operation.Revision < expired.Operation.Revision ||
				renewed.Operation.SessionAccount != (operatorchannel.SessionAccountAdmission{}) || renewed.Operation.ActivationRevision != 0 ||
				renewed.IdentityOperation != nil || renewed.Binding != nil || renewed.Readiness != nil && renewed.Readiness.Ready ||
				renewed.Pairing == nil || renewed.Pairing.Status != "awaiting_qr" || renewed.Pairing.Code == "" ||
				renewed.Pairing.Code == original.Pairing.Code || !renewed.Pairing.ExpiresAt.After(original.Pairing.ExpiresAt) ||
				renewed.Pairing.Paired || renewed.Pairing.Connected {
				t.Fatal("replacement lost scope, lacked a fresh QR, or granted identity/execution")
			}
			if _, err := previous.ChannelExecution(t.Context()); err == nil {
				t.Fatal("retired original acquired channel execution")
			}
			if code := process.stop(); code != 0 {
				t.Fatal("replacement serving cleanup failed", code)
			}
		})
	}
}
