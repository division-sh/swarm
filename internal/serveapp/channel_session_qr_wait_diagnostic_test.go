//go:build linux || darwin

package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

func TestReadinessPublicUnpairedQRWaitDiagnosticBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"empty_batch_then_disconnect", "transport_failure"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
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
						filepath.Join(t.TempDir(), "qr-wait.sqlite"), channelOnboardingHostWorkspaceFields())
				} else {
					opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
				}
				process := startServeRuntimeTestProcess(t, opts)
				process.waitForReadyLine()
				owner := <-owners
				endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
				var result channelonboarding.Result
				requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", map[string]any{
					"provider": "whatsapp", "verb": "connect", "save_proof": false}, &result)
				result = requireServedNativePairingProgress(t, endpoint, result, time.Now().Add(5*time.Second))
				if result.Pairing == nil || result.Pairing.Code == "" || result.Pairing.Paired {
					t.Fatal("diagnostic did not reach a genuine unpaired SDK QR")
				}
				owner.mu.Lock()
				attempt := owner.connections[result.Operation.OperationID]
				owner.mu.Unlock()
				connection, err := attempt.finishedConnection(t.Context())
				if err != nil || connection == nil {
					t.Fatal("diagnostic lacks the original completed connection")
				}
				principalOwner, ok := owner.store.(interface {
					EnsureOperatorPrincipal(context.Context, time.Time) (operatorchannel.Principal, error)
				})
				if !ok {
					t.Fatal("diagnostic lacks the canonical principal reader")
				}
				principal, err := principalOwner.EnsureOperatorPrincipal(t.Context(), time.Now().UTC())
				if err != nil || principal.ID != result.Operation.PrincipalID || principal.Validate() != nil {
					t.Fatal("diagnostic principal differs from authenticated public operation")
				}
				foreignPrincipal := principal
				foreignPrincipal.ID = uuid.NewString()
				if foreign, err := connection.PairingReadback(t.Context(), foreignPrincipal); err == nil || foreign.Code != "" {
					t.Fatal("foreign principal obtained pairing material")
				}
				if response := requestServedJSONRPC(t, endpoint, "channel.list", map[string]any{}); response.Error != nil {
					t.Fatal("public list failed before the diagnostic cut")
				}
				peer.mu.Lock()
				wire := peer.wire
				peer.mu.Unlock()
				if mode == "empty_batch_then_disconnect" {
					exhausted := waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{
						"id": "QR_EXHAUSTED", "type": "set", "from": types.ServerJID},
						Content: []waBinary.Node{{Tag: "pair-device"}}}
					if err := wire.Send(peer.ctx, exhausted); err != nil {
						t.Fatal("send deterministic SDK QR exhaustion", err)
					}
					deadline := time.NewTimer(5 * time.Second)
					defer deadline.Stop()
					tick := time.NewTicker(time.Millisecond)
					defer tick.Stop()
					for {
						response := requestServedJSONRPC(t, endpoint, "channel.onboarding_get", map[string]any{
							"operation_id": result.Operation.OperationID})
						if response.Error != nil {
							t.Fatal("QR exhaustion before disconnect returned an internal error")
						}
						var current channelonboarding.Result
						if err := json.Unmarshal(response.Result, &current); err != nil {
							t.Fatal(err)
						}
						if current.Pairing != nil && current.Pairing.Status == "qr_expired" {
							if current.Pairing.Code != "" || current.Pairing.Paired || current.Operation.OperationID != result.Operation.OperationID ||
								!current.Operation.Coordinate.MatchesDeclaration(result.Operation.Coordinate) {
								t.Fatal("QR exhaustion changed scope or retained code")
							}
							t.Log("control: authenticated exhausted SDK batch has qr_expired, no code, original scope, socket still live")
							break
						}
						select {
						case <-tick.C:
						case <-deadline.C:
							t.Fatal("SDK did not consume the controlled empty QR batch")
						}
					}
				}
				peer.disconnect()
				wait, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				tick := time.NewTicker(time.Millisecond)
				defer tick.Stop()
				for connection.CheckBootstrap(t.Context()) == nil {
					select {
					case <-tick.C:
					case <-wait.Done():
						t.Fatal("SDK failure did not reach its existing lifetime fence")
					}
				}
				finished, finishedErr := attempt.finishedConnection(t.Context())
				_, currentErr := owner.currentConnection(t.Context(), result.Operation.OperationID)
				bootstrapErr := connection.CheckBootstrap(t.Context())
				private, privateErr := connection.PairingReadback(t.Context(), principal)
				t.Logf("discriminator retained_connection=%t finished_error=%s current_error=%s bootstrap_error=%s bootstrap_fenced=%t private_error=%s private_status=%q private_code_present=%t original_scope=%t",
					finished == connection, readinessErrorTypes(finishedErr), readinessErrorTypes(currentErr), readinessErrorTypes(bootstrapErr),
					bootstrapErr != nil && strings.Contains(bootstrapErr.Error(), "WhatsApp client occurrence is fenced"),
					readinessErrorTypes(privateErr), private.Status, private.Code != "",
					attempt.operation.OperationID == result.Operation.OperationID && attempt.operation.PrincipalID == principal.ID &&
						attempt.operation.Coordinate.MatchesDeclaration(result.Operation.Coordinate))
				if private.Code != "" {
					t.Fatal("stopped pairing disclosed code")
				}
				response := requestServedJSONRPC(t, endpoint, "channel.list", map[string]any{})
				if response.Error != nil {
					t.Errorf("public channel.list at controlled SDK cut: code=%d (no secret error payload logged)", response.Error.Code)
				}
				if code := process.stop(); code != 0 {
					t.Error("diagnostic serving cleanup failed", code)
				}
			})
		}
	}
}

func readinessErrorTypes(err error) string {
	if err == nil {
		return "none"
	}
	parts := []string{fmt.Sprintf("%T", err)}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		for _, child := range wrapped.Unwrap() {
			parts = append(parts, readinessErrorTypes(child))
		}
	case interface{ Unwrap() error }:
		parts = append(parts, readinessErrorTypes(wrapped.Unwrap()))
	}
	return strings.Join(parts, "/")
}
