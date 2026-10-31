//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/testutil"
)

type firstConnectCheckpointKey struct{}

// This adapter only observes the existing invocation-owned session. It returns
// the original native results, without supplying pairing or account authority.
type firstConnectPairingObserver struct {
	cliapp.ChannelRegistrationSession
	principal operatorchannel.Principal
	observe   func(channelonboarding.Result, bool)
}

func (s firstConnectPairingObserver) ReadSessionPairing(ctx context.Context, op channelonboarding.Operation, principal operatorchannel.Principal) (channelonboarding.PairingReadback, error) {
	view, err := s.ChannelRegistrationSession.ReadSessionPairing(ctx, op, principal)
	if err == nil {
		s.observe(channelonboarding.Result{Operation: op, Pairing: &view}, false)
	}
	return view, err
}

func (s firstConnectPairingObserver) CheckpointSessionPairing(ctx context.Context, op channelonboarding.Operation) (channelonboarding.Operation, bool, error) {
	next, paired, err := s.ChannelRegistrationSession.CheckpointSessionPairing(ctx, op)
	if err == nil && !paired {
		view, readErr := s.ChannelRegistrationSession.ReadSessionPairing(ctx, next, s.principal)
		if readErr == nil {
			s.observe(channelonboarding.Result{Operation: next, Pairing: &view}, true)
		}
	}
	return next, paired, err
}

// Sitting steps 1-3: public connect must register the approved account after a
// checkpoint BEFORE stream end, with serve stopped as well as already running.
func TestChannelConnectWhatsAppRegistersAccountBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, serving := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/serve_running=%t", backend, serving), func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
				peer := newServeNativeProtocolPeer(t)
				streamEnd := make(chan struct{})
				peer.loginHandoff, peer.loginHandoffRelease = true, streamEnd
				var release sync.Once
				t.Cleanup(func() { release.Do(func() { close(streamEnd) }) })
				qr := make(chan channelonboarding.Result, 1)
				var pendingCheckpoint atomic.Bool
				observe := func(result channelonboarding.Result, checkpoint bool) {
					if view := result.Pairing; view != nil {
						if view.Code != "" {
							select {
							case qr <- result:
							default:
							}
						}
						if checkpoint && view.Paired && !view.Connected && result.Operation.Phase == channelonboarding.PhaseActivatingProvider {
							peer.mu.Lock()
							beforeEnd := peer.connections == 1 && peer.disconnected == 0
							peer.mu.Unlock()
							if !beforeEnd {
								t.Error("first pending checkpoint did not precede stream end")
							}
							pendingCheckpoint.Store(true)
							release.Do(func() { close(streamEnd) })
						}
					}
				}
				factory := func(ctx context.Context, options cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
					session, err := OpenChannelRegistration(ctx, options)
					if err != nil {
						return nil, err
					}
					return firstConnectPairingObserver{ChannelRegistrationSession: session, principal: options.Principal, observe: observe}, nil
				}
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
					opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, filepath.Join(t.TempDir(), "first-connect.sqlite"), channelOnboardingHostWorkspaceFields())
				} else {
					opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
				}
				config, err := os.ReadFile(opts.ConfigPath)
				if err != nil {
					t.Fatal(err)
				}
				config = append(config, []byte(fmt.Sprintf("\npaths:\n  swarm_dir: %q\n", opts.SwarmDir))...)
				writeStandingCandidateFile(t, opts.ConfigPath, string(config))
				if serving {
					process := startServeRuntimeTestProcess(t, opts)
					process.waitForReadyLine()
					defer func() {
						if code := process.stop(); code != 0 {
							t.Errorf("served first-connect shutdown exit=%d", code)
						}
					}()
					target, err := url.Parse("http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()))
					if err != nil {
						t.Fatal(err)
					}
					forward := httputil.NewSingleHostReverseProxy(target)
					forward.ModifyResponse = func(response *http.Response) error {
						body, err := io.ReadAll(response.Body)
						if err != nil {
							return err
						}
						_ = response.Body.Close()
						response.Body = io.NopCloser(bytes.NewReader(body))
						var result struct {
							Result channelonboarding.Result `json:"result"`
						}
						if json.Unmarshal(body, &result) == nil {
							observe(result.Result, response.Request.Context().Value(firstConnectCheckpointKey{}) == true)
						}
						return nil
					}
					proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						body, err := io.ReadAll(r.Body)
						if err != nil {
							t.Error(err)
							http.Error(w, "read failed", http.StatusBadRequest)
							return
						}
						var request struct {
							Method string `json:"method"`
							Params struct {
								Checkpoint bool `json:"checkpoint_only"`
							} `json:"params"`
						}
						if json.Unmarshal(body, &request) != nil {
							t.Error("invalid public RPC")
							http.Error(w, "invalid RPC", http.StatusBadRequest)
							return
						}
						r.Body = io.NopCloser(bytes.NewReader(body))
						ctx := context.WithValue(r.Context(), firstConnectCheckpointKey{}, request.Method == "channel.onboarding_retry" && request.Params.Checkpoint)
						forward.ServeHTTP(w, r.WithContext(ctx))
					}))
					defer proxy.Close()
					token := filepath.Join(t.TempDir(), "api-token")
					writeStandingCandidateFile(t, token, apiv1.DefaultLoopbackAPIToken+"\n")
					writeStandingCandidateFile(t, opts.ConfigPath, string(config)+fmt.Sprintf("\nconnection:\n  api_server: %q\n  api_token_file: %q\n", proxy.URL, token))
				}
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
				defer cancel()
				out, errOut := &lockedBuffer{}, &lockedBuffer{}
				done := make(chan int, 1)
				joined := false
				defer func() {
					cancel()
					if !joined {
						<-done
					}
				}()
				go func() {
					done <- cliapp.Execute(ctx, []string{"--config", opts.ConfigPath, "channel", "connect", "whatsapp", "--yes"}, out, errOut, nil, nil, factory)
				}()
				var pairing channelonboarding.Result
				select {
				case pairing = <-qr:
				case code := <-done:
					joined = true
					t.Fatalf("public connect exited before QR: exit=%d err=%s", code, errOut.String())
				case <-ctx.Done():
					t.Fatal("public connect did not disclose its SDK QR")
				}
				if !pairing.Operation.DeploymentRegistration || pairing.Operation.AccountConfirmed || pairing.Pairing.Paired {
					t.Fatal("first QR already claimed a registered or paired account")
				}
				peer.pair(pairing.Pairing.Code)
				select {
				case code := <-done:
					joined = true
					if code != 0 {
						t.Fatalf("public signed first-connect exit=%d err=%s", code, errOut.String())
					}
				case <-ctx.Done():
					t.Fatalf("public signed first-connect stalled: err=%s", errOut.String())
				}
				if !pendingCheckpoint.Load() {
					t.Fatal("first checkpoint did not exercise the pre-stream-end pending window")
				}
				if !strings.Contains(out.String(), "Registered whatsapp account +15551234567.") || !strings.Contains(out.String(), "Linked to +15551234567") {
					t.Fatal("public connect omitted linked-account approval or registration")
				}
				var listing struct {
					Accounts []channelonboarding.Result `json:"accounts"`
				}
				var listed, listErr bytes.Buffer
				if code := cliapp.Execute(ctx, []string{"--config", opts.ConfigPath, "channel", "list", "--json"}, &listed, &listErr, nil, nil, factory); code != 0 || json.Unmarshal(listed.Bytes(), &listing) != nil || len(listing.Accounts) != 1 {
					t.Fatalf("public account-list exit=%d err=%s", code, listErr.String())
				}
				op := listing.Accounts[0].Operation
				if op.OperationID != pairing.Operation.OperationID || op.SessionConnectionID != pairing.Operation.SessionConnectionID || op.Phase != channelonboarding.PhaseSucceeded || !op.AccountConfirmed || op.AccountReference != "+15551234567" || op.IdentityOperationID != "" {
					t.Fatal("first connect replaced its device/operation or failed to register the approved receptionist account")
				}
				peer.mu.Lock()
				connections, logins, uploads := peer.connections, peer.postLogins, peer.prekeyUploads
				peer.mu.Unlock()
				if connections != 2 || logins != 1 || uploads != 1 {
					t.Fatalf("pairing/login ownership: connections=%d logins=%d uploads=%d", connections, logins, uploads)
				}
			})
		}
	}
}
