//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/sessioncapture"
	"github.com/division-sh/swarm/internal/store/sessionstate"
	"github.com/division-sh/swarm/internal/testutil"
	"go.mau.fi/whatsmeow/types"
)

type firstConnectCheckpointKey struct{}

type firstConnectLoginRead struct {
	channelonboarding.OperationStore
	login      *servedNativeLoginGate
	invocation context.Context
	opened     func() bool
	once       sync.Once
}

func (s *firstConnectLoginRead) GetChannelOnboarding(ctx context.Context, id string) (channelonboarding.Operation, error) {
	op, err := s.OperationStore.GetChannelOnboarding(ctx, id)
	// Constructor reads have no connection; Connect reads use a leased context.
	// Only the confirmed resume's invocation read releases the login response.
	if err == nil && ctx == s.invocation && s.opened() {
		select {
		case <-s.login.held:
			s.once.Do(func() { s.login.release <- true })
		case <-ctx.Done():
			return op, context.Cause(ctx)
		}
	}
	return op, err
}

// This adapter only observes the existing invocation-owned session. It returns
// the original native results, without supplying pairing or account authority.
type firstConnectPairingObserver struct {
	cliapp.ChannelRegistrationSession
	principal operatorchannel.Principal
	observe   func(channelonboarding.Result, bool)
	afterRead func(channelonboarding.PairingReadback, error)
}

func (s firstConnectPairingObserver) ReadSessionPairing(ctx context.Context, op channelonboarding.Operation, principal operatorchannel.Principal) (channelonboarding.PairingReadback, error) {
	view, err := s.ChannelRegistrationSession.ReadSessionPairing(ctx, op, principal)
	if s.afterRead != nil {
		s.afterRead(view, err)
	}
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
				var pairSuccessGate atomic.Pointer[servedPairSuccessGate]
				var pendingCheckpoint atomic.Bool
				observe := func(result channelonboarding.Result, checkpoint bool) {
					if view := result.Pairing; view != nil {
						if view.Code != "" {
							select {
							case qr <- result:
							default:
							}
						}
						if checkpoint && view.Paired && !view.Connected && result.Operation.Phase == channelonboarding.PhaseActivatingProvider && pendingCheckpoint.CompareAndSwap(false, true) {
							peer.mu.Lock()
							beforeEnd := peer.connections == 1 && peer.disconnected == 0
							peer.mu.Unlock()
							if !beforeEnd {
								t.Error("first pending checkpoint did not precede stream end")
							}
							release.Do(func() { close(streamEnd) })
						}
					}
				}
				factory := func(ctx context.Context, options cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
					session, err := OpenChannelRegistration(ctx, options)
					if err != nil {
						return nil, err
					}
					return firstConnectPairingObserver{ChannelRegistrationSession: session, principal: options.Principal, observe: observe,
						afterRead: func(view channelonboarding.PairingReadback, err error) {
							if gate := pairSuccessGate.Load(); gate != nil && (err != nil || view.Paired) {
								gate.complete()
							}
						}}, nil
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
				if op.OperationID != pairing.Operation.OperationID || op.Phase != channelonboarding.PhaseSucceeded || !op.AccountConfirmed || op.AccountReference != "+15551234567" || op.IdentityOperationID != "" {
					t.Fatal("first connect replaced its device/operation or failed to register the approved receptionist account")
				}
				peer.mu.Lock()
				connections, logins, uploads := peer.connections, peer.postLogins, peer.prekeyUploads
				peer.mu.Unlock()
				if connections != 2 || logins != 1 || uploads != 1 {
					t.Fatalf("pairing/login ownership: connections=%d logins=%d uploads=%d", connections, logins, uploads)
				}
				if !serving {
					requireOfflinePairedWhatsAppServiceRoundTrip(t, opts, peer, op)
					for _, earlyReconnect := range []bool{false, true} {
						name := "phone_unlink_reverify"
						if earlyReconnect {
							name = "provider_515_before_pair_success"
						}
						t.Run(name, func(t *testing.T) {
							var heldPairSuccess *servedPairSuccessGate
							if earlyReconnect {
								heldPairSuccess = peer.holdPairSuccess(t)
							}
							peer.mu.Lock()
							peer.rejectNextLogin = true
							peer.mu.Unlock()
							process := startServeRuntimeTestProcess(t, opts)
							if code, exited := process.waitForExit(5 * time.Second); !exited || code != 1 || serveOutputIsReady(process.outputString()) {
								t.Fatalf("revoked account did not refuse startup: exit=%d exited=%t\n%s", code, exited, process.outputString())
							}
							select {
							case <-qr:
							default:
							}
							ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
							login := &servedNativeLoginGate{held: make(chan struct{}), release: make(chan bool, 1)}
							peer.mu.Lock()
							peer.rejectNextLogin, peer.nextLogin = true, login
							peer.mu.Unlock()
							reverify := func(ctx context.Context, options cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
								reads := &firstConnectLoginRead{OperationStore: options.Operations, login: login, invocation: ctx}
								options.Operations = reads
								session, err := factory(ctx, options)
								if err != nil {
									return session, err
								}
								native := session.(firstConnectPairingObserver).ChannelRegistrationSession.(*deploymentChannelSession)
								reads.opened = func() bool { return native.connection != nil }
								return session, nil
							}
							out, errOut := &lockedBuffer{}, &lockedBuffer{}
							done := make(chan int, 1)
							joined := false
							defer func() {
								select {
								case login.release <- false:
								default:
								}
								cancel()
								if !joined {
									<-done
								}
							}()
							go func() {
								done <- cliapp.Execute(ctx, []string{"--config", opts.ConfigPath, "channel", "connect", "whatsapp", op.AccountReference, "--yes"}, out, errOut, nil, nil, reverify)
							}()
							var replacement channelonboarding.Result
							select {
							case replacement = <-qr:
							case code := <-done:
								joined = true
								t.Fatalf("revoked-account reverify exited before relink QR: exit=%d err=%s", code, errOut.String())
							case <-ctx.Done():
								t.Fatal("revoked-account reverify did not offer relink")
							}
							if replacement.Operation.OperationID == op.OperationID || replacement.Operation.SessionConnectionID == pairing.Operation.SessionConnectionID ||
								replacement.Operation.AccountConfirmed || replacement.Pairing.Paired || !strings.Contains(out.String(), "Account was logged out from the phone. Link it again:") {
								t.Fatal("revoked account retained approval or missed exact relink")
							}
							if earlyReconnect {
								pairSuccessGate.Store(heldPairSuccess)
								t.Cleanup(func() { pairSuccessGate.Store(nil) })
							}
							peer.pair(replacement.Pairing.Code)
							select {
							case code := <-done:
								joined = true
								if code != 0 {
									t.Fatalf("public same-account relink exit=%d err=%s", code, errOut.String())
								}
							case <-ctx.Done():
								t.Fatal("public same-account relink did not register")
							}
							var listed, listErr bytes.Buffer
							if code := cliapp.Execute(ctx, []string{"--config", opts.ConfigPath, "channel", "list", "--json"}, &listed, &listErr, nil, nil, factory); code != 0 ||
								json.Unmarshal(listed.Bytes(), &listing) != nil || len(listing.Accounts) != 1 {
								t.Fatalf("relinked public account-list exit=%d err=%s", code, listErr.String())
							}
							current := listing.Accounts[0].Operation
							if current.OperationID != replacement.Operation.OperationID ||
								current.AccountReference != op.AccountReference || !current.AccountConfirmed || current.Phase != channelonboarding.PhaseSucceeded || current.IdentityOperationID != "" {
								t.Fatal("relink did not register exactly the same approved account under its fresh reservation")
							}
							op = current
						})
					}
				}
			})
		}
	}
}

// Only the external WhatsApp service is mocked. Pairing, boot adoption, authored
// send, encrypted customer ingress and restart use their ordinary owners.
func requireOfflinePairedWhatsAppServiceRoundTrip(t *testing.T, opts cliapp.ServeOptions, peer *serveNativeProtocolPeer, registered channelonboarding.Operation) {
	t.Helper()
	enableChannelOnboardingRecoveryOnStartup(t, opts.ConfigPath)
	publicListener := reserveChannelOnboardingListener(t)
	opts.PublicWebhookListen, opts.PublicWebhookListener = publicListener.Addr().String(), publicListener
	publicTarget, err := url.Parse("http://" + opts.PublicWebhookListen)
	if err != nil {
		t.Fatal(err)
	}
	publicOrigin := httptest.NewTLSServer(httputil.NewSingleHostReverseProxy(publicTarget))
	t.Cleanup(publicOrigin.Close)
	opts.PublicWebhookBaseURL = publicOrigin.URL
	source := t.TempDir()
	copyReleaseFixtureTree(t, opts.SourceRoot, source)
	opts.SourceRoot = source
	writeStandingCandidateFile(t, filepath.Join(source, "events.yaml"), "outbound.requested:\n  destination: text\n  text: text\n")
	schema, err := os.ReadFile(filepath.Join(source, "schema.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	writeStandingCandidateFile(t, filepath.Join(source, "schema.yaml"), strings.Replace(string(schema), "    - inbound.whatsapp.raw", "    - outbound.requested\n    - inbound.whatsapp.raw", 1))
	nodes, err := os.ReadFile(filepath.Join(source, "nodes.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	writeStandingCandidateFile(t, filepath.Join(source, "nodes.yaml"), string(nodes)+`
service-probe:
  execution_type: system_node
  subscribes_to: [outbound.requested]
  event_handlers:
    outbound.requested:
      activity:
        id: service_probe
        tool: whatsapp.send_text
        input:
          account: policy.operator_account
          destination: payload.destination
          text: payload.text
`)
	process := startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
	peer.awaitPostLogin()
	var runs struct {
		Runs []operatorread.RunHeader `json:"runs"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.list", map[string]any{"limit": 100}, &runs)
	runID := ""
	for _, run := range runs.Runs {
		if run.FlowPath == "." && run.Status == "live" {
			if runID != "" {
				t.Fatal("service mock journey found competing live root runs")
			}
			runID = run.RunID
		}
	}
	if runID == "" {
		t.Fatal("service mock journey has no admitted live root run")
	}
	sender, customer, customerLID := peer.senderFrom(types.NewJID("15551234571", types.DefaultUserServer), types.NewJID("100000000005", types.HiddenUserServer))
	const outgoing = "A successful first send before the customer's reply"
	var published struct {
		EventID string `json:"event_id"`
	}
	requireServedJSONRPCResult(t, endpoint, "event.publish", map[string]any{
		"run_id": runID, "event_name": "outbound.requested", "idempotency_key": "service-mock-first-send",
		"payload": map[string]any{"destination": customer.String(), "text": outgoing},
	}, &published)
	if published.EventID == "" {
		t.Fatal("service mock initial send has no authored input receipt")
	}
	var sent servedNativeMessage
	select {
	case sent = <-peer.sent:
		if sent.To.ToNonAD() != customer && sent.To.ToNonAD() != customerLID || sent.Body.GetConversation() != outgoing || sent.ID == "" {
			t.Fatalf("service mock did not receive the exact first send: to=%s text=%q id=%s", sent.To, sent.Body.GetConversation(), sent.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("service mock did not receive the authored first send")
	}
	requireServedNativeActivityResult(t, endpoint, sent.ID, customer.String(), outgoing, false)
	bootOutput := process.outputString()
	for _, stale := range []string{"NOT READY · SESSION_ACCOUNT · session admission required", "No channels connected yet"} {
		if strings.Contains(bootOutput, stale) {
			t.Errorf("successful WhatsApp send retained stale boot message %q:\n%s", stale, bootOutput)
		}
	}
	if !strings.Contains(bootOutput, "WhatsApp "+registered.AccountReference+" (.): connected") {
		t.Errorf("boot omitted the connected WhatsApp account:\n%s", bootOutput)
	}
	const incomingID = "OFFLINE_PAIRED_FIRST_BUSINESS_REPLY"
	const incomingText = "The customer's first reply after the successful send"
	peer.sendEncryptedFrame(sender.EncryptReply(t, incomingText, incomingID, sent.ID, outgoing))
	acknowledged := false
	select {
	case receipt := <-peer.receipts:
		acknowledged = receipt.Attrs["id"] == incomingID && receipt.Attrs["type"] != "retry"
	case <-time.After(5 * time.Second):
	}
	if !acknowledged {
		// Keep the original failure, but still exercise the next boot. A fenced
		// account must not gain a permissive restart just to make this test green.
		peer.mu.Lock()
		connections, disconnected := peer.connections, peer.disconnected
		peer.mu.Unlock()
		t.Errorf("service mock first inbound reply was not durably acknowledged; connections=%d disconnected=%d", connections, disconnected)
		t.Logf("serve output after first inbound reply:\n%s", process.outputString())
	} else {
		select {
		case reply := <-peer.sent:
			if reply.To.ToNonAD() != customer && reply.To.ToNonAD() != customerLID || reply.Body.GetConversation() != incomingText || reply.ID == "" {
				t.Error("service mock business reply changed its account, customer or text")
			}
		case <-time.After(5 * time.Second):
			t.Error("service mock inbound reply did not reach the authored customer flow")
		}
	}
	if code := process.stop(); code != 0 {
		t.Errorf("service mock first serve shutdown exit=%d", code)
	}
	if !acknowledged {
		pending := readOfflinePairedWhatsAppCaptures(t, opts, registered)
		t.Logf("first reply capture retained=%d original_run=%t request_absent=%t", len(pending), len(pending) == 1 && pending[0].Event.Scope.PublicationBinding.RunID == runID,
			len(pending) == 1 && pending[0].Request == nil)
	}
	process = startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	peer.awaitPostLogin()
	endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString())
	config, err := os.ReadFile(opts.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	writeStandingCandidateFile(t, opts.ConfigPath, string(config)+fmt.Sprintf("\nconnection:\n  api_server: %q\n", endpoint))
	var listing struct {
		Accounts []channelonboarding.Result `json:"accounts"`
	}
	var out, errOut bytes.Buffer
	if code := cliapp.Execute(t.Context(), []string{"--config", opts.ConfigPath, "channel", "list", "--json"}, &out, &errOut, nil, nil, OpenChannelRegistration); code != 0 || json.Unmarshal(out.Bytes(), &listing) != nil {
		t.Errorf("service mock restart account readback exit=%d err=%s", code, errOut.String())
	}
	if len(listing.Accounts) != 1 || listing.Accounts[0].Operation.OperationID != registered.OperationID ||
		listing.Accounts[0].Operation.AccountReference != registered.AccountReference || !listing.Accounts[0].Operation.AccountConfirmed {
		t.Error("service mock restart replaced or lost the approved deployment account")
	}
	// An existing verifier fault leaves a genuine capture with no staged request,
	// the field failure's durable shape. The first subsequent boot must drain it.
	process.mu.Lock()
	rt := process.runtime
	process.mu.Unlock()
	rt.Bus.SetProviderOutputAuthorizationVerifier(&frozenIngressMutationVerifier{
		verify: rt.Options.ProviderTriggerCatalog.VerifyProviderOutputAuthorization,
		mutate: func() error { return errors.New("service mock interruption before publication staging") },
	})
	const retainedID = "OFFLINE_PAIRED_RETAINED_BUSINESS_REPLY"
	const retainedText = "A reply retained before publication staging"
	peer.sendEncryptedFrame(sender.EncryptReply(t, retainedText, retainedID, sent.ID, outgoing))
	deadline := time.Now().Add(5 * time.Second)
	for {
		peer.mu.Lock()
		fenced := peer.disconnected == peer.connections
		peer.mu.Unlock()
		if fenced {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("service mock interruption did not fence the original occurrence")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if code := process.stop(); code != 0 {
		t.Errorf("service mock interrupted serve shutdown exit=%d", code)
	}
	pending := readOfflinePairedWhatsAppCaptures(t, opts, registered)
	if len(pending) != 1 || pending[0].Request != nil || pending[0].Event.EventID != retainedID || pending[0].Event.Scope.PublicationBinding.RunID != runID {
		t.Fatal("service mock interruption did not retain the exact original reply without a publication request")
	}
	failure := sessioncapture.CallbackFailure{ConnectionID: registered.SessionConnectionID, OccurrenceID: pending[0].Event.OccurrenceID, Reason: "capture_failed"}
	readOfflinePairedWhatsAppCaptures(t, opts, registered, failure)
	// A completed Noise handshake is not a logged-in account. Hold only the
	// service's login response; never alter the client's fence or admission.
	login := &servedNativeLoginGate{held: make(chan struct{}), release: make(chan bool, 1)}
	t.Cleanup(func() {
		select {
		case login.release <- false:
		default:
		}
	})
	peer.mu.Lock()
	peer.nextLogin = login
	peer.mu.Unlock()
	process = startServeRuntimeTestProcess(t, opts)
	select {
	case <-login.held:
	case <-time.After(5 * time.Second):
		t.Fatal("post-fence boot did not reach the genuine SDK login response")
	}
	code, refused := process.waitForExit(5 * time.Second)
	login.release <- !refused
	if refused {
		output := process.outputString()
		if code != 1 || !strings.Contains(output, "serve failed \u00b7 channel onboarding \u00b7 WhatsApp client occurrence is fenced") || serveOutputIsReady(output) {
			t.Fatalf("post-fence boot failed at a different seat: exit=%d\n%s", code, output)
		}
		t.Errorf("FIRST post-fence boot refused a new login-pending occurrence: exit=%d\n%s", code, output)
		if rows := readOfflinePairedWhatsAppCaptures(t, opts, registered, failure); len(rows) != 1 || rows[0].Event.EventID != retainedID || rows[0].Request != nil {
			t.Fatal("refused boot changed the retained reply or historical failure evidence")
		}
		process = startServeRuntimeTestProcess(t, opts)
	}
	process.waitForReadyLine()
	peer.awaitPostLogin()
	t.Logf("post-fence boot outcome: first_refused=%t current_ready=true", refused)
	select {
	case reply := <-peer.sent:
		if reply.To.ToNonAD() != customer && reply.To.ToNonAD() != customerLID || reply.Body.GetConversation() != retainedText || reply.ID == "" {
			t.Error("first fixed boot changed the retained customer reply")
		}
	case <-time.After(5 * time.Second):
		t.Error("first fixed boot did not drain the retained reply into the holding flow")
	}
	if code := process.stop(); code != 0 {
		t.Errorf("service mock restarted serve shutdown exit=%d", code)
	}
	if pending := readOfflinePairedWhatsAppCaptures(t, opts, registered, failure); len(pending) != 0 {
		t.Fatal("first fixed boot left the acknowledged reply in private capture")
	}
}

func readOfflinePairedWhatsAppCaptures(t *testing.T, opts cliapp.ServeOptions, registered channelonboarding.Operation, failures ...sessioncapture.CallbackFailure) []sessioncapture.PendingCapture {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(opts.SwarmDir, "*", "provider.db"))
	if err != nil || len(paths) != 1 {
		t.Fatal("service mock has no exact private account database", err)
	}
	fixture, owner, err := sessionstate.OpenSDKFixture(t.Context(), paths[0])
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close()
	for _, failure := range failures {
		count, err := fixture.CallbackFailureCount(t.Context(), failure.ConnectionID, failure.OccurrenceID, failure.Reason)
		if err != nil || count != 1 {
			t.Fatalf("historical callback failure changed: count=%d err=%v", count, err)
		}
	}
	spool, err := owner.Captures(t.Context(), filepath.Base(filepath.Dir(paths[0])))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := spool.PendingPublications(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Event.Scope.OnboardingOperation != registered.OperationID || !row.Event.Scope.RegisteredBusiness {
			t.Fatal("service mock capture belongs to another registration")
		}
	}
	return rows
}
