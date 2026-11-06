package serveapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/store/sessionstate"
	"github.com/division-sh/swarm/tests/support/testutil"
	"go.mau.fi/whatsmeow/types"
	"golang.org/x/sys/unix"
)

type sittingPromptWriter struct {
	io.Writer
	prompts chan string
}

func (w sittingPromptWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if err == nil && (strings.HasPrefix(string(p), "Is this the right account?") || strings.HasPrefix(string(p), "Make them the operator?")) {
		w.prompts <- string(p)
	}
	return n, err
}

type sittingCloseObserver struct {
	cliapp.ChannelRegistrationSession
	observe func(context.Context) error
}

func (s sittingCloseObserver) Close(ctx context.Context) error {
	return errors.Join(s.ChannelRegistrationSession.Close(ctx), s.observe(ctx))
}

type sittingCancellationState struct {
	operation channelonboarding.Operation
	claims    []operatorchannel.Operation
	bindings  []operatorchannel.Binding
	captures  [][]byte
	account   string
	identity  [32]byte
}

func readSittingCancellationState(ctx context.Context, options cliapp.ChannelRegistrationOptions, operationID, account string) (state sittingCancellationState, err error) {
	state.operation, err = options.Operations.GetChannelOnboarding(ctx, operationID)
	if err != nil {
		return state, err
	}
	state.claims, err = options.Identities.ListOperatorChannelOperations(ctx, options.Principal.ID)
	if err != nil {
		return state, err
	}
	state.bindings, err = options.Identities.ListOperatorChannelBindings(ctx, options.Principal.ID)
	if err != nil {
		return state, err
	}
	fixture, owner, err := sessionstate.OpenSDKFixture(ctx, filepath.Join(options.Directory, state.operation.SessionConnectionID, "provider.db"))
	if err != nil {
		return state, err
	}
	defer func() { err = errors.Join(err, fixture.Close()) }()
	state.captures, err = fixture.CaptureRawRows(ctx)
	if err != nil {
		return state, err
	}
	device, err := owner.CurrentDevice(ctx, account)
	if err != nil {
		return state, err
	}
	state.account, state.identity = device.ID.String(), *device.IdentityKey.Pub
	return state, nil
}

func sittingTerminal(t *testing.T) *os.File {
	t.Helper()
	fd, err := unix.Open("/dev/ptmx", unix.O_RDWR|unix.O_NOCTTY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	master := os.NewFile(uintptr(fd), "sitting-terminal")
	t.Cleanup(func() { _ = master.Close() })
	if err := unix.IoctlSetPointerInt(fd, unix.TIOCSPTLCK, 0); err != nil {
		t.Fatal(err)
	}
	index, err := unix.IoctlGetInt(fd, unix.TIOCGPTN)
	if err != nil {
		t.Fatal(err)
	}
	slave, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", index), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	previous := os.Stdin
	os.Stdin = slave
	t.Cleanup(func() { os.Stdin = previous; _ = slave.Close() })
	return master
}

// This exercises terminal input and the real SDK against a synthetic peer, not
// a physical device. Cancellation must join without sending an answer or unlink.
type sittingClaimReadback struct {
	operation channelonboarding.Operation
	view      channelonboarding.SessionConnectionReadback
	err       error
}

type sittingClaimReadbackObserver struct {
	cliapp.ChannelRegistrationSession
	read chan sittingClaimReadback
}

func (s sittingClaimReadbackObserver) ReadSessionConnection(ctx context.Context, op channelonboarding.Operation) (channelonboarding.SessionConnectionReadback, error) {
	view, err := s.ChannelRegistrationSession.ReadSessionConnection(ctx, op)
	select {
	case s.read <- sittingClaimReadback{op, view, err}:
	case <-ctx.Done():
	}
	return view, err
}

func TestWhatsAppPublicSittingPromptCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, stage := range []string{"account", "claim_wait", "restored_login_cancel", "restored_login_disconnect", "claim_disconnect_plain", "claim_disconnect_miss"} {
			t.Run(backend+"/"+stage, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
				terminal := sittingTerminal(t)
				peer := newServeNativeProtocolPeer(t)
				ctx, stop := context.WithTimeout(t.Context(), 20*time.Second)
				defer stop()
				command, cancel := context.WithCancel(ctx)
				defer cancel()
				directory := t.TempDir()
				if err := os.Chmod(directory, 0o700); err != nil {
					t.Fatal(err)
				}
				var config string
				if backend == "sqlite" {
					config = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, filepath.Join(t.TempDir(), "sitting.sqlite"), channelOnboardingHostWorkspaceFields())
				} else {
					config = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
				}
				content, err := os.ReadFile(config)
				if err != nil {
					t.Fatal(err)
				}
				writeStandingCandidateFile(t, config, string(content)+fmt.Sprintf("\npaths:\n  swarm_dir: %q\nchannels:\n  hitl:\n    provider: whatsapp\n    account: '+15551234567'\n", directory))
				qr := make(chan channelonboarding.Result, 1)
				readbacks := make(chan sittingClaimReadback, 16)
				var registration cliapp.ChannelRegistrationOptions
				var operationID string
				snapshot := func(read context.Context) (sittingCancellationState, error) {
					return readSittingCancellationState(read, registration, operationID, peer.account.ToNonAD().String())
				}
				var after sittingCancellationState
				factory := func(call context.Context, options cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
					session, err := OpenChannelRegistration(call, options)
					if err != nil {
						return nil, err
					}
					registration = options
					var reader cliapp.ChannelRegistrationSession = session
					if strings.HasPrefix(stage, "claim_disconnect_") {
						reader = sittingClaimReadbackObserver{session, readbacks}
					}
					observed := firstConnectPairingObserver{ChannelRegistrationSession: reader, principal: options.Principal,
						observe: func(result channelonboarding.Result, _ bool) {
							if result.Pairing != nil && result.Pairing.Code != "" {
								select {
								case qr <- result:
								default:
								}
							}
						}, afterRead: func(_ context.Context, op channelonboarding.Operation, _ channelonboarding.PairingReadback, _ error) {
							operationID = op.OperationID
						}}
					return sittingCloseObserver{ChannelRegistrationSession: observed, observe: func(read context.Context) error {
						var err error
						after, err = snapshot(read)
						return err
					}}, nil
				}
				out, errOut := &lockedBuffer{}, &lockedBuffer{}
				prompts := make(chan string, 2)
				challenges := make(chan string, 1)
				writer := &sittingCLIWriter{Writer: out, qrRendered: make(chan struct{}), pause: func(stage, text string) error {
					if stage == "challenge" {
						challenges <- text
					}
					return nil
				}}
				done := make(chan int, 1)
				joined := false
				defer func() {
					cancel()
					if !joined {
						// Release the old blocking implementation on the RED control.
						_, _ = terminal.WriteString("y\n")
						<-done
					}
				}()
				go func() {
					done <- cliapp.Execute(command, []string{"--config", config, "channel", "connect", "whatsapp"}, writer, sittingPromptWriter{Writer: errOut, prompts: prompts}, nil, nil, factory)
				}()
				var pairedOperation channelonboarding.Operation
				select {
				case pairing := <-qr:
					pairedOperation = pairing.Operation
					peer.pair(pairing.Pairing.Code)
				case code := <-done:
					joined = true
					t.Fatalf("connect exited before synthetic pairing: exit=%d err=%s", code, errOut.String())
				case <-ctx.Done():
					t.Fatalf("connect did not reach synthetic pairing: %s", errOut.String())
				}
				select {
				case <-prompts:
				case <-ctx.Done():
					t.Fatal("account prompt missing")
				}
				var challenge string
				claimWait := strings.HasPrefix(stage, "claim_")
				if claimWait {
					if _, err := terminal.WriteString("y\n"); err != nil {
						t.Fatal(err)
					}
					var text string
					select {
					case text = <-challenges:
					case <-ctx.Done():
						t.Fatal("operator challenge missing")
					}
					parsed, _, ok := strings.Cut(strings.TrimPrefix(text, "Send the code "), " from your operator phone")
					if !ok || len(parsed) != 5 || strings.Trim(parsed, "0123456789") != "" {
						t.Fatal("operator challenge malformed")
					}
					challenge = parsed
				}
				before, err := snapshot(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if !claimWait && (before.operation.OperationID != pairedOperation.OperationID || before.operation.SessionConnectionID != pairedOperation.SessionConnectionID ||
					before.operation.AccountConfirmed || before.operation.AccountReference != "+15551234567" ||
					before.operation.SessionAccount.Validate() != nil || before.operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity) {
					t.Fatal("first preview lacks the original canonical unapproved account checkpoint")
				}
				if strings.HasPrefix(stage, "claim_disconnect_") {
					if stage == "claim_disconnect_miss" {
						miss := "15000"
						if challenge == miss {
							miss = "15001"
						}
						sender, _, _ := peer.senderFrom(types.NewJID("15551234590", types.DefaultUserServer), types.NewJID("100000000090", types.HiddenUserServer))
						peer.sendEncryptedFrame(sender.EncryptText(t, miss, "CLAIM_DISCONNECT_MISS"))
						for !strings.Contains(errOut.String(), "error: bad code\n") && ctx.Err() == nil {
							time.Sleep(time.Millisecond)
						}
						if ctx.Err() != nil {
							t.Fatal("public connect did not observe the miss before disconnect")
						}
					}
					peer.disconnect()
				readDisconnected:
					for {
						select {
						case observed := <-readbacks:
							if observed.err != nil {
								t.Fatal("optional notice made public readback fail after disconnect", observed.err)
							}
							if observed.view.State != "disconnected" {
								continue
							}
							if observed.view.Reason != "network unreachable" || !observed.view.Linked || observed.view.ClaimCodeNotice != nil ||
								observed.operation.OperationID != before.operation.OperationID || observed.operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity {
								t.Fatal("public disconnected state/reason changed with optional notice", observed)
							}
							break readDisconnected
						case code := <-done:
							joined = true
							t.Fatal("public wait failed rather than observing the disconnected account", code, errOut.String())
						case <-ctx.Done():
							t.Fatal("public disconnected readback was not observed")
						}
					}
				}
				cancel()
				select {
				case code := <-done:
					joined = true
					if code == 0 || !strings.Contains(errOut.String(), "context canceled") {
						t.Fatalf("cancellation did not fail the prompt: exit=%d err=%s", code, errOut.String())
					}
				case <-time.After(2 * time.Second):
					t.Fatal("cancelled interactive prompt stranded the CLI input read")
				}
				if !reflect.DeepEqual(before, after) {
					t.Fatal("cancel changed account, operation, claims, bindings, captures or pairing")
				}
				select {
				case <-peer.logouts:
					t.Fatal("cancel logged out the paired account")
				default:
				}
				if !claimWait {
					requireSittingPreviewReentry(t, ctx, config, directory, backend, terminal, peer, after, stage)
				}
			})
		}
	}
}
