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
func TestWhatsAppPublicSittingPromptCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, stage := range []string{"account", "claimant"} {
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
				var operations channelonboarding.OperationStore
				var identities operatorchannel.Store
				var principal operatorchannel.Principal
				var operationID string
				snapshot := func(read context.Context) (state sittingCancellationState, err error) {
					state.operation, err = operations.GetChannelOnboarding(read, operationID)
					if err != nil {
						return state, err
					}
					state.claims, err = identities.ListOperatorChannelOperations(read, principal.ID)
					if err != nil {
						return state, err
					}
					state.bindings, err = identities.ListOperatorChannelBindings(read, principal.ID)
					if err != nil {
						return state, err
					}
					fixture, owner, err := sessionstate.OpenSDKFixture(read, filepath.Join(directory, state.operation.SessionConnectionID, "provider.db"))
					if err != nil {
						return state, err
					}
					defer func() { err = errors.Join(err, fixture.Close()) }()
					state.captures, err = fixture.CaptureRawRows(read)
					if err != nil {
						return state, err
					}
					device, err := owner.CurrentDevice(read, peer.account.ToNonAD().String())
					if err != nil {
						return state, err
					}
					state.account, state.identity = device.ID.String(), *device.IdentityKey.Pub
					return state, nil
				}
				var after sittingCancellationState
				factory := func(call context.Context, options cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
					session, err := OpenChannelRegistration(call, options)
					if err != nil {
						return nil, err
					}
					operations, identities, principal = options.Operations, options.Identities, options.Principal
					observed := firstConnectPairingObserver{ChannelRegistrationSession: session, principal: principal,
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
				select {
				case pairing := <-qr:
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
				if stage == "claimant" {
					if _, err := terminal.WriteString("y\n"); err != nil {
						t.Fatal(err)
					}
					var text string
					select {
					case text = <-challenges:
					case <-ctx.Done():
						t.Fatal("operator challenge missing")
					}
					challenge, _, ok := strings.Cut(strings.TrimPrefix(text, "Send the code "), " from your operator phone")
					if !ok || challenge == "" {
						t.Fatal("operator challenge malformed")
					}
					peer.claim(challenge)
					select {
					case <-prompts:
					case <-ctx.Done():
						t.Fatal("claimant prompt missing")
					}
				}
				before, err := snapshot(ctx)
				if err != nil {
					t.Fatal(err)
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
				if stage == "account" {
					requireSittingPreviewReentry(t, ctx, config, directory, backend, terminal, peer, after)
				}
			})
		}
	}
}
