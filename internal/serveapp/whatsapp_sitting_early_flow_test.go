//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/store/sessionstate"
	"github.com/division-sh/swarm/tests/support/testutil"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type sittingCallbackLogger struct {
	waLog.Logger
	ctx       context.Context
	completed chan string
}

func (l sittingCallbackLogger) Warnf(format string, args ...any) {
	if format == "Handler for %s failed" && len(args) == 1 {
		if id := fmt.Sprint(args[0]); strings.HasPrefix(id, "SITTING_PENDING_") {
			select {
			case l.completed <- id:
			case <-l.ctx.Done():
			}
		}
	}
	l.Logger.Warnf(format, args...)
}

type sittingWait struct {
	stage, text string
	op          channelonboarding.Operation
	resume      chan struct{}
}

type sittingCLIWriter struct {
	io.Writer
	qr         sync.Once
	qrRendered chan struct{}
	pause      func(string, string) error
}

func (w *sittingCLIWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if err != nil {
		return n, err
	}
	text := string(p)
	if strings.Contains(text, "Open WhatsApp > Linked Devices") {
		w.qr.Do(func() { close(w.qrRendered) })
	}
	for _, stage := range []struct{ prefix, name string }{{"Linked to ", "preview"}, {"Send the code ", "challenge"}, {"Code received from ", "confirmation"}} {
		if strings.HasPrefix(text, stage.prefix) {
			return n, w.pause(stage.name, text)
		}
	}
	return n, nil
}

// The external Noise/WhatsApp service is a peer double. No persisted paired
// device shortcut is used; approval is scripted with --yes, not interactive TTY.
func TestWhatsAppPublicSittingEarlyFlowBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			peer := newServeNativeProtocolPeer(t)
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			completed := make(chan string, 4)
			previous := waLog.Noop
			waLog.Noop = sittingCallbackLogger{Logger: previous, ctx: ctx, completed: completed}
			t.Cleanup(func() { waLog.Noop = previous })
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
				opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, filepath.Join(t.TempDir(), "sitting.sqlite"), channelOnboardingHostWorkspaceFields())
			} else {
				opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
			}
			config, err := os.ReadFile(opts.ConfigPath)
			if err != nil {
				t.Fatal(err)
			}
			writeStandingCandidateFile(t, opts.ConfigPath, string(config)+fmt.Sprintf("\npaths:\n  swarm_dir: %q\nchannels:\n  hitl:\n    provider: whatsapp\n    account: '+15551234567'\n", opts.SwarmDir))
			waits := make(chan sittingWait, 1)
			qr := make(chan channelonboarding.Result, 1)
			var native *deploymentChannelSession
			var operations channelonboarding.OperationStore
			var operationID string
			pause := func(stage, text string) error {
				op, err := operations.GetChannelOnboarding(ctx, operationID)
				if err != nil {
					return err
				}
				wait := sittingWait{stage: stage, text: text, op: op, resume: make(chan struct{})}
				select {
				case waits <- wait:
				case <-ctx.Done():
					return ctx.Err()
				}
				select {
				case <-wait.resume:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			factory := func(call context.Context, options cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
				session, err := OpenChannelRegistration(call, options)
				if err != nil {
					return nil, err
				}
				native, operations = session.(*deploymentChannelSession), options.Operations
				return firstConnectPairingObserver{ChannelRegistrationSession: session, principal: options.Principal,
					observe: func(result channelonboarding.Result, _ bool) {
						if view := result.Pairing; view != nil && view.Code != "" {
							select {
							case qr <- result:
							default:
							}
						}
					},
					afterRead: func(_ context.Context, op channelonboarding.Operation, view channelonboarding.PairingReadback, readErr error) {
						operationID = op.OperationID
					}}, nil
			}
			out, errOut := &lockedBuffer{}, &lockedBuffer{}
			writer := &sittingCLIWriter{Writer: out, qrRendered: make(chan struct{}), pause: pause}
			done := make(chan int, 1)
			joined := false
			defer func() {
				cancel()
				if !joined {
					<-done
				}
			}()
			go func() {
				done <- cliapp.Execute(ctx, []string{"--config", opts.ConfigPath, "channel", "connect", "whatsapp", "--yes"}, writer, errOut, nil, nil, factory)
			}()
			var pairing channelonboarding.Result
			select {
			case pairing = <-qr:
			case <-ctx.Done():
				t.Fatal("public CLI did not offer its QR")
			}
			select {
			case <-writer.qrRendered:
			case <-ctx.Done():
				t.Fatal("public CLI did not render its QR")
			}
			if !strings.Contains(out.String(), "██") || pairing.Pairing.Paired || pairing.Operation.AccountConfirmed {
				t.Fatal("QR output did not precede pairing and approval")
			}
			peer.pair(pairing.Pairing.Code)
			sender, _, _ := peer.senderFrom(types.NewJID("15551234590", types.DefaultUserServer), types.NewJID("100000000090", types.HiddenUserServer))
			for _, stage := range []string{"preview", "challenge", "confirmation"} {
				var wait sittingWait
				select {
				case wait = <-waits:
				case code := <-done:
					joined = true
					t.Fatalf("connect exited before %s: exit=%d err=%s", stage, code, errOut.String())
				case <-ctx.Done():
					t.Fatalf("public CLI never reached %s", stage)
				}
				if wait.stage != stage || wait.op.OperationID != pairing.Operation.OperationID || wait.op.AccountConfirmed != (stage != "preview") {
					t.Fatalf("wrong public wait point: %s", wait.stage)
				}
				before := sittingCaptureRows(t, ctx, opts.SwarmDir, wait.op.SessionConnectionID)
				id := "SITTING_PENDING_" + stage
				text := "ordinary traffic during " + stage
				if stage == "challenge" {
					text = "15000"
					if strings.HasPrefix(wait.text, "Send the code 15000 ") {
						text = "15001"
					}
				}
				peer.sendEncryptedFrame(sender.EncryptText(t, text, id))
				select {
				case observed := <-completed:
					if observed != id {
						t.Fatal("callback completed for another message")
					}
				case <-ctx.Done():
					t.Fatal("pending callback did not complete", stage)
				}
				if reusable, err := native.connection.CheckSessionReuse(ctx, wait.op); err != nil || !reusable {
					t.Fatalf("%s callback fatally fenced connection: reusable=%t err=%v", stage, reusable, err)
				}
				after, err := operations.GetChannelOnboarding(ctx, wait.op.OperationID)
				if err != nil || !reflect.DeepEqual(after, wait.op) {
					t.Fatal("pending callback changed its operation", stage, err)
				}
				if !reflect.DeepEqual(before, sittingCaptureRows(t, ctx, opts.SwarmDir, wait.op.SessionConnectionID)) {
					t.Fatal("pending callback wrote captured input", stage)
				}
				select {
				case receipt := <-peer.receipts:
					t.Fatalf("pending callback acknowledged input: stage=%s id=%v", stage, receipt.Attrs["id"])
				default:
				}
				if stage == "preview" {
					if _, err := native.connection.AdmitSessionAccount(ctx, wait.op.SessionAccount); err == nil {
						t.Fatal("unapproved account gained authority")
					}
				}
				close(wait.resume)
				if stage == "challenge" {
					challenge, _, found := strings.Cut(strings.TrimPrefix(wait.text, "Send the code "), " from your operator phone")
					if !found || challenge == "" {
						t.Fatal("public CLI challenge is missing")
					}
					deadline := time.Now().Add(2 * time.Second)
					for !strings.Contains(errOut.String(), "error: bad code\n") && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
					if strings.Count(errOut.String(), "error: bad code\n") != 1 {
						t.Fatal("local public connect did not print the nonfatal numeric miss", errOut.String())
					}
					peer.claim(challenge)
				}
			}
			select {
			case code := <-done:
				joined = true
				if code != 0 {
					t.Fatalf("public enrollment failed: exit=%d err=%s", code, errOut.String())
				}
			case <-ctx.Done():
				t.Fatal("public enrollment did not join")
			}
			var listing struct {
				Accounts []channelonboarding.Result `json:"accounts"`
			}
			var listed, listErr bytes.Buffer
			if code := cliapp.Execute(ctx, []string{"--config", opts.ConfigPath, "channel", "list", "--json"}, &listed, &listErr, nil, nil, OpenChannelRegistration); code != 0 || json.Unmarshal(listed.Bytes(), &listing) != nil || len(listing.Accounts) != 1 {
				t.Fatal("public account readback failed", code, listErr.String())
			}
			registered := listing.Accounts[0]
			if registered.Operation.OperationID != pairing.Operation.OperationID || registered.Operation.Phase != channelonboarding.PhaseSucceeded || !registered.Operation.AccountConfirmed || registered.Operation.IdentityOperationID == "" || registered.Binding == nil || registered.Binding.ExternalAccountRef != "15551234568@s.whatsapp.net" {
				t.Fatal("public enrollment lost the original account or exact operator")
			}
			select {
			case sent := <-peer.sent:
				if sent.To.ToNonAD().String() != registered.Binding.ConversationRef || sent.Body.GetConversation() != "Swarm operator successfully connected" {
					t.Fatalf("registered operator acknowledgement mismatch: to=%s body=%v", sent.To, sent.Body)
				}
			case <-ctx.Done():
				t.Fatal("public enrollment succeeded without the operator acknowledgement")
			}
			requireOfflinePairedWhatsAppServiceRoundTrip(t, opts, peer, registered.Operation)
		})
	}
}

func sittingCaptureRows(t *testing.T, ctx context.Context, directory, connectionID string) [][]byte {
	t.Helper()
	fixture, _, err := sessionstate.OpenSDKFixture(ctx, filepath.Join(directory, connectionID, "provider.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	}()
	rows, err := fixture.CaptureRawRows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}
