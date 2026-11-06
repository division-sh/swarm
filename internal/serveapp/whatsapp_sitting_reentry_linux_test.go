package serveapp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type sittingReentryObservation struct {
	operation channelonboarding.Operation
	view      channelonboarding.SessionConnectionReadback
}

type sittingReentryObserver struct {
	cliapp.ChannelRegistrationSession
	views chan sittingReentryObservation
}

func (s sittingReentryObserver) ReadSessionConnection(ctx context.Context, op channelonboarding.Operation) (channelonboarding.SessionConnectionReadback, error) {
	view, err := s.ChannelRegistrationSession.ReadSessionConnection(ctx, op)
	if err == nil {
		select {
		case s.views <- sittingReentryObservation{op, view}:
		default:
		}
	}
	return view, err
}

// Extend the same signed pairing and cancelled PTY invocation, not a paired-state
// fixture. Only the external peer's authenticated login response is held.
func requireSittingPreviewReentry(t *testing.T, ctx context.Context, config, directory, backend string, terminal *os.File, peer *serveNativeProtocolPeer, retained sittingCancellationState) {
	t.Helper()
	login := &servedNativeLoginGate{held: make(chan struct{}), release: make(chan bool, 1)}
	defer func() {
		select {
		case login.release <- false:
		default:
		}
	}()
	peer.mu.Lock()
	peer.nextLogin = login
	peer.mu.Unlock()
	completed := make(chan string, 1)
	previous := waLog.Noop
	waLog.Noop = sittingCallbackLogger{Logger: previous, ctx: ctx, completed: completed}
	defer func() { waLog.Noop = previous }()
	views := make(chan sittingReentryObservation, 16)
	var native *deploymentChannelSession
	var operations channelonboarding.OperationStore
	factory := func(call context.Context, options cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
		session, err := OpenChannelRegistration(call, options)
		if err != nil {
			return nil, err
		}
		native, operations = session.(*deploymentChannelSession), options.Operations
		return sittingReentryObserver{session, views}, nil
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
	command, cancel := context.WithCancel(ctx)
	done := make(chan int, 1)
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-done
		}
	}()
	go func() {
		done <- cliapp.Execute(command, []string{"--config", config, "channel", "connect", "whatsapp", retained.operation.AccountReference}, writer, sittingPromptWriter{Writer: errOut, prompts: prompts}, nil, nil, factory)
	}()
	select {
	case <-login.held:
	case code := <-done:
		joined = true
		t.Fatalf("retained connect exited before restoring login: exit=%d err=%s", code, errOut.String())
	case <-ctx.Done():
		t.Fatalf("retained connect did not restore its signed SDK login: out=%s err=%s", out.String(), errOut.String())
	}
	for {
		select {
		case observed := <-views:
			if observed.view.State != "connecting" {
				continue
			}
			if !reflect.DeepEqual(observed.operation, retained.operation) || !observed.view.Linked {
				t.Fatal("login-pending preview changed the retained operation/account")
			}
		case prompt := <-prompts:
			t.Fatalf("CLI prompted before the authenticated login response: %s", prompt)
		case code := <-done:
			joined = true
			t.Fatalf("CLI refused login-pending re-entry: exit=%d err=%s", code, errOut.String())
		case <-ctx.Done():
			t.Fatal("retained connection never reported honest login progress")
		}
		break
	}
	login.release <- true
	select {
	case prompt := <-prompts:
		if !strings.HasPrefix(prompt, "Is this the right account?") {
			t.Fatal("re-entry skipped the explicit account question")
		}
	case code := <-done:
		joined = true
		t.Fatalf("retained connect exited before account preview: exit=%d err=%s", code, errOut.String())
	case <-ctx.Done():
		t.Fatal("restored login never reached its unapproved account preview")
	}
	select {
	case <-writer.qrRendered:
		t.Fatal("retained pairing requested another QR")
	default:
	}
	before, err := operations.GetChannelOnboarding(ctx, retained.operation.OperationID)
	if err != nil || !reflect.DeepEqual(before, retained.operation) || before.AccountConfirmed {
		t.Fatal("restoration implicitly approved or replaced the retained operation", err)
	}
	if _, err := native.connection.AdmitSessionAccount(ctx, before.SessionAccount); err == nil {
		t.Fatal("restored unapproved preview acquired native authority")
	}
	sender, _, _ := peer.senderFrom(types.NewJID("15551234590", types.DefaultUserServer), types.NewJID("100000000090", types.HiddenUserServer))
	const id = "SITTING_PENDING_restored_preview"
	peer.sendEncryptedFrame(sender.EncryptText(t, "ordinary input at restored preview", id))
	select {
	case observed := <-completed:
		if observed != id {
			t.Fatal("pending callback completion belonged to another message")
		}
	case <-ctx.Done():
		t.Fatal("restored pending callback did not complete")
	}
	after, err := operations.GetChannelOnboarding(ctx, before.OperationID)
	if err != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(retained.captures, sittingCaptureRows(t, ctx, directory, before.SessionConnectionID)) {
		t.Fatal("restored pending input mutated the operation or captures", err)
	}
	if reusable, err := native.connection.CheckSessionReuse(ctx, before); err != nil || !reusable {
		t.Fatal("restored pending input fenced its exact connection", err)
	}
	select {
	case receipt := <-peer.receipts:
		t.Fatalf("restored pending input was acknowledged: %v", receipt.Attrs["id"])
	default:
	}
	if _, err := terminal.WriteString("y\n"); err != nil {
		t.Fatal(err)
	}
	var text string
	select {
	case text = <-challenges:
	case <-ctx.Done():
		t.Fatal("explicit account approval did not reach operator enrollment")
	}
	challenge, _, ok := strings.Cut(strings.TrimPrefix(text, "Send the code "), " from your operator phone")
	if !ok || challenge == "" {
		t.Fatal("restored enrollment has no exact challenge")
	}
	peer.claim(challenge)
	select {
	case <-prompts:
	case <-ctx.Done():
		t.Fatal("restored enrollment has no claimant confirmation")
	}
	if _, err := terminal.WriteString("y\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-done:
		joined = true
		if code != 0 {
			t.Fatalf("restored public enrollment failed: exit=%d err=%s", code, errOut.String())
		}
	case <-ctx.Done():
		t.Fatal("restored public enrollment did not join")
	}
	var listing struct {
		Accounts []channelonboarding.Result `json:"accounts"`
	}
	listed := &lockedBuffer{}
	listedViews := make(chan sittingReentryObservation, 8)
	listFactory := func(call context.Context, options cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
		session, err := OpenChannelRegistration(call, options)
		return sittingReentryObserver{session, listedViews}, err
	}
	if code := cliapp.Execute(ctx, []string{"--config", config, "channel", "list", "--json"}, listed, errOut, nil, nil, listFactory); code != 0 {
		t.Fatal("restored public account readback failed", code, errOut.String())
	}
	if json.Unmarshal([]byte(listed.String()), &listing) != nil || len(listing.Accounts) != 1 {
		t.Fatal("restored account list did not return exactly one registration")
	}
	registered := listing.Accounts[0]
	if registered.Operation.OperationID != before.OperationID || registered.Operation.AccountReference != before.AccountReference || !registered.Operation.AccountConfirmed || registered.Operation.Phase != channelonboarding.PhaseSucceeded || registered.Binding == nil || registered.Binding.ExternalAccountRef != "15551234568@s.whatsapp.net" {
		t.Fatal("restored approval lost its retained account or exact operator")
	}
	select {
	case observed := <-listedViews:
		if observed.operation.SessionAccount != before.SessionAccount || observed.operation.SessionConnectionID != before.SessionConnectionID {
			t.Fatal("restored registration replaced its private pairing provenance")
		}
	default:
		t.Fatal("public account listing bypassed the original native readback")
	}
	select {
	case <-peer.logouts:
		t.Fatal("restored connect logged out the retained pairing")
	default:
	}
	opts := cliapp.DefaultServeOptions()
	opts.ConfigPath, opts.SourceRoot = config, filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session-reply")
	opts.SwarmDir, opts.SwarmDirSet, opts.StoreMode, opts.StoreModeSet = directory, true, backend, true
	opts.WorkspaceBackend, opts.WorkspaceBackendSet = "host", true
	opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
	t.Logf("restored same-account public journey completed on %s", backend)
	requireOfflinePairedWhatsAppServiceRoundTrip(t, opts, peer, registered.Operation)
}
