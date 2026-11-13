//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimechanneldelivery "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/tests/support/testutil"
	"github.com/google/uuid"
)

type diagnosticReadFixture struct {
	serveSessionBootstrapOwner
	items       []serveSessionDiagnostic
	recoveryAt  time.Time
	recoveryErr error
}

func (f *diagnosticReadFixture) sessionDiagnostics(context.Context) ([]serveSessionDiagnostic, error) {
	return f.items, nil
}

func (f *diagnosticReadFixture) sessionRecovery(context.Context, channelonboarding.Operation) (time.Time, bool, error) {
	return f.recoveryAt, false, f.recoveryErr
}

// Invariant: all eleven lines are fixed, operator-only; duplicate views print once.
func TestServeSessionDiagnosticFixedOutput(t *testing.T) {
	for _, failedRead := range []bool{false, true} {
		item := serveSessionDiagnostic{Operation: channelonboarding.Operation{Provider: "whatsapp"}, Account: "+15551234567",
			Fact: sessionprovider.SessionDiagnostic{ConnectionID: uuid.NewString(), OccurrenceID: uuid.NewString()}}
		owner := &diagnosticReadFixture{items: []serveSessionDiagnostic{item}, recoveryAt: time.Now()}
		if failedRead {
			owner.recoveryErr = errors.New("boot recovery read refused")
		}
		var out, errOut bytes.Buffer
		poll := serveSessionDiagnostics(owner, &channelonboarding.AccountReference{Provider: "whatsapp", Account: item.Account}, &out, &errOut, false)
		err := poll(t.Context())
		if failedRead {
			if !errors.Is(err, owner.recoveryErr) || out.Len() != 0 {
				t.Fatal("boot fallback suppressed its owner read failure", err, out.String())
			}
		} else if err != nil || out.String() != "ERROR WhatsApp disconnected: cause unknown\n" {
			t.Fatal("boot recovery fallback lost its disconnect form", err, out.String())
		}
	}
	for _, cell := range []struct{ socket, lifecycle, cause string }{
		{"peer_close", "", "closed by the other side"}, {"reset", "", "connection reset"},
		{"unreachable", "", "network path unavailable"}, {"timeout", "", "connection timed out"},
		{"reset", "logged out", "logged out"}, {"reset", "account temporarily banned", "account temporarily banned"},
		{"reset", "session replaced by another connection", "connection replaced"}, {"reset", "connection rejected by WhatsApp", "connection rejected"},
		{"reset", "WhatsApp authentication refresh failed", "authentication refresh failed"}, {"reset", "WhatsApp client needs an update", "client needs an update"},
		{"eof", "", "cause unknown"},
	} {
		for _, selection := range []string{"operator", "other", "none", "learned"} {
			for _, verbose := range []bool{false, true} {
				fact := sessionprovider.SessionDiagnostic{ConnectionID: uuid.NewString(), OccurrenceID: uuid.NewString(), Down: true, Disconnected: true, Socket: cell.socket, Lifecycle: cell.lifecycle, KeepAliveTimeout: true, KeepAliveRestored: true}
				item := serveSessionDiagnostic{Operation: channelonboarding.Operation{Provider: "whatsapp", DeploymentRegistration: selection != "learned"}, Account: "+15551234567", Fact: fact}
				owner := &diagnosticReadFixture{items: []serveSessionDiagnostic{item, item}}
				hitl := &channelonboarding.AccountReference{Provider: "whatsapp", Account: item.Account}
				if selection == "other" {
					hitl.Account = "+15559999999"
				}
				if selection == "none" {
					hitl = nil
				}
				var out, errOut bytes.Buffer
				poll := serveSessionDiagnostics(owner, hitl, &out, &errOut, verbose)
				if poll != nil {
					if err := poll(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
				want := ""
				if selection == "operator" || selection == "learned" {
					want = "ERROR WhatsApp disconnected: " + cell.cause + "\n"
				}
				if out.String() != want || verbose && (strings.Count(errOut.String(), "ERROR whatsapp occurrence=") != 1 || strings.Count(errOut.String(), "OBSERVATION WhatsApp:") != 2) || !verbose && errOut.Len() != 0 {
					t.Fatal("wrong fixed line, role, or sink", selection, out.String(), errOut.String())
				}
				before := out.String() + errOut.String()
				if poll != nil {
					if err := poll(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
				if before != out.String()+errOut.String() {
					t.Fatal("duplicate snapshot printed again")
				}
			}
		}
	}
	for _, cause := range []string{"incoming callback panic", "incoming capture failed", "storage failure", "currentness failure", "runtime release failed", "connection attempt failed", "connection attempt cancelled", "connection must be re-established", "PRIVATE-CANARY"} {
		fact := sessionprovider.SessionDiagnostic{ConnectionID: uuid.NewString(), OccurrenceID: uuid.NewString(), Down: true, Failure: cause}
		item := serveSessionDiagnostic{Operation: channelonboarding.Operation{Provider: "whatsapp"}, Account: "+15551234567", Fact: fact}
		owner := &diagnosticReadFixture{items: []serveSessionDiagnostic{item, item}}
		var out, errOut bytes.Buffer
		poll := serveSessionDiagnostics(owner, &channelonboarding.AccountReference{Provider: "whatsapp", Account: item.Account}, &out, &errOut, false)
		if err := poll(t.Context()); err != nil {
			t.Fatal(err)
		}
		if out.String() != "ERROR WhatsApp: "+fact.Cause()+"\n" || strings.Contains(out.String(), "PRIVATE-CANARY") || errOut.Len() != 0 {
			t.Fatal("non-drop line changed wording or exposed raw evidence", out.String())
		}
		owner.items = nil
		if err := poll(t.Context()); err != nil {
			t.Fatal(err)
		}
		item.Fact = sessionprovider.SessionDiagnostic{ConnectionID: fact.ConnectionID, OccurrenceID: fact.OccurrenceID, Down: true, Intentional: true}
		owner.items = []serveSessionDiagnostic{item}
		if err := poll(t.Context()); err != nil || strings.Count(out.String(), "ERROR WhatsApp:") != 1 {
			t.Fatal("intentional stop invented a failure", err)
		}
	}
}

type diagnosticHeldWriter struct {
	lockedBuffer
	gate             sync.Mutex
	plain            *lockedBuffer
	entered, release chan struct{}
	once             sync.Once
}

func (w *diagnosticHeldWriter) Write(p []byte) (int, error) {
	w.gate.Lock()
	plain := w.plain
	w.gate.Unlock()
	if strings.HasPrefix(string(p), "ERROR whatsapp occurrence=") && plain != nil && strings.Contains(plain.String(), "ERROR WhatsApp disconnected:") {
		w.once.Do(func() { close(w.entered) })
		<-w.release
	}
	return w.lockedBuffer.Write(p)
}

// Invariant: real registered observations do not recover/write; held diagnostic IO cannot hold native cleanup.
func TestServeRegisteredDiagnosticReadAndHeldWriterBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		if !t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			peer := newServeNativeProtocolPeer(t)
			opts := cliapp.DefaultServeOptions()
			opts.SourceRoot = filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session-reply")
			opts.SwarmDir, opts.SwarmDirSet = t.TempDir(), true
			if err := os.Chmod(opts.SwarmDir, 0o700); err != nil {
				t.Fatal(err)
			}
			opts.WorkspaceBackend, opts.WorkspaceBackendSet = "host", true
			opts.StoreMode, opts.StoreModeSet = backend, true
			opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
			if backend == "sqlite" {
				opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, filepath.Join(t.TempDir(), "diagnostic.sqlite"), channelOnboardingHostWorkspaceFields())
			} else {
				opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
			}
			config, err := os.ReadFile(opts.ConfigPath)
			if err != nil {
				t.Fatal(err)
			}
			writeStandingCandidateFile(t, opts.ConfigPath, string(config)+"\npaths:\n  swarm_dir: "+opts.SwarmDir+"\n")
			op := issue3011RegisterForSend(t, opts, peer)
			writeStandingCandidateFile(t, opts.ConfigPath, string(config)+"\npaths:\n  swarm_dir: "+opts.SwarmDir+"\nchannels:\n  hitl:\n    provider: whatsapp\n    account: '+15551234567'\n")
			prior := newServeSessionBootstrap
			owners := make(chan *serveSessionBootstrap, 1)
			newServeSessionBootstrap = func(selectRuntime serveSessionBootstrapRuntime, store channelonboarding.Store, credentials operatorchannel.CredentialCurrentness, directory string, invocation *sessionprovider.InvocationState, enabled channelonboarding.TargetEnablementReader) (serveSessionBootstrapOwner, error) {
				value, err := prior(selectRuntime, store, credentials, directory, invocation, enabled)
				if err == nil {
					owners <- value.(*serveSessionBootstrap)
				}
				return value, err
			}
			t.Cleanup(func() { newServeSessionBootstrap = prior })
			writer := &diagnosticHeldWriter{entered: make(chan struct{}), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(writer.release) })
			opts.ErrorOutput, opts.Verbose = writer, true
			opts.TestChannelReconcileCadence = runtimechanneldelivery.ReconcileCadence{Native: 20 * time.Millisecond}
			process := startServeRuntimeTestProcess(t, opts)
			t.Cleanup(release)
			process.waitForReadyLine()
			owner := <-owners
			writer.gate.Lock()
			writer.plain = process.out
			writer.gate.Unlock()
			runtime := servedTestProcessRuntime(t, process)
			ctx := servedRuntimeProofAuthorActivityContext(t, runtime, runtime.Options.SourceArtifactFact.BundleHash())
			before, err := owner.registeredSessionConnections(ctx)
			if err != nil || len(before) != 1 || before[0].Operation.OperationID != op.OperationID || before[0].Fact.Down || before[0].Account != op.AccountReference {
				t.Fatal("registered read did not use exact native owner", err, before)
			}
			activations, err := owner.store.ListCurrentConnectedChannelActivations(ctx)
			if err != nil || len(activations) != 0 {
				t.Fatal("registered proof acquired a learned activation", err)
			}
			after, err := owner.registeredSessionConnections(ctx)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("observation mutated owner", err)
			}
			wrong := before[0].Operation
			wrong.PrincipalID = uuid.NewString()
			if _, err := owner.readSessionDiagnostic(ctx, wrong); err == nil {
				t.Fatal("foreign parent observed a fact")
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := owner.registeredSessionConnections(canceled); err == nil {
				t.Fatal("cancellation became observation")
			}
			peer.mu.Lock()
			peer.closeReason = "PRIVATE-CANARY-ACCOUNT-SESSION-KEY"
			peer.mu.Unlock()
			peer.disconnect()
			select {
			case <-writer.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("worker did not reach diagnostic writer")
			}
			items, err := owner.registeredSessionConnections(ctx)
			if err != nil || len(items) != 1 || !items[0].Fact.Down || items[0].Fact.Cause() != "closed by the other side" {
				t.Fatal("held output blocked socket/fence observation", items, err)
			}
			connection := owner.connection(op.OperationID)
			var status channelonboarding.AccountStatus
			endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			stamp, found := runtime.Options.ProcessWorkOwner.OriginalProcess()
			if !found {
				t.Fatal("status read lost its exact serve holder")
			}
			holder := startupownership.AuthorityFromLease(stamp)
			requireServedJSONRPCResult(t, endpoint, "channel.list", map[string]any{"scope": "deployment", "holder": holder, "provider": "whatsapp", "account": op.AccountReference}, &status)
			if status.Health != "error" || status.State != "refused" || status.Reason != "network unreachable" || status.Cause != "closed by the other side" {
				t.Fatal("native failure projection changed the legacy reason", status)
			}
			var list struct {
				Accounts []channelonboarding.Result `json:"accounts"`
			}
			requireServedJSONRPCResult(t, endpoint, "channel.list", map[string]any{"scope": "deployment", "holder": holder, "provider": "whatsapp"}, &list)
			if len(list.Accounts) != 1 || list.Accounts[0].Status == nil || list.Accounts[0].Connection == nil ||
				list.Accounts[0].Status.Cause != status.Cause || list.Accounts[0].Connection.Cause != status.Cause || list.Accounts[0].Connection.State != "error" || list.Accounts[0].Connection.Reason != status.Reason {
				t.Fatal("list status and connection lost the same native failure", list)
			}
			if connection.CheckBootstrap(ctx) == nil {
				t.Fatal("down diagnostic granted native health")
			}
			if err := connection.Close(ctx); err != nil {
				t.Fatal("held writer blocked native join", err)
			}
			process.cancel()
			select {
			case code := <-process.done:
				process.recordStopped(code)
				t.Fatal("blocked worker falsely joined", code)
			case <-time.After(25 * time.Millisecond):
			}
			release()
			if code := process.stop(); code != 0 {
				t.Fatal("released writer did not join process", code)
			}
			plain, detail := process.outputString(), writer.String()
			if strings.Count(plain, "ERROR WhatsApp disconnected: closed by the other side") != 1 || !strings.Contains(detail, "socket=peer_close") || strings.Contains(plain+detail, "PRIVATE-CANARY") {
				t.Fatal("wrong new line or raw diagnostic leak")
			}
			select {
			case <-peer.sent:
				t.Fatal("diagnostic emitted an application send")
			default:
			}
		}) {
			t.Fatal("first backend failure; later backend not run")
		}
	}
}
