//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/store/sessionstate"
	"github.com/division-sh/swarm/tests/support/testutil"
	"go.mau.fi/whatsmeow/types"
	"gopkg.in/yaml.v3"
)

func captureRecoveryBootstrap(t *testing.T) func() *serveSessionBootstrap {
	t.Helper()
	prior := newServeSessionBootstrap
	var latest atomic.Pointer[serveSessionBootstrap]
	newServeSessionBootstrap = func(selectRuntime serveSessionBootstrapRuntime, store channelonboarding.Store, credentials operatorchannel.CredentialCurrentness, directory string, invocation *sessionprovider.InvocationState, enabled channelonboarding.TargetEnablementReader) (serveSessionBootstrapOwner, error) {
		owner, err := prior(selectRuntime, store, credentials, directory, invocation, enabled)
		if err == nil {
			latest.Store(owner.(*serveSessionBootstrap))
		}
		return owner, err
	}
	t.Cleanup(func() { newServeSessionBootstrap = prior })
	return latest.Load
}

// Invariant: real learned pairing/claim/journal baseline precedes peer drop/recovery and authored send, both stores.
func TestRunServeSessionRecoveryLearnedBothStores(t *testing.T) {
	testRunServeWhatsAppSignedPairing(t, servedNativeJourneyProof{authoredReply: true, recoveryOwner: captureRecoveryBootstrap(t)})
}

// Invariant: real public registered connect uses the exact cached owner through drop, boot and held shutdown, both stores.
func TestRunServeSessionRecoveryRegisteredBothStores(t *testing.T) {
	read := captureRecoveryBootstrap(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, permanent := range []string{"transient", "logout", "replacement", "ban"} {
			if !t.Run(backend+"/"+permanent, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
				peer := newServeNativeProtocolPeer(t)
				opts := cliapp.DefaultServeOptions()
				opts.SourceRoot = filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session-reply")
				opts.SwarmDir, opts.SwarmDirSet = t.TempDir(), true
				if err := os.Chmod(opts.SwarmDir, 0700); err != nil {
					t.Fatal(err)
				}
				opts.WorkspaceBackend, opts.WorkspaceBackendSet = "host", true
				opts.StoreMode, opts.StoreModeSet = backend, true
				opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
				if backend == "sqlite" {
					opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, filepath.Join(t.TempDir(), "recovery.sqlite"), channelOnboardingHostWorkspaceFields())
				} else {
					opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
				}
				config, err := os.ReadFile(opts.ConfigPath)
				if err != nil {
					t.Fatal(err)
				}
				writeStandingCandidateFile(t, opts.ConfigPath, string(config)+"\npaths:\n  swarm_dir: "+opts.SwarmDir+"\n")
				op := issue3011RegisterForSend(t, opts, peer)
				if permanent != "transient" {
					requireRecoveryPermanentBoot(t, opts, peer, permanent)
					return
				}
				process := startServeRuntimeTestProcess(t, opts)
				process.waitForReadyLine()
				exerciseServedSessionRecovery(t, opts, process, peer, op.OperationID, true, read)
			}) {
				t.Fatal("first registered backend failed")
			}
		}
	}
}

func recoveryLoginGate(t *testing.T, peer *serveNativeProtocolPeer) (*servedNativeLoginGate, func(bool)) {
	t.Helper()
	gate := &servedNativeLoginGate{held: make(chan struct{}), release: make(chan bool, 1)}
	var once sync.Once
	release := func(proceed bool) { once.Do(func() { gate.release <- proceed }) }
	t.Cleanup(func() { release(true) })
	peer.mu.Lock()
	peer.nextLogin = gate
	peer.mu.Unlock()
	return gate, release
}

func exerciseServedSessionRecovery(t *testing.T, opts cliapp.ServeOptions, process *serveRuntimeTestProcess, peer *serveNativeProtocolPeer, id string, registered bool, read func() *serveSessionBootstrap) {
	t.Helper()
	operation := func() channelonboarding.Operation {
		t.Helper()
		owner := read()
		if owner == nil {
			t.Fatal("missing original bootstrap")
		}
		var store channelonboarding.OperationStore = owner.store
		if registered {
			store = owner.store.(interface {
				DeploymentChannelOperations() channelonboarding.OperationStore
			}).DeploymentChannelOperations()
		}
		runtime := servedTestProcessRuntime(t, process)
		ctx := servedRuntimeProofAuthorActivityContext(t, runtime, runtime.Options.SourceArtifactFact.BundleHash())
		op, err := store.GetChannelOnboarding(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return op
	}
	wait := func(check func() bool) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for !check() {
			if time.Now().After(deadline) {
				t.Fatal("recovery observation timed out", process.outputString())
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	counts := func() (int, int, int) {
		peer.mu.Lock()
		defer peer.mu.Unlock()
		return peer.connections, peer.postLogins, peer.acceptedSends
	}
	healthy := func() bool {
		op := operation()
		c := read().connection(id)
		if c == nil {
			return false
		}
		runtime := servedTestProcessRuntime(t, process)
		ctx := servedRuntimeProofAuthorActivityContext(t, runtime, runtime.Options.SourceArtifactFact.BundleHash())
		_, ok, err := c.SessionRecovery(ctx, op)
		return ok && err == nil
	}
	wait(healthy)
	original := operation()
	old := read().connection(id)
	connections, logins, sends := counts()
	peer.disconnect()
	wait(func() bool { n, l, _ := counts(); return n > connections && l > logins && healthy() })
	n, l, accepted := counts()
	current := operation()
	if n != connections+1 || l != logins+1 || accepted != sends || read().connection(id) == old || !old.Closed() || current.OperationID != original.OperationID || current.SessionConnectionID != original.SessionConnectionID || current.SessionAccount != original.SessionAccount {
		t.Fatal("recovery replaced pairing/authority or resent", n, l, accepted)
	}
	if strings.Count(process.outputString(), "WhatsApp disconnected: network unreachable, reconnecting...") != 1 {
		t.Fatal("drop sentence not once", process.outputString())
	}
	endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
	requireServedNativeAuthoredCustomerReply(t, endpoint, peer, false)
	_, _, sends = counts()
	if code := process.stop(); code != 0 {
		t.Fatal("healthy recovery did not join", code)
	}
	process = startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	wait(healthy)
	_, _, accepted = counts()
	if accepted != sends {
		t.Fatal("retained restart resent", accepted, sends)
	}
	if code := process.stop(); code != 0 {
		t.Fatal("retained startup did not join", code)
	}
	var observation channelReconcileObservation
	opts.TestChannelReconcileStarted = observation.started
	opts.TestBeforeReadinessCommit = func() error {
		observation.mu.Lock()
		defer observation.mu.Unlock()
		if observation.mark == nil {
			return errors.New("READY has no installed recovery subscription")
		}
		if _, active := observation.mark(); !active {
			return errors.New("READY recovery subscription is retired")
		}
		return nil
	}
	if registered {
		body, err := os.ReadFile(opts.ConfigPath)
		if err != nil {
			t.Fatal(err)
		}
		var cfg map[string]any
		if err := yaml.Unmarshal(body, &cfg); err != nil {
			t.Fatal(err)
		}
		cfg["channels"] = map[string]any{"hitl": map[string]any{"provider": "whatsapp", "account": "+" + peer.account.User}}
		body, err = yaml.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		writeStandingCandidateFile(t, opts.ConfigPath, string(body))
	}
	gate, release := recoveryLoginGate(t, peer)
	peer.mu.Lock()
	peer.failNextDial = true
	peer.mu.Unlock()
	process = startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	runtime := servedTestProcessRuntime(t, process)
	ctx := servedRuntimeProofAuthorActivityContext(t, runtime, runtime.Options.SourceArtifactFact.BundleHash())
	startAt, connected, err := read().connection(id).SessionRecovery(ctx, operation())
	if err != nil || connected || startAt.IsZero() {
		t.Fatal("actual SDK dial failure lost recoverable ownership", err)
	}
	endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
	var down channelonboarding.Result
	if registered {
		down = issue3011RegisteredStatus(t, opts, endpoint, operation())
		if down.Status.Health != "disconnected" || down.Status.Reason == "" || len(down.Status.Holders) != 1 {
			t.Fatal("deferred registered status lost its exact live holder/reason", down.Status)
		}
		wait(func() bool { return strings.Contains(process.outputString(), "ERROR WhatsApp disconnected: ") })
	} else {
		requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &down)
	}
	if down.Operation.OperationID != id || down.Connection == nil || down.Connection.State != "disconnected" || down.Connection.Reason == "" {
		t.Fatal("deferred public readback lost disconnected/reason", down)
	}
	lease, currentPublication := runtime.ChannelActivations.AcquirePresentation()
	if !currentPublication {
		t.Fatal("unavailable boot omitted the structural publication")
	}
	for _, activation := range lease.Activations() {
		if activation.OnboardingOperationID == id {
			t.Error("unavailable boot published native execution")
		}
	}
	lease.Release()
	select {
	case <-gate.held:
	case <-time.After(10 * time.Second):
		t.Fatal("boot worker did not reach its first retained-login retry")
	}
	if time.Now().Before(startAt.Add(5 * time.Second)) {
		t.Fatal("boot recovery retried before the five-second due")
	}
	failed := read().connection(id)
	release(false)
	var failedAt time.Time
	wait(func() bool {
		failedAt, _, err = failed.SessionRecovery(ctx, operation())
		return err == nil && !failedAt.IsZero()
	})
	wait(healthy)
	if time.Now().Before(failedAt.Add(10 * time.Second)) {
		t.Fatal("SDK dial-start and later login failure did not share one schedule")
	}
	published := false
	wait(func() bool {
		lease, currentPublication = runtime.ChannelActivations.AcquirePresentation()
		if !currentPublication {
			t.Fatal("recovered publication absent")
		}
		for _, activation := range lease.Activations() {
			published = published || activation.OnboardingOperationID == id
		}
		lease.Release()
		return published
	})
	if !published || strings.Contains(process.outputString(), "WhatsApp: reconnected ()") {
		t.Fatal("recovery lacks live publication or fresh account presentation", process.outputString())
	}
	if registered && strings.Count(process.outputString(), "ERROR WhatsApp disconnected: ") != 1 {
		t.Fatal("failed retries repeated the operator boot ERROR", process.outputString())
	}
	_, _, accepted = counts()
	if accepted != sends || operation().SessionAccount != original.SessionAccount {
		t.Fatal("unavailable boot reset or resent")
	}
	endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
	requireServedNativeAuthoredCustomerReply(t, endpoint, peer, false, "RECOVERY_BOOT_CUSTOMER")
	_, _, sends = counts()
	gate, release = recoveryLoginGate(t, peer)
	current = operation()
	peer.disconnect()
	select {
	case <-gate.held:
	case <-time.After(20 * time.Second):
		t.Fatal("scheduled attempt did not hold")
	}
	if code := process.stop(); code != 0 {
		release(false)
		t.Fatal("held native attempt did not cancel/join", code)
	}
	release(false)
	// Fixture-controlled row deletion after joined shutdown, NOT phone/provider unlink.
	fixture, native, err := sessionstate.OpenSDKFixture(context.Background(), filepath.Join(read().directory, current.SessionConnectionID, "provider.db"))
	if err != nil {
		t.Fatal(err)
	}
	jid, err := types.ParseJID(current.SessionAccount.AccountRef)
	if err != nil {
		t.Fatal(err)
	}
	device, err := native.GetDevice(context.Background(), jid)
	if err != nil || device == nil {
		t.Fatal("fixture device", err)
	}
	if err := device.Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	before, _, _ := counts()
	process = startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	wait(func() bool {
		return strings.Contains(process.outputString(), "Not linked: run swarm channel connect whatsapp")
	})
	after, _, _ := counts()
	if after != before {
		t.Fatal("missing-device boot dialed or requested QR")
	}
	if code := process.stop(); code != 0 {
		t.Fatal("not-linked boot failed join", code)
	}
}

func requireRecoveryPermanentBoot(t *testing.T, opts cliapp.ServeOptions, peer *serveNativeProtocolPeer, kind string) {
	t.Helper()
	peer.mu.Lock()
	before, sends := peer.connections, peer.acceptedSends
	peer.permanentLogin = kind
	peer.mu.Unlock()
	process := startServeRuntimeTestProcess(t, opts)
	if kind == "logout" {
		process.waitForReadyLine()
		deadline := time.Now().Add(5 * time.Second)
		for !strings.Contains(process.outputString(), "Not linked: run swarm channel connect whatsapp") {
			if time.Now().After(deadline) {
				t.Fatal("revoked boot omitted terminal Not linked", process.outputString())
			}
			time.Sleep(10 * time.Millisecond)
		}
		if process.stop() != 0 {
			t.Fatal("revoked boot did not join")
		}
	} else if code, done := process.waitForExit(5 * time.Second); !done || code != 1 || serveOutputIsReady(process.outputString()) {
		t.Fatal("permanent boot was deferred instead of refused", code, done, process.outputString())
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		peer.mu.Lock()
		joined := peer.connections == peer.disconnected
		peer.mu.Unlock()
		if joined {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("permanent boot peer did not close its owned handler")
		}
		time.Sleep(10 * time.Millisecond)
	}
	peer.mu.Lock()
	after, closed, accepted := peer.connections, peer.disconnected, peer.acceptedSends
	peer.mu.Unlock()
	if after != before+1 || after != closed || accepted != sends || strings.Contains(process.outputString(), "reconnecting...") {
		t.Fatal("permanent boot retried/resent or failed its owned join", before, after, closed, accepted)
	}
}
