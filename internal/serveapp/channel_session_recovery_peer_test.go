//go:build linux || darwin

package serveapp

import (
	"context"
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
		if !t.Run(backend, func(t *testing.T) {
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
			process := startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			exerciseServedSessionRecovery(t, opts, process, peer, op.OperationID, true, read)
		}) {
			t.Fatal("first registered backend failed")
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
	gate, release := recoveryLoginGate(t, peer)
	process = startServeRuntimeTestProcess(t, opts)
	select {
	case <-gate.held:
	case <-time.After(5 * time.Second):
		t.Fatal("boot did not reach retained login")
	}
	release(false)
	process.waitForReadyLine()
	wait(healthy)
	_, _, accepted = counts()
	if accepted != sends || operation().SessionAccount != original.SessionAccount {
		t.Fatal("unavailable boot reset or resent")
	}
	gate, release = recoveryLoginGate(t, peer)
	current = operation()
	peer.disconnect()
	select {
	case <-gate.held:
	case <-time.After(20 * time.Second):
		t.Fatal("scheduled attempt did not hold")
	}
	if code := process.stop(); code != 0 {
		release(true)
		t.Fatal("held native attempt did not cancel/join", code)
	}
	release(true)
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
