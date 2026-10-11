//go:build linux || darwin

package serveapp

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"go.mau.fi/whatsmeow/types"
)

func TestRunServeWhatsAppDevDoesNotRetainPrivateState(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
	root := t.TempDir()
	schema, err := os.ReadFile(filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session/schema.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), schema, 0o600); err != nil {
		t.Fatal(err)
	}
	peer := newServeNativeProtocolPeer(t)
	opts := cliapp.ServeOptions{SourceRoot: root,
		PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath),
		ConfigPath:       writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", "", channelOnboardingHostWorkspaceFields()),
		SwarmDir:         t.TempDir(), SwarmDirSet: true, StoreMode: "sqlite", StoreModeSet: true,
		WorkspaceBackend: "host", WorkspaceBackendSet: true, SelfCheck: true, Dev: true, NoFeed: true,
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0"}
	if err := os.Chmod(opts.SwarmDir, 0o700); err != nil {
		t.Fatal(err)
	}
	nativeTemp := t.TempDir()
	t.Setenv("TMPDIR", nativeTemp)
	process := startServeRuntimeTestProcessAtRepo(t, root, opts)
	process.waitForReadyLine()
	if base := servedNativeInvocationBase(t, nativeTemp); base != "" {
		t.Fatal("dev boot allocated native state without an admitted connection", base)
	}
	endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
	ready := requireServedNativeProcessPairing(t, endpoint, peer)
	connectionID := readServedNativeConnectionID(t, "sqlite", filepath.Join(root, ".swarm", "stores", "dev-scratch.db"), ready.Operation.OperationID)
	base := servedNativeInvocationBase(t, nativeTemp)
	if base == "" {
		t.Fatal("active dev connection has no invocation-private namespace")
	}
	for _, member := range []string{"session.json", "provider.db", "possession.lock"} {
		if _, err := os.Stat(filepath.Join(base, connectionID, member)); err != nil {
			t.Fatal("actual active native state is not invocation-owned", member, err)
		}
	}
	header, err := os.ReadFile(filepath.Join(base, connectionID, "session.json"))
	if err != nil {
		t.Fatal(err)
	}
	rebound := requireServedNativeProcessPairingClaim(t, endpoint, peer, channelonboarding.VerbRebind,
		"native-dev-rebind", false, func(challenge string) types.JID {
			return peer.claimMessageFrom(challenge, "DEV_REBIND_CLAIM",
				types.NewJID("15551234569", types.DefaultUserServer), types.NewJID("100000000004", types.HiddenUserServer))
		})
	if current := servedNativeInvocationBase(t, nativeTemp); current != base {
		t.Fatal("same-process rebind replaced invocation state", current, base)
	}
	requireServedJSONRPCResult(t, endpoint, "channel.unbind", map[string]any{
		"interface": rebound.Operation.Interface.Selector, "expected_revision": rebound.Operation.BindingRevision,
		"idempotency_key": "native-dev-unbind"}, &struct{}{})
	requireServedNativeDisconnect(t, peer)
	if _, err := os.Stat(filepath.Join(base, connectionID, "provider.db")); err != nil {
		t.Fatal("same-process unbind deleted historical native pairing", err)
	}
	requireServedJSONRPCResult(t, endpoint, "runtime.nuke", map[string]any{
		"include_source_artifacts": false, "idempotency_key": "native-dev-reset"}, &struct{}{})
	requireServedNativeOperationWithdrawn(t, endpoint, rebound.Operation.OperationID)
	if current := servedNativeInvocationBase(t, nativeTemp); current != base {
		t.Fatal("same-process reset deleted or replaced invocation state", current, base)
	}
	retainedHeader, err := os.ReadFile(filepath.Join(base, connectionID, "session.json"))
	if err != nil || !bytes.Equal(header, retainedHeader) {
		t.Fatal("same-process reset changed original pairing identity", err)
	}
	if _, err := os.Stat(filepath.Join(base, connectionID, "provider.db")); err != nil {
		t.Fatal("same-process reset discarded original native state", err)
	}
	if code := process.stop(); code != 0 {
		t.Fatal("dev serve did not completely join native work", code, process.outputString())
	}
	retiredConnections := requireServedNativeDisconnect(t, peer)
	if _, err := os.Stat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("complete dev retirement did not remove its actual native namespace", err)
	}
	if _, err := os.Stat(filepath.Join(opts.SwarmDir, connectionID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("dev session state survived in the persistent deployment directory", connectionID, err)
	}
	select {
	case logout := <-peer.logouts:
		t.Fatal("ordinary dev shutdown inferred destructive provider logout", logout)
	default:
	}
	process = startServeRuntimeTestProcessAtRepo(t, root, opts)
	process.waitForReadyLine()
	endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
	requireServedJSONRPCError(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": ready.Operation.OperationID})
	peer.mu.Lock()
	connections, disconnected := peer.connections, peer.disconnected
	peer.mu.Unlock()
	if connections != retiredConnections || disconnected != connections {
		t.Fatal("fresh dev invocation resumed obsolete pairing", connections, disconnected)
	}
	if base := servedNativeInvocationBase(t, nativeTemp); base != "" {
		t.Fatal("fresh dev invocation adopted or eagerly created native state", base)
	}
	next := requireServedNativeProcessPairing(t, endpoint, peer)
	nextBase := servedNativeInvocationBase(t, nativeTemp)
	if nextBase == "" || nextBase == base || next.Operation.OperationID == ready.Operation.OperationID {
		t.Fatal("repeated dev pairing reused a retired invocation", nextBase)
	}
	if code := process.stop(); code != 0 {
		t.Fatal("fresh dev invocation did not join", code, process.outputString())
	}
	requireServedNativeDisconnect(t, peer)
	if _, err := os.Stat(nextBase); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("second complete dev retirement retained native state", err)
	}
	select {
	case logout := <-peer.logouts:
		t.Fatal("repeated dev cleanup inferred remote logout", logout)
	default:
	}
}

func servedNativeInvocationBase(t *testing.T, parent string) string {
	t.Helper()
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	var found string
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "swarm-native-") {
			continue
		}
		if found != "" || !entry.IsDir() {
			t.Fatal("dev invocation has contradictory private namespaces", entries)
		}
		found = filepath.Join(parent, entry.Name())
	}
	return found
}
