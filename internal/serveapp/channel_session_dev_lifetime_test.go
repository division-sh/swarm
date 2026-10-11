//go:build linux || darwin

package serveapp

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
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
	process := startServeRuntimeTestProcessAtRepo(t, root, opts)
	process.waitForReadyLine()
	endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
	ready := requireServedNativeProcessPairing(t, endpoint, peer)
	connectionID := readServedNativeConnectionID(t, "sqlite", filepath.Join(root, ".swarm", "stores", "dev-scratch.db"), ready.Operation.OperationID)
	if code := process.stop(); code != 0 {
		t.Fatal("dev serve did not completely join native work", code, process.outputString())
	}
	requireServedNativeDisconnect(t, peer)
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
	if connections != 1 || disconnected != connections {
		t.Fatal("fresh dev invocation resumed obsolete pairing", connections, disconnected)
	}
	if code := process.stop(); code != 0 {
		t.Fatal("fresh dev invocation did not join", code, process.outputString())
	}
}
