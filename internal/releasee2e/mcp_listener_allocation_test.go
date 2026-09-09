package releasee2e

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReleaseServeAllocatesMCPAtBind(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	source := filepath.Join(root, "bundle")
	copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/node_identity"), source)
	store := goldenSQLiteStore(root)
	config := filepath.Join(root, "swarm.yaml")
	writeReleaseFile(t, config, goldenRuntimeConfig(store))
	token := filepath.Join(root, "api-token")
	writeReleaseFile(t, token, goldenAPIToken+"\n")
	env := goldenProcessEnv(t, root, store.passwordEnv, 0)
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	// A selected number is not ownership. Hold a real competing socket while
	// the release binary reaches the same MCP bind that failed in CI.
	negative := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary,
		"serve", source, "--config", config, "--store", "sqlite",
		"--backend", "claude_cli", "--workspace-backend", "host",
		"--api-token-file", token, "--api-listen-addr", "127.0.0.1:0",
		"--mcp-listen-addr", occupied.Addr().String(), "--no-color")
	if negative.err == nil || !strings.Contains(negative.output, "mcp listener bind failed") || !strings.Contains(negative.output, "address already in use") {
		t.Fatalf("occupied MCP port did not reproduce bind failure: %v\n%s", negative.err, negative.output)
	}
	process := startReleaseServe(t, releaseProcessSpec{
		BinaryPath: binary, WorkingDir: root, ConfigPath: config, Source: source,
		Store: "sqlite", APIPort: freeReleaseTCPPort(t), TokenFile: token, Token: goldenAPIToken, Env: env,
	})
	ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
	defer cancel()
	if err := process.waitReady(ctx); err != nil {
		t.Fatal(err)
	}
	goldenServedBundleHash(t, process.rpc, "live")
	if err := process.stopAndWait(10 * time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestReleaseServeDoesNotPreselectMCPPort(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/process_harness_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "MCPPort") || !strings.Contains(string(raw), `"--mcp-listen-addr", "127.0.0.1:0"`) {
		t.Fatal("the child must allocate its MCP listener at bind; no parent-selected MCP port is needed")
	}
}
