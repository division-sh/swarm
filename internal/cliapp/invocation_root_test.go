package cliapp

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInvocationRootCanonicalizesSymlinksAndResolvesOperands(t *testing.T) {
	realRoot := t.TempDir()
	linkParent := t.TempDir()
	link := filepath.Join(linkParent, "project")
	if err := os.Symlink(realRoot, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	root, err := NewInvocationRoot(link)
	if err != nil {
		t.Fatal(err)
	}
	if root.Path() != realRoot {
		t.Fatalf("canonical invocation root = %q, want %q", root.Path(), realRoot)
	}
	if got := root.Resolve(filepath.Join("nested", "file.json")); got != filepath.Join(realRoot, "nested", "file.json") {
		t.Fatalf("relative projection = %q", got)
	}
	absolute := filepath.Join(t.TempDir(), "absolute.json")
	if got := root.Resolve(absolute); got != absolute {
		t.Fatalf("absolute projection = %q, want unchanged %q", got, absolute)
	}
	if _, err := NewInvocationRoot(""); err == nil || !strings.Contains(err.Error(), "invocation root is required") {
		t.Fatalf("missing root error = %v", err)
	}
	if _, err := NewInvocationRoot("relative"); err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("relative root error = %v", err)
	}
}

func TestCLIRelativeSwarmDirUsesInvocationRoot(t *testing.T) {
	root := mustInvocationRootForTest(t.TempDir())
	flag, err := resolveCLISwarmDirFromConfig(root, cliSwarmDirOptions{SwarmDir: "flag-state", SwarmDirFlagSet: true}, cliCommandConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if flag.Path != filepath.Join(root.Path(), "flag-state") {
		t.Fatalf("relative --swarm-dir = %q", flag.Path)
	}
	configured, err := resolveCLISwarmDirFromConfig(root, cliSwarmDirOptions{}, cliCommandConfig{Paths: cliPathsConfig{SwarmDir: "config-state", SwarmDirSet: true}})
	if err != nil {
		t.Fatal(err)
	}
	if configured.Path != filepath.Join(root.Path(), "config-state") {
		t.Fatalf("relative paths.swarm_dir = %q", configured.Path)
	}
}

func TestCLIRelativeTokenFilesUseInvocationRoot(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := mustInvocationRootForTest(t.TempDir())
	if err := os.WriteFile(root.Resolve("client.token"), []byte("client-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(root.Resolve("server.token"), []byte("server-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(root.Resolve("operator.yaml"), []byte("serve:\n  api_token_file: server.token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	chdirForTest(t, t.TempDir())

	client, err := resolveCLIAPITokenForTarget(rootCommandOptions{invocationRoot: root}, cliCommandConfig{Connection: cliConnectionConfig{APITokenFile: "client.token"}}, cliAPITargetResolution{rpcEndpoint: "http://127.0.0.1:8081/v1/rpc"})
	if err != nil {
		t.Fatal(err)
	}
	if client.token != "client-secret" {
		t.Fatalf("client token = %q", client.token)
	}
	server, err := ResolveServeAPIAuth(root, ServeOptions{ConfigPath: "operator.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	if len(server.Tokens) != 1 || server.Tokens[0] != "server-secret" || server.TokenFile != root.Resolve("server.token") {
		t.Fatalf("server auth = %#v", server)
	}
}

func TestCLIArchetypeOutputUsesInvocationRoot(t *testing.T) {
	root := mustInvocationRootForTest(t.TempDir())
	chdirForTest(t, t.TempDir())
	if err := scaffoldArchetype(root, io.Discard, "zero-agent-automation", "starter"); err != nil {
		t.Fatal(err)
	}
	for _, relative := range []string{"manifest.yaml", filepath.Join("automation", "schema.yaml")} {
		if _, err := os.Stat(root.Resolve(filepath.Join("starter", relative))); err != nil {
			t.Fatalf("relative scaffold output %s: %v", relative, err)
		}
	}
	absolute := filepath.Join(t.TempDir(), "absolute-starter")
	if err := scaffoldArchetype(root, io.Discard, "zero-agent-automation", absolute); err != nil {
		t.Fatal(err)
	}
	for _, relative := range []string{"manifest.yaml", filepath.Join("automation", "schema.yaml")} {
		if _, err := os.Stat(filepath.Join(absolute, relative)); err != nil {
			t.Fatalf("absolute scaffold output %s: %v", relative, err)
		}
	}
}

func TestCLIInvocationRootRediscoveryGuard(t *testing.T) {
	dir := filepath.Join(RepoRoot(), "internal", "cliapp")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		source := string(body)
		for _, forbidden := range []string{"DiscoverRepoRoot", "assetCommandRepoRoot", "func RepoRoot(", `"go.mod"`} {
			if strings.Contains(source, forbidden) {
				t.Fatalf("cli_invocation_root_authority: %s restores forbidden interpreter %q", entry.Name(), forbidden)
			}
		}
		if entry.Name() != "invocation_root.go" && strings.Contains(source, "os.Getwd(") {
			t.Fatalf("cli_invocation_root_authority: %s reads cwd outside Execute owner", entry.Name())
		}
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandAtInvocation(context.Background(), InvocationRoot{}, []string{"version"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != CLIExitValidation || !strings.Contains(stderr.String(), "canonical invocation root is required") {
		t.Fatalf("zero root code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}
