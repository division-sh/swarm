package releasee2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSchemaRelationFactoringCompiledReaders(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	config, env := clockReleaseConfig(t, root, "sqlite")
	source := filepath.Join(root, "contracts")
	copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/workspace_mcp"), source)
	imported := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, "import", "provider.telegram.hitl_channel", source, "--config", config, "--json")
	if imported.err != nil {
		t.Fatalf("public pack import: %v\n%s", imported.err, imported.output)
	}
	for _, args := range [][]string{{"verify", "--portable", "--json"}, {"describe", "--json"}} {
		result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, append(args, source, "--config", config)...)
		if result.err != nil || !strings.Contains(result.output, `"live_readiness":"not_evaluated"`) {
			t.Fatalf("compiled structural reader: %v\n%s", result.err, result.output)
		}
	}
	// A real admitted channel output relation feeds compiled-result admission.
	// Project-owned imports derive identity from authored bytes, so a changed
	// target minimum reaches directional proof without bypassing the seal owner.
	path := filepath.Join(source, "packs", "provider.telegram.hitl_channel", "channel.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	needle := "id: {type: integer, minimum: 1, maximum: 2147483647}"
	if !strings.Contains(string(body), needle) {
		t.Fatal("channel fixture no longer carries the bounded result id")
	}
	writeReleaseFile(t, path, strings.Replace(string(body), needle, "id: {type: integer, minimum: 2, maximum: 2147483647}", 1))
	for _, args := range [][]string{{"verify", "--portable", "--json"}, {"describe", "--json"}} {
		result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, append(args, source, "--config", config)...)
		if result.err == nil || !strings.Contains(result.output, "source minimum is broader than target minimum 2") {
			t.Fatalf("compiled directional refusal: %v\n%s", result.err, result.output)
		}
	}
}

func TestSchemaRelationFactoringCompiledHostMCP(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	env := goldenProcessEnv(t, root, "", 0)
	assertGoldenProcessHasNoExternalExecutables(t, env)
	source := filepath.Join(root, "contracts")
	copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/workspace_mcp"), source)
	result := runReleaseCommand(t, time.Minute, root, env, "", binary, "test", source, "tests/transport.yaml", "--timeout", "20s", "--poll-interval", "25ms")
	if result.err != nil || !strings.Contains(result.output, "swarm test ok: scenarios=1") {
		t.Fatalf("compiled MockOnly host / real HTTP MCP / mapped read and emit settlement: %v\n%s", result.err, result.output)
	}
}
