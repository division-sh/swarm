package releasee2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"gopkg.in/yaml.v3"
)

func TestSchemaRelationFactoringCompiledReaders(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	config, env := clockReleaseConfig(t, root, "sqlite")
	source := filepath.Join(root, "contracts")
	copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/workspace_mcp"), source)
	configure := func(replacements map[string][]byte) {
		t.Helper()
		_, directories := packfixture.DevelopmentBase(t, replacements)
		body, err := os.ReadFile(config)
		if err != nil {
			t.Fatal(err)
		}
		var document map[string]any
		if err := yaml.Unmarshal(body, &document); err != nil {
			t.Fatal(err)
		}
		document["platform"] = map[string]any{"packs": map[string]any{"platform_dirs": directories}}
		body, err = yaml.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		writeReleaseFile(t, config, string(body))
	}
	configure(nil)
	for _, args := range [][]string{{"verify", "--portable", "--json"}, {"describe", "--json"}} {
		result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, append(args, source, "--config", config)...)
		if result.err != nil || !strings.Contains(result.output, `"live_readiness":"not_evaluated"`) {
			t.Fatalf("compiled structural reader: %v\n%s", result.err, result.output)
		}
	}
	// A real admitted channel output relation feeds compiled-result admission.
	// Changing the target minimum preserves all lexical/seal gates, then refuses.
	replacements := map[string][]byte{}
	for _, entry := range packfixture.EmbeddedBase(t).Entries() {
		if entry.Type() != "channel" {
			continue
		}
		body := string(entry.ManifestBody())
		needle := "id: {type: integer, minimum: 1, maximum: 2147483647}"
		if !strings.Contains(body, needle) {
			t.Fatal("channel fixture no longer carries the bounded result id")
		}
		replacements[entry.ID()] = []byte(strings.Replace(body, needle, "id: {type: integer, minimum: 2, maximum: 2147483647}", 1))
	}
	if len(replacements) != 1 {
		t.Fatal("expected one exact channel fixture")
	}
	configure(replacements)
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
