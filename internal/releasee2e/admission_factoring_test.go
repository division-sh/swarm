package releasee2e

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAdmissionFactoringCompiledStructuralSurfaces(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	config, env := clockReleaseConfig(t, root, "sqlite")
	source := copyFactoringStandingSource(t)
	for _, args := range [][]string{
		{"verify", "--portable", "--json"}, {"verify", "--json"},
		{"describe"}, {"describe", "--json"}, {"describe", "--quiet"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, append(args, source, "--config", config)...)
			deploymentVerify := args[0] == "verify" && len(args) == 2
			if deploymentVerify {
				exit, ok := result.err.(*exec.ExitError)
				if !ok || exit.ExitCode() != 4 || !strings.Contains(result.output, "credential_key_exists") || !strings.Contains(result.output, "telegram.telegram_bot_token") || !strings.Contains(result.output, "telegram.webhook_signing_secret") {
					t.Fatalf("deployment verify lost exact missing-credential refusal: %v\n%s", result.err, result.output)
				}
			} else if result.err != nil {
				t.Fatalf("compiled source reader: %v\n%s", result.err, result.output)
			}
			for _, retired := range []string{"capability_subjects", "production_valid", "workspace_backend"} {
				if strings.Contains(result.output, `"`+retired+`"`) {
					t.Fatalf("structural read reports %s: %s", retired, result.output)
				}
			}
			if strings.Contains(strings.Join(args, " "), "--json") {
				// Public JSON names are deliberately asserted separately from text output.
				var fields map[string]json.RawMessage
				if err := json.Unmarshal([]byte(result.output), &fields); err != nil {
					t.Fatal(err)
				}
				if string(fields["live_readiness"]) != `"not_evaluated"` {
					t.Fatalf("invented live readiness: %s", result.output)
				}
				if args[0] == "verify" && len(args) == 2 {
					// First-use store absence is valid but must remain explicit.
					if string(fields["validation_scope"]) != `"deployment"` || string(fields["admission_complete"]) != "false" || !strings.Contains(string(fields["observations"]), `"selected_store_access"`) || !strings.Contains(string(fields["observations"]), `"startup_process_possession"`) {
						t.Fatalf("lost fresh-store deployment admission: %s", result.output)
					}
				}
			}
		})
	}
	// The authored source gate stays intact: describe/verify must not bypass
	// malformed recursive definitions simply to produce a projection.
	writeReleaseFile(t, filepath.Join(source, "tools.yaml"), "schema_guard:\n  description: schema admission guard\n  input_schema: {type: array}\n")
	result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, "verify", source, "--portable", "--json", "--config", config)
	if result.err == nil || !strings.Contains(result.output, "array requires items") {
		t.Fatalf("compiled verify did not reach canonical schema refusal: %v\n%s", result.err, result.output)
	}
}

func copyFactoringStandingSource(t *testing.T) string {
	t.Helper()
	root := filepath.Join(goldenReleaseRoot(t), "contracts")
	copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/admission_factoring"), root)
	return root
}

func TestAdmissionFactoringRegisteredToolExecution(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	env := goldenProcessEnv(t, root, "", 0)
	assertGoldenProcessHasNoExternalExecutables(t, env)
	source := filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/workspace_mcp")
	result := runReleaseCommand(t, time.Minute, root, env, "", binary, "test", source, "tests/transport.yaml", "--timeout", "20s", "--poll-interval", "25ms")
	if result.err != nil || !strings.Contains(result.output, "swarm test ok: scenarios=1") {
		t.Fatalf("compiled MockOnly host worker / real MCP / read / emit settlement: %v\n%s", result.err, result.output)
	}
	t.Log("public host MockOnly/private SQLite; both-store durable output/journal credit is separately named, not a deployment-config override")
}
