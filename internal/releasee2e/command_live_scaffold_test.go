package releasee2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This is live readiness, not message/turn proof. Provisioning belongs to an
// isolated operator home; generated projects receive no authored configuration.
func TestCommandLiveUneditedScaffoldReadiness(t *testing.T) {
	if os.Getenv("SWARM_COMMAND_SCAFFOLD_LIVE_E2E") != "1" {
		t.Skip("set SWARM_COMMAND_SCAFFOLD_LIVE_E2E=1 with provisioned live prerequisites")
	}
	prerequisites, err := readCommandLivePrerequisites(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	for _, archetype := range []string{"zero-agent-automation", "webhook-responder"} {
		t.Run(archetype, func(t *testing.T) {
			operator := filepath.Join(root, archetype+"-operator")
			project := filepath.Join(root, archetype)
			env := goldenProcessEnv(t, operator, "", 0)
			env = slices.DeleteFunc(env, func(entry string) bool {
				return strings.HasPrefix(entry, "PATH=") || strings.HasPrefix(entry, "TELEGRAM_BOT_TOKEN=")
			})
			env = append(env, "PATH="+os.Getenv("PATH"),
				"SWARM_CREDENTIALS_FILE="+filepath.Join(operator, "credentials.json"),
				"SWARM_MANAGED_CREDENTIALS_FILE="+filepath.Join(operator, "managed-credentials.json"))
			// The built-in profile remains Anthropic API. Select the provisioned
			// Claude transport in operator config, never in generated source.
			config := "llm:\n  backend: claude_cli\nworkspace:\n  image: " + strconv.Quote(prerequisites.image) + "\n"
			if prerequisites.network != "" {
				config += "  network: " + strconv.Quote(prerequisites.network) + "\n"
			}
			writeReleaseFile(t, filepath.Join(operator, "home", ".config", "swarm", "swarm.yaml"), config)
			result := runReleaseCommand(t, time.Minute, root, env, "", binary, "new", archetype, "--output", project)
			if result.err != nil {
				t.Fatalf("new: %v\n%s", result.err, result.output)
			}
			before, err := releaseSourceTree(project, true)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"swarm.yaml", "swarm.live.yaml", ".swarm/swarm.yaml"} {
				if _, err := os.Stat(filepath.Join(project, name)); !os.IsNotExist(err) {
					t.Fatalf("scaffold generated deployment configuration %s: %v", name, err)
				}
			}
			verify := runReleaseCommand(t, time.Minute, project, env, "", binary, "verify", "--json")
			assertFullLifecycleVerifySuccess(t, verify)
			for _, command := range [][]string{{"describe", "--json"}, {"test"}} {
				result := runReleaseCommand(t, time.Minute, project, env, "", binary, command...)
				if result.err != nil {
					t.Fatalf("bare %s: %v\n%s", command[0], result.err, result.output)
				}
			}
			var secret [32]byte
			if _, err := rand.Read(secret[:]); err != nil {
				t.Fatal(err)
			}
			signing := hex.EncodeToString(secret[:])
			for key, value := range map[string]string{
				"CLAUDE_CODE_OAUTH_TOKEN": prerequisites.claudeToken,
				"telegram_bot_token":      prerequisites.botToken, "webhook_signing.telegram": signing,
			} {
				result := runReleaseCommand(t, time.Minute, project, env, value+"\n", binary, "secrets", "set", key, "--stdin")
				if result.err != nil || strings.Contains(result.output, value) {
					t.Fatalf("public provisioning failed or leaked %s (output withheld)", key)
				}
			}
			tokenFile := filepath.Join(operator, "api-token")
			writeReleaseFile(t, tokenFile, goldenAPIToken+"\n")
			for _, dev := range []bool{false, true} {
				t.Run("dev="+strconv.FormatBool(dev), func(t *testing.T) {
					process := startReleaseServe(t, releaseProcessSpec{
						BinaryPath: binary, WorkingDir: project, DefaultExecutionSelection: true,
						Dev: dev, APIPort: freeReleaseTCPPort(t),
						MCPListenHost: prerequisites.mcpHost, TokenFile: tokenFile, Token: goldenAPIToken,
						Env: env, RedactValues: []string{prerequisites.claudeToken, prerequisites.botToken, signing},
					})
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
					defer cancel()
					if err := process.waitReady(ctx); err != nil {
						t.Fatalf("default-selected serve: %v\n%s", err, process.output.String())
					}
					requireLifecycleHealthPosture(t, process.rpc, "live")
					if err := process.stopAndWait(30 * time.Second); err != nil {
						t.Fatalf("stop: %v\n%s", err, process.output.String())
					}
					if after, err := releaseSourceTree(project, true); err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("bare command mutated authored source: %v", err)
					}
					t.Log("public live readiness; operator-global Claude provisioning; unchanged scaffold; no project config or command source/backend/store selector; no messages sent")
				})
			}
		})
	}
}
