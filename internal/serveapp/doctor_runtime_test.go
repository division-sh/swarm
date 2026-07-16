package serveapp

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

func setDoctorEmptyProviderSecrets(t *testing.T) {
	t.Helper()
	setDoctorProviderSecrets(t, nil)
}

func setDoctorProviderSecrets(t *testing.T, values map[string]string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "provider-credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", path)
	store, err := runtimecredentials.NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for key, value := range values {
		if err := store.Set(context.Background(), key, value); err != nil {
			t.Fatalf("Set provider credential: %v", err)
		}
	}
}

func TestRunServeRuntimeConsumesLocalClaudePreflightAfterBundleDecision(t *testing.T) {
	dockerBin := configureDoctorDockerStub(t)
	setDoctorEmptyProviderSecrets(t)
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	var out bytes.Buffer
	code := Run(context.Background(), cliapp.RepoRoot(), cliapp.ServeOptions{
		ConfigPath:         writeDoctorClaudeConfig(t, dockerBin),
		ContractsPath:      doctorAgentContractsPath,
		DataSource:         t.TempDir(),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "not-a-store",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: false,
		Verbose:            true,
		Output:             &out,
		Dev:                true,
	})
	if code == 0 {
		t.Fatalf("Run unexpectedly succeeded\noutput:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "claude_cli preflight: failed") || !strings.Contains(out.String(), "missing_backend_credential") {
		t.Fatalf("serve output missing shared local preflight failure:\n%s", out.String())
	}
	for _, want := range []string{"db_connection", "bundle_load", "workspace backend: docker"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("serve output missing preflight ordering proof %q:\n%s", want, out.String())
		}
	}
}

func TestRunServeRuntimeRejectsDeclaredProviderTriggerPackBeforeStoreSelection(t *testing.T) {
	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "swarm.yaml")
	writeRuntimeConfigText(t, configPath, strings.Join([]string{
		"runtime:",
		"  recovery_on_startup: false",
		"workspace:",
		"  data_source: " + t.TempDir(),
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
		"provider_triggers:",
		"  packs:",
		"    platform_dirs:",
		"      - packs/missing-provider",
	}, "\n")+"\n")

	var out bytes.Buffer
	code := Run(context.Background(), cliapp.RepoRoot(), cliapp.ServeOptions{
		ConfigPath:         configPath,
		ContractsPath:      doctorAgentContractsPath,
		DataSource:         t.TempDir(),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "not-a-store",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: false,
		Verbose:            true,
		Output:             &out,
		Dev:                true,
	})
	if code != 1 {
		t.Fatalf("Run code = %d, want config load failure\noutput:\n%s", code, out.String())
	}
	if !strings.Contains(out.String(), "config_load") || !strings.Contains(out.String(), filepath.Join(configDir, "packs", "missing-provider")) {
		t.Fatalf("serve output missing declared provider pack load failure:\n%s", out.String())
	}
	if strings.Contains(out.String(), "not-a-store") || strings.Contains(out.String(), "db_connection") {
		t.Fatalf("serve reached store selection instead of failing provider pack admission first:\n%s", out.String())
	}
}

const doctorAgentContractsPath = "tests/tier8-boot-verification/test-boot-prompt-stub"

func writeDoctorClaudeConfig(t *testing.T, dockerBin string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "claude.yaml")
	storePath := filepath.Join(t.TempDir(), "runtime.db")
	workspace := []string{
		"workspace:",
		"  data_source: " + t.TempDir(),
		"  image: doctor-test-image:latest",
	}
	if strings.TrimSpace(dockerBin) != "" {
		workspace = append(workspace, fmt.Sprintf("  docker_bin: %q", dockerBin))
	}
	providerPacks := []string{"provider_triggers:", "  packs:", "    platform_dirs:"}
	for _, dir := range testProviderTriggerPackDirs(t) {
		providerPacks = append(providerPacks, fmt.Sprintf("      - %q", dir))
	}
	writeRuntimeConfigText(t, path, strings.Join([]string{
		"store:",
		"  backend: sqlite",
		"  sqlite:",
		"    path: " + storePath,
		"runtime:",
		"  recovery_on_startup: false",
		strings.Join(workspace, "\n"),
		"llm:",
		"  backend: claude_cli",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
		"  claude_cli:",
		"    command: claude",
		"    timeout: 2s",
		"    output_format: json",
		strings.Join(providerPacks, "\n"),
	}, "\n")+"\n")
	return path
}

func configureDoctorDockerStub(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "docker")
	script := `#!/bin/sh
if [ -n "${SWARM_TEST_DOCKER_LOG:-}" ]; then
  printf '%s\n' "$*" >> "${SWARM_TEST_DOCKER_LOG}"
fi
case "$1:$2" in
  version:--format)
    if [ "${SWARM_TEST_DOCKER_UNAVAILABLE:-}" = "1" ]; then
      echo "docker unavailable" >&2
      exit 1
    fi
    exit 0
    ;;
  image:inspect)
    if [ "${SWARM_TEST_DOCKER_IMAGE_MISSING:-}" = "1" ]; then
      echo "no such image" >&2
      exit 1
    fi
    exit 0
    ;;
  run:--rm)
    if [ "${SWARM_TEST_DOCKER_CLI_MISSING:-}" = "1" ]; then
      echo "claude: not found" >&2
      exit 127
    fi
    if [ "${SWARM_TEST_DOCKER_CLI_BROKEN:-}" = "1" ]; then
      case "$*" in
        *"command -v"*"--version"*)
          echo "claude launcher failed after command lookup" >&2
          exit 126
          ;;
      esac
    fi
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write docker stub: %v", err)
	}
	return path
}
