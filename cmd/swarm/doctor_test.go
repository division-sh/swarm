package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"gopkg.in/yaml.v3"
)

func TestDoctorClaudeCLIPreflightReportsMissingPrerequisites(t *testing.T) {
	configureDoctorDockerStub(t)
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")
	t.Setenv("SWARM_TEST_DOCKER_IMAGE_MISSING", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), doctorClaudeArgs(t, writeDoctorClaudeConfig(t), false), &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitRuntime, stdout.String(), stderr.String())
	}
	for _, want := range []string{
		"claude_cli preflight: failed",
		"backend_prerequisite/missing_backend_credential",
		"workspace_prerequisite/workspace_image_unavailable",
		"set CLAUDE_CODE_OAUTH_TOKEN",
		"set SWARM_WORKSPACE_IMAGE",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("doctor output missing %q:\n%s", want, stdout.String())
		}
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestDoctorClaudeCLIPreflightReportsMissingDocker(t *testing.T) {
	configureDoctorDockerStub(t)
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
	t.Setenv("SWARM_TEST_DOCKER_UNAVAILABLE", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), doctorClaudeArgs(t, writeDoctorClaudeConfig(t), false), &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitRuntime, stdout.String(), stderr.String())
	}
	for _, want := range []string{
		"workspace_prerequisite/docker_unavailable",
		"docker is not available",
		"start Docker or set SWARM_DOCKER_BIN",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("doctor output missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestDoctorClaudeCLIPreflightJSONReportsOKWithoutDB(t *testing.T) {
	configureDoctorDockerStub(t)
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")
	t.Setenv(storebackend.EnvSQLitePath, filepath.Join(t.TempDir(), "must-not-be-used.db"))

	args := doctorClaudeArgs(t, writeDoctorClaudeConfig(t), true)
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), args, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	var report localPreflightReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("parse doctor json: %v\n%s", err, stdout.String())
	}
	if !report.OK || report.Owner != localPreflightOwner || report.Mode != "doctor" || report.Backend != "claude_cli" {
		t.Fatalf("report = %#v", report)
	}
	for _, want := range []string{"backend_credential_present", "docker_available", "workspace_image_available", "workspace_claude_cli_available"} {
		if !localPreflightReportHasCode(report, want) {
			t.Fatalf("report missing finding %q: %#v", want, report.Findings)
		}
	}
}

func TestDoctorClaudeCLIPreflightSkipsCredentialForAgentFreeContracts(t *testing.T) {
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	args := doctorClaudeArgs(t, writeDoctorClaudeConfig(t), false)
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "--contracts" {
			args[i+1] = writeDoctorAgentFreeContractsFixture(t)
			break
		}
	}
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), args, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	for _, want := range []string{
		"backend_prerequisite/backend_credential_skipped_agent_free",
		"workspace_prerequisite/agent_free_source",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("doctor output missing %q:\n%s", want, stdout.String())
		}
	}
	for _, forbidden := range []string{
		"missing_backend_credential",
		"workspace_claude_cli_unavailable",
	} {
		if strings.Contains(stdout.String(), forbidden) {
			t.Fatalf("doctor output contains %q for agent-free source:\n%s", forbidden, stdout.String())
		}
	}
}

func TestDoctorClaudeCLIPreflightReportsMissingWorkspaceCLI(t *testing.T) {
	configureDoctorDockerStub(t)
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
	t.Setenv("SWARM_TEST_DOCKER_CLI_MISSING", "1")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), doctorClaudeArgs(t, writeDoctorClaudeConfig(t), false), &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitRuntime, stdout.String(), stderr.String())
	}
	for _, want := range []string{
		"workspace_prerequisite/workspace_claude_cli_unavailable",
		"configured Claude CLI command",
		"workspace image",
		"contains the configured Claude CLI command",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("doctor output missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestDoctorClaudeCLIPreflightUsesCredentialStoreForContractSecrets(t *testing.T) {
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	args := doctorClaudeArgs(t, writeDoctorClaudeConfig(t), false)
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "--contracts" {
			args[i+1] = writeSecretsCommandContractsFixture(t)
			break
		}
	}
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), args, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitRuntime, stdout.String(), stderr.String())
	}
	for _, want := range []string{
		"contract_secret_prerequisite/missing_contract_secret",
		"sendgrid_api_key",
		"tool:email_api",
		"swarm secrets set sendgrid_api_key",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("doctor output missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestDoctorClaudeCLIPreflightReportsRetiredBackendEnv(t *testing.T) {
	t.Setenv("SWARM_LLM_BACKEND", "cli_test")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), doctorClaudeArgs(t, writeDoctorClaudeConfig(t), false), &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitRuntime, stdout.String(), stderr.String())
	}
	for _, want := range []string{
		"backend_prerequisite/config_load_failed",
		"SWARM_LLM_BACKEND is retired",
		"use --backend or llm.backend",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("doctor output missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestDoctorClaudeCLIPreflightReportsStaleGatewayEnv(t *testing.T) {
	configureDoctorDockerStub(t)
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "operator-token")

	mcpPort := freeDoctorTCPPort(t)
	oldPort := freeDoctorTCPPort(t)
	if oldPort == mcpPort {
		oldPort = freeDoctorTCPPort(t)
	}
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:"+oldPort)
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:"+oldPort)

	args := doctorClaudeArgs(t, writeDoctorClaudeConfig(t), false)
	args = append(args[:len(args)-4], "--api-listen-addr", "127.0.0.1:0", "--mcp-listen-addr", "127.0.0.1:"+mcpPort)
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), args, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitRuntime, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "gateway_prerequisite/swarm_tool_gateway_url_stale") || !strings.Contains(stdout.String(), "must target the MCP listener port "+mcpPort) {
		t.Fatalf("stale gateway env not reported:\n%s", stdout.String())
	}
}

func TestRunServeRuntimeConsumesLocalClaudePreflightBeforeStoreSelection(t *testing.T) {
	configureDoctorDockerStub(t)
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	var out bytes.Buffer
	code := runServeRuntime(context.Background(), repoRoot(), serveOptions{
		ConfigPath:         writeDoctorClaudeConfig(t),
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
	if code != cliExitRuntime {
		t.Fatalf("runServeRuntime code = %d, want %d\noutput:\n%s", code, cliExitRuntime, out.String())
	}
	if !strings.Contains(out.String(), "local_preflight") || !strings.Contains(out.String(), "missing_backend_credential") {
		t.Fatalf("serve output missing shared local preflight failure:\n%s", out.String())
	}
	if strings.Contains(out.String(), "not-a-store") || strings.Contains(out.String(), "db_connection") {
		t.Fatalf("serve reached store selection instead of failing preflight first:\n%s", out.String())
	}
}

func TestPlatformSpecLocalClaudeCLIPreflightAdmissionPromoted(t *testing.T) {
	var spec struct {
		CLISpecification struct {
			Foundations struct {
				Preflight struct {
					PromotedBy           string   `yaml:"promoted_by"`
					ImplementationStatus string   `yaml:"implementation_status"`
					CanonicalOwner       string   `yaml:"canonical_owner"`
					ImplementationOwner  string   `yaml:"implementation_owner"`
					Scope                string   `yaml:"scope"`
					FindingCategories    []string `yaml:"finding_categories"`
					CommandModeRule      string   `yaml:"command_mode_rule"`
					OwnerConsumptionRule string   `yaml:"owner_consumption_rule"`
					SplitTail            []string `yaml:"split_tail"`
				} `yaml:"local_claude_cli_preflight_admission"`
			} `yaml:"foundations"`
			CommandCatalog struct {
				Doctor struct {
					Command              string   `yaml:"command"`
					ImplementationStatus string   `yaml:"implementation_status"`
					Owner                string   `yaml:"owner"`
					Behavior             string   `yaml:"behavior"`
					SplitScope           []string `yaml:"split_scope"`
				} `yaml:"doctor"`
			} `yaml:"command_catalog"`
		} `yaml:"cli_specification"`
	}
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
	preflight := spec.CLISpecification.Foundations.Preflight
	if preflight.PromotedBy != "#1565" || preflight.ImplementationStatus != "implemented" || !strings.Contains(preflight.CanonicalOwner, "local_claude_cli_preflight_admission") {
		t.Fatalf("preflight spec = %#v", preflight)
	}
	for _, want := range []string{"backend_prerequisite", "workspace_prerequisite", "serve_listener_prerequisite", "gateway_prerequisite", "contract_secret_prerequisite"} {
		if !stringSliceContains(preflight.FindingCategories, want) {
			t.Fatalf("preflight categories missing %q: %#v", want, preflight.FindingCategories)
		}
	}
	for _, want := range []string{"swarm doctor", "swarm serve --dev", "swarm run --backend claude_cli"} {
		if !strings.Contains(preflight.CommandModeRule, want) && !strings.Contains(preflight.Scope, want) {
			t.Fatalf("preflight spec missing consumer %q:\n%#v", want, preflight)
		}
	}
	for _, want := range []string{"llm_provider_selection_config_authority", "tool_model.credential_store", "workspace lifecycle", "serve startup/listener"} {
		if !strings.Contains(preflight.OwnerConsumptionRule, want) {
			t.Fatalf("owner consumption rule missing %q:\n%s", want, preflight.OwnerConsumptionRule)
		}
	}
	doctor := spec.CLISpecification.CommandCatalog.Doctor
	if doctor.Command != "swarm doctor --backend claude_cli [--contracts <path>] [--json]" || doctor.ImplementationStatus != "implemented" || doctor.Owner != "cli_specification.foundations.local_claude_cli_preflight_admission" {
		t.Fatalf("doctor command catalog = %#v", doctor)
	}
	if !strings.Contains(doctor.Behavior, "without starting runtime") || !strings.Contains(doctor.Behavior, "DB state") {
		t.Fatalf("doctor behavior missing runtime/DB boundary: %s", doctor.Behavior)
	}
}

func doctorClaudeArgs(t *testing.T, configPath string, asJSON bool) []string {
	t.Helper()
	args := []string{
		"doctor",
		"--backend", "claude_cli",
		"--config", configPath,
		"--contracts", doctorAgentContractsPath,
		"--platform-spec", defaultPlatformSpecPath,
		"--data", t.TempDir(),
		"--api-listen-addr", "127.0.0.1:0",
		"--mcp-listen-addr", "127.0.0.1:0",
	}
	if asJSON {
		args = append(args, "--json")
	}
	return args
}

const doctorAgentContractsPath = "tests/tier8-boot-verification/test-boot-prompt-stub"

func writeDoctorClaudeConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "claude.yaml")
	writeRuntimeConfigText(t, path, strings.Join([]string{
		"runtime:",
		"  recovery_on_startup: false",
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
		"    retries: 1",
		"    no_session_persistence: false",
	}, "\n")+"\n")
	return path
}

func writeDoctorAgentFreeContractsFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: agent-free-doctor
version: "1.0.0"
platform_version: ">=1.0.0"
flows: []
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: agent-free-doctor
initial_state: idle
states:
  - idle
terminal_states:
  - idle
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	return root
}

func configureDoctorDockerStub(t *testing.T) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "docker")
	script := `#!/bin/sh
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
	t.Setenv("SWARM_DOCKER_BIN", path)
	t.Setenv("SWARM_WORKSPACE_IMAGE", "doctor-test-image:latest")
	t.Setenv("SWARM_WORKSPACE_VOLUMES_FROM", "")
}

func freeDoctorTCPPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for free port: %v", err)
	}
	defer listener.Close()
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split free port: %v", err)
	}
	return port
}

func localPreflightReportHasCode(report localPreflightReport, code string) bool {
	for _, finding := range report.Findings {
		if finding.Code == code {
			return true
		}
	}
	return false
}
