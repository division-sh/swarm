package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"gopkg.in/yaml.v3"
)

func setDoctorProviderSecret(t *testing.T, key, value string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "provider-credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", path)
	store, err := runtimecredentials.NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(context.Background(), key, value); err != nil {
		t.Fatalf("Set provider credential: %v", err)
	}
}

func TestDoctorClaudeCLIPreflightReportsMissingPrerequisites(t *testing.T) {
	configureDoctorDockerStub(t)
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "provider-credentials.json"))
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
		"swarm secrets set CLAUDE_CODE_OAUTH_TOKEN",
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
	setDoctorProviderSecret(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
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
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "stale-oauth-token")
	setDoctorProviderSecret(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

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
	for _, want := range []string{
		"provider_trigger_pack_github",
		"provider_trigger_pack_intercom",
		"provider_trigger_pack_shopify",
		"provider_trigger_pack_slack",
		"provider_trigger_pack_stripe",
		"provider_trigger_pack_telegram",
		"provider_trigger_pack_twilio",
		"provider_trigger_pack_typeform",
	} {
		if !localPreflightReportHasCode(report, want) {
			t.Fatalf("report missing provider pack finding %q: %#v", want, report.Findings)
		}
	}
	if !localPreflightReportFindingContains(report, "provider_trigger_pack_github", "CAN receive HTTPS route /webhooks/{entity}/github") ||
		!localPreflightReportFindingContains(report, "provider_trigger_pack_github", "CANNOT emit_undeclared_events") ||
		!localPreflightReportFindingContains(report, "provider_trigger_pack_github", "requires webhook_signing.github=UNBOUND") {
		t.Fatalf("github provider pack surface missing CAN/CANNOT/requires readback: %#v", report.Findings)
	}
	if !localPreflightReportFindingContains(report, "provider_trigger_pack_stripe", "CAN receive HTTPS route /webhooks/{entity}/stripe") ||
		!localPreflightReportFindingContains(report, "provider_trigger_pack_stripe", "CANNOT emit_undeclared_events") ||
		!localPreflightReportFindingContains(report, "provider_trigger_pack_stripe", "requires webhook_signing.stripe=UNBOUND") {
		t.Fatalf("stripe provider pack surface missing CAN/CANNOT/requires readback: %#v", report.Findings)
	}
	if !localPreflightReportFindingContains(report, "provider_trigger_pack_telegram", "CAN receive HTTPS route /webhooks/{entity}/telegram") ||
		!localPreflightReportFindingContains(report, "provider_trigger_pack_telegram", "CANNOT emit_undeclared_events") ||
		!localPreflightReportFindingContains(report, "provider_trigger_pack_telegram", "requires webhook_signing.telegram=UNBOUND") {
		t.Fatalf("telegram provider pack surface missing CAN/CANNOT/requires readback: %#v", report.Findings)
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
	setDoctorProviderSecret(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
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

func TestDoctorClaudeCLIPreflightReportsBrokenWorkspaceCLI(t *testing.T) {
	configureDoctorDockerStub(t)
	setDoctorProviderSecret(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
	t.Setenv("SWARM_TEST_DOCKER_CLI_BROKEN", "1")
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
		"cannot execute in workspace image",
		"claude launcher failed after command lookup",
		"runnable Claude CLI",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("doctor output missing %q:\n%s", want, stdout.String())
		}
	}
}

func TestDoctorClaudeCLIPreflightUsesCredentialStoreForContractSecrets(t *testing.T) {
	setDoctorProviderSecret(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
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
	setDoctorProviderSecret(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")

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

func TestDoctorClaudeCLIPreflightBlocksGeneratedGatewayParentEnv(t *testing.T) {
	configureDoctorDockerStub(t)
	setDoctorProviderSecret(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "operator-token")

	mcpPort := freeDoctorTCPPort(t)
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:"+mcpPort)
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:"+mcpPort)

	args := doctorClaudeArgs(t, writeDoctorClaudeConfig(t), false)
	args = append(args[:len(args)-4], "--api-listen-addr", "127.0.0.1:0", "--mcp-listen-addr", "127.0.0.1:"+mcpPort)
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), args, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitRuntime, stdout.String(), stderr.String())
	}
	for _, want := range []string{
		"[BLOCKER] env/generated_boundary @ doctor: SWARM_TOOL_GATEWAY_URL",
		"[BLOCKER] env/generated_boundary @ doctor: SWARM_TOOL_GATEWAY_CONTAINER_URL",
		"[BLOCKER] env/known_retired @ doctor: SWARM_TOOL_GATEWAY_TOKEN",
		"[WARN] gateway_prerequisite/swarm_tool_gateway_url_retired @ doctor:",
		"[WARN] gateway_prerequisite/swarm_tool_gateway_container_url_retired @ doctor:",
		"SWARM_TOOL_GATEWAY_URL is retired and not accepted as gateway endpoint configuration",
		"unset SWARM_TOOL_GATEWAY_URL; local serve/run derives the gateway binding from the bound MCP listener and ignores this retired URL",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("doctor output missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.Contains(stdout.String(), "shadowed_or_empty") || strings.Contains(stdout.String(), "must target the MCP listener port") {
		t.Fatalf("doctor output still renders retired URL env through old acceptance model:\n%s", stdout.String())
	}
}

func TestDoctorTargetHumanExplainsResolutionWithoutPreflight(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := writeDoctorTargetRepo(t)
	flagSwarmDir := filepath.Join(t.TempDir(), "flag-state")
	configSwarmDir := filepath.Join(t.TempDir(), "config-state")
	t.Setenv("SWARM_CONFIG", writeCLIAPIConfigFile(t, map[string]string{
		"swarm_dir":  configSwarmDir,
		"api_server": "http://127.0.0.1:19001",
	}))

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"--swarm-dir", flagSwarmDir,
		"doctor", "--target",
		"--contracts", filepath.Join(repo, "contracts"),
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitOK {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitOK, stdout.String(), stderr.String())
	}
	canonicalRepo, _ := canonicalizeDoctorTargetPath(repo)
	for _, want := range []string{
		"swarm target diagnostics: ok",
		"swarm_dir: " + flagSwarmDir + " (source: --swarm-dir)",
		"project_root: " + repo,
		"api_server: http://127.0.0.1:19001 (source: config api_server)",
		"descriptor_registry: empty (" + localContextRegistryOwner,
		"runtime_identity: unavailable (platform-spec.yaml#api_specification.method_catalog.runtime.identity)",
		"store_path: " + filepath.Join(canonicalRepo, ".swarm", "stores", "dev.db"),
		"data_dir: " + filepath.Join(canonicalRepo, ".swarm", "data"),
		"command_classes:",
		"read_only_inspection: implemented",
		"store/data migration and swarm run start semantics are implemented",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("doctor target output missing %q:\n%s", want, stdout.String())
		}
	}
	if strings.Contains(stdout.String(), "claude_cli preflight") {
		t.Fatalf("doctor target ran backend preflight:\n%s", stdout.String())
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestDoctorTargetJSONPreservesScriptableOutput(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := writeDoctorTargetRepo(t)
	tokenFile := writeCLIAPITokenFile(t, "target-token")
	apiServer := "http://127.0.0.1:19002"

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"doctor", "--target", "--json",
		"--api-server", apiServer,
		"--api-token-file", tokenFile,
		"--contracts", filepath.Join(repo, "contracts"),
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitOK {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitOK, stdout.String(), stderr.String())
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	var report doctorTargetReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("parse target json: %v\n%s", err, stdout.String())
	}
	if !report.OK || report.Owner != localTargetOwner || report.Mode != "target" {
		t.Fatalf("report identity = %#v", report)
	}
	if report.API.Server != apiServer || report.API.Source != "--api-server" || report.API.Auth.Source != "--api-token-file" {
		t.Fatalf("api resolution = %#v", report.API)
	}
	if report.Context.Registry.Status != "empty" || report.RuntimeIdentity.Status != "unavailable" {
		t.Fatalf("registry should be empty and runtime identity unavailable, report = %#v", report)
	}
	if len(report.CommandClasses) == 0 || len(report.SplitSiblings) == 0 {
		t.Fatalf("report missing command classes or split siblings: %#v", report)
	}
}

func TestDoctorTargetJSONReportsMissingAuthWithoutAborting(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := writeDoctorTargetRepo(t)
	apiServer := "http://192.0.2.10:8081"

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"doctor", "--target", "--json",
		"--api-server", apiServer,
		"--contracts", filepath.Join(repo, "contracts"),
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitOK {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitOK, stdout.String(), stderr.String())
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	var report doctorTargetReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("parse target json: %v\n%s", err, stdout.String())
	}
	if report.API.Server != apiServer || report.API.Source != "--api-server" {
		t.Fatalf("api target = %#v, want %q from --api-server", report.API, apiServer)
	}
	if report.API.Auth.Source != "none" || report.API.Auth.Status != "missing_explicit_token" {
		t.Fatalf("api auth = %#v, want structured missing token diagnostic", report.API.Auth)
	}
}

func TestDoctorTargetReportsRemovedAPIClientEnv(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := writeDoctorTargetRepo(t)
	t.Setenv("SWARM_API_SERVER", "http://127.0.0.1:19002")
	t.Setenv("SWARM_API_TOKEN", "env-token")
	t.Setenv("SWARM_API_TOKEN_FILE", writeCLIAPITokenFile(t, "env-file-token"))

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"doctor", "--target", "--json",
		"--contracts", filepath.Join(repo, "contracts"),
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitRuntime, stdout.String(), stderr.String())
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	var report doctorTargetReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("parse target json: %v\n%s", err, stdout.String())
	}
	if report.OK {
		t.Fatalf("report OK=true, want env blockers: %#v", report)
	}
	assertDoctorTargetEnvFinding(t, report, string(swarmEnvCategoryKnownRetired), "SWARM_API_SERVER")
	assertDoctorTargetEnvFinding(t, report, string(swarmEnvCategoryKnownRetired), "SWARM_API_TOKEN")
	assertDoctorTargetEnvFinding(t, report, string(swarmEnvCategoryKnownRetired), "SWARM_API_TOKEN_FILE")
}

func TestDoctorTargetUsesResolvedSwarmDirForExplicitContext(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := writeDoctorTargetRepo(t)
	swarmDir := filepath.Join(t.TempDir(), "state")
	server := startCLIAPIRuntimeIdentityServer(t, "runtime-target")
	registry := newLocalContextRegistry(swarmDir)
	writeCLIAPITestContext(t, registry, "target", "runtime-target", server.URL, "")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"--swarm-dir", swarmDir,
		"doctor", "--target", "--json",
		"--context", "target",
		"--contracts", filepath.Join(repo, "contracts"),
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitOK {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitOK, stdout.String(), stderr.String())
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	var report doctorTargetReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("parse target json: %v\n%s", err, stdout.String())
	}
	if report.SwarmDir.Path != swarmDir || report.SwarmDir.Source != "--swarm-dir" {
		t.Fatalf("swarm dir = %#v, want %q from --swarm-dir", report.SwarmDir, swarmDir)
	}
	if report.API.Server != server.URL || report.API.Source != "--context" {
		t.Fatalf("api target = %#v, want explicit context from resolved swarm-dir", report.API)
	}
	if report.API.Auth.Source != "context descriptor "+localContextAuthBuiltinLoopback || report.API.Auth.Status != "configured" {
		t.Fatalf("api auth = %#v, want context descriptor auth", report.API.Auth)
	}
}

func TestDoctorTargetConsumesRuntimeConfigStoreAndData(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	unsetStoreSelectorEnv(t)
	repo := writeDoctorTargetRepo(t)
	sqlitePath := filepath.Join(t.TempDir(), "configured-dev.db")
	dataDir := filepath.Join(t.TempDir(), "configured-data")
	configPath := writeDoctorTargetRuntimeConfig(t, `
store:
  backend: sqlite
  sqlite:
    path: `+sqlitePath+`
workspace:
  data_source: `+dataDir+`
`)

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"doctor", "--target", "--json",
		"--contracts", filepath.Join(repo, "contracts"),
		"--config", configPath,
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitOK {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitOK, stdout.String(), stderr.String())
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	var report doctorTargetReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("parse target json: %v\n%s", err, stdout.String())
	}
	if report.Store.Path != sqlitePath || report.Store.Source != string(storebackend.SourceRuntimeConfig) || report.Store.Status != "resolved" {
		t.Fatalf("store resolution = %#v, want configured sqlite path from runtime config", report.Store)
	}
	if report.Data.Path != dataDir || report.Data.Source != "workspace.data_source" || report.Data.Status != "resolved" {
		t.Fatalf("data resolution = %#v, want configured workspace data source", report.Data)
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("configured target data stat error = %v, want dry-run without directory creation", err)
	}
}

func TestDoctorTargetConsumesRuntimeConfigPostgresStore(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := writeDoctorTargetRepo(t)
	configPath := writeDoctorTargetRuntimeConfig(t, `
store:
  backend: postgres
database:
  password_file: /run/secrets/db-password
`)

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"doctor", "--target", "--json",
		"--contracts", filepath.Join(repo, "contracts"),
		"--config", configPath,
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitOK {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitOK, stdout.String(), stderr.String())
	}
	var report doctorTargetReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("parse target json: %v\n%s", err, stdout.String())
	}
	if report.Store.Path != "" ||
		report.Store.Source != string(storebackend.SourceRuntimeConfig) ||
		report.Store.Status != "not_applicable" ||
		!strings.Contains(report.Store.Detail, "postgres runtime store selected") {
		t.Fatalf("store resolution = %#v, want postgres runtime-config diagnostic", report.Store)
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestDoctorTargetAPIFlagsAfterRootSwarmDir(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := writeDoctorTargetRepo(t)
	swarmDir := filepath.Join(t.TempDir(), "state")
	apiServer := "http://127.0.0.1:19004"

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"--swarm-dir", swarmDir,
		"doctor", "--target", "--json",
		"--api-server", apiServer,
		"--contracts", filepath.Join(repo, "contracts"),
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitOK {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitOK, stdout.String(), stderr.String())
	}
	var report doctorTargetReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("parse target json: %v\n%s", err, stdout.String())
	}
	if report.SwarmDir.Path != swarmDir || report.SwarmDir.Source != "--swarm-dir" {
		t.Fatalf("swarm dir = %#v, want %q from --swarm-dir", report.SwarmDir, swarmDir)
	}
	if report.API.Server != apiServer || report.API.Source != "--api-server" {
		t.Fatalf("api target = %#v, want %q from --api-server", report.API, apiServer)
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestDoctorTargetQuietRemainsUnsupported(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"doctor", "--target", "--quiet"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "unknown flag: --quiet") {
		t.Fatalf("stderr = %q, want unsupported quiet flag", stderr.String())
	}
	if stdout.String() != "" {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

func TestDoctorAPIFlagsRequireTargetMode(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"doctor", "--api-server", "http://127.0.0.1:19003"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "--api-server and --api-token-file require --target") {
		t.Fatalf("stderr = %q, want target-only API flag validation", stderr.String())
	}
}

func TestRunServeRuntimeConsumesLocalClaudePreflightAfterBundleDecision(t *testing.T) {
	configureDoctorDockerStub(t)
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "provider-credentials.json"))
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
		"    external_dirs:",
		"      - packs/missing-provider",
	}, "\n")+"\n")

	var out bytes.Buffer
	code := runServeRuntime(context.Background(), repoRoot(), serveOptions{
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
		t.Fatalf("runServeRuntime code = %d, want config load failure\noutput:\n%s", code, out.String())
	}
	if !strings.Contains(out.String(), "config_load") || !strings.Contains(out.String(), filepath.Join(configDir, "packs", "missing-provider")) {
		t.Fatalf("serve output missing declared provider pack load failure:\n%s", out.String())
	}
	if strings.Contains(out.String(), "not-a-store") || strings.Contains(out.String(), "db_connection") {
		t.Fatalf("serve reached store selection instead of failing provider pack admission first:\n%s", out.String())
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
	for _, want := range []string{"swarm doctor", "swarm serve --dev", "swarm run start --backend claude_cli"} {
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
	if doctor.Command != "swarm doctor [--backend claude_cli] [--target] [--contracts <path>] [--json]" || doctor.ImplementationStatus != "implemented" || !strings.Contains(doctor.Owner, "local_claude_cli_preflight_admission") {
		t.Fatalf("doctor command catalog = %#v", doctor)
	}
	if !strings.Contains(doctor.Owner, "local_target_resolution_authority") {
		t.Fatalf("doctor command catalog missing target owner: %#v", doctor)
	}
	for _, want := range []string{"without starting runtime", "DB state", "--target", "shared typed diagnostic list renderer", "[BLOCKER]", "existing doctor/preflight report shape"} {
		if !strings.Contains(doctor.Behavior, want) {
			t.Fatalf("doctor behavior missing %q: %s", want, doctor.Behavior)
		}
	}
}

func TestPlatformSpecLocalTargetResolutionAuthorityPromoted(t *testing.T) {
	var spec struct {
		CLISpecification struct {
			Foundations struct {
				LocalTarget struct {
					PromotedBy           string `yaml:"promoted_by"`
					ImplementationStatus string `yaml:"implementation_status"`
					CanonicalOwner       string `yaml:"canonical_owner"`
					SwarmDir             struct {
						SourceOrder     []string          `yaml:"source_order"`
						RejectedSources map[string]string `yaml:"rejected_sources"`
					} `yaml:"swarm_dir"`
					TargetPrecedence struct {
						SourceOrder []string `yaml:"source_order"`
					} `yaml:"target_precedence"`
					CommandClasses map[string]struct {
						Status   string   `yaml:"status"`
						Commands []string `yaml:"commands"`
					} `yaml:"command_classes"`
					DoctorTargetSurface struct {
						Command  string `yaml:"command"`
						Behavior string `yaml:"behavior"`
					} `yaml:"doctor_target_surface"`
					SplitSiblings []string `yaml:"split_siblings"`
				} `yaml:"local_target_resolution_authority"`
				LocalContextRegistry struct {
					PromotedBy           string   `yaml:"promoted_by"`
					ImplementationStatus string   `yaml:"implementation_status"`
					CanonicalOwner       string   `yaml:"canonical_owner"`
					ValidationStatuses   []string `yaml:"validation_statuses"`
					LifecycleSurface     struct {
						Commands []string `yaml:"commands"`
					} `yaml:"lifecycle_surface"`
					ImplementationBoundaries []string `yaml:"implementation_boundaries"`
				} `yaml:"local_context_registry_authority"`
			} `yaml:"foundations"`
		} `yaml:"cli_specification"`
	}
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
	target := spec.CLISpecification.Foundations.LocalTarget
	if target.PromotedBy != "#1612" || target.ImplementationStatus != "implemented_first_slice" || target.CanonicalOwner != localTargetOwner {
		t.Fatalf("local target owner = %#v", target)
	}
	wantSwarmDirOrder := []string{"--swarm-dir", "config swarm_dir", "default ~/.swarm"}
	if !reflect.DeepEqual(target.SwarmDir.SourceOrder, wantSwarmDirOrder) {
		t.Fatalf("swarm_dir source order = %#v, want %#v", target.SwarmDir.SourceOrder, wantSwarmDirOrder)
	}
	for _, rejected := range []string{"--datadir", "SWARM_DIR", "SWARM_HOME", "<swarm-dir>/config.yaml"} {
		if _, ok := target.SwarmDir.RejectedSources[rejected]; !ok {
			t.Fatalf("swarm_dir rejected sources missing %q: %#v", rejected, target.SwarmDir.RejectedSources)
		}
	}
	for _, want := range []string{"explicit_api_flags", "live_project_scoped_context", "selected_or_default_global_context", "built_in_loopback_default"} {
		if !stringSliceContains(target.TargetPrecedence.SourceOrder, want) {
			t.Fatalf("target precedence missing %q: %#v", want, target.TargetPrecedence.SourceOrder)
		}
	}
	if stringSliceContains(target.TargetPrecedence.SourceOrder, "existing_api_environment") {
		t.Fatalf("target precedence still includes removed API environment source: %#v", target.TargetPrecedence.SourceOrder)
	}
	for _, class := range []string{"target_diagnostic", "read_only_inspection", "mutating_runtime_state", "control_destructive", "startup_and_run"} {
		if _, ok := target.CommandClasses[class]; !ok {
			t.Fatalf("command classes missing %q: %#v", class, target.CommandClasses)
		}
	}
	if target.CommandClasses["target_diagnostic"].Status != "implemented" || !stringSliceContains(target.CommandClasses["target_diagnostic"].Commands, "swarm doctor --target") {
		t.Fatalf("target diagnostic class = %#v", target.CommandClasses["target_diagnostic"])
	}
	if !strings.Contains(target.DoctorTargetSurface.Command, "swarm doctor --target") || !strings.Contains(target.DoctorTargetSurface.Behavior, "MUST NOT require backend preflight") {
		t.Fatalf("doctor target surface = %#v", target.DoctorTargetSurface)
	}
	for _, sibling := range []string{"#1614", "#1615", "#1576"} {
		if !stringSliceContainsPrefix(target.SplitSiblings, sibling) {
			t.Fatalf("split siblings missing %q: %#v", sibling, target.SplitSiblings)
		}
	}
	registry := spec.CLISpecification.Foundations.LocalContextRegistry
	if registry.PromotedBy != "#1613" || registry.ImplementationStatus != "implemented_primitive_registry" || registry.CanonicalOwner != localContextRegistryOwner {
		t.Fatalf("local context registry owner = %#v", registry)
	}
	for _, status := range []string{localContextStatusOK, localContextStatusNoServer, localContextStatusIdentityMismatch, localContextStatusUnsupportedTransport, localContextStatusAuthFailure, localContextStatusPermissionDenied, localContextStatusCorruptDescriptor} {
		if !stringSliceContains(registry.ValidationStatuses, status) {
			t.Fatalf("registry validation statuses missing %q: %#v", status, registry.ValidationStatuses)
		}
	}
	for _, command := range []string{"swarm context current", "swarm context list", "swarm context prune"} {
		if !stringSliceContains(registry.LifecycleSurface.Commands, command) {
			t.Fatalf("registry lifecycle commands missing %q: %#v", command, registry.LifecycleSurface.Commands)
		}
	}
	for _, sibling := range []string{"#1614", "#1615", "#1576"} {
		if !stringSliceContainsPrefix(registry.ImplementationBoundaries, "No") || !strings.Contains(strings.Join(registry.ImplementationBoundaries, "\n"), sibling) {
			t.Fatalf("registry boundaries missing %s: %#v", sibling, registry.ImplementationBoundaries)
		}
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
		"workspace:",
		"  data_source: " + t.TempDir(),
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

func writeDoctorTargetRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	contracts := filepath.Join(repo, "contracts")
	if err := os.MkdirAll(contracts, 0o755); err != nil {
		t.Fatalf("mkdir contracts: %v", err)
	}
	if err := os.WriteFile(filepath.Join(contracts, "package.yaml"), []byte("name: target-fixture\nversion: 0.0.1\n"), 0o644); err != nil {
		t.Fatalf("write package: %v", err)
	}
	return repo
}

func writeDoctorTargetRuntimeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runtime.yaml")
	writeRuntimeConfigText(t, path, strings.TrimSpace(body)+"\n"+strings.Join([]string{
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
platform_version: ">=0.7.0 <0.8.0"
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

func localPreflightReportFindingContains(report localPreflightReport, code, want string) bool {
	for _, finding := range report.Findings {
		if finding.Code == code && strings.Contains(finding.Message, want) {
			return true
		}
	}
	return false
}

func stringSliceContainsPrefix(values []string, prefix string) bool {
	for _, value := range values {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}
