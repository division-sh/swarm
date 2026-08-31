package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func unsetStoreSelectorEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"SWARM_STORE_BACKEND", "SWARM_SQLITE_PATH"} {
		previous, ok := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
		t.Cleanup(func() {
			if ok {
				_ = os.Setenv(key, previous)
				return
			}
			_ = os.Unsetenv(key)
		})
	}
}

func setDoctorProviderSecret(t *testing.T, key, value string) {
	t.Helper()
	setDoctorProviderSecrets(t, map[string]string{key: value})
}

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

func setDoctorManagedCredentials(t *testing.T, records ...runtimemanagedcredentials.Record) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "managed-credentials.json")
	t.Setenv("SWARM_MANAGED_CREDENTIALS_FILE", path)
	store, err := runtimemanagedcredentials.NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore managed credentials: %v", err)
	}
	for _, record := range records {
		if err := store.Put(context.Background(), record); err != nil {
			t.Fatalf("Put managed credential: %v", err)
		}
	}
}

func TestDoctorSchemaInventoryOwnsTypedHumanAndJSONReadback(t *testing.T) {
	dockerBin := configureDoctorDockerStub(t)
	setDoctorEmptyProviderSecrets(t)
	configPath := writeDoctorClaudeConfig(t, dockerBin)

	var humanOut, humanErr bytes.Buffer
	humanArgs := append(doctorClaudeArgs(t, configPath, false), "--schema-inventory")
	humanCode := executeRootCommandWithOptions(context.Background(), t.TempDir(), humanArgs, &humanOut, &humanErr, defaultRootCommandOptions())
	if humanCode != 0 {
		t.Fatalf("human doctor code = %d, want success\nstdout=%s\nstderr=%s", humanCode, humanOut.String(), humanErr.String())
	}
	for _, want := range []string{"schema inventory:", " tables · ", "  events · ", " columns"} {
		if !strings.Contains(humanOut.String(), want) {
			t.Fatalf("human schema inventory missing %q:\n%s", want, humanOut.String())
		}
	}

	var jsonOut, jsonErr bytes.Buffer
	jsonArgs := append(doctorClaudeArgs(t, configPath, true), "--schema-inventory")
	jsonCode := executeRootCommandWithOptions(context.Background(), t.TempDir(), jsonArgs, &jsonOut, &jsonErr, defaultRootCommandOptions())
	if jsonCode != 0 {
		t.Fatalf("json doctor code = %d, want success\nstdout=%s\nstderr=%s", jsonCode, jsonOut.String(), jsonErr.String())
	}
	var report LocalPreflightReport
	if err := json.Unmarshal(jsonOut.Bytes(), &report); err != nil {
		t.Fatalf("parse doctor schema inventory JSON: %v\n%s", err, jsonOut.String())
	}
	if report.SchemaInventory == nil || report.SchemaInventory.Owner != doctorSchemaInventoryOwner {
		t.Fatalf("schema inventory owner = %#v", report.SchemaInventory)
	}
	if report.SchemaInventory.TableCount == 0 || report.SchemaInventory.ColumnCount == 0 || len(report.SchemaInventory.Tables) != report.SchemaInventory.TableCount {
		t.Fatalf("schema inventory counts = %#v", report.SchemaInventory)
	}
	for i := 1; i < len(report.SchemaInventory.Tables); i++ {
		if report.SchemaInventory.Tables[i-1].Name >= report.SchemaInventory.Tables[i].Name {
			t.Fatalf("schema inventory is not sorted: %#v", report.SchemaInventory.Tables)
		}
	}
	if strings.Contains(humanOut.String(), "events(") || strings.Contains(jsonOut.String(), "events(") {
		t.Fatalf("doctor retained retired table(count) compatibility rendering\nhuman=%s\njson=%s", humanOut.String(), jsonOut.String())
	}
}

func TestDoctorIsSourceFreeAcrossHostileInvocationTrees(t *testing.T) {
	setDoctorEmptyProviderSecrets(t)
	configPath := writeDoctorClaudeConfig(t, "")
	left := t.TempDir()
	right := t.TempDir()
	if err := os.WriteFile(filepath.Join(left, "package.yaml"), []byte("not: valid: yaml\n"), 0o600); err != nil {
		t.Fatalf("write retired source canary: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(left, ".swarm"), 0o755); err != nil {
		t.Fatalf("mkdir excluded source canary: %v", err)
	}
	if err := os.WriteFile(filepath.Join(left, ".swarm", "pack-selection.yaml"), []byte("invalid"), 0o600); err != nil {
		t.Fatalf("write excluded source canary: %v", err)
	}

	run := func(root string) LocalPreflightReport {
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), root, doctorClaudeArgs(t, configPath, true), &stdout, &stderr, defaultRootCommandOptions())
		if code != 0 {
			t.Fatalf("doctor root %s code=%d stdout=%s stderr=%s", root, code, stdout.String(), stderr.String())
		}
		var report LocalPreflightReport
		if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
			t.Fatalf("parse doctor JSON: %v\n%s", err, stdout.String())
		}
		return report
	}

	leftReport := run(left)
	rightReport := run(right)
	if !reflect.DeepEqual(leftReport, rightReport) {
		t.Fatalf("doctor result depends on invocation-tree source bytes:\nleft=%#v\nright=%#v", leftReport, rightReport)
	}
	for _, forbidden := range []string{"contract_source_load_failed", "workspace_source_valid", "agent_free_source"} {
		if localPreflightReportHasCode(leftReport, forbidden) {
			t.Fatalf("source-free doctor emitted source finding %q: %#v", forbidden, leftReport.Findings)
		}
	}
}

func TestDoctorSchemaInventoryRejectsTargetMode(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"doctor", "--target", "--schema-inventory"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != CLIExitValidation || !strings.Contains(stderr.String(), "--schema-inventory cannot be combined with --target") {
		t.Fatalf("doctor target/schema inventory code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
}

func TestDoctorClaudeCLIPreflightReportsRetiredBackendEnv(t *testing.T) {
	t.Setenv("SWARM_LLM_BACKEND", "cli_test")
	setDoctorProviderSecret(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), doctorAgentSourceRoot(), doctorClaudeArgs(t, writeDoctorClaudeConfig(t, ""), false), &stdout, &stderr, defaultRootCommandOptions())
	if code != CLIExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitRuntime, stdout.String(), stderr.String())
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
	dockerBin := configureDoctorDockerStub(t)
	setDoctorProviderSecret(t, "CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "operator-token")

	mcpPort := freeDoctorTCPPort(t)
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:"+mcpPort)
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:"+mcpPort)

	args := doctorClaudeArgs(t, writeDoctorClaudeConfig(t, dockerBin), false)
	args = append(args[:len(args)-4], "--api-listen-addr", "127.0.0.1:0", "--mcp-listen-addr", "127.0.0.1:"+mcpPort)
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), doctorAgentSourceRoot(), args, &stdout, &stderr, defaultRootCommandOptions())
	if code != CLIExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitRuntime, stdout.String(), stderr.String())
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
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitOK {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitOK, stdout.String(), stderr.String())
	}
	for _, want := range []string{
		"swarm target diagnostics: ok",
		"swarm_dir: " + flagSwarmDir + " (source: --swarm-dir)",
		"project_root: source_free",
		"api_server: http://127.0.0.1:19001 (source: config connection.api_server)",
		"descriptor_registry: empty (" + localContextRegistryOwner,
		"runtime_identity: unavailable (platform-spec.yaml#api_specification.method_catalog.runtime.identity)",
		"store_path: " + filepath.Join(flagSwarmDir, "stores", "default", "dev.db"),
		"data_dir: runtime_projected",
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
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != CLIExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitRuntime, stdout.String(), stderr.String())
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

func TestDoctorTargetConsumesRuntimeConfigStoreWithoutAmbientData(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	unsetStoreSelectorEnv(t)
	repo := writeDoctorTargetRepo(t)
	sqlitePath := filepath.Join(t.TempDir(), "configured-dev.db")
	configPath := writeDoctorTargetRuntimeConfig(t, `
store:
  backend: sqlite
  sqlite:
    path: `+sqlitePath+`
`)

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"doctor", "--target", "--json",
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
	if report.Data.Path != "" || report.Data.Source != "platform-spec.yaml#durable_data_resources.workspace_projection" || report.Data.Status != "runtime_projected" {
		t.Fatalf("data resolution = %#v, want selected-run data projection", report.Data)
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
	if code != CLIExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
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
	if code != CLIExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "--api-server and --api-token-file require --target") {
		t.Fatalf("stderr = %q, want target-only API flag validation", stderr.String())
	}
}

func TestPlatformSpecLocalClaudeCLIPreflightAdmissionPromoted(t *testing.T) {
	var spec struct {
		CLISpecification struct {
			Foundations struct {
				Preflight struct {
					PromotedBy              string   `yaml:"promoted_by"`
					ImplementationStatus    string   `yaml:"implementation_status"`
					CanonicalOwner          string   `yaml:"canonical_owner"`
					ImplementationOwner     string   `yaml:"implementation_owner"`
					Scope                   string   `yaml:"scope"`
					FindingCategories       []string `yaml:"finding_categories"`
					CommandModeRule         string   `yaml:"command_mode_rule"`
					OwnerConsumptionRule    string   `yaml:"owner_consumption_rule"`
					DependencyReportingRule string   `yaml:"dependency_reporting_rule"`
					SplitTail               []string `yaml:"split_tail"`
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
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
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
	for _, want := range []string{"complete typed blocking finding", "including remediation", "error output", "Direct non-dev serve", "log-only cause is not a substitute"} {
		if !strings.Contains(preflight.CommandModeRule, want) {
			t.Fatalf("preflight command-mode rule missing %q:\n%s", want, preflight.CommandModeRule)
		}
	}
	for _, want := range []string{"llm_provider_selection_config_authority", "tool_model.credential_store", "workspace lifecycle", "serve startup/listener"} {
		if !strings.Contains(preflight.OwnerConsumptionRule, want) {
			t.Fatalf("owner consumption rule missing %q:\n%s", want, preflight.OwnerConsumptionRule)
		}
	}
	for _, want := range []string{"Docker daemon probe fails", "`skipped`/not measured", "MUST NOT emit derivative blockers", "configured-image probe", "Text and JSON"} {
		if !strings.Contains(preflight.DependencyReportingRule, want) {
			t.Fatalf("dependency reporting rule missing %q:\n%s", want, preflight.DependencyReportingRule)
		}
	}
	doctor := spec.CLISpecification.CommandCatalog.Doctor
	if doctor.Command != "swarm doctor [--backend claude_cli] [--target] [--schema-inventory] [--json]" || doctor.ImplementationStatus != "implemented" || !strings.Contains(doctor.Owner, "local_claude_cli_preflight_admission") {
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
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), defaultPlatformSpecPath), &spec)
	target := spec.CLISpecification.Foundations.LocalTarget
	if target.PromotedBy != "#1612" || target.ImplementationStatus != "implemented_first_slice" || target.CanonicalOwner != localTargetOwner {
		t.Fatalf("local target owner = %#v", target)
	}
	wantSwarmDirOrder := []string{"--swarm-dir", "config paths.swarm_dir", "default ~/.swarm"}
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
		"--api-listen-addr", "127.0.0.1:0",
		"--mcp-listen-addr", "127.0.0.1:0",
	}
	if asJSON {
		args = append(args, "--json")
	}
	return args
}

const doctorAgentContractsPath = "tests/tier8-boot-verification/test-boot-prompt-stub"

func doctorAgentSourceRoot() string {
	return filepath.Join(RepoRoot(), doctorAgentContractsPath)
}

func writeDoctorClaudeConfig(t *testing.T, dockerBin string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "claude.yaml")
	storePath := filepath.Join(t.TempDir(), "runtime.db")
	workspace := []string{
		"workspace:",
		"  image: doctor-test-image:latest",
	}
	if strings.TrimSpace(dockerBin) != "" {
		workspace = append(workspace, fmt.Sprintf("  docker_bin: %q", dockerBin))
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
	}, "\n")+"\n")
	return path
}

func writeDoctorClaudeHostConfig(t *testing.T, dockerBin string) string {
	t.Helper()
	configPath := writeDoctorClaudeConfig(t, dockerBin)
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read doctor config: %v", err)
	}
	writeRuntimeConfigText(t, configPath, strings.Replace(string(raw), "workspace:\n", "workspace:\n  backend: host\n", 1))
	return configPath
}

func writeDoctorTargetRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	contracts := filepath.Join(repo, "contracts")
	if err := os.MkdirAll(contracts, 0o755); err != nil {
		t.Fatalf("mkdir contracts: %v", err)
	}
	if err := os.WriteFile(filepath.Join(contracts, "schema.yaml"), []byte("name: target-fixture\nversion: 0.0.1\n"), 0o644); err != nil {
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
	}, "\n")+"\n")
	return path
}

func writeDoctorAgentFreeContractsFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: agent-free-doctor
initial_state: idle
states:
  - idle
terminal_states:
  - idle
`)
	return root
}

type doctorMockExecutionFixtureOptions struct {
	IncludeUnmocked bool
	NativeBash      bool
	ExecTool        bool
}

func writeDoctorMockExecutionFixture(t *testing.T, options doctorMockExecutionFixtureOptions) string {
	t.Helper()
	root := t.TempDir()
	if err := os.CopyFS(root, os.DirFS(filepath.Join(RepoRoot(), doctorAgentContractsPath))); err != nil {
		t.Fatalf("copy doctor agent contracts: %v", err)
	}
	agentLines := []string{
		"stub-agent:",
		"  model: regular",
		"  intent:",
		"    inline: Complete the requested doctor fixture task.",
		"  subscriptions:",
		"    - task.requested",
		"  emit_events:",
		"    - task.completed",
		"  mock:",
		"    kind: python",
		"    module: mocks/stub-agent.py",
	}
	if options.NativeBash {
		agentLines = append(agentLines, "  native_tools:", "    bash: true")
	}
	if options.ExecTool {
		agentLines = append(agentLines, "  tools:", "    - shell")
		writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `
shell:
  description: execute a shell command
  input_schema:
    type: object
  output_schema:
    type: object
`)
	}
	if options.IncludeUnmocked {
		agentLines = append(agentLines,
			"live-agent:",
			"  model: regular",
			"  intent:",
			"    inline: Complete the requested live doctor fixture task.",
			"  subscriptions:",
			"    - task.requested",
			"  emit_events:",
			"    - task.completed",
		)
	}
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), strings.Join(agentLines, "\n")+"\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "mocks", "stub-agent.py"), "def handle(input):\n    return {'text': 'ok'}\n")
	return root
}

func runDoctorPreflightJSON(t *testing.T, configPath, sourceRoot, backend string) (LocalPreflightReport, int, string) {
	t.Helper()
	args := doctorClaudeArgs(t, configPath, true)
	for index := 0; index+1 < len(args); index++ {
		switch args[index] {
		case "--backend":
			args[index+1] = backend
		}
	}
	var stdout, stderr bytes.Buffer
	if !filepath.IsAbs(sourceRoot) {
		sourceRoot = filepath.Join(RepoRoot(), sourceRoot)
	}
	code := executeRootCommandWithOptions(context.Background(), sourceRoot, args, &stdout, &stderr, defaultRootCommandOptions())
	var report LocalPreflightReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("parse doctor preflight JSON: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
	}
	return report, code, stderr.String()
}

func writeDoctorTelegramConnectorContractsFixture(t *testing.T) string {
	t.Helper()
	root := writeDoctorAgentFreeContractsFixture(t)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `
telegram.send_message:
  category: provider_connector
  description: send Telegram messages
  handler_type: http
  effect_class: non_idempotent_write
  credentials:
    - telegram_bot_token
  input_schema:
    type: object
    required: [chat_id, text]
    properties:
      chat_id:
        type: string
      text:
        type: string
  output_schema:
    type: object
  response_success:
    kind: http_status_2xx
  http:
    method: POST
    url: https://api.telegram.org/bot{{credentials.telegram_bot_token}}/sendMessage
    body:
      chat_id: "{{input.chat_id}}"
      text: "{{input.text}}"
`)
	return root
}

func writeDoctorSlackConnectorPackContractsFixture(t *testing.T) string {
	t.Helper()
	root := writeDoctorAgentFreeContractsFixture(t)

	return root
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

func assertLocalPreflightFindingState(t *testing.T, report LocalPreflightReport, code string, status LocalPreflightFindingStatus, severity LocalPreflightSeverity) {
	t.Helper()
	finding, ok := localPreflightReportFinding(report, code)
	if !ok {
		t.Fatalf("report missing finding %q: %#v", code, report.Findings)
	}
	if finding.Status != status || finding.Severity != severity {
		t.Fatalf("finding %q = %#v, want status=%s severity=%s", code, finding, status, severity)
	}
}

func assertDoctorDockerCalls(t *testing.T, path string, required, forbidden []string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Docker call log: %v", err)
	}
	calls := string(raw)
	for _, want := range required {
		if !strings.Contains(calls, want) {
			t.Fatalf("Docker calls missing %q:\n%s", want, calls)
		}
	}
	for _, forbiddenCall := range forbidden {
		if strings.Contains(calls, forbiddenCall) {
			t.Fatalf("Docker calls include dependent probe %q after upstream failure:\n%s", forbiddenCall, calls)
		}
	}
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

func localPreflightReportHasCode(report LocalPreflightReport, code string) bool {
	for _, finding := range report.Findings {
		if finding.Code == code {
			return true
		}
	}
	return false
}

func localPreflightReportFindingContains(report LocalPreflightReport, code, want string) bool {
	for _, finding := range report.Findings {
		if finding.Code == code && strings.Contains(finding.Message, want) {
			return true
		}
	}
	return false
}

func localPreflightReportFinding(report LocalPreflightReport, code string) (localPreflightFinding, bool) {
	for _, finding := range report.Findings {
		if finding.Code == code {
			return finding, true
		}
	}
	return localPreflightFinding{}, false
}

func stringSliceContainsPrefix(values []string, prefix string) bool {
	for _, value := range values {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}
