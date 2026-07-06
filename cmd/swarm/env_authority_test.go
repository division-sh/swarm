package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestServeCommandIgnoresMalformedRepoDotEnv(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := writeEnvAuthorityRepoWithMalformedDotEnv(t)

	var capturedRepo string
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, repo string, _ serveOptions) int {
		capturedRepo = repo
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{"serve"}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("serve code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if capturedRepo != repo {
		t.Fatalf("serve repo = %q, want %q", capturedRepo, repo)
	}
	assertNoDotEnvLoadFailure(t, stdout.String()+stderr.String())
}

func TestDescribeCommandIgnoresMalformedRepoDotEnv(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := writeEnvAuthorityRepoWithMalformedDotEnv(t)
	contractsRoot := writeEnvAuthorityContractsFixture(t, "describe-dotenv-ignored")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"describe",
		"--contracts", contractsRoot,
		"--json",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("describe code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertNoDotEnvLoadFailure(t, stdout.String()+stderr.String())
}

func TestDoctorCommandIgnoresMalformedRepoDotEnv(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	configureDoctorDockerStub(t)
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")
	repo := writeEnvAuthorityRepoWithMalformedDotEnv(t)
	contractsRoot := writeEnvAuthorityContractsFixture(t, "doctor-dotenv-ignored")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"doctor",
		"--backend", "claude_cli",
		"--config", writeDoctorClaudeConfig(t),
		"--contracts", contractsRoot,
		"--platform-spec", defaultPlatformSpecPath,
		"--data", t.TempDir(),
		"--api-listen-addr", "127.0.0.1:0",
		"--mcp-listen-addr", "127.0.0.1:0",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code == 0 {
		t.Fatalf("doctor unexpectedly succeeded; expected non-env preflight failure to keep proof meaningful")
	}
	assertNoDotEnvLoadFailure(t, stdout.String()+stderr.String())
}

func TestForkHarnessIgnoresMalformedRepoDotEnv(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := writeEnvAuthorityRepoWithMalformedDotEnv(t)

	var stdout bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), repo, []string{"--dry-run"}, &stdout)
	if code == 0 {
		t.Fatalf("fork unexpectedly succeeded; expected non-env runtime/store failure to keep proof meaningful")
	}
	assertNoDotEnvLoadFailure(t, stdout.String())
}

func TestPublicEnvTemplateIsNonAuthoritative(t *testing.T) {
	root := repoRoot()
	envExample, err := os.ReadFile(filepath.Join(root, ".env.example"))
	if err != nil {
		t.Fatalf("read .env.example: %v", err)
	}
	envText := string(envExample)
	for _, forbidden := range []string{
		"SWARM_API_TOKEN=",
		"SWARM_TOOL_GATEWAY_TOKEN=",
		"ANTHROPIC_API_KEY=",
		"OPENAI_API_KEY=",
		"POSTGRES_DSN=",
		"SWARM_STORE_BACKEND=",
		"SWARM_WORKSPACE_DATA_SOURCE=",
		"SWARM_WORKSPACE_BACKEND=",
		"SWARM_ARTIFACT_ROOT=",
		"SWARM_MONITOR_DIR=",
		"SWARM_SQL_DEBUG=",
	} {
		if strings.Contains(envText, forbidden) {
			t.Fatalf(".env.example still advertises authoritative assignment %q:\n%s", forbidden, envText)
		}
	}
	for _, want := range []string{"non-authoritative", "config.example.yaml", "swarm secrets"} {
		if !strings.Contains(envText, want) {
			t.Fatalf(".env.example missing guidance %q:\n%s", want, envText)
		}
	}

	readme, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	contributing, err := os.ReadFile(filepath.Join(root, "CONTRIBUTING.md"))
	if err != nil {
		t.Fatalf("read CONTRIBUTING.md: %v", err)
	}
	publicDocs := string(readme) + "\n" + string(contributing)
	for _, forbidden := range []string{"Copy to .env", "public environment template"} {
		if strings.Contains(publicDocs, forbidden) {
			t.Fatalf("public docs still promote .env authority phrase %q", forbidden)
		}
	}
	for _, forbidden := range []string{
		"SWARM_ARTIFACT_ROOT",
		"SWARM_MONITOR_DIR",
		"SWARM_PROMPTS_DIR",
		"SWARM_SQL_DEBUG",
		"SWARM_BOOT_WARNINGS_FATAL",
		"SWARM_EMIT_SCHEMA_STRICT",
		"SWARM_CATALOG_E2E_DEBUG",
	} {
		if strings.Contains(publicDocs, forbidden) {
			t.Fatalf("public docs still advertise #1640 env %q as normal setup", forbidden)
		}
	}
	for _, want := range []string{"config.example.yaml", "swarm secrets"} {
		if !strings.Contains(publicDocs, want) {
			t.Fatalf("public docs missing replacement setup guidance %q", want)
		}
	}

	runtimeConfig, err := os.ReadFile(filepath.Join(root, "runtime-config.example.yaml"))
	if err != nil {
		t.Fatalf("read runtime-config.example.yaml: %v", err)
	}
	runtimeConfigText := string(runtimeConfig)
	if strings.Contains(runtimeConfigText, "\n  data_source:") {
		t.Fatalf("runtime-config.example.yaml sets an explicit data_source that fresh repos may not have:\n%s", runtimeConfigText)
	}
	if !strings.Contains(runtimeConfigText, "default project data directory") {
		t.Fatalf("runtime-config.example.yaml missing default data directory guidance:\n%s", runtimeConfigText)
	}
}

func TestPlatformSpecIssue1640EnvClassificationCoversRetainedSlice(t *testing.T) {
	var spec struct {
		EnvironmentSourceAuthority struct {
			WorkspaceMonitorArtifactDebugSlice struct {
				PromotedBy           string `yaml:"promoted_by"`
				ParentTracker        string `yaml:"parent_tracker"`
				ImplementationStatus string `yaml:"implementation_status"`
				CanonicalOwner       string `yaml:"canonical_owner"`
				Scope                string `yaml:"scope"`
				ClassificationRule   string `yaml:"classification_rule"`
				Classifications      map[string]struct {
					Disposition string   `yaml:"disposition"`
					Owner       string   `yaml:"owner"`
					EnvVars     []string `yaml:"env_vars"`
					EnvPrefixes []string `yaml:"env_prefixes"`
					Rule        string   `yaml:"rule"`
				} `yaml:"classifications"`
				PublicDocQuarantine struct {
					Rule string `yaml:"rule"`
				} `yaml:"public_doc_quarantine"`
				SplitSiblings []string `yaml:"split_siblings"`
			} `yaml:"workspace_monitor_artifact_debug_slice"`
		} `yaml:"environment_source_authority"`
	}
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}

	authority := spec.EnvironmentSourceAuthority.WorkspaceMonitorArtifactDebugSlice
	if authority.PromotedBy != "#1640" || authority.ParentTracker != "#1600" || authority.ImplementationStatus != "classification_spec_lock_only" {
		t.Fatalf("#1640 env authority status = promoted_by:%q parent:%q status:%q", authority.PromotedBy, authority.ParentTracker, authority.ImplementationStatus)
	}
	if !strings.Contains(authority.CanonicalOwner, "environment_source_authority.workspace_monitor_artifact_debug_slice") {
		t.Fatalf("#1640 canonical owner = %q", authority.CanonicalOwner)
	}
	for _, want := range []string{"classification authority", "#1731", "does not implement unknown-env", "Public docs"} {
		if !strings.Contains(authority.Scope+authority.ClassificationRule+authority.PublicDocQuarantine.Rule, want) {
			t.Fatalf("#1640 env authority missing %q:\n%#v", want, authority)
		}
	}
	if !joinedContains(authority.SplitSiblings, "#1731") || !joinedContains(authority.SplitSiblings, "unknown-env guard") {
		t.Fatalf("#1640 split siblings do not preserve #1731 guard split: %#v", authority.SplitSiblings)
	}

	for name, wantDisposition := range map[string]string{
		"existing_config_owned_workspace_sources":     "existing_config_owned_env_source",
		"production_bootstrap_or_deployment_plumbing": "keep_env_first_slice_bootstrap_or_deployment_plumbing",
		"internal_workspace_lifecycle_names":          "keep_env_first_slice_internal_runtime_plumbing",
		"runtime_private_storage_and_observability":   "keep_env_first_slice_runtime_private_storage_or_observability",
		"internal_prompt_and_spec_helpers":            "keep_env_first_slice_internal_developer_helper",
		"debug_and_test_quarantine":                   "keep_env_first_slice_debug_or_test_quarantine",
		"governed_outside_1640":                       "governed_by_existing_owner_or_split",
	} {
		got, ok := authority.Classifications[name]
		if !ok {
			t.Fatalf("#1640 env classification missing category %q: %#v", name, authority.Classifications)
		}
		if got.Disposition != wantDisposition {
			t.Fatalf("#1640 classification %q disposition = %q, want %q", name, got.Disposition, wantDisposition)
		}
		if strings.TrimSpace(got.Owner) == "" || strings.TrimSpace(got.Rule) == "" {
			t.Fatalf("#1640 classification %q missing owner/rule: %#v", name, got)
		}
	}

	classified := make(map[string]string)
	for category, entry := range authority.Classifications {
		for _, envName := range entry.EnvVars {
			classified[envName] = category
		}
	}
	for _, want := range []string{
		"SWARM_WORKSPACE_DATA_SOURCE",
		"SWARM_WORKSPACE_BACKEND",
		"SWARM_DOCKER_BIN",
		"SWARM_WORKSPACE_IMAGE",
		"SWARM_WORKSPACE_HOST_ROOT",
		"SWARM_WORKSPACE_VOLUMES_FROM",
		"SWARM_WORKSPACE_NETWORK",
		"SWARM_WORKSPACE_DATA_MOUNT",
		"SWARM_WORKSPACE_CONTRACTS_SOURCE",
		"SWARM_WORKSPACE_CONTRACTS_MOUNT",
		"SWARM_SCAFFOLD_CONTAINER",
		"SWARM_SCAFFOLD_WORKDIR",
		"SWARM_SCAFFOLD_VOLUME",
		"SWARM_SYSTEM_CONTAINER",
		"SWARM_SYSTEM_WORKDIR",
		"SWARM_SYSTEM_ENTITIES_VOLUME",
		"SWARM_SYSTEM_NGINX_VOLUME",
		"SWARM_SYSTEM_SYSTEMD_VOLUME",
		"SWARM_ENTITY_CONTAINER_PREFIX",
		"SWARM_ENTITY_WORKDIR",
		"SWARM_ARTIFACT_ROOT",
		"SWARM_MONITOR_DIR",
		"SWARM_PROMPTS_DIR",
		"SWARM_AGENT_CONFIG_MAP_FILE",
		"SWARM_VERIFICATION_GATES_FILE",
		"SWARM_TOOLING_LOCK_FILE",
		"SWARM_SQL_DEBUG",
		"SWARM_BOOT_WARNINGS_FATAL",
		"SWARM_EMIT_SCHEMA_STRICT",
		"SWARM_CATALOG_E2E_DEBUG",
		"SWARM_TEST_POSTGRES_DSN",
		"SWARM_TEST_POSTGRES_TEMPLATE_CLEANUP",
		"SWARM_TEST_POSTGRES_TEMPLATE_PARENT_PID",
		"SWARM_TEST_POSTGRES_TEMPLATE_ADMIN_DSN",
		"SWARM_TEST_POSTGRES_TEMPLATE_NAME",
		"SWARM_LOG_LEVEL",
	} {
		if classified[want] == "" {
			t.Fatalf("#1640 env %q missing classification; classified=%#v", want, classified)
		}
	}
	if !stringSliceContains(authority.Classifications["debug_and_test_quarantine"].EnvPrefixes, "SWARM_TEST_") {
		t.Fatalf("#1640 debug/test quarantine missing SWARM_TEST_ prefix: %#v", authority.Classifications["debug_and_test_quarantine"].EnvPrefixes)
	}
}

func writeEnvAuthorityRepoWithMalformedDotEnv(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, "go.mod"), "module dotenvignored\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, ".env"), "SWARM_API_TOKEN=repo-token\nBROKEN\n")
	return repo
}

func writeEnvAuthorityContractsFixture(t *testing.T, name string) string {
	t.Helper()
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: `+name+`
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: `+name+`
initial_state: idle
states:
  - idle
terminal_states:
  - idle
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	return root
}

func assertNoDotEnvLoadFailure(t testing.TB, output string) {
	t.Helper()
	for _, forbidden := range []string{"load .env", "expected KEY=VALUE"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("output still shows repo .env parsing failure %q:\n%s", forbidden, output)
		}
	}
}
