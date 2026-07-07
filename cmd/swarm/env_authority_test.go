package main

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestRepoWideSwarmEnvAcceptedSetMatchesSpec(t *testing.T) {
	var spec struct {
		EnvironmentSourceAuthority struct {
			RepoWideSwarmEnvAcceptedSet struct {
				PromotedBy           string   `yaml:"promoted_by"`
				ParentTracker        string   `yaml:"parent_tracker"`
				ImplementationStatus string   `yaml:"implementation_status"`
				CanonicalOwner       string   `yaml:"canonical_owner"`
				SourceAuthorityRule  string   `yaml:"source_authority_rule"`
				Categories           []string `yaml:"categories"`
				AcceptedEnv          []struct {
					Name      string `yaml:"name"`
					Prefix    string `yaml:"prefix"`
					Category  string `yaml:"category"`
					Owner     string `yaml:"owner"`
					Migration string `yaml:"migration"`
				} `yaml:"accepted_env"`
			} `yaml:"repo_wide_swarm_env_accepted_set"`
		} `yaml:"environment_source_authority"`
	}
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}

	authority := spec.EnvironmentSourceAuthority.RepoWideSwarmEnvAcceptedSet
	if authority.PromotedBy != "#1731" || authority.ParentTracker != "#1600" || authority.ImplementationStatus != "implemented_first_slice" {
		t.Fatalf("#1731 env authority status = promoted_by:%q parent:%q status:%q", authority.PromotedBy, authority.ParentTracker, authority.ImplementationStatus)
	}
	if authority.CanonicalOwner != swarmEnvAuthorityOwner {
		t.Fatalf("#1731 canonical owner = %q, want %q", authority.CanonicalOwner, swarmEnvAuthorityOwner)
	}
	for _, want := range []string{"no ambient source authority", "typed delegation", "doctor", "generated-boundary", "fail closed"} {
		if !strings.Contains(authority.SourceAuthorityRule, want) {
			t.Fatalf("#1731 source authority rule missing %q:\n%s", want, authority.SourceAuthorityRule)
		}
	}

	specCategories := map[string]bool{}
	for _, category := range authority.Categories {
		if specCategories[category] {
			t.Fatalf("#1731 duplicate category %q", category)
		}
		specCategories[category] = true
	}
	for _, category := range []swarmEnvCategory{
		swarmEnvCategoryBootstrap,
		swarmEnvCategoryTypedDelegation,
		swarmEnvCategoryGeneratedBoundary,
		swarmEnvCategoryTestQuarantine,
		swarmEnvCategorySeededLegacy,
		swarmEnvCategoryKnownRetired,
		swarmEnvCategoryUnknownStale,
	} {
		if !specCategories[string(category)] {
			t.Fatalf("#1731 spec missing category %q: %#v", category, authority.Categories)
		}
	}

	specRows := map[string]struct {
		Category  string
		Owner     string
		Migration string
	}{}
	for _, row := range authority.AcceptedEnv {
		key := specSwarmEnvRowKey(row.Name, row.Prefix)
		if key == "" {
			t.Fatalf("#1731 accepted env row has neither name nor prefix: %#v", row)
		}
		if specRows[key].Category != "" {
			t.Fatalf("#1731 duplicate accepted env row %q", key)
		}
		if !specCategories[row.Category] {
			t.Fatalf("#1731 row %q uses unknown category %q", key, row.Category)
		}
		if strings.TrimSpace(row.Owner) == "" || strings.TrimSpace(row.Migration) == "" {
			t.Fatalf("#1731 row %q missing owner/migration: %#v", key, row)
		}
		if row.Category == string(swarmEnvCategorySeededLegacy) && !strings.Contains(row.Migration, "#") && !strings.Contains(row.Migration, "config") && !strings.Contains(row.Migration, "--") {
			t.Fatalf("#1731 seeded legacy row %q missing migration pointer: %#v", key, row)
		}
		specRows[key] = struct {
			Category  string
			Owner     string
			Migration string
		}{Category: row.Category, Owner: row.Owner, Migration: row.Migration}
	}

	codeRows := map[string]swarmEnvCatalogEntry{}
	for _, entry := range swarmEnvCatalogEntries() {
		key := catalogSwarmEnvRowKey(entry)
		if key == "" {
			t.Fatalf("code catalog entry has neither name nor prefix: %#v", entry)
		}
		if _, ok := codeRows[key]; ok {
			t.Fatalf("code catalog duplicate row %q", key)
		}
		codeRows[key] = entry
	}
	for key, entry := range codeRows {
		row, ok := specRows[key]
		if !ok {
			t.Fatalf("code catalog row %q missing from #1731 spec", key)
		}
		if row.Category != string(entry.Category) || row.Owner != entry.Owner || row.Migration != entry.Migration {
			t.Fatalf("#1731 spec row %q mismatch\nspec: %#v\ncode: %#v", key, row, entry)
		}
	}
	for key := range specRows {
		if _, ok := codeRows[key]; !ok {
			t.Fatalf("#1731 spec row %q missing from code catalog", key)
		}
	}
}

func TestSwarmEnvGuardBlocksUnknownWithSuggestion(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_WORSKPACE_IMAGE", "stale")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"serve",
		"--api-listen-addr", "127.0.0.1:0",
		"--mcp-listen-addr", "127.0.0.1:0",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
	}
	for _, want := range []string{
		"[BLOCKER] env/unknown_stale: SWARM_WORSKPACE_IMAGE",
		"did you mean SWARM_WORKSPACE_IMAGE",
		"unset SWARM_WORSKPACE_IMAGE",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
		}
	}
}

func TestSwarmEnvGuardSkipsPureVersionAndCompletion(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_WORSKPACE_IMAGE", "stale")

	for _, args := range [][]string{
		{"version"},
		{"completion", "bash"},
	} {
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), repoRoot(), args, &stdout, &stderr, defaultRootCommandOptions())
		if code != cliExitOK {
			t.Fatalf("%v code = %d, want %d stdout=%s stderr=%s", args, code, cliExitOK, stdout.String(), stderr.String())
		}
		if strings.Contains(stdout.String()+stderr.String(), "SWARM_WORSKPACE_IMAGE") {
			t.Fatalf("%v should skip env guard, got stdout=%s stderr=%s", args, stdout.String(), stderr.String())
		}
	}
}

func TestSwarmEnvGuardSkipsPureSubcommandHelpFlags(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_WORSKPACE_IMAGE", "stale")

	for _, args := range [][]string{
		{"event", "publish", "--help"},
		{"run", "start", "--help"},
		{"run", "start", "-h"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), repoRoot(), args, &stdout, &stderr, defaultRootCommandOptions())
			if code != cliExitOK {
				t.Fatalf("%v code = %d, want %d stdout=%s stderr=%s", args, code, cliExitOK, stdout.String(), stderr.String())
			}
			output := stdout.String() + stderr.String()
			if strings.Contains(output, "SWARM_WORSKPACE_IMAGE") {
				t.Fatalf("%v should skip env guard, got stdout=%s stderr=%s", args, stdout.String(), stderr.String())
			}
			if !strings.Contains(output, "Usage:") {
				t.Fatalf("%v help output missing Usage: stdout=%s stderr=%s", args, stdout.String(), stderr.String())
			}
		})
	}
}

func TestSwarmEnvGuardDoesNotSkipWhenHelpIsCommandData(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_WORSKPACE_IMAGE", "stale")

	cases := []struct {
		name string
		args []string
	}{
		{
			name: "positional help",
			args: []string{"event", "publish", "help", "--payload-json", "{}"},
		},
		{
			name: "double dash help data",
			args: []string{"event", "publish", "--payload-json", "{}", "--", "--help"},
		},
		{
			name: "flag value help data",
			args: []string{"event", "publish", "--payload-json", "--help"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), repoRoot(), tc.args, &stdout, &stderr, defaultRootCommandOptions())
			if code != cliExitValidation {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
			}
			for _, want := range []string{
				"[BLOCKER] env/unknown_stale: SWARM_WORSKPACE_IMAGE",
				"did you mean SWARM_WORKSPACE_IMAGE",
			} {
				if !strings.Contains(stderr.String(), want) {
					t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
				}
			}
		})
	}
}

func TestSwarmEnvGuardDoesNotSkipVersionServerEqualsTrue(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_WORSKPACE_IMAGE", "stale")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"version",
		"--server=true",
		"--api-server", "http://127.0.0.1:9",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
	}
	for _, want := range []string{
		"[BLOCKER] env/unknown_stale: SWARM_WORSKPACE_IMAGE",
		"did you mean SWARM_WORKSPACE_IMAGE",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
		}
	}
}

func TestSwarmEnvGuardBlocksGeneratedBoundaryParentEnv(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:19002")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"serve",
		"--api-listen-addr", "127.0.0.1:0",
		"--mcp-listen-addr", "127.0.0.1:0",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
	}
	for _, want := range []string{
		"[BLOCKER] env/generated_boundary: SWARM_TOOL_GATEWAY_URL",
		"generated final-boundary env must be injected by Swarm",
		"unset SWARM_TOOL_GATEWAY_URL",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
		}
	}
}

func TestSwarmEnvGuardBlocksRetiredRuntimeLLMConfigEnv(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    []string
		notWant []string
	}{
		{name: "SWARM_RUNTIME_RECOVERY_ON_STARTUP", value: "true", want: []string{"env/known_retired: SWARM_RUNTIME_RECOVERY_ON_STARTUP", "runtime.recovery_on_startup"}},
		{name: "SWARM_LLM_SESSION_LOCK_TTL", value: "1s", want: []string{"env/known_retired: SWARM_LLM_SESSION_LOCK_TTL", "llm.session.lock_ttl"}},
		{name: "SWARM_LLM_SESSION_ROTATE_AFTER_TURNS", value: "2", want: []string{"env/known_retired: SWARM_LLM_SESSION_ROTATE_AFTER_TURNS", "llm.session.rotate_after_turns"}},
		{name: "SWARM_LLM_SESSION_ROTATE_ON_PARSE_FAILURES", value: "2", want: []string{"env/known_retired: SWARM_LLM_SESSION_ROTATE_ON_PARSE_FAILURES", "llm.session.rotate_on_parse_failures"}},
		{name: "SWARM_CLAUDE_API_MAX_RETRIES", value: "7", want: []string{"env/known_retired: SWARM_CLAUDE_API_MAX_RETRIES", "llm.claude_api.max_retries"}},
		{name: "SWARM_CLAUDE_API_RETRY_BACKOFF", value: "7s", want: []string{"env/known_retired: SWARM_CLAUDE_API_RETRY_BACKOFF", "llm.claude_api.retry_backoff"}},
		{name: "SWARM_CLAUDE_CLI_COMMAND", value: "false", want: []string{"env/known_retired: SWARM_CLAUDE_CLI_COMMAND", "llm.claude_cli.command"}},
		{name: "SWARM_CLAUDE_CLI_TIMEOUT", value: "1s", want: []string{"env/known_retired: SWARM_CLAUDE_CLI_TIMEOUT", "llm.claude_cli.timeout"}},
		{name: "SWARM_CLAUDE_CLI_OUTPUT_FORMAT", value: "bad", want: []string{"env/known_retired: SWARM_CLAUDE_CLI_OUTPUT_FORMAT", "llm.claude_cli.output_format"}},
		{name: "SWARM_CLAUDE_TIMEOUT_SECONDS", value: "1", want: []string{"env/known_retired: SWARM_CLAUDE_TIMEOUT_SECONDS", "llm.claude_cli.timeout"}},
		{name: "SWARM_CLAUDE_CLI_RETRIES", value: "7", want: []string{"env/known_retired: SWARM_CLAUDE_CLI_RETRIES", "no supported replacement", "#1803"}, notWant: []string{"llm.claude_cli.retries"}},
		{name: "SWARM_CLAUDE_CLI_NO_SESSION_PERSISTENCE", value: "true", want: []string{"env/known_retired: SWARM_CLAUDE_CLI_NO_SESSION_PERSISTENCE", "no supported replacement", "#1803"}, notWant: []string{"llm.claude_cli.no_session_persistence"}},
		{name: "SWARM_CLAUDE_CLI_USE_TMUX", value: "true", want: []string{"env/known_retired: SWARM_CLAUDE_CLI_USE_TMUX", "no supported replacement", "#1803"}, notWant: []string{"llm.claude_cli.use_tmux"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv(tc.name, tc.value)

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
				"serve",
				"--api-listen-addr", "127.0.0.1:0",
				"--mcp-listen-addr", "127.0.0.1:0",
			}, &stdout, &stderr, defaultRootCommandOptions())
			if code != cliExitValidation {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
			}
			output := stdout.String() + stderr.String()
			for _, want := range tc.want {
				if !strings.Contains(output, want) {
					t.Fatalf("output missing %q:\n%s", want, output)
				}
			}
			for _, notWant := range tc.notWant {
				if strings.Contains(output, notWant) {
					t.Fatalf("output contains fake replacement %q:\n%s", notWant, output)
				}
			}
		})
	}
}

func TestSwarmEnvGuardBlocksRetiredStoreDatabaseConfigEnv(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  []string
	}{
		{name: "SWARM_STORE_BACKEND", value: "postgres", want: []string{"env/known_retired", "SWARM_STORE_BACKEND", "--store or store.backend"}},
		{name: "SWARM_SQLITE_PATH", value: "dev.db", want: []string{"env/known_retired", "SWARM_SQLITE_PATH", "store.sqlite.path"}},
		{name: "SWARM_DB_HOST", value: "db.example.test", want: []string{"env/known_retired", "SWARM_DB_HOST", "database.host"}},
		{name: "SWARM_DB_PORT", value: "15432", want: []string{"env/known_retired", "SWARM_DB_PORT", "database.port"}},
		{name: "SWARM_DB_NAME", value: "swarm_test", want: []string{"env/known_retired", "SWARM_DB_NAME", "database.name"}},
		{name: "SWARM_DB_USER", value: "swarm_user", want: []string{"env/known_retired", "SWARM_DB_USER", "database.user"}},
		{name: "SWARM_DB_SSLMODE", value: "require", want: []string{"env/known_retired", "SWARM_DB_SSLMODE", "database.sslmode"}},
		{name: "SWARM_DB_POOL_SIZE", value: "9", want: []string{"env/known_retired", "SWARM_DB_POOL_SIZE", "database.pool_size"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv(tc.name, tc.value)

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
				"serve",
				"--api-listen-addr", "127.0.0.1:0",
				"--mcp-listen-addr", "127.0.0.1:0",
			}, &stdout, &stderr, defaultRootCommandOptions())
			if code != cliExitValidation {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
			}
			output := stdout.String() + stderr.String()
			for _, want := range tc.want {
				if !strings.Contains(output, want) {
					t.Fatalf("output missing %q:\n%s", want, output)
				}
			}
		})
	}
}

func TestSwarmEnvGuardAllowsTypedDatabasePasswordEnvDelegation(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_DB_PASSWORD", "secret")
	configPath := filepath.Join(t.TempDir(), "runtime.yaml")
	writeWorkflowValidationFixtureFile(t, configPath, "database:\n  password_env: SWARM_DB_PASSWORD\n")

	called := false
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, _ serveOptions) int {
		called = true
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"serve",
		"--config", configPath,
		"--api-listen-addr", "127.0.0.1:0",
		"--mcp-listen-addr", "127.0.0.1:0",
	}, &stdout, &stderr, opts)
	if code != cliExitOK {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitOK, stdout.String(), stderr.String())
	}
	if !called {
		t.Fatalf("serve callback was not called; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}

func TestSwarmEnvGuardAllowsExecutableAdjacentTypedDatabasePasswordEnvDelegation(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_DB_PASSWORD", "secret")
	binDir := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(binDir, "config.yaml"), "database:\n  password_env: SWARM_DB_PASSWORD\n")
	originalExecutablePath := runtimeConfigExecutablePath
	runtimeConfigExecutablePath = func() (string, error) {
		return filepath.Join(binDir, "swarm"), nil
	}
	t.Cleanup(func() { runtimeConfigExecutablePath = originalExecutablePath })

	called := false
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, _ serveOptions) int {
		called = true
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"serve",
		"--api-listen-addr", "127.0.0.1:0",
		"--mcp-listen-addr", "127.0.0.1:0",
	}, &stdout, &stderr, opts)
	if code != cliExitOK {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitOK, stdout.String(), stderr.String())
	}
	if !called {
		t.Fatalf("serve callback was not called; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}

func TestSwarmEnvGuardEmptyRetiredEnvUsesNonEmptyBoundary(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_RUNTIME_MAX_CONCURRENT_AGENTS", "")
	t.Setenv("SWARM_LLM_BACKEND", "")

	called := false
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ string, _ serveOptions) int {
		called = true
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"serve",
		"--api-listen-addr", "127.0.0.1:0",
		"--mcp-listen-addr", "127.0.0.1:0",
	}, &stdout, &stderr, opts)
	if code != cliExitOK {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitOK, stdout.String(), stderr.String())
	}
	if !called {
		t.Fatalf("serve callback was not called; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}

func TestDoctorReportsSwarmEnvFindingsWithConfigFailure(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_WORSKPACE_IMAGE", "stale")
	contractsRoot := writeEnvAuthorityContractsFixture(t, "doctor-env-report")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"doctor",
		"--json",
		"--config", filepath.Join(t.TempDir(), "missing-runtime.yaml"),
		"--contracts", contractsRoot,
		"--platform-spec", defaultPlatformSpecPath,
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitRuntime, stdout.String(), stderr.String())
	}
	var report localPreflightReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("parse doctor JSON: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
	}
	if report.OK {
		t.Fatalf("doctor report OK=true, want blockers: %#v", report)
	}
	assertLocalPreflightFinding(t, report, localPreflightEnvPrerequisite, string(swarmEnvCategoryUnknownStale), "SWARM_WORSKPACE_IMAGE")
	assertLocalPreflightFinding(t, report, localPreflightBackendPrerequisite, "config_load_failed", "missing-runtime.yaml")
}

func TestDoctorTargetReportsSwarmEnvFindings(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_WORSKPACE_IMAGE", "stale")
	repo := writeDoctorTargetRepo(t)

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"doctor",
		"--target",
		"--json",
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
		t.Fatalf("parse doctor target JSON: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
	}
	if report.OK {
		t.Fatalf("doctor target report OK=true, want env blocker: %#v", report)
	}
	assertDoctorTargetEnvFinding(t, report, string(swarmEnvCategoryUnknownStale), "SWARM_WORSKPACE_IMAGE")
}

func TestDoctorTargetReportsRuntimeConfigEnvRejectors(t *testing.T) {
	cases := []string{
		"SWARM_LLM_BACKEND",
		"SWARM_RUNTIME_MAX_CONCURRENT_AGENTS",
		"SWARM_OPENAI_COMPATIBLE_BASE_URL",
		"SWARM_OPENAI_COMPATIBLE_DEFAULT_MODEL",
		"SWARM_RUNTIME_RECOVERY_ON_STARTUP",
		"SWARM_CLAUDE_CLI_TIMEOUT",
		"SWARM_CLAUDE_TIMEOUT_SECONDS",
		"SWARM_CLAUDE_CLI_RETRIES",
		"SWARM_STORE_BACKEND",
		"SWARM_SQLITE_PATH",
		"SWARM_DB_HOST",
		"SWARM_DB_PORT",
		"SWARM_DB_NAME",
		"SWARM_DB_USER",
		"SWARM_DB_SSLMODE",
		"SWARM_DB_POOL_SIZE",
	}
	for _, envName := range cases {
		t.Run(envName, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv(envName, "stale")
			repo := writeDoctorTargetRepo(t)

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), repo, []string{
				"doctor",
				"--target",
				"--json",
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
				t.Fatalf("parse doctor target JSON: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
			}
			if report.OK {
				t.Fatalf("doctor target report OK=true, want env blocker: %#v", report)
			}
			assertDoctorTargetEnvFinding(t, report, string(swarmEnvCategoryKnownRetired), envName)
			if envName == "SWARM_CLAUDE_CLI_RETRIES" {
				finding := findDoctorTargetEnvFinding(t, report, string(swarmEnvCategoryKnownRetired), envName)
				if !strings.Contains(finding.Message, "no supported replacement") || !strings.Contains(finding.Message, "#1803") {
					t.Fatalf("retired inert finding message = %q, want #1803/no replacement", finding.Message)
				}
				if strings.Contains(finding.Message+finding.Remediation, "llm.claude_cli.retries") {
					t.Fatalf("retired inert finding advertises fake replacement: %#v", finding)
				}
			}
		})
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

func specSwarmEnvRowKey(name, prefix string) string {
	name = strings.TrimSpace(name)
	prefix = strings.TrimSpace(prefix)
	switch {
	case name != "" && prefix == "":
		return "name:" + name
	case prefix != "" && name == "":
		return "prefix:" + prefix
	default:
		return ""
	}
}

func catalogSwarmEnvRowKey(entry swarmEnvCatalogEntry) string {
	return specSwarmEnvRowKey(entry.Name, entry.Prefix)
}

func assertLocalPreflightFinding(t *testing.T, report localPreflightReport, category localPreflightCategory, code, messagePart string) {
	t.Helper()
	for _, finding := range report.Findings {
		if finding.Category == category && finding.Code == code && strings.Contains(finding.Message, messagePart) {
			return
		}
	}
	t.Fatalf("missing finding category=%s code=%s message containing %q: %#v", category, code, messagePart, report.Findings)
}

func assertDoctorTargetEnvFinding(t *testing.T, report doctorTargetReport, code, messagePart string) {
	t.Helper()
	_ = findDoctorTargetEnvFinding(t, report, code, messagePart)
}

func findDoctorTargetEnvFinding(t *testing.T, report doctorTargetReport, code, messagePart string) localPreflightFinding {
	t.Helper()
	for _, finding := range report.Env {
		if finding.Category == localPreflightEnvPrerequisite && finding.Code == code && strings.Contains(finding.Message, messagePart) {
			return finding
		}
	}
	t.Fatalf("missing target env finding code=%s message containing %q: %#v", code, messagePart, report.Env)
	return localPreflightFinding{}
}

func assertNoDotEnvLoadFailure(t testing.TB, output string) {
	t.Helper()
	for _, forbidden := range []string{"load .env", "expected KEY=VALUE"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("output still shows repo .env parsing failure %q:\n%s", forbidden, output)
		}
	}
}
