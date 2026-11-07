package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/userfacing"
)

func TestServeCommandIgnoresMalformedRepoDotEnv(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := writeEnvAuthorityRepoWithMalformedDotEnv(t)

	var capturedRepo string
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, root InvocationRoot, _ ServeOptions) int {
		capturedRepo = root.Path()
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{"serve", "."}, &stdout, &stderr, opts)
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
	sourceRoot := writeEnvAuthorityContractsFixture(t, "describe-dotenv-ignored")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"describe", sourceRoot,
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
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"doctor",
		"--backend", "claude_cli",
		"--config", writeDoctorClaudeConfig(t, ""),
		"--api-listen-addr", "127.0.0.1:0",
		"--mcp-listen-addr", "127.0.0.1:0",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("doctor exited %d; with the embedded platform spec the preflight must complete, stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertNoDotEnvLoadFailure(t, stdout.String()+stderr.String())
}

func TestPublicEnvTemplateIsNonAuthoritative(t *testing.T) {
	root := RepoRoot()
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
	for _, want := range []string{"non-authoritative", "swarm.example.yaml", "swarm secrets"} {
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
		"SWARM_SQL_DEBUG",
		"SWARM_BOOT_WARNINGS_FATAL",
		"SWARM_EMIT_SCHEMA_STRICT",
		"SWARM_CATALOG_E2E_DEBUG",
	} {
		if strings.Contains(publicDocs, forbidden) {
			t.Fatalf("public docs still advertise #1640 env %q as normal setup", forbidden)
		}
	}
	for _, want := range []string{"swarm.example.yaml", "swarm secrets"} {
		if !strings.Contains(publicDocs, want) {
			t.Fatalf("public docs missing replacement setup guidance %q", want)
		}
	}

	example, err := os.ReadFile(filepath.Join(root, "swarm.example.yaml"))
	if err != nil {
		t.Fatalf("read swarm.example.yaml: %v", err)
	}
	exampleText := string(example)
	for _, want := range []string{"swarm.yaml - Swarm's config file", ".swarm/swarm.yaml or --config <file>", "Deployment keys", "Secret references", "swarm doctor --target"} {
		if !strings.Contains(exampleText, want) {
			t.Fatalf("swarm.example.yaml missing generated guidance %q:\n%s", want, exampleText)
		}
	}
	if forbidden := userfacing.FindForbidden(userfacing.ProfileGeneratedConfig, exampleText); len(forbidden) > 0 {
		t.Fatalf("swarm.example.yaml still exposes forbidden guidance %q:\n%s", forbidden, exampleText)
	}
}

func TestPublicConfigGuidanceUsesUnifiedSwarmYAML(t *testing.T) {
	root := RepoRoot()
	publicFiles := []string{
		".env.example",
		"README.md",
		"CONTRIBUTING.md",
		filepath.Join("internal", "cliapp", "cli.go"),
		filepath.Join("internal", "cliapp", "doctor.go"),
		filepath.Join("internal", "cliapp", "env_guard.go"),
		filepath.Join("internal", "cliapp", "local_preflight.go"),
		filepath.Join("internal", "cliapp", "run_command.go"),
		filepath.Join("internal", "cliapp", "workspace_build.go"),
	}
	for _, rel := range publicFiles {
		body, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		text := string(body)
		if forbidden := userfacing.FindForbidden(userfacing.ProfilePublicConfigGuidance, text); len(forbidden) > 0 {
			t.Fatalf("%s still teaches stale public config-source guidance %q", rel, forbidden)
		}
	}
}

func TestSwarmEnvGuardBlocksRetiredArtifactRoot(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_ARTIFACT_ROOT", t.TempDir())
	opts := defaultRootCommandOptions()
	opts.runServe = func(context.Context, InvocationRoot, ServeOptions) int {
		t.Fatal("retired artifact environment reached serve")
		return 0
	}
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"serve", "."}, &stdout, &stderr, opts)
	if code != CLIExitValidation {
		t.Fatalf("code = %d, want %d; stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
	}
	output := stdout.String() + stderr.String()
	for _, want := range []string{"env/unknown_stale", "SWARM_ARTIFACT_ROOT", "unset SWARM_ARTIFACT_ROOT"} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "paths.artifact_root") {
		t.Fatalf("output advertises retired config as replacement:\n%s", output)
	}
}

func TestSwarmEnvGuardBlocksUnknownWithSuggestion(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_MONITOR_DR", "stale")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"serve", ".",
		"--api-listen-addr", "127.0.0.1:0",
		"--mcp-listen-addr", "127.0.0.1:0",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != CLIExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
	}
	for _, want := range []string{
		"[BLOCKER] env/unknown_stale @ SWARM_MONITOR_DR:",
		"did you mean SWARM_MONITOR_DIR",
		"unset SWARM_MONITOR_DR",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
		}
	}
}

func TestOwnedLifecycleSignalUsesExistingTestQuarantine(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	const unsupported = "SWARM_INTERNAL_MOCK_LIFECYCLE_REQUEST"
	const quarantined = "SWARM_TEST_INTERNAL_MOCK_LIFECYCLE_REQUEST"
	t.Setenv(unsupported, "{}")
	t.Setenv(quarantined, "{}")
	args := os.Args
	t.Cleanup(func() { os.Args = args })
	for _, tc := range []struct {
		binary   string
		severity string
	}{
		{binary: "serveapp.test", severity: "info"},
		{binary: "swarm", severity: "blocker"},
	} {
		t.Run(tc.binary, func(t *testing.T) {
			os.Args = []string{tc.binary}
			findings := collectSwarmEnvFindings(swarmEnvGuardContext{RepoRoot: t.TempDir()})
			seen := map[string]bool{}
			for _, finding := range findings {
				switch finding.Name {
				case unsupported:
					seen[unsupported] = true
					if finding.Category != swarmEnvCategoryUnknownStale || finding.Severity != "blocker" || finding.AcceptedBy != "" {
						t.Fatalf("unsupported lifecycle signal must remain unknown: %#v", finding)
					}
				case quarantined:
					seen[quarantined] = true
					if finding.Category != swarmEnvCategoryTestQuarantine || finding.Severity != tc.severity {
						t.Fatalf("test signal must use existing context restriction: %#v", finding)
					}
					if (finding.AcceptedBy != "") != (tc.severity == "info") {
						t.Fatalf("test signal authority disagrees with its context: %#v", finding)
					}
				}
			}
			if !seen[unsupported] || !seen[quarantined] {
				t.Fatalf("missing lifecycle signal findings: %#v", findings)
			}
		})
	}
}

func TestSwarmEnvGuardSkipsPureVersionAndCompletion(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_MONITOR_DR", "stale")

	for _, args := range [][]string{
		{"version"},
		{"completion", "bash"},
	} {
		var stdout, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), RepoRoot(), args, &stdout, &stderr, defaultRootCommandOptions())
		if code != cliExitOK {
			t.Fatalf("%v code = %d, want %d stdout=%s stderr=%s", args, code, cliExitOK, stdout.String(), stderr.String())
		}
		if strings.Contains(stdout.String()+stderr.String(), "SWARM_MONITOR_DR") {
			t.Fatalf("%v should skip env guard, got stdout=%s stderr=%s", args, stdout.String(), stderr.String())
		}
	}
}

func TestSwarmEnvGuardSkipsPureSubcommandHelpFlags(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_MONITOR_DR", "stale")

	for _, args := range [][]string{
		{"event", "publish", "--help"},
		{"run", "start", "--help"},
		{"run", "start", "-h"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), args, &stdout, &stderr, defaultRootCommandOptions())
			if code != cliExitOK {
				t.Fatalf("%v code = %d, want %d stdout=%s stderr=%s", args, code, cliExitOK, stdout.String(), stderr.String())
			}
			output := stdout.String() + stderr.String()
			if strings.Contains(output, "SWARM_MONITOR_DR") {
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
	t.Setenv("SWARM_MONITOR_DR", "stale")

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
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), tc.args, &stdout, &stderr, defaultRootCommandOptions())
			if code != CLIExitValidation {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
			}
			for _, want := range []string{
				"[BLOCKER] env/unknown_stale @ SWARM_MONITOR_DR:",
				"did you mean SWARM_MONITOR_DIR",
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
	t.Setenv("SWARM_MONITOR_DR", "stale")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"version",
		"--server=true",
		"--api-server", "http://127.0.0.1:9",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != CLIExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
	}
	for _, want := range []string{
		"[BLOCKER] env/unknown_stale @ SWARM_MONITOR_DR:",
		"did you mean SWARM_MONITOR_DIR",
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
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"serve",
		"--api-listen-addr", "127.0.0.1:0",
		"--mcp-listen-addr", "127.0.0.1:0",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != CLIExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
	}
	for _, want := range []string{
		"[BLOCKER] env/generated_boundary @ SWARM_TOOL_GATEWAY_URL:",
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
		{name: "SWARM_RUNTIME_RECOVERY_ON_STARTUP", value: "true", want: []string{"env/unknown_stale @ SWARM_RUNTIME_RECOVERY_ON_STARTUP", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_LLM_SESSION_LOCK_TTL", value: "1s", want: []string{"env/unknown_stale @ SWARM_LLM_SESSION_LOCK_TTL", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_LLM_SESSION_ROTATE_AFTER_TURNS", value: "2", want: []string{"env/unknown_stale @ SWARM_LLM_SESSION_ROTATE_AFTER_TURNS", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_LLM_SESSION_ROTATE_ON_PARSE_FAILURES", value: "2", want: []string{"env/unknown_stale @ SWARM_LLM_SESSION_ROTATE_ON_PARSE_FAILURES", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_CLAUDE_API_MAX_RETRIES", value: "7", want: []string{"env/unknown_stale @ SWARM_CLAUDE_API_MAX_RETRIES", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_CLAUDE_API_RETRY_BACKOFF", value: "7s", want: []string{"env/unknown_stale @ SWARM_CLAUDE_API_RETRY_BACKOFF", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_CLAUDE_CLI_COMMAND", value: "false", want: []string{"env/unknown_stale @ SWARM_CLAUDE_CLI_COMMAND", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_CLAUDE_CLI_TIMEOUT", value: "1s", want: []string{"env/unknown_stale @ SWARM_CLAUDE_CLI_TIMEOUT", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_CLAUDE_CLI_OUTPUT_FORMAT", value: "bad", want: []string{"env/unknown_stale @ SWARM_CLAUDE_CLI_OUTPUT_FORMAT", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_CLAUDE_TIMEOUT_SECONDS", value: "1", want: []string{"env/unknown_stale @ SWARM_CLAUDE_TIMEOUT_SECONDS", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_CLAUDE_CLI_RETRIES", value: "7", want: []string{"env/unknown_stale @ SWARM_CLAUDE_CLI_RETRIES", "unknown SWARM_* env is not accepted"}, notWant: []string{"llm.claude_cli.retries", "#1803"}},
		{name: "SWARM_CLAUDE_CLI_NO_SESSION_PERSISTENCE", value: "true", want: []string{"env/unknown_stale @ SWARM_CLAUDE_CLI_NO_SESSION_PERSISTENCE", "unknown SWARM_* env is not accepted"}, notWant: []string{"llm.claude_cli.no_session_persistence", "#1803"}},
		{name: "SWARM_CLAUDE_CLI_USE_TMUX", value: "true", want: []string{"env/unknown_stale @ SWARM_CLAUDE_CLI_USE_TMUX", "unknown SWARM_* env is not accepted"}, notWant: []string{"llm.claude_cli.use_tmux", "#1803"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv(tc.name, tc.value)

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
				"serve",
				"--api-listen-addr", "127.0.0.1:0",
				"--mcp-listen-addr", "127.0.0.1:0",
			}, &stdout, &stderr, defaultRootCommandOptions())
			if code != CLIExitValidation {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
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
		{name: "SWARM_STORE_BACKEND", value: "postgres", want: []string{"env/unknown_stale", "SWARM_STORE_BACKEND", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_SQLITE_PATH", value: "dev.db", want: []string{"env/unknown_stale", "SWARM_SQLITE_PATH", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_DB_HOST", value: "db.example.test", want: []string{"env/unknown_stale", "SWARM_DB_HOST", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_DB_PORT", value: "15432", want: []string{"env/unknown_stale", "SWARM_DB_PORT", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_DB_NAME", value: "swarm_test", want: []string{"env/unknown_stale", "SWARM_DB_NAME", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_DB_USER", value: "swarm_user", want: []string{"env/unknown_stale", "SWARM_DB_USER", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_DB_SSLMODE", value: "require", want: []string{"env/unknown_stale", "SWARM_DB_SSLMODE", "unknown SWARM_* env is not accepted"}},
		{name: "SWARM_DB_POOL_SIZE", value: "9", want: []string{"env/unknown_stale", "SWARM_DB_POOL_SIZE", "unknown SWARM_* env is not accepted"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv(tc.name, tc.value)

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
				"serve",
				"--api-listen-addr", "127.0.0.1:0",
				"--mcp-listen-addr", "127.0.0.1:0",
			}, &stdout, &stderr, defaultRootCommandOptions())
			if code != CLIExitValidation {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
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
	opts.runServe = func(_ context.Context, _ InvocationRoot, _ ServeOptions) int {
		called = true
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"serve", ".",
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

func TestSwarmEnvGuardAllowsTypedDatabasePasswordEnvDelegationFromSelectedFile(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(t *testing.T, repo string)
	}{
		{
			name: "local operator",
			setup: func(t *testing.T, repo string) {
				t.Helper()
				dir := filepath.Join(repo, ".swarm")
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatalf("mkdir local operator config: %v", err)
				}
				writeWorkflowValidationFixtureFile(t, filepath.Join(dir, "swarm.yaml"), "database:\n  password_env: SWARM_DB_PASSWORD\n")
			},
		},
		{
			name: "locator",
			setup: func(t *testing.T, repo string) {
				t.Helper()
				path := filepath.Join(repo, "operator.yaml")
				writeWorkflowValidationFixtureFile(t, path, "database:\n  password_env: SWARM_DB_PASSWORD\n")
				t.Setenv("SWARM_CONFIG", path)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_DB_PASSWORD", "secret")
			repo := t.TempDir()
			tt.setup(t, repo)

			called := false
			opts := defaultRootCommandOptions()
			opts.runServe = func(_ context.Context, _ InvocationRoot, _ ServeOptions) int {
				called = true
				return 0
			}

			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), repo, []string{
				"serve", ".",
				"--api-listen-addr", "127.0.0.1:0",
				"--mcp-listen-addr", "127.0.0.1:0",
			}, &stdout, &stderr, opts)
			if code != cliExitOK {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitOK, stdout.String(), stderr.String())
			}
			if !called {
				t.Fatalf("serve callback was not called; stdout=%s stderr=%s", stdout.String(), stderr.String())
			}
		})
	}
}

func TestSwarmEnvGuardIgnoresProjectTypedDatabasePasswordEnvDelegation(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_DB_PASSWORD", "secret")
	repo := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, "swarm.yaml"), "database:\n  password_env: SWARM_DB_PASSWORD\n")

	called := false
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ InvocationRoot, _ ServeOptions) int {
		called = true
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"serve",
		"--api-listen-addr", "127.0.0.1:0",
		"--mcp-listen-addr", "127.0.0.1:0",
	}, &stdout, &stderr, opts)
	if code != CLIExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
	}
	if called {
		t.Fatalf("serve callback was called after project config delegated env; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	output := stdout.String() + stderr.String()
	for _, want := range []string{"env/unknown_stale", "SWARM_DB_PASSWORD", "unknown SWARM_* env is not accepted"} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "accepted by explicit typed config delegation") {
		t.Fatalf("project config incorrectly authorized typed env delegation:\n%s", output)
	}
}

func TestSwarmEnvGuardHonorsTypedDatabasePasswordEnvClearOverride(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_DB_PASSWORD", "secret")
	repo := t.TempDir()
	userPath := ignoredUserGlobalConfigPath(t)
	if err := os.MkdirAll(filepath.Dir(userPath), 0o755); err != nil {
		t.Fatalf("mkdir user global config: %v", err)
	}
	writeWorkflowValidationFixtureFile(t, userPath, "database:\n  password_env: SWARM_DB_PASSWORD\n")
	localDir := filepath.Join(repo, ".swarm")
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		t.Fatalf("mkdir local operator config: %v", err)
	}
	writeWorkflowValidationFixtureFile(t, filepath.Join(localDir, "swarm.yaml"), "database:\n  password_env: \"\"\n")

	called := false
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ InvocationRoot, _ ServeOptions) int {
		called = true
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"serve",
		"--api-listen-addr", "127.0.0.1:0",
		"--mcp-listen-addr", "127.0.0.1:0",
	}, &stdout, &stderr, opts)
	if code != CLIExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
	}
	if called {
		t.Fatalf("serve callback was called after typed env delegation was cleared; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	output := stdout.String() + stderr.String()
	for _, want := range []string{"env/unknown_stale", "SWARM_DB_PASSWORD", "unknown SWARM_* env is not accepted"} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "accepted by explicit typed config delegation") {
		t.Fatalf("cleared typed env delegation remained active:\n%s", output)
	}
}

func TestSwarmEnvGuardRejectsExecutableAdjacentTypedDatabasePasswordEnvDelegation(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_DB_PASSWORD", "secret")
	writeIgnoredExecutableConfig(t, "database:\n  password_env: SWARM_DB_PASSWORD\n")

	called := false
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ InvocationRoot, _ ServeOptions) int {
		called = true
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"serve", ".",
		"--api-listen-addr", "127.0.0.1:0",
		"--mcp-listen-addr", "127.0.0.1:0",
	}, &stdout, &stderr, opts)
	if code != CLIExitValidation {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitValidation, stdout.String(), stderr.String())
	}
	if called {
		t.Fatalf("serve callback was called after executable-adjacent config delegated env; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	for _, want := range []string{"SWARM_DB_PASSWORD", "unknown SWARM_* env is not accepted", "unknown_stale"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
		}
	}
}

func TestSwarmEnvGuardEmptyRetiredEnvUsesNonEmptyBoundary(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_RUNTIME_MAX_CONCURRENT_AGENTS", "")
	t.Setenv("SWARM_LLM_BACKEND", "")

	called := false
	opts := defaultRootCommandOptions()
	opts.runServe = func(_ context.Context, _ InvocationRoot, _ ServeOptions) int {
		called = true
		return 0
	}

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"serve", ".",
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
	t.Setenv("SWARM_MONITOR_DR", "stale")
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{
		"doctor",
		"--json",
		"--config", filepath.Join(t.TempDir(), "missing-runtime.yaml"),
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != CLIExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitRuntime, stdout.String(), stderr.String())
	}
	var report LocalPreflightReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("parse doctor JSON: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
	}
	if report.OK {
		t.Fatalf("doctor report OK=true, want blockers: %#v", report)
	}
	assertLocalPreflightFinding(t, report, localPreflightEnvPrerequisite, string(swarmEnvCategoryUnknownStale), "SWARM_MONITOR_DR")
	assertLocalPreflightFinding(t, report, localPreflightBackendPrerequisite, "config_load_failed", "missing-runtime.yaml")
}

func TestDoctorTargetReportsSwarmEnvFindings(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_MONITOR_DR", "stale")
	repo := writeDoctorTargetRepo(t)

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repo, []string{
		"doctor",
		"--target",
		"--json",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != CLIExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitRuntime, stdout.String(), stderr.String())
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
	assertDoctorTargetEnvFinding(t, report, string(swarmEnvCategoryUnknownStale), "SWARM_MONITOR_DR")
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
			}, &stdout, &stderr, defaultRootCommandOptions())
			if code != CLIExitRuntime {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, CLIExitRuntime, stdout.String(), stderr.String())
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
			assertDoctorTargetEnvFinding(t, report, string(swarmEnvCategoryUnknownStale), envName)
			if envName == "SWARM_CLAUDE_CLI_RETRIES" {
				finding := findDoctorTargetEnvFinding(t, report, string(swarmEnvCategoryUnknownStale), envName)
				if !strings.Contains(finding.Message, "unknown SWARM_* env is not accepted") {
					t.Fatalf("retired inert finding message = %q, want no replacement guidance", finding.Message)
				}
				if strings.Contains(finding.Message+finding.Remediation, "#1803") {
					t.Fatalf("retired inert finding leaks internal issue ref: %#v", finding)
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

	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "\n# fixture: "+
		name+`
stages: []
`)
	return root
}

func assertLocalPreflightFinding(t *testing.T, report LocalPreflightReport, category localPreflightCategory, code, messagePart string) {
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
