package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
