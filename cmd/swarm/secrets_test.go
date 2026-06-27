package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSecretsSetListCheckAndRemoveUseFileTierWithoutLeakingValues(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, "go.mod"), "module secrets-test\n")
	contractsRoot := writeSecretsCommandContractsFixture(t)
	credentialsPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialsPath)

	code, stdout, stderr := executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "check", "--contracts", contractsRoot, "--json"}, "")
	if code != cliExitRuntime {
		t.Fatalf("initial check code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	var missing secretsCheckResult
	if err := json.Unmarshal([]byte(stdout), &missing); err != nil {
		t.Fatalf("decode check json: %v\n%s", err, stdout)
	}
	if missing.OK || len(missing.Missing) != 1 || missing.Missing[0].Key != "sendgrid_api_key" {
		t.Fatalf("initial check result = %+v", missing)
	}

	secretValue := "super-secret-token"
	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "set", "sendgrid_api_key", "--stdin"}, secretValue+"\n")
	if code != 0 {
		t.Fatalf("set code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	if strings.Contains(stdout+stderr, secretValue) {
		t.Fatalf("set output leaked secret value stdout=%q stderr=%q", stdout, stderr)
	}
	raw, err := os.ReadFile(credentialsPath)
	if err != nil {
		t.Fatalf("read credentials file: %v", err)
	}
	if !strings.Contains(string(raw), secretValue) {
		t.Fatalf("credential file did not receive secret value: %s", string(raw))
	}

	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "list", "--contracts", contractsRoot}, "")
	if code != 0 {
		t.Fatalf("list code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	for _, want := range []string{"sendgrid_api_key", "file", "tool:email_api"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("list output missing %q:\n%s", want, stdout)
		}
	}
	if strings.Contains(stdout+stderr, secretValue) {
		t.Fatalf("list output leaked secret value stdout=%q stderr=%q", stdout, stderr)
	}

	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "check", "--contracts", contractsRoot}, "")
	if code != 0 {
		t.Fatalf("check code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "all required secrets present") {
		t.Fatalf("check output = %s", stdout)
	}

	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "rm", "sendgrid_api_key"}, "")
	if code != 0 {
		t.Fatalf("rm code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "present=no") || strings.Contains(stdout+stderr, secretValue) {
		t.Fatalf("rm output = stdout=%q stderr=%q", stdout, stderr)
	}

	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "check", "--contracts", contractsRoot}, "")
	if code != cliExitRuntime {
		t.Fatalf("check after rm code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "missing required secrets") || !strings.Contains(stdout, "sendgrid_api_key") {
		t.Fatalf("check after rm output = %s", stdout)
	}
}

func TestSecretsListShowsEnvShadowingAndRemoveKeepsEnvEffective(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, "go.mod"), "module secrets-test\n")
	contractsRoot := writeSecretsCommandContractsFixture(t)
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
	t.Setenv("SENDGRID_API_KEY", "env-secret-token")

	code, stdout, stderr := executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "set", "sendgrid_api_key"}, "file-secret-token\n")
	if code != 0 {
		t.Fatalf("set code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}

	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "list", "--contracts", contractsRoot, "--json"}, "")
	if code != 0 {
		t.Fatalf("list code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	var listed secretsListResult
	if err := json.Unmarshal([]byte(stdout), &listed); err != nil {
		t.Fatalf("decode list json: %v\n%s", err, stdout)
	}
	if len(listed.Secrets) != 1 {
		t.Fatalf("list result = %+v", listed)
	}
	record := listed.Secrets[0]
	if record.Source != "env" || !record.Shadowed || record.Writable || !record.Present {
		t.Fatalf("shadowed record = %+v", record)
	}
	if strings.Contains(stdout+stderr, "env-secret-token") || strings.Contains(stdout+stderr, "file-secret-token") {
		t.Fatalf("list leaked secret value stdout=%q stderr=%q", stdout, stderr)
	}

	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "rm", "sendgrid_api_key"}, "")
	if code != 0 {
		t.Fatalf("rm code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	for _, want := range []string{"source=env", "shadowed=no", "present=yes"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("rm output missing %q:\n%s", want, stdout)
		}
	}
}

func TestSecretsSetRejectsPlaintextArgvValues(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, "go.mod"), "module secrets-test\n")
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))

	code, stdout, stderr := executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "set", "sendgrid_api_key", "plain-secret"}, "")
	if code != cliExitValidation {
		t.Fatalf("positional code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stderr, "not argv") {
		t.Fatalf("positional stderr = %s", stderr)
	}
	if strings.Contains(stdout+stderr, "plain-secret") {
		t.Fatalf("positional output leaked secret stdout=%q stderr=%q", stdout, stderr)
	}

	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "set", "sendgrid_api_key", "--value", "plain-secret"}, "")
	if code != 2 {
		t.Fatalf("--value code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	if strings.Contains(stdout+stderr, "plain-secret") {
		t.Fatalf("--value output leaked secret stdout=%q stderr=%q", stdout, stderr)
	}
}

func executeRootCommandWithInput(ctx context.Context, repo string, args []string, input string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	if err := validateCLIAPIConnectionFlagPlacement(args); err != nil {
		stderr.WriteString(err.Error() + "\n")
		return cliExitValidation, stdout.String(), stderr.String()
	}
	if err := validateCLILoggingFlagPlacement(args); err != nil {
		stderr.WriteString(err.Error() + "\n")
		return cliExitValidation, stdout.String(), stderr.String()
	}
	cmd := newRootCommandWithOptions(ctx, repo, &stdout, &stderr, defaultRootCommandOptions())
	cmd.SetArgs(args)
	cmd.SetIn(strings.NewReader(input))
	if err := cmd.ExecuteContext(ctx); err != nil {
		if exit, ok := err.(commandExitError); ok {
			return exit.code, stdout.String(), stderr.String()
		}
		stderr.WriteString(err.Error() + "\n")
		return 2, stdout.String(), stderr.String()
	}
	return 0, stdout.String(), stderr.String()
}

func writeSecretsCommandContractsFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: secrets-command-fixture
version: "1.0.0"
platform: ">=1.6.0"
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: secrets-command-fixture\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `
email_api:
  description: Send email through a provider.
  handler_type: http
  input_schema:
    type: object
    properties: {}
  http:
    method: POST
    url: https://email.example.test/send
  credentials:
    - sendgrid_api_key
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	return root
}
