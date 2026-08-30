package cliapp

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
	credentialsPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialsPath)

	secretValue := "super-secret-token"
	code, stdout, stderr := executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "set", "sendgrid_api_key", "--stdin"}, secretValue+"\n")
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

	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "list"}, "")
	if code != 0 {
		t.Fatalf("list code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	for _, want := range []string{"sendgrid_api_key", "file"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("list output missing %q:\n%s", want, stdout)
		}
	}
	if strings.Contains(stdout+stderr, secretValue) {
		t.Fatalf("list output leaked secret value stdout=%q stderr=%q", stdout, stderr)
	}

	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "rm", "sendgrid_api_key"}, "")
	if code != 0 {
		t.Fatalf("rm code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "present=no") || strings.Contains(stdout+stderr, secretValue) {
		t.Fatalf("rm output = stdout=%q stderr=%q", stdout, stderr)
	}
}

func TestSecretsCheckIncludesSelectedProviderCredential(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, "go.mod"), "module provider-secrets-test\n")
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
	t.Setenv("OPENAI_API_KEY", "env-only-openai-key")
	withUnifiedRuntimeConfig(t, strings.Join([]string{
		"llm:",
		"  backend: openai_responses",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	}, "\n")+"\n")

	code, stdout, stderr := executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "check", "--json"}, "")
	if code != CLIExitRuntime {
		t.Fatalf("initial check code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	var missing secretsCheckResult
	if err := json.Unmarshal([]byte(stdout), &missing); err != nil {
		t.Fatalf("decode check json: %v\n%s", err, stdout)
	}
	if missing.OK || len(missing.Missing) != 1 {
		t.Fatalf("initial check result = %+v", missing)
	}
	provider := missing.Missing[0]
	if provider.Key != "OPENAI_API_KEY" || provider.Present || provider.Source != "" {
		t.Fatalf("provider missing record = %+v, want file-backed missing OPENAI_API_KEY", provider)
	}
	if len(provider.RequiredBy) != 1 || provider.RequiredBy[0].Kind != "provider" || provider.RequiredBy[0].Name != "openai_responses" {
		t.Fatalf("provider required_by = %+v, want provider:openai_responses", provider.RequiredBy)
	}

	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "list", "--json"}, "")
	if code != 0 {
		t.Fatalf("list --missing code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	var listed secretsListResult
	if err := json.Unmarshal([]byte(stdout), &listed); err != nil {
		t.Fatalf("decode list json: %v\n%s", err, stdout)
	}
	if len(listed.Secrets) != 1 || listed.Secrets[0].Key != "OPENAI_API_KEY" || listed.Secrets[0].Present {
		t.Fatalf("list result = %+v, want missing OPENAI_API_KEY", listed)
	}

	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "set", "OPENAI_API_KEY", "--stdin"}, "stored-openai-key\n")
	if code != 0 {
		t.Fatalf("set provider code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	if strings.Contains(stdout+stderr, "stored-openai-key") || strings.Contains(stdout+stderr, "env-only-openai-key") {
		t.Fatalf("set provider output leaked secret stdout=%q stderr=%q", stdout, stderr)
	}

	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "check", "--json"}, "")
	if code != 0 {
		t.Fatalf("check after set code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	var ok secretsCheckResult
	if err := json.Unmarshal([]byte(stdout), &ok); err != nil {
		t.Fatalf("decode ok json: %v\n%s", err, stdout)
	}
	if !ok.OK || len(ok.Missing) != 0 {
		t.Fatalf("check after set result = %+v, want ok", ok)
	}
}

func TestSecretsListShowsEnvShadowingAndRemoveKeepsEnvEffective(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, "go.mod"), "module secrets-test\n")
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
	t.Setenv("SENDGRID_API_KEY", "env-secret-token")

	code, stdout, stderr := executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "set", "sendgrid_api_key"}, "file-secret-token\n")
	if code != 0 {
		t.Fatalf("set code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}

	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "list", "--json"}, "")
	if code != 0 {
		t.Fatalf("list code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	var listed secretsListResult
	if err := json.Unmarshal([]byte(stdout), &listed); err != nil {
		t.Fatalf("decode list json: %v\n%s", err, stdout)
	}
	record, ok := secretRecordByKey(listed.Secrets, "sendgrid_api_key")
	if !ok {
		t.Fatalf("list result missing sendgrid_api_key: %+v", listed)
	}
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

func TestSecretsCommandsIgnoreRepoDotEnv(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	unsetSecretEnvForTest(t, "SENDGRID_API_KEY")
	repo := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, "go.mod"), "module secrets-test\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, ".env"), "SENDGRID_API_KEY=repo-env-secret\nBROKEN\n")
	credentialsPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialsPath)

	code, stdout, stderr := executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "set", "sendgrid_api_key", "--stdin"}, "file-secret-token\n")
	if code != 0 {
		t.Fatalf("set code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	assertNoDotEnvLoadFailure(t, stdout+stderr)

	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "list", "--json"}, "")
	if code != 0 {
		t.Fatalf("list code = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	assertNoDotEnvLoadFailure(t, stdout+stderr)
	var listed secretsListResult
	if err := json.Unmarshal([]byte(stdout), &listed); err != nil {
		t.Fatalf("decode list json: %v\n%s", err, stdout)
	}
	record, ok := secretRecordByKey(listed.Secrets, "sendgrid_api_key")
	if !ok {
		t.Fatalf("list result missing sendgrid_api_key: %+v", listed)
	}
	if record.Source != "file" || record.Shadowed || !record.Writable || !record.Present {
		t.Fatalf("repo .env unexpectedly supplied or shadowed file-tier secret: %+v", record)
	}
	if strings.Contains(stdout+stderr, "repo-env-secret") || strings.Contains(stdout+stderr, "file-secret-token") {
		t.Fatalf("secrets output leaked secret value stdout=%q stderr=%q", stdout, stderr)
	}
}

func TestSecretsCheckIsSourceFree(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	if err := os.WriteFile(filepath.Join(repo, "package.yaml"), []byte("invalid: ["), 0o600); err != nil {
		t.Fatalf("write retired source canary: %v", err)
	}
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
	setCode, setOut, setErr := executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "set", "ANTHROPIC_API_KEY", "--stdin"}, "provider-key\n")
	if setCode != 0 {
		t.Fatalf("seed provider credential code=%d stdout=%q stderr=%q", setCode, setOut, setErr)
	}

	code, stdout, stderr := executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "check"}, "")
	if code != 0 || !strings.Contains(stdout, "all configured secret requirements present") || stderr != "" {
		t.Fatalf("source-free secrets check code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
}

func secretRecordByKey(records []secretRecord, key string) (secretRecord, bool) {
	for _, record := range records {
		if record.Key == key {
			return record, true
		}
	}
	return secretRecord{}, false
}

func unsetSecretEnvForTest(t *testing.T, key string) {
	t.Helper()
	old, ok := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unset %s: %v", key, err)
	}
	t.Cleanup(func() {
		if ok {
			_ = os.Setenv(key, old)
			return
		}
		_ = os.Unsetenv(key)
	})
}

func TestSecretsSetRejectsPlaintextArgvValues(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, "go.mod"), "module secrets-test\n")
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))

	code, stdout, stderr := executeRootCommandWithInput(context.Background(), repo, []string{"secrets", "set", "sendgrid_api_key", "plain-secret"}, "")
	if code != CLIExitValidation {
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
		return CLIExitValidation, stdout.String(), stderr.String()
	}
	if err := validateCLILoggingFlagPlacement(args); err != nil {
		stderr.WriteString(err.Error() + "\n")
		return CLIExitValidation, stdout.String(), stderr.String()
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

	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: secrets-command-fixture\n")
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
	return root
}

func writeProviderSecretsCommandContractsFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: provider-secrets-command-fixture\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), `
provider-agent:
  id: provider-agent
  role: provider
  intent: prompts/provider-agent.md
  model: regular
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "prompts", "provider-agent.md"), "Handle provider-backed work.\n")
	return root
}

func withUnifiedRuntimeConfig(t *testing.T, configText string) {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "swarm.yaml")
	writeRuntimeConfigText(t, configPath, configText)
	t.Setenv("SWARM_CONFIG", configPath)
}
