package cliapp

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestSingleDeploymentConfigSecretsSelectProvider(t *testing.T) {
	for _, selection := range []string{"deployment", "locator", "flag", "flag_over_invalid_locator"} {
		for _, profile := range []struct {
			backend string
			key     string
		}{
			{backend: "claude_cli", key: "CLAUDE_CODE_OAUTH_TOKEN"},
			{backend: "anthropic", key: "ANTHROPIC_API_KEY"},
			{backend: "openai_responses", key: "OPENAI_API_KEY"},
		} {
			for _, command := range []string{"list", "check"} {
				t.Run(selection+"/"+profile.backend+"/"+command, func(t *testing.T) {
					isolateCLIAPIConfigEnv(t)
					t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
					unsetSecretEnvForTest(t, "ANTHROPIC_API_KEY")
					unsetSecretEnvForTest(t, "OPENAI_API_KEY")
					unsetSecretEnvForTest(t, "CLAUDE_CODE_OAUTH_TOKEN")
					root := t.TempDir()
					writeSingleDeploymentConfig(t, filepath.Join(root, "swarm.yaml"), "ignored: [\n")
					writeSingleDeploymentConfig(t, ignoredUserGlobalConfigPath(t), "ignored: [\n")
					deployment := filepath.Join(root, ".swarm", "swarm.yaml")
					selected := deployment
					args := []string{"secrets", command, "--json"}
					if selection != "deployment" {
						writeSingleDeploymentConfig(t, deployment, "ignored: [\n")
						selected = filepath.Join(t.TempDir(), "selected.yaml")
						if selection == "locator" {
							t.Setenv("SWARM_CONFIG", selected)
						} else {
							locator := filepath.Join(t.TempDir(), "unselected.yaml")
							otherBackend := "claude_cli"
							if profile.backend == otherBackend {
								otherBackend = "anthropic"
							}
							body := "llm:\n  backend: " + otherBackend + "\n"
							if selection == "flag_over_invalid_locator" {
								body = "ignored: [\n"
							}
							writeSingleDeploymentConfig(t, locator, body)
							t.Setenv("SWARM_CONFIG", locator)
							args = append([]string{"--config", selected}, args...)
						}
					}
					writeSingleDeploymentConfig(t, selected, "llm:\n  backend: "+profile.backend+"\n")
					code, stdout, stderr := executeRootCommandWithInput(context.Background(), root, args, "")
					wantCode := 0
					if command == "check" {
						wantCode = CLIExitRuntime
					}
					if code != wantCode || stderr != "" {
						t.Fatalf("selected provider code=%d, want %d; stdout=%s stderr=%s", code, wantCode, stdout, stderr)
					}
					var records []secretRecord
					if command == "list" {
						var result secretsListResult
						if err := json.Unmarshal([]byte(stdout), &result); err != nil {
							t.Fatalf("decode list: %v; %s", err, stdout)
						}
						records = result.Secrets
					} else {
						var result secretsCheckResult
						if err := json.Unmarshal([]byte(stdout), &result); err != nil {
							t.Fatalf("decode check: %v; %s", err, stdout)
						}
						if result.OK {
							t.Fatalf("selected provider OK=%t for %s", result.OK, profile.backend)
						}
						records = result.Missing
					}
					if len(records) != 1 {
						t.Fatalf("selected provider requirements=%+v, want only %s", records, profile.key)
					}
					record := records[0]
					if record.Key != profile.key || record.Present || record.Source != "" || len(record.RequiredBy) != 1 || record.RequiredBy[0] != (secretRequirement{Kind: "provider", Name: profile.backend}) {
						t.Fatalf("selected provider requirement=%+v, want missing %s for %s", record, profile.key, profile.backend)
					}
				})
			}
		}
	}
}

func TestSingleDeploymentConfigSecretsExplicitProviderCredentialReadback(t *testing.T) {
	for _, profile := range []struct{ backend, key string }{
		{backend: "anthropic", key: "ANTHROPIC_API_KEY"},
		{backend: "openai_responses", key: "OPENAI_API_KEY"},
	} {
		t.Run(profile.backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			t.Setenv(profile.key, "unselected-environment-secret")
			root := t.TempDir()
			writeSingleDeploymentConfig(t, filepath.Join(root, ".swarm", "swarm.yaml"), "llm:\n  backend: claude_cli\n")
			selected := filepath.Join(t.TempDir(), "selected.yaml")
			writeSingleDeploymentConfig(t, selected, "llm:\n  backend: "+profile.backend+"\n")
			prefix := []string{"--config", selected, "secrets"}
			code, stdout, stderr := executeRootCommandWithInput(context.Background(), root, append(prefix, "check", "--json"), "")
			var missing secretsCheckResult
			if err := json.Unmarshal([]byte(stdout), &missing); err != nil || code != CLIExitRuntime || missing.OK || len(missing.Missing) != 1 || missing.Missing[0].Key != profile.key || missing.Missing[0].Present {
				t.Fatalf("environment must not satisfy selected provider: code=%d result=%+v error=%v stdout=%s stderr=%s", code, missing, err, stdout, stderr)
			}
			secret := "selected-file-provider-secret"
			code, stdout, stderr = executeRootCommandWithInput(context.Background(), root, append(prefix, "set", profile.key, "--stdin"), secret+"\n")
			if code != 0 || strings.Contains(stdout+stderr, secret) || strings.Contains(stdout+stderr, "unselected-environment-secret") {
				t.Fatalf("set failed/leaked value: code=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
			code, stdout, stderr = executeRootCommandWithInput(context.Background(), root, append(prefix, "list", "--present", "--json"), "")
			var listed secretsListResult
			if err := json.Unmarshal([]byte(stdout), &listed); err != nil || code != 0 || len(listed.Secrets) != 1 {
				t.Fatalf("list readback: code=%d result=%+v error=%v stdout=%s stderr=%s", code, listed, err, stdout, stderr)
			}
			record := listed.Secrets[0]
			if record.Key != profile.key || !record.Present || record.Source != "file" || !record.Writable || len(record.RequiredBy) != 1 || record.RequiredBy[0] != (secretRequirement{Kind: "provider", Name: profile.backend}) {
				t.Fatalf("file-backed provider readback=%+v", record)
			}
			if strings.Contains(stdout+stderr, secret) || strings.Contains(stdout+stderr, "unselected-environment-secret") {
				t.Fatalf("readback leaked secret: stdout=%s stderr=%s", stdout, stderr)
			}
			code, stdout, stderr = executeRootCommandWithInput(context.Background(), root, append(prefix, "check", "--json"), "")
			var present secretsCheckResult
			if err := json.Unmarshal([]byte(stdout), &present); err != nil || code != 0 || !present.OK || len(present.Missing) != 0 || strings.Contains(stdout+stderr, secret) || strings.Contains(stdout+stderr, "unselected-environment-secret") {
				t.Fatalf("check after set: code=%d result=%+v error=%v stdout=%s stderr=%s", code, present, err, stdout, stderr)
			}
		})
	}
}

func TestSingleDeploymentConfigSecretsRefuseInvalidSelectedFile(t *testing.T) {
	for _, invalid := range []struct{ name, body, diagnostic string }{
		{name: "missing"},
		{name: "parse", body: "llm: [\n"},
		{name: "backend", body: "llm:\n  backend: unsupported_backend\n", diagnostic: "unsupported_backend"},
	} {
		for _, selection := range []string{"locator", "flag"} {
			for _, command := range []string{"list", "check"} {
				t.Run(selection+"/"+invalid.name+"/"+command, func(t *testing.T) {
					isolateCLIAPIConfigEnv(t)
					t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
					root := t.TempDir()
					deployment := filepath.Join(root, ".swarm", "swarm.yaml")
					writeSingleDeploymentConfig(t, deployment, "llm:\n  backend: claude_cli\n")
					selected := filepath.Join(t.TempDir(), "selected.yaml")
					if invalid.name != "missing" {
						writeSingleDeploymentConfig(t, selected, invalid.body)
					}
					args := []string{"secrets", command, "--json"}
					if selection == "locator" {
						t.Setenv("SWARM_CONFIG", selected)
					} else {
						t.Setenv("SWARM_CONFIG", deployment)
						args = append([]string{"--config", selected}, args...)
					}
					code, stdout, stderr := executeRootCommandWithInput(context.Background(), root, args, "")
					wantDiagnostic := invalid.diagnostic
					if wantDiagnostic == "" {
						wantDiagnostic = selected
					}
					if code != CLIExitRuntime || stdout != "" || !strings.Contains(stderr, wantDiagnostic) {
						t.Fatalf("invalid selected file must refuse without fallback: code=%d stdout=%s stderr=%s", code, stdout, stderr)
					}
				})
			}
		}
	}
}
