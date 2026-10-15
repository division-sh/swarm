package releasee2e

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSingleDeploymentConfigReleaseSecretsReadback(t *testing.T) {
	binary := buildReleaseBinary(t, t.TempDir())
	for _, profile := range []struct{ backend, key string }{
		{"claude_cli", "CLAUDE_CODE_OAUTH_TOKEN"},
		{"anthropic", "ANTHROPIC_API_KEY"},
		{"openai_responses", "OPENAI_API_KEY"},
	} {
		for _, mode := range []string{"deployment", "locator", "explicit"} {
			t.Run(profile.backend+"/"+mode, func(t *testing.T) {
				root := goldenReleaseRoot(t)
				env := goldenProcessEnv(t, root, "", 1)
				env = append(env, "SWARM_CREDENTIALS_FILE="+filepath.Join(root, "credentials.json"))
				for _, path := range []string{"swarm.yaml", "home/.config/swarm/swarm.yaml", "home/.config/swarm/config.yaml"} {
					writeReleaseFile(t, filepath.Join(root, path), "llm: [\n")
				}
				selected := filepath.Join(root, ".swarm", "swarm.yaml")
				var selector []string
				if mode != "deployment" {
					writeReleaseFile(t, selected, "llm: [\n")
					selected = filepath.Join(root, "operator.yaml")
					if mode == "locator" {
						env = append(env, "SWARM_CONFIG=operator.yaml")
					} else {
						env = append(env, "SWARM_CONFIG=missing-unselected.yaml")
						selector = []string{"--config", "operator.yaml"}
					}
				}
				writeReleaseFile(t, selected, "llm:\n  backend: "+profile.backend+"\n")
				const environmentValue = "unselected-config-proof-environment-value"
				// API provider environment values require explicit delegation, not file selection.
				if profile.backend != "claude_cli" {
					env = append(env, profile.key+"="+environmentValue)
				}
				const fileValue = "selected-config-proof-file-value"
				run := func(command, input string, args ...string) releaseCommandResult {
					t.Helper()
					argv := append([]string{"secrets", command}, selector...)
					argv = append(argv, args...)
					result := runReleaseCommand(t, goldenStartupTimeout, root, env, input, binary, argv...)
					if strings.Contains(result.output, environmentValue) || strings.Contains(result.output, fileValue) {
						t.Fatal("compiled secrets command exposed a credential value")
					}
					return result
				}
				listed := run("list", "", "--json")
				assertDeploymentSecretReadback(t, listed, "list", profile.key, profile.backend, false)
				missing := run("check", "", "--json")
				assertDeploymentSecretReadback(t, missing, "check", profile.key, profile.backend, false)
				set := run("set", fileValue+"\n", profile.key, "--stdin")
				wantWriteMetadata := "source=file writable=yes shadowed=no present=yes"
				if profile.backend != "claude_cli" {
					wantWriteMetadata = "source=env writable=no shadowed=yes present=yes"
				}
				if set.err != nil || !strings.Contains(set.output, "key="+profile.key+" "+wantWriteMetadata) {
					t.Fatalf("compiled stdin credential write: %v\n%s", set.err, set.output)
				}
				present := run("list", "", "--present", "--json")
				assertDeploymentSecretReadback(t, present, "list", profile.key, profile.backend, true)
				checked := run("check", "", "--json")
				assertDeploymentSecretReadback(t, checked, "check", profile.key, profile.backend, true)
			})
		}
	}
	for _, invalid := range []struct{ name, body, diagnostic string }{
		{name: "missing"},
		{name: "parse", body: "llm: [\n"},
		{name: "backend", body: "llm:\n  backend: unsupported_backend\n", diagnostic: "unsupported_backend"},
	} {
		for _, mode := range []string{"locator", "explicit"} {
			t.Run("refusal/"+invalid.name+"/"+mode, func(t *testing.T) {
				root := goldenReleaseRoot(t)
				env := goldenProcessEnv(t, root, "", 1)
				env = append(env, "SWARM_CREDENTIALS_FILE="+filepath.Join(root, "credentials.json"))
				writeReleaseFile(t, filepath.Join(root, ".swarm", "swarm.yaml"), "llm:\n  backend: claude_cli\n")
				selected := filepath.Join(root, "operator.yaml")
				if invalid.name != "missing" {
					writeReleaseFile(t, selected, invalid.body)
				}
				var selector []string
				if mode == "locator" {
					env = append(env, "SWARM_CONFIG=operator.yaml")
				} else {
					env = append(env, "SWARM_CONFIG=.swarm/swarm.yaml")
					selector = []string{"--config", "operator.yaml"}
				}
				for _, command := range []string{"list", "check"} {
					result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, append([]string{"secrets", command, "--json"}, selector...)...)
					exit, exited := result.err.(*exec.ExitError)
					want := invalid.diagnostic
					if want == "" {
						want = selected
					}
					if !exited || exit.ExitCode() != 3 || !strings.Contains(result.output, want) || json.Valid([]byte(result.output)) {
						t.Fatalf("compiled %s must refuse the selected config without fallback: %v\n%s", command, result.err, result.output)
					}
				}
			})
		}
	}
}

func assertDeploymentSecretReadback(t *testing.T, result releaseCommandResult, command, key, backend string, present bool) {
	t.Helper()
	var report struct {
		OK      bool
		Secrets []struct {
			Key, Source string
			Writable    bool
			Present     bool
			RequiredBy  []struct{ Kind, Name string } `json:"required_by"`
		}
		Missing []struct {
			Key, Source string
			Present     bool
			RequiredBy  []struct{ Kind, Name string } `json:"required_by"`
		}
	}
	if err := json.Unmarshal([]byte(result.output), &report); err != nil {
		t.Fatalf("decode compiled secrets %s: %v\n%s", command, err, result.output)
	}
	if command == "check" && !present {
		exit, exited := result.err.(*exec.ExitError)
		if !exited || exit.ExitCode() != 3 || report.OK || len(report.Missing) != 1 {
			t.Fatalf("compiled selected requirement: %v\n%s", result.err, result.output)
		}
		record := report.Missing[0]
		if record.Key != key || record.Present || record.Source != "" || len(record.RequiredBy) != 1 || record.RequiredBy[0].Kind != "provider" || record.RequiredBy[0].Name != backend {
			t.Fatalf("wrong selected missing requirement: %s", result.output)
		}
		return
	}
	if result.err != nil {
		t.Fatalf("compiled secrets %s: %v\n%s", command, result.err, result.output)
	}
	if command == "check" {
		if !report.OK || len(report.Missing) != 0 {
			t.Fatalf("compiled check lost file-backed credential: %s", result.output)
		}
		return
	}
	if len(report.Secrets) != 1 {
		t.Fatalf("compiled list must name only the selected provider: %s", result.output)
	}
	record := report.Secrets[0]
	wantSource := ""
	if present {
		wantSource = "file"
	}
	if record.Key != key || record.Present != present || record.Source != wantSource || (present && !record.Writable) || len(record.RequiredBy) != 1 || record.RequiredBy[0].Kind != "provider" || record.RequiredBy[0].Name != backend {
		t.Fatalf("wrong selected provider credential readback: %s", result.output)
	}
}
