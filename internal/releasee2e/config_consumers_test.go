package releasee2e

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSingleDeploymentConfigReleaseReadConsumers(t *testing.T) {
	binary := buildReleaseBinary(t, t.TempDir())
	for _, mode := range []string{"deployment", "locator", "explicit"} {
		t.Run(mode, func(t *testing.T) {
			base := goldenReleaseRoot(t)
			root := filepath.Join(base, "invocation")
			source := filepath.Join(base, "source")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "examples/routing/root-ingress"), source)
			env := goldenProcessEnv(t, root, "", 1)
			// Neither an ancestor nor the source operand owns deployment discovery.
			for _, path := range []string{
				filepath.Join(base, ".swarm", "swarm.yaml"),
				filepath.Join(source, ".swarm", "swarm.yaml"),
				filepath.Join(root, "swarm.yaml"),
				filepath.Join(root, "home", ".config", "swarm", "swarm.yaml"),
				filepath.Join(root, "home", ".config", "swarm", "config.yaml"),
			} {
				writeReleaseFile(t, path, "runtime: null\n")
			}
			configPath := filepath.Join(root, ".swarm", "swarm.yaml")
			var selector []string
			if mode != "deployment" {
				writeReleaseFile(t, configPath, "runtime: null\n")
				configPath = filepath.Join(base, "deployment", "operator.yaml")
				if mode == "locator" {
					env = append(env, "SWARM_CONFIG=../deployment/operator.yaml")
				} else {
					selector = []string{"--config", "../deployment/operator.yaml"}
					env = append(env, "SWARM_CONFIG=missing-unselected.yaml")
				}
			}
			writeReleaseFile(t, configPath, "paths:\n  swarm_dir: selected-state\n")
			commands := []struct {
				name string
				args []string
			}{
				{"verify", []string{"verify", source, "--portable", "--json"}},
				{"describe", []string{"describe", source, "--json"}},
				{"routes", []string{"describe", "routes", source, "--json"}},
			}
			for _, command := range commands {
				t.Run("source/"+command.name, func(t *testing.T) {
					result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, append(command.args, selector...)...)
					var report struct {
						OK              bool
						BundleHash      string `json:"bundle_hash"`
						SourceHash      string `json:"source_hash"`
						SourceRoot      string `json:"source_root"`
						ValidationScope string `json:"validation_scope"`
						LiveReadiness   string `json:"live_readiness"`
						ProjectionOnly  bool   `json:"projection_only"`
						SchemaVersion   string `json:"schema_version"`
					}
					if err := json.Unmarshal([]byte(result.output), &report); err != nil || result.err != nil {
						t.Fatalf("compiled source consumer: command=%v json=%v\n%s", result.err, err, result.output)
					}
					switch command.name {
					case "verify":
						if !report.OK || report.SourceRoot != source || !strings.HasPrefix(report.BundleHash, "bundle-v2:sha256:") {
							t.Fatalf("wrong verification source: %+v", report)
						}
					case "routes":
						if !report.ProjectionOnly || report.SchemaVersion != "routing-topology/v1" {
							t.Fatalf("wrong routing projection: %+v", report)
						}
					default:
						if !strings.HasPrefix(report.SourceHash, "bundle-v2:sha256:") {
							t.Fatalf("missing describe source identity: %+v", report)
						}
					}
					if command.name != "routes" && (report.ValidationScope != "structural" || report.LiveReadiness != "not_evaluated") {
						t.Fatalf("structural read claimed deployment readiness: %+v", report)
					}
				})
			}
			registry := filepath.Join(root, "selected-state", "contexts")
			writeReleaseFile(t, registry, "not a registry directory\n")
			for _, command := range []string{"list", "current"} {
				t.Run("registry/"+command, func(t *testing.T) {
					args := append([]string{"context", command, "--json"}, selector...)
					result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, args...)
					var report struct {
						Status, Detail string
						Current        json.RawMessage
						Entries        []json.RawMessage
					}
					if err := json.Unmarshal([]byte(result.output), &report); err != nil || result.err != nil || report.Status != "invalid_descriptor" || !strings.Contains(report.Detail, registry) || len(report.Current) != 0 || len(report.Entries) != 0 {
						t.Fatalf("wrong selected registry: command=%v json=%v\n%s", result.err, err, result.output)
					}
					if raw, err := os.ReadFile(registry); err != nil || string(raw) != "not a registry directory\n" {
						t.Fatalf("inspection changed registry bytes: %q %v", raw, err)
					}
				})
			}
			writeReleaseFile(t, configPath, "paths:\n  tooling_lock_file: null\n")
			for _, args := range [][]string{
				{"verify", "missing-source", "--portable", "--json"},
				{"describe", "missing-source", "--json"},
				{"describe", "routes", "missing-source", "--json"},
				{"context", "list", "--json"},
				{"context", "current", "--json"},
				{"test", "missing-source"},
			} {
				t.Run("refusal/"+strings.Join(args, " "), func(t *testing.T) {
					result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, append(args, selector...)...)
					exit, exited := result.err.(*exec.ExitError)
					if !exited || exit.ExitCode() != 2 {
						t.Fatalf("want config validation exit 2, got %v\n%s", result.err, result.output)
					}
					message := result.output
					if args[0] == "verify" {
						var report struct {
							OK                bool
							AdmissionComplete bool   `json:"admission_complete"`
							BundleHash        string `json:"bundle_hash"`
							Errors            []struct {
								CheckID string `json:"check_id"`
								Message string
							}
							Observations []struct {
								CheckID string `json:"check_id"`
								Status  string
							}
						}
						if err := json.Unmarshal([]byte(result.output), &report); err != nil || report.OK || report.AdmissionComplete || report.BundleHash != "" || len(report.Errors) != 1 || report.Errors[0].CheckID != "runtime_configuration" || len(report.Observations) == 0 {
							t.Fatalf("wrong prerequisite refusal: json=%v\n%s", err, result.output)
						}
						message = report.Errors[0].Message
						failed := 0
						for _, observation := range report.Observations {
							if observation.CheckID == "runtime_configuration" && observation.Status == "failed" {
								failed++
							} else if observation.Status != "not_run" {
								t.Errorf("check ran past refused configuration: %+v", observation)
							}
						}
						if failed != 1 {
							t.Errorf("want one failed config observation, got %d", failed)
						}
					}
					for _, want := range []string{configPath, `unknown config key "paths.tooling_lock_file"`, "line 2", "column 3"} {
						if !strings.Contains(message, want) {
							t.Errorf("source refusal missing %q\n%s", want, result.output)
						}
					}
					for _, premature := range []string{"resolve source directory", "invalid_descriptor", "serve ready"} {
						if strings.Contains(result.output, premature) {
							t.Errorf("work preceded config refusal: %s", result.output)
						}
					}
				})
			}
			for _, state := range []string{filepath.Join(root, ".swarm", "stores"), filepath.Join(root, "home", ".swarm"), filepath.Join(base, "deployment", "selected-state"), filepath.Join(source, ".swarm", "stores")} {
				if _, err := os.Stat(state); !os.IsNotExist(err) {
					t.Fatalf("read-only commands created state %s: %v", state, err)
				}
			}
			if entries, err := os.ReadDir(filepath.Dir(registry)); err != nil || len(entries) != 1 || entries[0].Name() != "contexts" {
				t.Fatalf("inspection added selected state: %v %v", entries, err)
			}
		})
	}
}
