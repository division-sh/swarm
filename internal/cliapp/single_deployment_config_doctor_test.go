package cliapp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSingleDeploymentConfigDoctorReportsOneSource(t *testing.T) {
	for _, target := range []bool{false, true} {
		for _, selection := range []string{"defaults", "deployment", "locator", "root_flag", "command_flag", "missing", "malformed", "runtime_invalid", "broken_link", "inspection_loop"} {
			t.Run(selection+"/target="+yesNo(target), func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				setDoctorEmptyProviderSecrets(t)
				root := t.TempDir()
				selected := ""
				source := unifiedConfigSourceDefaults
				present, admitted := false, true
				presenceKnown := true
				args := []string{"doctor", "--json"}
				if target {
					args = append(args, "--target")
				} else {
					args = append(args, "--api-listen-addr", "127.0.0.1:0", "--mcp-listen-addr", "127.0.0.1:0")
				}
				if selection != "defaults" {
					selected = filepath.Join(t.TempDir(), "operator.yaml")
					source, present = unifiedConfigSourceExplicit, true
					body := "{}\n"
					switch selection {
					case "deployment", "broken_link":
						selected = filepath.Join(root, ".swarm", "swarm.yaml")
						source = unifiedConfigSourceDeployment
					case "missing":
						present, admitted = false, false
					case "malformed":
						body, admitted = "llm: [\n", false
					case "runtime_invalid":
						body, admitted = "llm:\n  session:\n    rotate_after_turns: 0\n", false
					case "inspection_loop":
						loop := filepath.Join(root, "loop")
						if err := os.Symlink("loop", loop); err != nil {
							t.Fatal(err)
						}
						selected = filepath.Join(loop, "operator.yaml")
						present, admitted, presenceKnown = false, false, false
					}
					if selection == "broken_link" {
						if err := os.MkdirAll(filepath.Dir(selected), 0o700); err != nil {
							t.Fatal(err)
						}
						if err := os.Symlink(filepath.Join(root, "missing.yaml"), selected); err != nil {
							t.Fatal(err)
						}
						admitted = false
					} else if present {
						writeSingleDeploymentConfig(t, selected, body)
					}
					switch selection {
					case "deployment", "broken_link":
					case "locator":
						t.Setenv("SWARM_CONFIG", selected)
						writeSingleDeploymentConfig(t, filepath.Join(root, ".swarm", "swarm.yaml"), "ignored: [\n")
					case "command_flag":
						args = append(args, "--config", selected)
					default:
						locator := filepath.Join(t.TempDir(), "ignored.yaml")
						writeSingleDeploymentConfig(t, locator, "ignored: [\n")
						t.Setenv("SWARM_CONFIG", locator)
						args = append([]string{"--config", selected}, args...)
					}
				}
				code, stdout, stderr := executeRootCommandWithInput(context.Background(), root, args, "")
				wantCode := 0
				if !admitted {
					wantCode = CLIExitRuntime
				}
				if code != wantCode || stderr != "" {
					t.Fatalf("doctor code=%d want=%d stdout=%s stderr=%s", code, wantCode, stdout, stderr)
				}
				var report struct {
					OK           bool `json:"ok"`
					ConfigSource *struct {
						Source  string `json:"source"`
						Path    string `json:"path"`
						Present *bool  `json:"present"`
					} `json:"config_source"`
				}
				if err := json.Unmarshal([]byte(stdout), &report); err != nil {
					t.Fatalf("decode doctor JSON: %v; %s", err, stdout)
				}
				if report.OK != admitted || report.ConfigSource == nil {
					t.Fatalf("missing/incorrect single-source projection: %+v; %s", report, stdout)
				}
				got := report.ConfigSource
				if got.Source != source || got.Path != selected || (got.Present != nil) != presenceKnown || (presenceKnown && *got.Present != present) {
					t.Fatalf("config source=%+v, want source=%s path=%s present=%t", got, source, selected, present)
				}
				if strings.Contains(stdout, "key_origins") || strings.Contains(stdout, "layers") {
					t.Fatalf("retired detailed provenance returned: %s", stdout)
				}
			})
		}
	}
}

func TestSingleDeploymentConfigDoctorUnreadProjectNotice(t *testing.T) {
	for _, target := range []bool{false, true} {
		for _, format := range []string{"text", "json"} {
			for _, entry := range []string{"absent", "invalid", "inline_secret", "broken_link", "directory", "selected", "selected_alias", "selected_hardlink", "selected_invalid", "failed_selected"} {
				t.Run(entry+"/"+format+"/target="+yesNo(target), func(t *testing.T) {
					isolateCLIAPIConfigEnv(t)
					setDoctorEmptyProviderSecrets(t)
					root := t.TempDir()
					project := filepath.Join(root, "swarm.yaml")
					selected := filepath.Join(t.TempDir(), "operator.yaml")
					body := "{}\n"
					if entry == "failed_selected" || entry == "selected_invalid" {
						body = "runtime: null\n"
					}
					writeSingleDeploymentConfig(t, selected, body)
					wantNotice := entry != "absent" && entry != "directory" && entry != "selected" && entry != "selected_alias" && entry != "selected_hardlink" && entry != "selected_invalid"
					switch entry {
					case "absent":
					case "directory":
						if err := os.Mkdir(project, 0o700); err != nil {
							t.Fatal(err)
						}
					case "broken_link":
						if err := os.Symlink(filepath.Join(root, "missing.yaml"), project); err != nil {
							t.Fatal(err)
						}
					case "selected_alias":
						if err := os.Symlink(selected, project); err != nil {
							t.Fatal(err)
						}
					case "selected_hardlink":
						if err := os.Link(selected, project); err != nil {
							t.Fatal(err)
						}
					case "selected", "selected_invalid":
						selected = project
						writeSingleDeploymentConfig(t, selected, body)
					case "inline_secret":
						writeSingleDeploymentConfig(t, project, "database:\n  password: unread-secret-canary\n")
					default:
						writeSingleDeploymentConfig(t, project, "ignored: [\n")
					}
					args := []string{"--config", selected, "doctor"}
					if target {
						args = append(args, "--target")
					} else {
						args = append(args, "--api-listen-addr", "127.0.0.1:0", "--mcp-listen-addr", "127.0.0.1:0")
					}
					if format == "json" {
						args = append(args, "--json")
					}
					code, stdout, stderr := executeRootCommandWithInput(context.Background(), root, args, "")
					wantCode := 0
					if entry == "failed_selected" || entry == "selected_invalid" {
						wantCode = CLIExitRuntime
					}
					if code != wantCode || stderr != "" || strings.Contains(stdout, "unread-secret-canary") {
						t.Fatalf("unread root affected admission/leaked contents: code=%d stdout=%s stderr=%s", code, stdout, stderr)
					}
					count := strings.Count(stdout, "configuration lives in .swarm/swarm.yaml or --config")
					if count != boolCount(wantNotice) || (wantNotice && !strings.Contains(stdout, project+" is not read")) {
						t.Fatalf("unread-project notice count=%d want=%d: %s", count, boolCount(wantNotice), stdout)
					}
					if format == "text" && !strings.Contains(stdout, "config: "+selected) {
						t.Fatalf("compact selected source missing: %s", stdout)
					}
				})
			}
		}
	}
}

func TestSingleDeploymentConfigDoctorRefusesLaterInvalidDelegation(t *testing.T) {
	for _, body := range []string{
		"llm:\n  backend: unsupported_backend\n",
		"llm:\n  session:\n    rotate_after_turns: 0\n",
		"connection:\n  api_server: [not, text]\n",
	} {
		t.Run(body, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_SELECTED_PASSWORD", "delegated-secret-canary")
			root := t.TempDir()
			selected := filepath.Join(t.TempDir(), "operator.yaml")
			writeSingleDeploymentConfig(t, selected, "database:\n  password_env: SWARM_SELECTED_PASSWORD\n"+body)
			loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: selected})
			if err == nil {
				t.Fatal("later-invalid source was admitted by the command config owner")
			}
			for _, finding := range doctorSwarmEnvFindings(loaded) {
				if finding.Name == "SWARM_SELECTED_PASSWORD" && (finding.AcceptedBy != "" || finding.Category == swarmEnvCategoryTypedDelegation) {
					t.Fatalf("later-invalid file credited by doctor: %+v", finding)
				}
			}
			for _, target := range []bool{false, true} {
				args := []string{"--config", selected, "doctor", "--json"}
				if target {
					args = append(args, "--target")
				}
				code, stdout, stderr := executeRootCommandWithInput(context.Background(), root, args, "")
				if code != CLIExitRuntime || stderr != "" || strings.Contains(stdout, "delegated-secret-canary") || strings.Contains(stdout, "accepted by explicit typed config delegation") || strings.Contains(stdout, `"code": "config_loaded"`) {
					t.Fatalf("invalid doctor authority: code=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
			}
			code, stdout, stderr := executeRootCommandWithInput(context.Background(), root, []string{"--config", selected, "secrets", "list", "--json"}, "")
			if code != CLIExitRuntime || stdout != "" || strings.Contains(stderr, "delegated-secret-canary") {
				t.Fatalf("runtime-invalid file reached the command result: code=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
		})
	}
}

func TestSingleDeploymentConfigDoctorUsesEffectiveBackendDelegation(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	setDoctorEmptyProviderSecrets(t)
	t.Setenv("SWARM_SELECTED_PASSWORD", "delegated-secret-canary")
	root := t.TempDir()
	selected := filepath.Join(t.TempDir(), "operator.yaml")
	writeSingleDeploymentConfig(t, selected, "database:\n  password_env: SWARM_SELECTED_PASSWORD\nllm:\n  backend: openai_compatible\n")
	if _, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: selected}); err == nil {
		t.Fatal("control needs invalid active provider config before the supported override")
	}
	for _, target := range []bool{false, true} {
		args := []string{"--config", selected, "doctor", "--backend", "anthropic", "--json"}
		if target {
			args = append(args, "--target")
		} else {
			args = append(args, "--api-listen-addr", "127.0.0.1:0", "--mcp-listen-addr", "127.0.0.1:0")
		}
		code, stdout, stderr := executeRootCommandWithInput(context.Background(), root, args, "")
		if code != 0 || stderr != "" || !strings.Contains(stdout, "accepted by explicit typed config delegation") || strings.Contains(stdout, "delegated-secret-canary") {
			t.Fatalf("effective provider override/delegation changed: code=%d stdout=%s stderr=%s", code, stdout, stderr)
		}
	}
}

func TestSingleDeploymentConfigDoctorDefaultsTextIsCompact(t *testing.T) {
	for _, target := range []bool{false, true} {
		t.Run("target="+yesNo(target), func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			setDoctorEmptyProviderSecrets(t)
			args := []string{"doctor"}
			if target {
				args = append(args, "--target")
			} else {
				args = append(args, "--api-listen-addr", "127.0.0.1:0", "--mcp-listen-addr", "127.0.0.1:0")
			}
			code, stdout, stderr := executeRootCommandWithInput(context.Background(), t.TempDir(), args, "")
			if code != 0 || stderr != "" || strings.Count(stdout, "built-in defaults") != 1 || !strings.Contains(stdout, "config: built-in defaults") {
				t.Fatalf("defaults source must be reported once: code=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
		})
	}
}

func TestSingleDeploymentConfigDoctorRuntimeBlockerNamesDocumentLocation(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	selected := filepath.Join(t.TempDir(), "operator.yaml")
	writeSingleDeploymentConfig(t, selected, "llm:\n  session:\n    rotate_after_turns: 0\n")
	loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: t.TempDir(), ExplicitPath: selected})
	if err == nil || loaded.validated {
		t.Fatalf("invalid runtime config was admitted: %v", err)
	}
	for _, diagnostic := range loaded.Diagnostics {
		if diagnostic.Kind == unifiedConfigDiagnosticValidationFailed && strings.Contains(diagnostic.Message, "llm.session.rotate_after_turns") {
			if diagnostic.Path != selected || diagnostic.Line < 1 || diagnostic.Column < 1 || !strings.Contains(diagnostic.Message, selected+":1:1:") {
				t.Fatalf("runtime blocker lost selected document location: %+v", diagnostic)
			}
			return
		}
	}
	t.Fatalf("runtime validation cause lost: %+v", loaded.Diagnostics)
}

func TestSingleDeploymentConfigDoctorRetainsCapturedAdmission(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_SELECTED_PASSWORD", "captured-secret-canary")
	root := t.TempDir()
	selected := filepath.Join(t.TempDir(), "operator.yaml")
	writeSingleDeploymentConfig(t, selected, "database:\n  password_env: SWARM_SELECTED_PASSWORD\n")
	loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: selected})
	if err != nil {
		t.Fatal(err)
	}
	writeSingleDeploymentConfig(t, selected, "runtime: null\n")
	findings := doctorSwarmEnvFindings(loaded)
	accepted := false
	for _, finding := range findings {
		if finding.Name == "SWARM_SELECTED_PASSWORD" {
			accepted = finding.Category == swarmEnvCategoryTypedDelegation && finding.AcceptedBy == "database.password_env"
		}
	}
	var report LocalPreflightReport
	addDoctorConfigReadback(&report, root, loaded)
	if !accepted || !loaded.validated || report.ConfigSource == nil || report.ConfigSource.Path != selected || report.HasBlockers() {
		t.Fatalf("doctor reread or discarded captured admission: %+v; %+v", report, findings)
	}
	if _, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: selected}); err == nil {
		t.Fatal("replacement file must actually refuse if reread")
	}
}

func TestSingleDeploymentConfigDoctorTargetOmitsUninspectedFacts(t *testing.T) {
	for _, format := range []string{"text", "json"} {
		for _, selection := range []string{"missing", "malformed", "runtime_invalid", "shape_invalid", "null_container", "broken_link", "inspection_loop", "valid_with_env_blocker"} {
			t.Run(selection+"/"+format, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				setDoctorEmptyProviderSecrets(t)
				t.Setenv("SWARM_MONITOR_DR", "unread-env-value-canary")
				root := t.TempDir()
				selected := filepath.Join(t.TempDir(), "operator.yaml")
				swarmDir := filepath.Join(t.TempDir(), "uninspected-state")
				body := "{}\n"
				switch selection {
				case "malformed":
					body = "llm: [\n"
				case "runtime_invalid":
					body = "llm:\n  session:\n    rotate_after_turns: 0\n"
				case "shape_invalid":
					body = "connection:\n  api_server: [not, text]\n"
				case "null_container":
					body = "runtime: null\n"
				case "broken_link":
					if err := os.Symlink(filepath.Join(root, "missing.yaml"), selected); err != nil {
						t.Fatal(err)
					}
				case "inspection_loop":
					loop := filepath.Join(root, "loop")
					if err := os.Symlink("loop", loop); err != nil {
						t.Fatal(err)
					}
					selected = filepath.Join(loop, "operator.yaml")
				}
				if selection != "missing" && selection != "broken_link" && selection != "inspection_loop" {
					writeSingleDeploymentConfig(t, selected, body)
				}
				args := []string{"--config", selected, "--swarm-dir", swarmDir, "doctor", "--target"}
				if format == "json" {
					args = append(args, "--json")
				}
				code, stdout, stderr := executeRootCommandWithInput(context.Background(), root, args, "")
				if code != CLIExitRuntime || stderr != "" || strings.Contains(stdout, "unread-env-value-canary") {
					t.Fatalf("structured refusal changed: code=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
				if !strings.Contains(stdout, selected) || !strings.Contains(stdout, "SWARM_MONITOR_DR") {
					t.Fatalf("source/env findings missing: %s", stdout)
				}
				inspected := selection == "valid_with_env_blocker"
				if format == "json" {
					var report map[string]json.RawMessage
					if err := json.Unmarshal([]byte(stdout), &report); err != nil {
						t.Fatalf("decode target refusal: %v; %s", err, stdout)
					}
					for _, key := range []string{"owner", "mode", "ok", "env", "config", "config_source"} {
						if _, exists := report[key]; !exists {
							t.Fatalf("missing refusal field %q: %s", key, stdout)
						}
					}
					for _, key := range []string{"swarm_dir", "project", "api", "context", "project_owner", "runtime_identity", "store", "data", "command_classes", "split_siblings"} {
						if _, exists := report[key]; exists != inspected {
							t.Fatalf("target fact %q present=%t, inspected=%t: %s", key, exists, inspected, stdout)
						}
					}
					var typed doctorTargetReport
					if err := json.Unmarshal([]byte(stdout), &typed); err != nil || typed.OK || typed.Owner != localTargetOwner || typed.Mode != "target" || typed.ConfigSource == nil {
						t.Fatalf("target report identity/refusal changed: %+v; %v", typed, err)
					}
					configReport := LocalPreflightReport{Findings: typed.Config}
					if !inspected && !configReport.HasBlockers() {
						t.Fatalf("config refusal lost its blocker: %s", stdout)
					}
				} else {
					for _, label := range []string{"swarm_dir:", "project_root:", "api_server:", "rpc_endpoint:", "api_auth:", "target_reason:", "project_context:", "Project owner:", "selected_global_context:", "descriptor_registry:", "runtime_identity:", "store_path:", "data_dir:", "command_classes:", "split_siblings:"} {
						if strings.Contains(stdout, label) != inspected {
							t.Fatalf("target fact %q rendered, inspected=%t: %s", label, inspected, stdout)
						}
					}
					if !strings.Contains(stdout, "swarm target diagnostics: failed") || !strings.Contains(stdout, "env:\n") || !strings.Contains(stdout, "config:\n") {
						t.Fatalf("structured source/env refusal lost: %s", stdout)
					}
				}
				if !inspected {
					if _, err := os.Stat(swarmDir); !os.IsNotExist(err) {
						t.Fatalf("refused config touched target state: %v", err)
					}
				}
			})
		}
	}
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}
