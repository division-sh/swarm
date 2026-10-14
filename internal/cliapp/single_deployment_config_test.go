package cliapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeSingleDeploymentConfig(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	writeRuntimeConfigText(t, path, body)
}

func TestSingleDeploymentConfigSelectsOnlyDeclaredFile(t *testing.T) {
	for _, selection := range []string{"defaults", "deployment", "locator", "flag"} {
		for _, ignored := range []string{"valid", "invalid"} {
			t.Run(selection+"/"+ignored, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				root := t.TempDir()
				globalBody := "llm:\n  session:\n    rotate_after_turns: 7\nconnection:\n  api_server: http://127.0.0.1:4101\n"
				projectBody := "runtime:\n  recovery_on_startup: false\nserve:\n  api_listen_addr: 127.0.0.1:4102\n"
				if ignored == "invalid" {
					globalBody, projectBody = "llm: [\n", "invented_project_setting: true\n"
				}
				global := filepath.Join(os.Getenv("XDG_CONFIG_HOME"), "swarm", "swarm.yaml")
				writeSingleDeploymentConfig(t, global, globalBody)
				writeSingleDeploymentConfig(t, filepath.Join(root, "swarm.yaml"), projectBody)
				deployment := filepath.Join(root, ".swarm", "swarm.yaml")
				selected := "runtime:\n  recovery_on_startup: false\nserve:\n  api_listen_addr: 127.0.0.1:4200\n"
				opts := RuntimeConfigLoadOptions{RepoRoot: root}
				wantPath := ""
				switch selection {
				case "deployment":
					writeSingleDeploymentConfig(t, deployment, selected)
					wantPath = deployment
				case "locator", "flag":
					writeSingleDeploymentConfig(t, deployment, "llm:\n  session:\n    rotate_after_turns: 8\n")
					wantPath = filepath.Join(t.TempDir(), "operator.yaml")
					writeSingleDeploymentConfig(t, wantPath, selected)
					if selection == "locator" {
						t.Setenv("SWARM_CONFIG", wantPath)
					} else {
						opts.ExplicitPath = wantPath
						locator := filepath.Join(t.TempDir(), "ignored-locator.yaml")
						writeSingleDeploymentConfig(t, locator, "unknown: true\n")
						t.Setenv("SWARM_CONFIG", locator)
					}
				}
				loaded, err := LoadRuntimeConfigWithOptions(opts)
				if err != nil {
					t.Fatalf("ignored files affected selected config: %v", err)
				}
				if loaded.Path != wantPath || loaded.DeploymentConfigured() != (wantPath != "") {
					t.Fatalf("source path/intent = %q/%t, want %q/%t", loaded.Path, loaded.DeploymentConfigured(), wantPath, wantPath != "")
				}
				if loaded.Config.LLM.Session.RotateAfterTurns != 40 || loaded.cli.Connection.APIServer != "" {
					t.Fatalf("ignored file supplied omitted values: rotate=%d server=%q", loaded.Config.LLM.Session.RotateAfterTurns, loaded.cli.Connection.APIServer)
				}
				wantListen := ""
				if wantPath != "" {
					wantListen = "127.0.0.1:4200"
				}
				if loaded.Config.Runtime.RecoveryOnStartup != (wantPath == "") || loaded.cli.Serve.APIListenAddr != wantListen {
					t.Fatalf("recovery/listener = %t/%q, want %t/%q", loaded.Config.Runtime.RecoveryOnStartup, loaded.cli.Serve.APIListenAddr, wantPath == "", wantListen)
				}
			})
		}
	}
}

func TestSingleDeploymentConfigDoesNotProbeDeletedLegacyLocations(t *testing.T) {
	for _, location := range []string{"xdg", "executable"} {
		t.Run(location, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root := t.TempDir()
			path := filepath.Join(os.Getenv("XDG_CONFIG_HOME"), "swarm", "config.yaml")
			if location == "executable" {
				bin := t.TempDir()
				path = filepath.Join(bin, "config.yaml")
				previous := runtimeConfigExecutablePath
				runtimeConfigExecutablePath = func() (string, error) { return filepath.Join(bin, "swarm"), nil }
				t.Cleanup(func() { runtimeConfigExecutablePath = previous })
			}
			writeSingleDeploymentConfig(t, path, "malformed: [\n")
			selected := filepath.Join(root, "selected.yaml")
			writeSingleDeploymentConfig(t, selected, "runtime:\n  recovery_on_startup: false\n")
			loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: selected})
			if err != nil || loaded.Path != selected || loaded.Config == nil || loaded.Config.Runtime.RecoveryOnStartup {
				t.Fatalf("unselected legacy file affected explicit selection: path=%q error=%v", loaded.Path, err)
			}
		})
	}
}

func TestSingleDeploymentConfigAdmitsConnectionFromDeploymentFile(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := t.TempDir()
	selected := filepath.Join(root, ".swarm", "swarm.yaml")
	writeSingleDeploymentConfig(t, selected, "connection:\n  api_server: http://127.0.0.1:4310\n  api_token_file: client-token\nserve:\n  api_token_file: server-token\n")
	loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.cli.Connection.APIServer != "http://127.0.0.1:4310" || loaded.cli.Connection.APITokenFile != "client-token" || loaded.cli.Serve.APITokenFile != "server-token" {
		t.Fatalf("deployment connection/server auth lost: %+v", loaded.cli)
	}
}

func TestSingleDeploymentConfigRefusesInvalidSelectedFile(t *testing.T) {
	for _, body := range []string{"missing", "serve: [\n", "serve:\n  invented: true\n"} {
		t.Run(body, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root := t.TempDir()
			writeSingleDeploymentConfig(t, filepath.Join(root, ".swarm", "swarm.yaml"), "{}\n")
			path := filepath.Join(root, "selected.yaml")
			if body != "missing" {
				writeSingleDeploymentConfig(t, path, body)
			}
			_, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: path})
			if err == nil || !strings.Contains(err.Error(), path) {
				t.Fatalf("invalid selected file did not refuse with its source: %v", err)
			}
		})
	}
}

func TestSingleDeploymentConfigDoesNotTreatDefaultDirectoryAsAbsence(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := t.TempDir()
	path := filepath.Join(root, ".swarm", "swarm.yaml")
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root})
	if err == nil || !strings.Contains(err.Error(), path) {
		t.Fatalf("existing invalid default path silently selected built-in defaults: %v", err)
	}
}

func TestSingleDeploymentConfigDoesNotTreatBrokenSymlinkAsAbsence(t *testing.T) {
	for _, target := range []string{"missing.yaml", "swarm.yaml"} {
		t.Run(target, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root := t.TempDir()
			path := filepath.Join(root, ".swarm", "swarm.yaml")
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
			_, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root})
			if err == nil || !strings.Contains(err.Error(), path) {
				t.Fatalf("broken deployment entry silently selected defaults: %v", err)
			}
		})
	}
}

func TestSingleDeploymentConfigReadsValidDefaultSymlink(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := t.TempDir()
	selected := filepath.Join(root, ".swarm", "swarm.yaml")
	target := filepath.Join(t.TempDir(), "operator.yaml")
	writeSingleDeploymentConfig(t, target, "runtime:\n  recovery_on_startup: false\n")
	if err := os.MkdirAll(filepath.Dir(selected), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, selected); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root})
	if err != nil || loaded.Path != selected || !loaded.DeploymentConfigured() || loaded.Config.Runtime.RecoveryOnStartup {
		t.Fatalf("valid selected symlink lost path, intent or contents: path=%q error=%v", loaded.Path, err)
	}
}

func TestSingleDeploymentConfigKeepsSecretAndSplitRefusals(t *testing.T) {
	for _, selection := range []string{"deployment", "locator", "flag"} {
		for _, row := range []struct{ body, key, refusal string }{
			{"database: {password: private-value-not-for-diagnostics}\n", "database.password", "plaintext secret"},
			{"runtime: {max_concurrent_agents: 2}\n", "runtime.max_concurrent_agents", "recognized but not yet supported"},
			{"llm: {claude_cli: {retries: 2}}\n", "llm.claude_cli.retries", "recognized but not yet supported"},
		} {
			t.Run(selection+"/"+row.key, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				root := t.TempDir()
				selected := filepath.Join(root, ".swarm", "swarm.yaml")
				opts := RuntimeConfigLoadOptions{RepoRoot: root}
				if selection != "deployment" {
					selected = filepath.Join(root, "operator.yaml")
					if selection == "locator" {
						t.Setenv("SWARM_CONFIG", selected)
					} else {
						opts.ExplicitPath = selected
					}
				}
				writeSingleDeploymentConfig(t, selected, row.body)
				loaded, err := LoadRuntimeConfigWithOptions(opts)
				if err == nil || !strings.Contains(err.Error(), row.key) || !strings.Contains(err.Error(), row.refusal) || strings.Contains(err.Error(), "private-value-not-for-diagnostics") {
					t.Fatalf("secret or unsupported key refusal changed: %v", err)
				}
				found := false
				for _, diagnostic := range loaded.Diagnostics {
					if diagnostic.blocker() && diagnostic.Key == row.key && diagnostic.Path == selected {
						found = true
					}
				}
				if !found {
					t.Fatalf("refusal lost selected path/key: %#v", loaded.Diagnostics)
				}
			})
		}
	}
}

func TestSingleDeploymentConfigNullContainerRefusalNamesSelectedFile(t *testing.T) {
	for _, selection := range []string{"deployment", "locator", "flag"} {
		for _, row := range []struct{ body, key string }{
			{"runtime: null\n", "runtime"},
			{"llm:\n  provider_limits: null\n", "llm.provider_limits"},
			{"platform:\n  packs:\n    platform_dirs: null\n", "platform.packs.platform_dirs"},
			{"connection: null\n", "connection"},
			{"serve: null\n", "serve"},
			{"paths: null\n", "paths"},
		} {
			t.Run(selection+"/"+row.key, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				root := t.TempDir()
				selected := filepath.Join(root, ".swarm", "swarm.yaml")
				opts := RuntimeConfigLoadOptions{RepoRoot: root}
				if selection != "deployment" {
					selected = filepath.Join(root, "operator.yaml")
					if selection == "locator" {
						t.Setenv("SWARM_CONFIG", selected)
					} else {
						opts.ExplicitPath = selected
					}
				}
				writeSingleDeploymentConfig(t, selected, row.body)
				loaded, err := LoadRuntimeConfigWithOptions(opts)
				if err == nil || !strings.Contains(err.Error(), selected) {
					t.Fatalf("selected null container admitted or lost its file: %v", err)
				}
				found := false
				for _, diagnostic := range loaded.Diagnostics {
					if diagnostic.Kind == unifiedConfigDiagnosticParseFailed && diagnostic.Path == selected && diagnostic.Key == row.key && diagnostic.Line > 0 && diagnostic.Column > 0 && strings.Contains(diagnostic.Remediation, "omit") {
						found = true
					}
				}
				if !found {
					t.Fatalf("null refusal lost key, coordinates or teaching: %#v", loaded.Diagnostics)
				}
			})
		}
	}
}
