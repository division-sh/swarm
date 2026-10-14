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
