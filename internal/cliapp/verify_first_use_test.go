package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
)

func issue2567VerifySource(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for name, body := range map[string]string{
		"schema.yaml": "name: first-verify\nstages: {waiting: {initial: true}, done: {terminal: true}}\npins: {inputs: [work.requested]}\n",
		"events.yaml": "work.requested:\n", "entities.yaml": "work: {}\n",
		"nodes.yaml": "worker:\n  execution_type: system_node\n  event_handlers:\n    work.requested: {advances_to: done}\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestIssue2567VerifyMissingStoreFormatsAndFlagPrecedence(t *testing.T) {
	for _, position := range []string{"before", "after", "config", "default"} {
		for _, mode := range []string{"json", "text", "quiet"} {
			t.Run(position+"/"+mode, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				root := issue2567VerifySource(t)
				home := t.TempDir()
				t.Setenv("HOME", home)
				t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
				configDir, flagDir := filepath.Join(t.TempDir(), "config"), filepath.Join(t.TempDir(), "flag")
				config := filepath.Join(root, "swarm.yaml")
				body := "serve: {api_listen_addr: '127.0.0.1:0', mcp_listen_addr: '127.0.0.1:0'}\n"
				wantDir := filepath.Join(home, ".swarm")
				if position != "default" {
					body += fmt.Sprintf("paths: {swarm_dir: %q}\n", configDir)
					wantDir = configDir
				}
				args := []string{"verify", root, "--config", config}
				if position == "before" {
					args = append([]string{"--swarm-dir", flagDir}, args...)
					wantDir = flagDir
				}
				if position == "after" {
					args = append(args, "--swarm-dir", flagDir)
					wantDir = flagDir
				}
				if mode != "text" {
					args = append(args, "--"+mode)
				}
				writeRuntimeConfigText(t, config, body)
				var out, errOut bytes.Buffer
				if code := executeRootCommand(context.Background(), RepoRoot(), args, &out, &errOut); code != 0 {
					t.Fatalf("exit=%d stdout=%s stderr=%s", code, &out, &errOut)
				}
				if mode == "json" {
					var result verifyCommandResult
					if err := json.Unmarshal(out.Bytes(), &result); err != nil {
						t.Fatal(err)
					}
					if !result.OK || result.AdmissionComplete || result.LiveReadiness != "not_evaluated" || result.ValidationScope != "deployment" {
						t.Fatalf("false readiness: %+v", result)
					}
					found := false
					independent := map[string]int{}
					for _, observation := range result.Observations {
						if observation.CheckID == "selected_store_access" {
							found = true
							if observation.Status != bootverify.AdmissionNotRun || observation.NotRunCause == nil || !strings.HasPrefix(observation.NotRunCause.Path, wantDir+string(filepath.Separator)) {
								t.Fatalf("missing exact selected absence: %+v", observation)
							}
						}
						if observation.Status == bootverify.AdmissionPassed {
							independent[observation.CheckID]++
						}
					}
					if !found {
						t.Fatal("missing absence observation")
					}
					for _, check := range []string{"serve_listener_configuration", "listener_cleanup", "declared_model_admission", "workspace_capability_admission", "native_tool_admission"} {
						if independent[check] == 0 {
							t.Fatalf("store-free check %s did not run: %v", check, independent)
						}
					}
					if independent["listener_availability"] != 2 {
						t.Fatalf("both listeners not observed: %v", independent)
					}
				} else if !strings.Contains(out.String(), "*") || !strings.Contains(out.String(), "store admission: not evaluated") || !strings.Contains(out.String(), wantDir) || !strings.Contains(out.String(), "created on first serve") {
					t.Fatalf("incomplete store hidden: %s", &out)
				}
				for _, path := range []string{configDir, flagDir, filepath.Join(home, ".swarm")} {
					if _, err := os.Lstat(path); !os.IsNotExist(err) {
						t.Fatalf("verify created %s: %v", path, err)
					}
				}
			})
		}
	}
}

func TestIssue2567VerifyExistingBadStoreAndIndependentFailureStillRefuse(t *testing.T) {
	for _, shape := range []string{"corrupt", "directory", "dangling_parent", "bad_binding", "blank_flag", "blank_config"} {
		t.Run(shape, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root := issue2567VerifySource(t)
			path := filepath.Join(t.TempDir(), "state.db")
			listeners := "api_listen_addr: '127.0.0.1:0', mcp_listen_addr: '127.0.0.1:0'"
			switch shape {
			case "corrupt":
				if err := os.WriteFile(path, []byte("not sqlite"), 0600); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "dangling_parent":
				if err := os.Symlink(filepath.Join(t.TempDir(), "missing"), path); err != nil {
					t.Fatal(err)
				}
				path = filepath.Join(path, "state.db")
			case "bad_binding":
				listeners = "api_listen_addr: 'not-an-address', mcp_listen_addr: '127.0.0.1:0'"
			}
			config := filepath.Join(root, "swarm.yaml")
			body := fmt.Sprintf("serve: {%s}\nstore: {backend: sqlite, sqlite: {path: %q}}\n", listeners, path)
			if shape == "blank_config" {
				body += "paths: {swarm_dir: ''}\n"
			}
			writeRuntimeConfigText(t, config, body)
			args := []string{"verify", root, "--config", config, "--json"}
			if shape == "blank_flag" {
				args = append(args, "--swarm-dir", "")
			}
			var out, errOut bytes.Buffer
			if code := executeRootCommand(context.Background(), RepoRoot(), args, &out, &errOut); code == 0 {
				t.Fatalf("bad %s admitted: %s", shape, &out)
			}
			var result verifyCommandResult
			if err := json.Unmarshal(out.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result.OK || result.AdmissionComplete {
				t.Fatalf("bad result admitted: %+v", result)
			}
			if shape == "bad_binding" {
				found := false
				for _, o := range result.Observations {
					if o.NotRunCause != nil {
						found = true
					}
				}
				if !found {
					t.Fatal("independent failure prevented absence observation")
				}
			}
		})
	}
}

func TestIssue2567VerifyDoctorRelativeSelectionAndExplicitStoreParity(t *testing.T) {
	for _, choice := range []string{"flag", "config", "default"} {
		for _, explicitStore := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%t", choice, explicitStore), func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				invocation, source, home := t.TempDir(), issue2567VerifySource(t), t.TempDir()
				t.Setenv("HOME", home)
				t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
				body := "serve: {api_listen_addr: '127.0.0.1:0', mcp_listen_addr: '127.0.0.1:0'}\n"
				configPath := filepath.Join(invocation, "swarm.yaml")
				wantDir := filepath.Join(home, ".swarm")
				var flags []string
				if choice != "default" {
					body += "paths: {swarm_dir: config-state}\n"
					wantDir = filepath.Join(invocation, "config-state")
				}
				if choice == "flag" {
					flags = []string{"--swarm-dir", "flag-state"}
					wantDir = filepath.Join(invocation, "flag-state")
				}
				canonicalSource, _ := canonicalizeDoctorTargetPath(source)
				wantStore := filepath.Join(wantDir, "stores", "projects", localRuntimeProjectKey(canonicalSource), "dev.db")
				if explicitStore {
					body += "store: {backend: sqlite, sqlite: {path: explicit-state/store.db}}\n"
					wantStore = filepath.Join(invocation, "explicit-state", "store.db")
				}
				writeRuntimeConfigText(t, configPath, body)
				var verifyOut, doctorOut, errOut bytes.Buffer
				args := append([]string{"verify", source, "--config", configPath, "--json"}, flags...)
				if code := executeRootCommand(context.Background(), invocation, args, &verifyOut, &errOut); code != 0 {
					t.Fatalf("verify exit=%d: %s %s", code, &verifyOut, &errOut)
				}
				var result verifyCommandResult
				if err := json.Unmarshal(verifyOut.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				found := false
				for _, observation := range result.Observations {
					if observation.NotRunCause != nil {
						found = observation.NotRunCause.Path == wantStore
					}
				}
				if !found {
					t.Fatalf("wrong invocation-owned coordinate, want %s: %s", wantStore, &verifyOut)
				}
				args = append([]string{"doctor", "--target", "--config", configPath, "--json"}, flags...)
				if code := executeRootCommand(context.Background(), invocation, args, &doctorOut, &errOut); code != 0 {
					t.Fatalf("doctor exit=%d: %s %s", code, &doctorOut, &errOut)
				}
				var doctor doctorTargetReport
				if err := json.Unmarshal(doctorOut.Bytes(), &doctor); err != nil {
					t.Fatal(err)
				}
				if doctor.SwarmDir.Path != wantDir || (explicitStore && doctor.Store.Path != wantStore) {
					t.Fatalf("doctor/verify disagree: %+v; want %s/%s", doctor, wantDir, wantStore)
				}
				for _, path := range []string{wantDir, filepath.Dir(wantStore)} {
					if _, err := os.Lstat(path); !os.IsNotExist(err) {
						t.Fatalf("inspection created %s: %v", path, err)
					}
				}
			})
		}
	}
}

func TestIssue2567VerifyLocalProjectFirstUse(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root, home := issue2567VerifySource(t), t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	writeRuntimeConfigText(t, filepath.Join(root, "swarm.yaml"), "serve: {api_listen_addr: '127.0.0.1:0', mcp_listen_addr: '127.0.0.1:0'}\n")
	var out, errOut bytes.Buffer
	if code := executeRootCommand(context.Background(), root, []string{"verify", ".", "--json"}, &out, &errOut); code != 0 {
		t.Fatalf("first project verify exit=%d: %s %s", code, &out, &errOut)
	}
	var result verifyCommandResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	canonicalRoot, _ := canonicalizeDoctorTargetPath(root)
	wantStore := filepath.Join(canonicalRoot, projectSQLiteStoreRelativePath)
	found := false
	for _, observation := range result.Observations {
		if observation.NotRunCause != nil {
			found = observation.NotRunCause.Path == wantStore
		}
	}
	if !result.OK || result.AdmissionComplete || !found {
		t.Fatalf("wrong first-use project result: %s", &out)
	}
	for _, path := range []string{filepath.Join(root, ".swarm"), filepath.Join(home, ".swarm")} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("verify created %s: %v", path, err)
		}
	}
}
