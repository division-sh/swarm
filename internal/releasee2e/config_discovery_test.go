package releasee2e

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSingleDeploymentConfigReleaseSelectionAndRefusal(t *testing.T) {
	binary := buildReleaseBinary(t, t.TempDir())
	type targetReport struct {
		OK           bool
		ConfigSource struct {
			Source  string
			Path    string
			Present *bool
		} `json:"config_source"`
		API    *struct{ Server string }
		Config []struct{ Code, Message, Severity, Status string }
	}
	for _, mode := range []string{"defaults", "deployment", "locator", "explicit"} {
		t.Run("selection/"+mode, func(t *testing.T) {
			root := goldenReleaseRoot(t)
			env := goldenProcessEnv(t, root, "", 1)
			for _, path := range []string{"swarm.yaml", "home/.config/swarm/swarm.yaml", "home/.config/swarm/config.yaml"} {
				writeReleaseFile(t, filepath.Join(root, path), "runtime: null\n# UNREAD_CONFIG_CONTENT\n")
			}
			args := []string{"doctor", "--target", "--json"}
			wantPath, wantSource, wantServer := "", "built-in default", "http://127.0.0.1:8081"
			if mode != "defaults" {
				wantPath = filepath.Join(root, ".swarm", "swarm.yaml")
				wantSource = "deployment_config"
				if mode != "deployment" {
					writeReleaseFile(t, wantPath, "runtime: null\n")
					wantPath, wantSource = filepath.Join(root, "operator.yaml"), "explicit_config"
					if mode == "locator" {
						env = append(env, "SWARM_CONFIG=operator.yaml")
					} else {
						args = append(args, "--config", "operator.yaml")
						env = append(env, "SWARM_CONFIG=missing-unselected.yaml")
					}
				}
				wantServer = "http://127.0.0.1:28341"
				writeReleaseFile(t, wantPath, "connection:\n  api_server: "+wantServer+"\npaths:\n  swarm_dir: operator-state\n")
			}
			result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, args...)
			var report targetReport
			if err := json.Unmarshal([]byte(result.output), &report); err != nil || result.err != nil || !report.OK {
				t.Fatalf("compiled selection: command=%v json=%v\n%s", result.err, err, result.output)
			}
			if report.ConfigSource.Source != wantSource || report.ConfigSource.Path != wantPath || report.ConfigSource.Present == nil || *report.ConfigSource.Present != (mode != "defaults") || report.API == nil || report.API.Server != wantServer {
				t.Fatalf("wrong selected source/target: %#v", report)
			}
			notices := 0
			for _, finding := range report.Config {
				if finding.Code == "config_unread_project" {
					notices++
					if finding.Status != "skipped" || !strings.Contains(finding.Message, filepath.Join(root, "swarm.yaml")) || !strings.Contains(finding.Message, "is not read") {
						t.Fatalf("wrong unread-file notice: %#v", finding)
					}
				}
			}
			if notices != 1 || strings.Contains(result.output, "UNREAD_CONFIG_CONTENT") {
				t.Fatalf("unread-file notice/content: count=%d\n%s", notices, result.output)
			}
			for _, state := range []string{"operator-state", ".swarm/stores", "home/.swarm"} {
				if _, err := os.Stat(filepath.Join(root, state)); !os.IsNotExist(err) {
					t.Fatalf("doctor created runtime state %s: %v", state, err)
				}
			}
		})
	}
	for _, mode := range []string{"deployment", "locator", "explicit"} {
		for _, key := range []string{"agent_config_map_file", "verification_gates_file", "tooling_lock_file"} {
			t.Run("refusal/"+mode+"/"+key, func(t *testing.T) {
				root := goldenReleaseRoot(t)
				env := goldenProcessEnv(t, root, "", 1)
				path, source := filepath.Join(root, ".swarm", "swarm.yaml"), "deployment_config"
				args := []string{"doctor", "--target", "--json"}
				if mode != "deployment" {
					path, source = filepath.Join(root, "operator.yaml"), "explicit_config"
					if mode == "locator" {
						env = append(env, "SWARM_CONFIG=operator.yaml")
					} else {
						args = append(args, "--config", "operator.yaml")
					}
				}
				writeReleaseFile(t, path, "paths:\n  "+key+": null\n")
				result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, args...)
				var report targetReport
				exit, exited := result.err.(*exec.ExitError)
				if err := json.Unmarshal([]byte(result.output), &report); err != nil || !exited || exit.ExitCode() != 3 || report.OK || report.API != nil {
					t.Fatalf("compiled refusal: command=%v json=%v\n%s", result.err, err, result.output)
				}
				if report.ConfigSource.Source != source || report.ConfigSource.Path != path || report.ConfigSource.Present == nil || !*report.ConfigSource.Present {
					t.Fatalf("lost refused source: %#v", report.ConfigSource)
				}
				blockers := 0
				for _, finding := range report.Config {
					if finding.Severity == "blocker" {
						blockers++
						if finding.Code != "config_unknown_key" || finding.Status != "failed" || !strings.Contains(finding.Message, path+":2:3") || !strings.Contains(finding.Message, "paths."+key) {
							t.Fatalf("wrong refusal: %#v", finding)
						}
					}
				}
				if blockers != 1 {
					t.Fatalf("want one source blocker, got %d\n%s", blockers, result.output)
				}
				if _, err := os.Stat(filepath.Join(root, ".swarm", "stores")); !os.IsNotExist(err) {
					t.Fatalf("refused config created store state: %v", err)
				}
			})
		}
	}
	for _, args := range [][]string{{"--help"}, {"serve", "--help"}, {"run", "start", "--help"}} {
		t.Run("help/"+strings.Join(args, " "), func(t *testing.T) {
			root := goldenReleaseRoot(t)
			result := runReleaseCommand(t, goldenStartupTimeout, root, goldenProcessEnv(t, root, "", 1), "", binary, args...)
			if result.err != nil {
				t.Fatalf("compiled help: %v\n%s", result.err, result.output)
			}
			for _, want := range []string{"Deployment config file", "whole file", ".swarm/swarm.yaml", "--config wins over SWARM_CONFIG"} {
				if !strings.Contains(result.output, want) {
					t.Errorf("compiled help missing %q", want)
				}
			}
		})
	}
}
