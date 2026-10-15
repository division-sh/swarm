package cliapp

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestRuntimeConfigRejectsRetiredAuxiliaryPaths(t *testing.T) {
	for _, key := range []string{"agent_config_map_file", "verification_gates_file", "tooling_lock_file", "platform_spec_path"} {
		for _, selector := range []string{"deployment", "explicit", "locator"} {
			for _, merged := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/merged=%t", key, selector, merged), func(t *testing.T) {
					isolateCLIAPIConfigEnv(t)
					root := t.TempDir()
					path := filepath.Join(root, ".swarm", "swarm.yaml")
					opts := RuntimeConfigLoadOptions{RepoRoot: root}
					wantSource := unifiedConfigSourceDeployment
					if selector != "deployment" {
						path = filepath.Join(root, "operator.yaml")
						wantSource = unifiedConfigSourceExplicit
						if selector == "explicit" {
							opts.ExplicitPath = path
							t.Setenv("SWARM_CONFIG", filepath.Join(root, "not-selected.yaml"))
						} else {
							t.Setenv("SWARM_CONFIG", path)
						}
					}
					if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
						t.Fatal(err)
					}
					source := fmt.Sprintf("paths:\n  %s: ./unused.yaml\n", key)
					if merged {
						source = fmt.Sprintf("paths:\n  <<: {%s: null}\n", key)
					}
					writeRuntimeConfigText(t, path, source)
					got, err := LoadRuntimeConfigWithOptions(opts)
					var refused unifiedConfigError
					if !errors.As(err, &refused) || got.validated || got.Path != path || got.Source != wantSource {
						t.Fatalf("retired path admission: error=%v path=%q source=%q validated=%v", err, got.Path, got.Source, got.validated)
					}
					blockers := unifiedConfigBlockers(refused.Diagnostics)
					if len(blockers) != 1 {
						t.Fatalf("want one exact source refusal, got %#v", blockers)
					}
					problem := blockers[0]
					if problem.Kind != unifiedConfigDiagnosticUnknownKey || problem.Path != path || problem.Key != "paths."+key || problem.Line == 0 || problem.Column == 0 {
						t.Fatalf("lost source key/location: %#v", problem)
					}
					if slices.Contains(problem.ValidOptions, key) || !slices.Contains(problem.ValidOptions, "swarm_dir") || !slices.Contains(problem.ValidOptions, "monitor_dir") {
						t.Fatalf("wrong current path vocabulary: %#v", problem.ValidOptions)
					}
					if !strings.Contains(err.Error(), path) || !strings.Contains(err.Error(), "paths."+key) {
						t.Fatalf("missing located current-key teaching: %v", err)
					}
					if key == "platform_spec_path" {
						want := fmt.Sprintf("`paths.platform_spec_path` at %s:%d:%d is not a config key; the platform spec is embedded in `swarm`.", path, problem.Line, problem.Column)
						if problem.Message != want || problem.Remediation != "" || err.Error() != "swarm.yaml config blockers:\n"+want {
							t.Fatalf("platform spec teaching differs from the user ruling: %#v / %v", problem, err)
						}
					} else if !strings.Contains(err.Error(), "Valid fields:") {
						t.Fatalf("missing current-key teaching: %v", err)
					}
					if _, err := os.Stat(filepath.Join(root, ".swarm", "stores")); !os.IsNotExist(err) {
						t.Fatalf("source refusal created store state: %v", err)
					}
				})
			}
		}
	}
}
