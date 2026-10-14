package releasee2e

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestSingleDeploymentConfigReleaseRelativePackSelection(t *testing.T) {
	binary := buildReleaseBinary(t, t.TempDir())
	type entry struct {
		ID, Type, Version, Source, Directory string
		ManifestHash                         string          `json:"manifest_hash"`
		Origin                               json.RawMessage `json:"origin"`
		Modified                             bool
		ShadowsBase                          bool `json:"shadows_base"`
	}
	type inventory struct {
		BaseMode        string   `json:"base_mode"`
		BaseDigest      string   `json:"base_digest"`
		BaseDirectories []string `json:"base_directories"`
		EffectiveDigest string   `json:"effective_digest"`
		Packs           []entry
	}
	base := goldenReleaseRoot(t)
	source := filepath.Join(base, "source")
	copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "examples/routing/root-ingress"), source)
	initial := runReleaseCommand(t, goldenStartupTimeout, base, goldenProcessEnv(t, base, "", 1), "", binary, "packs", "list", source, "--json")
	var embedded inventory
	if err := json.Unmarshal([]byte(initial.output), &embedded); err != nil || initial.err != nil || embedded.BaseMode != "embedded" || embedded.BaseDigest == "" || embedded.EffectiveDigest == "" || len(embedded.Packs) == 0 || len(embedded.BaseDirectories) != 0 {
		t.Fatalf("compiled embedded inventory: command=%v json=%v\n%s", initial.err, err, initial.output)
	}
	for _, mode := range []string{"deployment", "locator", "explicit"} {
		t.Run(mode, func(t *testing.T) {
			root := goldenReleaseRoot(t)
			env := goldenProcessEnv(t, root, "", 1)
			for _, path := range []string{"swarm.yaml", "home/.config/swarm/swarm.yaml", "home/.config/swarm/config.yaml"} {
				writeReleaseFile(t, filepath.Join(root, path), "runtime: null\n")
			}
			configPath := filepath.Join(root, ".swarm", "swarm.yaml")
			var selector []string
			if mode != "deployment" {
				writeReleaseFile(t, configPath, "runtime: null\n")
				configPath = filepath.Join(root, "operator", "deployment.yaml")
				if mode == "locator" {
					env = append(env, "SWARM_CONFIG=operator/deployment.yaml")
				} else {
					selector = []string{"--config", "operator/deployment.yaml"}
					env = append(env, "SWARM_CONFIG=missing-unselected.yaml")
				}
			}
			lines := []string{"platform:", "  packs:", "    platform_dirs:"}
			want := inventory{BaseMode: "development_override"}
			for _, pack := range embedded.Packs {
				if pack.Source != "embedded" || !filepath.IsLocal(pack.Directory) || pack.ID == "" || filepath.Base(pack.ID) != pack.ID || pack.ManifestHash == "" {
					t.Fatalf("invalid fixture pack coordinates: %+v", pack)
				}
				dir := filepath.Join(filepath.Dir(configPath), "selected-packs", pack.ID)
				copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "packs", pack.Directory), dir)
				lines = append(lines, "      - ./selected-packs/"+pack.ID)
				want.BaseDirectories = append(want.BaseDirectories, dir)
				pack.Directory, pack.Source = dir, "development_override"
				want.Packs = append(want.Packs, pack)
			}
			writeReleaseFile(t, configPath, strings.Join(lines, "\n")+"\n")
			for _, command := range []string{"packs", "verify"} {
				t.Run("inventory/"+command, func(t *testing.T) {
					args := []string{"packs", "list", source, "--json"}
					if command == "verify" {
						args = []string{"verify", source, "--portable", "--json"}
					}
					result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, append(args, selector...)...)
					payload := json.RawMessage(result.output)
					if command == "verify" {
						var report struct {
							OK              bool
							ValidationScope string          `json:"validation_scope"`
							LiveReadiness   string          `json:"live_readiness"`
							PackInventory   json.RawMessage `json:"pack_inventory"`
						}
						if err := json.Unmarshal(payload, &report); err != nil || !report.OK || report.ValidationScope != "structural" || report.LiveReadiness != "not_evaluated" {
							t.Fatalf("compiled portable verification: json=%v\n%s", err, result.output)
						}
						payload = report.PackInventory
					}
					var actual inventory
					if err := json.Unmarshal(payload, &actual); err != nil || result.err != nil || actual.BaseMode != want.BaseMode || actual.BaseDigest == "" || actual.BaseDigest == embedded.BaseDigest || actual.EffectiveDigest == "" || actual.EffectiveDigest == embedded.EffectiveDigest || !reflect.DeepEqual(actual.BaseDirectories, want.BaseDirectories) || !reflect.DeepEqual(actual.Packs, want.Packs) {
						t.Fatalf("selected-file relative pack inventory: command=%v json=%v\n%s", result.err, err, result.output)
					}
				})
			}
			seenTypes := map[string]bool{}
			for _, pack := range want.Packs {
				if seenTypes[pack.Type] {
					continue
				}
				seenTypes[pack.Type] = true
				t.Run("show/"+pack.Type, func(t *testing.T) {
					args := append([]string{"packs", "show", pack.ID, source, "--json"}, selector...)
					result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, args...)
					var show struct {
						Pack         entry
						EnvelopeYAML string `json:"envelope_yaml"`
						ManifestYAML string `json:"manifest_yaml"`
					}
					if err := json.Unmarshal([]byte(result.output), &show); err != nil || result.err != nil || !reflect.DeepEqual(show.Pack, pack) || show.ManifestYAML == "" {
						t.Fatalf("compiled selected pack: command=%v json=%v\n%s", result.err, err, result.output)
					}
					files, err := os.ReadDir(pack.Directory)
					if err != nil || len(files) != 2 {
						t.Fatalf("pack fixture files: %v %v", files, err)
					}
					for _, file := range files {
						body, err := os.ReadFile(filepath.Join(pack.Directory, file.Name()))
						actual := show.ManifestYAML
						if file.Name() == "pack.yaml" {
							actual = show.EnvelopeYAML
						}
						if err != nil || actual != string(body) {
							t.Fatalf("show changed selected %s bytes: %v", file.Name(), err)
						}
					}
				})
			}
			writeReleaseFile(t, configPath, "platform:\n  packs:\n    platform_dirs: [./missing-pack]\n")
			for _, command := range []struct {
				name string
				args []string
			}{
				{"list", []string{"packs", "list", source}},
				{"show", []string{"packs", "show", embedded.Packs[0].ID, source}},
				{"verify", []string{"verify", source, "--portable"}},
			} {
				t.Run("missing/"+command.name, func(t *testing.T) {
					result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, append(command.args, selector...)...)
					exit, exited := result.err.(*exec.ExitError)
					path := filepath.Join(filepath.Dir(configPath), "missing-pack")
					if !exited || exit.ExitCode() != 2 || !strings.Contains(result.output, fmt.Sprintf("development platform pack %q", path)) || !strings.Contains(result.output, "must be a real directory") {
						t.Fatalf("missing selected pack did not refuse at config-relative path: %v\n%s", result.err, result.output)
					}
				})
			}
			t.Run("replacement", func(t *testing.T) {
				writeReleaseFile(t, configPath, "runtime:\n  recovery_on_startup: false\n")
				args := append([]string{"packs", "list", source, "--json"}, selector...)
				result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, args...)
				var actual inventory
				if err := json.Unmarshal([]byte(result.output), &actual); err != nil || result.err != nil || !reflect.DeepEqual(actual, embedded) {
					t.Fatalf("omitted override retained another document's packs: command=%v json=%v\n%s", result.err, err, result.output)
				}
			})
			for _, path := range []string{filepath.Join(root, ".swarm", "stores"), filepath.Join(root, "home", ".swarm"), filepath.Join(source, ".swarm", "packs")} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("read command created state/imports %s: %v", path, err)
				}
			}
		})
	}
}
