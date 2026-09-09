package cliapp

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestSourceRootSelectionAliasMatrix(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "source")
	outside := filepath.Join(base, "outside")
	for _, dir := range []string{root, outside} {
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, target := range map[string]string{"alias": root, "chain": "alias", "ancestor": base, "outside/link": root, "broken": "absent", "cycle": "cycle", "file-link": "file"} {
		if err := os.Symlink(target, filepath.Join(base, name)); err != nil {
			t.Fatal(err)
		}
	}
	file := filepath.Join(base, "file")
	if err := os.WriteFile(file, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, invocation, operand string }{
		{"relative under cwd", base, "source"},
		{"absolute under cwd", base, root},
		{"relative outside cwd", outside, "../source"},
		{"absolute outside cwd", outside, root},
		{"omitted", root, ""},
		{"dot", root, "."},
		{"root alias", base, "alias"},
		{"absolute root alias", outside, filepath.Join(base, "alias")},
		{"alias chain", base, "chain"},
		{"ancestor alias", base, "ancestor/source"},
		{"relative alias parent", base, "outside/link/../source"},
		{"absolute alias parent", outside, outside + "/link/../source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveSourceRoot(tc.invocation, tc.operand)
			if err != nil || got != root {
				t.Fatalf("selection = %q, %v; want %q", got, err, root)
			}
		})
	}
	for _, tc := range []struct{ name, invocation, operand string }{
		{"empty invocation", "", "."},
		{"relative invocation", "relative", "."},
		{"empty invocation absolute operand", "", root},
		{"relative invocation absolute operand", "relative", root},
		{"missing", base, "absent"},
		{"broken", base, "broken"},
		{"cycle", base, "cycle"},
		{"file", base, file},
		{"file alias", base, "file-link"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := ResolveSourceRoot(tc.invocation, tc.operand); err == nil {
				t.Fatalf("invalid selection admitted: %q", got)
			}
		})
	}
}

func TestSourceInvocationCommandsShareSelectedRoot(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "bundle")
	if err := os.CopyFS(root, os.DirFS(filepath.Join(RepoRoot(), "internal/releasee2e/testdata/static_data_invocation"))); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(base, "alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(base, "swarm.yaml")
	if err := os.WriteFile(config, []byte("workspace:\n  backend: host\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, command := range [][]string{
		{"verify"}, {"describe"}, {"describe", "--graph"}, {"describe", "routes"},
		{"packs", "list"}, {"packs", "show", "provider.telegram"},
	} {
		t.Run(strings.Join(command, " "), func(t *testing.T) {
			var baseline string
			for _, selected := range []string{root, alias} {
				args := append(append([]string{}, command...), selected, "--config", config)
				var out, stderr bytes.Buffer
				code := executeRootCommandWithOptions(context.Background(), base, args, &out, &stderr, defaultRootCommandOptions())
				if code != 0 {
					t.Fatalf("%v code=%d stdout=%s stderr=%s", args, code, &out, &stderr)
				}
				if baseline == "" {
					baseline = out.String()
				} else if baseline != out.String() {
					t.Fatalf("alias changes command projection:\n%s\n%s", baseline, &out)
				}
			}
		})
	}
	t.Run("test selection and compilation", func(t *testing.T) {
		selected, spec, err := resolveScenarioTestSources(base, "alias", cliCommandConfig{})
		if err != nil || selected != root {
			t.Fatalf("scenario selection: %q %v", selected, err)
		}
		if _, _, err := NewSwarmWorkflowModule(base, selected, spec); err != nil {
			t.Fatal(err)
		}
		var out, stderr bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), base, []string{"test", alias, "--config", config}, &out, &stderr, defaultRootCommandOptions())
		if code == 0 || !strings.Contains(stderr.String(), "no scenario files found in an admitted tests/ resource branch") {
			t.Fatalf("scenario discovery not reached: %d %s %s", code, &out, &stderr)
		}
	})
	t.Run("local run serve handoff", func(t *testing.T) {
		server, _, _ := newRunCommandServer(t, runCommandServerOptions{expectedToken: apiv1.DefaultLoopbackAPIToken, rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			if req.Method != "health.check" {
				t.Errorf("unexpected RPC %s", req.Method)
			}
			return runCommandHealthResult()
		}})
		defer server.Close()
		opts := runCommandOptions{apiOptions: testRunCommandOptions(server), sourceRoot: alias, configPath: config}
		captured := make(chan ServeOptions, 1)
		opts.apiOptions.runServe = func(ctx context.Context, _ InvocationRoot, serve ServeOptions) int {
			captured <- serve
			<-ctx.Done()
			return 0
		}
		stop, err := startLocalRunServe(context.Background(), mustInvocationRootForTest(base), opts, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		stop()
		if got := <-captured; got.SourceRoot != root {
			t.Fatalf("local serve selected %s, want %s", got.SourceRoot, root)
		}
	})
	t.Run("import writes selected root", func(t *testing.T) {
		code, out, stderr := runPacksCommand(t, base, "import", "provider.telegram", alias, "--config", config, "--json")
		if code != 0 {
			t.Fatalf("import: %d %s %s", code, out, stderr)
		}
		if _, err := os.Stat(filepath.Join(root, "packs/manifest.yaml")); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(base, "packs")); !os.IsNotExist(err) {
			t.Fatalf("import wrote invocation root: %v", err)
		}
	})
}

func TestStaticDataCatalogInvocationInvariance(t *testing.T) {
	for _, sameContent := range []bool{false, true} {
		t.Run(map[bool]string{false: "different-content", true: "same-content"}[sameContent], func(t *testing.T) {
			base, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			root := filepath.Join(base, "bundle")
			if err := os.CopyFS(root, os.DirFS(filepath.Join(RepoRoot(), "internal/releasee2e/testdata/static_data_invocation"))); err != nil {
				t.Fatal(err)
			}
			if sameContent {
				body, err := os.ReadFile(filepath.Join(root, "data/resume.md"))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, "registry/data/resume.md"), body, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			outside := filepath.Join(base, "outside")
			if err := os.Mkdir(outside, 0o755); err != nil {
				t.Fatal(err)
			}
			for name, target := range map[string]string{"alias": root, "chain": "alias", "ancestor": base, "outside/link": root} {
				if err := os.Symlink(target, filepath.Join(base, name)); err != nil {
					t.Fatal(err)
				}
			}
			cells := []struct{ name, cwd, operand string }{
				{"relative-inside", base, "bundle"}, {"absolute-inside", base, root},
				{"relative-outside", outside, "../bundle"}, {"absolute-outside", outside, root},
				{"omitted", root, ""}, {"dot", root, "."},
				{"symlink-cwd", filepath.Join(base, "alias"), "."},
				{"alias", base, "alias"}, {"chain", base, "chain"}, {"ancestor", base, "ancestor/bundle"},
				{"relative-alias-parent", base, "outside/link/../bundle"},
				{"absolute-alias-parent", outside, outside + "/link/../bundle"},
			}
			var baseline *contracts.WorkflowContractBundle
			for _, missing := range []bool{false, true} {
				if missing {
					if err := os.Remove(filepath.Join(root, "data/resume.md")); err != nil {
						t.Fatal(err)
					}
				}
				for _, cell := range cells {
					t.Run(fmtCellName(cell.name, missing), func(t *testing.T) {
						invocation, err := NewInvocationRoot(cell.cwd)
						if err != nil {
							t.Fatal(err)
						}
						selected, err := ResolveSourceRoot(invocation.Path(), cell.operand)
						if err != nil {
							t.Fatal(err)
						}
						bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(RepoRoot(), selected, contracts.DefaultPlatformSpecFile(RepoRoot()))
						if missing {
							if err == nil || !strings.Contains(err.Error(), "resume.md") {
								t.Fatalf("missing declared data: %v", err)
							}
							return
						}
						if err != nil {
							t.Fatal(err)
						}
						if baseline == nil {
							baseline = bundle
						}
						if bundle.SourceArtifact.BundleHash() != baseline.SourceArtifact.BundleHash() || !reflect.DeepEqual(bundle.StaticData(), baseline.StaticData()) {
							t.Fatal("invocation changed hash or full catalog tuple")
						}
						items := bundle.StaticData()
						if len(items) != 2 || items[0].StaticID == items[1].StaticID {
							t.Fatalf("owning flows aliased: %#v", items)
						}
						labels := map[string]bool{}
						for _, item := range items {
							labels[item.Ref.CanonicalInputLabel] = true
						}
						if !labels["data/resume.md"] || !labels["registry/data/resume.md"] {
							t.Fatalf("labels: %v", labels)
						}
					})
				}
			}
		})
	}
}

func fmtCellName(name string, missing bool) string {
	if missing {
		return "missing/" + name
	}
	return "valid/" + name
}
