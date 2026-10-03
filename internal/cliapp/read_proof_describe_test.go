package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/runtime/contracts"
)

var describeProofFixtures = []struct{ name, path string }{
	{"select-or-create", "examples/routing/template-select-or-create"},
	{"create-minted-key", "examples/routing/template-create-minted-key"},
	{"telegram-agent", "examples/integrations/telegram-agent"},
	{"barrier", "examples/routing/fan-in/barrier"},
	{"golden-workload", "internal/releasee2e/testdata/golden_agent_workload"},
}

var describeProofSurfaces = []struct {
	name string
	args []string
}{
	{"describe-text", []string{"describe"}},
	{"describe-json", []string{"describe", "--json"}},
	{"describe-quiet", []string{"describe", "--quiet"}},
	{"describe-no-color", []string{"describe", "--no-color"}},
	{"describe-graph-text", []string{"describe", "--graph"}},
	{"describe-graph-json", []string{"describe", "--graph", "--json"}},
	{"routes-text", []string{"describe", "routes"}},
	{"routes-json", []string{"describe", "routes", "--json"}},
	{"routes-quiet", []string{"describe", "routes", "--quiet"}},
}

func TestReadProofFactoringCompiledDescribe(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "swarm")
	repo := RepoRoot()
	build := exec.Command("go", "build", "-o", binary, "./cmd/swarm")
	build.Dir = repo
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build public describe binary: %v\n%s", err, output)
	}
	corpus := filepath.Join(repo, "internal/cliapp/testdata/describe")
	update := os.Getenv("SWARM_UPDATE_DESCRIBE_CORPUS") == "1"
	if !update {
		if err := validateDescribeProofCorpus(corpus); err != nil {
			t.Fatal(err)
		}
	}
	// Independent read-only CLI cells share only the built binary and source.
	// Bound process concurrency without sharing configuration or cache state.
	processes := make(chan struct{}, 2)
	for _, fixture := range describeProofFixtures {
		// Admission is independent of both the public projection and the golden.
		bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(repo, fixture.path), contracts.DefaultPlatformSpecFile(repo))
		if err != nil {
			t.Fatal(err)
		}
		identity := bundle.SourceArtifact.BundleHash()
		for _, surface := range describeProofSurfaces {
			t.Run(fixture.name+"/"+surface.name, func(t *testing.T) {
				t.Parallel()
				processes <- struct{}{}
				defer func() { <-processes }()
				scope := t.TempDir()
				env := readProofCompiledScopeEnv(scope)
				args := surface.args
				golden := filepath.Join(corpus, fixture.name, surface.name+".golden")
				embedded := filepath.Join(scope, ".cache/swarm/embedded-assets", "platform-spec-"+platform.PlatformSpecDigest()[:16]+".yaml")
				var first []byte
				for repetition := 0; repetition < 2; repetition++ {
					ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
					cmd := exec.CommandContext(ctx, binary, append(append([]string{}, args...), filepath.Join(repo, fixture.path))...)
					cmd.Dir, cmd.Env = repo, env
					var stdout, stderr bytes.Buffer
					cmd.Stdout, cmd.Stderr = &stdout, &stderr
					err := cmd.Run()
					cancel()
					if err != nil || stderr.Len() != 0 {
						t.Fatalf("compiled %v: err=%v stdout=%s stderr=%s", args, err, &stdout, &stderr)
					}
					presentedIdentity := identity
					if !strings.HasSuffix(surface.name, "-json") {
						presentedIdentity = humanSourceIdentity(identity, bundle.SourceArtifact.HumanLabel())
					}
					normalized, err := normalizeDescribeProof(stdout.Bytes(), surface.name, presentedIdentity, repo, embedded)
					if err != nil {
						t.Fatal(err)
					}
					if strings.HasSuffix(surface.name, "-json") {
						var compact, readable bytes.Buffer
						if err := json.Compact(&compact, stdout.Bytes()); err != nil || !bytes.Equal(stdout.Bytes(), append(compact.Bytes(), '\n')) {
							t.Fatalf("JSON wire formatting changed: %v", err)
						}
						if err := json.Indent(&readable, normalized, "", "  "); err != nil {
							t.Fatal(err)
						}
						normalized = readable.Bytes()
					}
					if repetition == 0 {
						first = normalized
					} else if !bytes.Equal(first, normalized) {
						t.Fatal("repeated compiled output changed")
					}
					if update && repetition == 1 {
						if err := os.MkdirAll(filepath.Dir(golden), 0o755); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(golden, normalized, 0o644); err != nil {
							t.Fatal(err)
						}
					}
					if !update {
						want, err := os.ReadFile(golden)
						if err != nil {
							t.Fatal(err)
						}
						if !bytes.Equal(want, normalized) {
							t.Fatalf("compiled output differs from %s at byte %d", golden, describeProofDifference(want, normalized))
						}
					}
				}
			})
		}
	}
}

func describeProofDifference(want, got []byte) int {
	for i := 0; i < len(want) && i < len(got); i++ {
		if want[i] != got[i] {
			return i
		}
	}
	return min(len(want), len(got))
}

func validateDescribeProofCorpus(root string) error {
	want := map[string]bool{}
	directories := map[string]bool{root: true}
	for _, fixture := range describeProofFixtures {
		directories[filepath.Join(root, fixture.name)] = true
		for _, surface := range describeProofSurfaces {
			want[filepath.Join(fixture.name, surface.name+".golden")] = true
		}
	}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if !directories[path] {
				return fmt.Errorf("unexpected describe corpus directory %s", path)
			}
			return nil
		}
		name, err := filepath.Rel(root, path)
		if err != nil || !want[name] || entry.Type() != 0 {
			return fmt.Errorf("unexpected describe corpus entry %s", path)
		}
		delete(want, name)
		return nil
	})
	if err != nil {
		return err
	}
	if len(want) != 0 {
		return fmt.Errorf("describe corpus missing %d of 45 cells", len(want))
	}
	return nil
}
