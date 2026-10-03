package testplanning

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildProductColdExactReuseAndCorruptionFallback(t *testing.T) {
	t.Setenv("GOWORK", "off")
	root, cache := t.TempDir(), t.TempDir()
	for name, data := range map[string]string{"go.mod": "module fixture\ngo 1.25\n", "go.sum": "", ".github/test-proof-plan.yaml": "fixture", "cmd/probe/main.go": "package main\nimport \"fmt\"\nfunc main(){fmt.Println(\"fresh\")}\n"} {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{{"init"}, {"add", "."}, {"-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "fixture"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		if raw, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git: %v %s", err, raw)
		}
	}
	build := func() {
		t.Helper()
		output := filepath.Join(t.TempDir(), "probe")
		if err := BuildGoProduct(context.Background(), root, cache, output, ProfileCore, "build", "./cmd/probe"); err != nil {
			t.Fatal(err)
		}
		if raw, err := exec.Command(output).Output(); err != nil || string(raw) != "fresh\n" {
			t.Fatalf("product %s %v", raw, err)
		}
	}
	build()
	inputs, err := goProductInputs(context.Background(), root, ProfileCore, []string{"build", "./cmd/probe"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(inputs)
	key := filepath.Join(cache, digestBytes(raw))
	first, err := os.Stat(key)
	if err != nil {
		t.Fatal(err)
	}
	build()
	second, _ := os.Stat(key)
	if !first.ModTime().Equal(second.ModTime()) {
		t.Fatal("valid warm product rebuilt")
	}
	for _, mutation := range []string{"bytes", "manifest", "source", "tool", "profile", "flags", "dependency"} {
		t.Run(mutation, func(t *testing.T) {
			original, err := os.ReadFile(key + ".json")
			if err != nil {
				t.Fatal(err)
			}
			var manifest productManifest
			if err := json.Unmarshal(original, &manifest); err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "bytes":
				if err := os.WriteFile(key, []byte("corrupt"), 0700); err != nil {
					t.Fatal(err)
				}
			case "manifest":
				manifest.SHA256 = "wrong"
			case "source":
				manifest.Inputs.Source = "other"
			case "tool":
				manifest.Inputs.Environment = json.RawMessage(`{"GOVERSION":"wrong"}`)
			case "profile":
				manifest.Inputs.Profile = ProfileFull
			case "flags":
				manifest.Inputs.Arguments = []string{"build", "-race", "./cmd/probe"}
			case "dependency":
				manifest.Inputs.Inputs["go.sum"] = "wrong"
			}
			raw, _ := json.Marshal(manifest)
			if err := os.WriteFile(key+".json", raw, 0600); err != nil {
				t.Fatal(err)
			}
			build()
		})
	}
}

func TestBuildProductWarmCacheBypassesDirtyRootInputs(t *testing.T) {
	t.Setenv("GOWORK", "off")
	const rootGo = `package fixture
import "embed"
//go:embed Dockerfile.workspace
var dockerfile string
//go:embed examples/integrations/telegram-agent
var example embed.FS
func Inputs() string {
	raw, _ := example.ReadFile("examples/integrations/telegram-agent/marker.yaml")
	return "go-v1|" + dockerfile + "|" + string(raw)
}
`
	for _, mutation := range []struct {
		name, path, data, want string
	}{
		{"root_go", "platform_artifacts.go", strings.Replace(rootGo, "go-v1", "go-v2", 1), "go-v2|docker-v1|example-v1\n"},
		{"embedded_root_asset", "Dockerfile.workspace", "docker-v2", "go-v1|docker-v2|example-v1\n"},
		{"embedded_example", "examples/integrations/telegram-agent/marker.yaml", "example-v2", "go-v1|docker-v1|example-v2\n"},
		{"untracked_root_fixture", "extra.fixture", "untracked", "go-v1|docker-v1|example-v1\n"},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			root, cache := t.TempDir(), t.TempDir()
			createBuildProductFixture(t, root, map[string]string{
				"go.mod":                "module fixture\ngo 1.25\n",
				"platform_artifacts.go": rootGo,
				"Dockerfile.workspace":  "docker-v1",
				"examples/integrations/telegram-agent/marker.yaml": "example-v1",
				"cmd/probe/main.go": "package main\nimport (\"fmt\"; \"fixture\")\nfunc main(){fmt.Println(fixture.Inputs())}\n",
			})
			if got := buildProductFixtureOutput(t, root, cache); got != "go-v1|docker-v1|example-v1\n" {
				t.Fatalf("cold product: %q", got)
			}
			inputs, err := goProductInputs(context.Background(), root, ProfileCore, []string{"build", "./cmd/probe"})
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(inputs)
			key := filepath.Join(cache, digestBytes(raw))
			before, err := os.Stat(key)
			if err != nil {
				t.Fatal(err)
			}
			cached, err := os.ReadFile(key)
			if err != nil {
				t.Fatal(err)
			}
			if got := buildProductFixtureOutput(t, root, cache); got != "go-v1|docker-v1|example-v1\n" {
				t.Fatalf("warm product: %q", got)
			}
			after, err := os.Stat(key)
			if err != nil || !before.ModTime().Equal(after.ModTime()) {
				t.Fatalf("clean warm cache was not reused: %v", err)
			}
			if err := os.WriteFile(filepath.Join(root, mutation.path), []byte(mutation.data), 0600); err != nil {
				t.Fatal(err)
			}
			if got := buildProductFixtureOutput(t, root, cache); got != mutation.want {
				t.Fatalf("dirty root returned stale product: got %q, want %q", got, mutation.want)
			}
			if _, err := goProductInputs(context.Background(), root, ProfileCore, []string{"build", "./cmd/probe"}); err == nil {
				t.Fatal("non-ignored worktree dirt remained cacheable")
			}
			retained, err := os.ReadFile(key)
			if err != nil || digestBytes(retained) != digestBytes(cached) {
				t.Fatalf("dirty build replaced the clean cached product: %v", err)
			}
		})
	}
}

func TestBuildProductActiveWorkspaceBypassesCache(t *testing.T) {
	for _, mode := range []string{"explicit", "automatic"} {
		t.Run(mode, func(t *testing.T) {
			workspace, cache := t.TempDir(), t.TempDir()
			root := filepath.Join(workspace, "project")
			createBuildProductFixture(t, root, map[string]string{
				"go.mod":            "module fixture\ngo 1.25\nrequire example.invalid/builddependency v0.0.0\n",
				"cmd/probe/main.go": "package main\nimport (\"fmt\"; \"example.invalid/builddependency\")\nfunc main(){fmt.Println(builddependency.Value())}\n",
			})
			external := filepath.Join(workspace, "dependency")
			if err := os.MkdirAll(external, 0700); err != nil {
				t.Fatal(err)
			}
			for path, data := range map[string]string{
				filepath.Join(external, "go.mod"):        "module example.invalid/builddependency\ngo 1.25\n",
				filepath.Join(external, "dependency.go"): "package builddependency\nfunc Value() string { return \"external-v1\" }\n",
				filepath.Join(workspace, "go.work"):      "go 1.25\nuse (\n./project\n./dependency\n)\n",
			} {
				if err := os.WriteFile(path, []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			workFile := filepath.Join(workspace, "go.work")
			if mode == "automatic" {
				workFile = ""
			}
			t.Setenv("GOWORK", workFile)
			if got := buildProductFixtureOutput(t, root, cache); got != "external-v1\n" {
				t.Fatalf("first workspace product: %q", got)
			}
			if err := os.WriteFile(filepath.Join(external, "dependency.go"), []byte("package builddependency\nfunc Value() string { return \"external-v2\" }\n"), 0600); err != nil {
				t.Fatal(err)
			}
			status := exec.Command("git", "status", "--porcelain=v1", "--untracked-files=all", "--ignore-submodules=none")
			status.Dir = root
			if raw, err := status.Output(); err != nil || len(raw) != 0 {
				t.Fatalf("external edit changed repository dirt: %q %v", raw, err)
			}
			if got := buildProductFixtureOutput(t, root, cache); got != "external-v2\n" {
				t.Fatalf("workspace returned stale dependency: %q", got)
			}
			if _, err := goProductInputs(context.Background(), root, ProfileCore, []string{"build", "./cmd/probe"}); err == nil {
				t.Fatal("active workspace remained cacheable")
			}
			if entries, err := os.ReadDir(cache); err != nil || len(entries) != 0 {
				t.Fatalf("workspace build published cache inputs it cannot identify: %v %v", entries, err)
			}
		})
	}
}

func TestBuildProductIgnoredOutputsRemainCacheable(t *testing.T) {
	t.Setenv("GOWORK", "off")
	root, cache := t.TempDir(), t.TempDir()
	createBuildProductFixture(t, root, map[string]string{
		"go.mod":            "module fixture\ngo 1.25\n",
		".gitignore":        "ignored.out\n",
		"cmd/probe/main.go": "package main\nimport \"fmt\"\nfunc main(){fmt.Println(\"fresh\")}\n",
	})
	if got := buildProductFixtureOutput(t, root, cache); got != "fresh\n" {
		t.Fatalf("cold product: %q", got)
	}
	if err := os.WriteFile(filepath.Join(root, "ignored.out"), []byte("receipt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := goProductInputs(context.Background(), root, ProfileCore, []string{"build", "./cmd/probe"}); err != nil {
		t.Fatalf("ignored output refused cache reuse: %v", err)
	}
	if got := buildProductFixtureOutput(t, root, cache); got != "fresh\n" {
		t.Fatalf("warm product with ignored output: %q", got)
	}
}

func createBuildProductFixture(t *testing.T, root string, files map[string]string) {
	t.Helper()
	files["go.sum"] = ""
	files[".github/test-proof-plan.yaml"] = "fixture"
	for name, data := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{{"init"}, {"add", "."}, {"-c", "commit.gpgsign=false", "-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "fixture"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		if raw, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git: %v %s", err, raw)
		}
	}
}

func buildProductFixtureOutput(t *testing.T, root, cache string) string {
	t.Helper()
	output := filepath.Join(t.TempDir(), "probe")
	if err := BuildGoProduct(context.Background(), root, cache, output, ProfileCore, "build", "./cmd/probe"); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(output).CombinedOutput()
	if err != nil {
		t.Fatalf("execute product: %v %s", err, raw)
	}
	return string(raw)
}
