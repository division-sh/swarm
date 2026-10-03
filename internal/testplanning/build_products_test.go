package testplanning

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestBuildProductColdExactReuseAndCorruptionFallback(t *testing.T) {
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
