package cliapp

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"

	"gopkg.in/yaml.v3"
)

func TestCIExecutionSelectionConsumers(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve repository root")
	}
	root := filepath.Join(filepath.Dir(source), "..", "..", ".github")
	repo := filepath.Clean(filepath.Join(root, ".."))
	violations, err := ciFixtureExecutionViolations(repo)
	if err != nil {
		t.Fatal(err)
	}
	for _, violation := range violations {
		t.Error(violation)
	}
	raw, err := os.ReadFile(filepath.Join(root, "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct{ Name, Run string }
		}
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	// Pin the real public journey, not merely the absence of a retired fixture.
	want := `payload=$(mktemp)
printf '{"item_id":"smoke-item"}\n' > "$payload"
./swarm run start examples/routing/root-ingress \
  --event item.received \
  --payload "$payload" \
  --api-port 18091`
	found := 0
	for _, step := range workflow.Jobs["sqlite-local-dev"].Steps {
		if step.Name == "Prove no-selector SQLite local run" {
			found++
			if strings.TrimSpace(step.Run) != want {
				t.Errorf("SQLite smoke must retain the selector-free public live run:\n%s", step.Run)
			}
		}
	}
	if found != 1 {
		t.Fatalf("SQLite public smoke steps = %d, want one", found)
	}
}

func TestCIExecutionFixtureCensusExcludesNestedCheckout(t *testing.T) {
	repo := t.TempDir()
	local := filepath.Join(repo, ".github", "fixtures", "current.yaml")
	foreign := filepath.Join(repo, ".github", "fixtures", "foreign")
	for _, path := range []string{local, filepath.Join(foreign, "hostile.yaml")} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("runtime: {execution_posture: live}\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(foreign, ".git"), []byte("gitdir: elsewhere\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	violations, err := ciFixtureExecutionViolations(repo)
	if err != nil || len(violations) != 1 || !strings.Contains(violations[0], local) {
		t.Fatalf("CI fixture violations = %v, %v; want only current-local file", violations, err)
	}
	if err := os.Remove(local); err != nil {
		t.Fatal(err)
	}
	violations, err = ciFixtureExecutionViolations(repo)
	if err != nil || len(violations) != 0 {
		t.Fatalf("foreign-only CI fixture was rejected: %v, %v", violations, err)
	}
}

func ciFixtureExecutionViolations(repo string) ([]string, error) {
	root := filepath.Join(repo, ".github")
	var violations []string
	err := checkoutsource.WalkDir(repo, root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || (!strings.HasSuffix(path, ".yaml") && !strings.HasSuffix(path, ".yml")) {
			return err
		}
		if !strings.Contains(path, string(filepath.Separator)+"fixtures"+string(filepath.Separator)) {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var config struct {
			Runtime map[string]any `yaml:"runtime"`
			LLM     map[string]any `yaml:"llm"`
		}
		if err := yaml.Unmarshal(raw, &config); err != nil {
			return err
		}
		if _, present := config.Runtime["execution_posture"]; present {
			violations = append(violations, path+" supplies retired runtime.execution_posture")
		}
		if config.LLM["backend"] == "mock" {
			violations = append(violations, path+" supplies retired llm.backend: mock")
		}
		return nil
	})
	return violations, err
}
