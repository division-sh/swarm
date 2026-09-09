package cliapp

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCIExecutionSelectionConsumers(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve repository root")
	}
	root := filepath.Join(filepath.Dir(source), "..", "..", ".github")
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
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
			t.Errorf("%s supplies retired runtime.execution_posture", path)
		}
		if config.LLM["backend"] == "mock" {
			t.Errorf("%s supplies retired llm.backend: mock", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
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
