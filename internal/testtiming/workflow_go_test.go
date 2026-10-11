package testtiming

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCIVersionBoundToolsUsePlannedGoToolchain(t *testing.T) {
	root := testTimingRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, ".github/workflows/ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]ciWorkflowJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	batch, err := os.ReadFile(filepath.Join(root, ".github/scripts/run-proof-batch.sh"))
	if err != nil {
		t.Fatal(err)
	}
	checked := map[string]bool{}
	for name, job := range workflow.Jobs {
		steps := slices.Clone(job.Steps)
		for i := range steps {
			if steps[i].Run == "bash .github/scripts/run-proof-batch.sh" {
				steps[i].Run = string(batch)
			}
		}
		if !slices.ContainsFunc(steps, invokesVersionBoundTool) {
			continue
		}
		checked[name] = true
		t.Run(name, func(t *testing.T) {
			if problems := workflowGoSetupProblems(steps); len(problems) != 0 {
				t.Fatalf("GOVERSION-bound workflow consumer: %v", problems)
			}
			for _, cut := range []string{"missing", "wrong-file", "late"} {
				t.Run(cut, func(t *testing.T) {
					changed := make([]ciWorkflowStep, 0, len(steps))
					for _, step := range steps {
						if strings.HasPrefix(step.Uses, "actions/setup-go@") {
							continue
						}
						changed = append(changed, step)
					}
					setup := ciWorkflowStep{Uses: "actions/setup-go@v5", With: map[string]any{"go-version-file": "go.mod"}}
					if cut == "wrong-file" {
						setup.With["go-version-file"] = "other.mod"
						changed = append([]ciWorkflowStep{setup}, changed...)
					} else if cut == "late" {
						changed = append(changed, setup)
					}
					if problems := workflowGoSetupProblems(changed); len(problems) == 0 {
						t.Fatal("unplanned runner toolchain reached a version-bound consumer")
					}
				})
			}
		})
	}
	for _, name := range []string{"ci-plan", "proof-unit", "timing-budget", "required-tests", "publish-timing-model"} {
		if !checked[name] {
			t.Fatalf("required version-bound consumer %s escaped the workflow census", name)
		}
	}
}

func invokesVersionBoundTool(step ciWorkflowStep) bool {
	return strings.Contains(step.Run, "./cmd/swarm-test-timing") || strings.Contains(step.Run, "./cmd/swarm-test ")
}

func workflowGoSetupProblems(steps []ciWorkflowStep) []string {
	var problems []string
	planned := false
	for i, step := range steps {
		if strings.HasPrefix(step.Uses, "actions/setup-go@") {
			planned = step.Uses == "actions/setup-go@v5" && step.With["go-version-file"] == "go.mod" && step.With["go-version"] == nil && !step.ContinueOnError
		}
		if invokesVersionBoundTool(step) && !planned {
			problems = append(problems, fmt.Sprintf("step %d must follow actions/setup-go from go.mod", i))
		}
	}
	return problems
}
