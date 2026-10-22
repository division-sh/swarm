package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestWorkflowUnconditionalGateAndExecutableAggregate(t *testing.T) {
	b, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		On   map[string]any `yaml:"on"`
		Jobs map[string]struct {
			If    string   `yaml:"if"`
			Needs []string `yaml:"needs"`
			Steps []struct {
				Name, Run, Uses, If string
				With                map[string]any
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(b, &workflow); err != nil {
		t.Fatal(err)
	}
	job, ok := workflow.Jobs["complexity"]
	// Setup cannot run or attest a proof tier (#2718, Gate E 6248763428).
	// Every ordinary event retains the unconditional cheap-admission gate.
	const setupExclusion = "github.event_name != 'workflow_dispatch' || inputs.workspace_base_setup != true"
	admitsComplexity := func(condition string, needs []string) bool {
		return condition == setupExclusion && slices.Equal(needs, []string{"ci-preflight"})
	}
	if !ok || !admitsComplexity(job.If, job.Needs) {
		t.Fatal("complexity must depend only on cheap admission, excluding only isolated workspace-base setup, never proof profile/expensive plan")
	}
	for _, hostile := range []struct {
		name, condition string
		needs           []string
	}{
		{"setup also executes", "", []string{"ci-preflight"}},
		{"tier gate", "(" + setupExclusion + ") && needs.ci-plan.outputs.profile == 'full'", []string{"ci-preflight"}},
		{"draft gate", "(" + setupExclusion + ") && !github.event.pull_request.draft", []string{"ci-preflight"}},
		{"bypassed exclusion", setupExclusion + " || true", []string{"ci-preflight"}},
		{"missing admission", setupExclusion, nil},
		{"duplicate admission", setupExclusion, []string{"ci-preflight", "ci-preflight"}},
		{"proof plan dependency", setupExclusion, []string{"ci-preflight", "ci-plan"}},
		{"image dependency", setupExclusion, []string{"ci-preflight", "sqlite-local-dev"}},
		{"setup dependency", setupExclusion, []string{"ci-preflight", "workspace-base-setup"}},
	} {
		t.Run(hostile.name, func(t *testing.T) {
			if admitsComplexity(hostile.condition, hostile.needs) {
				t.Fatal("changed complexity gate or dependency was admitted")
			}
		})
	}
	for _, event := range []string{"pull_request", "push", "workflow_dispatch", "schedule"} {
		if _, ok := workflow.On[event]; !ok {
			t.Fatal("missing event", event)
		}
	}
	checkout, command, upload := false, false, false
	for _, step := range job.Steps {
		if strings.Contains(step.Uses, "actions/checkout") {
			checkout = step.With["ref"] == "${{ github.event.pull_request.head.sha || github.sha }}" && step.With["fetch-depth"] == 0
		}
		if strings.Contains(step.Run, "./cmd/swarm-complexity") {
			command = step.If == "" && strings.Contains(step.Run, `-event "$EVENT_NAME" -event-file "$GITHUB_EVENT_PATH"`)
		}
		if strings.Contains(step.Uses, "actions/upload-artifact") {
			upload = step.If == "always()" && step.With["if-no-files-found"] == "error"
		}
	}
	if !checkout || !command || !upload {
		t.Fatalf("checkout=%v command=%v upload=%v", checkout, command, upload)
	}
	aggregate := workflow.Jobs["required-tests"]
	found := false
	for _, need := range aggregate.Needs {
		if need == "complexity" {
			found = true
		}
	}
	if !found || aggregate.If != "always() && ("+setupExclusion+")" {
		t.Fatal("missing required complexity result")
	}
	var summary string
	for _, step := range aggregate.Steps {
		if step.Name == "Summarize required checks" {
			summary = step.Run
		}
	}
	if summary == "" {
		t.Fatal("missing executable summary")
	}
	for _, tier := range []string{"core", "lifecycle", "full"} {
		for _, status := range []string{"success", "skipped", "cancelled", "failure", "", "unknown"} {
			script := summary
			for _, need := range aggregate.Needs {
				value := "success"
				if strings.HasPrefix(need, "unused-") {
					value = "skipped"
				}
				if need == "complexity" {
					value = status
				}
				script = strings.ReplaceAll(script, "${{ needs."+need+".result }}", value)
			}
			script = strings.ReplaceAll(script, "${{ needs.ci-plan.outputs.profile }}", tier)
			script = strings.ReplaceAll(script, "${{ needs.ci-plan.outputs.master_replay }}", "false")
			script = strings.ReplaceAll(script, "${{ needs.ci-plan.outputs.soak_matrix }}", `{"include":[]}`)
			c := exec.Command("bash", "-c", script)
			c.Env = append(os.Environ(), "IS_DRAFT=false", "EVENT_NAME=pull_request", "EVENT_REF=refs/pull/1/merge", "GITHUB_STEP_SUMMARY="+filepath.Join(t.TempDir(), "summary"))
			out, err := c.CombinedOutput()
			if (err == nil) != (status == "success") {
				t.Fatalf("%s aggregate complexity status %q: %v %s", tier, status, err, out)
			}
		}
	}
}
