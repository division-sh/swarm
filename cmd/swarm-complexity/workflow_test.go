package main

import (
	"os"
	"os/exec"
	"path/filepath"
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
	if !ok || job.If != "" || len(job.Needs) != 0 {
		t.Fatal("complexity is conditional on proof profile/plan")
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
	if !found || aggregate.If != "always()" {
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
				if tier != "full" && strings.HasPrefix(need, "unused-") {
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
			c.Env = append(os.Environ(), "IS_DRAFT=false", "GITHUB_STEP_SUMMARY="+filepath.Join(t.TempDir(), "summary"))
			out, err := c.CombinedOutput()
			if (err == nil) != (status == "success") {
				t.Fatalf("%s aggregate complexity status %q: %v %s", tier, status, err, out)
			}
		}
	}
}
