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

func TestWorkflowNativeUnionIsRequired(t *testing.T) {
	b, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			If              string   `yaml:"if"`
			RunsOn          string   `yaml:"runs-on"`
			Needs           []string `yaml:"needs"`
			ContinueOnError any      `yaml:"continue-on-error"`
			TimeoutMinutes  any      `yaml:"timeout-minutes"`
			Steps           []struct {
				Name, Run, Uses, If string
				ContinueOnError     any            `yaml:"continue-on-error"`
				With                map[string]any `yaml:"with"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(b, &workflow); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []struct {
		job, platform, runner string
		timeoutMinutes        int
	}{
		{"unused-linux", "linux", "ubuntu-latest", 15}, {"unused-darwin", "darwin", "macos-latest", 19},
	} {
		job, ok := workflow.Jobs[owner.job]
		if !ok || job.If != "needs.ci-plan.outputs.profile == 'full' && needs.ci-plan.outputs.master_replay != 'true'" || !slices.Equal(job.Needs, []string{"ci-plan"}) || job.ContinueOnError != nil || job.RunsOn != owner.runner || job.TimeoutMinutes != owner.timeoutMinutes {
			t.Fatalf("native %s analysis must qualify every selected full plan on its native platform", owner.platform)
		}
		checkout, collect, upload := false, false, false
		for _, step := range job.Steps {
			if strings.Contains(step.Uses, "actions/checkout@") {
				checkout = step.With["ref"] == "${{ github.sha }}"
			}
			if strings.Contains(step.Run, "./cmd/swarm-unused") {
				collect = step.Run == "go run ./cmd/swarm-unused -collect -out test-results/unused/"+owner.platform && step.If == "" && step.ContinueOnError == nil
			}
			if strings.Contains(step.Uses, "actions/upload-artifact@") && step.With["name"] == "unused-"+owner.platform {
				upload = step.If == "always()" && step.With["path"] == "test-results/unused/"+owner.platform && step.With["if-no-files-found"] == "error"
			}
		}
		if !checkout || !collect || !upload {
			t.Fatalf("%s: exact checkout=%v collect=%v upload=%v", owner.job, checkout, collect, upload)
		}
	}
	for _, owner := range []string{"static-checks", "macos-sqlite-possession"} {
		job, ok := workflow.Jobs[owner]
		if !ok || job.TimeoutMinutes != 15 {
			t.Fatalf("%s: preserve the product proof budget", owner)
		}
		for _, step := range job.Steps {
			if strings.Contains(step.Run, "./cmd/swarm-unused") {
				t.Fatalf("%s: cold analysis must not consume the product proof budget", owner)
			}
		}
	}
	union, ok := workflow.Jobs["unused-checks"]
	if !ok || union.If != "needs.ci-plan.outputs.profile == 'full' && needs.ci-plan.outputs.master_replay != 'true'" || union.ContinueOnError != nil || union.TimeoutMinutes != 15 || !slices.Equal(union.Needs, []string{"ci-plan", "unused-linux", "unused-darwin"}) {
		t.Fatal("selected full union must require both native collectors")
	}
	checkout, merge := false, false
	downloads := map[string]bool{}
	for _, step := range union.Steps {
		if strings.Contains(step.Uses, "actions/checkout@") {
			checkout = step.With["ref"] == "${{ github.sha }}"
		}
		if strings.Contains(step.Run, "./cmd/swarm-unused") {
			merge = step.Run == "go run ./cmd/swarm-unused -merge -in test-results/unused" && step.If == "" && step.ContinueOnError == nil
		}
		for _, platform := range []string{"linux", "darwin"} {
			if strings.Contains(step.Uses, "actions/download-artifact@") && step.With["name"] == "unused-"+platform {
				downloads[platform] = step.With["path"] == "test-results/unused/"+platform && step.If == "" && step.With["run-id"] == nil
			}
		}
	}
	if !checkout || !merge || !downloads["linux"] || !downloads["darwin"] {
		t.Fatal("union must merge exact-head native artifacts from this run")
	}
	aggregate := workflow.Jobs["required-tests"]
	if aggregate.If != "always()" {
		t.Fatal("required aggregate must execute even when a required job fails/skips")
	}
	var summary string
	for _, step := range aggregate.Steps {
		if step.Name == "Summarize required checks" {
			summary = step.Run
		}
	}
	if summary == "" {
		t.Fatal("missing executable aggregate")
	}
	for _, tier := range []string{"core", "lifecycle", "full"} {
		for _, owner := range []string{"static-checks", "macos-sqlite-possession", "unused-linux", "unused-darwin", "unused-checks"} {
			if !slices.Contains(aggregate.Needs, owner) {
				t.Fatalf("missing aggregate owner %s", owner)
			}
			for _, status := range []string{"success", "failure", "skipped", "cancelled", "", "unknown"} {
				script := summary
				for _, need := range aggregate.Needs {
					value := "success"
					if tier != "full" && strings.HasPrefix(need, "unused-") {
						value = "skipped"
					}
					if need == owner {
						value = status
					}
					script = strings.ReplaceAll(script, "${{ needs."+need+".result }}", value)
				}
				script = strings.ReplaceAll(script, "${{ needs.ci-plan.outputs.profile }}", tier)
				script = strings.ReplaceAll(script, "${{ needs.ci-plan.outputs.master_replay }}", "false")
				script = strings.ReplaceAll(script, "${{ needs.ci-plan.outputs.soak_matrix }}", `{"include":[]}`)
				c := exec.Command("bash", "-c", script)
				c.Env = append(os.Environ(), "IS_DRAFT=false", "GITHUB_STEP_SUMMARY="+filepath.Join(t.TempDir(), "summary"))
				b, err := c.CombinedOutput()
				expected := "success"
				if tier != "full" && strings.HasPrefix(owner, "unused-") {
					expected = "skipped"
				}
				if (err == nil) != (status == expected) {
					t.Fatalf("%s required %s status %q: %v %s", tier, owner, status, err, b)
				}
			}
		}
	}
}
