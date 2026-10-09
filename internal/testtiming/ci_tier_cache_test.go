package testtiming

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestCITierCheckWaitsForSameRunSuccessfulSummary(t *testing.T) {
	workflow := loadAdmissionWorkflow(t)
	job := workflow.Jobs["ci-tier"]
	if job.Name != "CI tier: ${{ needs.ci-plan.outputs.profile }}" || !slices.Equal(job.Needs, []string{"ci-plan", "required-tests"}) {
		t.Fatalf("tier has another authority or does not await completion: %+v", job)
	}
	for _, plan := range []string{"success", "failure", "cancelled", "skipped"} {
		for _, summary := range []string{"success", "failure", "cancelled", "skipped"} {
			facts := ciEventFacts("pull_request", false, "")
			facts["needs.ci-plan.result"] = strconv.Quote(plan)
			facts["needs.required-tests.result"] = strconv.Quote(summary)
			if got := evaluateCICondition(t, job.If, facts, false); got != (plan == "success" && summary == "success") {
				t.Fatalf("tier evidence admitted plan=%s summary=%s", plan, summary)
			}
		}
	}
	step := findWorkflowStep(job.Steps, "Publish only completed same-run tier evidence")
	if step == nil || step.ContinueOnError || step.Env["TIER"] != "${{ needs.ci-plan.outputs.profile }}" {
		t.Fatal("tier check bypasses canonical plan")
	}
	for _, tier := range []string{"core", "lifecycle", "full", "", "unknown", "full; exit 0"} {
		output := filepath.Join(t.TempDir(), "summary")
		command := exec.Command("bash", "-c", step.Run)
		command.Env = append(os.Environ(), "TIER="+tier, "PLAN_DIGEST=plan", "EXECUTION_SHA=source", "GITHUB_RUN_ID=42", "GITHUB_RUN_ATTEMPT=2", "GITHUB_STEP_SUMMARY="+output)
		_, err := command.CombinedOutput()
		valid := tier == "core" || tier == "lifecycle" || tier == "full"
		if (err == nil) != valid {
			t.Fatalf("tier %q admission: %v", tier, err)
		}
		if valid {
			data, err := os.ReadFile(output)
			if err != nil || !strings.Contains(string(data), "run 42 attempt 2; execution source; plan plan") {
				t.Fatalf("missing source/run/attempt/plan evidence: %s %v", data, err)
			}
		}
	}
}

func TestDebtAnalysisCIUsesExactReadOnlyRestoreAndProtectedProducer(t *testing.T) {
	workflow := loadAdmissionWorkflow(t)
	execution := findWorkflowStep(workflow.Jobs["proof-unit"].Steps, "Run exact planned proof unit")
	if execution == nil {
		t.Fatal("missing owned proof dispatcher")
	}
	if _, global := execution.Env["SWARM_DEBT_CACHE_DIR"]; global {
		t.Fatal("test-only debt cache env must not reach ordinary or served proof children")
	}
	consumer := findWorkflowStep(workflow.Jobs["proof-unit"].Steps, "Restore exact trusted-base analysis read-only")
	if consumer == nil || consumer.Uses != "actions/cache/restore@v4" || consumer.If != "matrix.unit == 'persistence-authority-debt-census'" || consumer.With["key"] != "${{ steps.debt-identity.outputs.key }}" || consumer.With["restore-keys"] != nil {
		t.Fatal("base analysis consumer is not exact-key/read-only/isolated")
	}
	job := workflow.Jobs["publish-debt-analysis"]
	if !strings.Contains(job.If, "github.ref == 'refs/heads/master'") || !strings.Contains(job.If, "github.event_name == 'push'") || !strings.Contains(job.If, "needs.required-tests.result == 'success'") || strings.Contains(job.If, "master_replay") {
		t.Fatal("producer must qualify protected master and also run after proof replay")
	}
	proof := findWorkflowStep(job.Steps, "Analyze protected-master archive even after proof replay")
	identity := findWorkflowStep(job.Steps, "Resolve exact protected-master cache key")
	restore := findWorkflowStep(job.Steps, "Restore exact immutable analysis")
	save := findWorkflowStep(job.Steps, "Publish exact immutable analysis")
	if proof == nil || !strings.Contains(proof.Run, "TestPersistenceAuthorityDebtAnalysisCachePublishMaster") || proof.ContinueOnError || save == nil || save.Uses != "actions/cache/save@v4" || save.With["key"] != "${{ steps.identity.outputs.key }}" {
		t.Fatal("shared publication is not bound to the successful immutable archive scan")
	}
	const cachePath = "${{ runner.temp }}/debt-analysis"
	if job.Env["SWARM_DEBT_CACHE_DIR"] != "" || job.Env["SWARM_DEBT_CACHE_PUBLISH"] != "1" {
		t.Fatal("runner-local cache path must be resolved at step scope, not unsupported job scope")
	}
	if identity == nil || restore == nil || identity.Env["SWARM_DEBT_CACHE_DIR"] != cachePath || proof.Env["SWARM_DEBT_CACHE_DIR"] != cachePath || restore.With["path"] != cachePath || save.With["path"] != cachePath {
		t.Fatal("producer identity, analysis, restore and save must share one runner-local cache path")
	}
}

func TestNightlyProofReusesExhaustiveOwnerWithoutRemovingPRProof(t *testing.T) {
	workflow := loadAdmissionWorkflow(t)
	job := workflow.Jobs["nightly-proof"]
	if !slices.Equal(job.Needs, []string{"ci-plan", "required-tests", "ci-tier"}) ||
		!strings.Contains(job.If, "github.event_name == 'schedule'") ||
		!strings.Contains(job.If, "github.event_name == 'workflow_dispatch'") ||
		!strings.Contains(job.If, "github.ref == 'refs/heads/master'") ||
		!strings.Contains(job.If, "needs.ci-plan.outputs.profile == 'full'") ||
		!strings.Contains(job.If, "needs.required-tests.result == 'success'") ||
		!strings.Contains(job.If, "needs.ci-tier.result == 'success'") {
		t.Fatal("nightly must qualify the existing exhaustive owner, not partial feedback")
	}
	for _, event := range []string{"schedule", "workflow_dispatch", "pull_request", "push"} {
		for _, ref := range []string{"refs/heads/master", "refs/heads/topic"} {
			for _, status := range []string{"success", "failure", "cancelled", "skipped"} {
				facts := ciEventFacts(event, false, "")
				facts["github.ref"] = strconv.Quote(ref)
				facts["needs.ci-tier.result"] = strconv.Quote(status)
				want := (event == "schedule" || event == "workflow_dispatch") && ref == "refs/heads/master" && status == "success"
				if got := evaluateCICondition(t, job.If, facts, false); got != want {
					t.Fatalf("nightly event=%s ref=%s completion=%s admitted=%v", event, ref, status, got)
				}
			}
		}
	}
	for _, retained := range []string{"proof-unit", "unused-linux", "unused-darwin", "unused-checks", "macos-sqlite-possession"} {
		if !slices.Contains(workflow.Jobs["required-tests"].Needs, retained) || strings.Contains(workflow.Jobs[retained].If, "schedule") {
			t.Fatalf("original PR proof moved before bootstrap: %s", retained)
		}
	}
}
