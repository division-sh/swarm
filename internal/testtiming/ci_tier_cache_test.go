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

func TestCITierCheckRecordsSameRunSelectionAndOutcome(t *testing.T) {
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
			if got := evaluateCICondition(t, job.If, facts, false); got != (plan == "success") {
				t.Fatalf("tier evidence admitted plan=%s summary=%s", plan, summary)
			}
		}
	}
	step := findWorkflowStep(job.Steps, "Record same-run tier selection and proof outcome")
	if step == nil || step.ContinueOnError || step.Env["TIER"] != "${{ needs.ci-plan.outputs.profile }}" || step.Env["PROOF_RESULT"] != "${{ needs.required-tests.result }}" {
		t.Fatal("tier check bypasses canonical plan")
	}
	for _, tier := range []string{"core", "lifecycle", "full", "", "unknown", "full; exit 0"} {
		output := filepath.Join(t.TempDir(), "summary")
		command := exec.Command("bash", "-c", step.Run)
		command.Env = append(os.Environ(), "TIER="+tier, "PROOF_RESULT=failure", "PLAN_DIGEST=plan", "EXECUTION_SHA=source", "GITHUB_RUN_ID=42", "GITHUB_RUN_ATTEMPT=2", "GITHUB_STEP_SUMMARY="+output)
		_, err := command.CombinedOutput()
		valid := tier == "core" || tier == "lifecycle" || tier == "full"
		if (err == nil) != valid {
			t.Fatalf("tier %q admission: %v", tier, err)
		}
		if valid {
			data, err := os.ReadFile(output)
			if err != nil || !strings.Contains(string(data), "run 42 attempt 2; execution source; plan plan; Required test summary: failure") || !strings.Contains(string(data), "Tier selection alone is not qualification") || strings.Contains(string(data), "qualified by") {
				t.Fatalf("missing source/run/attempt/plan evidence: %s %v", data, err)
			}
		}
	}
	for _, outcome := range []string{"success", "failure", "cancelled", "skipped", "", "unknown"} {
		output := filepath.Join(t.TempDir(), "summary")
		command := exec.Command("bash", "-c", step.Run)
		command.Env = append(os.Environ(), "TIER=full", "PROOF_RESULT="+outcome, "PLAN_DIGEST=plan", "EXECUTION_SHA=source", "GITHUB_RUN_ID=42", "GITHUB_RUN_ATTEMPT=2", "GITHUB_STEP_SUMMARY="+output)
		_, err := command.CombinedOutput()
		valid := outcome == "success" || outcome == "failure" || outcome == "cancelled" || outcome == "skipped"
		if (err == nil) != valid {
			t.Fatalf("summary outcome %q admission: %v", outcome, err)
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

func TestDebtAnalysisCIPublisherPreservesArchiveContract(t *testing.T) {
	job := loadAdmissionWorkflow(t).Jobs["publish-debt-analysis"]
	if !slices.Equal(job.Needs, []string{"ci-plan", "required-tests"}) || job.TimeoutMinutes != 20 || len(job.Permissions) != 1 || job.Permissions["contents"] != "read" {
		t.Fatal("producer dependencies, deadline or permissions changed")
	}
	restore := findWorkflowStep(job.Steps, "Restore exact immutable analysis")
	save := findWorkflowStep(job.Steps, "Publish exact immutable analysis")
	if restore == nil || save == nil {
		t.Fatal("producer must retain the existing restore and save owners")
	}
	if restore.Uses != "actions/cache/restore@v4" || restore.With["key"] != "${{ steps.identity.outputs.key }}" || restore.With["restore-keys"] != nil || save.If != "steps.analysis.outputs.cache-hit != 'true' && success()" {
		t.Fatal("producer must restore and save only its exact successful immutable archive")
	}
	if len(job.Steps) != 6 || job.Steps[0].Uses != "actions/checkout@v4" || job.Steps[0].With["ref"] != "${{ needs.ci-plan.outputs.execution_sha }}" || job.Steps[0].With["fetch-depth"] != 0 {
		t.Fatal("producer checkout must retain the qualified execution source and full archive history")
	}
}

func TestDebtAnalysisCIPublisherReplayAdmission(t *testing.T) {
	job := loadAdmissionWorkflow(t).Jobs["publish-debt-analysis"]
	for _, tc := range []struct {
		name, event, ref               string
		needsSucceeded, canceled, want bool
	}{
		{"master-replay", "push", "refs/heads/master", false, false, true},
		{"master-push", "push", "refs/heads/master", true, false, true},
		{"master-schedule", "schedule", "refs/heads/master", true, false, true},
		{"canceled-replay", "push", "refs/heads/master", false, true, false},
		{"canceled-push", "push", "refs/heads/master", true, true, false},
		{"canceled-schedule", "schedule", "refs/heads/master", true, true, false},
		{"PR-master-ref", "pull_request", "refs/heads/master", true, false, false},
		{"fork-PR", "pull_request", "refs/pull/1/merge", true, false, false},
		{"topic-push", "push", "refs/heads/topic", true, false, false},
		{"main-push", "push", "refs/heads/main", true, false, false},
		{"topic-schedule", "schedule", "refs/heads/topic", true, false, false},
		{"manual-master", "workflow_dispatch", "refs/heads/master", true, false, false},
		{"missing-ref", "push", "", true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			facts := ciEventFacts(tc.event, false, "")
			facts["github.ref"] = strconv.Quote(tc.ref)
			facts["needs.ci-plan.result"] = strconv.Quote("success")
			facts["cancelled()"] = strconv.FormatBool(tc.canceled)
			if got := evaluateCICondition(t, job.If, facts, tc.needsSucceeded); got != tc.want {
				t.Fatalf("publisher admitted=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestDebtAnalysisCIPublisherRejectsUnsuccessfulAuthorities(t *testing.T) {
	job := loadAdmissionWorkflow(t).Jobs["publish-debt-analysis"]
	for _, owner := range []string{"ci-plan", "required-tests"} {
		for _, status := range []string{"", "skipped", "failure", "cancelled", "unknown"} {
			t.Run(owner+"/"+status, func(t *testing.T) {
				facts := ciEventFacts("push", false, "")
				facts["github.ref"] = strconv.Quote("refs/heads/master")
				facts["needs.ci-plan.result"] = strconv.Quote("success")
				facts["needs."+owner+".result"] = strconv.Quote(status)
				facts["cancelled()"] = "false"
				if evaluateCICondition(t, job.If, facts, true) {
					t.Fatal("unsuccessful authority admitted publication")
				}
			})
		}
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
	for _, outcome := range []string{"failure", "cancelled", "skipped"} {
		facts := ciEventFacts("schedule", false, "")
		facts["github.ref"] = strconv.Quote("refs/heads/master")
		facts["needs.ci-tier.result"] = strconv.Quote("success")
		facts["needs.required-tests.result"] = strconv.Quote(outcome)
		if evaluateCICondition(t, job.If, facts, false) {
			t.Fatalf("successful tier metadata granted nightly credit for %s proof", outcome)
		}
	}
	for _, retained := range []string{"proof-unit", "unused-linux", "unused-darwin", "unused-checks", "macos-sqlite-possession"} {
		if !slices.Contains(workflow.Jobs["required-tests"].Needs, retained) || strings.Contains(workflow.Jobs[retained].If, "schedule") {
			t.Fatalf("original PR proof moved before bootstrap: %s", retained)
		}
	}
}
