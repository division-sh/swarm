package testtiming

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

var nativeUnusedJobs = []string{"unused-linux", "unused-darwin", "unused-checks"}

type nativeUnusedCadenceCase struct {
	event, ref string
	collect    bool
}

func nativeUnusedCadenceCases() []nativeUnusedCadenceCase {
	return []nativeUnusedCadenceCase{
		{"pull_request", "refs/pull/1/merge", false},
		{"pull_request", "refs/heads/master", false},
		{"push", "refs/heads/master", false},
		{"push", "refs/heads/topic", false},
		{"schedule", "refs/heads/master", true},
		{"schedule", "refs/heads/topic", false},
		{"schedule", "", false},
		{"schedule", "unknown", false},
		{"workflow_dispatch", "refs/heads/master", true},
		{"workflow_dispatch", "refs/heads/topic", true},
		{"workflow_dispatch", "refs/heads/topic'$(false)", true},
		{"", "refs/heads/master", false},
		{"unknown", "refs/heads/master", false},
	}
}

func TestNativeUnusedCadenceConsumesActualWorkflowConditions(t *testing.T) {
	workflow := loadAdmissionWorkflow(t)
	for _, tc := range nativeUnusedCadenceCases() {
		for _, tier := range []string{"core", "lifecycle", "full", "", "unknown"} {
			for _, draft := range []bool{false, true} {
				for _, replay := range []bool{false, true} {
					for _, dependencies := range []bool{false, true} {
						facts := ciEventFacts(tc.event, draft, "")
						facts["github.ref"] = strconv.Quote(tc.ref)
						facts["needs.ci-plan.outputs.profile"] = strconv.Quote(tier)
						facts["needs.ci-plan.outputs.master_replay"] = strconv.Quote(strconv.FormatBool(replay))
						want := tc.collect && tier == "full" && !replay && dependencies
						for _, job := range nativeUnusedJobs {
							if got := evaluateCICondition(t, workflow.Jobs[job].If, facts, dependencies); got != want {
								t.Fatalf("%s event=%q ref=%q tier=%q draft=%t replay=%t dependencies=%t: admitted=%t want=%t", job, tc.event, tc.ref, tier, draft, replay, dependencies, got, want)
							}
						}
					}
				}
			}
		}
	}
}

func nativeUnusedSummaryStatuses(native string, replay bool) map[string]string {
	statuses := map[string]string{}
	for _, job := range nativeUnusedJobs {
		statuses[job] = native
	}
	if replay {
		for _, job := range []string{"macos-sqlite-possession", "proof-unit", "semantic-smoke", "timing-budget"} {
			statuses[job] = "skipped"
		}
	}
	return statuses
}

func TestNativeUnusedRequiredSummaryCadenceAndStrictStatuses(t *testing.T) {
	for _, tc := range nativeUnusedCadenceCases() {
		if tc.event == "" || tc.event == "unknown" {
			continue // Invalid events are refused by ci-plan, not reinterpreted by the summary.
		}
		for _, replay := range []bool{false, true} {
			if replay && tc.event != "push" {
				continue
			}
			for _, owner := range nativeUnusedJobs {
				for _, status := range []string{"success", "failure", "cancelled", "skipped", "", "unknown"} {
					name := fmt.Sprintf("%s/%s/replay=%t/%s/%s", tc.event, tc.ref, replay, owner, status)
					t.Run(name, func(t *testing.T) {
						expected := "skipped"
						if tc.collect {
							expected = "success"
						}
						statuses := nativeUnusedSummaryStatuses(expected, replay)
						statuses[owner] = status
						out, err := executeCISummaryAt(t, tc.event, tc.ref, "full", replay, false, statuses, `{"include":[]}`)
						if (err == nil) != (status == expected) {
							t.Fatalf("native result=%q expected=%q: %v %s", status, expected, err, out)
						}
						if !strings.Contains(string(out), owner+": `"+status+"` (expected `"+expected+"`)") {
							t.Fatalf("native result/expectation is not visible: %s", out)
						}
					})
				}
			}
		}
	}
}

func TestNativeUnusedNightlyFailureCannotMintMasterMarker(t *testing.T) {
	workflow := loadAdmissionWorkflow(t)
	for _, event := range []string{"schedule", "workflow_dispatch"} {
		for _, ref := range []string{"refs/heads/master", "refs/heads/topic"} {
			for _, owner := range nativeUnusedJobs {
				for _, status := range []string{"success", "failure", "cancelled", "skipped", "", "unknown"} {
					statuses := nativeUnusedSummaryStatuses("success", false)
					statuses[owner] = status
					_, err := executeCISummaryAt(t, event, ref, "full", false, false, statuses, `{"include":[]}`)
					facts := ciEventFacts(event, false, "")
					facts["github.ref"] = strconv.Quote(ref)
					facts["needs.ci-tier.result"] = strconv.Quote("success")
					summaryResult := "success"
					if err != nil {
						summaryResult = "failure"
					}
					facts["needs.required-tests.result"] = strconv.Quote(summaryResult)
					want := ref == "refs/heads/master" && status == "success"
					if got := evaluateCICondition(t, workflow.Jobs["nightly-proof"].If, facts, err == nil); got != want {
						t.Fatalf("event=%s ref=%s %s=%q summary=%s: marker=%t want=%t", event, ref, owner, status, summaryResult, got, want)
					}
				}
			}
		}
	}
}

func TestNativeUnusedSkippedJobsCannotHideFailedPlanOrDraft(t *testing.T) {
	for _, tc := range nativeUnusedCadenceCases() {
		for _, plan := range []string{"failure", "cancelled", "skipped", "", "unknown"} {
			statuses := nativeUnusedSummaryStatuses("skipped", false)
			statuses["ci-plan"] = plan
			out, err := executeCISummaryAt(t, tc.event, tc.ref, "", false, false, statuses, `{"include":[]}`)
			if err == nil {
				t.Fatalf("event=%q ref=%q qualified ci-plan=%q: %s", tc.event, tc.ref, plan, out)
			}
		}
	}
	for _, tier := range []string{"core", "lifecycle", "full"} {
		out, err := executeCISummaryAt(t, "pull_request", "refs/pull/1/merge", tier, false, true, nativeUnusedSummaryStatuses("skipped", false), `{"include":[]}`)
		if err == nil || !strings.Contains(string(out), "Draft PR is not qualified") {
			t.Fatalf("draft %s qualified after native skip: %v %s", tier, err, out)
		}
	}
}
