package testtiming

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const preflightCIAttemptA = "90ca6bd3-114d-4c7a-8af6-30e173bb1b6b"
const preflightCIAttemptB = "62a856ec-3954-46db-a68d-0a8c93537969"

type localCIStatus struct {
	ID          int    `json:"id"`
	State       string `json:"state"`
	Context     string `json:"context"`
	Description string `json:"description"`
}

func localStatus(state, attempt string) localCIStatus {
	return localCIStatus{State: state, Context: "preflight/local", Description: "attempt:" + attempt + " receipt:20261014T000000.000000000-" + attempt}
}

func localStatusHistory(chronology ...localCIStatus) [][]localCIStatus {
	for i := range chronology {
		chronology[i].ID = i + 1
	}
	slices.Reverse(chronology)
	return [][]localCIStatus{chronology}
}

func runLocalCIAdmission(t *testing.T, event, ref string, payload map[string]any, pages any, apiFailure bool) (string, []string, error) {
	t.Helper()
	step := findWorkflowStep(loadAdmissionWorkflow(t).Jobs["ci-preflight"].Steps, "Admit exact source before checkout")
	if step == nil || step.Env["EVENT_NAME"] != "${{ github.event_name }}" || step.Env["GH_TOKEN"] != "${{ github.token }}" {
		t.Fatal("missing native event/read-token admission script")
	}
	dir := t.TempDir()
	writeJSON := func(name string, value any) {
		t.Helper()
		raw, err := json.Marshal(value)
		if err == nil {
			err = os.WriteFile(filepath.Join(dir, name), raw, 0600)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	writeJSON("event.json", payload)
	writeJSON("pages.json", pages)
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$ADMISSION_CALLS\"\n[ \"$ADMISSION_API_FAILURE\" != true ] || exit 1\ncat \"$ADMISSION_PAGES\"\n"
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("bash", "-c", step.Run)
	command.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"EVENT_NAME="+event, "GITHUB_REF="+ref, "GITHUB_SHA="+strings.Repeat("b", 40), "GITHUB_REPOSITORY=division-sh/swarm",
		"GITHUB_EVENT_PATH="+filepath.Join(dir, "event.json"), "GITHUB_STEP_SUMMARY="+filepath.Join(dir, "summary"), "RUNNER_TEMP="+dir,
		"ADMISSION_PAGES="+filepath.Join(dir, "pages.json"), "ADMISSION_CALLS="+filepath.Join(dir, "calls"), "ADMISSION_API_FAILURE="+strconv.FormatBool(apiFailure))
	output, err := command.CombinedOutput()
	if summary, readErr := os.ReadFile(filepath.Join(dir, "summary")); readErr == nil {
		output = append(output, summary...)
	}
	var calls []string
	if raw, readErr := os.ReadFile(filepath.Join(dir, "calls")); readErr == nil {
		calls = strings.Split(strings.TrimSpace(string(raw)), "\n")
	}
	return string(output), calls, err
}

func localCIEvent(event, repository, branch string) (string, map[string]any) {
	ref := "refs/heads/master"
	root := map[string]any{"repository": map[string]any{"full_name": "division-sh/swarm"}}
	switch event {
	case "pull_request":
		ref = "refs/pull/17/merge"
		root["action"] = "synchronize"
		root["pull_request"] = map[string]any{"draft": false, "head": map[string]any{
			"repo": map[string]any{"full_name": repository}, "sha": strings.Repeat("a", 40), "ref": branch,
		}}
	case "push":
		ref = "refs/heads/" + branch
		root["ref"], root["after"], root["deleted"] = ref, strings.Repeat("b", 40), false
	case "merge_group":
		ref = "refs/heads/gh-readonly-queue/master/pr-17"
		root["action"] = "checks_requested"
		root["merge_group"] = map[string]any{"head_ref": ref, "head_sha": strings.Repeat("b", 40), "base_ref": "refs/heads/master"}
	}
	return ref, root
}

func TestLocalPreflightCIReadsLatestStartedAttempt(t *testing.T) {
	ref, event := localCIEvent("pull_request", "division-sh/swarm", "agent-f/topic")
	for _, tc := range []struct {
		name string
		list []localCIStatus
		pass bool
	}{
		{"complete", []localCIStatus{localStatus("pending", preflightCIAttemptA), localStatus("success", preflightCIAttemptA)}, true},
		{"missing", nil, false},
		{"manual-success", []localCIStatus{localStatus("success", preflightCIAttemptA)}, false},
		{"death-after-pending", []localCIStatus{localStatus("pending", preflightCIAttemptA)}, false},
		{"canceled-after-pending", []localCIStatus{localStatus("pending", preflightCIAttemptA), localStatus("error", preflightCIAttemptA)}, false},
		{"failed", []localCIStatus{localStatus("pending", preflightCIAttemptA), localStatus("failure", preflightCIAttemptA)}, false},
		{"older-success-after-new-pending", []localCIStatus{localStatus("pending", preflightCIAttemptA), localStatus("pending", preflightCIAttemptB), localStatus("success", preflightCIAttemptA)}, false},
		{"older-success-after-new-failure", []localCIStatus{localStatus("pending", preflightCIAttemptA), localStatus("pending", preflightCIAttemptB), localStatus("failure", preflightCIAttemptB), localStatus("success", preflightCIAttemptA)}, false},
		{"newer-success-last", []localCIStatus{localStatus("pending", preflightCIAttemptA), localStatus("pending", preflightCIAttemptB), localStatus("success", preflightCIAttemptA), localStatus("success", preflightCIAttemptB)}, true},
		{"older-success-last", []localCIStatus{localStatus("pending", preflightCIAttemptA), localStatus("pending", preflightCIAttemptB), localStatus("success", preflightCIAttemptB), localStatus("success", preflightCIAttemptA)}, false},
		{"older-failure-last", []localCIStatus{localStatus("pending", preflightCIAttemptA), localStatus("pending", preflightCIAttemptB), localStatus("success", preflightCIAttemptB), localStatus("failure", preflightCIAttemptA)}, false},
		{"newer-failure-last", []localCIStatus{localStatus("pending", preflightCIAttemptA), localStatus("pending", preflightCIAttemptB), localStatus("success", preflightCIAttemptA), localStatus("failure", preflightCIAttemptB)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, _, err := runLocalCIAdmission(t, "pull_request", ref, event, localStatusHistory(tc.list...), false)
			if (err == nil) != tc.pass {
				t.Fatalf("admitted=%t want=%t: %v\n%s", err == nil, tc.pass, err, out)
			}
		})
	}
}

func TestLocalPreflightCIRefusesMalformedTransportAndIdentity(t *testing.T) {
	ref, event := localCIEvent("pull_request", "division-sh/swarm", "topic")
	for _, mode := range []string{"missing-attempt", "wrong-receipt", "different-attempt", "invalid-attempt", "wrong-context-case", "non-numeric-id", "duplicate-id", "out-of-order", "api-error", "invalid-pages", "missing-start"} {
		t.Run(mode, func(t *testing.T) {
			pages := localStatusHistory(localStatus("pending", preflightCIAttemptA), localStatus("success", preflightCIAttemptA))
			var transport any = pages
			switch mode {
			case "missing-attempt":
				pages[0][0].Description = "receipt:20261014T000000.000000000"
			case "wrong-receipt":
				pages[0][0].Description = strings.Replace(pages[0][0].Description, "receipt:20261014", "receipt:20261015", 1)
			case "different-attempt":
				pages[0][0].Description = localStatus("success", preflightCIAttemptB).Description
			case "invalid-attempt":
				pages[0][0].Description = strings.ReplaceAll(pages[0][0].Description, preflightCIAttemptA, "invalid")
			case "wrong-context-case":
				pages[0][0].Context = "Preflight/local"
			case "non-numeric-id":
				transport = [][]map[string]any{{{"id": "2", "state": "success", "context": "preflight/local", "description": pages[0][0].Description}}}
			case "duplicate-id":
				pages[0][1].ID = pages[0][0].ID
			case "out-of-order":
				pages[0] = append(pages[0], localCIStatus{ID: 10, State: "failure", Context: "preflight/local"})
			case "invalid-pages":
				transport = map[string]string{"message": "unavailable"}
			case "missing-start":
				pages[0] = pages[0][:1]
			}
			out, _, err := runLocalCIAdmission(t, "pull_request", ref, event, transport, mode == "api-error")
			if err == nil {
				t.Fatalf("invalid transport/identity admitted: %s", out)
			}
		})
	}
}

func TestLocalPreflightCIEventAndPaginationBoundaries(t *testing.T) {
	for _, keepGoing := range []bool{false, true} {
		ref, event := localCIEvent("workflow_dispatch", "division-sh/swarm", "master")
		event["inputs"] = map[string]any{"keep_going": keepGoing, "profile": "full"}
		if out, calls, err := runLocalCIAdmission(t, "workflow_dispatch", ref, event, nil, true); err != nil || len(calls) != 0 {
			t.Fatalf("dispatch keep_going=%t lost its status-only exemption: %v %s", keepGoing, err, out)
		}
	}
	for _, tc := range []struct {
		event, repo, branch string
		exempt              bool
	}{
		{"pull_request", "division-sh/swarm", "topic", false},
		{"pull_request", "division-sh/swarm", "automation/weights", true},
		{"pull_request", "outside/fork", "automation/weights", false},
		{"pull_request", "outside/fork", "topic", false},
		{"pull_request", "division-sh/swarm", "gh-readonly-queue/master/pr-17", false},
		{"push", "division-sh/swarm", "master", true},
		{"push", "division-sh/swarm", "main", false},
		{"schedule", "division-sh/swarm", "master", true},
		{"workflow_dispatch", "division-sh/swarm", "master", true},
		{"merge_group", "division-sh/swarm", "master", true},
	} {
		t.Run(tc.event+"/"+tc.repo+"/"+tc.branch, func(t *testing.T) {
			ref, event := localCIEvent(tc.event, tc.repo, tc.branch)
			pages := localStatusHistory(localStatus("pending", preflightCIAttemptA), localStatus("success", preflightCIAttemptA))
			pages = [][]localCIStatus{{pages[0][0]}, {pages[0][1]}}
			out, calls, err := runLocalCIAdmission(t, tc.event, ref, event, pages, false)
			if err != nil {
				t.Fatalf("admission: %v\n%s", err, out)
			}
			if tc.exempt {
				if len(calls) != 0 {
					t.Fatalf("exemption used status API: %v", calls)
				}
				return
			}
			head := strings.Repeat("a", 40)
			if tc.event == "push" {
				head = strings.Repeat("b", 40)
			}
			want := []string{"api", "--hostname", "github.com", "--paginate", "--slurp", "repos/" + tc.repo + "/commits/" + head + "/statuses?per_page=100"}
			if !slices.Equal(calls, want) {
				t.Fatalf("wrong head repository/SHA or pagination: %v want=%v", calls, want)
			}
		})
	}
	for _, mode := range []string{"unknown", "foreign-repository", "missing-head", "bad-head", "invalid-ref", "invalid-action", "bad-merge-group", "framing-collision"} {
		t.Run(mode, func(t *testing.T) {
			name := "pull_request"
			if mode == "unknown" {
				name = "pull_request_target"
			} else if mode == "bad-merge-group" {
				name = "merge_group"
			}
			ref, event := localCIEvent(name, "division-sh/swarm", "topic")
			switch mode {
			case "foreign-repository":
				event["repository"] = map[string]any{"full_name": "foreign/repository"}
			case "missing-head":
				delete(event["pull_request"].(map[string]any), "head")
			case "bad-head":
				event["pull_request"].(map[string]any)["head"].(map[string]any)["sha"] = "invalid"
			case "framing-collision":
				head := event["pull_request"].(map[string]any)["head"].(map[string]any)
				head["repo"].(map[string]any)["full_name"] = "division-sh/swarm\n" + strings.Repeat("a", 40)
				head["sha"] = nil
			case "invalid-ref", "bad-merge-group":
				ref = "refs/heads/not-a-pr"
			case "invalid-action":
				event["action"] = "unknown"
			}
			pages := localStatusHistory(localStatus("pending", preflightCIAttemptA), localStatus("success", preflightCIAttemptA))
			if out, calls, err := runLocalCIAdmission(t, name, ref, event, pages, false); err == nil || len(calls) != 0 {
				t.Fatalf("malformed event admitted: %s", out)
			}
		})
	}
}

func TestLocalPreflightCIHasNoExpensiveBypass(t *testing.T) {
	workflow := loadAdmissionWorkflow(t)
	admission := workflow.Jobs["ci-preflight"]
	if admission.TimeoutMinutes != 2 || len(admission.Needs) != 0 || len(admission.Steps) != 1 || admission.Permissions["statuses"] != "read" || len(admission.Permissions) != 1 || admission.Steps[0].Uses != "" {
		t.Fatal("admission must use only runner tools with statuses:read before checkout/Go")
	}
	for _, name := range []string{"complexity", "static-checks"} {
		if !slices.Equal(workflow.Jobs[name].Needs, []string{"ci-preflight"}) {
			t.Fatalf("%s bypasses cheap admission or waits for expensive plan", name)
		}
	}
	for _, state := range []string{"success", "failure", "cancelled", "skipped", "", "unknown"} {
		for _, event := range []string{"pull_request", "push", "schedule", "workflow_dispatch", "merge_group"} {
			for _, draft := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/draft=%t", state, event, draft), func(t *testing.T) {
					facts := ciEventFacts(event, draft, `{"include":[]}`)
					facts["needs.ci-preflight.result"] = strconv.Quote(state)
					facts["needs.ci-plan.result"] = strconv.Quote("skipped")
					for _, name := range []string{"ci-plan", "complexity", "static-checks", "sqlite-local-dev", "timing-budget", "ci-tier"} {
						want := state == "success"
						if name == "ci-tier" || ((name == "ci-plan" || name == "sqlite-local-dev" || name == "timing-budget") && event == "pull_request" && draft) {
							want = false
						}
						if got := evaluateCICondition(t, workflow.Jobs[name].If, facts, state == "success"); got != want {
							t.Fatalf("%s executes=%t want=%t", name, got, want)
						}
						if name != "complexity" && name != "static-checks" && !slices.Contains(workflow.Jobs[name].Needs, "ci-preflight") {
							t.Fatalf("%s has no admission dependency", name)
						}
					}
					facts["needs.ci-plan.outputs.master_replay"] = strconv.Quote("false")
					for _, step := range workflow.Jobs["required-tests"].Steps {
						if state != "success" && evaluateCICondition(t, step.If, facts, true) && step.Name != "Refuse missing local preflight admission" {
							t.Fatalf("denied admission reaches summary build/plan step %s/%s", step.Name, step.Uses)
						}
					}
					if !evaluateCICondition(t, workflow.Jobs["required-tests"].If, facts, false) {
						t.Fatal("denied summary disappeared")
					}
					if state != "success" {
						facts["needs.required-tests.result"] = strconv.Quote("failure")
						facts["needs.ci-tier.result"] = strconv.Quote("skipped")
						facts["cancelled()"] = strconv.FormatBool(state == "cancelled")
						for name, job := range workflow.Jobs {
							if name != "ci-preflight" && name != "required-tests" && evaluateCICondition(t, job.If, facts, false) {
								t.Fatalf("denied admission reaches downstream consumer %s", name)
							}
						}
						step := findWorkflowStep(workflow.Jobs["required-tests"].Steps, "Refuse missing local preflight admission")
						cmd := exec.Command("bash", "-c", step.Run)
						cmd.Env = append(os.Environ(), "GITHUB_STEP_SUMMARY="+filepath.Join(t.TempDir(), "summary"))
						if out, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(out), "no checkout") {
							t.Fatalf("denied summary: %v %s", err, out)
						}
					}
				})
			}
		}
	}
}
