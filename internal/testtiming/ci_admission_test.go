package testtiming

import (
	"go/constant"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
	"gopkg.in/yaml.v3"
)

type admissionWorkflow struct {
	On struct {
		PullRequest struct {
			Types []string `yaml:"types"`
		} `yaml:"pull_request"`
		WorkflowDispatch struct {
			Inputs map[string]struct {
				Default string   `yaml:"default"`
				Options []string `yaml:"options"`
			} `yaml:"inputs"`
		} `yaml:"workflow_dispatch"`
	} `yaml:"on"`
	Concurrency struct {
		Group  string `yaml:"group"`
		Cancel bool   `yaml:"cancel-in-progress"`
	} `yaml:"concurrency"`
	Jobs map[string]ciWorkflowJob `yaml:"jobs"`
}

func loadAdmissionWorkflow(t *testing.T) admissionWorkflow {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testTimingRepoRoot(t), ".github/workflows/ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow admissionWorkflow
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	return workflow
}

// Evaluate the workflow's actual boolean conditions, not a copied draft predicate.
// These conditions use the common constant boolean/string expression subset.
func evaluateCICondition(t *testing.T, expression string, facts map[string]string, needsSucceeded bool) bool {
	t.Helper()
	expression = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(expression, "${{"), "}}"))
	if expression == "" {
		return needsSucceeded
	}
	explicitStatus := strings.Contains(expression, "always()") || strings.Contains(expression, "success()")
	expression = regexp.MustCompile(`'([^']*)'`).ReplaceAllStringFunc(expression, func(value string) string {
		return strconv.Quote(value[1 : len(value)-1])
	})
	expression = regexp.MustCompile(`github\.[a-z_.]+|needs\.[a-z_.-]+|always\(\)|success\(\)`).ReplaceAllStringFunc(expression, func(key string) string {
		switch key {
		case "always()":
			return "true"
		case "success()":
			return strconv.FormatBool(needsSucceeded)
		default:
			value, ok := facts[key]
			if !ok {
				t.Fatalf("unbound condition fact %s", key)
			}
			return value
		}
	})
	value, err := types.Eval(token.NewFileSet(), nil, token.NoPos, expression)
	if err != nil || value.Value == nil || value.Value.Kind() != constant.Bool {
		t.Fatalf("condition %q: %v", expression, err)
	}
	return constant.BoolVal(value.Value) && (explicitStatus || needsSucceeded)
}

func ciEventFacts(event string, draft bool, soak string) map[string]string {
	return map[string]string{
		"github.event_name":                   strconv.Quote(event),
		"github.event.pull_request.draft":     strconv.FormatBool(draft),
		"needs.ci-plan.outputs.soak_matrix":   strconv.Quote(soak),
		"needs.required-tests.result":         strconv.Quote("success"),
		"needs.ci-plan.outputs.master_replay": strconv.Quote("false"),
		"needs.ci-plan.outputs.proof_matrix":  strconv.Quote(`{"include":[{"unit":"ordinary"}]}`),
		"needs.ci-plan.outputs.profile": strconv.Quote(func() string {
			if event == "pull_request" && draft {
				return ""
			}
			return "full"
		}()),
	}
}

func TestCITierIncreaseKeepsLateSummaryAndNewHeadEvent(t *testing.T) {
	workflow := loadAdmissionWorkflow(t)
	if slices.Contains(workflow.On.PullRequest.Types, "edited") ||
		!slices.Contains(workflow.On.PullRequest.Types, "synchronize") ||
		!slices.Contains(workflow.On.PullRequest.Types, "ready_for_review") {
		t.Fatal("tier increases must use new-head/ready qualification, not redundant edited runs")
	}
	summary := workflow.Jobs["required-tests"]
	wantNeeds := []string{"complexity", "ci-plan", "static-checks", "sqlite-local-dev", "macos-sqlite-possession", "unused-linux", "unused-darwin", "unused-checks", "proof-unit", "semantic-smoke", "timing-budget"}
	if summary.TimeoutMinutes != 5 || !slices.Equal(summary.Needs, wantNeeds) {
		t.Fatalf("summary must remain late and bounded: timeout=%v needs=%v", summary.TimeoutMinutes, summary.Needs)
	}
	step := findWorkflowStep(summary.Steps, "Revalidate current PR tier")
	if step == nil || !strings.Contains(step.Run, "-check-ci-tier") ||
		!strings.Contains(step.Run, "-workflow-head-sha") ||
		!strings.Contains(step.Run, "current-pr.json") {
		t.Fatal("late summary lost exact current-body/head refusal")
	}
}

func TestCIManualDispatchFullSummaryHasOnlyFullAdmission(t *testing.T) {
	workflow := loadAdmissionWorkflow(t)
	profile, ok := workflow.On.WorkflowDispatch.Inputs["profile"]
	if !ok || profile.Default != testplanning.ProfileFull || !slices.Equal(profile.Options, []string{testplanning.ProfileFull}) {
		t.Fatalf("manual full summary admits thin input: %+v", profile)
	}
	if !strings.Contains(workflow.Jobs["required-tests"].Name, "Full dispatch summary") {
		t.Fatal("manual exhaustive summary lost its full label")
	}
}

func TestCIDraftAdmissionAndReadyTransitions(t *testing.T) {
	workflow := loadAdmissionWorkflow(t)
	actions := []string{"opened", "synchronize", "reopened", "ready_for_review", "converted_to_draft"}
	if !slices.Equal(workflow.On.PullRequest.Types, actions) {
		t.Fatalf("PR subscriptions = %v, want %v", workflow.On.PullRequest.Types, actions)
	}
	heavy := []string{"ci-plan", "sqlite-local-dev", "macos-sqlite-possession", "unused-linux", "unused-darwin", "unused-checks", "proof-unit", "semantic-smoke", "timing-budget"}
	for _, action := range actions {
		for _, draft := range []bool{false, true} {
			t.Run(action+"/draft="+strconv.FormatBool(draft), func(t *testing.T) {
				facts := ciEventFacts("pull_request", draft, `{"include":[{"unit":"soak"}]}`)
				for _, job := range heavy {
					if got := evaluateCICondition(t, workflow.Jobs[job].If, facts, true); got == draft {
						t.Errorf("%s admitted=%t, draft=%t", job, got, draft)
					}
				}
				for _, job := range []string{"complexity", "static-checks", "required-tests"} {
					if !evaluateCICondition(t, workflow.Jobs[job].If, facts, true) {
						t.Errorf("missing %s feedback/refusal", job)
					}
				}
			})
		}
	}
	for _, event := range []string{"push", "workflow_dispatch", "schedule"} {
		for _, job := range heavy {
			if !evaluateCICondition(t, workflow.Jobs[job].If, ciEventFacts(event, true, `{"include":[{"unit":"soak"}]}`), true) {
				t.Errorf("%s/%s incorrectly treated as draft", event, job)
			}
		}
	}
	// Adding a title/label/actor selector must not silently become a WIP authority.
	for job, definition := range workflow.Jobs {
		for _, forbidden := range []string{".title", ".labels", "github.actor", "github.event.action"} {
			if strings.Contains(definition.If, forbidden) {
				t.Errorf("%s uses %s admission heuristic", job, forbidden)
			}
		}
	}
}

func TestCIDraftSkipsPlanAndProofExpansion(t *testing.T) {
	workflow := loadAdmissionWorkflow(t)
	for _, soak := range []string{"", "invalid JSON", `{"include":[]}`, `{"include":[{"unit":"soak"}]}`} {
		facts := ciEventFacts("pull_request", true, soak)
		for _, job := range []string{"ci-plan", "proof-unit", "timing-budget"} {
			if evaluateCICondition(t, workflow.Jobs[job].If, facts, true) {
				t.Fatalf("draft %s evaluated plan/matrix/download with %q", job, soak)
			}
		}
	}
	for _, job := range []string{"proof-unit", "unused-checks"} {
		if evaluateCICondition(t, workflow.Jobs[job].If, ciEventFacts("pull_request", false, "invalid JSON"), false) {
			t.Fatalf("%s admits a failed/missing dependency before matrix/artifact consumption", job)
		}
	}
	if !evaluateCICondition(t, workflow.Jobs["timing-budget"].If, ciEventFacts("pull_request", false, ""), false) {
		t.Fatal("ready timing evaluator must still refuse incomplete evidence")
	}
	facts := ciEventFacts("pull_request", false, `{"include":[]}`)
	facts["needs.ci-plan.outputs.proof_matrix"] = strconv.Quote(`{"include":[]}`)
	if evaluateCICondition(t, workflow.Jobs["proof-unit"].If, facts, true) {
		t.Fatal("empty unified proof selection admitted")
	}
	for _, job := range []string{"proof-unit"} {
		if workflow.Jobs[job].Strategy.FailFast == nil || *workflow.Jobs[job].Strategy.FailFast != "${{ !inputs.keep_going }}" || workflow.Jobs[job].Strategy.MaxParallel != nil {
			t.Fatalf("%s lost full matrix evidence", job)
		}
	}
}

func executeCISummary(t *testing.T, draft bool, statuses map[string]string, soak string) ([]byte, error) {
	t.Helper()
	job := loadAdmissionWorkflow(t).Jobs["required-tests"]
	step := findWorkflowStep(job.Steps, "Summarize required checks")
	if step == nil {
		t.Fatal("missing summary")
	}
	script := step.Run
	for _, need := range job.Needs {
		status := "success"
		if override, ok := statuses[need]; ok {
			status = override
		}
		script = strings.ReplaceAll(script, "${{ needs."+need+".result }}", status)
	}
	script = strings.ReplaceAll(script, "${{ needs.ci-plan.outputs.soak_matrix }}", soak)
	script = strings.ReplaceAll(script, "${{ needs.ci-plan.outputs.profile }}", "full")
	script = strings.ReplaceAll(script, "${{ needs.ci-plan.outputs.master_replay }}", "false")
	if step.Env["IS_DRAFT"] != "${{ github.event_name == 'pull_request' && github.event.pull_request.draft }}" {
		t.Fatal("summary does not consume native draft authority")
	}
	command := exec.Command("bash", "-c", script)
	command.Env = append(os.Environ(), "IS_DRAFT="+strconv.FormatBool(draft), "GITHUB_STEP_SUMMARY="+filepath.Join(t.TempDir(), "summary"))
	return command.CombinedOutput()
}

func TestCIRequiredSummaryRefusesDraftQualification(t *testing.T) {
	for _, statuses := range []map[string]string{nil, {"ci-plan": "skipped", "proof-unit": "skipped", "timing-budget": "skipped"}} {
		out, err := executeCISummary(t, true, statuses, "")
		if err == nil || !strings.Contains(string(out), "Draft PR is not qualified") {
			t.Fatalf("draft summary: %v %s", err, out)
		}
	}
	for _, need := range loadAdmissionWorkflow(t).Jobs["required-tests"].Needs {
		for _, status := range []string{"success", "failure", "skipped", "cancelled", "", "unknown"} {
			t.Run(need+"/"+status, func(t *testing.T) {
				out, err := executeCISummary(t, false, map[string]string{need: status}, `{"include":[{"unit":"soak"}]}`)
				if (err == nil) != (status == "success") {
					t.Fatalf("ready %s=%q: %v %s", need, status, err, out)
				}
			})
		}
	}
	if out, err := executeCISummary(t, false, nil, `{"include":[]}`); err != nil {
		t.Fatalf("valid empty soak: %v %s", err, out)
	}
}

func TestCISameSHARequiredContextTransition(t *testing.T) {
	// Execute separate attempts of the real summary, retaining historical results.
	// Actual GitHub protected-rollup acceptance is a separate mandatory hosted proof.
	var history []bool
	for _, draft := range []bool{true, false, true, false} {
		_, err := executeCISummary(t, draft, nil, `{"include":[]}`)
		history = append(history, err == nil)
	}
	if !slices.Equal(history, []bool{false, true, false, true}) {
		t.Fatalf("same-head qualification history = %v", history)
	}
}

func TestCIConcurrencyNamespaceIsolation(t *testing.T) {
	workflow := loadAdmissionWorkflow(t)
	if !workflow.Concurrency.Cancel {
		t.Fatal("superseded runs must cancel")
	}
	render := func(repo, fork, head, ref, event string) string {
		facts := map[string]string{"github.repository": repo, "github.event.pull_request.head.repo.full_name": fork, "github.head_ref": head, "github.ref_name": ref, "github.event_name": event}
		return regexp.MustCompile(`\$\{\{(.*?)\}\}`).ReplaceAllStringFunc(workflow.Concurrency.Group, func(expression string) string {
			for _, key := range strings.Split(strings.TrimSuffix(strings.TrimPrefix(expression, "${{"), "}}"), "||") {
				if value := facts[strings.TrimSpace(key)]; value != "" {
					return value
				}
			}
			t.Fatalf("unbound concurrency expression %s", expression)
			return ""
		})
	}
	pr := render("division-sh/swarm", "division-sh/swarm", "agent-g/test", "1/merge", "pull_request")
	if pr != render("division-sh/swarm", "division-sh/swarm", "agent-g/test", "2/merge", "pull_request") {
		t.Fatal("same branch cannot cancel obsolete work")
	}
	for _, other := range []string{
		render("division-sh/swarm", "division-sh/swarm", "agent-a/test", "2/merge", "pull_request"),
		render("division-sh/swarm", "other/swarm", "agent-g/test", "2/merge", "pull_request"),
		render("division-sh/swarm", "", "", "agent-g/test", "workflow_dispatch"),
	} {
		if other == pr {
			t.Fatal("concurrency crosses repository/branch/event authority")
		}
	}
}

func TestGeneratedPublisherUsesOnlyAutomaticPRQualification(t *testing.T) {
	step := findWorkflowStep(loadAdmissionWorkflow(t).Jobs["publish-timing-model"].Steps, "Generate and publish model-only PR")
	if step == nil {
		t.Fatal("missing publisher")
	}
	for _, tc := range []struct {
		name, result, pr, failure string
		refs, foreign, success    bool
	}{
		{name: "create", result: "changed", success: true},
		{name: "update", result: "changed", pr: "2325", refs: true, success: true},
		{name: "unchanged", result: "unchanged", success: true},
		{name: "invalid result", result: "unexpected"},
		{name: "foreign diff", result: "changed", foreign: true},
		{name: "staging failure", result: "changed", failure: "--method POST repos/division-sh/swarm/git/refs"},
		{name: "blob failure", result: "changed", failure: "--method PUT"},
		{name: "stable ref failure", result: "changed", refs: true, failure: "--method PATCH repos/division-sh/swarm/git/refs/heads/automation/test-timing-model "},
		{name: "list failure", result: "changed", failure: "pr list"},
		{name: "create failure", result: "changed", failure: "pr create"},
		{name: "edit failure", result: "changed", pr: "2325", refs: true, failure: "pr edit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.MkdirAll(filepath.Join(dir, ".github"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(dir, "test-results"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, testplanning.GeneratedWeightModelPath), []byte("before\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, ".github/foreign.txt"), []byte("before\n"), 0600); err != nil {
				t.Fatal(err)
			}
			for _, args := range [][]string{{"init", "-q"}, {"add", "."}, {"-c", "user.email=test@example.invalid", "-c", "user.name=Publisher proof", "commit", "-qm", "fixture"}} {
				command := exec.Command("git", args...)
				command.Dir = dir
				if out, err := command.CombinedOutput(); err != nil {
					t.Fatalf("git: %v %s", err, out)
				}
			}
			bin := filepath.Join(dir, "bin")
			if err := os.Mkdir(bin, 0700); err != nil {
				t.Fatal(err)
			}
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			for name, script := range map[string]string{
				"go": "#!/bin/bash\nexec \"$PUBLISHER_TEST_BINARY\" -test.run='^TestPublisherCommandProcess$' -- \"$@\"\n",
				"gh": `#!/bin/bash
set -eu
printf '%s\n' "$*" >> "$PUBLISHER_CALLS"
if [ -n "$PUBLISHER_FAILURE" ] && [[ "$*" == *"$PUBLISHER_FAILURE"* ]]; then exit 1; fi
case "$*" in
  api\ repos/division-sh/swarm/git/ref/*) test "$PUBLISHER_REFS" = true ;;
  api\ *--method\ PUT*) echo generated-commit ;;
  api\ repos/division-sh/swarm/contents/*) echo blob ;;
  pr\ list*) printf '%s' "$PUBLISHER_PR" ;;
  workflow*) exit 1 ;;
esac
`,
			} {
				if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0700); err != nil {
					t.Fatal(err)
				}
			}
			calls := filepath.Join(dir, "calls")
			command := exec.Command("bash", "-c", step.Run)
			command.Dir = dir
			command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "PUBLISHER_TEST_BINARY="+executable, "PUBLISHER_PROCESS=1", "PUBLISHER_RESULT="+tc.result, "PUBLISHER_FOREIGN="+strconv.FormatBool(tc.foreign), "PUBLISHER_REFS="+strconv.FormatBool(tc.refs), "PUBLISHER_PR="+tc.pr, "PUBLISHER_FAILURE="+tc.failure, "PUBLISHER_CALLS="+calls, "GITHUB_STEP_SUMMARY="+filepath.Join(dir, "summary"), "GITHUB_RUN_ID=123", "GITHUB_RUN_ATTEMPT=1", "SOURCE_RUN_ID=proof", "EXECUTION_SHA=fixture-sha")
			out, err := command.CombinedOutput()
			if (err == nil) != tc.success {
				t.Fatalf("publisher: %v %s", err, out)
			}
			record, readErr := os.ReadFile(calls)
			if readErr != nil && !os.IsNotExist(readErr) {
				t.Fatal(readErr)
			}
			text := string(record)
			if strings.Contains(text, "workflow") || strings.Contains(text, "pr merge") || strings.Contains(text, "refs/heads/master") {
				t.Fatalf("unexpected publication authority: %s", text)
			}
			if tc.result != "changed" || tc.foreign {
				if text != "" {
					t.Fatalf("nonpublishable change reached GitHub: %s", text)
				}
			} else if tc.success {
				verb := "pr create"
				if tc.pr != "" {
					verb = "pr edit 2325"
				}
				if strings.Count(text, verb) != 1 || !strings.Contains(text, "pr list --head automation/test-timing-model --base master --state open") || !strings.Contains(text, "DELETE repos/division-sh/swarm/git/refs/heads/automation/test-timing-model-build") {
					t.Fatalf("missing canonical publication/cleanup: %s", text)
				}
				if strings.Count(text, "CI-Tier: core\nLocal-Tier: core\n") != 1 || !strings.Contains(text, "--body Generated from independently validated successful full CI evidence.") || !strings.Contains(text, "Part of #2535. Related to #1967.") {
					t.Fatalf("model-only create/update must declare identical honest core tiers: %s", text)
				}
			}
		})
	}
}

// The publisher uses real git and shell control flow; only the timing command's
// update result and external GitHub responses are injected. Diff admission still
// executes the existing canonical Go owner.
func TestPublisherCommandProcess(t *testing.T) {
	if os.Getenv("PUBLISHER_PROCESS") != "1" {
		return
	}
	args := strings.Join(os.Args, " ")
	switch {
	case strings.Contains(args, "-assert-execution-sha"):
		os.Exit(0)
	case strings.Contains(args, "-update-weight-model"):
		if os.Getenv("PUBLISHER_RESULT") == "changed" {
			if err := os.WriteFile(testplanning.GeneratedWeightModelPath, []byte("after\n"), 0600); err != nil {
				os.Exit(1)
			}
			if os.Getenv("PUBLISHER_FOREIGN") == "true" {
				if err := os.WriteFile(".github/foreign.txt", []byte("foreign\n"), 0600); err != nil {
					os.Exit(1)
				}
			}
		}
		_, _ = os.Stdout.WriteString(os.Getenv("PUBLISHER_RESULT"))
		os.Exit(0)
	case strings.Contains(args, "-validate-publish-diff"):
		data, err := os.ReadFile("test-results/generated-diff.txt")
		if err != nil || testplanning.ValidatePublicationDiff(strings.Split(string(data), "\n")) != nil {
			os.Exit(1)
		}
		os.Exit(0)
	default:
		os.Exit(1)
	}
}
