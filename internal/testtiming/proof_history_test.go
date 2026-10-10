package testtiming

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

type proofHistoryCase struct {
	name, pkg                                                         string
	selected, present, fetchFailure, incomplete, changedHead, failure bool
}

func TestProofBatchProvidesHistoricalCorpusInputsBeforeExecution(t *testing.T) {
	const corpus = "github.com/division-sh/swarm/scripts/rewrite-stages-2566"
	for _, tc := range []proofHistoryCase{
		{name: "selected renamed unit", pkg: corpus, selected: true},
		{name: "store transition", pkg: "github.com/division-sh/swarm/internal/store", selected: true},
		{name: "already complete", pkg: corpus, selected: true, present: true},
		{name: "foreign package", pkg: "other/scripts/rewrite-stages-2566", selected: true},
		{name: "unselected corpus", pkg: corpus},
		{name: "fetch refusal", pkg: corpus, selected: true, fetchFailure: true, failure: true},
		{name: "missing fetched objects", pkg: corpus, selected: true, incomplete: true, failure: true},
		{name: "changed execution head", pkg: corpus, selected: true, changedHead: true, failure: true},
	} {
		t.Run(tc.name, func(t *testing.T) { proveProofHistoryPreparation(t, tc) })
	}
}

func proveProofHistoryPreparation(t *testing.T, tc proofHistoryCase) {
	t.Helper()
	root := t.TempDir()
	writeProofHistoryPlan(t, root, tc)
	writeProofHistoryCommands(t, root)
	log := filepath.Join(root, "probe.log")
	command := exec.Command("bash", filepath.Join(testTimingRepoRoot(t), ".github/scripts/run-proof-batch.sh"))
	command.Dir = root
	command.Env = append(os.Environ(), "PATH="+root+string(os.PathListSeparator)+os.Getenv("PATH"), "BATCH_ID=arbitrary-batch", "GITHUB_RUN_ID=1", "GITHUB_RUN_ATTEMPT=1", "GITHUB_STEP_SUMMARY="+filepath.Join(root, "summary"), "PROBE_LOG="+log, "PRESENT="+proofHistoryBool(tc.present), "FETCH_FAILURE="+proofHistoryBool(tc.fetchFailure), "INCOMPLETE="+proofHistoryBool(tc.incomplete), "CHANGED_HEAD="+proofHistoryBool(tc.changedHead))
	output, err := command.CombinedOutput()
	if (err != nil) != tc.failure {
		t.Fatalf("history admission failed=%t want=%t: %v %s", err != nil, tc.failure, err, output)
	}
	raw, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	want := "execute ordinary\n"
	if tc.selected && !tc.present {
		for _, commit := range proofHistoryInputs(t)[tc.pkg] {
			want += "fetch --no-tags --depth=1 origin " + commit + "\n"
			if tc.fetchFailure || tc.incomplete {
				break
			}
		}
	}
	if !tc.failure {
		want += "execute relocated\n"
	}
	if string(raw) != want {
		t.Fatalf("history/proof ordering = %q, want %q", raw, want)
	}
}

func proofHistoryBool(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func writeProofHistoryPlan(t *testing.T, root string, tc proofHistoryCase) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "test-results/plan"), 0700); err != nil {
		t.Fatal(err)
	}
	field := "unselected_roots"
	if tc.selected {
		field = "selected_roots"
	}
	plan, err := json.Marshal(map[string]any{
		"batches": []map[string]any{{"id": "arbitrary-batch", "units": []string{"ordinary", "relocated"}}},
		"units":   []map[string]any{{"id": "ordinary"}, {"id": "relocated", field: []map[string]string{{"package": tc.pkg, "name": "TestRewrite2566CurrentSelectorsRetainExactSourceCorrespondence"}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "test-results/plan/proof-plan.json"), plan, 0600); err != nil {
		t.Fatal(err)
	}
}

func writeProofHistoryCommands(t *testing.T, root string) {
	t.Helper()
	commands := map[string]string{
		"git": `#!/usr/bin/env bash
set -euo pipefail
if [ "$1" = fetch ]; then
  printf 'fetch %s\n' "${*:2}" >> "$PROBE_LOG"
  test "$FETCH_FAILURE" != true
elif [ "$*" = 'rev-parse --is-shallow-repository' ]; then
  exit 1
elif [ "$1" = cat-file ]; then
  test "$PRESENT" = true || { test "$INCOMPLETE" != true && grep -q "${3:0:40}" "$PROBE_LOG"; }
elif [ "$*" = 'rev-parse HEAD' ]; then
  if [ "$CHANGED_HEAD" = true ] && grep -q '^fetch ' "$PROBE_LOG"; then printf 'foreign-sha\n'; else printf 'fixture-sha\n'; fi
else
  exit 1
fi
`,
		"go": `#!/usr/bin/env bash
set -euo pipefail
if [ "$2" = ./cmd/swarm-test ]; then
  printf 'execute %s\n' "$5" >> "$PROBE_LOG"
  printf '{}\n'
fi
previous=''
for arg in "$@"; do
  if [ "$previous" = -evidence ] || [ "$previous" = -markdown ]; then printf '{}\n' > "$arg"; fi
  previous="$arg"
done
`,
	}
	for name, body := range commands {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0700); err != nil {
			t.Fatal(err)
		}
	}
}

func proofHistoryInputs(t *testing.T) map[string][]string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(testTimingRepoRoot(t), ".github/test-proof-history-inputs.json"))
	if err != nil {
		t.Fatal(err)
	}
	var inputs map[string][]string
	if err := json.Unmarshal(body, &inputs); err != nil {
		t.Fatal(err)
	}
	return inputs
}

func TestProofHistoryInputsMatchPinnedSourceConsumers(t *testing.T) {
	files := map[string][]string{
		"github.com/division-sh/swarm/internal/store":              {"internal/store/persistence_authority_debt_run_fixture_transition_test.go"},
		"github.com/division-sh/swarm/scripts/rewrite-stages-2566": {"scripts/rewrite-stages-2566/current_entries_test.go", "scripts/rewrite-stages-2566/embedded_entries.go", "scripts/rewrite-stages-2566/intent.json"},
	}
	inputs := proofHistoryInputs(t)
	if len(inputs) != len(files) {
		t.Fatal("historical consumer package inventory changed")
	}
	pinned := regexp.MustCompile(`"([0-9a-f]{40})(?:"|:)`)
	for pkg, paths := range files {
		commits := map[string]bool{}
		for _, path := range paths {
			body, err := os.ReadFile(filepath.Join(testTimingRepoRoot(t), path))
			if err != nil {
				t.Fatal(err)
			}
			for _, match := range pinned.FindAllSubmatch(body, -1) {
				commits[string(match[1])] = true
			}
		}
		var want []string
		for commit := range commits {
			want = append(want, commit)
		}
		sort.Strings(want)
		if !slices.Equal(inputs[pkg], want) {
			t.Fatalf("%s pinned inputs=%v, source consumers require %v", pkg, inputs[pkg], want)
		}
	}
}

func TestProofHistoryFetchesExactObjectsInDepthOneCheckout(t *testing.T) {
	origin := t.TempDir()
	git := func(dir string, args ...string) string {
		t.Helper()
		command := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=Proof", "-c", "user.email=proof@example.invalid", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null"}, args...)...)
		body, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, body)
		}
		return strings.TrimSpace(string(body))
	}
	git(origin, "init", "-q")
	var pinned []string
	for _, value := range []string{"first", "second", "head"} {
		if err := os.WriteFile(filepath.Join(origin, "input"), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
		git(origin, "add", "input")
		git(origin, "commit", "-qm", value)
		pinned = append(pinned, git(origin, "rev-parse", "HEAD"))
	}
	clone := filepath.Join(t.TempDir(), "clone")
	git(origin, "clone", "--depth=1", "file://"+origin, clone)
	head := git(clone, "rev-parse", "HEAD")
	for _, commit := range pinned[:2] {
		if exec.Command("git", "-C", clone, "show", commit+":input").Run() == nil {
			t.Fatal("depth-one checkout unexpectedly has historical input")
		}
	}
	const pkg = "github.com/division-sh/swarm/internal/store"
	plan := filepath.Join(clone, "plan.json")
	inputs := filepath.Join(clone, "inputs.json")
	for path, value := range map[string]any{
		plan:   map[string]any{"units": []map[string]any{{"id": "relocated", "selected_roots": []map[string]string{{"package": pkg}}}}},
		inputs: map[string][]string{pkg: {pinned[0], pinned[1]}},
	} {
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	prepare := func() {
		t.Helper()
		command := exec.Command("bash", filepath.Join(testTimingRepoRoot(t), ".github/scripts/prepare-proof-history.sh"), plan, "relocated", inputs)
		command.Dir = clone
		if body, err := command.CombinedOutput(); err != nil {
			t.Fatalf("prepare exact historical inputs: %v %s", err, body)
		}
	}
	prepare()
	for i, commit := range pinned[:2] {
		if got := git(clone, "show", commit+":input"); got != []string{"first", "second"}[i] {
			t.Fatalf("historical bytes changed: %q", got)
		}
		git(clone, "archive", "--format=tar", commit)
	}
	if git(clone, "rev-parse", "HEAD") != head || git(clone, "rev-parse", "--is-shallow-repository") != "true" {
		t.Fatal("input preparation changed HEAD or fetched all ancestry")
	}
	git(clone, "remote", "set-url", "origin", "file:///nonexistent-proof-origin")
	prepare()
}

func TestProofHistoryRejectsInvalidOrMissingInputs(t *testing.T) {
	root := t.TempDir()
	plan := filepath.Join(root, "plan.json")
	inputs := filepath.Join(root, "inputs.json")
	if err := os.WriteFile(plan, []byte(`{"units":[{"id":"relocated"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{"{", `{}`, `{"foreign/package":["055bbbaba13e8d604ed80b73af98ca97c3b7acfb"]}`, `{"github.com/division-sh/swarm/internal/store":["HEAD"]}`, `{"github.com/division-sh/swarm/internal/store":[]}`} {
		if err := os.WriteFile(inputs, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		command := exec.Command("bash", filepath.Join(testTimingRepoRoot(t), ".github/scripts/prepare-proof-history.sh"), plan, "relocated", inputs)
		command.Dir = root
		if body, err := command.CombinedOutput(); err == nil {
			t.Fatalf("invalid input inventory accepted: %s: %s", data, body)
		}
	}
}
