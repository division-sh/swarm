package testtiming

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

type proofHistoryCase struct {
	name, pkg, shallow, want                        string
	selected, fetchFailure, inspectFailure, failure bool
}

func TestProofBatchProvidesHistoricalCorpusInputsBeforeExecution(t *testing.T) {
	const corpus = "github.com/division-sh/swarm/scripts/rewrite-stages-2566"
	for _, tc := range []proofHistoryCase{
		{name: "selected renamed unit", pkg: corpus, selected: true, shallow: "true", want: "execute ordinary\nfetch --no-tags --unshallow origin fixture-sha\nexecute relocated\n"},
		{name: "already complete", pkg: corpus, selected: true, shallow: "false", want: "execute ordinary\nexecute relocated\n"},
		{name: "foreign package", pkg: "other/scripts/rewrite-stages-2566", selected: true, shallow: "true", want: "execute ordinary\nexecute relocated\n"},
		{name: "unselected corpus", pkg: corpus, shallow: "true", want: "execute ordinary\nexecute relocated\n"},
		{name: "fetch refusal", pkg: corpus, selected: true, shallow: "true", fetchFailure: true, failure: true, want: "execute ordinary\nfetch --no-tags --unshallow origin fixture-sha\n"},
		{name: "inspection refusal", pkg: corpus, selected: true, inspectFailure: true, failure: true, want: "execute ordinary\n"},
		{name: "invalid history status", pkg: corpus, selected: true, shallow: "unknown", failure: true, want: "execute ordinary\n"},
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
	command.Env = append(os.Environ(), "PATH="+root+string(os.PathListSeparator)+os.Getenv("PATH"), "BATCH_ID=arbitrary-batch", "GITHUB_RUN_ID=1", "GITHUB_RUN_ATTEMPT=1", "GITHUB_STEP_SUMMARY="+filepath.Join(root, "summary"), "PROBE_LOG="+log, "SHALLOW="+tc.shallow, "FETCH_FAILURE="+proofHistoryBool(tc.fetchFailure), "INSPECT_FAILURE="+proofHistoryBool(tc.inspectFailure))
	output, err := command.CombinedOutput()
	if (err != nil) != tc.failure {
		t.Fatalf("history admission failed=%t want=%t: %v %s", err != nil, tc.failure, err, output)
	}
	raw, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != tc.want {
		t.Fatalf("history/proof ordering = %q, want %q", raw, tc.want)
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
  test "$INSPECT_FAILURE" != true
  printf '%s\n' "$SHALLOW"
elif [ "$*" = 'rev-parse HEAD' ]; then
  printf 'fixture-sha\n'
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
