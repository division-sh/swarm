package testtiming

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestProofBatchCollectsLaterUnitAfterFailureWithFreshProcessesAndTemps(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "test-results/plan"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "test-results/plan/proof-plan.json"), []byte(`{"batches":[{"id":"batch","units":["one","two"]}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	goShim := `#!/usr/bin/env bash
set -euo pipefail
if [ "$2" = ./cmd/swarm-test ]; then
  printf '%s %s %s\n' "$5" "$$" "$TMPDIR" >> "$PROBE_LOG"
  printf '{}\n'
  if [ "$5" = one ]; then exit 1; fi
fi
if [ "$2" = ./cmd/swarm-test-timing ]; then
  previous=''
  for arg in "$@"; do
    if [ "$previous" = -evidence ] || [ "$previous" = -markdown ]; then printf '{}\n' > "$arg"; fi
    previous="$arg"
  done
fi
`
	if err := os.WriteFile(filepath.Join(root, "go"), []byte(goShim), 0700); err != nil {
		t.Fatal(err)
	}
	git := exec.Command("git", "init")
	git.Dir = root
	if log, err := git.CombinedOutput(); err != nil {
		t.Fatalf("git init %v %s", err, log)
	}
	git = exec.Command("git", "-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "-m", "fixture")
	git.Dir = root
	if log, err := git.CombinedOutput(); err != nil {
		t.Fatalf("git commit %v %s", err, log)
	}
	command := exec.Command("bash", filepath.Join(testTimingRepoRoot(t), ".github/scripts/run-proof-batch.sh"))
	command.Dir = root
	log := filepath.Join(root, "probe.log")
	command.Env = append(os.Environ(), "PATH="+root+string(os.PathListSeparator)+os.Getenv("PATH"), "BATCH_ID=batch", "GITHUB_RUN_ID=1", "GITHUB_RUN_ATTEMPT=1", "GITHUB_STEP_SUMMARY="+filepath.Join(root, "summary"), "PROBE_LOG="+log)
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("failed first member was suppressed: %s", output)
	}
	raw, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("later member not executed: %s", raw)
	}
	one, two := strings.Fields(lines[0]), strings.Fields(lines[1])
	if len(one) != 3 || len(two) != 3 || one[0] != "one" || two[0] != "two" || one[1] == two[1] || one[2] == two[2] {
		t.Fatalf("unit process/temp isolation lost: %s", raw)
	}
	for _, id := range []string{"one", "two"} {
		if _, err := os.Stat(filepath.Join(root, "test-results/evidence", id+"-primary-evidence.json")); err != nil {
			t.Fatal(err)
		}
	}
}
