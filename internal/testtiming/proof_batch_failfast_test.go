package testtiming

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestProofBatchFailFastRetainsFailedReceiptAndJoinsOwnedCleanup(t *testing.T) {
	script := filepath.Join(testTimingRepoRoot(t), ".github/scripts/run-proof-batch.sh")
	for _, keepGoing := range []string{"false", "true"} {
		t.Run(keepGoing, func(t *testing.T) {
			root := t.TempDir()
			plan := filepath.Join(root, "test-results/plan")
			bin := filepath.Join(root, "bin")
			scratch := filepath.Join(root, "scratch")
			for _, path := range []string{plan, bin, scratch} {
				if err := os.MkdirAll(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			data := `{"batches":[{"id":"batch","units":["first","second"]}],"units":[{"id":"first","budget_class":"broad"},{"id":"second","budget_class":"broad"}]}`
			if err := os.WriteFile(filepath.Join(plan, "proof-plan.json"), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			// This shell counterexample tests scheduling/receipt order, not Go proof credit.
			goStub := `#!/bin/bash
printf '%s\n' "$*" >> "$CALLS"
case "$*" in
  *'--planned'*'first') touch "$TMPDIR/owned"; exit 1 ;;
  *'--planned'*'second') touch "$TMPDIR/owned"; exit 0 ;;
esac
exit 0
`
			if err := os.WriteFile(filepath.Join(bin, "go"), []byte(goStub), 0700); err != nil {
				t.Fatal(err)
			}
			calls := filepath.Join(root, "calls")
			command := exec.Command("bash", script)
			command.Dir = root
			command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "BATCH_ID=batch", "KEEP_GOING="+keepGoing, "CALLS="+calls, "TMPDIR="+scratch, "GITHUB_STEP_SUMMARY="+filepath.Join(root, "summary"), "GITHUB_RUN_ID=1", "GITHUB_RUN_ATTEMPT=1")
			out, err := command.CombinedOutput()
			if err == nil {
				t.Fatalf("failed primary must refuse qualification: %s", out)
			}
			log, err := os.ReadFile(calls)
			if err != nil {
				t.Fatal(err)
			}
			text := string(log)
			if !strings.Contains(text, "-unit first -attempt primary") || !strings.Contains(text, "-exit-code 1") {
				t.Fatalf("failed receipt was not attempted before exit: %s", text)
			}
			if strings.Contains(text, "--planned test-results/plan/proof-plan.json second") != (keepGoing == "true") {
				t.Fatalf("wrong sibling execution policy: %s", text)
			}
			remaining, err := os.ReadDir(scratch)
			if err != nil || len(remaining) != 0 {
				t.Fatalf("owned cleanup not joined: %v %v", remaining, err)
			}
		})
	}
}
