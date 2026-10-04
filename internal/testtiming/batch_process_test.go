package testtiming

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestProofBatchPreparesDockerFixtureOnlyForExactRequiredUnit(t *testing.T) {
	const pkg = "github.com/division-sh/swarm/internal/runtime/workspace"
	const name = "TestVerifyCLIImageProbeLifecycleRealDocker"
	for _, tc := range []struct {
		name, pkg, root string
		required, fail  bool
		wantPull        bool
	}{
		{"required", pkg, name, true, false, true},
		{"other package", "other/workspace", name, true, false, false},
		{"other root", pkg, name + "Other", true, false, false},
		{"not required", pkg, name, false, false, false},
		{"fixture failure", pkg, name, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.MkdirAll(filepath.Join(root, "test-results/plan"), 0700); err != nil {
				t.Fatal(err)
			}
			second := map[string]any{"id": "two"}
			field := "tests"
			if tc.required {
				field = "required_tests"
			}
			second[field] = []map[string]string{{"package": tc.pkg, "name": tc.root}}
			plan, err := json.Marshal(map[string]any{
				"batches": []map[string]any{{"id": "batch", "units": []string{"one", "two"}}},
				"units":   []map[string]any{{"id": "one"}, second},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "test-results/plan/proof-plan.json"), plan, 0600); err != nil {
				t.Fatal(err)
			}
			shims := map[string]string{
				"git": "#!/usr/bin/env bash\nprintf 'fixture-sha\\n'\n",
				"docker": `#!/usr/bin/env bash
printf 'docker %s\n' "$*" >> "$PROBE_LOG"
if [ "$FAIL_FIXTURE" = true ]; then exit 1; fi
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
			for name, body := range shims {
				if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0700); err != nil {
					t.Fatal(err)
				}
			}
			log := filepath.Join(root, "probe.log")
			fail := "false"
			if tc.fail {
				fail = "true"
			}
			command := exec.Command("bash", filepath.Join(testTimingRepoRoot(t), ".github/scripts/run-proof-batch.sh"))
			command.Dir = root
			command.Env = append(os.Environ(), "PATH="+root+string(os.PathListSeparator)+os.Getenv("PATH"), "BATCH_ID=batch", "GITHUB_RUN_ID=1", "GITHUB_RUN_ATTEMPT=1", "GITHUB_STEP_SUMMARY="+filepath.Join(root, "summary"), "PROBE_LOG="+log, "FAIL_FIXTURE="+fail)
			output, err := command.CombinedOutput()
			if (err != nil) != tc.fail {
				t.Fatalf("fixture failure = %t, want %t: %v %s", err != nil, tc.fail, err, output)
			}
			raw, err := os.ReadFile(log)
			if err != nil {
				t.Fatal(err)
			}
			want := "execute one\n"
			if tc.wantPull {
				want += "docker pull golang:1.25-bookworm\n"
			}
			if !tc.fail {
				want += "execute two\n"
			}
			if string(raw) != want {
				t.Fatalf("fixture preparation or unit isolation changed:\n%s\nwant:\n%s", raw, want)
			}
		})
	}
}

func TestProofBatchCollectsLaterUnitAfterFailureWithFreshProcessesAndTemps(t *testing.T) {
	for _, failure := range []string{"one", "two", "signal"} {
		t.Run(failure, func(t *testing.T) {
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
  mkdir "$TMPDIR/readonly"
  touch "$TMPDIR/readonly/downloaded-module"
  chmod a-w "$TMPDIR/readonly"
  printf '{}\n'
  if [ "$FAIL_UNIT" = signal ] && [ "$5" = one ]; then kill -TERM "$$"; fi
  if [ "$5" = "$FAIL_UNIT" ]; then exit 1; fi
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
			command.Env = append(os.Environ(), "PATH="+root+string(os.PathListSeparator)+os.Getenv("PATH"), "BATCH_ID=batch", "GITHUB_RUN_ID=1", "GITHUB_RUN_ATTEMPT=1", "GITHUB_STEP_SUMMARY="+filepath.Join(root, "summary"), "PROBE_LOG="+log, "FAIL_UNIT="+failure)
			if output, err := command.CombinedOutput(); err == nil {
				t.Fatalf("failed member %s was suppressed: %s", failure, output)
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
			for _, path := range []string{one[2], two[2]} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("owned read-only temp was not disposed: %s %v", path, err)
				}
			}
		})
	}
}
