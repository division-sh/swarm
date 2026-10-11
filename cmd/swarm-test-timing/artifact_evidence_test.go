package main

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testtiming"
	"gopkg.in/yaml.v3"
)

type artifactWorkflowStep struct {
	Name, Uses, Run string
	With            map[string]any
}

func artifactDownloadStep(t *testing.T, job, pattern string) artifactWorkflowStep {
	t.Helper()
	for _, step := range artifactWorkflowSteps(t, job) {
		if strings.Contains(step.Uses, "download-artifact") && strings.HasPrefix(fmt.Sprint(step.With["pattern"]), pattern) {
			return step
		}
	}
	t.Fatalf("missing %s artifact consumer", job)
	return artifactWorkflowStep{}
}

func artifactWorkflowSteps(t *testing.T, job string) []artifactWorkflowStep {
	t.Helper()
	raw, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	var w struct {
		Jobs map[string]struct{ Steps []artifactWorkflowStep }
	}
	if err := yaml.Unmarshal(raw, &w); err != nil {
		t.Fatal(err)
	}
	return w.Jobs[job].Steps
}

// Execute the parsed action's namespace and directory projection against a
// retained catalogue. Actual hosted acquisition is a separate required receipt.
func acquireAttemptProofFixture(t *testing.T, step artifactWorkflowStep, catalogue map[string]map[string][]byte, target string) {
	t.Helper()
	if err := os.MkdirAll(target, 0700); err != nil {
		t.Fatal(err)
	}
	pattern := strings.NewReplacer("${{ github.run_id }}", "42", "${{ github.run_attempt }}", "2").Replace(fmt.Sprint(step.With["pattern"]))
	for name, files := range catalogue {
		matched, err := filepath.Match(pattern, name)
		if err != nil {
			t.Fatal(err)
		}
		if !matched {
			continue
		}
		dir := target
		if step.With["merge-multiple"] != true {
			dir = filepath.Join(dir, name)
		}
		for name, data := range files {
			path := filepath.Join(dir, name)
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func currentAttemptEvidenceFixture(t *testing.T) (config, testtiming.CommandEvidence, []byte) {
	t.Helper()
	cfg := ratchetCLIConfig(t, "full", 0, false)
	cfg.event, cfg.workflowAttempt = "pull_request", 2
	path := filepath.Join(cfg.evidenceRoot, "command-primary-evidence.json")
	evidence, err := readEvidence(path)
	if err != nil {
		t.Fatal(err)
	}
	evidence.WorkflowAttempt, evidence.ElapsedSeconds = 2, 106
	evidence.Report.Tests[0].Elapsed = 106
	evidence.Report.Packages[0].Elapsed = 106
	evidence.Report.Summary.PackageElapsedSec = 106
	if err := writeJSON(path, evidence); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var jobs []struct {
		Jobs []testtiming.ActionJob `json:"jobs"`
	}
	if err := readJSON(cfg.jobsPath, &jobs); err != nil {
		t.Fatal(err)
	}
	job := &jobs[0].Jobs[0]
	job.RunAttempt = 2
	job.CompletedAt = job.StartedAt.Add(140 * time.Second)
	job.Steps[0].CompletedAt = job.StartedAt.Add(130 * time.Second)
	job.Steps[1].StartedAt = job.Steps[0].CompletedAt
	job.Steps[1].CompletedAt = job.CompletedAt.Add(-time.Second)
	if err := writeJSON(cfg.jobsPath, jobs); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(cfg.evidenceRoot); err != nil {
		t.Fatal(err)
	}
	return cfg, evidence, raw
}

func TestAttemptQualifiedProofDownloadsPreserveNativeRefusals(t *testing.T) {
	step := artifactDownloadStep(t, "timing-budget", "proof-")
	for _, cut := range []string{"current", "missing_current", "duplicate_current", "foreign_run", "foreign_attempt", "foreign_head", "failed_current", "partial_current"} {
		t.Run(cut, func(t *testing.T) {
			cfg, evidence, raw := currentAttemptEvidenceFixture(t)
			old := evidence
			old.WorkflowAttempt, old.ElapsedSeconds = 1, 139
			oldPath := filepath.Join(t.TempDir(), "old.json")
			if err := writeJSON(oldPath, old); err != nil {
				t.Fatal(err)
			}
			oldRaw, err := os.ReadFile(oldPath)
			if err != nil {
				t.Fatal(err)
			}
			catalogue := map[string]map[string][]byte{
				"proof-42-1-timer-timing": {"command-primary-evidence.json": oldRaw},
				"proof-42-1-store-timing": {"primary.json": []byte(`{"partial":true}`)},
			}
			switch cut {
			case "foreign_run":
				evidence.WorkflowRunID++
			case "foreign_attempt":
				evidence.WorkflowAttempt--
			case "foreign_head":
				evidence.HeadSHA = "foreign"
			case "failed_current":
				evidence.ExitCode = 1
			}
			changed := filepath.Join(t.TempDir(), "changed.json")
			if err := writeJSON(changed, evidence); err != nil {
				t.Fatal(err)
			}
			changedRaw, err := os.ReadFile(changed)
			if err != nil {
				t.Fatal(err)
			}
			if cut != "missing_current" {
				catalogue["proof-42-2-timer-timing"] = map[string][]byte{"command-primary-evidence.json": changedRaw}
			}
			if cut == "duplicate_current" {
				catalogue["proof-42-2-store-timing"] = map[string][]byte{"command-primary-evidence.json": raw}
			}
			if cut == "partial_current" {
				catalogue["proof-42-2-timer-timing"] = map[string][]byte{"primary.json": []byte(`{"partial":true}`)}
			}
			acquireAttemptProofFixture(t, step, catalogue, cfg.evidenceRoot)
			err = run(cfg)
			if cut == "current" {
				if err != nil {
					t.Fatalf("A01/A02/A23 complete current proof rejected: %v", err)
				}
				items, problems := readEvidenceTree(cfg.evidenceRoot)
				if len(problems) != 0 || len(items) != 1 || items[0].WorkflowAttempt != 2 || items[0].ElapsedSeconds != 106 {
					t.Fatalf("retained139s/partial store replaced current106s: %+v %v", items, problems)
				}
			} else if err == nil || readRatchetCLIResult(t, cfg).ExitCode() == 0 {
				t.Fatalf("A06/A07/A08/A10/A24 %s falsely qualified", cut)
			}
		})
	}
}

func TestPublisherAttemptEvidenceRefusalDoesNotMutateModel(t *testing.T) {
	step := artifactDownloadStep(t, "publish-timing-model", "proof-")
	for _, cut := range []string{"missing_current", "duplicate_current", "foreign_attempt", "failed_current"} {
		t.Run(cut, func(t *testing.T) {
			cfg, receipt, raw := currentAttemptEvidenceFixture(t)
			before, err := os.ReadFile(cfg.weightModelPath)
			if err != nil {
				t.Fatal(err)
			}
			catalogue := map[string]map[string][]byte{"proof-42-1-old-timing": {"command-primary-evidence.json": raw}}
			if cut == "foreign_attempt" {
				receipt.WorkflowAttempt = 1
			}
			if cut == "failed_current" {
				receipt.ExitCode = 1
			}
			path := filepath.Join(t.TempDir(), "receipt.json")
			if err := writeJSON(path, receipt); err != nil {
				t.Fatal(err)
			}
			changed, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if cut != "missing_current" {
				catalogue["proof-42-2-current-timing"] = map[string][]byte{"command-primary-evidence.json": changed}
			}
			if cut == "duplicate_current" {
				catalogue["proof-42-2-duplicate-timing"] = map[string][]byte{"command-primary-evidence.json": raw}
			}
			acquireAttemptProofFixture(t, step, catalogue, cfg.evidenceRoot)
			cfg.updateWeights, cfg.evaluateBudget, cfg.sourceRunID = true, false, "github-actions-42-2"
			refusal := run(cfg)
			if refusal == nil {
				t.Fatal("A14/A15 invalid attempt published a model")
			}
			after, err := os.ReadFile(cfg.weightModelPath)
			if err != nil || string(before) != string(after) {
				t.Fatalf("rejected publisher changed bytes: %v", err)
			}
			runRejectedPublisherScript(t, cfg, refusal.Error())
		})
	}
}

func runRejectedPublisherScript(t *testing.T, cfg config, refusal string) {
	t.Helper()
	dir := t.TempDir()
	for from, to := range map[string]string{cfg.planPath: "test-results/plan/proof-plan.json", cfg.weightModelPath: ".github/test-timing-weights.json"} {
		raw, err := os.ReadFile(from)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, to)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := filepath.WalkDir(cfg.evidenceRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(cfg.evidenceRoot, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dir, "test-results/evidence", relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0600)
	}); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	for name, script := range map[string]string{
		"go":  "#!/bin/sh\nexec \"$PUBLISHER_NATIVE_BINARY\" -test.run='^TestPublisherNativeAttemptProcess$' -- \"$@\"\n",
		"git": "#!/bin/sh\ntest \"$*\" = 'rev-parse HEAD' || exit 1\nprintf '%s\\n' execution-head\n",
		"gh":  "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$PUBLISHER_CALLS\"\nexit 1\n",
	} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var script string
	for _, step := range artifactWorkflowSteps(t, "publish-timing-model") {
		if step.Name == "Generate and publish model-only PR" {
			script = step.Run
		}
	}
	if script == "" {
		t.Fatal("missing protected publisher script")
	}
	model := filepath.Join(dir, ".github/test-timing-weights.json")
	before, err := os.ReadFile(model)
	if err != nil {
		t.Fatal(err)
	}
	calls := filepath.Join(dir, "calls")
	cmd := exec.Command("bash", "-c", script)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "PUBLISHER_NATIVE_PROCESS=1", "PUBLISHER_NATIVE_BINARY="+executable, "PUBLISHER_CALLS="+calls, "GITHUB_RUN_ID=42", "GITHUB_RUN_ATTEMPT=2", "SOURCE_RUN_ID=github-actions-42-2")
	if out, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(out), refusal) {
		t.Fatalf("A15 refusal did not reach the real native writer: %v %s", err, out)
	}
	after, err := os.ReadFile(model)
	if err != nil || string(before) != string(after) {
		t.Fatalf("publisher refusal mutated model: %v", err)
	}
	if raw, err := os.ReadFile(calls); !os.IsNotExist(err) || len(raw) != 0 {
		t.Fatalf("publisher refusal reached GitHub: %s %v", raw, err)
	}
}

func TestPublisherNativeAttemptProcess(t *testing.T) {
	if os.Getenv("PUBLISHER_NATIVE_PROCESS") != "1" {
		return
	}
	index := slices.Index(os.Args, "--")
	if index < 0 || len(os.Args) < index+4 {
		os.Exit(1)
	}
	args := os.Args[index+1:]
	if args[0] != "run" || args[1] != "./cmd/swarm-test-timing" {
		os.Exit(1)
	}
	var cfg config
	fs := flag.NewFlagSet("publisher native command", flag.ContinueOnError)
	fs.BoolVar(&cfg.assertExecution, "assert-execution-sha", false, "")
	fs.BoolVar(&cfg.updateWeights, "update-weight-model", false, "")
	fs.StringVar(&cfg.planPath, "plan", "", "")
	fs.StringVar(&cfg.executionSHA, "execution-sha", "", "")
	fs.StringVar(&cfg.evidenceRoot, "evidence-root", "", "")
	fs.StringVar(&cfg.weightModelPath, "weight-model", "", "")
	fs.StringVar(&cfg.sourceRunID, "source-run-id", "", "")
	fs.Int64Var(&cfg.workflowRunID, "workflow-run-id", 0, "")
	fs.IntVar(&cfg.workflowAttempt, "workflow-attempt", 0, "")
	if err := fs.Parse(args[2:]); err != nil {
		os.Exit(1)
	}
	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
