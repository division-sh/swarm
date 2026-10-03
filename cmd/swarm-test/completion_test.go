package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testtiming"
)

func completionGit(t *testing.T, repo string, args ...string) string {
	t.Helper()
	command := append([]string{"-C", repo, "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", "-c", "user.name=Qualification Test", "-c", "user.email=qualification@example.invalid"}, args...)
	out, err := exec.Command("git", command...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func completionRepo(t *testing.T) (string, string) {
	t.Helper()
	repo := t.TempDir()
	completionGit(t, repo, "init", "--quiet")
	for path, contents := range map[string]string{
		"source_test.go": "package proof\n",
		".gitignore":     "test-results/\n",
	} {
		if err := os.WriteFile(filepath.Join(repo, path), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	completionGit(t, repo, "add", ".")
	completionGit(t, repo, "commit", "--quiet", "-m", "test: initial source")
	return repo, completionGit(t, repo, "rev-parse", "HEAD")
}

func TestExplicitCompletionSourceRejectsDirtyTrackedAndUntracked(t *testing.T) {
	for _, path := range []string{"source_test.go", "untracked-fixture.yaml"} {
		for _, staged := range []bool{false, true} {
			t.Run(path+"/staged="+map[bool]string{false: "false", true: "true"}[staged], func(t *testing.T) {
				repo, head := completionRepo(t)
				if got, err := completionSource(repo, head, true); err != nil || got != head {
					t.Fatalf("clean source: %s %v", got, err)
				}
				if err := os.WriteFile(filepath.Join(repo, path), []byte("changed fixture\n"), 0600); err != nil {
					t.Fatal(err)
				}
				if staged {
					completionGit(t, repo, "add", path)
				}
				for _, expected := range []string{"", head} {
					if got, err := completionSource(repo, expected, true); err == nil || got != head || !strings.Contains(err.Error(), path) {
						t.Fatalf("dirty admission/completion: %s %v", got, err)
					}
				}
				if got, err := completionSource(repo, "", false); err != nil || got != head {
					t.Fatalf("developer feedback refused: %s %v", got, err)
				}
				t.Chdir(repo)
				if code := runCompletion(testplanning.ProfileFull, true); code != 1 {
					t.Fatalf("dirty explicit full proceeded to planning: %d", code)
				}
				if _, err := os.Stat("test-results"); !os.IsNotExist(err) {
					t.Fatalf("dirty admission produced receipts: %v", err)
				}
			})
		}
	}
}

func TestExplicitCompletionSourceAllowsIgnoredReceiptsButRejectsChangedHead(t *testing.T) {
	repo, head := completionRepo(t)
	if err := os.Mkdir(filepath.Join(repo, "test-results"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "test-results", "receipt.json"), []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := completionSource(repo, head, true); err != nil || got != head {
		t.Fatalf("ignored output refused: %s %v", got, err)
	}
	completionGit(t, repo, "commit", "--allow-empty", "--quiet", "-m", "test: changed commit")
	actual := completionGit(t, repo, "rev-parse", "HEAD")
	if got, err := completionSource(repo, head, true); err == nil || got != actual || !strings.Contains(err.Error(), "HEAD changed") {
		t.Fatalf("changed commit admitted: %s %v", got, err)
	}
	if _, err := completionSource(t.TempDir(), "", true); err == nil {
		t.Fatal("non-repository source admitted")
	}
}

func TestExplicitCompletionPostExecutionDirtInvalidatesReceipt(t *testing.T) {
	repo, head := completionRepo(t)
	t.Chdir(repo)
	bin := t.TempDir()
	script := "#!/bin/sh\nset -eu\nprintf 'changed during command\\n' > source_test.go\nprintf '%s\\n' '{\"Action\":\"pass\",\"Package\":\"proof\",\"Elapsed\":0.001}'\n"
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("SWARM_TEST_RUN_SLOTS", "1")
	t.Setenv("SWARM_TEST_POSTGRES_DSN", "postgres://swarm:secret@127.0.0.1:1/postgres?sslmode=disable")
	receipts := t.TempDir()
	plan := testplanning.RunPlan{HeadSHA: head, Profile: testplanning.ProfileFull}
	unit := testplanning.ProofUnit{ID: "source-negative", Packages: []string{"./proof"}, GoTimeout: "10s"}
	if code := executeCompletionUnit(plan, unit, true, receipts); code != 1 {
		t.Fatalf("dirty child success earned credit: %d", code)
	}
	raw, err := os.ReadFile(filepath.Join(receipts, unit.ID+"-primary-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var evidence testtiming.CommandEvidence
	if err := json.Unmarshal(raw, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.HeadSHA != head || evidence.ExitCode != 1 {
		t.Fatalf("misattributed successful receipt: %+v", evidence)
	}
}

func TestPlannedQualificationWorkerRefusesStartDirt(t *testing.T) {
	repo, head := completionRepo(t)
	policy := testplanning.Policy{
		Version: testplanning.PolicyVersion, Module: "proof",
		Planning: testplanning.PlanningPolicy{TargetSeconds: 100, MaxShards: 1, UnknownPackageSeconds: 10},
		Profiles: map[string]testplanning.ProfilePolicy{}, Units: map[string]testplanning.UnitPolicy{},
	}
	for _, tier := range []string{testplanning.ProfileCore, testplanning.ProfileLifecycle, testplanning.ProfileFull} {
		policy.Profiles[tier] = testplanning.ProfilePolicy{CountMode: testtiming.CountModeOne, EnvironmentID: "fixture"}
	}
	plan, err := testplanning.BuildPlan(policy, testplanning.WeightModel{Version: testplanning.WeightModelVersion, SourceRunID: "fixture", Packages: map[string]float64{}}, []string{"proof"}, testplanning.ProfileCore, "explicit local tier selection; reviewer compares with Local-Tier", head, testplanning.BuildOptions{Venue: testplanning.VenueLocal})
	if err != nil {
		t.Fatal(err)
	}
	planFile := filepath.Join(t.TempDir(), "plan.json")
	raw, err := json.Marshal(plan)
	if err != nil || os.WriteFile(planFile, raw, 0600) != nil {
		t.Fatal("write worker plan", err)
	}
	if err := os.WriteFile(filepath.Join(repo, "untracked-embed.yaml"), []byte("dirty\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(repo)
	receipts := filepath.Join(repo, "test-results")
	stderr, err := os.Create(filepath.Join(t.TempDir(), "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	previous := os.Stderr
	os.Stderr = stderr
	code := runPlanned([]string{planFile, plan.Units[0].ID, "--receipts", receipts})
	os.Stderr = previous
	if err := stderr.Close(); err != nil {
		t.Fatal(err)
	}
	message, err := os.ReadFile(stderr.Name())
	if code != 1 || err != nil || !strings.Contains(string(message), "reviewer-bound qualification requires clean source") {
		t.Fatalf("dirty worker start proceeded: %d", code)
	}
	if _, err := os.Stat(receipts); !os.IsNotExist(err) {
		t.Fatal("refused worker published proof", err)
	}
}
