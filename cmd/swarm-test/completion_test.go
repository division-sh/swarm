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
