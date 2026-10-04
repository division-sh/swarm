package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func TestCadenceUsesObservedMasterFirstParentNotReachableSideBranch(t *testing.T) {
	root := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", root, "-c", "user.name=Cadence", "-c", "user.email=cadence@example.invalid", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null"}, args...)...)
		raw, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, raw)
		}
		return strings.TrimSpace(string(raw))
	}
	git("init", "-q", "-b", "master")
	if err := os.WriteFile(filepath.Join(root, "root"), []byte("root"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "initial")
	base := git("rev-parse", "HEAD")
	git("checkout", "-qb", "feature")
	git("commit", "--allow-empty", "-qm", "feature")
	side := git("rev-parse", "HEAD")
	git("checkout", "master")
	git("commit", "--allow-empty", "-qm", "master")
	middle := git("rev-parse", "HEAD")
	git("merge", "--no-ff", "-qm", "merge feature", "feature")
	head := git("rev-parse", "HEAD")
	git("update-ref", "refs/remotes/origin/master", head)
	if got := cadenceMasterLineage(context.Background(), root, head); !reflect.DeepEqual(got, []string{head, middle, base}) {
		t.Fatal("wrong first-parent lag input", got)
	}
	if got := cadenceMasterLineage(context.Background(), root, side); len(got) != 0 {
		t.Fatal("reachable side branch was guessed to be detected master", got)
	}
	git("commit", "--allow-empty", "-qm", "future")
	if got := cadenceMasterLineage(context.Background(), root, git("rev-parse", "HEAD")); len(got) != 0 {
		t.Fatal("unobserved future master earned lineage", got)
	}
	t.Run("shallow_checkout_then_retrospective_history", func(t *testing.T) {
		clone := filepath.Join(t.TempDir(), "checkout")
		if raw, err := exec.Command("git", "clone", "--depth=1", "file://"+root, clone).CombinedOutput(); err != nil {
			t.Fatal(err, string(raw))
		}
		if got := cadenceMasterLineage(context.Background(), clone, git("rev-parse", "HEAD")); len(got) != 0 {
			t.Fatal("single-commit checkout earned complete lineage", got)
		}
		if raw, err := exec.Command("git", "-C", clone, "fetch", "--unshallow", "origin").CombinedOutput(); err != nil {
			t.Fatal(err, string(raw))
		}
		if got := cadenceMasterLineage(context.Background(), clone, head); !reflect.DeepEqual(got, []string{head, middle, base}) {
			t.Fatal("archived detection did not use observed complete master lineage", got)
		}
	})
	t.Run("core_alias_requires_merged_owner", func(t *testing.T) {
		dir := t.TempDir()
		policyPath, weights, packages := writePlannerFixtures(t, dir)
		policy, err := readProofPolicy(policyPath)
		if err != nil {
			t.Fatal(err)
		}
		model, err := readWeightModel(weights)
		if err != nil {
			t.Fatal(err)
		}
		packageList, err := readLines(packages)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := testplanning.BuildPlan(policy, model, packageList, testplanning.ProfileCore, "metadata-only control", head)
		if err != nil {
			t.Fatal(err)
		}
		planPath := filepath.Join(dir, "plan.json")
		if err := writeJSON(planPath, plan); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "gh"), []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
		for _, row := range []struct {
			name, runHead, landing string
			pass                   bool
		}{
			{"same_source_metadata_only", head, "", true},
			{"unverified_alias", side, "", false},
			{"malformed_landing", side, "not-a-sha", false},
			{"owner_unavailable", side, head, false},
		} {
			t.Run(row.name, func(t *testing.T) {
				runPath := filepath.Join(t.TempDir(), "run.json")
				raw, _ := json.Marshal(testplanning.QualifiedRun{ID: 10, RunAttempt: 1, HeadSHA: row.runHead, Status: "completed", Conclusion: "success"})
				if err := os.WriteFile(runPath, raw, 0600); err != nil {
					t.Fatal(err)
				}
				proof, err := readCadenceCore(config{priorCorePlanPath: planPath, priorCoreEvidenceRoot: t.TempDir(), priorCoreRunPath: runPath, priorCoreLandingSHA: row.landing})
				if (err == nil) != row.pass || (!row.pass && proof != nil) {
					t.Fatal("source alias bypassed exact merged-proof authority", proof, err)
				}
			})
		}
	})
}
