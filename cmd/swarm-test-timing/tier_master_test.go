package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func TestCurrentPRTierRevalidationFailsClosed(t *testing.T) {
	dir := t.TempDir()
	policy, weights, packages := writePlannerFixtures(t, dir)
	planPath := writeSyntheticPlan(t, dir, policy, weights, packages, "execution")
	plan, err := readPlan(planPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, body, head         string
		malformed, missing, pass bool
	}{
		{"full", "CI-Tier: full", "branch", false, false, true},
		{"lower", "CI-Tier: core", "branch", false, false, true},
		{"missing_tier_full", "", "branch", false, false, true},
		{"wrong_head", "CI-Tier: full", "other", false, false, false},
		{"absent_head", "CI-Tier: full", "", false, false, false},
		{"malformed_api", "", "branch", true, false, false},
		{"missing_api", "", "branch", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "current.json")
			data, _ := json.Marshal(map[string]any{"body": tc.body, "head": map[string]string{"sha": tc.head}})
			if tc.malformed {
				data = []byte("{")
			}
			if !tc.missing {
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			err := run(config{checkCITier: true, planPath: planPath, currentPRPath: path, workflowHeadSHA: "branch"})
			if (err == nil) != tc.pass {
				t.Fatalf("revalidation %v, profile %s", err, plan.Profile)
			}
		})
	}
	// A thinner old green is not promoted by a later same-head body edit.
	if err := testplanning.CheckCurrentCITier(testplanning.ProfileCore, "CI-Tier: lifecycle"); err == nil {
		t.Fatal("old thin green accepted")
	}
}

func TestMergedProofAPIFailureFallsBackWithoutReplacingPlan(t *testing.T) {
	dir := t.TempDir()
	gh := filepath.Join(dir, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	sha := strings.Repeat("a", 40)
	eventPath, planPath := filepath.Join(dir, "event.json"), filepath.Join(dir, "plan.json")
	event, _ := json.Marshal(map[string]any{"ref": "refs/heads/master", "before": strings.Repeat("b", 40), "after": sha, "forced": false, "repository": map[string]string{"full_name": "division-sh/swarm"}})
	if err := os.WriteFile(eventPath, event, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(planPath, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	resultPath := filepath.Join(dir, "result.json")
	if err := verifyMergedProof(config{repository: "division-sh/swarm", branchRef: "refs/heads/master", headSHA: sha, eventPath: eventPath, planPath: planPath, matrixPath: filepath.Join(dir, "matrix.json"), resultJSONPath: resultPath}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Verified bool
		Reason   string
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if result.Verified || result.Reason == "" {
		t.Fatalf("uncertain observation %s", raw)
	}
	if raw, _ := os.ReadFile(planPath); string(raw) != "untouched" {
		t.Fatal("failed observation replaced plan")
	}
}

func TestMergedObservationRefusesSupersedingRun(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(`#!/bin/sh
printf '%s\n' '{"workflow_runs":[{"id":124,"run_attempt":1,"status":"queued"},{"id":123,"run_attempt":2,"status":"completed","conclusion":"success"}]}'
`), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	var pr testplanning.MergedPR
	pr.Head.SHA = "branch"
	err := revalidateMergedObservation(context.Background(), config{repository: "division-sh/swarm"}, pr, testplanning.QualifiedRun{ID: 123, RunAttempt: 2}, testplanning.RunPlan{})
	if err == nil || !strings.Contains(err.Error(), "changed during observation") {
		t.Fatalf("superseding run: %v", err)
	}
}
