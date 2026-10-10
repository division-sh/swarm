package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func TestPlanCIUnitsBindMatrixReportAndCurrentBody(t *testing.T) {
	dir := t.TempDir()
	policyPath, modelPath, packagesPath := productionPlannerInputs(t, dir)
	body := "CI-Tier: lifecycle\nCI-Units: conformance-soak-sqlite, conformance-soak-postgres"
	eventPath := filepath.Join(dir, "event.json")
	raw, _ := json.Marshal(map[string]any{"pull_request": map[string]string{"body": body}})
	if err := os.WriteFile(eventPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	planPath, matrixPath := filepath.Join(dir, "plan.json"), filepath.Join(dir, "matrix.json")
	if err := run(config{planCI: true, proofPolicyPath: policyPath, weightModelPath: modelPath, packagesPath: packagesPath, eventPath: eventPath, planPath: planPath, matrixPath: matrixPath, markdownPath: filepath.Join(dir, "plan.md"), event: "pull_request", headSHA: "execution"}); err != nil {
		t.Fatal(err)
	}
	plan, err := readPlan(planPath)
	if err != nil || !slices.Equal(plan.ExtraUnits, []string{"conformance-soak-postgres", "conformance-soak-sqlite"}) {
		t.Fatalf("plan lost exact selection: %+v %v", plan, err)
	}
	var matrix struct {
		Include []struct {
			Unit           string `json:"unit"`
			BudgetClass    string `json:"budget_class"`
			TimeoutMinutes int    `json:"timeout_minutes"`
		} `json:"include"`
	}
	if err := readJSON(matrixPath, &matrix); err != nil {
		t.Fatal(err)
	}
	var soaks []string
	for _, entry := range matrix.Include {
		if entry.BudgetClass == "soak" {
			soaks = append(soaks, entry.Unit)
		}
	}
	slices.Sort(soaks)
	if !slices.Equal(soaks, plan.ExtraUnits) {
		t.Fatalf("matrix lost or duplicated soak execution: %v", soaks)
	}
	reportPath, markdown := filepath.Join(dir, "selection.json"), filepath.Join(dir, "selection.md")
	if err := run(config{ciSelection: true, planPath: planPath, workflowRunID: 42, workflowAttempt: 2, resultJSONPath: reportPath, markdownPath: markdown}); err != nil {
		t.Fatal(err)
	}
	var report testplanning.CISelectionReport
	if err := readJSON(reportPath, &report); err != nil {
		t.Fatal(err)
	}
	selection, _ := plan.CISelection()
	if report.CheckName != selection.CheckName(42, 2) || report.PlanDigest != plan.Digest || report.ExecutionSHA != plan.HeadSHA || report.SelectionDigest != selection.Digest() || !slices.Equal(report.ExtraUnits, plan.ExtraUnits) {
		t.Fatalf("check report lost exact plan binding: %+v", report)
	}
	md, _ := os.ReadFile(markdown)
	for _, value := range []string{plan.Digest, plan.HeadSHA, report.SelectionDigest, "conformance-soak-postgres, conformance-soak-sqlite", "Selection alone is not qualification"} {
		if !strings.Contains(string(md), value) {
			t.Fatalf("human summary omitted %q: %s", value, md)
		}
	}
	currentPRPath := filepath.Join(dir, "current.json")
	for _, current := range []struct {
		body, head string
		pass       bool
	}{
		{body, "branch", true},
		{"CI-Tier: lifecycle", "branch", false},
		{body, "other", false},
		{"CI-Tier: lifecycle\nCI-Units: conformance-soak-postgres", "branch", false},
		{"CI-Tier: full", "branch", false},
	} {
		raw, _ := json.Marshal(map[string]any{"body": current.body, "head": map[string]string{"sha": current.head}})
		if err := os.WriteFile(currentPRPath, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if err := run(config{checkCITier: true, planPath: planPath, currentPRPath: currentPRPath, workflowHeadSHA: "branch"}); (err == nil) != current.pass {
			t.Fatalf("current selection accepted=%v: %v", current.pass, err)
		}
	}
}

func TestCISelectionReportModeRefusesUnboundRun(t *testing.T) {
	dir := t.TempDir()
	policy, weights, packages := writePlannerFixtures(t, dir)
	plan := writeSyntheticPlan(t, dir, policy, weights, packages, "execution")
	for _, cfg := range []config{
		{ciSelection: true, planPath: plan, workflowAttempt: 1, resultJSONPath: filepath.Join(dir, "output")},
		{ciSelection: true, planPath: plan, workflowRunID: 42, resultJSONPath: filepath.Join(dir, "output")},
		{ciSelection: true, planPath: plan, workflowRunID: 42, workflowAttempt: 1},
		{ciSelection: true, planCI: true},
	} {
		if err := run(cfg); err == nil {
			t.Fatal("unbound report mode admitted")
		}
	}
}
