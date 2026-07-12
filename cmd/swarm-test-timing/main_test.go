package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testtiming"
)

func TestPlanCIEmitsDigestBoundPlanAndMinimalMatrix(t *testing.T) {
	dir := t.TempDir()
	policyPath, modelPath, packagesPath := writePlannerFixtures(t, dir)
	planPath := filepath.Join(dir, "plan.json")
	matrixPath := filepath.Join(dir, "matrix.json")
	markdownPath := filepath.Join(dir, "plan.md")
	if err := run(config{
		planCI:          true,
		proofPolicyPath: policyPath,
		weightModelPath: modelPath,
		packagesPath:    packagesPath,
		planPath:        planPath,
		matrixPath:      matrixPath,
		markdownPath:    markdownPath,
		event:           "pull_request",
		headSHA:         "abc",
	}); err != nil {
		t.Fatalf("run plan: %v", err)
	}
	plan, err := readPlan(planPath)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Profile != testplanning.ProfilePRCommon || plan.Digest == "" {
		t.Fatalf("plan = %+v", plan)
	}
	raw, err := os.ReadFile(matrixPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "packages") || !strings.Contains(string(raw), `"unit"`) {
		t.Fatalf("matrix contains another planning authority: %s", raw)
	}
}

func TestRecordEvidenceBindsPlanIdentity(t *testing.T) {
	dir := t.TempDir()
	policyPath, modelPath, packagesPath := writePlannerFixtures(t, dir)
	planPath := filepath.Join(dir, "plan.json")
	if err := run(config{planCI: true, proofPolicyPath: policyPath, weightModelPath: modelPath, packagesPath: packagesPath, planPath: planPath, matrixPath: filepath.Join(dir, "matrix.json"), markdownPath: filepath.Join(dir, "plan.md"), event: "push", headSHA: "abc"}); err != nil {
		t.Fatal(err)
	}
	plan, err := readPlan(planPath)
	if err != nil {
		t.Fatal(err)
	}
	unit := plan.Units[0]
	jsonPath := filepath.Join(dir, "go-test.json")
	var lines []string
	for _, pkg := range unit.Packages {
		lines = append(lines, `{"Action":"pass","Package":"`+pkg+`","Elapsed":1}`)
	}
	if err := os.WriteFile(jsonPath, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	evidencePath := filepath.Join(dir, "unit-primary-evidence.json")
	if err := run(config{recordEvidence: true, planPath: planPath, unitID: unit.ID, evidencePath: evidencePath, inputPath: jsonPath, attempt: testtiming.AttemptPrimary, elapsedSeconds: 1, exitCode: 0}); err != nil {
		t.Fatal(err)
	}
	evidence, err := readEvidence(evidencePath)
	if err != nil {
		t.Fatal(err)
	}
	if problems := testtiming.ValidateCommandEvidence(evidence, plan); len(problems) > 0 {
		t.Fatalf("evidence problems: %v", problems)
	}
}

func TestAssertExecutionSHARejectsDifferentCheckout(t *testing.T) {
	dir := t.TempDir()
	policyPath, modelPath, packagesPath := writePlannerFixtures(t, dir)
	planPath := filepath.Join(dir, "plan.json")
	if err := run(config{planCI: true, proofPolicyPath: policyPath, weightModelPath: modelPath, packagesPath: packagesPath, planPath: planPath, matrixPath: filepath.Join(dir, "matrix.json"), markdownPath: filepath.Join(dir, "plan.md"), event: "push", headSHA: "executed-sha"}); err != nil {
		t.Fatal(err)
	}
	if err := run(config{assertExecution: true, planPath: planPath, executionSHA: "executed-sha"}); err != nil {
		t.Fatalf("matching checkout: %v", err)
	}
	if err := run(config{assertExecution: true, planPath: planPath, executionSHA: "different-sha"}); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("wrong checkout error = %v", err)
	}
}

func TestUpdateWeightModelIsMaterialDiffOnly(t *testing.T) {
	dir := t.TempDir()
	policyPath, modelPath, packagesPath := writePlannerFixtures(t, dir)
	planPath := filepath.Join(dir, "plan.json")
	if err := run(config{planCI: true, proofPolicyPath: policyPath, weightModelPath: modelPath, packagesPath: packagesPath, planPath: planPath, matrixPath: filepath.Join(dir, "matrix.json"), markdownPath: filepath.Join(dir, "plan.md"), event: "push", headSHA: "abc"}); err != nil {
		t.Fatal(err)
	}
	plan, err := readPlan(planPath)
	if err != nil {
		t.Fatal(err)
	}
	evidenceRoot := filepath.Join(dir, "evidence")
	if err := os.MkdirAll(evidenceRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, unit := range plan.Units {
		report := testtiming.Report{}
		for _, pkg := range unit.Packages {
			report.Packages = append(report.Packages, testtiming.PackageTiming{Package: pkg, Result: "pass", Elapsed: 1})
			report.Summary.Events++
			report.Summary.Packages++
			report.Summary.PackageElapsedSec++
		}
		evidence := testtiming.CommandEvidence{Version: 2, PlanDigest: plan.Digest, Profile: plan.Profile, HeadSHA: plan.HeadSHA, UnitID: unit.ID, Surface: unit.ID, Attempt: testtiming.AttemptPrimary, Packages: unit.Packages, EnvironmentID: unit.EnvironmentID, CountMode: unit.CountMode, Report: report}
		if err := writeJSON(filepath.Join(evidenceRoot, unit.ID+"-primary-evidence.json"), evidence); err != nil {
			t.Fatal(err)
		}
	}
	if err := run(config{updateWeights: true, planPath: planPath, evidenceRoot: evidenceRoot, weightModelPath: modelPath, sourceRunID: "new-run"}); err != nil {
		t.Fatal(err)
	}
	model, err := readWeightModel(modelPath)
	if err != nil {
		t.Fatal(err)
	}
	if model.SourceRunID != "new-run" || len(model.Packages) != 2 {
		t.Fatalf("updated model = %+v", model)
	}
	before, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := run(config{updateWeights: true, planPath: planPath, evidenceRoot: evidenceRoot, weightModelPath: modelPath, sourceRunID: "another-run"}); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("idempotent refresh rewrote an unchanged model")
	}
}

func writePlannerFixtures(t *testing.T, dir string) (string, string, string) {
	t.Helper()
	policy := `
version: 1
module: module
planning: {target_seconds: 100, max_shards: 2, unknown_package_seconds: 10}
escalation_paths: ['^internal/runtime/conformance/']
special_packages: [module/catalog]
profiles:
  pr-common: {count_mode: cache-default, environment_id: env, units: [catalog-smoke]}
  pr-escalated: {count_mode: count-1, environment_id: env, units: [catalog-full]}
  full: {count_mode: count-1, environment_id: env, units: [catalog-full]}
  nightly: {count_mode: count-1, environment_id: env, units: [catalog-full]}
units:
  catalog-smoke: {packages: [module/catalog], run: '^TestSmoke$', count_mode: count-1, environment_id: env, budget_class: full}
  catalog-full: {packages: [module/catalog], count_mode: count-1, environment_id: env, budget_class: full}
projections: {required-full: {profile: full}}
`
	policyPath := filepath.Join(dir, "policy.yaml")
	if err := os.WriteFile(policyPath, []byte(policy), 0o644); err != nil {
		t.Fatal(err)
	}
	modelPath := filepath.Join(dir, "weights.json")
	model := testplanning.WeightModel{Version: 1, SourceRunID: "old-run", Packages: map[string]float64{"module/a": 20, "module/catalog": 20}}
	file, err := os.Create(modelPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := testplanning.WriteWeightModel(file, model); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	packagesPath := filepath.Join(dir, "packages.txt")
	if err := os.WriteFile(packagesPath, []byte("module/a\nmodule/catalog\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return policyPath, modelPath, packagesPath
}

func TestReadJSONRejectsUnknownPlanFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"unknown":true}`), 0o644); err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := readJSON(path, &value); err != nil {
		t.Fatalf("maps intentionally accept arbitrary keys: %v", err)
	}
	var plan testplanning.RunPlan
	file, _ := os.Open(path)
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err == nil {
		t.Fatal("typed plan accepted unknown field")
	}
}
