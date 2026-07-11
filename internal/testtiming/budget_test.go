package testtiming

import (
	"bytes"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func TestLoadBudgetPolicyValidatesHardOwner(t *testing.T) {
	valid := `
version: 1
hard:
  max_shard_command_seconds: {limit_seconds: 270, justification: measured broad command}
  full_conformance_command_seconds: {limit_seconds: 330, justification: measured catalog command}
`
	if _, err := LoadBudgetPolicy(strings.NewReader(valid)); err != nil {
		t.Fatalf("LoadBudgetPolicy: %v", err)
	}
	if _, err := LoadBudgetPolicy(strings.NewReader(valid + "unknown: true\n")); err == nil || !strings.Contains(err.Error(), "field unknown") {
		t.Fatalf("unknown field error = %v", err)
	}
}

func TestConfirmationRequiredUsesExactPlannedUnit(t *testing.T) {
	plan := timingTestPlan(t)
	policy := timingTestPolicy()
	evidence := timingTestEvidence(plan, "broad-01", AttemptPrimary, 271)
	required, err := ConfirmationRequired(policy, plan, evidence)
	if err != nil || !required {
		t.Fatalf("ConfirmationRequired = %v, %v; want true", required, err)
	}
	evidence.PlanDigest = "wrong"
	if _, err := ConfirmationRequired(policy, plan, evidence); err == nil || !strings.Contains(err.Error(), "plan_digest") {
		t.Fatalf("wrong-plan error = %v", err)
	}
}

func TestEvaluateBudgetRequiresEveryPlanUnitExactlyOnce(t *testing.T) {
	plan := timingTestPlan(t)
	policy := timingTestPolicy()
	complete := make([]CommandEvidence, 0, len(plan.Units))
	for _, unit := range plan.Units {
		complete = append(complete, timingTestEvidence(plan, unit.ID, AttemptPrimary, 10))
	}
	opts := EvaluationOptions{Plan: plan, HistoricalWeights: map[string]float64{"module/a": 1, "module/catalog": 1}}
	if got := EvaluateBudget(policy, opts, complete).Status; got != BudgetPass {
		t.Fatalf("complete status = %s, want PASS", got)
	}
	tests := []struct {
		name string
		edit func([]CommandEvidence) []CommandEvidence
	}{
		{name: "missing", edit: func(values []CommandEvidence) []CommandEvidence { return values[:1] }},
		{name: "duplicate", edit: func(values []CommandEvidence) []CommandEvidence { return append(values, values[0]) }},
		{name: "undeclared", edit: func(values []CommandEvidence) []CommandEvidence { values[0].UnitID = "other"; return values }},
		{name: "wrong profile", edit: func(values []CommandEvidence) []CommandEvidence { values[0].Profile = "other"; return values }},
		{name: "wrong environment", edit: func(values []CommandEvidence) []CommandEvidence { values[0].EnvironmentID = "other"; return values }},
		{name: "wrong packages", edit: func(values []CommandEvidence) []CommandEvidence {
			values[0].Packages = []string{"other"}
			return values
		}},
		{name: "wrong head", edit: func(values []CommandEvidence) []CommandEvidence { values[0].HeadSHA = "other"; return values }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := append([]CommandEvidence(nil), complete...)
			if got := EvaluateBudget(policy, opts, tt.edit(values)).Status; got != BudgetIncomplete {
				t.Fatalf("status = %s, want INCOMPLETE", got)
			}
		})
	}
}

func TestEvaluateBudgetConfirmsOnlyResponsibleUnit(t *testing.T) {
	plan := timingTestPlan(t)
	policy := timingTestPolicy()
	values := make([]CommandEvidence, 0, len(plan.Units)+1)
	for _, unit := range plan.Units {
		elapsed := 10.0
		if unit.ID == "broad-01" {
			elapsed = 271
		}
		values = append(values, timingTestEvidence(plan, unit.ID, AttemptPrimary, elapsed))
	}
	values = append(values, timingTestEvidence(plan, "broad-01", AttemptConfirmation, 269))
	result := EvaluateBudget(policy, EvaluationOptions{Plan: plan}, values)
	if result.Status != BudgetWarn {
		t.Fatalf("status = %s, want WARN: %+v", result.Status, result)
	}
	for _, surface := range result.Surfaces {
		if surface.Surface != "broad-01" && surface.ConfirmationSeconds != nil {
			t.Fatalf("unrelated unit %s was confirmed", surface.Surface)
		}
	}
}

func TestPackageDiagnosticsConsumeGeneratedWeights(t *testing.T) {
	plan := timingTestPlan(t)
	values := make([]CommandEvidence, 0, len(plan.Units))
	for _, unit := range plan.Units {
		values = append(values, timingTestEvidence(plan, unit.ID, AttemptPrimary, 10))
	}
	result := EvaluateBudget(timingTestPolicy(), EvaluationOptions{
		Plan: plan,
		HistoricalWeights: map[string]float64{
			"module/a":       1,
			"module/catalog": 1,
			"module/stale":   2,
		},
	}, values)
	if len(result.PackageDiagnostics) != 1 || result.PackageDiagnostics[0].Kind != "stale" {
		t.Fatalf("diagnostics = %+v, want one stale generated weight", result.PackageDiagnostics)
	}
}

func TestBudgetMarkdownDoesNotAskImplementersToRebalance(t *testing.T) {
	result := BudgetResult{Version: BudgetResultVersion, Status: BudgetIncomplete}
	var out bytes.Buffer
	if err := WriteBudgetMarkdown(&out, result); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "generate-shards") || strings.Contains(out.String(), "shards 6") {
		t.Fatalf("legacy manual remediation survives:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "planner-owned") {
		t.Fatalf("planner remediation missing:\n%s", out.String())
	}
}

func timingTestPlan(t *testing.T) testplanning.RunPlan {
	t.Helper()
	policy := testplanning.Policy{
		Version: 1,
		Module:  "module",
		Planning: testplanning.PlanningPolicy{
			TargetSeconds:         100,
			MaxShards:             2,
			UnknownPackageSeconds: 10,
			MaxImbalance:          0.25,
		},
		SpecialPackages: []string{"module/catalog"},
		Profiles: map[string]testplanning.ProfilePolicy{
			testplanning.ProfilePRCommon:    {CountMode: CountModeCacheDefault, EnvironmentID: "env", Units: []string{"catalog"}},
			testplanning.ProfilePREscalated: {CountMode: CountModeOne, EnvironmentID: "env", Units: []string{"catalog"}},
			testplanning.ProfileFull:        {CountMode: CountModeOne, EnvironmentID: "env", Units: []string{"catalog"}},
			testplanning.ProfileNightly:     {CountMode: CountModeOne, EnvironmentID: "env", Units: []string{"catalog"}},
		},
		Units: map[string]testplanning.UnitPolicy{
			"catalog": {Packages: []string{"module/catalog"}, CountMode: CountModeOne, EnvironmentID: "env", BudgetClass: "full"},
		},
		Projections: map[string]testplanning.ProjectionPolicy{},
	}
	plan, err := testplanning.BuildPlan(policy, testplanning.WeightModel{Version: 1, SourceRunID: "run", Packages: map[string]float64{}}, []string{"module/a", "module/catalog"}, testplanning.ProfilePRCommon, "test", "head")
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	return plan
}

func timingTestPolicy() BudgetPolicy {
	return BudgetPolicy{
		Version: 1,
		Hard: HardBudgets{
			MaxShardCommandSeconds:        CommandBudget{LimitSeconds: 270, Justification: "test"},
			FullConformanceCommandSeconds: CommandBudget{LimitSeconds: 330, Justification: "test"},
		},
	}
}

func timingTestEvidence(plan testplanning.RunPlan, unitID, attempt string, elapsed float64) CommandEvidence {
	unit, err := plan.Unit(unitID)
	if err != nil {
		panic(err)
	}
	countMode := unit.CountMode
	if attempt == AttemptConfirmation {
		countMode = CountModeOne
	}
	report := Report{}
	for _, pkg := range unit.Packages {
		report.Packages = append(report.Packages, PackageTiming{Package: pkg, Result: "pass", Elapsed: 1})
		report.Summary.Events++
		report.Summary.Packages++
		report.Summary.PackageElapsedSec++
	}
	return CommandEvidence{
		Version:        CommandEvidenceVersion,
		PlanDigest:     plan.Digest,
		Profile:        plan.Profile,
		HeadSHA:        plan.HeadSHA,
		UnitID:         unit.ID,
		Surface:        unit.ID,
		Attempt:        attempt,
		ElapsedSeconds: elapsed,
		Packages:       append([]string(nil), unit.Packages...),
		EnvironmentID:  unit.EnvironmentID,
		CountMode:      countMode,
		Report:         report,
	}
}
