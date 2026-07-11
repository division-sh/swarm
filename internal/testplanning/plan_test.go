package testplanning

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestBuildPlanDiscoversUnknownPackagesAndBalancesDeterministically(t *testing.T) {
	policy := testPolicy()
	model := WeightModel{Version: 1, SourceRunID: "run", Packages: map[string]float64{"module/a": 200, "module/b": 100}}
	packages := []string{"module/catalog", "module/new", "module/b", "module/a"}
	first, err := BuildPlan(policy, model, packages, ProfilePREscalated, "changed path", "abc")
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	second, err := BuildPlan(policy, model, packages, ProfilePREscalated, "changed path", "abc")
	if err != nil {
		t.Fatalf("BuildPlan second: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("plans are not deterministic:\n%+v\n%+v", first, second)
	}
	if len(first.Units) != 3 {
		t.Fatalf("unit count = %d, want two broad shards plus catalog", len(first.Units))
	}
	owners := map[string]string{}
	for _, unit := range first.Units {
		for _, pkg := range unit.Packages {
			if owners[pkg] != "" {
				t.Fatalf("package %s owned twice", pkg)
			}
			owners[pkg] = unit.ID
		}
	}
	for _, pkg := range packages {
		if owners[pkg] == "" {
			t.Fatalf("package %s was suppressed", pkg)
		}
	}
	if first.Units[len(first.Units)-1].ID != "catalog-full" {
		t.Fatalf("full delta = %s, want catalog-full", first.Units[len(first.Units)-1].ID)
	}
}

func TestResolveProfileCoversEveryEventAndEscalationFamily(t *testing.T) {
	policy := testPolicy()
	tests := []struct {
		event   string
		changed []string
		want    string
	}{
		{event: "pull_request", want: ProfilePRCommon},
		{event: "pull_request", changed: []string{"internal/runtime/conformance/a.go"}, want: ProfilePREscalated},
		{event: "push", want: ProfileFull},
		{event: "workflow_dispatch", want: ProfileFull},
		{event: "schedule", want: ProfileNightly},
	}
	for _, tt := range tests {
		got, _, err := policy.ResolveProfile(tt.event, tt.changed, "")
		if err != nil {
			t.Fatalf("ResolveProfile(%s): %v", tt.event, err)
		}
		if got != tt.want {
			t.Fatalf("ResolveProfile(%s, %v) = %s, want %s", tt.event, tt.changed, got, tt.want)
		}
	}
}

func TestRunPlanRejectsWrongDigestAndDuplicatePackage(t *testing.T) {
	plan, err := BuildPlan(testPolicy(), WeightModel{Version: 1, SourceRunID: "run", Packages: map[string]float64{}}, []string{"module/a", "module/catalog"}, ProfileFull, "full", "abc")
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	badDigest := plan
	badDigest.HeadSHA = "wrong"
	if err := badDigest.Validate(); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("wrong digest error = %v", err)
	}
	duplicate := plan
	duplicate.Digest = ""
	duplicate.Units[1].Packages = []string{"module/a"}
	digest, err := planDigest(duplicate)
	if err != nil {
		t.Fatal(err)
	}
	duplicate.Digest = digest
	if err := duplicate.Validate(); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("duplicate error = %v", err)
	}
}

func TestLoadersFailClosedOnUnknownFields(t *testing.T) {
	policyYAML := `
version: 1
module: module
planning: {target_seconds: 10, max_shards: 2, unknown_package_seconds: 3, max_imbalance: 0.2}
escalation_paths: []
special_packages: []
profiles:
  pr-common: {count_mode: cache-default, environment_id: env, units: []}
  pr-escalated: {count_mode: count-1, environment_id: env, units: []}
  full: {count_mode: count-1, environment_id: env, units: []}
  nightly: {count_mode: count-1, environment_id: env, units: []}
units: {}
projections: {}
unknown: true
`
	if _, err := LoadPolicy(strings.NewReader(policyYAML)); err == nil || !strings.Contains(err.Error(), "field unknown") {
		t.Fatalf("unknown policy field error = %v", err)
	}
	if _, err := LoadWeightModel(strings.NewReader(`{"version":1,"source_run_id":"run","packages":{},"unknown":true}`)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown model field error = %v", err)
	}
}

func TestMatrixContainsOnlyPlanUnitIDs(t *testing.T) {
	plan, err := BuildPlan(testPolicy(), WeightModel{Version: 1, SourceRunID: "run", Packages: map[string]float64{}}, []string{"module/a", "module/catalog"}, ProfilePRCommon, "common", "abc")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := MatrixJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	var matrix struct {
		Include []map[string]string `json:"include"`
	}
	if err := json.NewDecoder(bytes.NewReader(raw)).Decode(&matrix); err != nil {
		t.Fatal(err)
	}
	if len(matrix.Include) != len(plan.Units) {
		t.Fatalf("matrix rows = %d, want %d", len(matrix.Include), len(plan.Units))
	}
	for _, row := range matrix.Include {
		if len(row) != 1 || row["unit"] == "" {
			t.Fatalf("matrix leaks a second authority: %v", row)
		}
	}
}

func TestValidatePublicationDiffFailsClosed(t *testing.T) {
	if err := ValidatePublicationDiff([]string{GeneratedWeightModelPath}); err != nil {
		t.Fatalf("canonical diff: %v", err)
	}
	for _, paths := range [][]string{
		nil,
		{"README.md"},
		{GeneratedWeightModelPath, "internal/testplanning/plan.go"},
	} {
		if err := ValidatePublicationDiff(paths); err == nil {
			t.Fatalf("ValidatePublicationDiff(%v) succeeded", paths)
		}
	}
}

func testPolicy() Policy {
	return Policy{
		Version: 1,
		Module:  "module",
		Planning: PlanningPolicy{
			TargetSeconds:         200,
			MaxShards:             4,
			UnknownPackageSeconds: 30,
			MaxImbalance:          0.25,
		},
		EscalationPaths: []string{`^internal/runtime/conformance/`},
		SpecialPackages: []string{"module/catalog"},
		Profiles: map[string]ProfilePolicy{
			ProfilePRCommon:    {CountMode: "cache-default", EnvironmentID: "env", Units: []string{"catalog-smoke"}},
			ProfilePREscalated: {CountMode: "count-1", EnvironmentID: "env", Units: []string{"catalog-full"}},
			ProfileFull:        {CountMode: "count-1", EnvironmentID: "env", Units: []string{"catalog-full"}},
			ProfileNightly:     {CountMode: "count-1", EnvironmentID: "env", Units: []string{"catalog-full"}},
		},
		Units: map[string]UnitPolicy{
			"catalog-smoke": {Packages: []string{"module/catalog"}, Run: "^TestSmoke$", CountMode: "count-1", EnvironmentID: "env", BudgetClass: "full"},
			"catalog-full":  {Packages: []string{"module/catalog"}, CountMode: "count-1", EnvironmentID: "env", BudgetClass: "full"},
		},
		Projections: map[string]ProjectionPolicy{"required-full": {Profile: ProfileFull}},
	}
}
