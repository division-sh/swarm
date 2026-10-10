package testplanning

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestCIUnitsCanonicalBodyAndRefusals(t *testing.T) {
	for _, tc := range []struct {
		body string
		want []string
		bad  bool
	}{
		{"CI-Tier: lifecycle", []string{}, false},
		{"CI-Units: z, a\r\n", []string{"a", "z"}, false},
		{"CI-Units: a", []string{"a"}, false},
		{"CI-Units: ", nil, true},
		{"CI-Units:", nil, true},
		{" CI-Units: a", nil, true},
		{"CI-Units=a", nil, true},
		{"CI-Units: a,b", nil, true},
		{"CI-Units: a,  b", nil, true},
		{"CI-Units: a, a", nil, true},
		{"CI-Units: a, ", nil, true},
		{"CI-Units: a\nCI-Units: b", nil, true},
		{"CI-Units: a ", nil, true},
		{"CI-Units: a\tb", nil, true},
	} {
		t.Run(tc.body, func(t *testing.T) {
			got, err := CIUnits(tc.body)
			if (err != nil) != tc.bad || (!tc.bad && !slices.Equal(got, tc.want)) {
				t.Fatalf("CIUnits = %v, %v; want %v, bad=%v", got, err, tc.want, tc.bad)
			}
		})
	}
}

func TestBuildPlanCIUnitsAllowlistAndOverlap(t *testing.T) {
	policy := testPolicy()
	full := policy.Profiles[ProfileFull]
	full.Units = append(slices.Clone(full.Units), "extra", "local-only")
	policy.Profiles[ProfileFull] = full
	policy.Units["extra"] = UnitPolicy{Packages: []string{"module/extra"}, Run: "^TestExtra$", CountMode: "count-1", EnvironmentID: "env", BudgetClass: "full"}
	policy.Units["local-only"] = UnitPolicy{Packages: []string{"module/extra"}, Run: "^TestLocal$", CountMode: "count-1", EnvironmentIDs: map[string]string{VenueLocal: "local-only"}, BudgetClass: "full"}
	policy.SpecialPackages = append(policy.SpecialPackages, "module/extra")
	model := WeightModel{Version: WeightModelVersion, SourceRunID: "fixture"}
	packages := []string{"module/catalog", "module/extra"}
	plan, err := BuildPlan(policy, model, packages, ProfileLifecycle, "fixture", "source", BuildOptions{ExtraUnits: []string{"extra"}})
	if err != nil {
		t.Fatal(err)
	}
	unit, err := plan.Unit("extra")
	if err != nil || unit.Run != "^TestExtra$" || unit.CountMode != "count-1" || unit.EnvironmentID != "env" || unit.WorkloadProfile != ProfileFull || !slices.Equal(plan.ExtraUnits, []string{"extra"}) {
		t.Fatalf("extra-unit contract changed: %+v %v", plan, err)
	}
	base, _ := plan.Unit("catalog-full")
	if plan.Profile != ProfileLifecycle || base.WorkloadProfile != ProfileLifecycle {
		t.Fatal("supplement promoted the entire base tier")
	}
	for _, tc := range []struct {
		name, tier, venue string
		ids               []string
	}{
		{"unknown", ProfileLifecycle, VenueCI, []string{"foreign"}},
		{"duplicate", ProfileLifecycle, VenueCI, []string{"extra", "extra"}},
		{"base-noop", ProfileLifecycle, VenueCI, []string{"catalog-full"}},
		{"local-only", ProfileLifecycle, VenueCI, []string{"local-only"}},
		{"local-selection", ProfileLifecycle, VenueLocal, []string{"extra"}},
		{"full-noop", ProfileFull, VenueCI, []string{"extra"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := BuildPlan(policy, model, packages, tc.tier, "fixture", "source", BuildOptions{Venue: tc.venue, ExtraUnits: tc.ids}); err == nil {
				t.Fatal("invalid selection admitted")
			}
		})
	}
	policy.Units["extra"] = policy.Units["catalog-full"]
	if _, err := BuildPlan(policy, model, packages, ProfileLifecycle, "fixture", "source", BuildOptions{ExtraUnits: []string{"extra"}}); err == nil {
		t.Fatal("overlapping unfiltered execution admitted")
	}
}

func TestCISelectionWireDigestAndCurrentBodyBinding(t *testing.T) {
	selection := CISelection{Version: 1, Tier: ProfileLifecycle, ExtraUnits: []string{"conformance-soak-postgres", "conformance-soak-sqlite"}}
	raw, _ := json.Marshal(selection)
	if string(raw) != `{"version":1,"tier":"lifecycle","extra_units":["conformance-soak-postgres","conformance-soak-sqlite"]}` {
		t.Fatalf("gate wire changed: %s", raw)
	}
	if selection.Digest() != "b87e30b2de619424f00cf27adb31f37cc7ced6e4da75274f485944b10b22e5a0" {
		t.Fatal("merge-gate digest vector changed")
	}
	plan := RunPlan{Profile: selection.Tier, ExtraUnits: selection.ExtraUnits}
	for _, body := range []string{
		"CI-Tier: lifecycle\nCI-Units: conformance-soak-sqlite, conformance-soak-postgres",
		"CI-Tier: lifecycle\nCI-Units: conformance-soak-postgres, conformance-soak-sqlite",
	} {
		if err := CheckCurrentCISelection(plan, body); err != nil {
			t.Fatal(err)
		}
	}
	for _, body := range []string{
		"CI-Tier: full", "CI-Tier: core", "CI-Tier: lifecycle", "CI-Tier: lifecycle\nCI-Units: conformance-soak-sqlite",
		"CI-Tier: lifecycle\nCI-Units: foreign", "CI-Tier: lifecycle\nCI-Units: conformance-soak-sqlite, conformance-soak-sqlite",
	} {
		if CheckCurrentCISelection(plan, body) == nil {
			t.Fatalf("body edit accepted old green: %q", body)
		}
	}
	if name := selection.CheckName(42, 2); name != "CI tier: lifecycle; selection: "+selection.Digest()+"; run: 42; attempt: 2" {
		t.Fatal(name)
	}
	if selection.CheckName(42, 1) == selection.CheckName(42, 2) || selection.CheckName(41, 2) == selection.CheckName(42, 2) {
		t.Fatal("check identity lost run/attempt")
	}
}

func TestCISelectionReportRequiresHostedBoundPlan(t *testing.T) {
	plan, err := BuildPlan(testPolicy(), WeightModel{Version: WeightModelVersion, SourceRunID: "fixture"}, []string{"module/catalog"}, ProfileCore, "fixture", "source")
	if err != nil {
		t.Fatal(err)
	}
	report, err := plan.CISelectionReport(42, 2)
	if err != nil || report.PlanDigest != plan.Digest || report.ExecutionSHA != plan.HeadSHA || report.WorkflowRunID != 42 || report.WorkflowAttempt != 2 || report.CheckName == "" || report.ExtraUnits == nil {
		t.Fatalf("report lost binding: %+v %v", report, err)
	}
	for _, change := range []string{"plan", "run", "attempt", "local", "missing-unit", "unsorted"} {
		t.Run(change, func(t *testing.T) {
			candidate, runID, attempt := plan, int64(42), 2
			switch change {
			case "plan":
				candidate.Digest = "wrong"
			case "run":
				runID = 0
			case "attempt":
				attempt = 0
			case "local":
				candidate.Venue = VenueLocal
				candidate.Digest, _ = planDigest(candidate)
			case "missing-unit":
				candidate.ExtraUnits = []string{"foreign"}
			case "unsorted":
				candidate.ExtraUnits = []string{"z", "a"}
			}
			if _, err := candidate.CISelectionReport(runID, attempt); err == nil {
				t.Fatal("invalid metadata admitted")
			}
		})
	}
	if !reflect.DeepEqual(report.CISelection, CISelection{Version: 1, Tier: ProfileCore, ExtraUnits: []string{}}) || strings.Contains(report.CheckName, "extra_units") {
		t.Fatal("no-extra canonical identity changed")
	}
}
