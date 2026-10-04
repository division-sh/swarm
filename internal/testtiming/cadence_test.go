package testtiming

import (
	"bytes"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func cadenceFixturePlan(t *testing.T, profile, head string) testplanning.RunPlan {
	t.Helper()
	const release = "github.com/division-sh/swarm/internal/releasee2e"
	policy := testplanning.Policy{Version: testplanning.PolicyVersion, Module: "module", Planning: testplanning.PlanningPolicy{TargetSeconds: 100, MaxShards: 1, UnknownPackageSeconds: 10}, SpecialPackages: []string{"module/extra", release, testplanning.SoakPackage}, Profiles: map[string]testplanning.ProfilePolicy{}, Units: map[string]testplanning.UnitPolicy{
		"extra":   {Packages: []string{"module/extra"}, CountMode: CountModeOne, EnvironmentID: "env", BudgetClass: "full"},
		"release": {Packages: []string{release}, CountMode: CountModeOne, EnvironmentID: "env", BudgetClass: "full"},
	}}
	for _, tier := range []string{testplanning.ProfileCore, testplanning.ProfileLifecycle, testplanning.ProfileFull} {
		policy.Profiles[tier] = testplanning.ProfilePolicy{CountMode: CountModeOne, EnvironmentID: "env"}
	}
	fullUnits := []string{"extra", "release"}
	for _, backend := range []string{"sqlite", "postgres"} {
		name := "soak-" + backend
		policy.Units[name] = testplanning.UnitPolicy{Packages: []string{testplanning.SoakPackage}, CountMode: CountModeOne, EnvironmentID: "env", BudgetClass: "soak", Run: testplanning.SoakRun + "/^" + backend + "$", GoTimeout: testplanning.SoakGoTimeout}
		fullUnits = append(fullUnits, name)
	}
	policy.Profiles[testplanning.ProfileFull] = testplanning.ProfilePolicy{CountMode: CountModeOne, EnvironmentID: "env", Units: fullUnits}
	inventory := testplanning.RootInventory{BuildContext: testplanning.BuildContext{GOOS: "linux", GOARCH: "amd64", CGOEnabled: "1"}, Packages: map[string]testplanning.PackageRoots{"module/proof": {Package: "module/proof", HasTestFiles: true, Roots: []string{"TestCore"}}, "module/extra": {Package: "module/extra", HasTestFiles: true, Roots: []string{"TestFull"}}}}
	inventory.Packages[release] = testplanning.PackageRoots{Package: release, HasTestFiles: true, Roots: []string{"TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends", "TestCompiledProcessFullLifecycleJourneysSQLitePostgres", "TestGoldenAgentWorkloadBurstConcurrencyOnBothBackendsIteration1", "TestGoldenAgentWorkloadBurstConcurrencyOnBothBackendsIteration2"}}
	inventory.Packages[testplanning.SoakPackage] = testplanning.PackageRoots{Package: testplanning.SoakPackage, HasTestFiles: true, Roots: []string{testplanning.SoakTest}}
	plan, err := testplanning.BuildPlan(policy, testplanning.WeightModel{Version: testplanning.WeightModelVersion, SourceRunID: "observed", Packages: map[string]float64{}}, []string{"module/proof", "module/extra", release, testplanning.SoakPackage}, profile, "cadence control", head)
	if err == nil {
		err = testplanning.BindExecution(&plan, inventory, nil, policy)
	}
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func cadenceFixtureAttempt(t *testing.T, profile, head string, run int64, failed bool) CadenceAttempt {
	t.Helper()
	plan := cadenceFixturePlan(t, profile, head)
	result := CadenceAttempt{Plan: plan, RunID: run, Attempt: 1}
	for _, unit := range plan.Units {
		evidence := timingTestEvidence(plan, unit.ID, AttemptPrimary, 1)
		evidence.WorkflowRunID = run
		evidence.BuildContext = plan.BuildContext
		evidence.WorkloadProfile, evidence.ExecutionTier = unit.WorkloadProfile, unit.ExecutionTier
		for _, root := range unit.SelectedRoots {
			status := "pass"
			if failed && root.Name == "TestFull" {
				status = "fail"
				evidence.ExitCode = 1
				evidence.Report.Summary.FailedTests++
				evidence.Report.Summary.FailedPackages++
				evidence.Report.Packages[0].Result = "fail"
			}
			evidence.Report.Tests = append(evidence.Report.Tests, TestTiming{Package: root.Package, Test: root.Name, Result: status, Elapsed: 1})
			evidence.Report.Summary.Tests++
			evidence.Report.Summary.Events++
		}
		if backend, ok := testplanning.SoakBackend(unit.Run); ok {
			evidence.Report.Tests = append(evidence.Report.Tests, TestTiming{Package: testplanning.SoakPackage, Test: testplanning.SoakTest + "/" + backend, Result: "pass", Elapsed: 950})
			evidence.Report.Summary.Tests++
			evidence.Report.Summary.Events++
		}
		result.Evidence = append(result.Evidence, evidence)
	}
	return result
}

func TestFullCadenceEvidenceAndHonestUnknowns(t *testing.T) {
	fullHead, coreHead, intro := strings.Repeat("c", 40), strings.Repeat("b", 40), strings.Repeat("a", 40)
	full := cadenceFixtureAttempt(t, testplanning.ProfileFull, fullHead, 20, true)
	core := CadenceCoreProof{CadenceAttempt: cadenceFixtureAttempt(t, testplanning.ProfileCore, coreHead, 10, false), SuccessfulRun: true}
	landed := CadenceCoreProof{CadenceAttempt: cadenceFixtureAttempt(t, testplanning.ProfileCore, strings.Repeat("d", 40), 10, false), SuccessfulRun: true, LandedHeadSHA: coreHead}
	lineage := []string{fullHead, coreHead, intro}
	review := CadenceAttribution{Package: "module/extra", Root: "TestFull", Kind: "regression", Review: "https://github.com/division-sh/swarm/issues/2353#issuecomment-1", IntroducingCommit: intro, FirstDetectionRunID: 20}
	for _, row := range []struct {
		name      string
		core      *CadenceCoreProof
		review    []CadenceAttribution
		lineage   []string
		confirmed bool
	}{
		{"unattributed", nil, nil, lineage, false},
		{"confirmed", &core, []CadenceAttribution{review}, lineage, true},
		{"owner_verified_core_landing", &landed, []CadenceAttribution{review}, lineage, true},
		{"no_core", nil, []CadenceAttribution{review}, lineage, false},
		{"no_master_lineage", &core, []CadenceAttribution{review}, nil, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			got, err := ObserveFullCadence(full, row.core, row.review, row.lineage)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Findings) != 1 || got.Findings[0].ConfirmedCoreEscape != row.confirmed {
				t.Fatalf("wrong credit: %+v", got)
			}
			if row.confirmed {
				if got.ConfirmedCoreEscapes != 1 || got.ReviewedRegressions != 1 || got.EscapeRateStatus != "unmeasured" || got.Findings[0].FirstParentLag == nil || *got.Findings[0].FirstParentLag != 2 {
					t.Fatal(got)
				}
			} else if got.EscapeRateStatus != "unmeasured" || got.Findings[0].FirstParentLag != nil {
				t.Fatal("unknown became zero/known credit")
			}
		})
	}
	green := cadenceFixtureAttempt(t, testplanning.ProfileFull, fullHead, 20, false)
	got, err := ObserveFullCadence(green, nil, nil, lineage)
	var markdown bytes.Buffer
	if err != nil || len(got.Findings) != 0 || got.EscapeRateStatus != "unmeasured" {
		t.Fatal(got, err)
	}
	if err := WriteCadenceMarkdown(&markdown, got); err != nil || !strings.Contains(markdown.String(), "**N/A**") {
		t.Fatal("empty week was reassuring zero", err)
	}
	for _, kind := range []string{"infra", "harness", "budget", "non_regression"} {
		classified := review
		classified.Kind = kind
		got, err := ObserveFullCadence(full, nil, []CadenceAttribution{classified}, lineage)
		if err != nil || got.EscapeRateStatus != "unmeasured" || got.Findings[0].Classification != kind {
			t.Fatal(got, err)
		}
	}
	for _, kind := range []string{"foreign", "duplicate", "unknown_kind"} {
		classified := review
		items := []CadenceAttribution{classified}
		switch kind {
		case "foreign":
			items[0].Package = "foreign"
		case "duplicate":
			items = append(items, classified)
		case "unknown_kind":
			items[0].Kind = "guessed_escape"
		}
		if _, err := ObserveFullCadence(full, &core, items, lineage); err == nil {
			t.Fatalf("%s attribution accepted", kind)
		}
	}
	for _, kind := range []string{"missing", "foreign_attempt", "malformed"} {
		broken := cadenceFixtureAttempt(t, testplanning.ProfileFull, fullHead, 20, true)
		switch kind {
		case "missing":
			broken.Evidence = nil
		case "foreign_attempt":
			for i := range broken.Evidence {
				broken.Evidence[i].WorkflowAttempt = 2
			}
		case "malformed":
			for i := range broken.Evidence {
				broken.Evidence[i].Report.Summary.MalformedLines = 1
			}
		}
		got, err := ObserveFullCadence(broken, nil, nil, lineage)
		if err != nil || len(got.Problems) == 0 || got.EscapeRateStatus != "unmeasured" {
			t.Fatalf("%s incomplete evidence lost: %+v %v", kind, got, err)
		}
	}
	for _, kind := range []string{"future_introducer", "foreign_introducer", "wrong_detection", "failed_core", "missing_core"} {
		classified := review
		prior := core
		switch kind {
		case "future_introducer":
			classified.IntroducingCommit = fullHead
		case "foreign_introducer":
			classified.IntroducingCommit = strings.Repeat("d", 40)
		case "wrong_detection":
			classified.FirstDetectionRunID = 19
		case "failed_core":
			prior.SuccessfulRun = false
		case "missing_core":
			prior.Evidence = nil
		}
		got, err := ObserveFullCadence(full, &prior, []CadenceAttribution{classified}, lineage)
		if err != nil || got.EscapeRateStatus != "unmeasured" || len(got.Problems) == 0 {
			t.Fatalf("%s claimed known escape: %+v %v", kind, got, err)
		}
	}
}

func TestFullCadenceDoesNotCreditCoreSelectedFailure(t *testing.T) {
	fullHead, coreHead, intro := strings.Repeat("c", 40), strings.Repeat("b", 40), strings.Repeat("a", 40)
	full := cadenceFixtureAttempt(t, testplanning.ProfileFull, fullHead, 20, false)
	core := CadenceCoreProof{CadenceAttempt: cadenceFixtureAttempt(t, testplanning.ProfileCore, coreHead, 10, false), SuccessfulRun: true}
	failed := false
	for i := range full.Evidence {
		evidence := &full.Evidence[i]
		for j := range evidence.Report.Tests {
			result := &evidence.Report.Tests[j]
			if result.Package != "module/proof" || result.Test != "TestCore" {
				continue
			}
			result.Result = "fail"
			evidence.ExitCode = 1
			evidence.Report.Summary.FailedTests++
			evidence.Report.Summary.FailedPackages++
			for k := range evidence.Report.Packages {
				if evidence.Report.Packages[k].Package == result.Package {
					evidence.Report.Packages[k].Result = "fail"
				}
			}
			failed = true
		}
	}
	if !failed {
		t.Fatal("full fixture omitted the core-selected failure")
	}
	review := CadenceAttribution{Package: "module/proof", Root: "TestCore", Kind: "regression", Review: "https://github.com/division-sh/swarm/issues/2353#issuecomment-1", IntroducingCommit: intro, FirstDetectionRunID: 20}
	got, err := ObserveFullCadence(full, &core, []CadenceAttribution{review}, []string{fullHead, coreHead, intro})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Findings) != 1 || got.Findings[0].ConfirmedCoreEscape || got.Findings[0].FirstParentLag != nil || got.EscapeRateStatus != "unmeasured" || got.ReviewedRegressions != 1 || got.ConfirmedCoreEscapes != 0 {
		t.Fatalf("core-selected failure earned escape credit: %+v", got)
	}
	if !strings.Contains(strings.Join(got.Problems, "\n"), "failure is not a full-only root") {
		t.Fatalf("missing exact core-membership refusal: %+v", got)
	}
	t.Run("mixed_reviewed_findings_do_not_invent_rate", func(t *testing.T) {
		for i := range full.Evidence {
			evidence := &full.Evidence[i]
			for j := range evidence.Report.Tests {
				result := &evidence.Report.Tests[j]
				if result.Package == "module/extra" && result.Test == "TestFull" {
					result.Result = "fail"
					evidence.ExitCode = 1
					evidence.Report.Summary.FailedTests++
					evidence.Report.Summary.FailedPackages++
					evidence.Report.Packages[0].Result = "fail"
				}
			}
		}
		escape := review
		escape.Package, escape.Root = "module/extra", "TestFull"
		mixed, err := ObserveFullCadence(full, &core, []CadenceAttribution{review, escape}, []string{fullHead, coreHead, intro})
		if err != nil || mixed.ReviewedRegressions != 2 || mixed.ConfirmedCoreEscapes != 1 || mixed.EscapeRateStatus != "unmeasured" {
			t.Fatal("counts became a tautological rate", mixed, err)
		}
		var markdown bytes.Buffer
		if err := WriteCadenceMarkdown(&markdown, mixed); err != nil || !strings.Contains(markdown.String(), "**N/A**") || strings.Contains(markdown.String(), "100.0%") {
			t.Fatal("mixed outcomes earned an unsupported rate", markdown.String(), err)
		}
	})
}
