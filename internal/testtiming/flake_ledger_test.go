package testtiming

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func TestFailureLedgerFrozen2353SourceIndex(t *testing.T) {
	file, err := os.Open("../../.github/flake-ledger.json")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	ledger, err := ReadFailureLedger(file)
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.Historical) != 61 || len(ledger.Sources) != 331 || len(ledger.Records) != 6 {
		t.Fatalf("frozen import/source index changed without reviewed reconciliation: historical=%d sources=%d curated=%d", len(ledger.Historical), len(ledger.Sources), len(ledger.Records))
	}
	for i, record := range ledger.Historical {
		if record.ID != fmt.Sprintf("LSF-%03d", i+1) || !slices.Contains(record.Sources, "2353-body") {
			t.Fatalf("lost existing source identity: %+v", record)
		}
	}
	for _, record := range ledger.Records {
		if record.ID == "CI-2353-fixed-plan-pg" || record.ID == "CI-2353-fixed-plan-sqlite" {
			if record.Status != "open" || record.Attribution != "unknown" || record.Rate != nil {
				t.Fatalf("green resampling relabeled fixed-plan evidence: %+v", record)
			}
		}
	}
}

func failureLedgerFixture() (FailureLedger, ObservedFailure) {
	url := "https://github.com/division-sh/swarm/issues/2353#issuecomment-1"
	failure := ObservedFailure{RunID: 1, Attempt: 1, JobID: 11, HeadSHA: strings.Repeat("a", 40), Source: strings.Repeat("b", 40), Package: "proof", Test: "TestGuard/postgres", Job: "Go proof owner", Detail: "precise assertion", GOOS: "linux"}
	ledger := FailureLedger{Version: 1, Sources: []FailureSource{{ID: "source", URL: url, SHA256: strings.Repeat("c", 64)}}, Records: []FailureRecord{{
		ID: "known", Package: failure.Package, Test: failure.Test, Job: failure.Job, Signature: "precise assertion", GOOS: "linux",
		Phenomenon: "unknown-variance", Attribution: "unknown", Status: "open", Repair: "unclassified", FirstSeen: "2026-10-12", LastSeen: "2026-10-12", Tracking: url, Review: url, Sources: []string{"source"},
		Observations: []FailureObservation{{RunID: 1, Attempt: 1, JobID: 11, Source: failure.Source, Attribution: "non-PR-proven", Review: url}},
	}}}
	return ledger, failure
}

func TestFailureLedgerRefusesAmbiguousAndUnreviewedEvidence(t *testing.T) {
	for _, name := range []string{"duplicate-source", "missing-source", "unknown-kind", "unreviewed", "invalid-rate", "flaky-exclusion", "duplicate-record", "ambiguous-match", "false-review-url"} {
		t.Run(name, func(t *testing.T) {
			ledger, failure := failureLedgerFixture()
			switch name {
			case "duplicate-source":
				ledger.Sources = append(ledger.Sources, ledger.Sources[0])
			case "missing-source":
				ledger.Records[0].Sources = []string{"absent"}
			case "unknown-kind":
				ledger.Records[0].Phenomenon = "probably-load"
			case "unreviewed":
				ledger.Records[0].Review = ""
			case "false-review-url":
				ledger.Records[0].Review = "https://github.com/division-sh/swarm/issues/not-a-review"
			case "invalid-rate":
				ledger.Records[0].Rate = &FailureRate{Failures: 1, Attempts: 0}
			case "flaky-exclusion":
				ledger.Records[0].ExcludePreflight = true
			case "duplicate-record":
				ledger.Records = append(ledger.Records, ledger.Records[0])
			case "ambiguous-match":
				other := ledger.Records[0]
				other.ID = "other"
				ledger.Records = append(ledger.Records, other)
			}
			_, err := ledger.Match(failure)
			if err == nil {
				t.Fatalf("accepted %s evidence", name)
			}
		})
	}
	if _, err := ReadFailureLedger(strings.NewReader(`{"version":1,"unknown":true}`)); err == nil {
		t.Fatal("accepted unknown ledger field")
	}
	t.Run("finite-whole-root-exclusion", func(t *testing.T) {
		ledger, _ := failureLedgerFixture()
		ledger.Records[0].Phenomenon, ledger.Records[0].ExcludePreflight = "flake", true
		plan := testplanning.PreflightPlan{Version: 1, HeadSHA: strings.Repeat("a", 40), BaseSHA: strings.Repeat("b", 40), BuildContext: testplanning.BuildContext{GOOS: "linux", GOARCH: "amd64", CGOEnabled: "1", GOVERSION: "go1.26.8"}, Packages: []testplanning.PreflightPackage{{Package: "proof", Run: "^(TestGuard)$", Required: []testplanning.RequiredTest{{TestRoot: testplanning.TestRoot{Package: "proof", Name: "TestGuard"}}}}}}
		bind := func() {
			plan.Digest = ""
			raw, err := json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(raw)
			plan.Digest = hex.EncodeToString(sum[:])
		}
		bind()
		if err := ValidatePreflightEligibility(plan, ledger); err == nil {
			t.Fatal("known flaky child did not exclude the whole root")
		}
		plan.BuildContext.GOOS = "darwin"
		bind()
		if err := ValidatePreflightEligibility(plan, ledger); err != nil {
			t.Fatal(err)
		}
		ledger.Records[0].ExcludePreflight = false
		ledger.Records[0].Phenomenon = "timing-budget"
		plan.BuildContext.GOOS = "linux"
		bind()
		if err := ValidatePreflightEligibility(plan, ledger); err != nil {
			t.Fatalf("unknown/timing record became a blanket exclusion: %v", err)
		}
	})
}

func TestFailureLedgerMatchingNeverAdoptsForeignAttribution(t *testing.T) {
	ledger, failure := failureLedgerFixture()
	match, err := ledger.Match(failure)
	if err != nil || match.Record != "known" || match.Attribution != "non-PR-proven" {
		t.Fatalf("exact reviewed observation: %+v / %v", match, err)
	}
	for _, name := range []string{"new-run", "new-attempt", "new-source", "foreign-leaf", "foreign-os", "different-assertion", "missing-source", "missing-log"} {
		t.Run(name, func(t *testing.T) {
			observed := failure
			switch name {
			case "new-run":
				observed.RunID++
			case "new-attempt":
				observed.Attempt++
			case "new-source":
				observed.Source = strings.Repeat("d", 40)
			case "foreign-leaf":
				observed.Test = "TestGuard/sqlite"
			case "foreign-os":
				observed.GOOS = "darwin"
			case "different-assertion":
				observed.Detail = "new authority assertion failed"
			case "missing-source":
				observed.Source = ""
			case "missing-log":
				observed.Problem = "log missing"
			}
			match, err := ledger.Match(observed)
			if err != nil || match.Attribution != "unknown" {
				t.Fatalf("adopted foreign reviewed authority: %+v / %v", match, err)
			}
		})
	}
}

func TestFailurePopulationRetainsUnknownMixedAndEarlierAttempts(t *testing.T) {
	ledger, failure := failureLedgerFixture()
	runs := []FailureRun{{ID: 1, RunAttempt: 2, Event: "pull_request", HeadSHA: failure.HeadSHA, Status: "completed", Conclusion: "success", Attempts: []FailureAttempt{
		{Number: 1, Conclusion: "failure", Failures: []ObservedFailure{failure}}, {Number: 2, Conclusion: "success"},
	}}}
	summary, err := ledger.Summarize(runs)
	if err != nil || summary.NonPROnly != 1 || summary.RedRuns != 1 || summary.EarlierRedNowGreen != 1 || summary.ByPhenomenon["unknown-variance"].ReviewedNonPRRuns != 1 {
		t.Fatalf("earlier reviewed RED vanished: %+v / %v", summary, err)
	}
	unknown := failure
	unknown.Test = "TestOther"
	runs[0].Attempts[0].Failures = append(runs[0].Attempts[0].Failures, unknown)
	summary, err = ledger.Summarize(runs)
	if err != nil || summary.Mixed != 1 || summary.NonPROnly != 0 {
		t.Fatalf("unknown was excluded from population: %+v / %v", summary, err)
	}
	if summary.ByPhenomenon["unknown-variance"].UnknownObservations != 1 {
		t.Fatalf("class summary lost the unmatched leaf: %+v", summary)
	}
	runs = append(runs, FailureRun{ID: 2, Event: "pull_request", HeadSHA: failure.HeadSHA, Status: "queued"}, FailureRun{ID: 3, Event: "pull_request", HeadSHA: failure.HeadSHA, Status: "completed", Conclusion: "cancelled"})
	summary, err = ledger.Summarize(runs)
	if err != nil || summary.PRRuns != 3 || summary.InFlight != 1 || summary.Cancelled != 1 {
		t.Fatalf("missing exposure accounting: %+v / %v", summary, err)
	}
	if summary.Incomplete != 2 {
		t.Fatalf("missing attempts were counted as complete population evidence: %+v", summary)
	}
	runs[0].Attempts[0].Failures[0].Attempt = 3
	if _, err := ledger.Summarize(runs); err == nil {
		t.Fatal("foreign attempt qualified population")
	}
	checkFailureAttemptHistory(t)
}

func checkFailureAttemptHistory(t *testing.T) {
	for _, mode := range []string{"missing-first", "missing-middle", "missing-final", "missing-count", "complete", "extra"} {
		t.Run(mode, func(t *testing.T) {
			ledger, f := failureLedgerFixture()
			run := FailureRun{ID: 1, RunAttempt: 3, Event: "pull_request", HeadSHA: f.HeadSHA, Status: "completed", Conclusion: "success", Attempts: []FailureAttempt{{Number: 1, Conclusion: "failure", Failures: []ObservedFailure{f}}, {Number: 2, Conclusion: "success"}, {Number: 3, Conclusion: "success"}}}
			switch mode {
			case "missing-first":
				run.Attempts = run.Attempts[1:]
			case "missing-middle":
				run.Attempts = append(run.Attempts[:1], run.Attempts[2:]...)
			case "missing-final":
				run.Attempts = run.Attempts[:2]
			case "missing-count":
				run.RunAttempt = 0
			case "extra":
				run.RunAttempt = 2
			}
			raw, err := json.Marshal(run)
			if err != nil {
				t.Fatal(err)
			}
			var offline FailureRun
			if err := json.Unmarshal(raw, &offline); err != nil {
				t.Fatal(err)
			}
			got, err := ledger.Summarize([]FailureRun{offline})
			if mode == "extra" {
				if err == nil {
					t.Fatal("attempt outside declared history accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "complete" {
				if got.Incomplete != 0 || got.NonPROnly != 1 || got.EarlierRedNowGreen != 1 {
					t.Fatalf("complete history: %+v", got)
				}
			} else if got.Incomplete != 1 || got.NonPROnly != 0 {
				t.Fatalf("missing history became complete/attributed: %+v", got)
			}
		})
	}
}
