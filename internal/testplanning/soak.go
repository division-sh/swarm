package testplanning

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	SoakPackage              = "github.com/division-sh/swarm/internal/runtime/conformance"
	SoakTest                 = "TestIssue2394TwentyTwoIntentFifteenMinuteSoakBothStores"
	SoakRun                  = "^" + SoakTest + "$"
	SoakGoTimeout            = "22m"
	ServedReporterPackage    = "github.com/division-sh/swarm/internal/serveapp"
	ServedReporterRun        = "^(TestIssue2394ServedOriginalReporterFiveHundredBothStores|TestIssue2394ServedOriginalReporterMeasuredIssuanceBothStores|TestIssue2394ServedReporterTransactionCensusBothStores)$"
	ServedReporterStressRun  = "^TestIssue2394ServedOriginalReporterFiveHundredDelayedBothStores$"
	ServedReporterGoTimeout  = "15m"
	ServedPreservationTest   = "TestIssue2394ServedOneSecondCommitPreservesTwoFullChunksBothStores"
	ServedPreservationRun    = "^" + ServedPreservationTest + "$"
	PersistenceDebtGoTimeout = "20m"
)

const conformanceRebalancedReporterRoot = "TestNumericFanOutReporterShapeCompletesAndPreservesSemanticRejectionsOnBothBackends"

// SoakBackend recognizes only the two approved backend-partitioned soak cells.
func SoakBackend(run string) (string, bool) {
	for _, backend := range []string{"sqlite", "postgres"} {
		if run == SoakRun+"/^"+backend+"$" {
			return backend, true
		}
	}
	return "", false
}

// The served reporter's longer timeout keeps its ratified drain ceiling
// reachable without splitting or omitting either backend's whole proof.
func validateSoakSelection(packages []string, run, skip, timeout, count, budget string) error {
	if budget == "soak" {
		_, ok := SoakBackend(run)
		if !ok || len(packages) != 1 || packages[0] != SoakPackage || skip != "" || timeout != SoakGoTimeout || count != "count-1" {
			return fmt.Errorf("soak requires one exact backend, original test/package, count-1, %s timeout and no exclusion", SoakGoTimeout)
		}
		return nil
	}
	if isServedReporterSelection(packages, run, skip, timeout, count, budget) {
		return nil
	}
	if timeout == PersistenceDebtGoTimeout && len(packages) == 1 && packages[0] == "github.com/division-sh/swarm/internal/store" && run == "^(TestNativeFixtureFamiliesDoNotReceiveRawAuthority|TestPersistenceAuthorityDebtRatchet)$" && skip == "" && count == "count-1" && budget == "broad" {
		return nil
	}
	if timeout != "" || strings.Contains(run, "/") {
		return fmt.Errorf("only the exact soak, served reporter or persistence census may set its approved timeout; only the soak may filter a backend")
	}
	if isPipelineRestSelection(packages, run, skip, count, budget) {
		return nil
	}
	if isConformanceReporterExclusion(packages, run, skip, count, budget) {
		return nil
	}
	if skip != "" && (skip != SoakRun || len(packages) != 1 || packages[0] != SoakPackage) {
		return fmt.Errorf("only the exact separately executed soak, pipeline or conformance reporter families may be excluded")
	}
	return nil
}

// The served progress and all-commit roots share one exact timeout envelope.
func isServedReporterSelection(packages []string, run, skip, timeout, count, budget string) bool {
	return timeout == ServedReporterGoTimeout && len(packages) == 1 && packages[0] == ServedReporterPackage &&
		(run == ServedReporterRun || run == ServedReporterStressRun) && skip == "" &&
		count == "count-1" && budget == "full"
}

// The complementary pipeline owner excludes only its two separately executed
// whole-root families, never a backend or descendant case.
func isPipelineRestSelection(packages []string, run, skip, count, budget string) bool {
	return len(packages) == 1 && packages[0] == "github.com/division-sh/swarm/internal/runtime/pipeline" &&
		run == `^(Test|Fuzz|Example)` && skip == `^Test(A2|CompiledTransition|Guard|PipelineCompiled|WorkflowCurrentTransition|JoinCapturedLoop|WorkflowTimer|WorkflowJoin|AcceptedWorkflowTimer|MutationFreeAcceptedEventTimer|Issue2564M09).*$` &&
		count == "count-1" && budget == "broad"
}

func isConformanceReporterExclusion(packages []string, run, skip, count, budget string) bool {
	return len(packages) == 1 && packages[0] == SoakPackage &&
		run == `^(Test($|[^FHIV].*|I($|[^s].*|s($|[^s].*|s($|[^u].*|u($|[^e].*|e($|[^2].*|2($|[^3].*|3($|[^9].*|9($|[^4].*)))))))))|Example.*|Fuzz.*)$` &&
		skip == "^"+conformanceRebalancedReporterRoot+"$" && count == "count-1" && budget == "broad"
}

// ValidateConformanceProofPartition expands exactly the two approved backend
// cells into one top-level owner, then uses the ordinary declaration census.
// It does not permit arbitrary partial-subtest filters.
func ValidateConformanceProofPartition(dir string, units []ProofUnit) error {
	backends := map[string]bool{}
	var matchers []func(string) bool
	for _, unit := range units {
		if err := validateSoakSelection(unit.Packages, unit.Run, unit.Skip, unit.GoTimeout, unit.CountMode, unit.BudgetClass); err != nil {
			return err
		}
		if unit.BudgetClass == "soak" {
			backend, _ := SoakBackend(unit.Run)
			if backends[backend] {
				return fmt.Errorf("duplicate soak backend %s", backend)
			}
			backends[backend] = true
			continue
		}
		pattern, err := regexp.Compile(unit.Run)
		if err != nil {
			return err
		}
		if pattern.MatchString(SoakTest) && unit.Skip != SoakRun {
			return fmt.Errorf("ordinary unit %s also executes the soak", unit.ID)
		}
		skipped, err := regexp.Compile(unit.Skip)
		if err != nil {
			return err
		}
		matchers = append(matchers, func(name string) bool {
			return pattern.MatchString(name) && (unit.Skip == "" || !skipped.MatchString(name))
		})
	}
	if len(backends) != 0 && (!backends["sqlite"] || !backends["postgres"]) {
		return fmt.Errorf("scheduled soak requires both sqlite and postgres cells")
	}
	if len(backends) == 0 {
		return validateGoProofMatchersExcept(dir, matchers, map[string]bool{SoakTest: true})
	}
	// The validated complete backend pair owns the declaration once.
	matchers = append(matchers, func(name string) bool { return name == SoakTest })
	return validateGoProofMatchers(dir, matchers)
}
