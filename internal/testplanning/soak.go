package testplanning

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	SoakPackage   = "github.com/division-sh/swarm/internal/runtime/conformance"
	SoakTest      = "TestIssue2394TwentyTwoIntentFifteenMinuteSoakBothStores"
	SoakRun       = "^" + SoakTest + "$"
	SoakGoTimeout = "22m"
)

// Only the lead-approved full-window proof may use the longer budget or a
// backend partition. All other proofs retain whole top-level selection.
func SoakBackend(run string) (string, bool) {
	for _, backend := range []string{"sqlite", "postgres"} {
		if run == SoakRun+"/^"+backend+"$" {
			return backend, true
		}
	}
	return "", false
}

func validateSoakSelection(packages []string, run, skip, timeout, count, budget string) error {
	if budget == "soak" {
		_, ok := SoakBackend(run)
		if !ok || len(packages) != 1 || packages[0] != SoakPackage || skip != "" || timeout != SoakGoTimeout || count != "count-1" {
			return fmt.Errorf("soak requires one exact backend, original test/package, count-1, %s timeout and no exclusion", SoakGoTimeout)
		}
		return nil
	}
	if timeout != "" || strings.Contains(run, "/") {
		return fmt.Errorf("only the mandatory soak may set a timeout or backend filter")
	}
	if skip != "" && (skip != SoakRun || len(packages) != 1 || packages[0] != SoakPackage) {
		return fmt.Errorf("only the exact separately executed soak may be excluded")
	}
	return nil
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
		matchers = append(matchers, func(name string) bool {
			return pattern.MatchString(name) && !(unit.Skip == SoakRun && name == SoakTest)
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
