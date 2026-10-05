package testplanning

import (
	"fmt"
	"go/build"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// ValidateGoProofPartition checks the finite repository census, not regex
// subtyping. Top-level selection preserves every selected proof's subtests.
func ValidateGoProofPartition(dir string, runs []string) error {
	units := make([]ProofUnit, len(runs))
	for i, run := range runs {
		units[i].Run = run
	}
	return ValidateGoProofUnitPartition(dir, units)
}

// ValidateGoProofUnitPartition counts effective ownership, including the finite
// separately executed exclusions admitted by the proof policy.
func ValidateGoProofUnitPartition(dir string, units []ProofUnit) error {
	if len(units) == 0 {
		return fmt.Errorf("proof package %s has no execution units", dir)
	}
	matchers := make([]func(string) bool, len(units))
	for i, unit := range units {
		run := unit.Run
		if strings.Contains(run, "/") {
			return fmt.Errorf("proof partition %d has partial-subtest filter %q", i, run)
		}
		if run == "" && len(units) != 1 {
			return fmt.Errorf("unfiltered proof owner cannot coexist with partitions")
		}
		pattern, err := regexp.Compile(run)
		if err != nil {
			return fmt.Errorf("proof partition %d: %w", i, err)
		}
		if err := validateSoakSelection(unit.Packages, run, unit.Skip, unit.GoTimeout, unit.CountMode, unit.BudgetClass); err != nil {
			return err
		}
		skipped, err := regexp.Compile(unit.Skip)
		if err != nil {
			return err
		}
		matchers[i] = func(name string) bool {
			return pattern.MatchString(name) && (unit.Skip == "" || !skipped.MatchString(name))
		}
	}
	return validateGoProofMatchers(dir, matchers)
}

// Match effective top-level ownership after a caller has validated any exact
// exclusion and its separately executed replacement.
func validateGoProofMatchers(dir string, matchers []func(string) bool) error {
	return validateGoProofMatchersExcept(dir, matchers, nil)
}

func validateGoProofMatchersExcept(dir string, matchers []func(string) bool, excluded map[string]bool) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	matched := make([]bool, len(matchers))
	proofs := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		active, err := build.Default.MatchFile(dir, entry.Name())
		if err != nil {
			return fmt.Errorf("match active test file %s: %w", entry.Name(), err)
		}
		if !active {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, entry.Name()), nil, parser.ParseComments)
		if err != nil {
			return err
		}
		for _, name := range executableTestRoots(file) {
			if excluded[name] {
				continue
			}
			proofs++
			owners := 0
			for i, matches := range matchers {
				if matches(name) {
					owners++
					matched[i] = true
				}
			}
			if owners != 1 {
				return fmt.Errorf("proof %s matches %d execution units, want exactly one", name, owners)
			}
		}
	}
	if proofs == 0 {
		return fmt.Errorf("proof package %s has an empty census", dir)
	}
	for i, live := range matched {
		if !live {
			return fmt.Errorf("proof partition %d matches no proof", i)
		}
	}
	return nil
}
