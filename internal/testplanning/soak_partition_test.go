package testplanning

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestConformanceSoakPartitionEffectiveOwnership(t *testing.T) {
	dir := t.TempDir()
	source := "package proof\nimport \"testing\"\nfunc TestOrdinary(t *testing.T) {}\nfunc " + SoakTest + "(t *testing.T) {}\n"
	if err := os.WriteFile(filepath.Join(dir, "proof_test.go"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	for _, shape := range []string{"structural exclusion", "exact skip", "unfiltered exact skip"} {
		t.Run(shape, func(t *testing.T) {
			ordinary := ProofUnit{ID: "ordinary", Packages: []string{SoakPackage}, Run: "^TestOrdinary$", CountMode: "count-1", BudgetClass: "broad"}
			if shape != "structural exclusion" {
				ordinary.Run, ordinary.Skip = "^Test.*$", SoakRun
			}
			if shape == "unfiltered exact skip" {
				ordinary.Run = ""
			}
			units := []ProofUnit{ordinary}
			for _, backend := range []string{"sqlite", "postgres"} {
				units = append(units, ProofUnit{ID: backend, Packages: []string{SoakPackage}, Run: SoakRun + "/^" + backend + "$", GoTimeout: SoakGoTimeout, CountMode: "count-1", BudgetClass: "soak"})
			}
			if err := ValidateConformanceProofPartition(dir, units); err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name string
				edit func([]ProofUnit) []ProofUnit
			}{
				{"missing sqlite", func(u []ProofUnit) []ProofUnit { return append(u[:1], u[2:]...) }},
				{"missing postgres", func(u []ProofUnit) []ProofUnit { return u[:2] }},
				{"duplicate backend", func(u []ProofUnit) []ProofUnit { return append(u, u[1]) }},
				{"missing ordinary", func(u []ProofUnit) []ProofUnit { return u[1:] }},
				{"duplicate ordinary", func(u []ProofUnit) []ProofUnit { return append(u, u[0]) }},
				{"unskipped soak", func(u []ProofUnit) []ProofUnit {
					u[0].Run, u[0].Skip = "^Test.*$", ""
					return u
				}},
				{"broad skip", func(u []ProofUnit) []ProofUnit { u[0].Skip = "^Test.*$"; return u }},
				{"partial ordinary", func(u []ProofUnit) []ProofUnit { u[0].Run += "/sqlite"; return u }},
				{"partial soak", func(u []ProofUnit) []ProofUnit { u[1].Run += "/partial"; return u }},
				{"wrong timeout", func(u []ProofUnit) []ProofUnit { u[1].GoTimeout = "30m"; return u }},
				{"cached soak", func(u []ProofUnit) []ProofUnit { u[1].CountMode = "cache-default"; return u }},
				{"wrong package", func(u []ProofUnit) []ProofUnit { u[1].Packages = []string{"other"}; return u }},
				{"skipped soak cell", func(u []ProofUnit) []ProofUnit { u[1].Skip = SoakRun; return u }},
				{"empty effective ordinary", func(u []ProofUnit) []ProofUnit {
					u[0].Run, u[0].Skip = SoakRun, SoakRun
					return append(u, ordinary)
				}},
				{"malformed selector", func(u []ProofUnit) []ProofUnit { u[0].Run = "["; return u }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if err := ValidateConformanceProofPartition(dir, tc.edit(slices.Clone(units))); err == nil {
						t.Fatal("accepted changed effective proof ownership or soak envelope")
					}
				})
			}
		})
	}
}

func TestServedReporterTimeoutEnvelopeIsExact(t *testing.T) {
	packages, run, timeout, count, budget := []string{ServedReporterPackage}, ServedReporterRun, ServedReporterGoTimeout, "count-1", "full"
	if err := validateSoakSelection(packages, run, "", timeout, count, budget); err != nil {
		t.Fatalf("approved whole reporter proof rejected: %v", err)
	}
	for _, tc := range []struct {
		name                              string
		packages                          []string
		run, skip, timeout, count, budget string
	}{
		{name: "short timeout", packages: packages, run: run, timeout: "10m", count: count, budget: budget},
		{name: "long timeout", packages: packages, run: run, timeout: "22m", count: count, budget: budget},
		{name: "wrong package", packages: []string{SoakPackage}, run: run, timeout: timeout, count: count, budget: budget},
		{name: "partial test", packages: packages, run: run + "/sqlite", timeout: timeout, count: count, budget: budget},
		{name: "narrowed selector", packages: packages, run: "^TestIssue2394ServedOriginalReporterFiveHundredDelayedBothStores$", timeout: timeout, count: count, budget: budget},
		{name: "skip", packages: packages, run: run, skip: "^TestIssue2394.*$", timeout: timeout, count: count, budget: budget},
		{name: "cached", packages: packages, run: run, timeout: timeout, count: "cache-default", budget: budget},
		{name: "wrong budget", packages: packages, run: run, timeout: timeout, count: count, budget: "broad"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateSoakSelection(tc.packages, tc.run, tc.skip, tc.timeout, tc.count, tc.budget); err == nil {
				t.Fatal("changed served reporter proof envelope accepted")
			}
		})
	}
}
