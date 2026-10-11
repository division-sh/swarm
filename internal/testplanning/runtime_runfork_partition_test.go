package testplanning

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestRuntimeRunForkPartitionOwnsExactProspectiveTerritory(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	ordinary, fork := policy.Units["store-runtime-full-01"], policy.Units["store-runtime-full-07-fork"]
	names := []string{"TestRun", "TestRunFuture", "TestRuntime", "TestRuntimeFuture", "TestRunF", "TestRunFanOut", "TestRunFanOutBarrierStatusRefusesMissingInvalidAndClosedEvidenceBothStores", "TestRunFo", "TestRunFor", "TestRunFork", "TestRunForkFuture"}
	for i := 2; i < len("RunFork"); i++ {
		for _, ch := range "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_" {
			names = append(names, "Test"+"RunFork"[:i]+string(ch)+"Future")
		}
	}
	ordinaryMatch, forkMatch := regexp.MustCompile(ordinary.Run), regexp.MustCompile(fork.Run)
	var source strings.Builder
	source.WriteString("package proof\nimport \"testing\"\n")
	seen := map[string]bool{}
	for _, name := range names {
		wantFork := strings.HasPrefix(name, "TestRunFork")
		if ordinaryMatch.MatchString(name) != !wantFork || forkMatch.MatchString(name) != wantFork {
			t.Errorf("%s ordinary=%v fork=%v want %v/%v", name, ordinaryMatch.MatchString(name), forkMatch.MatchString(name), !wantFork, wantFork)
		}
		if !seen[name] {
			source.WriteString("func " + name + "(t *testing.T) {}\n")
			seen[name] = true
		}
	}
	if t.Failed() {
		return
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "prospective_test.go"), []byte(source.String()), 0600); err != nil {
		t.Fatal(err)
	}
	units := []ProofUnit{{ID: "store-runtime-full-01", Run: ordinary.Run, Skip: ordinary.Skip}, {ID: "store-runtime-full-07-fork", Run: fork.Run, Skip: fork.Skip}}
	if err := ValidateGoProofUnitPartition(dir, units); err != nil {
		t.Fatal(err)
	}
	units[0].Run = "^TestRun.*$"
	if err := ValidateGoProofUnitPartition(dir, units); err == nil || !strings.Contains(err.Error(), "matches 2") {
		t.Fatalf("fork overlap accepted: %v", err)
	}
	units[0].Run = ordinary.Run
	units[0].Run = `^TestRun($|[^F].*)$`
	if err := ValidateGoProofUnitPartition(dir, units); err == nil || !strings.Contains(err.Error(), "matches 0") {
		t.Fatalf("RunF complement gap accepted: %v", err)
	}
}
