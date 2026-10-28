package testplanning

import (
	"path/filepath"

	"regexp"
	"slices"
	"testing"
)

const codemodProofPackage = "github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations"
const codemodIntegrationRoots = `^(TestNativeBusSourceBoundaryRejectsLostProductionClaimAndSettlementFacts|TestNativeBusOriginAssertionFailureJoinsBlockedSQLAndRuntimeOwners|TestReviewedSnapshotCandidateOverlayTypeChecks)$`

// Run-only complement: the policy permits skip only for the original soak.
// The finite equality and future-prefix controls below pin this selector.
const codemodOrdinaryRoots = `^(?:T(?:e(?:s(?:t(?:N(?:a(?:t(?:i(?:v(?:e(?:B(?:u(?:s(?:O(?:r(?:i(?:g(?:i(?:n(?:A(?:s(?:s(?:e(?:r(?:t(?:i(?:o(?:n(?:F(?:a(?:i(?:l(?:u(?:r(?:e(?:J(?:o(?:i(?:n(?:s(?:B(?:l(?:o(?:c(?:k(?:e(?:d(?:S(?:Q(?:L(?:A(?:n(?:d(?:R(?:u(?:n(?:t(?:i(?:m(?:e(?:O(?:w(?:n(?:e(?:r(?:s.+|[^s].*)?|[^r].*)?|[^e].*)?|[^n].*)?|[^w].*)?|[^O].*)?|[^e].*)?|[^m].*)?|[^i].*)?|[^t].*)?|[^n].*)?|[^u].*)?|[^R].*)?|[^d].*)?|[^n].*)?|[^A].*)?|[^L].*)?|[^Q].*)?|[^S].*)?|[^d].*)?|[^e].*)?|[^k].*)?|[^c].*)?|[^o].*)?|[^l].*)?|[^B].*)?|[^s].*)?|[^n].*)?|[^i].*)?|[^o].*)?|[^J].*)?|[^e].*)?|[^r].*)?|[^u].*)?|[^l].*)?|[^i].*)?|[^a].*)?|[^F].*)?|[^n].*)?|[^o].*)?|[^i].*)?|[^t].*)?|[^r].*)?|[^e].*)?|[^s].*)?|[^s].*)?|[^A].*)?|[^n].*)?|[^i].*)?|[^g].*)?|[^i].*)?|[^r].*)?|S(?:o(?:u(?:r(?:c(?:e(?:B(?:o(?:u(?:n(?:d(?:a(?:r(?:y(?:R(?:e(?:j(?:e(?:c(?:t(?:s(?:L(?:o(?:s(?:t(?:P(?:r(?:o(?:d(?:u(?:c(?:t(?:i(?:o(?:n(?:C(?:l(?:a(?:i(?:m(?:A(?:n(?:d(?:S(?:e(?:t(?:t(?:l(?:e(?:m(?:e(?:n(?:t(?:F(?:a(?:c(?:t(?:s.+|[^s].*)?|[^t].*)?|[^c].*)?|[^a].*)?|[^F].*)?|[^t].*)?|[^n].*)?|[^e].*)?|[^m].*)?|[^e].*)?|[^l].*)?|[^t].*)?|[^t].*)?|[^e].*)?|[^S].*)?|[^d].*)?|[^n].*)?|[^A].*)?|[^m].*)?|[^i].*)?|[^a].*)?|[^l].*)?|[^C].*)?|[^n].*)?|[^o].*)?|[^i].*)?|[^t].*)?|[^c].*)?|[^u].*)?|[^d].*)?|[^o].*)?|[^r].*)?|[^P].*)?|[^t].*)?|[^s].*)?|[^o].*)?|[^L].*)?|[^s].*)?|[^t].*)?|[^c].*)?|[^e].*)?|[^j].*)?|[^e].*)?|[^R].*)?|[^y].*)?|[^r].*)?|[^a].*)?|[^d].*)?|[^n].*)?|[^u].*)?|[^o].*)?|[^B].*)?|[^e].*)?|[^c].*)?|[^r].*)?|[^u].*)?|[^o].*)?|[^OS].*)?|[^s].*)?|[^u].*)?|[^B].*)?|[^e].*)?|[^v].*)?|[^i].*)?|[^t].*)?|[^a].*)?|R(?:e(?:v(?:i(?:e(?:w(?:e(?:d(?:S(?:n(?:a(?:p(?:s(?:h(?:o(?:t(?:C(?:a(?:n(?:d(?:i(?:d(?:a(?:t(?:e(?:O(?:v(?:e(?:r(?:l(?:a(?:y(?:T(?:y(?:p(?:e(?:C(?:h(?:e(?:c(?:k(?:s.+|[^s].*)?|[^k].*)?|[^c].*)?|[^e].*)?|[^h].*)?|[^C].*)?|[^e].*)?|[^p].*)?|[^y].*)?|[^T].*)?|[^y].*)?|[^a].*)?|[^l].*)?|[^r].*)?|[^e].*)?|[^v].*)?|[^O].*)?|[^e].*)?|[^t].*)?|[^a].*)?|[^d].*)?|[^i].*)?|[^d].*)?|[^n].*)?|[^a].*)?|[^C].*)?|[^t].*)?|[^o].*)?|[^h].*)?|[^s].*)?|[^p].*)?|[^a].*)?|[^n].*)?|[^S].*)?|[^d].*)?|[^e].*)?|[^w].*)?|[^e].*)?|[^i].*)?|[^v].*)?|[^e].*)?|[^NR].*)?|[^t].*)?|[^s].*)?|[^e].*)?|[^T].*)?$`

func TestCodemodProofPlacementRetainsExhaustivePartition(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	dir := filepath.Join("..", "..", "tools/fixture-codemod/pipeline-observations")
	for _, tier := range []string{ProfileLifecycle, ProfileFull} {
		var units []ProofUnit
		for _, id := range policy.Profiles[tier].Units {
			unit := policy.Units[id]
			if slices.Contains(unit.Packages, codemodProofPackage) {
				units = append(units, ProofUnit{ID: id, Run: unit.Run, Skip: unit.Skip})
			}
		}
		if err := ValidateGoProofUnitPartition(dir, units); err != nil {
			t.Fatalf("%s executed/deferred partition lost an original/new codemod root: %v", tier, err)
		}
	}
	core := policy.Units["codemod-owner-guards"]
	if err := ValidateGoProofUnitPartition(dir, []ProofUnit{{Run: core.Run, Skip: core.Skip}, {Run: codemodIntegrationRoots}}); err != nil {
		t.Fatalf("core loses more than the exact three integration roots: %v", err)
	}
	for _, future := range []string{"TestFutureCodemodOwnerGuard", "ExampleFutureCodemod", "FuzzFutureCodemod"} {
		selected, err := selectedByUnit(ProofUnit{Run: core.Run, Skip: core.Skip}, future)
		if err != nil || !selected {
			t.Fatalf("new ordinary root silently deferred: %s/%v", future, err)
		}
	}
}

func TestCodemodProofPlacementExcludesOnlyExactIntegrationNames(t *testing.T) {
	ordinary := regexp.MustCompile(codemodOrdinaryRoots)
	integration := regexp.MustCompile(codemodIntegrationRoots)
	names := []string{
		"TestNativeBusSourceBoundaryRejectsLostProductionClaimAndSettlementFacts",
	}
	for _, name := range names {
		if ordinary.MatchString(name) || !integration.MatchString(name) {
			t.Fatalf("integration root not exclusively deferred: %s", name)
		}
		for i := len("Test"); i < len(name); i++ {
			for _, candidate := range []string{name[:i], name[:i] + "X" + name[i+1:], name[:i] + "_" + name[i:]} {
				if ordinary.MatchString(candidate) == integration.MatchString(candidate) {
					t.Fatalf("future prefix/neighbor has zero or two owners: %s", candidate)
				}
			}
		}
		for _, suffix := range []string{"Future", "_2", "X"} {
			if !ordinary.MatchString(name+suffix) || integration.MatchString(name+suffix) {
				t.Fatalf("future extension silently deferred: %s", name+suffix)
			}
		}
	}
}
