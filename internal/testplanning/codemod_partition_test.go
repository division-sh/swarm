package testplanning

import (
	"fmt"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"testing"
)

const codemodProofPackage = "github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations"
const codemodIntegrationRoots = `^(TestNativeTimerSuccessorObserverRejectsRepublicationBothStores|TestNativeHandlerExecutionWindowRejectsEarlyProductionDispatchBothStores|TestNativeBusSourceBoundaryRejectsLostProductionClaimAndSettlementFacts|TestNativeBusOriginAssertionFailureJoinsBlockedSQLAndRuntimeOwners|TestReviewedSnapshotCandidateOverlayTypeChecks)$`

// Run-only complement: the policy permits skip only for the original soak.
// The finite equality and future-prefix controls below pin this selector.
const codemodOrdinaryRoots = `^(?:T(?:e(?:s(?:t(?:N(?:a(?:t(?:i(?:v(?:e(?:B(?:u(?:s(?:O(?:r(?:i(?:g(?:i(?:n(?:A(?:s(?:s(?:e(?:r(?:t(?:i(?:o(?:n(?:F(?:a(?:i(?:l(?:u(?:r(?:e(?:J(?:o(?:i(?:n(?:s(?:B(?:l(?:o(?:c(?:k(?:e(?:d(?:S(?:Q(?:L(?:A(?:n(?:d(?:R(?:u(?:n(?:t(?:i(?:m(?:e(?:O(?:w(?:n(?:e(?:r(?:s.+|[^s].*)?|[^r].*)?|[^e].*)?|[^n].*)?|[^w].*)?|[^O].*)?|[^e].*)?|[^m].*)?|[^i].*)?|[^t].*)?|[^n].*)?|[^u].*)?|[^R].*)?|[^d].*)?|[^n].*)?|[^A].*)?|[^L].*)?|[^Q].*)?|[^S].*)?|[^d].*)?|[^e].*)?|[^k].*)?|[^c].*)?|[^o].*)?|[^l].*)?|[^B].*)?|[^s].*)?|[^n].*)?|[^i].*)?|[^o].*)?|[^J].*)?|[^e].*)?|[^r].*)?|[^u].*)?|[^l].*)?|[^i].*)?|[^a].*)?|[^F].*)?|[^n].*)?|[^o].*)?|[^i].*)?|[^t].*)?|[^r].*)?|[^e].*)?|[^s].*)?|[^s].*)?|[^A].*)?|[^n].*)?|[^i].*)?|[^g].*)?|[^i].*)?|[^r].*)?|S(?:o(?:u(?:r(?:c(?:e(?:B(?:o(?:u(?:n(?:d(?:a(?:r(?:y(?:R(?:e(?:j(?:e(?:c(?:t(?:s(?:L(?:o(?:s(?:t(?:P(?:r(?:o(?:d(?:u(?:c(?:t(?:i(?:o(?:n(?:C(?:l(?:a(?:i(?:m(?:A(?:n(?:d(?:S(?:e(?:t(?:t(?:l(?:e(?:m(?:e(?:n(?:t(?:F(?:a(?:c(?:t(?:s.+|[^s].*)?|[^t].*)?|[^c].*)?|[^a].*)?|[^F].*)?|[^t].*)?|[^n].*)?|[^e].*)?|[^m].*)?|[^e].*)?|[^l].*)?|[^t].*)?|[^t].*)?|[^e].*)?|[^S].*)?|[^d].*)?|[^n].*)?|[^A].*)?|[^m].*)?|[^i].*)?|[^a].*)?|[^l].*)?|[^C].*)?|[^n].*)?|[^o].*)?|[^i].*)?|[^t].*)?|[^c].*)?|[^u].*)?|[^d].*)?|[^o].*)?|[^r].*)?|[^P].*)?|[^t].*)?|[^s].*)?|[^o].*)?|[^L].*)?|[^s].*)?|[^t].*)?|[^c].*)?|[^e].*)?|[^j].*)?|[^e].*)?|[^R].*)?|[^y].*)?|[^r].*)?|[^a].*)?|[^d].*)?|[^n].*)?|[^u].*)?|[^o].*)?|[^B].*)?|[^e].*)?|[^c].*)?|[^r].*)?|[^u].*)?|[^o].*)?|[^OS].*)?|[^s].*)?|[^u].*)?|H(?:a(?:n(?:d(?:l(?:e(?:r(?:E(?:x(?:e(?:c(?:u(?:t(?:i(?:o(?:n(?:W(?:i(?:n(?:d(?:o(?:w(?:R(?:e(?:j(?:e(?:c(?:t(?:s(?:E(?:a(?:r(?:l(?:y(?:P(?:r(?:o(?:d(?:u(?:c(?:t(?:i(?:o(?:n(?:D(?:i(?:s(?:p(?:a(?:t(?:c(?:h(?:B(?:o(?:t(?:h(?:S(?:t(?:o(?:r(?:e(?:s.+|[^s].*)?|[^e].*)?|[^r].*)?|[^o].*)?|[^t].*)?|[^S].*)?|[^h].*)?|[^t].*)?|[^o].*)?|[^B].*)?|[^h].*)?|[^c].*)?|[^t].*)?|[^a].*)?|[^p].*)?|[^s].*)?|[^i].*)?|[^D].*)?|[^n].*)?|[^o].*)?|[^i].*)?|[^t].*)?|[^c].*)?|[^u].*)?|[^d].*)?|[^o].*)?|[^r].*)?|[^P].*)?|[^y].*)?|[^l].*)?|[^r].*)?|[^a].*)?|[^E].*)?|[^s].*)?|[^t].*)?|[^c].*)?|[^e].*)?|[^j].*)?|[^e].*)?|[^R].*)?|[^w].*)?|[^o].*)?|[^d].*)?|[^n].*)?|[^i].*)?|[^W].*)?|[^n].*)?|[^o].*)?|[^i].*)?|[^t].*)?|[^u].*)?|[^c].*)?|[^e].*)?|[^x].*)?|[^E].*)?|[^r].*)?|[^e].*)?|[^l].*)?|[^d].*)?|[^n].*)?|[^a].*)?|T(?:i(?:m(?:e(?:r(?:S(?:u(?:c(?:c(?:e(?:s(?:s(?:o(?:r(?:O(?:b(?:s(?:e(?:r(?:v(?:e(?:r(?:R(?:e(?:j(?:e(?:c(?:t(?:s(?:R(?:e(?:p(?:u(?:b(?:l(?:i(?:c(?:a(?:t(?:i(?:o(?:n(?:B(?:o(?:t(?:h(?:S(?:t(?:o(?:r(?:e(?:s.+|[^s].*)?|[^e].*)?|[^r].*)?|[^o].*)?|[^t].*)?|[^S].*)?|[^h].*)?|[^t].*)?|[^o].*)?|[^B].*)?|[^n].*)?|[^o].*)?|[^i].*)?|[^t].*)?|[^a].*)?|[^c].*)?|[^i].*)?|[^l].*)?|[^b].*)?|[^u].*)?|[^p].*)?|[^e].*)?|[^R].*)?|[^s].*)?|[^t].*)?|[^c].*)?|[^e].*)?|[^j].*)?|[^e].*)?|[^R].*)?|[^r].*)?|[^e].*)?|[^v].*)?|[^r].*)?|[^e].*)?|[^s].*)?|[^b].*)?|[^O].*)?|[^r].*)?|[^o].*)?|[^s].*)?|[^s].*)?|[^e].*)?|[^c].*)?|[^c].*)?|[^u].*)?|[^S].*)?|[^r].*)?|[^e].*)?|[^m].*)?|[^i].*)?|[^BHT].*)?|[^e].*)?|[^v].*)?|[^i].*)?|[^t].*)?|[^a].*)?|R(?:e(?:v(?:i(?:e(?:w(?:e(?:d(?:S(?:n(?:a(?:p(?:s(?:h(?:o(?:t(?:C(?:a(?:n(?:d(?:i(?:d(?:a(?:t(?:e(?:O(?:v(?:e(?:r(?:l(?:a(?:y(?:T(?:y(?:p(?:e(?:C(?:h(?:e(?:c(?:k(?:s.+|[^s].*)?|[^k].*)?|[^c].*)?|[^e].*)?|[^h].*)?|[^C].*)?|[^e].*)?|[^p].*)?|[^y].*)?|[^T].*)?|[^y].*)?|[^a].*)?|[^l].*)?|[^r].*)?|[^e].*)?|[^v].*)?|[^O].*)?|[^e].*)?|[^t].*)?|[^a].*)?|[^d].*)?|[^i].*)?|[^d].*)?|[^n].*)?|[^a].*)?|[^C].*)?|[^t].*)?|[^o].*)?|[^h].*)?|[^s].*)?|[^p].*)?|[^a].*)?|[^n].*)?|[^S].*)?|[^d].*)?|[^e].*)?|[^w].*)?|[^e].*)?|[^i].*)?|[^v].*)?|[^e].*)?|[^NR].*)?|[^t].*)?|[^s].*)?|[^e].*)?|[^T].*)?$`

func codemodProofEnvelopes() map[string]UnitPolicy {
	common := UnitPolicy{Packages: []string{codemodProofPackage}, CountMode: "count-1", EnvironmentIDs: managedEnvironmentIDs(), BudgetClass: "broad"}
	guards, pipeline, bus, candidate := common, common, common, common
	guards.Run = codemodOrdinaryRoots
	pipeline.Run = `^(TestNativeTimerSuccessorObserverRejectsRepublicationBothStores|TestNativeHandlerExecutionWindowRejectsEarlyProductionDispatchBothStores)$`
	pipeline.RequiredChildren = map[string][]string{
		"TestNativeTimerSuccessorObserverRejectsRepublicationBothStores":           {"duplicate", "stale_generation"},
		"TestNativeHandlerExecutionWindowRejectsEarlyProductionDispatchBothStores": {"sqlite", "postgres"},
	}
	bus.Run = `^(TestNativeBusSourceBoundaryRejectsLostProductionClaimAndSettlementFacts|TestNativeBusOriginAssertionFailureJoinsBlockedSQLAndRuntimeOwners)$`
	bus.RequiredChildren = map[string][]string{"TestNativeBusSourceBoundaryRejectsLostProductionClaimAndSettlementFacts": {"claim", "settle"}}
	candidate.Run = `^TestReviewedSnapshotCandidateOverlayTypeChecks$`
	return map[string]UnitPolicy{"codemod-owner-guards": guards, "codemod-pipeline-mutation": pipeline, "codemod-bus-mutation": bus, "codemod-candidate-overlay": candidate}
}

func validateCodemodProofEnvelopes(policy Policy) error {
	if !slices.Contains(policy.SpecialPackages, codemodProofPackage) {
		return fmt.Errorf("codemod proof package escaped explicit ownership")
	}
	for id, want := range codemodProofEnvelopes() {
		if !reflect.DeepEqual(policy.Units[id], want) {
			return fmt.Errorf("%s changed count/environment/selector/child/budget envelope", id)
		}
		for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
			want := tier == ProfileFull || id == "codemod-owner-guards" || tier == ProfileLifecycle && id != "codemod-candidate-overlay"
			if slices.Contains(policy.Profiles[tier].Units, id) != want {
				return fmt.Errorf("%s has incorrect %s placement", id, tier)
			}
		}
	}
	return nil
}

func TestCodemodProofPlacementPreservesCompleteEnvelopes(t *testing.T) {
	if err := validateCodemodProofEnvelopes(loadPersistenceDebtPolicy(t)); err != nil {
		t.Fatal(err)
	}
}

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
		if tier == ProfileLifecycle {
			// This one exact full-only deferral remains owned, not executed by
			// lifecycle or replaced by a cheaper compiler proxy.
			candidate := policy.Units["codemod-candidate-overlay"]
			units = append(units, ProofUnit{ID: "deferred-full-candidate", Run: candidate.Run})
		}
		if err := ValidateGoProofUnitPartition(dir, units); err != nil {
			t.Fatalf("%s executed/deferred partition lost an original/new codemod root: %v", tier, err)
		}
	}
	core := policy.Units["codemod-owner-guards"]
	if err := ValidateGoProofUnitPartition(dir, []ProofUnit{{Run: core.Run, Skip: core.Skip}, {Run: codemodIntegrationRoots}}); err != nil {
		t.Fatalf("core loses more than the exact five integration roots: %v", err)
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
		"TestNativeTimerSuccessorObserverRejectsRepublicationBothStores",
		"TestNativeHandlerExecutionWindowRejectsEarlyProductionDispatchBothStores",
		"TestNativeBusSourceBoundaryRejectsLostProductionClaimAndSettlementFacts",
		"TestNativeBusOriginAssertionFailureJoinsBlockedSQLAndRuntimeOwners",
		"TestReviewedSnapshotCandidateOverlayTypeChecks",
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

func TestCodemodProofPlacementRejectsEnvelopeAndCoverageLoss(t *testing.T) {
	for name, change := range map[string]func(*Policy){
		"missing special owner": func(p *Policy) { p.SpecialPackages = nil },
		"missing lifecycle unit": func(p *Policy) {
			profile := p.Profiles[ProfileLifecycle]
			profile.Units = slices.DeleteFunc(profile.Units, func(id string) bool { return id == "codemod-bus-mutation" })
			p.Profiles[ProfileLifecycle] = profile
		},
		"promoted expensive core": func(p *Policy) {
			profile := p.Profiles[ProfileCore]
			profile.Units = append(profile.Units, "codemod-pipeline-mutation")
			p.Profiles[ProfileCore] = profile
		},
		"lost branch": func(p *Policy) {
			unit := p.Units["codemod-pipeline-mutation"]
			unit.RequiredChildren = nil
			p.Units["codemod-pipeline-mutation"] = unit
		},
		"lost ordinary roots": func(p *Policy) {
			unit := p.Units["codemod-owner-guards"]
			unit.Run = "^TestCandidate"
			p.Units["codemod-owner-guards"] = unit
		},
		"changed environment": func(p *Policy) {
			unit := p.Units["codemod-candidate-overlay"]
			unit.EnvironmentIDs = nil
			p.Units["codemod-candidate-overlay"] = unit
		},
		"changed count": func(p *Policy) {
			unit := p.Units["codemod-bus-mutation"]
			unit.CountMode = "cached"
			p.Units["codemod-bus-mutation"] = unit
		},
		"raised budget": func(p *Policy) {
			unit := p.Units["codemod-pipeline-mutation"]
			unit.BudgetClass = "full"
			p.Units["codemod-pipeline-mutation"] = unit
		},
	} {
		t.Run(name, func(t *testing.T) {
			policy := loadPersistenceDebtPolicy(t)
			change(&policy)
			if err := validateCodemodProofEnvelopes(policy); err == nil {
				t.Fatal("changed qualification contract accepted")
			}
		})
	}
}
