package testplanning

import (
	"fmt"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"testing"
)

const unusedProofPackage = "github.com/division-sh/swarm/cmd/swarm-unused"
const unusedTransportRoot = "TestAttemptQualifiedNativeDownloadsUseRealUnionAndRefuseOldPartners"

// Positive complement excludes one whole root, not its future siblings.
const unusedOrdinaryRoots = `^(?:Test(?:[^A].*|A(?:[^t].*|t(?:[^t].*|t(?:[^e].*|e(?:[^m].*|m(?:[^p].*|p(?:[^t].*|t(?:[^Q].*|Q(?:[^u].*|u(?:[^a].*|a(?:[^l].*|l(?:[^i].*|i(?:[^f].*|f(?:[^i].*|i(?:[^e].*|e(?:[^d].*|d(?:[^N].*|N(?:[^a].*|a(?:[^t].*|t(?:[^i].*|i(?:[^v].*|v(?:[^e].*|e(?:[^D].*|D(?:[^o].*|o(?:[^w].*|w(?:[^n].*|n(?:[^l].*|l(?:[^o].*|o(?:[^a].*|a(?:[^d].*|d(?:[^s].*|s(?:[^U].*|U(?:[^s].*|s(?:[^e].*|e(?:[^R].*|R(?:[^e].*|e(?:[^a].*|a(?:[^l].*|l(?:[^U].*|U(?:[^n].*|n(?:[^i].*|i(?:[^o].*|o(?:[^n].*|n(?:[^A].*|A(?:[^n].*|n(?:[^d].*|d(?:[^R].*|R(?:[^e].*|e(?:[^f].*|f(?:[^u].*|u(?:[^s].*|s(?:[^e].*|e(?:[^O].*|O(?:[^l].*|l(?:[^d].*|d(?:[^P].*|P(?:[^a].*|a(?:[^r].*|r(?:[^t].*|t(?:[^n].*|n(?:[^e].*|e(?:[^r].*|r(?:[^s].*|s.+|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|)|Fuzz.*|Example.*)$`

func unusedProofEnvelopes() map[string]UnitPolicy {
	common := UnitPolicy{Packages: []string{unusedProofPackage}, CountMode: "count-1", EnvironmentIDs: managedEnvironmentIDs(), BudgetClass: "broad"}
	controls, transport := common, common
	controls.Run = unusedOrdinaryRoots
	transport.Run = "^" + unusedTransportRoot + "$"
	transport.RequiredChildren = map[string][]string{unusedTransportRoot: {
		"current_pair", "no_current_pair", "missing_linux", "missing_darwin",
		"failed_linux", "failed_darwin", "skipped_linux", "skipped_darwin",
		"cancelled_linux", "cancelled_darwin", "partial_linux", "partial_darwin",
	}}
	return map[string]UnitPolicy{"unused-controls": controls, "unused-attempt-transport": transport}
}

func validateUnusedProofEnvelopes(policy Policy) error {
	if !slices.Contains(policy.SpecialPackages, unusedProofPackage) {
		return fmt.Errorf("native transport package escaped special ownership")
	}
	for id, want := range unusedProofEnvelopes() {
		if !reflect.DeepEqual(policy.Units[id], want) {
			return fmt.Errorf("%s changed its complete execution envelope", id)
		}
		for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
			if slices.Contains(policy.Profiles[tier].Units, id) != (id == "unused-controls" || tier == ProfileFull) {
				return fmt.Errorf("%s changed %s placement", id, tier)
			}
		}
	}
	return nil
}

func TestUnusedTransportPartitionRetainsEveryRootAndCase(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	if err := validateUnusedProofEnvelopes(policy); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join("../..", "cmd/swarm-unused")
	for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
		var units []ProofUnit
		for _, id := range policy.Profiles[tier].Units {
			u := policy.Units[id]
			if slices.Contains(u.Packages, unusedProofPackage) {
				units = append(units, ProofUnit{ID: id, Run: u.Run, Skip: u.Skip})
			}
		}
		if tier != ProfileFull {
			// Census only: this retained full owner earns no lower-tier execution.
			units = append(units, ProofUnit{ID: "deferred-full-native-transport", Run: "^" + unusedTransportRoot + "$"})
		}
		if err := ValidateGoProofUnitPartition(dir, units); err != nil {
			t.Fatalf("%s lost exact whole-root ownership: %v", tier, err)
		}
	}
}

func TestUnusedTransportPartitionOnlyDefersOneExactRoot(t *testing.T) {
	ordinary := regexp.MustCompile(unusedOrdinaryRoots)
	transport := regexp.MustCompile("^" + unusedTransportRoot + "$")
	if ordinary.MatchString(unusedTransportRoot) {
		t.Fatal("ordinary owner duplicates full-only root")
	}
	for i := len("Test"); i < len(unusedTransportRoot); i++ {
		for _, name := range []string{unusedTransportRoot[:i], unusedTransportRoot[:i] + "X" + unusedTransportRoot[i+1:], unusedTransportRoot[:i] + "_" + unusedTransportRoot[i:]} {
			if !ordinary.MatchString(name) || transport.MatchString(name) {
				t.Fatalf("future prefix/neighbor silently deferred: %s", name)
			}
		}
	}
	for _, name := range []string{unusedTransportRoot + "Future", unusedTransportRoot + "_2", "TestFuture", "FuzzFuture", "ExampleFuture"} {
		if !ordinary.MatchString(name) || transport.MatchString(name) {
			t.Fatalf("ordinary future root lost all-tier owner: %s", name)
		}
	}
}

func TestUnusedTransportPartitionRejectsOmissionOverlapAndPromotion(t *testing.T) {
	for name, change := range map[string]func(*Policy){
		"missing special owner": func(p *Policy) { p.SpecialPackages = nil },
		"omitted ordinary roots": func(p *Policy) {
			u := p.Units["unused-controls"]
			u.Run = "^TestFuture$"
			p.Units["unused-controls"] = u
		},
		"overlapping full root": func(p *Policy) { u := p.Units["unused-controls"]; u.Run = ""; p.Units["unused-controls"] = u },
		"lost partner case": func(p *Policy) {
			u := p.Units["unused-attempt-transport"]
			u.RequiredChildren = nil
			p.Units["unused-attempt-transport"] = u
		},
		"wrong count": func(p *Policy) {
			u := p.Units["unused-attempt-transport"]
			u.CountMode = "cache-default"
			p.Units["unused-attempt-transport"] = u
		},
		"core promotion": func(p *Policy) {
			q := p.Profiles[ProfileCore]
			q.Units = append(q.Units, "unused-attempt-transport")
			p.Profiles[ProfileCore] = q
		},
		"lifecycle promotion": func(p *Policy) {
			q := p.Profiles[ProfileLifecycle]
			q.Units = append(q.Units, "unused-attempt-transport")
			p.Profiles[ProfileLifecycle] = q
		},
		"missing full owner": func(p *Policy) {
			q := p.Profiles[ProfileFull]
			q.Units = slices.DeleteFunc(q.Units, func(id string) bool { return id == "unused-attempt-transport" })
			p.Profiles[ProfileFull] = q
		},
	} {
		t.Run(name, func(t *testing.T) {
			policy := loadPersistenceDebtPolicy(t)
			change(&policy)
			if validateUnusedProofEnvelopes(policy) == nil {
				t.Fatal("native transport envelope drift accepted")
			}
		})
	}
	dir := filepath.Join("../..", "cmd/swarm-unused")
	for _, units := range [][]ProofUnit{
		{{Run: unusedOrdinaryRoots}},
		{{Run: unusedOrdinaryRoots}, {Run: "^" + unusedTransportRoot + "$"}, {Run: "^" + unusedTransportRoot + "$"}},
	} {
		if ValidateGoProofUnitPartition(dir, units) == nil {
			t.Fatal("missing/duplicate real source root accepted")
		}
	}
}
