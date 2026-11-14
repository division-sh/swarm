package testplanning

import (
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestReleaseRestCompositionPreservesExactBoundaryOwnership(t *testing.T) {
	f := loadCatalogTierFixture(t)
	full := f.bind(t, ProfileFull)
	release, _ := full.Unit(releaseRestUnit)
	base := f.bind(t, ProfileCore)
	plan := f.bind(t, ProfileCore, releaseRestUnit)
	if !reflect.DeepEqual(channelRequiredUnion(base, release), channelRequiredUnion(plan)) {
		t.Fatal("core/release-rest required root and child union changed")
	}
	actual, _ := plan.Unit(releaseRestUnit)
	actual.WeightSeconds = release.WeightSeconds
	if !reflect.DeepEqual(actual, release) {
		t.Fatal("complete release-rest recipe or obligations changed")
	}
	owners := conformancePlanOwners(plan)
	for _, name := range []string{"TestReleaseE2ECapacityFixtureImportBoundary", "TestReleaseE2EImportBoundaryRejectsRuntimeAndUnscopedPlanning", "TestReleaseE2EPackageStaysAtPublicProcessBoundary"} {
		if !slices.Equal(owners[TestRoot{Package: releaseProofPackage, Name: name}], []string{releaseRestUnit}) {
			t.Fatalf("boundary proof %s does not have exactly the full release-rest owner", name)
		}
	}
	for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
		t.Run("baseline/"+tier, func(t *testing.T) {
			baseline := f.bind(t, tier)
			if _, err := baseline.Unit(structuralComplement); err == nil {
				t.Fatal("private complement changed a baseline plan")
			}
			if tier == ProfileCore {
				if _, err := baseline.Unit(structuralUnit); err != nil {
					t.Fatal("plain core lost its original structural owner")
				}
			}
		})
	}
	for _, tc := range []struct {
		name string
		edit func(*Policy)
	}{
		{"missing complement", func(p *Policy) { delete(p.Units, structuralComplement) }},
		{"missing complement root", func(p *Policy) {
			u := p.Units[structuralComplement]
			u.Run = strings.ReplaceAll(u.Run, "TestAgentLifecycleDependenciesAreExplicit|", "")
			p.Units[structuralComplement] = u
		}},
		{"extra complement root", func(p *Policy) {
			u := p.Units[structuralComplement]
			u.Run = "(?:" + u.Run + ")|^TestNamesOnlyRootInterfaceUsesOrdinaryValidationPolicy$"
			p.Units[structuralComplement] = u
		}},
		{"missing approved transfer", func(p *Policy) {
			u := p.Units[structuralUnit]
			u.Run = strings.ReplaceAll(u.Run, "TestReleaseE2ECapacityFixtureImportBoundary|", "")
			p.Units[structuralUnit] = u
		}},
		{"extra unapproved transfer", func(p *Policy) {
			u := p.Units[structuralUnit]
			u.Run = "(?:" + u.Run + ")|^TestRootInputFeedLocalAndConnectedCLIExecuteBothStores$"
			p.Units[structuralUnit] = u
		}},
		{"changed complement recipe", func(p *Policy) {
			u := p.Units[structuralComplement]
			u.CountMode = "cache-default"
			p.Units[structuralComplement] = u
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := f
			changed.policy.Units = maps.Clone(f.policy.Units)
			tc.edit(&changed.policy)
			invalid, err := BuildPlan(changed.policy, f.model, f.packages, ProfileCore, "hostile release-rest composition", "same-source", BuildOptions{Venue: VenueCI, ExtraUnits: []string{releaseRestUnit}})
			if err == nil {
				err = BindExecution(&invalid, f.inventory, f.proofs, changed.policy)
			}
			if err == nil {
				t.Fatal("missing, extra or changed boundary ownership was accepted")
			}
		})
	}
}
