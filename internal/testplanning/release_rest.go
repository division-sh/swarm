package testplanning

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
)

const (
	releaseRestUnit      = "hitl-releasee2e-rest"
	structuralUnit       = "core-structural-owner-guards"
	structuralComplement = "core-structural-owner-guards-without-release"
	releaseProofPackage  = "github.com/division-sh/swarm/internal/releasee2e"
)

func composeReleaseRest(profile, venue string, extras []string) bool {
	return profile == ProfileCore && venue == VenueCI && slices.Contains(extras, releaseRestUnit)
}

func (p Policy) composeReleaseRestCanaries(ids []string) ([]string, error) {
	if !slices.Contains(ids, structuralUnit) {
		return nil, fmt.Errorf("release-rest composition requires its original structural owner")
	}
	if _, ok := p.Units[structuralComplement]; !ok {
		return nil, fmt.Errorf("release-rest composition requires declared unit %s", structuralComplement)
	}
	composed := slices.Clone(ids)
	composed[slices.Index(composed, structuralUnit)] = structuralComplement
	return composed, nil
}

func bindReleaseRestComposition(plan *RunPlan, full RunPlan, inventory RootInventory, parity []ParityProof, policy Policy) error {
	if !composeReleaseRest(plan.Profile, plan.Venue, plan.ExtraUnits) {
		return nil
	}
	extras := slices.DeleteFunc(slices.Clone(plan.ExtraUnits), func(id string) bool { return id == releaseRestUnit })
	var packages []string
	for pkg := range inventory.Packages {
		packages = append(packages, pkg)
	}
	model := WeightModel{Version: WeightModelVersion, SourceRunID: "release-rest-composition-census"}
	base, err := BuildPlan(policy, model, packages, ProfileCore, "release-rest composition base", plan.HeadSHA, BuildOptions{Venue: VenueCI, ExtraUnits: extras})
	if err == nil {
		err = BindExecution(&base, inventory, parity, policy)
	}
	if err != nil {
		return fmt.Errorf("release-rest composition base: %w", err)
	}
	release, err := full.Unit(releaseRestUnit)
	if err != nil {
		return err
	}
	transfers := map[TestRoot]bool{}
	for _, name := range []string{"TestReleaseE2ECapacityFixtureImportBoundary", "TestReleaseE2EImportBoundaryRejectsRuntimeAndUnscopedPlanning", "TestReleaseE2EPackageStaysAtPublicProcessBoundary"} {
		transfers[TestRoot{Package: releaseProofPackage, Name: name}] = false
	}
	for _, unit := range base.Units {
		if strings.HasPrefix(unit.ID, "broad-") {
			continue
		}
		if unit.ID == structuralUnit {
			unit.ID = structuralComplement
			unit.Packages = slices.DeleteFunc(slices.Clone(unit.Packages), func(pkg string) bool { return pkg == releaseProofPackage })
			unit.TestBearingPackages = slices.DeleteFunc(slices.Clone(unit.TestBearingPackages), func(pkg string) bool { return pkg == releaseProofPackage })
			unit.SelectedRoots = slices.DeleteFunc(slices.Clone(unit.SelectedRoots), func(root TestRoot) bool {
				if _, approved := transfers[root]; approved {
					transfers[root] = true
					return true
				}
				return false
			})
			unit.RequiredTests = slices.DeleteFunc(slices.Clone(unit.RequiredTests), func(proof RequiredTest) bool { return transfers[proof.TestRoot] })
		}
		actual, err := plan.Unit(unit.ID)
		unit.WeightSeconds, actual.WeightSeconds = 0, 0
		if err != nil || !reflect.DeepEqual(unit, actual) {
			return fmt.Errorf("release-rest composition changed roots, children or recipe of %s", unit.ID)
		}
	}
	for root, transferred := range transfers {
		if !transferred || !slices.Contains(release.SelectedRoots, root) {
			return fmt.Errorf("release-rest composition must transfer exactly its three approved boundary proofs")
		}
	}
	wantedRequired, wantedDeferred := channelProofUnion(append(slices.Clone(base.Units), release))
	actualRequired, actualDeferred := channelProofUnion(plan.Units)
	if !reflect.DeepEqual(wantedRequired, actualRequired) || !reflect.DeepEqual(wantedDeferred, actualDeferred) {
		return fmt.Errorf("release-rest composition changed the core/release root and child obligation union")
	}
	return nil
}
