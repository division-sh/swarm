package testplanning

import (
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
)

const serveappBeforeCapacityRun = `^Test(Canonical.*|Channel(Connect.*|Draft.*|Duplicate.*|Effect.*|Listener.*|OneBot.*|Onboarding(Admitted.*|Early.*|E2E.*|Crash.*))|Claude.*|CLI_.*|Composed.*|Configure.*|Connected.*|Create.*|Receiver.*)$`
const serveappBeforeCapacityLateRun = `^(Test([J-LN-Q].*|ChannelOnboardingPendingResetRestartToReadyE2E|Issue2394(Held.*|Served(F.*|S.*)|Stop.*|Selected.*)|Issue2564(DecisionCard.*|Served(H3.*|M33.*))|Issue2566ReporterFiniteFixtureCanCloseBothStores|Inbound.*|Initialize.*|Internal.*|M($|[^a].*)|Ma[^i].*|ServedPublicationDirect.*)|Example.*|Fuzz.*)$`

func serveappCapacityMoves() map[string]string {
	return map[string]string{
		"TestChannelOnboardingPendingResetRestartToReadyE2E":                           "serveapp-channel",
		"TestProviderAliasesDeliverOnlyLocalAndConnectedConsumersBothStores":           "serveapp-channel-neutral",
		"TestProviderSelectedRootStandingBootBothStores":                               "serveapp-channel-neutral",
		"TestProviderAliasPhysicalTupleProofPreservesScopeNullsAndMultiplicity":        "serveapp-channel-neutral",
		"TestProviderIngressProofRoleRegistryRejectsHiddenAuthorityFromSupportedProof": "serveapp-channel-neutral",
		"TestProviderRegistrationRotationCannotRefreshRuntimeIngressAdmission":         "serveapp-channel-neutral",
		"TestProviderRestartRecoveryBothStores":                                        "serveapp-channel-neutral",
		"TestProviderRestartRecoveryProcess":                                           "serveapp-channel-neutral",
	}
}

func TestServeappCapacityRebalancePreservesActualPlans(t *testing.T) {
	policy, inventory, parity, packages := loadVerifyRunPlanInputs(t)
	file, err := os.Open(filepath.Join("..", "..", GeneratedWeightModelPath))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	model, err := LoadWeightModel(file)
	if err != nil {
		t.Fatal(err)
	}
	original := serveappBeforeCapacityPolicy(policy)
	for _, venue := range []string{VenueCI, VenueLocal} {
		for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
			t.Run(venue+"/"+tier, func(t *testing.T) {
				plans := make([]RunPlan, 2)
				for i, selected := range []Policy{original, policy} {
					plan, err := BuildPlan(selected, model, packages, tier, "served capacity rebalance", "same-source", BuildOptions{Venue: venue})
					if err == nil {
						err = BindExecution(&plan, inventory, parity, selected)
					}
					if err != nil {
						t.Fatal(err)
					}
					plans[i] = plan
				}
				assertServeappCapacityPlans(t, plans[0], plans[1])
				t.Logf("%s/%s: complete root union and sibling recipes conserved; plan digests %s -> %s", venue, tier, plans[0].Digest, plans[1].Digest)
			})
		}
	}
}

func TestServeappCapacityRebalanceCoreSupplementsRefuseCanaryOverlap(t *testing.T) {
	policy, inventory, parity, packages := loadVerifyRunPlanInputs(t)
	model := WeightModel{Version: WeightModelVersion, SourceRunID: "served-capacity-core-overlap"}
	extras := []string{"serveapp-channel", "serveapp-channel-neutral", "serveapp-other-late"}
	plan, err := BuildPlan(policy, model, packages, ProfileCore, "served capacity supplemental selection", "same-source", BuildOptions{Venue: VenueCI, ExtraUnits: extras})
	if err == nil {
		err = BindExecution(&plan, inventory, parity, policy)
	}
	if err == nil || !strings.Contains(err.Error(), "2 primary owners") {
		t.Fatalf("overlapping core canaries and served supplements must refuse: %v", err)
	}
}

func serveappBeforeCapacityPolicy(policy Policy) Policy {
	original := policy
	original.Units = maps.Clone(policy.Units)
	for id, run := range map[string]string{
		"serveapp-channel":         serveappBeforeCapacityRun,
		"serveapp-channel-neutral": `^TestChannel(TextReply.*|OptionalCapabilities.*|CollidingCaption.*|PageOracle.*)$`,
		"serveapp-other-late":      serveappBeforeCapacityLateRun,
	} {
		unit := original.Units[id]
		unit.Run = run
		unit.RequiredChildren = maps.Clone(unit.RequiredChildren)
		if id == "serveapp-channel-neutral" {
			delete(unit.RequiredChildren, "TestProviderSelectedRootStandingBootBothStores")
			if len(unit.RequiredChildren) == 0 {
				unit.RequiredChildren = nil
			}
		}
		if id == "serveapp-other-late" {
			unit.RequiredChildren["TestProviderSelectedRootStandingBootBothStores"] = []string{"default_sqlite", "explicit_postgres"}
		}
		original.Units[id] = unit
	}
	return original
}

func assertServeappCapacityPlans(t *testing.T, before, after RunPlan) {
	t.Helper()
	oldOwners, newOwners := conformancePlanOwners(before), conformancePlanOwners(after)
	if before.Profile == ProfileCore {
		if !reflect.DeepEqual(before.Units, after.Units) {
			t.Fatal("core selection or recipe changed")
		}
	} else {
		for name, owner := range serveappCapacityMoves() {
			root := TestRoot{Package: ServedReporterPackage, Name: name}
			if !slices.Equal(oldOwners[root], []string{"serveapp-other-late"}) || !slices.Equal(newOwners[root], []string{owner}) {
				t.Fatalf("%s must move exactly once: %v -> %v, want %s", name, oldOwners[root], newOwners[root], owner)
			}
			old, _ := before.Unit("serveapp-other-late")
			current, _ := after.Unit(owner)
			if !reflect.DeepEqual(serveappCapacityObligation(old, name), serveappCapacityObligation(current, name)) || serveappCapacityObligation(current, name).Name == "" {
				t.Fatalf("%s lost its complete required-summary obligation", name)
			}
			delete(oldOwners, root)
			delete(newOwners, root)
		}
	}
	if !reflect.DeepEqual(oldOwners, newOwners) || !slices.Equal(before.Packages, after.Packages) || len(before.Units) != len(after.Units) {
		t.Fatal("root/Example/Fuzz union or another root's owner changed")
	}
	for _, old := range before.Units {
		current, err := after.Unit(old.ID)
		if err != nil {
			t.Fatal(err)
		}
		switch old.ID {
		case "serveapp-channel", "serveapp-channel-neutral", "serveapp-other-late":
			old, current = serveappWithoutCapacityMoves(old), serveappWithoutCapacityMoves(current)
		}
		if !reflect.DeepEqual(old, current) {
			t.Fatalf("%s changed sibling obligations, recipe, weight or limits", old.ID)
		}
	}
}

func serveappCapacityObligation(unit ProofUnit, name string) RequiredTest {
	for _, required := range unit.RequiredTests {
		if required.Name == name {
			return required
		}
	}
	return RequiredTest{}
}

func serveappWithoutCapacityMoves(unit ProofUnit) ProofUnit {
	moves := serveappCapacityMoves()
	unit.SelectedRoots = slices.DeleteFunc(slices.Clone(unit.SelectedRoots), func(root TestRoot) bool { _, ok := moves[root.Name]; return ok })
	unit.RequiredTests = slices.DeleteFunc(slices.Clone(unit.RequiredTests), func(root RequiredTest) bool { _, ok := moves[root.Name]; return ok })
	unit.RequiredChildren = maps.Clone(unit.RequiredChildren)
	delete(unit.RequiredChildren, "TestProviderSelectedRootStandingBootBothStores")
	if len(unit.RequiredChildren) == 0 {
		unit.RequiredChildren = nil
	}
	unit.Run = ""
	// Selector changes invalidate historical recipe weights; the normal planner
	// falls back until the validated timing publisher measures the new recipe.
	unit.WeightSeconds = 0
	return unit
}

func TestServeappCapacityRebalanceRejectsMissingOrDuplicateOwners(t *testing.T) {
	policy, inventory, parity, packages := loadVerifyRunPlanInputs(t)
	model := WeightModel{Version: WeightModelVersion, SourceRunID: "served-capacity-negative-controls"}
	for _, tier := range []string{ProfileLifecycle, ProfileFull} {
		for name, owner := range serveappCapacityMoves() {
			for _, defect := range []string{"missing", "duplicate"} {
				t.Run(tier+"/"+name+"/"+defect, func(t *testing.T) {
					changed := policy
					changed.Units = maps.Clone(policy.Units)
					id := owner
					if defect == "duplicate" {
						id = "serveapp-other-late"
					}
					unit := changed.Units[id]
					unit.Run = serveappCapacityDefectiveRun(unit.Run, inventory.Packages[ServedReporterPackage].Roots, name, defect)
					changed.Units[id] = unit
					plan, err := BuildPlan(changed, model, packages, tier, "invalid served capacity ownership", "same-source")
					if err == nil {
						err = BindExecution(&plan, inventory, parity, changed)
					}
					if err == nil || !strings.Contains(err.Error(), name) {
						t.Fatalf("%s owner was not refused at the actual plan boundary: %v", defect, err)
					}
				})
			}
		}
	}
}

func serveappCapacityDefectiveRun(run string, roots []string, omitted, defect string) string {
	if defect == "duplicate" {
		return "(?:" + run + ")|^" + omitted + "$"
	}
	pattern := regexp.MustCompile(run)
	var remaining []string
	for _, name := range roots {
		if name != omitted && pattern.MatchString(name) {
			remaining = append(remaining, regexp.QuoteMeta(name))
		}
	}
	return "^(" + strings.Join(remaining, "|") + ")$"
}

func TestServeappCapacityRebalanceNearPrefixesUseActualPlans(t *testing.T) {
	policy, inventory, parity, packages := loadVerifyRunPlanInputs(t)
	model := WeightModel{Version: WeightModelVersion, SourceRunID: "served-capacity-prefix-controls"}
	pkg := inventory.Packages[ServedReporterPackage]
	complement := []string{"TestP", "TestPr", "TestPro", "TestPublicBoundary", "TestPrivateBoundary", "TestPostgresBoundary", "TestProcessBoundary", "TestProspectiveBoundary"}
	providers := []string{"TestProv", "TestProviderBoundary"}
	pkg.Roots = append(slices.Clone(pkg.Roots), append(slices.Clone(complement), providers...)...)
	slices.Sort(pkg.Roots)
	inventory.Packages[ServedReporterPackage] = pkg
	for _, venue := range []string{VenueCI, VenueLocal} {
		for _, tier := range []string{ProfileLifecycle, ProfileFull} {
			t.Run(venue+"/"+tier, func(t *testing.T) {
				plan, err := BuildPlan(policy, model, packages, tier, "served prefix boundaries", "same-source", BuildOptions{Venue: venue})
				if err == nil {
					err = BindExecution(&plan, inventory, parity, policy)
				}
				if err != nil {
					t.Fatal(err)
				}
				owners := conformancePlanOwners(plan)
				for owner, names := range map[string][]string{"serveapp-other-late": complement, "serveapp-channel-neutral": providers} {
					for _, name := range names {
						if got := owners[TestRoot{Package: ServedReporterPackage, Name: name}]; !slices.Equal(got, []string{owner}) {
							t.Errorf("%s owners=%v, want exactly %s", name, got, owner)
						}
					}
				}
			})
		}
	}
}
