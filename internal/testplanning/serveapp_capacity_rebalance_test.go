package testplanning

import (
	"maps"

	"regexp"
	"slices"
	"strings"
	"testing"
)

func serveappCapacityMoves() map[string]string {
	return map[string]string{
		"TestChannelOnboardingPendingResetRestartToReadyE2E":                           "serveapp-channel",
		"TestProviderAliasesDeliverOnlyLocalAndConnectedConsumersBothStores":           "serveapp-channel-neutral",
		"TestProviderSelectedRootStandingBootBothStores":                               "serveapp-channel-neutral",
		"TestProviderAliasPhysicalTupleProofPreservesScopeNullsAndMultiplicity":        "serveapp-channel-neutral",
		"TestProviderIngressProofRoleRegistryRejectsHiddenAuthorityFromSupportedProof": "serveapp-channel-neutral",
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
