package testplanning

import (
	"fmt"
	"reflect"
	"slices"
	"testing"
)

func validateReporterPartition(policy Policy) error {
	for _, family := range []struct {
		id, pkg, run, timeout, budget string
		roots                         []string
		fullOnly                      bool
	}{
		{"serveapp-i-reporter", ServedReporterPackage, ServedReporterRun, ServedReporterGoTimeout, "full", []string{
			"TestIssue2394ServedOriginalReporterFiveHundredBothStores",
			"TestIssue2394ServedOriginalReporterMeasuredIssuanceBothStores",
			"TestIssue2394ServedReporterTransactionCensusBothStores",
		}, false},
		{"serveapp-i-reporter-all-commit", ServedReporterPackage, ServedReporterStressRun, ServedReporterGoTimeout, "full", []string{"TestIssue2394ServedOriginalReporterFiveHundredDelayedBothStores"}, true},
		{"conformance-2394-reporter", SoakPackage, "^TestIssue2394ReporterFiveHundredDelayedCommitsBothStores$", "", "broad", []string{"TestIssue2394ReporterFiveHundredDelayedCommitsBothStores"}, true},
		{"conformance-2394-reporter-window", SoakPackage, "^TestIssue2394ReporterFiveHundredServingWindowBothStores$", "", "broad", []string{"TestIssue2394ReporterFiveHundredServingWindowBothStores"}, false},
	} {
		unit := policy.Units[family.id]
		if !slices.Equal(unit.Packages, []string{family.pkg}) || unit.Run != family.run || unit.Skip != "" || unit.GoTimeout != family.timeout || unit.CountMode != "count-1" || unit.BudgetClass != family.budget || unit.EnvironmentID != "" || !reflect.DeepEqual(unit.EnvironmentIDs, managedEnvironmentIDs()) || len(unit.RequiredChildren) != len(family.roots) {
			return fmt.Errorf("%s lost its exact complete reporter envelope: %+v", family.id, unit)
		}
		for _, root := range family.roots {
			if !slices.Equal(unit.RequiredChildren[root], []string{"sqlite", "postgres"}) {
				return fmt.Errorf("%s lost a reporter backend: %v", root, unit.RequiredChildren[root])
			}
		}
		for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
			count := 0
			for _, id := range policy.Profiles[tier].Units {
				if id == family.id {
					count++
				}
			}
			want := 1
			if tier == ProfileCore || tier == ProfileLifecycle && family.fullOnly {
				want = 0
			}
			if count != want {
				return fmt.Errorf("%s has %d %s owners, want %d", tier, count, family.id, want)
			}
		}
	}
	return nil
}

func reporterSelectedAndDeferredRuns(policy Policy, profile string, runs []string) ([]string, error) {
	if err := validateReporterPartition(policy); err != nil {
		return nil, err
	}
	if profile == ProfileLifecycle {
		// Census the selected-plus-deferred union without claiming lifecycle
		// execution of the single exact full-only served reporter root.
		runs = append(runs, policy.Units["serveapp-i-reporter-all-commit"].Run)
	}
	return runs, nil
}

func TestReporterPartitionRetainsWholeRootsAndBackends(t *testing.T) {
	policy, _, _ := conformance2394Fixture(t)
	if err := validateReporterPartition(policy); err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"missing root", "overlap", "missing backend", "duplicate backend", "early window", "filtered backend", "cached", "deadline", "missing full", "duplicate full", "stress in lifecycle", "missing normal lifecycle"} {
		t.Run(change, func(t *testing.T) {
			policy, _, _ := conformance2394Fixture(t)
			id := "serveapp-i-reporter-all-commit"
			unit := policy.Units[id]
			switch change {
			case "missing root":
				unit.Run = "^$"
			case "overlap":
				unit.Run = "^TestIssue2394Served.*$"
			case "missing backend":
				unit.RequiredChildren["TestIssue2394ServedOriginalReporterFiveHundredDelayedBothStores"] = []string{"sqlite"}
			case "duplicate backend":
				unit.RequiredChildren["TestIssue2394ServedOriginalReporterFiveHundredDelayedBothStores"] = []string{"sqlite", "sqlite"}
			case "early window":
				unit.Run = "^TestIssue2394ServedOriginalReporterMeasuredIssuanceBothStores$"
			case "filtered backend":
				unit.Run += "/sqlite"
			case "cached":
				unit.CountMode = "cache-default"
			case "deadline":
				unit.GoTimeout = "16m"
			case "missing full", "duplicate full":
				profile := policy.Profiles[ProfileFull]
				if change == "missing full" {
					profile.Units = slices.DeleteFunc(profile.Units, func(member string) bool { return member == id })
				} else {
					profile.Units = append(profile.Units, id)
				}
				policy.Profiles[ProfileFull] = profile
			case "stress in lifecycle", "missing normal lifecycle":
				profile := policy.Profiles[ProfileLifecycle]
				if change == "stress in lifecycle" {
					profile.Units = append(profile.Units, id)
				} else {
					profile.Units = slices.DeleteFunc(profile.Units, func(member string) bool { return member == "serveapp-i-reporter" })
				}
				policy.Profiles[ProfileLifecycle] = profile
			}
			policy.Units[id] = unit
			if err := validateReporterPartition(policy); err == nil {
				t.Fatal("reporter proof mutation was accepted")
			}
		})
	}
}
