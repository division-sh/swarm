package testplanning

import (
	"path/filepath"
	"reflect"
	"regexp"
	"testing"
)

const originalServeappLateSelection = `^(Test([J-LN-Q].*|ChannelOnboardingPendingResetRestartToReadyE2E|Issue2394(Held.*|Served(One.*|F.*|S.*)|Stop.*|Selected.*)|Issue2566ReporterFiniteFixtureCanCloseBothStores|Inbound.*|Initialize.*|Internal.*|M($|[^a].*)|Ma[^i].*|ServedPublicationDirect.*)|Example.*|Fuzz.*)$`

func TestServeappLateSplitRetainsProofEnvelopes(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	for id, want := range serveappLateSplitEnvelopes() {
		if !reflect.DeepEqual(policy.Units[id], want) {
			t.Errorf("%s changed complete-root/backend/count/environment/budget/timeout envelope: %+v", id, policy.Units[id])
		}
		assertServeappLateProfileMembership(t, policy, id)
	}
}

func assertServeappLateProfileMembership(t *testing.T, policy Policy, id string) {
	t.Helper()
	for _, profile := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
		want := 1
		if profile == ProfileCore {
			want = 0
		}
		count := 0
		for _, member := range policy.Profiles[profile].Units {
			if member == id {
				count++
			}
		}
		if count != want {
			t.Errorf("%s scheduled %d times in %s, want %d", id, count, profile, want)
		}
	}
}

func serveappLateSplitEnvelopes() map[string]UnitPolicy {
	return map[string]UnitPolicy{
		"serveapp-other-late": {
			Packages: []string{"github.com/division-sh/swarm/internal/serveapp"},
			Run:      `^(Test([J-LN-Q].*|ChannelOnboardingPendingResetRestartToReadyE2E|Issue2394(Held.*|Served(F.*|S.*)|Stop.*|Selected.*)|Issue2566ReporterFiniteFixtureCanCloseBothStores|Inbound.*|Initialize.*|Internal.*|M($|[^a].*)|Ma[^i].*|ServedPublicationDirect.*)|Example.*|Fuzz.*)$`,
			RequiredChildren: map[string][]string{
				"TestProviderSelectedRootStandingBootBothStores":       {"default_sqlite", "explicit_postgres"},
				"TestIssue2566ReporterFiniteFixtureCanCloseBothStores": {"sqlite", "postgres"},
			},
			CountMode: "count-1", EnvironmentID: "ci-postgres-gateway-empty-v1", BudgetClass: "full",
		},
		"serveapp-delayed-commit-preservation": {
			Packages:         []string{"github.com/division-sh/swarm/internal/serveapp"},
			Run:              `^TestIssue2394ServedOne.*$`,
			RequiredChildren: map[string][]string{"TestIssue2394ServedOneSecondCommitPreservesTwoFullChunksBothStores": {"sqlite", "postgres"}},
			CountMode:        "count-1", EnvironmentID: "ci-postgres-gateway-empty-v1", BudgetClass: "full",
		},
	}
}

func TestServeappLateSplitPreservesOriginalRootPartition(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	old := regexp.MustCompile(originalServeappLateSelection)
	matchers := []func(string) bool{func(name string) bool { return !old.MatchString(name) }}
	for _, id := range []string{"serveapp-other-late", "serveapp-delayed-commit-preservation"} {
		matchers = append(matchers, regexp.MustCompile(policy.Units[id].Run).MatchString)
	}
	dir := filepath.Join("..", "..", "internal", "serveapp")
	if err := validateGoProofMatchers(dir, matchers); err != nil {
		t.Fatal(err)
	}
	for _, profile := range []string{ProfileLifecycle, ProfileFull} {
		var runs []string
		for _, id := range policy.Profiles[profile].Units {
			unit := policy.Units[id]
			if reflect.DeepEqual(unit.Packages, []string{policy.Module + "/internal/serveapp"}) {
				runs = append(runs, unit.Run)
			}
		}
		if err := ValidateGoProofPartition(dir, runs); err != nil {
			t.Fatalf("%s whole serveapp partition: %v", profile, err)
		}
	}
}

func TestServeappLateSplitRejectsOmissionAndOverlap(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	old := regexp.MustCompile(originalServeappLateSelection)
	dir := filepath.Join("..", "..", "internal", "serveapp")
	for _, tc := range []struct {
		name string
		runs []string
	}{
		{"omission", []string{policy.Units["serveapp-other-late"].Run}},
		{"overlap", []string{originalServeappLateSelection, policy.Units["serveapp-delayed-commit-preservation"].Run}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			matchers := []func(string) bool{func(name string) bool { return !old.MatchString(name) }}
			for _, run := range tc.runs {
				matchers = append(matchers, regexp.MustCompile(run).MatchString)
			}
			if err := validateGoProofMatchers(dir, matchers); err == nil {
				t.Fatal("changed original root ownership was accepted")
			}
		})
	}
}
