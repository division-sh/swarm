package testplanning

import (
	"path/filepath"
	"reflect"
	"regexp"
	"testing"
)

const originalServeappLateSelection = `^(Test([J-LN-Q].*|ChannelOnboardingPendingResetRestartToReadyE2E|Issue2394(Held.*|Served(One.*|F.*|S.*)|Stop.*|Selected.*)|Issue2564(DecisionCard.*|Served(H3.*|M33.*))|Issue2566ReporterFiniteFixtureCanCloseBothStores|Inbound.*|Initialize.*|Internal.*|M($|[^a].*)|Ma[^i].*|ServedPublicationDirect.*)|Example.*|Fuzz.*)$`

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
			Run:      `^(Test([J-LNOQ].*|P($|[^r].*|r($|[^o].*|o($|[^v].*)))|Issue2394(Held.*|Served(F.*|S.*)|Stop.*|Selected.*)|Issue2564(DecisionCard.*|Served(H3.*|M33.*))|Issue2566ReporterFiniteFixtureCanCloseBothStores|Inbound.*|Initialize.*|Internal.*|M($|[^a].*)|Ma[^i].*|ServedPublicationDirect.*)|Example.*|Fuzz.*)$`,
			RequiredChildren: map[string][]string{
				"TestPublicPublicationCompletionBoundaryBothStores":                    {"default_sqlite", "explicit_postgres"},
				"TestPublicCompletionCapacityServedBothStores":                         {"default_sqlite", "explicit_postgres"},
				"TestPublicPublicationCompletionExactBytesBothStores":                  {"default_sqlite", "explicit_postgres"},
				"TestPublicFeedOnlyCompletionUnloadRestartBothStores":                  {"default_sqlite", "explicit_postgres"},
				"TestPublicPublicationRetirementRecoveryBothStores":                    {"default_sqlite", "explicit_postgres"},
				"TestPublicPublicationSixHubOriginalDeadlineBothStores":                {"sqlite", "postgres"},
				"TestPublicPublicationCreatingTurnOrderBothStores":                     {"sqlite", "postgres"},
				"TestPublicPublicationCreatingTurnRestartBothStores":                   {"sqlite", "postgres"},
				"TestPublicPublicationInvalidAcknowledgedResultBothStores":             {"default_sqlite/completion", "default_sqlite/publication", "explicit_postgres/completion", "explicit_postgres/publication"},
				"TestPublicFeedOnlyInvalidAcknowledgedResultBothStores":                {"default_sqlite", "explicit_postgres"},
				"TestPublicCompletionPreAckRetirementBothStores":                       {"default_sqlite/event.publish", "default_sqlite/run.start", "explicit_postgres/event.publish", "explicit_postgres/run.start"},
				"TestPublicCompletionNativePreAckRetirementBothStores":                 {"default_sqlite/event.publish", "default_sqlite/run.start", "explicit_postgres/event.publish", "explicit_postgres/run.start"},
				"TestPublicCompletionAcknowledgedRetirementBothStores":                 {"default_sqlite/event.publish", "default_sqlite/run.start", "explicit_postgres/event.publish", "explicit_postgres/run.start"},
				"TestPublicCompletionAcknowledgedShutdownBeforeFinalizationBothStores": {"default_sqlite/event.publish", "default_sqlite/run.start", "explicit_postgres/event.publish", "explicit_postgres/run.start"},
				"TestPublicCompletionAcknowledgedDefaultAbandonmentBothStores":         {"default_sqlite/event.publish", "default_sqlite/run.start/event-backed", "default_sqlite/run.start/feed-only", "explicit_postgres/event.publish", "explicit_postgres/run.start/event-backed", "explicit_postgres/run.start/feed-only"},
				"TestPublicCompletionAcknowledgedFenceRecoveryBothStores":              {"default_sqlite/event.publish", "default_sqlite/run.start/event-backed", "default_sqlite/run.start/feed-only", "explicit_postgres/event.publish", "explicit_postgres/run.start/event-backed", "explicit_postgres/run.start/feed-only"},
				"TestPublicCompletionManagedCommitCutBothStores":                       {"default_sqlite/bundle/event.publish", "default_sqlite/bundle/run.start/event-backed", "default_sqlite/bundle/run.start/feed-only", "default_sqlite/supervisor/event.publish", "default_sqlite/supervisor/run.start/event-backed", "default_sqlite/supervisor/run.start/feed-only", "explicit_postgres/bundle/event.publish", "explicit_postgres/bundle/run.start/event-backed", "explicit_postgres/bundle/run.start/feed-only", "explicit_postgres/supervisor/event.publish", "explicit_postgres/supervisor/run.start/event-backed", "explicit_postgres/supervisor/run.start/feed-only"},
				"TestIssue2566ReporterFiniteFixtureCanCloseBothStores":                 {"sqlite", "postgres"},
				"TestManagedEmitPublicationExactScopeBothStores":                       {"default_sqlite/root", "default_sqlite/imported", "default_sqlite/nested", "default_sqlite/template", "explicit_postgres/root", "explicit_postgres/imported", "explicit_postgres/nested", "explicit_postgres/template"},
			},
			CountMode: "count-1", EnvironmentIDs: managedEnvironmentIDs(), BudgetClass: "full",
		},
		"serveapp-delayed-commit-preservation": {
			Packages:         []string{"github.com/division-sh/swarm/internal/serveapp"},
			Run:              `^TestIssue2394ServedOne.*$`,
			RequiredChildren: map[string][]string{"TestIssue2394ServedOneSecondCommitPreservesTwoFullChunksBothStores": {"sqlite", "postgres"}},
			CountMode:        "count-1", EnvironmentIDs: managedEnvironmentIDs(), BudgetClass: "full",
		},
	}
}

func TestServeappLateSplitPreservesOriginalRootPartition(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	old := regexp.MustCompile(originalServeappLateSelection)
	matchers := []func(string) bool{func(name string) bool { return !old.MatchString(name) }}
	for _, id := range []string{"serveapp-other-late", "serveapp-delayed-commit-preservation", "serveapp-channel", "serveapp-channel-neutral"} {
		pattern := regexp.MustCompile(policy.Units[id].Run)
		matchers = append(matchers, func(name string) bool { return old.MatchString(name) && pattern.MatchString(name) })
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
		var err error
		runs, err = reporterSelectedAndDeferredRuns(policy, profile, runs)
		if err != nil {
			t.Fatal(err)
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
