package testplanning

import (
	"reflect"
	"regexp"
	"slices"
	"testing"
)

func TestIssue642ServedContinuationProofsHaveOneLifecycleOwner(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	const owner = "serveapp-selected-rest"
	unit := policy.Units[owner]
	if unit.Skip != "" || unit.CountMode != "count-1" || unit.GoTimeout != "" {
		t.Fatal("selected continuation proof envelope changed")
	}
	for _, tier := range []string{ProfileLifecycle, ProfileFull} {
		if !slices.Contains(policy.Profiles[tier].Units, owner) {
			t.Fatalf("%s omits selected continuation owner", tier)
		}
		for root, children := range map[string][]string{
			"TestIssue642OrdinaryTimerForkContinuationBothStores":                 {"sqlite", "postgres"},
			"TestIssue642ArmedTimerKeepsDueAcrossSelectedDelayChangeBothStores":   {"sqlite/1ms", "sqlite/1h", "postgres/1ms", "postgres/1h"},
			"TestIssue642OrdinaryTimerStagedActivationBothStores":                 {"sqlite", "postgres"},
			"TestIssue642RecurringTimerForkPreservesDueArmAndCadenceBothStores":   {"sqlite", "postgres"},
			"TestIssue642RemovedArmedTimerCancelsWithoutPhantomFireBothStores":    {"sqlite/non_final", "sqlite/selected_final", "postgres/non_final", "postgres/selected_final"},
			"TestIssue642NewlyArmedTimerUsesSelectedDelayAndEffectBothStores":     {"sqlite/25ms", "sqlite/1500ms", "postgres/25ms", "postgres/1500ms"},
			"TestIssue642StagedTimerCorruptionRefusesBeforeActivationBothStores":  {"sqlite/missing", "sqlite/progressed", "sqlite/foreign_origin", "postgres/missing", "postgres/progressed", "postgres/foreign_origin"},
			"TestIssue642SustainedRecurringTimerForkExecutesTwoEffectsBothStores": {"sqlite", "postgres"},
		} {
			var owners []string
			for _, id := range policy.Profiles[tier].Units {
				candidate := policy.Units[id]
				if slices.Contains(candidate.Packages, policy.Module+"/internal/serveapp") && regexp.MustCompile(candidate.Run).MatchString(root) {
					owners = append(owners, id)
				}
			}
			if !reflect.DeepEqual(owners, []string{owner}) || !slices.Equal(unit.RequiredChildren[root], children) {
				t.Errorf("%s proof %s: owners=%v children=%v, want exact one owner and %v", tier, root, owners, unit.RequiredChildren[root], children)
			}
		}
	}
}
