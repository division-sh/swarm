package testplanning

import (
	"path/filepath"
	"reflect"
	"regexp"
	"testing"
)

const originalChannelLearnedSelection = `^TestChannel(Learned.*|TextReply.*|OptionalCapabilities.*|CollidingCaption.*|PageOracle.*)$`

func TestChannelNeutralSplitRetainsCompleteProofEnvelopes(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	for id, run := range map[string]string{
		"serveapp-channel-learned": `^TestChannelLearned.*$`,
		"serveapp-channel-neutral": `^TestChannel(TextReply.*|OptionalCapabilities.*|CollidingCaption.*|PageOracle.*)$`,
	} {
		want := UnitPolicy{
			Packages: []string{policy.Module + "/internal/serveapp"}, Run: run,
			CountMode: "count-1", EnvironmentIDs: managedEnvironmentIDs(), BudgetClass: "full",
		}
		if !reflect.DeepEqual(policy.Units[id], want) {
			t.Errorf("%s changed complete-root/backend/count/environment/budget/timeout envelope: %+v", id, policy.Units[id])
		}
		assertServeappLateProfileMembership(t, policy, id)
	}
}

func TestChannelNeutralSplitPreservesOriginalRootPartition(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	if err := validateChannelNeutralSplit(policy); err != nil {
		t.Fatal(err)
	}
}

func validateChannelNeutralSplit(policy Policy) error {
	old := regexp.MustCompile(originalChannelLearnedSelection)
	matchers := []func(string) bool{func(name string) bool { return !old.MatchString(name) }}
	for _, id := range []string{"serveapp-channel-learned", "serveapp-channel-neutral"} {
		matchers = append(matchers, regexp.MustCompile(policy.Units[id].Run).MatchString)
	}
	return validateGoProofMatchers(filepath.Join("..", "..", "internal", "serveapp"), matchers)
}

func TestChannelNeutralSplitRejectsOmissionOverlapAndBackendFiltering(t *testing.T) {
	for _, run := range []string{`^$`, originalChannelLearnedSelection, `^TestChannelTextReplyBaselinePublicJourneyBothStores$/sqlite`, `^TestChannel.*$`} {
		t.Run(run, func(t *testing.T) {
			policy := loadPersistenceDebtPolicy(t)
			unit := policy.Units["serveapp-channel-neutral"]
			unit.Run = run
			policy.Units["serveapp-channel-neutral"] = unit
			if validateChannelNeutralSplit(policy) == nil {
				t.Fatal("changed original complete-root ownership was accepted")
			}
		})
	}
}
