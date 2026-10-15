package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

const humanTaskExpiryPriorObservation = `							rt, owner = restart()
							recovered, err := owner.GetDecisionCard(servedControlProofAuthorActivityContext(t, rt), id)
`

// #2672/6191459119 and6191468991 authorize only a read-side join in the
// original startup budget. All historical setup and final oracles stay pinned.
const humanTaskExpiryBoundedObservation = `							restartDeadline := time.Now().Add(serveRuntimeReadyTimeout)
							rt, owner = restart()
							expiryCtx, cancelExpiry := context.WithDeadline(servedControlProofAuthorActivityContext(t, rt), restartDeadline)
							defer cancelExpiry()
							recovered, err := waitHumanTaskExpiryCommit(expiryCtx, card, func(ctx context.Context) (decisioncard.Card, error) {
								return owner.GetDecisionCard(ctx, id)
							})
`

func validateHumanTaskExpirySuccessor(row recipe, actual string) error {
	if row.Family != "native-served-delivery-quiescence-caller" || row.File != "internal/serveapp/mailbox_human_topology_test.go" ||
		row.Function != "TestHumanTaskRealRequesterTopologyBothStores" || row.Removed || row.Mechanical != nil ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(row.Before))) != "33917f16661b87e196c39a90be34f1d2bbbc82762132af0ec98eeb8abe59bb24" ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(row.After))) != "b19dbf7580d8be01269d0af6013be4582cd4b214b98a9c5e4a919ac85a7efdf7" ||
		strings.Count(actual, humanTaskExpiryBoundedObservation) != 1 {
		return fmt.Errorf("human-task expiry successor changed its exact observation or historical evidence")
	}
	restored, reverseErr := canonicalFunction(strings.Replace(actual, humanTaskExpiryBoundedObservation, humanTaskExpiryPriorObservation, 1))
	prior, priorErr := canonicalFunction(row.After)
	current, currentErr := canonicalFunction(row.Successor)
	got, actualErr := canonicalFunction(actual)
	if reverseErr != nil || priorErr != nil || currentErr != nil || actualErr != nil || restored != prior || got != current {
		return fmt.Errorf("human-task expiry change exceeds its bounded canonical-card observation")
	}
	return nil
}

func TestHumanTaskExpirySuccessorPreservesEveryOriginalOracle(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Function != "TestHumanTaskRealRequesterTopologyBothStores" {
			continue
		}
		matched++
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		row, actual = requireCatchUp2672MasterPair(t, row, actual)
		if err := validateHumanTaskExpirySuccessor(row, actual); err != nil {
			t.Fatal(err)
		}
		for name, changed := range map[string]string{
			"old_immediate_read": strings.Replace(actual, humanTaskExpiryBoundedObservation, humanTaskExpiryPriorObservation, 1),
			"duplicate_window":   strings.Replace(actual, humanTaskExpiryBoundedObservation, humanTaskExpiryBoundedObservation+humanTaskExpiryBoundedObservation, 1),
			"fresh_post_ready_window": strings.Replace(actual,
				"restartDeadline := time.Now().Add(serveRuntimeReadyTimeout)\n\t\t\t\t\t\t\trt, owner = restart()",
				"rt, owner = restart()\n\t\t\t\t\t\t\trestartDeadline := time.Now().Add(serveRuntimeReadyTimeout)", 1),
			"extra_budget":      strings.Replace(actual, "time.Now().Add(serveRuntimeReadyTimeout)", "time.Now().Add(2*serveRuntimeReadyTimeout)", 1),
			"wrong_card":        strings.Replace(actual, "owner.GetDecisionCard(ctx, id)", "owner.GetDecisionCard(ctx, card.RunID)", 1),
			"authored_deadline": strings.Replace(actual, "Add(2 * time.Second)", "Add(3 * time.Second)", 1),
			"expired_oracle":    strings.Replace(actual, "recovered.Status != decisioncard.StatusExpired", "recovered.Status != decisioncard.StatusPending", 1),
			"anchor_oracle":     strings.Replace(actual, "!reflect.DeepEqual(recovered.Anchor, card.Anchor)", "false", 1),
			"delivery_count":    strings.Replace(actual, "if count != 1 {", "if count < 1 {", 1),
		} {
			t.Run(name, func(t *testing.T) {
				if changed == actual {
					t.Fatal("negative control did not change source")
				}
				candidate := row
				candidate.Successor = changed
				if validateHumanTaskExpirySuccessor(candidate, changed) == nil {
					t.Fatal("matching changed source and snapshot accepted")
				}
			})
		}
		changed := row
		changed.After = row.Successor
		if validateHumanTaskExpirySuccessor(changed, actual) == nil {
			t.Fatal("historical endpoint replacement accepted")
		}
	}
	if matched != 1 {
		t.Fatalf("human-task expiry recipe count=%d, want1", matched)
	}
}
