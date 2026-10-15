package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// Preserve the complete prior successor as well as Before/After. Q2's diagnostic
// exclusion requires real business history for the existing unfrozen branch.
//
//go:embed testdata/receiver_fork_business_predecessor.json
var receiverForkBusinessPredecessorBytes []byte

const receiverForkBusinessSetup = `				forkCtx := servedControlProofAuthorActivityContext(t, rt)
				if surface == "admitted_http" {
					// Branch isolation needs later business history, not excluded runtime logs.
					if rt.SQLite != nil {
						advanceLifecycleForkSourceHistory(t, forkCtx, rt.SQLite, request.RunID)
					} else {
						advanceLifecycleForkSourceHistory(t, forkCtx, rt.Postgres, request.RunID)
					}
				}
`

func receiverForkBusinessPredecessor(row recipe, actual string) (recipe, string, error) {
	var prior string
	if err := json.Unmarshal(receiverForkBusinessPredecessorBytes, &prior); err != nil {
		return recipe{}, "", err
	}
	if row.Family != "native-served-delivery-quiescence-caller" || row.File != "internal/serveapp/receiver_fork_test.go" ||
		row.Function != "TestReceiverCompositionForkBothStores" || row.Removed || row.Mechanical != nil ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(row.Before))) != "aec7c2b55b37e0cfb14e198ede16302cfd22200569cc1694abbc4300fafb0d33" ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(row.After))) != "fecc19b2cd332c799fb77740e3e4e9c0c857810022536660b0f188d1b87791e6" ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(prior))) != "cad3f95bf7b7218845b2b32b1725bae7c69708d32edc90410ba7b86f8436b8c1" {
		return recipe{}, "", fmt.Errorf("receiver-fork business setup changed historical evidence")
	}
	const currentCall = "family.Execute(forkCtx, runtimerunforkexecution.SelectedContractExecutionRequest{"
	const priorCall = "family.Execute(servedControlProofAuthorActivityContext(t, rt), runtimerunforkexecution.SelectedContractExecutionRequest{"
	if strings.Count(actual, receiverForkBusinessSetup) != 1 || strings.Count(actual, currentCall) != 1 {
		return recipe{}, "", fmt.Errorf("receiver-fork business setup or original execution context differs")
	}
	restored := strings.Replace(actual, receiverForkBusinessSetup, "", 1)
	restored = strings.Replace(restored, currentCall, priorCall, 1)
	got, actualErr := canonicalFunction(actual)
	want, currentErr := canonicalFunction(row.Successor)
	old, oldErr := canonicalFunction(prior)
	reversed, reverseErr := canonicalFunction(restored)
	if actualErr != nil || currentErr != nil || oldErr != nil || reverseErr != nil || got != want || reversed != old {
		return recipe{}, "", fmt.Errorf("receiver-fork change exceeds exact business setup and context reuse")
	}
	row.Successor = prior
	return row, restored, nil
}

func TestReceiverForkBusinessSetupPreservesAllHistoricalAssertions(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Function != "TestReceiverCompositionForkBothStores" {
			continue
		}
		matched++
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		if err := validateReceiverForkMilestoneSuccessor(row, actual); err != nil {
			t.Fatal(err)
		}
		for name, changed := range map[string]string{
			"missing_setup":   strings.Replace(actual, receiverForkBusinessSetup, "", 1),
			"duplicate_setup": strings.Replace(actual, receiverForkBusinessSetup, receiverForkBusinessSetup+receiverForkBusinessSetup, 1),
			"wrong_surface":   strings.Replace(actual, receiverForkBusinessSetup, strings.Replace(receiverForkBusinessSetup, `surface == "admitted_http"`, `surface == "admitted"`, 1), 1),
			"wrong_run":       strings.Replace(actual, receiverForkBusinessSetup, strings.ReplaceAll(receiverForkBusinessSetup, "request.RunID", "seedEventID"), 1),
			"wrong_store":     strings.Replace(actual, receiverForkBusinessSetup, strings.Replace(receiverForkBusinessSetup, "forkCtx, rt.SQLite", "forkCtx, rt.Postgres", 1), 1),
			"source_freeze":   strings.Replace(actual, `fork.SourceRunStatus != "paused"`, `fork.SourceRunStatus != "forked"`, 1),
			"receiver_count":  strings.Replace(actual, "fork.ExecutedEventCount != 1", "fork.ExecutedEventCount != 0", 1),
			"missing_refusal": strings.Replace(actual, "source_committed_replay_scope_advanced_after_fork_point", "unreviewed_refusal", 1),
			"context_changed": strings.Replace(actual, "family.Execute(forkCtx,", "family.Execute(context.Background(),", 1),
		} {
			t.Run(name, func(t *testing.T) {
				if changed == actual {
					t.Fatal("negative control did not change source")
				}
				candidate := row
				candidate.Successor = changed
				if validateReceiverForkMilestoneSuccessor(candidate, changed) == nil {
					t.Fatal("matching changed source and successor accepted")
				}
			})
		}
		changed := row
		changed.After = row.Successor
		if validateReceiverForkMilestoneSuccessor(changed, actual) == nil {
			t.Fatal("historical endpoint replacement accepted")
		}
	}
	if matched != 1 {
		t.Fatalf("receiver-fork recipe count=%d, want1", matched)
	}
}
