package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// #2592/6192759467 replaces the retry-age write with the committed-due wait.
// The old 39 receipts and every business/temporal assertion remain immutable.
func TestQ1RetryDecisionSuccessorPreservesPriorReceiptsAndOracles(t *testing.T) {
	var raw []json.RawMessage
	var pins []fixtureSetupSuccessor2617
	var rows []recipe
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &raw); err != nil || len(raw) != 40 {
		t.Fatalf("fixture packet shape: rows=%d err=%v", len(raw), err)
	}
	prior, err := json.Marshal(raw[:39])
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(prior)) != "daf060dec5ac400e00a35c4a7dea0321a38feeaae8e3bbcb4b20c87cbab0b5b2" {
		t.Fatal("Q1 rewrote earlier fixture evidence")
	}
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &pins); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	pin := pins[39]
	if pin.Original.Family != "native-shared-coordinator-authority" || pin.Original.File != "internal/runtime/node_delivery_startup_recovery_test.go" || pin.Original.Function != "TestPipelineCoordinatorStandingRecoveryClaimsNewlyEligibleNodeDeliveries" || len(pin.Edits) != 2 {
		t.Fatal("Q1 successor changed its exact fixture or finite edit set")
	}
	want := strings.Replace(pin.Original.Successor, "RetryBase: time.Hour", "RetryBase: time.Millisecond", 1)
	want = strings.Replace(want, "storetest.MakeManagerRetryEligible(", "storetest.WaitManagerRetryDecision(", 1)
	if pin.Candidate != want {
		t.Fatal("Q1 successor changed more than its explicit retry setup and canonical due wait")
	}
	matched := 0
	for _, row := range rows {
		if !sameReviewedRecipeKey(row, pin.Original) {
			continue
		}
		matched++
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		if _, err := fixtureSetupCurrentEndpoint2617(row, actual); err != nil {
			t.Fatal(err)
		}
		for label, cut := range map[string][2]string{
			"prior_age_writer": {"storetest.WaitManagerRetryDecision(", "storetest.MakeManagerRetryEligible("},
			"prior_due":        {"time.Millisecond", "time.Hour"},
			"changed_bound":    {"time.Second", "time.Minute"},
			"changed_origin":   {"ctx, selected, event, route", "ctx, selected, expiringEvent, route"},
			"extra_setup":      {"{", "{ unreviewedSetup();"},
		} {
			mutant := row
			source := strings.Replace(actual, cut[0], cut[1], 1)
			if source == actual {
				t.Fatalf("control did not change fixture: %s", label)
			}
			mutant.Successor = source
			if _, err := fixtureSetupCurrentEndpoint2617(mutant, source); err == nil {
				t.Fatalf("matching changed source/recipe admitted: %s", label)
			}
		}
	}
	if matched != 1 {
		t.Fatalf("Q1 recipe cardinality=%d, want1", matched)
	}
}
