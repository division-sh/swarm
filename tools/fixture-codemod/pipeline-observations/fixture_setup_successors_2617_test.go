package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// Append-only fixture evidence under reviewer-c6160153832; prior source
// Source checkpoint daea24400 retains eleven reviewed fixture endpoints;
// checkpoint211293506 adds the exact admitted-parent barrier setup and
// checkpoint4b2c02e90 retains the admitted count-join source through recovery.
// reviewer-c6163224137 adds four exact compiled-mode gate endpoints.
// reviewer-c6164135426 adds the five explicit-parent join endpoints; the
// three admitted-source gate repairs append to their prior Mode receipts.
// reviewer-c6164473807 retains the two exact recovery-port successors; its
// separate dead-letter timing RED remains unqualified.
// reviewer-c6169605343 and6170114891 append only the CAS and sibling-source
// setup successors, retaining their original owner/negative proofs.
// reviewer-c6170011034 and6170480146 retain the five exact A2 source/stage
// setup successors, without changing their prior transformations.
// reviewer-c6172109052 appends the completed proposed-route source successor.
// reviewer-c6173181699 retains the exact admitted keyed-parent tree proof.
// #2287/6148375122 appends the CAS and two join presentation endpoints;
// every prior setup, mechanical receipt and execution assertion is retained.
//
//go:embed testdata/fixture_setup_successors_2617.json
var fixtureSetupSuccessorBytes2617 []byte

const fixtureSetupSuccessorPacketSHA256 = "4fab3ee53d02626879682865f47b64d402442f4cdf08a0cf0873a1e805e50548"

// These settled fixture receipts compose with, but do not replace, the earlier
// historical transformation and current-endpoint packets.
func fixtureSetupCurrentEndpoint2617(row recipe, actual string) (recipe, error) {
	switch row.Family + "/" + row.File + "/" + row.Function {
	case "native-fanout-barrier-authority/internal/store/internal/runtimepersistence/fan_out_delivery_barrier_selected_store_test.go/TestFanOutDeliveryBarrierCompletionAndSupersessionWinnerMatrixOnBothStores",
		"native-pipeline-delivery-exact-join-semantic/internal/runtime/pipeline/workflow_join_lifecycle_test.go/TestWorkflowJoinUsesSelectedStoreScheduleOwnerOnBothStores",
		"native-pipeline-delivery-exact-join-semantic/internal/runtime/pipeline/workflow_join_lifecycle_test.go/TestWorkflowJoinSchedulePreservesMockExecutionModeOnBothStores",
		"native-pipeline-delivery-exact-join-semantic/internal/runtime/pipeline/workflow_join_lifecycle_test.go/TestWorkflowJoinDurableIdentityIncludesStageOnBothStores",
		"native-pipeline-delivery-exact-join-semantic/internal/runtime/pipeline/workflow_join_lifecycle_test.go/TestWorkflowJoinCountWaitsDespiteEmptyStateMembersOnBothStores",
		"native-pipeline-delivery-exact-join-semantic/internal/runtime/pipeline/workflow_join_lifecycle_test.go/TestWorkflowJoinFailurePersistsCanonicalDeliveryOutcomeAndRuntimeLog",
		"native-shared-coordinator-authority/internal/runtime/node_delivery_startup_recovery_test.go/TestPipelineCoordinatorRecoveryContinuesAfterCommittedDeadLetterParity",
		"native-shared-coordinator-authority/internal/runtime/node_delivery_startup_recovery_test.go/TestPipelineCoordinatorStandingRecoveryClaimsNewlyEligibleNodeDeliveries":
	default:
		return recipe{}, fmt.Errorf("unlisted fixture comparator endpoint")
	}
	if fmt.Sprintf("%x", sha256.Sum256(fixtureSetupSuccessorBytes2617)) != fixtureSetupSuccessorPacketSHA256 {
		return recipe{}, fmt.Errorf("fixture comparator evidence changed")
	}
	var pins []fixtureSetupSuccessor2617
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &pins); err != nil {
		return recipe{}, err
	}
	if len(pins) != 40 {
		return recipe{}, fmt.Errorf("fixture comparator receipt count changed")
	}
	var original recipe
	var current string
	for _, pin := range pins {
		if !sameReviewedRecipeKey(row, pin.Original) {
			continue
		}
		if current == "" {
			original = pin.Original
			current = original.Successor
			if current == "" {
				current = original.After
			}
		} else {
			prior := original
			prior.Successor = current
			if !reflect.DeepEqual(prior, pin.Original) {
				return recipe{}, fmt.Errorf("fixture comparator crossed its historical receipt")
			}
		}
		reversed, err := reverseReviewedSuccessor2617(pin.Candidate, pin.Edits)
		if err != nil || !canonicalComparatorEqual2617(reversed, current) {
			return recipe{}, fmt.Errorf("fixture comparator failed exact reversal")
		}
		current = pin.Candidate
	}
	history := row
	history.Successor = original.Successor
	if current == "" || !reflect.DeepEqual(history, original) || row.Successor == "" ||
		!canonicalComparatorEqual2617(row.Successor, current) || !canonicalComparatorEqual2617(actual, current) {
		return recipe{}, fmt.Errorf("fixture comparator current source or history differs")
	}
	return original, nil
}

func TestFixtureSetupCurrentComparators2617RejectUnlistedAndCrossedPairs(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		name := row.Function
		if row.Family == "native-pipeline-delivery-exact-join-semantic" {
			name = nativePipelineRecipeReplacementName(t, row)
		}
		if row.Function != "TestFanOutDeliveryBarrierCompletionAndSupersessionWinnerMatrixOnBothStores" &&
			row.Function != "TestWorkflowJoinUsesSelectedStoreScheduleOwnerOnBothStores" &&
			row.Function != "TestWorkflowJoinSchedulePreservesMockExecutionModeOnBothStores" &&
			row.Function != "TestWorkflowJoinDurableIdentityIncludesStageOnBothStores" &&
			row.Function != "TestWorkflowJoinCountWaitsDespiteEmptyStateMembersOnBothStores" &&
			row.Function != "TestWorkflowJoinFailurePersistsCanonicalDeliveryOutcomeAndRuntimeLog" &&
			row.Function != "TestPipelineCoordinatorRecoveryContinuesAfterCommittedDeadLetterParity" &&
			row.Function != "TestPipelineCoordinatorStandingRecoveryClaimsNewlyEligibleNodeDeliveries" {
			continue
		}
		matched++
		t.Run(row.Function, func(t *testing.T) {
			actual := selectedCausalObservationBody(t, row.File, name)
			original, err := fixtureSetupCurrentEndpoint2617(row, actual)
			if err != nil {
				t.Fatal(err)
			}
			prior := original.Successor
			if prior == "" {
				prior = original.After
			}
			if _, err := fixtureSetupCurrentEndpoint2617(row, prior); err == nil {
				t.Fatal("historical endpoint accepted as current")
			}
			for _, field := range []string{"family", "file", "function", "before", "after", "removed", "mechanical", "missing", "current"} {
				mutant, source := row, actual
				switch field {
				case "family":
					mutant.Family += "-unlisted"
				case "file":
					mutant.File = "unlisted.go"
				case "function":
					mutant.Function += "Unlisted"
				case "before":
					mutant.Before = row.After
				case "after":
					mutant.After = row.Before
				case "removed":
					mutant.Removed = true
				case "mechanical":
					mutant.Mechanical = &mechanicalSnapshot{SourceCommit: strings.Repeat("0", 40), After: row.After}
				case "missing":
					mutant.Successor = ""
				case "current":
					source = strings.Replace(actual, "{", "{ unreviewedFixtureCut();", 1)
					mutant.Successor = source
				}
				if _, err := fixtureSetupCurrentEndpoint2617(mutant, source); err == nil {
					t.Fatalf("fixture comparator accepted %s mutation", field)
				}
			}
		})
	}
	if matched != 8 {
		t.Fatalf("fixture comparator endpoints=%d, want8", matched)
	}
}
