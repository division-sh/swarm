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
//
//go:embed testdata/fixture_setup_successors_2617.json
var fixtureSetupSuccessorBytes2617 []byte

type fixtureSetupSuccessor2617 struct {
	Original  recipe
	Candidate string
	Edits     []reviewedSuccessorEdit
}

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
	if fmt.Sprintf("%x", sha256.Sum256(fixtureSetupSuccessorBytes2617)) != "4b16cd811e0a3ebd2a8af6e2ee0a18b90498b7992b43d460200e5bc54b323690" {
		return recipe{}, fmt.Errorf("fixture comparator evidence changed")
	}
	var pins []fixtureSetupSuccessor2617
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &pins); err != nil {
		return recipe{}, err
	}
	if len(pins) != 35 {
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

func TestFixtureSetupSuccessors2617PreservePriorHistoryAndExactCurrentOwners(t *testing.T) {
	if fmt.Sprintf("%x", sha256.Sum256(fixtureSetupSuccessorBytes2617)) != "4b16cd811e0a3ebd2a8af6e2ee0a18b90498b7992b43d460200e5bc54b323690" {
		t.Fatal("append-only fixture setup evidence changed")
	}
	var pins []fixtureSetupSuccessor2617
	var rows []recipe
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &pins); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	if len(pins) != 35 {
		t.Fatal("fixture successor row count changed")
	}
	for index, pin := range pins {
		t.Run(pin.Original.Function, func(t *testing.T) {
			latest := fixtureSetupLatestSuccessor2617(t, pins, index)
			var matches []recipe
			for _, row := range rows {
				if sameReviewedRecipeKey(row, pin.Original) {
					matches = append(matches, row)
				}
			}
			if len(matches) != 1 {
				t.Fatalf("fixture successor multiplicity=%d", len(matches))
			}
			row := matches[0]
			actual := selectedCausalObservationBody(t, row.File, projectionShapeFunction(t, latest.Candidate).Name.Name)
			expected, err := canonicalFunction(latest.Candidate)
			if err != nil {
				t.Fatal(err)
			}
			valid := func(candidate recipe, source string) bool {
				history := candidate
				history.Successor = pin.Original.Successor
				current, sourceErr := canonicalFunction(source)
				endpoint, endpointErr := canonicalFunction(candidate.Successor)
				return reflect.DeepEqual(history, pin.Original) && sourceErr == nil && endpointErr == nil &&
					current == expected && endpoint == expected
			}
			if !valid(row, actual) {
				t.Fatal("current fixture source/Successor differs from exact settled endpoint")
			}
			prior := pin.Original.Successor
			if prior == "" {
				prior = pin.Original.After
			}
			reversed, err := reverseReviewedSuccessor2617(pin.Candidate, pin.Edits)
			want, wantErr := canonicalFunction(prior)
			got, gotErr := canonicalFunction(reversed)
			if err != nil || wantErr != nil || gotErr != nil || want != got {
				t.Fatalf("fixture setup reversal failed: %v", err)
			}
			if files, changes, err := prepareFiles("../../..", []recipe{row}); err != nil || len(files) != 0 || len(changes) != 0 {
				t.Fatalf("current fixture endpoint is not inert: %v", err)
			}
			for _, field := range []string{"history", "key", "missing", "body"} {
				mutant := row
				source := actual
				switch field {
				case "history":
					mutant.After += " changed"
				case "key":
					mutant.Function += "Unlisted"
				case "missing":
					mutant.Successor = ""
				case "body":
					source = strings.Replace(actual, "{", "{ unreviewedSetup();", 1)
					mutant.Successor = source
				}
				if valid(mutant, source) {
					t.Fatalf("hostile fixture source/Successor pair accepted: %s", field)
				}
			}
		})
	}
}

func fixtureSetupLatestSuccessor2617(t *testing.T, pins []fixtureSetupSuccessor2617, index int) fixtureSetupSuccessor2617 {
	t.Helper()
	latest := pins[index]
	for _, next := range pins[index+1:] {
		if !sameReviewedRecipeKey(next.Original, latest.Original) {
			continue
		}
		original := next.Original
		original.Successor = latest.Original.Successor
		if !reflect.DeepEqual(original, latest.Original) || next.Original.Successor != latest.Candidate {
			t.Fatal("finite fixture receipt chain crossed or rewrote its prior endpoint")
		}
		latest = next
	}
	return latest
}
