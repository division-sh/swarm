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
// #2730/6244508696 appends seven original-process setup endpoints; the first
// 39 receipts and every historical Before/After/Mechanical field are retained.
// #2730/6244831965 appends six mapped setup-chain endpoints, retaining
// those 46 receipts and every prior Successor.
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
		"native-shared-coordinator-authority/internal/runtime/node_delivery_startup_recovery_test.go/TestPipelineCoordinatorStandingRecoveryClaimsNewlyEligibleNodeDeliveries",
		"native-bus-terminal-refusal-state/internal/runtime/bus/eventbus_publish_test.go/TestEventBusRejectsTerminalRunEventsThroughEveryPublishOwnerPostgres",
		"native-bus-terminal-refusal-state/internal/runtime/bus/eventbus_publish_test.go/TestEventBusRejectsTerminalRunEventsThroughEveryPublishOwnerSQLite",
		"native-bus-selected-run-construction/internal/runtime/bus/eventbus_publish_test.go/eventBusTestRunContextForSource",
		"native-bus-run-control-construction/internal/runtime/bus/run_control_dispatch_test.go/seedRunControlTestRun",
		"native-bus-source-hash/internal/runtime/bus/eventbus_publish_test.go/TestEventBusPublish_ClassifiesCanonicalRunSourceArtifactThroughRunLifecycleOwner",
		"native-bus-target-construction/internal/runtime/bus/target_owner_store_parity_test.go/TestCrossFlowConstructedTargetOwnershipRoundTripOnBothBackends",
		"native-bus-dispatch-seed/internal/runtime/bus/event_identity_dispatch_surface_test.go/newCompleteEventDispatchFixtureWithOrigin",
		"native-startup-actor-construction/internal/runtime/runtime_constructed_actor_restart_test.go/proveRuntimeConstructedActorCensusBothStores",
		"native-run-setup-family144/internal/store/internal/runtimepersistence/flow_instance_descriptor_authority_test.go/TestDynamicFlowRuntimeReadinessObservedStateGuardBothStores",
		"native-run-setup-family144/internal/store/internal/runtimepersistence/reset_inventory_parity_test.go/TestResetInventoryAndOperationLeaseBothStores",
		"native-pipeline-source-work-owner/internal/runtime/pipeline/author_activity_external_test_context_test.go/newScopedTestEventBus",
		"native-conversation-session-owner/internal/runtime/conformance/persisted_surfaces_test.go/TestCanonicalSessionWatchdogSurface_RoundTripsThroughConversationReader",
		"native-pipeline-delivery-shared-tail-semantic/internal/runtime/pipeline/workflow_timer_attachment_fixture_external_test.go/newTimerReplayAttachmentOwner":
	default:
		return recipe{}, fmt.Errorf("unlisted fixture comparator endpoint")
	}
	if fmt.Sprintf("%x", sha256.Sum256(fixtureSetupSuccessorBytes2617)) != "6808e28b1441525beef44d9f85b6b6996e8af360cdf8e9131b2b5642d532f482" {
		return recipe{}, fmt.Errorf("fixture comparator evidence changed")
	}
	var pins []fixtureSetupSuccessor2617
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &pins); err != nil {
		return recipe{}, err
	}
	if len(pins) != 52 {
		return recipe{}, fmt.Errorf("fixture comparator receipt count changed")
	}
	var original recipe
	var current string
	exactOriginalSetup := false
	for index, pin := range pins {
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
		exactOriginalSetup = exactOriginalSetup || index >= 46
	}
	history := row
	history.Successor = original.Successor
	endpointCurrent := canonicalComparatorEqual2617(row.Successor, current) && canonicalComparatorEqual2617(actual, current)
	if exactOriginalSetup {
		// These setup deltas carry custody; the older comparator normalization
		// intentionally ignores some setup syntax and cannot prove this cut.
		endpoint, endpointErr := canonicalFunction(row.Successor)
		source, sourceErr := canonicalFunction(actual)
		want, wantErr := canonicalFunction(current)
		endpointCurrent = endpointErr == nil && sourceErr == nil && wantErr == nil && endpoint == want && source == want
	}
	if current == "" || !reflect.DeepEqual(history, original) || row.Successor == "" || !endpointCurrent {
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
		if row.Function == "newScopedTestEventBus" && (row.Family != "native-pipeline-source-work-owner" || row.File != "internal/runtime/pipeline/author_activity_external_test_context_test.go") {
			continue
		}
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
			row.Function != "TestPipelineCoordinatorStandingRecoveryClaimsNewlyEligibleNodeDeliveries" &&
			row.Function != "TestEventBusRejectsTerminalRunEventsThroughEveryPublishOwnerPostgres" &&
			row.Function != "TestEventBusRejectsTerminalRunEventsThroughEveryPublishOwnerSQLite" &&
			row.Function != "eventBusTestRunContextForSource" &&
			row.Function != "seedRunControlTestRun" &&
			row.Function != "TestEventBusPublish_ClassifiesCanonicalRunSourceArtifactThroughRunLifecycleOwner" &&
			row.Function != "TestCrossFlowConstructedTargetOwnershipRoundTripOnBothBackends" &&
			row.Function != "newCompleteEventDispatchFixtureWithOrigin" &&
			row.Function != "proveRuntimeConstructedActorCensusBothStores" &&
			row.Function != "TestDynamicFlowRuntimeReadinessObservedStateGuardBothStores" &&
			row.Function != "TestResetInventoryAndOperationLeaseBothStores" &&
			row.Function != "newScopedTestEventBus" &&
			row.Function != "TestCanonicalSessionWatchdogSurface_RoundTripsThroughConversationReader" &&
			row.Function != "newTimerReplayAttachmentOwner" {
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
	if matched != 21 {
		t.Fatalf("fixture comparator endpoints=%d, want21", matched)
	}
}

func TestFixtureSetupSuccessors2617PreservePriorHistoryAndExactCurrentOwners(t *testing.T) {
	if fmt.Sprintf("%x", sha256.Sum256(fixtureSetupSuccessorBytes2617)) != "6808e28b1441525beef44d9f85b6b6996e8af360cdf8e9131b2b5642d532f482" {
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
	if len(pins) != 52 {
		t.Fatal("fixture successor row count changed")
	}
	prefix, err := json.Marshal(pins[:39])
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(prefix)) != "daf060dec5ac400e00a35c4a7dea0321a38feeaae8e3bbcb4b20c87cbab0b5b2" {
		t.Fatal("#2730 changed the immutable 39-receipt predecessor prefix")
	}
	prefix, err = json.Marshal(pins[:46])
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(prefix)) != "b9f77e265a38ef4729f1b497c637271afc580c01908284dccc91e2983a97bf97" {
		t.Fatal("#2730 changed the immutable 46-receipt setup predecessor")
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
