package main

import (
	"bytes"
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
// reviewer-a6190268703 appends the two exact #642 owner/caller successors.
// reviewer-a6190555639 appends the exact delegated catalog harness.
// reviewer-a6191161328 appends three exact #642 lineage/execution successors.
// reviewer-a6191625495 appends the exact native source-outcome seed successor.
// LIGHT under6191161328/6192796272 appends the native retained-join handoff.
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
		"native-selected-construction-raw-argument-retirement/internal/runtime/cataloge2e/tier12_runtime_fork_e2e_test.go/selectedContractExecutionOwnerForCatalogTest",
		"native-selected-construction-raw-argument-retirement/internal/runtime/cataloge2e/tier12_runtime_fork_e2e_test.go/selectedContractExecutionOwnerForCatalogHarness",
		"native-served-delivery-quiescence-caller/internal/serveapp/fork_retained_join_test.go/testServedJoinWriterCompletedHistoryRefusal",
		"native-handler-source-coordinator/internal/runtime/pipeline/workflow_timer_replay_selected_store_external_test.go/TestWorkflowTimerCauseReplayReopenAndIsolationBothStores",
		"native-run-setup-family144/internal/runtime/runforkexecution/constructed_agent_input_execution_test.go/proveSelectedConstructedAgentInput",
		"native-run-setup-family144/internal/runtime/runforkexecution/execution_test.go/TestSelectedContractForkRejectsSyntheticCarryDynamicCreationBeforeMutation",
		"native-run-setup-family144/internal/runtime/runforkexecution/runtime_outcome_test.go/seedSelectedRuntimeOutcomeSource":
	default:
		return recipe{}, fmt.Errorf("unlisted fixture comparator endpoint")
	}
	if fmt.Sprintf("%x", sha256.Sum256(fixtureSetupSuccessorBytes2617)) != "3698f8e49a693211ba853bb110ce400011ec3c828d3af40289505711202daf28" {
		return recipe{}, fmt.Errorf("fixture comparator evidence changed")
	}
	var pins []fixtureSetupSuccessor2617
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &pins); err != nil {
		return recipe{}, err
	}
	if len(pins) != 44 {
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
			row.Function != "TestPipelineCoordinatorStandingRecoveryClaimsNewlyEligibleNodeDeliveries" &&
			row.Function != "selectedContractExecutionOwnerForCatalogTest" &&
			row.Function != "selectedContractExecutionOwnerForCatalogHarness" &&
			row.Function != "testServedJoinWriterCompletedHistoryRefusal" &&
			row.Function != "TestWorkflowTimerCauseReplayReopenAndIsolationBothStores" &&
			row.Function != "proveSelectedConstructedAgentInput" &&
			row.Function != "TestSelectedContractForkRejectsSyntheticCarryDynamicCreationBeforeMutation" &&
			row.Function != "seedSelectedRuntimeOutcomeSource" {
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
	if matched != 15 {
		t.Fatalf("fixture comparator endpoints=%d, want15", matched)
	}
}

func TestFixtureSetupSuccessors2617PreservePriorHistoryAndExactCurrentOwners(t *testing.T) {
	if fmt.Sprintf("%x", sha256.Sum256(fixtureSetupSuccessorBytes2617)) != "3698f8e49a693211ba853bb110ce400011ec3c828d3af40289505711202daf28" {
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
	if len(pins) != 44 {
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

func TestIssue642FixtureSuccessorsPreservePriorPacketAndDelegatedOwner(t *testing.T) {
	var pins []fixtureSetupSuccessor2617
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &pins); err != nil || len(pins) != 44 {
		t.Fatalf("exact #642 successor inventory changed: %v", err)
	}
	var prior bytes.Buffer
	encoder := json.NewEncoder(&prior)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(pins[:36]); err != nil || fmt.Sprintf("%x", sha256.Sum256(prior.Bytes())) != "1cafae311b85749160553e56ca1a3123336ba397882464c9c0709a16d04e10ca" {
		t.Fatal("an original history/Successor/mechanical receipt was rewritten")
	}
	prior.Reset()
	var rawPins []json.RawMessage
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &rawPins); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Encode(rawPins[:39]); err != nil || fmt.Sprintf("%x", sha256.Sum256(prior.Bytes())) != "997a93df95f26aafdc82edb30cddc68378451e362cb7cc549aa45d6b8eb341d4" {
		t.Fatal("an earlier #642 successor was rewritten")
	}
	prior.Reset()
	if err := encoder.Encode(rawPins[:42]); err != nil || fmt.Sprintf("%x", sha256.Sum256(prior.Bytes())) != "7558f1939d627ff145c5641f322bc51b51f32b6562805b452fe608638639d6d0" {
		t.Fatal("an authorized #642 completion successor was rewritten")
	}
	prior.Reset()
	if err := encoder.Encode(rawPins[:43]); err != nil || fmt.Sprintf("%x", sha256.Sum256(prior.Bytes())) != "dc9462f1af05ec13e8fc9c03570c48082fe74b126bb6d0da8597dd4b2689ba62" {
		t.Fatal("the preceding 43 fixture successors were rewritten")
	}
	nativeJoin := pins[43]
	if !sameReviewedRecipeKey(nativeJoin.Original, pins[37].Original) || nativeJoin.Original.Successor != pins[37].Candidate {
		t.Fatal("native retained join successor crossed its predecessor")
	}
	helper := selectedCausalObservationBody(t, nativeJoin.Original.File, "requireServedRetainedJoinForkSuccess")
	const helperPin = "bc4ec0ce6b477d248dc1833bfed4c1f066907bb30617ae89543171876aafa56d"
	if !historicalCurrentSourcePinned(helper, helperPin) {
		t.Fatal("native retained join helper changed its exact observations or assertions")
	}
	for _, cut := range []string{"storetest.ReadSelectedForkApplicationStorageSnapshot", "compareRetainedJoinActivations(childJoins, repeated)", "reflect.DeepEqual(ack, stableACK)", "reflect.DeepEqual(childBefore, childAfter)", "len(sourceBefore) != 7", "reflect.DeepEqual(sourceBefore, sourceAfter)"} {
		mutant := strings.Replace(helper, cut, "unreviewedJoinObservation", 1)
		if mutant == helper || historicalCurrentSourcePinned(mutant, helperPin) {
			t.Fatalf("native retained join helper pin missed %s", cut)
		}
	}
	if pins[36].Original.File != "internal/runtime/cataloge2e/tier12_runtime_fork_e2e_test.go" || pins[36].Original.Function != "selectedContractExecutionOwnerForCatalogTest" ||
		pins[37].Original.File != "internal/serveapp/fork_retained_join_test.go" || pins[37].Original.Function != "testServedJoinWriterCompletedHistoryRefusal" ||
		pins[38].Original.File != pins[36].Original.File || pins[38].Original.Function != "selectedContractExecutionOwnerForCatalogHarness" {
		t.Fatal("unreviewed #642 successor key")
	}
	for i, key := range [][2]string{
		{"internal/runtime/pipeline/workflow_timer_replay_selected_store_external_test.go", "TestWorkflowTimerCauseReplayReopenAndIsolationBothStores"},
		{"internal/runtime/runforkexecution/constructed_agent_input_execution_test.go", "proveSelectedConstructedAgentInput"},
		{"internal/runtime/runforkexecution/execution_test.go", "TestSelectedContractForkRejectsSyntheticCarryDynamicCreationBeforeMutation"},
	} {
		if pins[39+i].Original.File != key[0] || pins[39+i].Original.Function != key[1] {
			t.Fatal("unreviewed #642 completion successor key")
		}
	}
	seed := pins[42]
	if seed.Original.Family != "native-run-setup-family144" || seed.Original.File != "internal/runtime/runforkexecution/runtime_outcome_test.go" ||
		seed.Original.Function != "seedSelectedRuntimeOutcomeSource" || seed.Original.Successor != "" {
		t.Fatal("source-outcome seed successor has the wrong key or invented prior endpoint")
	}
	var recipes []recipe
	if err := json.Unmarshal(recipeBytes, &recipes); err != nil {
		t.Fatal(err)
	}
	for _, row := range recipes {
		if !sameReviewedRecipeKey(row, seed.Original) {
			continue
		}
		for _, cut := range []string{"backend == \"postgres\"", "runtimefailures.ClassConnectorFailure", "RunID: runID, EventID: eventID, EntityID: runID, CreatedAt: at, Failure: failure", "commitSelectedOperationRootFixture(t, ctx, selected, loaded, event)", "storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)", "if err := storetest.SeedSelectedSourceOutcome", "t.Fatal(err)"} {
			mutant := strings.Replace(seed.Candidate, cut, "unreviewedOutcome", 1)
			changed := row
			changed.Successor = mutant
			if mutant == seed.Candidate {
				t.Fatalf("missing source-outcome guard cut: %s", cut)
			}
			if _, err := fixtureSetupCurrentEndpoint2617(changed, mutant); err == nil {
				t.Fatalf("self-authorizing source-outcome pair accepted: %s", cut)
			}
		}
	}
	scan := selectedCausalObservationBody(t, pins[40].Original.File, "CommitSelectedForkEvent")
	const scanPin = "5d887d501b83625c68bf5f08be3010db393a9e06fc2f9012d7409b688dbdec8e"
	if !historicalCurrentSourcePinned(scan, scanPin) {
		t.Fatal("delegated scan observer changed")
	}
	for _, cut := range []string{"p.observed <- err", "p.SelectedContractReplayPersistence.CommitSelectedForkEvent(ctx, req)", "p.store.ScanDeliveryContinuations", "len(result.DeliveryHandoffs) == 0", "return result, errSelectedHistoricalScanCut"} {
		mutant := strings.Replace(scan, cut, "unreviewedScan", 1)
		if mutant == scan || historicalCurrentSourcePinned(mutant, scanPin) {
			t.Fatalf("scan observer pin missed %s", cut)
		}
	}
	restored, err := restoreRetainedJoinRefusalBranch(pins[37].Candidate)
	if err != nil || !canonicalComparatorEqual2617(restored, pins[37].Original.Successor) {
		t.Fatalf("#642 positive sibling changed the recorded refusal: %v", err)
	}
	for _, entry := range []struct {
		name, arguments string
	}{
		{"TestServedJoinWriterCompletedHistoryRefusalBothStores", "false, false"},
		{"TestServedJoinWriterCompletedHistoryRefusalSeparateCheckpointBothStores", "true, false"},
		{"TestServedJoinWriterForkRetainedGenerationsBothStores", "false, true"},
		{"TestServedJoinWriterForkRetainedGenerationsSeparateCheckpointBothStores", "true, true"},
	} {
		actual := selectedCausalObservationBody(t, pins[37].Original.File, entry.name)
		want := "func " + entry.name + "(t *testing.T) { testServedJoinWriterCompletedHistoryRefusal(t, " + entry.arguments + ") }"
		if !canonicalComparatorEqual2617(actual, want) {
			t.Fatalf("original refusal/positive sibling selection changed: %s", entry.name)
		}
	}
	const delegate = "selectedContractExecutionOwnerForCatalogTestWithReplay"
	source := selectedCausalObservationBody(t, pins[36].Original.File, delegate)
	const digest = "6aa0b7864d08a276004e9497f247a5039207ff2c1d8f2e8e1c6a10b4abd51efa"
	if !historicalCurrentSourcePinned(source, digest) {
		t.Fatal("reviewed catalog delegate's native dependencies/overrides/retirement changed")
	}
	for _, cut := range []string{"NewSelectedContractExecutionOwner", "replay = selected", "fork = forkOverride[0]", "owner.RetireSelectedContexts"} {
		mutant := strings.Replace(source, cut, "unreviewedOwner", 1)
		if mutant == source || historicalCurrentSourcePinned(mutant, digest) {
			t.Fatalf("delegate pin failed to reject %s mutation", cut)
		}
	}
	for _, pin := range []struct {
		name, digest string
		cuts         []string
	}{
		{"selectedContractExecutionOwnerForCatalogHarness", "", []string{"t, h, nil, forkOverride..."}},
		{"selectedContractExecutionOwnerForCatalogHarnessWithReplay", "af49a44db4e729f63c1af928ef8d4bc344bec027d5d605952a66b7d3a6bda299",
			[]string{"replay == nil && len(forkOverride) == 0", "bindCatalogSelectedOwner(t, h, owner)", "h.selectedOwners = append(h.selectedOwners, owner)", "t, h, replay, forkOverride..."}},
		{"constructCatalogSelectedExecutionOwner", "cbf2fc07867a758a104c852faa2a8d51293dc4f4a21a1a91a7fd00b38e09d7e2",
			[]string{"if h.pg != nil", "selected := h.sqlite", "t, h.pg, replay, forkOverride...", "replay = selected", "fork = forkOverride[0]", "owner.RetireSelectedContexts"}},
	} {
		current := selectedCausalObservationBody(t, pins[38].Original.File, pin.name)
		accepted := func(candidate string) bool {
			if pin.digest == "" {
				return canonicalComparatorEqual2617(candidate, pins[38].Candidate)
			}
			return historicalCurrentSourcePinned(candidate, pin.digest)
		}
		if !accepted(current) {
			t.Fatalf("reviewed harness delegate changed: %s", pin.name)
		}
		for _, cut := range pin.cuts {
			mutant := strings.Replace(current, cut, "unreviewedOwner", 1)
			if mutant == current || accepted(mutant) {
				t.Fatalf("harness pin failed to reject %s/%s", pin.name, cut)
			}
		}
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
