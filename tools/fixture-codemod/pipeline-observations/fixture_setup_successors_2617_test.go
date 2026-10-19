package main

import (
	"crypto/sha256"
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
// #2730/6246451663 and6246457824 append the 27 mapped fixture endpoints
// and two exact callback continuations plus timer-reopen and native-coordinate endpoints,
// retaining all 52 prior receipts and the complete mechanical history.
// The final native-runtime fixture sweep appends 21 original-context/grant
// continuations, retaining the complete 83-receipt predecessor prefix.
// #2730/6248981600 appends the catalog's same-process pre-seed setup only.
// #2738/6251218086 appends four exact selected-preparation setup endpoints,
// preserving the complete 108-receipt prefix and all prior hostile controls.
// #2738/6253061297 adds the exact original before selected refusal setup,
// preserving the complete 112-receipt prefix and every business assertion.
//

const fixtureSetupSuccessorPacketSHA256 = "81ef61021364745f473c86a62eaab8790ae95d12b8c6c01205ac95dc17a14992"

// These settled fixture receipts compose with, but do not replace, the earlier
// historical transformation and current-endpoint packets.

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
		if row.Successor != "" {
			name = projectionShapeFunction(t, row.Successor).Name.Name
		}
		if row.Family == "native-pipeline-delivery-exact-join-semantic" {
			name = nativePipelineRecipeReplacementName(t, row)
		}
		if row.Function != "newRuntimeHarnessWithTerminalProvider" &&
			row.Function != "seedSource" &&
			row.Function != "buildSelectedForkProofContainer" &&
			row.Function != "seedSelectedExecutionSourceRunWithPrimaryRouteModeAndSource" &&
			row.Function != "seedSelectedExecutionStateOnlySourceRun" &&
			row.Function != "proveSelectedConstructedAgentInput" &&
			row.Function != "TestSelectedContractForkRejectsSyntheticCarryDynamicCreationBeforeMutation" &&
			row.Function != "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority" &&
			row.Function != "TestFanOutDeliveryBarrierCompletionAndSupersessionWinnerMatrixOnBothStores" &&
			row.Function != "TestWorkflowJoinUsesSelectedStoreScheduleOwnerOnBothStores" &&
			row.Function != "TestWorkflowJoinSchedulePreservesMockExecutionModeOnBothStores" &&
			row.Function != "TestWorkflowJoinDurableIdentityIncludesStageOnBothStores" &&
			row.Function != "TestWorkflowJoinCountWaitsDespiteEmptyStateMembersOnBothStores" &&
			row.Function != "TestWorkflowJoinFailurePersistsCanonicalDeliveryOutcomeAndRuntimeLog" &&
			row.Function != "TestPipelineCoordinatorRecoveryContinuesAfterCommittedDeadLetterParity" &&
			row.Function != "TestPipelineCoordinatorStandingRecoveryClaimsNewlyEligibleNodeDeliveries" &&
			row.Function != "TestDeliveryContinuationCoordinatorRecoversNodeDeliveriesThroughCanonicalSelectedStore" &&
			row.Function != "TestTemplateInstanceActivationConfigSubscriberPersistsRenderedRouteAndDeliveryRows" &&
			row.Function != "TestEventBusRejectsTerminalRunEventsThroughEveryPublishOwnerPostgres" &&
			row.Function != "TestEventBusRejectsTerminalRunEventsThroughEveryPublishOwnerSQLite" &&
			row.Function != "eventBusTestRunContextForSource" &&
			row.Function != "TestSelectedForkCommitFailurePreventsDispatchAndProviderReachability" &&
			row.Function != "seedRunControlTestRun" &&
			row.Function != "TestEventBusPublish_ClassifiesCanonicalRunSourceArtifactThroughRunLifecycleOwner" &&
			row.Function != "TestCrossFlowConstructedTargetOwnershipRoundTripOnBothBackends" &&
			row.Function != "newCompleteEventDispatchFixtureWithOrigin" &&
			row.Function != "proveRuntimeConstructedActorCensusBothStores" &&
			row.Function != "TestDynamicFlowRuntimeReadinessObservedStateGuardBothStores" &&
			row.Function != "TestResetInventoryAndOperationLeaseBothStores" &&
			row.Function != "newScopedTestEventBus" &&
			row.Function != "TestCanonicalSessionWatchdogSurface_RoundTripsThroughConversationReader" &&
			row.Function != "newTimerReplayAttachmentOwner" &&
			row.Function != "TestConfiguredChannelRuntimeDispatchesImportedAgentDurablyAcrossSelectedStores" &&
			row.Function != "TestFinalFlowInstanceAuthoringRuntime_PublishActivatesAndExecutesSelectedTemplateInstance" &&
			row.Function != "newTestAgentManagerWithOptions" &&
			row.Function != "TestManagerClaimPostcommitErrorConsumesOnlyAcknowledgedClaim" &&
			row.Function != "TestWriteReceiptPreservesCommittedSettlementAndContinuation" &&
			row.Function != "TestProcessEventDoesNotResettleAcknowledgedReceiptError" &&
			row.Function != "TestProcessEventRenewsExactClaimAroundAgentHandler" &&
			row.Function != "TestProcessEventHeartbeatCoversBlockedOutputAndPreventsReclaim" &&
			row.Function != "TestProcessEventSettlementFailureIsNotReportedAsReplayed" &&
			row.Function != "TestWriteReceipt_LogsRetryingAndExhaustedDeliveryLifecycleTransitions" &&
			row.Function != "TestWriteReceiptUsesCanonicalHandlerRetryBase" &&
			row.Function != "TestWriteReceipt_ContextCancellationReturnsFailureAndLeavesClaimForLeaseRecovery" &&
			row.Function != "TestWriteReceiptLongRunningClaimUsesExactRenewalTime" &&
			row.Function != "TestRunningManagerInterventionSettlementFailureShutsDownAndRecoversClaim" &&
			row.Function != "TestProcessEventOutcomeUncertainTerminalDeliverySuppressesReplay" &&
			row.Function != "TestRunningManagerDeliveryCarrierDispositionMatrix" &&
			row.Function != "TestCompletionBudgetRecoveryProjectionParity" &&
			row.Function != "openManagerNativeSelectedDelivery" &&
			row.Function != "nativeRuntimeLogFixture" &&
			row.Function != "TestRuntimeShutdownDeliveryFixtureClaimsThroughCanonicalAdapter" &&
			row.Function != "TestRuntimeShutdownWithOptions_PropagatesConfiguredGraceToManagerDrain" &&
			row.Function != "TestRuntimeShutdown_ClosesAdmissionBeforeManagerDrainAndInboundIngress" &&
			row.Function != "TestRuntimeShutdownFanOutCommittedTurnKeepsDependenciesLive" &&
			row.Function != "newStartupReadinessTestRuntime" &&
			row.Function != "TestRuntimeStartFailsClosedWhenManagerHydrationWouldWithholdWorkflowTimersOnBothStores" &&
			row.Function != "assertNativeRuntimeStartCarriesMemoryIdentity" &&
			row.Function != "TestRuntimeStart_RecoveryDisabledEmitsDeniedDecisionForActiveSchedules" &&
			row.Function != "TestRuntimeStart_RecoveryDisabledAllowsAndLogsManagerSnapshotWork" &&
			row.Function != "TestRuntimeStart_RecoveryEnabledEmitsAllowedDecisionSummary" &&
			row.Function != "TestRuntimeStart_WorkflowOnlyRecoveryUsesFamilyAwareBootAndRestorationDetail" &&
			row.Function != "TestRuntimeStart_RecoveryFailureEmitsDegradedDecisionSummary" &&
			row.Function != "TestRuntimeStart_DynamicFlowReadinessFinalizationFailureIsBootFatal" &&
			row.Function != "TestRuntimeStart_RecoveryInspectionAndManagerHydrationFailureIsBootFatal" &&
			row.Function != "TestRuntimeLogger_Log_StampsSourceArtifactFactOnRunRow" &&
			row.Function != "TestRuntimeLogger_LogRejectsDeletedPersistedSourceArtifactFact" &&
			row.Function != "TestRuntimeLogger_Log_PersistsCanonicalRunOwnershipFromContext" &&
			row.Function != "TestRuntimeLogger_Log_DerivesLineageFromPersistedSubjectEvent" &&
			row.Function != "TestRuntimeLogger_Log_DoesNotDeriveLineageFromUnpersistedSubjectEvent" &&
			row.Function != "TestRuntimeLogger_Log_PersistsTypedRuntimeLineage" &&
			row.Function != "TestGenericScheduleLifecyclePublishesOneShotAndRecurringThroughWorkflowRuntimeOnBothStores" &&
			row.Function != "TestRuntimeStartRestoresWorkflowTimersWithoutGenericScheduleStoreOnBothStores" &&
			row.Function != "TestRuntimeStartWithholdsDueSchedulesAndTimersUntilDynamicTopologyCompletesOnBothStores" &&
			row.Function != "TestWorkflowTimerCauseReplayReopenAndIsolationBothStores" {
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
	if matched != 75 {
		t.Fatalf("fixture comparator endpoints=%d, want75", matched)
	}
}

func TestFixtureSetupSuccessors2617PreservePriorHistoryAndExactCurrentOwners(t *testing.T) {
	if fmt.Sprintf("%x", sha256.Sum256(fixtureSetupSuccessorBytes2617)) != fixtureSetupSuccessorPacketSHA256 {
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
			directEndpoint := pin.Original.Function == "proveSelectedConstructedAgentInput" ||
				pin.Original.Function == "TestSelectedContractForkRejectsSyntheticCarryDynamicCreationBeforeMutation"
			historical := row
			if !directEndpoint {
				historical, _, err = embeddedSpecHistoricalPair1801(row, actual)
				if err != nil {
					t.Fatal(err)
				}
			}
			currentSource, err := q10FixtureSetupCurrentSource(historical)
			if err != nil {
				t.Fatal(err)
			}
			expectedCurrent, err := canonicalFunction(currentSource)
			if err != nil {
				t.Fatal(err)
			}
			valid := func(candidate recipe, source string) bool {
				if !directEndpoint {
					var err error
					candidate, source, err = embeddedSpecHistoricalPair1801(candidate, source)
					if err != nil {
						return false
					}
				}
				history := candidate
				history.Successor = pin.Original.Successor
				current, sourceErr := canonicalFunction(source)
				endpoint, endpointErr := canonicalFunction(candidate.Successor)
				return reflect.DeepEqual(history, pin.Original) && sourceErr == nil && endpointErr == nil &&
					current == expectedCurrent && endpoint == expected
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
			if err := validateRecipeSnapshots(row); err != nil {
				t.Fatal(err)
			}
			liveSource := currentSource
			if !reflect.DeepEqual(historical, row) {
				liveSource, err = embeddedSpecFieldDeletion1801(currentSource)
				if err != nil {
					t.Fatal(err)
				}
				if valid(row, currentSource) || valid(historical, actual) {
					t.Fatal("partial embedded-spec fixture stage admitted as the complete live pair")
				}
			}
			currentGuard := recipe{Family: row.Family, File: row.File, Function: row.Function, Before: liveSource, After: liveSource}
			if files, changes, err := prepareFiles("../../..", []recipe{currentGuard}); err != nil || len(files) != 0 || len(changes) != 0 {
				t.Fatalf("current fixture endpoint is not inert: %v", err)
			}
			if expectedCurrent != expected {
				if valid(row, latest.Candidate) {
					t.Fatal("historical fixture source admitted as the current header-backed endpoint")
				}
				for _, cut := range []string{"SourceArtifactFact: sourceFact", "fi.run_id=es.run_id", "fi.entity_id=es.entity_id", "fi.instance_path=es.flow_instance", "fi.revision", "ReadWorkflowDuplicateProjectionStorage", "initialRevision != 1", "fields != initialFields", "headerRevision != initialRevision", "current.Lifecycle.State.Revision"} {
					mutant := strings.Replace(liveSource, cut, "foreignCut", 1)
					if mutant != liveSource && valid(row, mutant) {
						t.Fatalf("current fixture lost exact source, read owner, identity, field or revision: %s", cut)
					}
				}
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
