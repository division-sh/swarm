package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

//go:embed family145-transitions.json
var family145TransitionBytes []byte

func TestFamily145AdmissionSnapshotsRetainWorkloadsAndRejectDrift(t *testing.T) {
	var rows []struct {
		File, Function, Before, After string
		Removed                       bool `json:"Removed,omitempty"`
	}
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(rows)
	if err != nil || len(rows) != 817 || fmt.Sprintf("%x", sha256.Sum256(raw)) != "990cbc7a864ca9c6b49839a6d0c59ee1a9959a914e0f49e0974587181b5aa71f" {
		t.Fatalf("finite family145 source transition changed: %d/%v", len(rows), err)
	}
	for _, row := range rows {
		finite := recipe{File: row.File, Function: row.Function, Before: row.Before, After: row.After}
		if row.Removed {
			finite.After, finite.Removed = row.Before, true
			if _, err := checkpointRecipes([]byte("package proof\n"+row.Before), []recipe{finite}); err == nil {
				t.Fatalf("retired raw helper survived: %s", row.Function)
			}
			if remaining, err := checkpointRecipes([]byte("package proof\n"), []recipe{finite}); err != nil || len(remaining) != 0 {
				t.Fatalf("exact helper retirement failed: %s/%v", row.Function, err)
			}
			continue
		}
		if family145CheckRunPhysicalWholeRule(t, row.File, row.Function, row.After) {
			continue
		}
		if family145CheckStandingDiagnosticWholeRule(t, row.File, row.Function, row.After) {
			continue
		}
		if family145CheckStopWitnessAmendment(t, row.File, row.Function, row.After) {
			continue
		}
		source := []byte("package proof\n" + row.Before)
		after, changed, err := rewriteFunction(row.File, source, finite)
		if err != nil || !changed {
			t.Fatalf("finite rewrite failed: %s/%s/%v", row.File, row.Function, err)
		}
		if _, changed, err := rewriteFunction(row.File, after, finite); err != nil || changed {
			t.Fatalf("replay changed native successor: %s/%v", row.Function, err)
		}
		hostile := strings.Replace(row.Before, "{", "{ unreviewedAuthority();", 1)
		if _, _, err := rewriteFunction(row.File, []byte("package proof\n"+hostile), finite); err == nil {
			t.Fatalf("changed binding/workload accepted: %s", row.Function)
		}
		if family145CheckServedEventCountWholeRule(t, row.File, row.Function, row.After) || family145CheckChannelEventCountWholeRule(t, row.File, row.Function, row.After) || family145CheckChannelDecisionWholeRule(t, row.File, row.Function, row.After) || family145CheckWorkflowStateWholeRule(t, row.File, row.Function, row.After) || family145CheckChannelAnchorWholeRule(t, row.File, row.Function, row.After) || family145CheckEventNameWholeRule(t, row.File, row.Function, row.After) || family145CheckDeliveryStatusWholeRule(t, row.File, row.Function, row.After) || family145CheckRunScalarWholeRule(t, row.File, row.Function, row.After) || family145CheckRequestKeyWholeRule(t, row.File, row.Function, row.After) || family145CheckChannelIntentWholeRule(t, row.File, row.Function, row.After) || family145CheckServedDebugWholeRule(t, row.File, row.Function, row.After) || family145CheckForkCompletionWholeRule(t, row.File, row.Function, row.After) || family145CheckLifecycleReaderWholeRule(t, row.File, row.Function, row.After) || family145CheckWriterStageWholeRule(t, row.File, row.Function, row.After) || row.Function == "requireServedEventPublishEntityState" || row.Function == "requireLifecycleEventCount" || row.Function == "readLifecycleLoop" {
			continue
		}
		if family145CheckPublicA2EndpointWholeRule(t, row.File, row.Function, row.After) {
			continue
		}
		if family145CheckReceiverPublicReadbackWholeRule(t, row.File, row.Function, row.After) {
			continue
		}
		if family145CheckPublicJoinWholeRule(t, row.File, row.Function, row.After) {
			continue
		}
		if family145CheckForkCompanionWholeRule(t, row.File, row.Function, row.After) {
			continue
		}
		if family145CheckForkNoticeWholeRule(t, row.File, row.Function, row.After) {
			continue
		}
		if family145CLIForkRow(row.File, row.Function) {
			if err := family145CLIForkProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145ServingLifetimeHelpers[row.Function] != nil {
			if err := family145ServingLifetimeReadProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145SelectedCapabilityCuts[row.Function] != nil {
			if err := family145SelectedCapabilityProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145DeploymentReadCuts[row.Function] != nil {
			if err := family145DeploymentReadProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145DeploymentPublicRowCuts[row.Function] != nil {
			if err := family145DeploymentPublicRowProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.File == "internal/runtime/eventbus_receiver_authority_e2e_test.go" {
			if err := family145ClosedReceiverProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "gateCompletionHarness" {
			if err := family145GateCompletionConstructorProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145ServedSnapshotProjection(row.Function) {
			if err := family145ServedProjectionProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "runMockAgentSupportedSurface" || row.Function == "runStandingTelegramMemorySupportedSurface" {
			if err := family145MockMemoryConstructionProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "TestStandingRestartMixedHealthyAndTerminalProcessParity" {
			if err := family145StandingMixedConstructionProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "TestCompletionBudgetRecoveryProjectionParity" || row.Function == "TestBudgetRecoveryLoadsExactRetainedStagesAndStatelessPostureBothStores" {
			if err := family145BudgetConstructionProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "forEachDurableDataStore" {
			if err := family145DurableDataConstructionProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "snapshotForkReceiverApplication" {
			if err := family145ServedForkSnapshotProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145CheckWholeObservationReader(t, row.Function, row.Before, row.After) {
			continue
		}
		if family145LifecycleBusinessCallers[row.Function] != 0 {
			if err := family145LifecycleBusinessCallerProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145ForkSourceDomainCallers[row.Function] != 0 {
			if err := family145ForkSourceDomainCallerProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "forkReceiverChangedTables" || row.Function == "TestSelectedForkPendingInputMixedCompletionBothStores" {
			if err := family145ServedSnapshotConsumerProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "readRetainedRootJoins" {
			if err := family145RetainedJoinReaderProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "testServedJoinWriterCompletedHistoryRefusal" {
			if err := family145RetainedJoinConsumerProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145ServedSnapshotCallers[row.Function] != 0 {
			if err := family145ServedSnapshotCallerProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145ForkReceiverJoinedCallers[row.Function] != 0 {
			if err := family145ForkReceiverJoinedCallerProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "readForkReceiverProducerEvidence" {
			if err := family145ForkProducerReaderProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145ForkProducerCallers[row.Function] != 0 {
			if err := family145ForkProducerCallerProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, known := family145ServedRestartConstructors[row.Function]; known {
			if err := family145ServedRestartConstructionProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "TestOwnedNumericTimerInspection" {
			if err := family145NumericTimerInspectionProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "openSQLiteGateRecoveryStore" || row.Function == "openPostgresGateRecoveryStore" {
			if err := family145GateCarrierConstructorProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "TestA2MapFanOutRawBusinessSourceReaderRefusesOnBothStores" {
			if err := family145GateDialectQualificationProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145CompoundFootprintRow(row.File, row.Function) {
			if err := family145CompoundFootprintProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			if row.Function != "TestA2PersistedJoinHydrationRefusesCorruptionOnBothStores" {
				continue
			}
		}
		if family145GateFinalDiagnosticCuts[row.Function] != nil {
			if err := family145GateFinalDiagnosticProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
		}
		if row.Function == "a2CorruptBoundReplyEntry" {
			if err := family145RetainedReplyFaultProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "a2BoundReplySourceSnapshot" {
			if err := family145BoundSourceSnapshotProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.File == "internal/runtime/pipeline/a2_accumulator_persistence_external_test.go" && row.Function == "evidence" {
			if err := family145AccumulatorEvidenceProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
		}
		if family145A2ClaimReadCuts[row.Function] != nil {
			if err := family145A2ClaimReadProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "a2MultiUntilChildren" || row.Function == "testA2StageEntryPublicationBinding" {
			if err := family145SourceChildrenProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			if row.Function == "a2MultiUntilChildren" {
				continue
			}
		}
		if family145EventDeliveryReadCuts[row.Function] != "" {
			if err := family145EventDeliveryReadProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
		}
		if family145TimerPhysicalReadCuts[row.Function] != nil {
			if err := family145TimerPhysicalReadProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145TimerEventReadCuts[row.Function] != nil {
			if err := family145TimerEventReadProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145ExactJoinReadCuts[row.Function] != nil {
			if err := family145ExactJoinReadProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145RootAccumulatorWireCuts[row.Function] != nil {
			if err := family145RootAccumulatorWireProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
		}
		if family145PinnedMapSourceRow(row.File, row.Function) {
			if err := family145PinnedMapSourceProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			if row.Function == "readRaw" {
				continue
			}
		}
		if row.Function == "TestA2MapFanOutRetainedProjectionAndRangeRefuseOnBothStores" {
			if err := family145FanOutMetadataProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.File == "internal/runtime/pipeline/a2_map_fan_out_source_corruption_external_test.go" && row.Function == "writeRaw" {
			if problem := family145PinnedWireFaultProofError(row.After); problem != "" {
				t.Fatal(problem)
			}
			continue
		}
		if family145CanonicalClaimSettlementCuts[row.Function] != nil {
			if err := family145CanonicalClaimSettlementProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "TestA2MultiUntilIndependentRecipientEntriesAndRestartOnBothStores" {
			if err := family145PublicationHistoryProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "TestA2MapFanOutRawBusinessSourcePumpRefusesWithoutProgressOnBothStores" {
			if err := family145SourceFactDiagnosticProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145A2CreationFaultCuts[row.Function] != nil {
			if err := family145A2CreationFaultProofError(family145GateScalarReadKey(row.File, row.Function), row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			if !family145GateCountFunctions[row.Function] {
				continue
			}
		}
		if row.File == "internal/runtime/runforkadmission/revision_source_route_test.go" && row.Function == "TestRunForkPointRevisionedSourceRouteDrivesSelectedHistoryMatrixPostgres" {
			if err := family145SourceRouteMatrixProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145CanonicalRecordCaller(row.Function) {
			if err := family145CanonicalRecordCallerProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, ok := family145NativeRootCallerBindings[row.Function]; ok {
			if err := family145NativeRootCallerProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.File == "internal/store/storetest/event.go" && row.Function == "InsertCanonicalEventRecord" {
			// Exact admission/replay policy and its original private writer are
			// checked by the complete canonical-record owner/caller guard.
			continue
		}
		if owner := family145CompetingRevisionPairOwner(row.File, row.Function); owner != "" {
			if err := family145CompetingRevisionPairProofError(owner, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145GateEntityConflictKeys[row.Function] != "" {
			if err := family145GateEntityConflictProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145GateCountFunctions[row.Function] {
			key := family145GateScalarReadKey(row.File, row.Function)
			if err := family145GateCountProofError(key, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			if family145GateFailureReadCuts[key] != nil {
				if err := family145GateFailureReadProofError(key, row.Before, row.After); err != nil {
					t.Fatal(err)
				}
			}
			if family145GateScalarReadCuts[key] != nil {
				if err := family145GateScalarReadProofError(key, row.Before, row.After); err != nil {
					t.Fatal(err)
				}
			}
			continue
		}
		if key := family145GateScalarReadKey(row.File, row.Function); family145GateFailureReadCuts[key] != nil {
			if err := family145GateFailureReadProofError(key, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			if family145GateScalarReadCuts[key] != nil {
				if err := family145GateScalarReadProofError(key, row.Before, row.After); err != nil {
					t.Fatal(err)
				}
			}
			continue
		}
		if key := family145GateScalarReadKey(row.File, row.Function); family145GateScalarReadCuts[key] != nil {
			if err := family145GateScalarReadProofError(key, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if family145SemanticOwner(row.Function) {
			assertFamily145SemanticProofs(t, row.Function, row.Before, row.After)
			continue
		}
		if _, ok := family145FanInHelperCalls[row.Function]; ok {
			if err := family145FanInHelperProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, ok := family145SemanticReadHelperCalls[row.Function]; ok {
			if err := family145SemanticReadHelperProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if calls, ok := family145NestedReadCalls[row.File+"|"+row.Function]; ok {
			if err := family145NestedReadProofError(row.Function, row.Before, row.After, calls); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.File == "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go" && row.Function == "runNumericFanOutReporterShape" {
			if err := family145ReporterRebaseProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.File == "internal/runtime/template_instance_delivery_test.go" && (row.Function == "waitRuntimeDBCount" || row.Function == "assertRuntimeDBCount" || row.Function == "waitRuntimeEventID") {
			if err := family145TemplateWaitAssertions(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.File == "internal/runtime/conformance/fan_out_supplemental_helpers_test.go" && row.Function == "assertSupplementalHistory" {
			if err := family145SupplementalHistoryProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, ok := family145DescriptorReadinessCalls[row.Function]; ok {
			if err := family145DescriptorReadinessProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, ok := family145ChannelJournalCalls[row.Function]; ok {
			if err := family145ChannelJournalProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, ok := family145OperatorChannelCalls[row.Function]; ok {
			if err := family145OperatorChannelProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, ok := family145NativeBusRoutingCalls[row.Function]; ok {
			if err := family145NativeBusRoutingProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, ok := family145ArtifactCallbackCuts[row.Function]; ok {
			if err := family145ArtifactCallbackProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		_, gateHelper := family145GateRecoveryHelpers[row.Function]
		_, gateCaller := family145GateRecoveryCallers[row.Function]
		if gateHelper || gateCaller {
			if err := family145GateRecoveryProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, ok := family145ConversationForkCuts[row.Function]; ok {
			if err := family145ConversationForkProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "TestRuntimeNukeDurableReplaySurvivesAPICompletionLossAndExpiration" {
			if err := family145ResetCompletionProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, ok := family145AdministrativeLayeringCuts[row.Function]; ok {
			if err := family145AdministrativeLayeringProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, ok := family145StageLifecycleCuts[row.Function]; ok {
			if err := family145StageLifecycleProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, ok := family145SystemNodeCompletionCalls[row.Function]; ok {
			if err := family145SystemNodeCompletionProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, ok := family145RunStateCalls[row.Function]; ok {
			if err := family145RunStateProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, ok := family145SelectedCleanupCalls[row.Function]; ok {
			if err := family145SelectedCleanupProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "TestCanonicalMutationSurface_FailsOnMalformedCanonicalMutationField" {
			if err := family145GateEmptyPathProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if row.Function == "TestStartupPipelineReplayAftermathSurface_RoundTripsThroughObservabilityReader" {
			if err := family145RecoveryRunFaultProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, ok := family145ConnectorCalls[row.Function]; ok {
			if err := family145ConnectorProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, ok := family145InboundAcknowledgmentCalls[row.Function]; ok {
			if err := family145InboundAcknowledgmentProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if normalized := family145MailboxCompletionCallerInverse(row.After); normalized != row.After {
			family145MailboxCompletionCheckCaller(t, row.Function, row.Before, row.After, normalized)
			continue
		}
		beforeAssertions, beforeErr := runSetupAssertions(row.Before)
		afterAssertions, afterErr := runSetupAssertions(row.After)
		if beforeErr != nil || afterErr != nil || strings.Join(beforeAssertions, "\n") != strings.Join(afterAssertions, "\n") {
			t.Fatalf("caller lost an assertion: %s/%v/%v", row.Function, beforeErr, afterErr)
		}
	}
}

func assertFamily145SemanticProofs(t testing.TB, function, beforeSource, afterSource string) {
	t.Helper()
	if function == "TestRevisionProjectedSourceRouteDrivesFrontierAndHistoryAcrossReceiverContext" {
		before, beforeErr := runSetupAssertions(beforeSource)
		after, afterErr := runSetupAssertions(afterSource)
		var preserved []string
		transport := map[string]bool{
			`t.Fatalf("begin revision capture: %v", err)`: true,
			`t.Fatalf("capture revision: %v", err)`:       true,
			`t.Fatalf("commit revision: %v", err)`:        true,
		}
		for _, assertion := range before {
			if !transport[assertion] {
				preserved = append(preserved, assertion)
			}
		}
		if beforeErr != nil || afterErr != nil || strings.Join(preserved, "\n") != strings.Join(after, "\n") ||
			strings.Count(afterSource, "storetest.CaptureSelectedRunForkRevision(t, ctx, pg, runID, runforkrevision.AllFamilies()...)") != 1 ||
			strings.Count(afterSource, "storetest.StartPostgresRuntimeStore(t)") != 1 || strings.Contains(afterSource, "db.") {
			t.Fatal("native revision capture lost original owner/context/families or routing/history assertions")
		}
		return
	}
	if function == "proveSelectedForkPreparedPreflight" {
		if err := family145ProspectivePreflightError(beforeSource, afterSource); err != nil {
			t.Fatal(err)
		}
		return
	}
	if function == "buildSelectedForkProofContainer" || function == "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority" {
		if err := family145NativeBindingConstructionError(function, beforeSource, afterSource); err != nil {
			t.Fatal(err)
		}
		return
	}
	if function == "selectedContractTestRuntimeAuthority" {
		if err := family145ComponentRuntimeAuthorityError(afterSource); err != nil {
			t.Fatal(err)
		}
		return
	}
	if function == "selectedPreparationDatabaseSnapshot" {
		if err := family145PreparationSnapshotOwnerError(afterSource); err != nil {
			t.Fatal(err)
		}
		return
	}
	if function == "seedSelectedOperationSource" {
		if err := family145OperationSourceCaptureError(beforeSource, afterSource); err != nil {
			t.Fatal(err)
		}
		return
	}
	if family145ActivitySetupOwner(function) {
		if err := family145ActivitySetupProofError(function, beforeSource, afterSource); err != nil {
			t.Fatal(err)
		}
		return
	}
	if family145ActivityReadOwner(function) {
		if err := family145ActivityReadChainProofError(function, beforeSource, afterSource); err != nil {
			t.Fatal(err)
		}
		return
	}
	if function == "capture" {
		if !strings.Contains(beforeSource, "runforkrevision.CaptureSQLite(ctx, tx, runID, runforkrevision.AllFamilies()...)") {
			t.Fatal("activity capture lost its original complete historical family set")
		}
		if err := family145ActivitySnapshotError(afterSource); err != nil {
			t.Fatal(err)
		}
		return
	}
	if function == "seedSelectedExecutionSourceReplayScopeMarker" || function == "seedSelectedExecutionPostForkSourceEvent" {
		assertFamily145ReplayScopeHistoryProofs(t, function, beforeSource, afterSource)
		return
	}
	if function == "seedSelectedExecutionDiagnosticPlatformDeadLetter" || function == "seedRunForkCLISelectedExecutionDiagnosticPlatformDeadLetter" {
		assertFamily145DiagnosticHistoryProofs(t, function, beforeSource, afterSource)
		return
	}
	if function == "assertNoForkExecutionRowsForRun" || function == "assertNoSelectedContractExecutionMutationForSource" {
		assertFamily145RefusalProofs(t, function, beforeSource, afterSource)
		return
	}
	if function == "seedSourceOutcomeThatMustNotSuppressFork" || function == "seedSelectedRuntimeOutcomeSource" {
		assertFamily145SourceOutcomeProofs(t, function, beforeSource, afterSource)
		return
	}
	if function == "TestOperatorEventReplayDispatchesCompleteCanonicalSnapshotParity" {
		assertFamily145ReplayFaultAssertions(t, beforeSource, afterSource)
		return
	}
	if family145PhysicalBusinessWitness(function) {
		before, beforeErr := family145DiagnosticBusinessAssertions(beforeSource)
		after, afterErr := family145DiagnosticBusinessAssertions(afterSource)
		if beforeErr != nil || afterErr != nil || strings.Join(before, "\n") != strings.Join(after, "\n") {
			t.Fatalf("physical observation lost a behavioral assertion: %v/%v", beforeErr, afterErr)
		}
		return
	}
	if function != "TestTerminalPanicFinalizationBothStores" {
		return
	}
	before, after := family145PanicLifecycleCalls(beforeSource), family145PanicLifecycleCalls(afterSource)
	if len(before) != 2 || strings.Join(before, "\n") != strings.Join(after, "\n") {
		t.Fatal("terminal panic migration changed or removed its real lifecycle proofs")
	}
}

func assertFamily145ReplayFaultAssertions(t testing.TB, beforeSource, afterSource string) {
	t.Helper()
	before, beforeErr := runSetupAssertions(beforeSource)
	after, afterErr := runSetupAssertions(afterSource)
	if beforeErr != nil || afterErr != nil {
		t.Fatalf("replay proof parse: %v/%v", beforeErr, afterErr)
	}
	remove := func(calls []string, expected string) []string {
		var kept []string
		found := 0
		for _, call := range calls {
			if call == expected {
				found++
			} else {
				kept = append(kept, call)
			}
		}
		if found != 1 {
			t.Fatalf("replay fault transport assertion %q occurs %d times, want one", expected, found)
		}
		return kept
	}
	before = remove(before, `t.Fatalf("encode original target route: %v", err)`)
	after = remove(after, `t.Fatalf("corrupt replay target route: %v", err)`)
	if strings.Join(before, "\n") != strings.Join(after, "\n") {
		t.Fatal("replay migration lost a behavioral assertion")
	}
}

func family145PanicLifecycleCalls(source string) []string {
	file, err := parser.ParseFile(token.NewFileSet(), "panic.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return nil
	}
	var out []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		member, ok := call.Fun.(*ast.SelectorExpr)
		if ok && (member.Sel.Name == "ProveTerminalPanicFinalization" || member.Sel.Name == "ProveTerminalPanicMutationFailure") {
			out = append(out, family145NodeString(call))
		}
		return true
	})
	return out
}

func family145SemanticOwner(function string) bool {
	if family145ActivityReadOwner(function) || family145ActivitySetupOwner(function) {
		return true
	}
	switch function {
	case "captureSelectedExecutionSourceRevision", "admitSelectedExecutionSourceArtifact",
		"seedSelectedExecutionStateOnlySourceRun", "seedSelectedExecutionSourceRunWithPrimaryRouteModeAndSource",
		"selectedForkExecutableMutationSnapshotForTest", "selectedContractDynamicRuntimeGlobalSnapshot",
		"assertSelectedContractDeferredWorkRejectionHasNoForkMutation",
		"postgresEventDeliveryTargetRoutes", "scanDeliveryTargetRoutes", "seedEventPublishConstructedFlow",
		"assertRunStartPersistence", "TestTerminalPanicFinalizationBothStores",
		"TestOperatorEventReplayDispatchesCompleteCanonicalSnapshotParity", "gateLifecycleDiagnosticOverlap", "loadNotifyAllChildrenNumericRegistrations", "loadNotifyAllChildrenSingleEventID",
		"loadNotifyAllChildrenSemanticRejections", "assertReporterFanOutSingleChunkHistory", "assertServingMatrixHistory", "requireSourceRevisionLifecycleDiagnosticReceipts":
		return true
	case "seedSourceOutcomeThatMustNotSuppressFork", "seedSelectedRuntimeOutcomeSource":
		return true
	case "assertNoForkExecutionRowsForRun", "assertNoSelectedContractExecutionMutationForSource":
		return true
	case "seedSelectedExecutionDiagnosticPlatformDeadLetter", "seedRunForkCLISelectedExecutionDiagnosticPlatformDeadLetter":
		return true
	case "seedSelectedExecutionSourceReplayScopeMarker", "seedSelectedExecutionPostForkSourceEvent":
		return true
	case "capture":
		return true
	case "seedSelectedOperationSource":
		return true
	case "selectedPreparationDatabaseSnapshot":
		return true
	case "selectedContractTestRuntimeAuthority":
		return true
	case "proveSelectedForkPreparedPreflight":
		return true
	case "TestRevisionProjectedSourceRouteDrivesFrontierAndHistoryAcrossReceiverContext":
		return true
	case "buildSelectedForkProofContainer", "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority":
		return true
	}
	return false
}

func family145PhysicalBusinessWitness(function string) bool {
	switch function {
	case "gateLifecycleDiagnosticOverlap", "loadNotifyAllChildrenNumericRegistrations", "loadNotifyAllChildrenSingleEventID",
		"loadNotifyAllChildrenSemanticRejections", "assertReporterFanOutSingleChunkHistory", "assertServingMatrixHistory", "requireSourceRevisionLifecycleDiagnosticReceipts":
		return true
	}
	return false
}

func family145DiagnosticBusinessAssertions(source string) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "diagnostic.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	ast.Inspect(file, func(node ast.Node) bool {
		block, ok := node.(*ast.BlockStmt)
		if !ok {
			return true
		}
		var kept []ast.Stmt
		for _, stmt := range block.List {
			conditional, ok := stmt.(*ast.IfStmt)
			if ok && conditional.Init != nil && family145PhysicalTransportCheck(family145NodeString(conditional.Init)) {
				continue
			}
			kept = append(kept, stmt)
		}
		block.List = kept
		return true
	})
	return runSetupAssertions(family145NodeString(file.Decls[0]))
}

func family145PhysicalTransportCheck(initialization string) bool {
	return strings.HasPrefix(initialization, "err := rows.Scan(") || strings.HasPrefix(initialization, "err := logs.Scan(") ||
		initialization == "err := rows.Err()" || initialization == "err := logs.Err()"
}

func TestFamily145PublicationCallersHaveNoRawConstructionOrReadback(t *testing.T) {
	for _, name := range []string{"operator_event_publish_test.go", "operator_run_start_test.go", "operator_event_replay_test.go", "sqlite_observability_supported_surface_test.go", "operator_agent_control_test.go", "operator_runtime_context_test.go", "operator_test_setup_test.go", "operator_stage_completion_journey_test.go", "operator_stage_completion_read_test.go"} {
		data, err := os.ReadFile(filepath.Join("../../..", "internal/apiv1", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := family145PublicationRawEscape(data); err != nil {
			t.Fatalf("%s retained a raw publication dependency: %v", name, err)
		}
	}
	for _, expression := range []string{"storetest.AdmitPostgresRuntimeStore(t, db)", "storetest.AdmitSQLiteRuntimeStore(t, db)", "storetest.DatabaseForTest(selected)", "db.QueryRow(query)", "db.Exec(query)"} {
		source := []byte("package proof\nfunc witness() { " + expression + " }")
		if err := family145PublicationRawEscape(source); err == nil {
			t.Fatalf("publication escape control passed: %s", expression)
		}
	}
}

func family145PublicationRawEscape(source []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "publication.go", source, parser.AllErrors)
	if err != nil {
		return err
	}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		member, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch member.Sel.Name {
		case "AdmitPostgresRuntimeStore", "AdmitSQLiteRuntimeStore", "NewPostgresStoreForTest", "DatabaseForTest", "Database", "QueryRow", "QueryRowContext", "Query", "QueryContext", "Exec", "ExecContext", "Begin", "BeginTx":
			err = fmt.Errorf("raw call %s", member.Sel.Name)
			return false
		}
		return err == nil
	})
	return err
}

func TestFamily145StageCompletionIndependenceAssertionRejectsBypass(t *testing.T) {
	data, err := os.ReadFile("../../../internal/apiv1/operator_stage_completion_journey_test.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := family145StageCompletionIndependenceAssertion(data); err != nil {
		t.Fatal(err)
	}
	for _, condition := range []string{"false", "recovered == nil", "recovered != original"} {
		hostile := strings.Replace(string(data), "recovered == nil || recovered == original", condition, 1)
		if err := family145StageCompletionIndependenceAssertion([]byte(hostile)); err == nil {
			t.Fatalf("independent-peer assertion bypass accepted: %s", condition)
		}
	}
}

func family145StageCompletionIndependenceAssertion(source []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "journey.go", source, parser.AllErrors)
	if err != nil {
		return err
	}
	fn, err := uniqueFunction(file, "requireStageCompletionIndependentOwner")
	if err != nil {
		return err
	}
	expected, err := canonicalFunction(`func requireStageCompletionIndependentOwner(t testing.TB, original, recovered stageCompletionJourneyStore) {
	t.Helper()
	if recovered == nil || recovered == original {
		t.Fatal("stage completion rehydration requires an independent native same-location owner")
	}
}`)
	if err != nil {
		return err
	}
	if family145NodeString(fn) != expected {
		return fmt.Errorf("stage completion independence assertion changed")
	}
	return nil
}

func TestFamily145ForkCallerRewritePreservesExactWorkload(t *testing.T) {
	before := `package runforkexecution
func caller(t testing.TB, db *sql.DB, pg *store.PostgresStore) {
	before := readExactClock()
	revision := captureSelectedExecutionSourceRevision(t, db, runID, runforkrevision.FamilyEvents)
	seedSelectedConstructedRootHistory(t, ctx, db, pg, loaded, runID, eventID, eventName, at, mode, routes, input)
	seedSelectedConstructedAgentRootHistory(t, ctx, db, pg, loaded, runID, eventID, at)
	assertExactRevisionAndClock(t, revision, before)
}`
	after, revisions, histories, helpers, err := rewriteFamily145ForkCallerSource("execution_test.go", []byte(before))
	if err != nil || revisions != 1 || histories != 2 || helpers != 0 {
		t.Fatalf("finite rewrite: %d/%d/%d err=%v", revisions, histories, helpers, err)
	}
	want := strings.ReplaceAll(before, "captureSelectedExecutionSourceRevision(t, db,", "captureSelectedExecutionSourceRevision(t, pg,")
	want = strings.ReplaceAll(want, "History(t, ctx, db, pg,", "History(t, ctx, pg,")
	beforeFile, err := parser.ParseFile(token.NewFileSet(), "want.go", want, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	afterFile, err := parser.ParseFile(token.NewFileSet(), "after.go", after, parser.AllErrors)
	if err != nil || family145NodeString(beforeFile.Decls[0]) != family145NodeString(afterFile.Decls[len(afterFile.Decls)-1]) {
		t.Fatalf("rewriter changed more than exact owner arguments: %s/%v", after, err)
	}
}

func TestFamily145ForkCallerRewriteRejectsUnreviewedBindings(t *testing.T) {
	for _, args := range []string{"t, peerDB, runID", "t, openPool(), runID", "t, db, runID"} {
		name := "execution_test.go"
		if args == "t, db, runID" {
			name = "unreviewed_test.go"
		}
		source := "package proof\nfunc caller() { captureSelectedExecutionSourceRevision(" + args + ") }"
		if _, _, _, _, err := rewriteFamily145ForkCallerSource(name, []byte(source)); err == nil {
			t.Fatalf("unreviewed binding accepted: %s/%s", name, args)
		}
	}
	for _, owner := range []string{"peer", "factory()", "selected.pg"} {
		source := "package proof\nfunc caller() { seedSelectedConstructedRootHistory(t, ctx, db, " + owner + ", loaded) }"
		if _, _, _, _, err := rewriteFamily145ForkCallerSource("execution_test.go", []byte(source)); err == nil {
			t.Fatalf("unreviewed history owner accepted: %s", owner)
		}
	}
}

func TestFamily145ForkCallerInventoryConsumesOriginalSelectedOwner(t *testing.T) {
	var revisions, histories, nativeSnapshots int
	for _, source := range family145ForkCallerSources {
		data, err := os.ReadFile(filepath.Join("../../..", "internal/runtime/runforkexecution", source.name))
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), source.name, data, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if source.name == "runtime_outcome_test.go" && fn.Name.Name == "seedSelectedRuntimeOutcomeSource" {
				if err := family145NativeOutcomeSnapshot(fn); err != nil {
					t.Fatalf("named native snapshot replacement changed: %v", err)
				}
				nativeSnapshots++
			}
			if fn.Name.Name == "captureSelectedExecutionSourceRevision" {
				want := `func captureSelectedExecutionSourceRevision(t testing.TB, selected any, runID string, families ...runforkrevision.Family) int64 {
	t.Helper()
	return storetest.CaptureSelectedRunForkRevision(t, context.Background(), selected, runID, families...)
}`
				actual, err := canonicalFunction(family145NodeString(fn))
				expected, wantErr := canonicalFunction(want)
				if err != nil || wantErr != nil || actual != expected {
					t.Fatal("revision helper bypasses original selected receipt owner")
				}
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				helper, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				switch helper.Name {
				case "captureSelectedExecutionSourceRevision":
					revisions++
					want := family145ForkRevisionOwner(source.name, fn.Name.Name)
					if len(call.Args) < 3 || want == "" || family145NodeString(call.Args[1]) != want {
						t.Errorf("revision owner changed: %s/%s", source.name, fn.Name.Name)
					}
				case "seedSelectedConstructedRootHistory", "seedSelectedConstructedAgentRootHistory":
					histories++
					if len(call.Args) < 4 || (family145NodeString(call.Args[2]) != "pg" && family145NodeString(call.Args[2]) != "selected") {
						t.Errorf("history owner changed: %s/%s", source.name, fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	if revisions != 29 || nativeSnapshots != 1 || revisions+nativeSnapshots != 30 || histories != 21 {
		t.Fatalf("finite caller inventory changed: revisions=%d nativeSnapshots=%d history=%d", revisions, nativeSnapshots, histories)
	}
}

func family145NativeOutcomeSnapshot(fn *ast.FuncDecl) error {
	var snapshots int
	var problem error
	ast.Inspect(fn, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := family145NodeString(call.Fun)
		switch {
		case name == "storetest.CaptureRunForkSnapshot":
			snapshots++
			if len(call.Args) != 4 || family145NodeString(call) != "storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)" {
				problem = fmt.Errorf("snapshot must consume the original selected owner and coordinates")
			}
		case name == "captureSelectedExecutionSourceRevision" || strings.Contains(name, "BeginTx") ||
			strings.Contains(name, "CaptureSQLite") || name == "storetest.DatabaseForTest" || strings.HasSuffix(name, ".Commit"):
			problem = fmt.Errorf("native snapshot retained a duplicate raw transaction protocol: %s", name)
		}
		return true
	})
	if problem != nil {
		return problem
	}
	if snapshots != 1 {
		return fmt.Errorf("native snapshot multiplicity=%d, want exactly one", snapshots)
	}
	return nil
}

func TestFamily145NativeOutcomeSnapshotReplacementRejectsMissingDuplicateForeignAndRawProtocol(t *testing.T) {
	want := "storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)"
	for _, body := range []string{
		"", want + ";" + want,
		strings.Replace(want, "selected", "peer", 1),
		strings.Replace(want, "ctx", "context.Background()", 1),
		strings.Replace(want, "runID", "foreignRunID", 1),
		want + "; db.BeginTx(ctx, nil)", want + "; storetest.DatabaseForTest(selected)",
		want + "; tx.Commit()", want + "; captureSelectedExecutionSourceRevision(t, selected, runID)",
	} {
		fn := sourceOutcomeAST("func seedSelectedRuntimeOutcomeSource() {" + body + "}").Decls[0].(*ast.FuncDecl)
		if err := family145NativeOutcomeSnapshot(fn); err == nil {
			t.Fatalf("native replacement accepted missing/duplicate/foreign/raw snapshot: %s", body)
		}
	}
	fn := sourceOutcomeAST("func seedSelectedRuntimeOutcomeSource() {" + want + "}").Decls[0].(*ast.FuncDecl)
	if err := family145NativeOutcomeSnapshot(fn); err != nil {
		t.Fatal(err)
	}
}
