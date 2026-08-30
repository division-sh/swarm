package runforkexecution

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestBuildContractSwapBootResumeAdmissionConsumesCanonicalPrerequisites(t *testing.T) {
	selection := testContractSwapSelection()
	selectedAdmission := testContractSwapSelectedExecutionAdmission(selection)
	replayAdmission := runfork.RunForkReplayResumeAdmission{
		Owner:                    runfork.RunForkReplayResumeAdmissionOwner,
		ReplayResumeFactsPresent: true,
		Dispositions: []runfork.RunForkReplayResumeDisposition{{
			Fact:        runfork.RunForkReplayResumeFactTimerHistory,
			Disposition: runfork.RunForkReplayResumeDispositionFailClosedBlocker,
			BlockerCode: runfork.RunForkBlockerTimerHistoryUnproven,
			Message:     "timer history remains unproven",
		}},
		UnsupportedBlockers: []runfork.RunForkUnsupportedBlocker{{
			Code:    runfork.RunForkBlockerTimerHistoryUnproven,
			Message: "timer history remains unproven",
		}},
	}
	routeRecovery := testContractSwapRouteRecovery(selectedAdmission)

	admission, err := BuildContractSwapBootResumeAdmission(ContractSwapBootResumeAdmissionRequest{
		SelectedExecutionAdmission: selectedAdmission,
		ReplayResumeAdmission:      replayAdmission,
		RouteRecovery:              &routeRecovery,
	})
	if err != nil {
		t.Fatalf("BuildContractSwapBootResumeAdmission: %v", err)
	}
	if admission.Owner != runfork.RunForkContractSwapBootResumeAdmissionOwner ||
		!admission.NonMutating ||
		admission.BootResumeSupported ||
		admission.FutureExecutionOwner != runfork.RunForkHistoricalReplayContractSwapBootResumeOwner {
		t.Fatalf("admission ownership = %#v", admission)
	}
	if admission.SelectedBindingOwner != runfork.RunForkSelectedContractBindingOwner ||
		admission.SelectedExecutionAdmissionOwner != runfork.RunForkSelectedContractExecutionAdmissionOwner ||
		admission.ReplayResumeAdmissionOwner != runfork.RunForkReplayResumeAdmissionOwner {
		t.Fatalf("owner consumption = %#v", admission)
	}
	if admission.RouteRecoveryOwner != runfork.RunForkSelectedContractRoutePersistenceOwner ||
		admission.RuntimeRouteRecoveryOwner != runfork.RunForkSelectedContractRouteRecoveryOwner {
		t.Fatalf("route recovery owners = store:%q runtime:%q", admission.RouteRecoveryOwner, admission.RuntimeRouteRecoveryOwner)
	}
	if !executionBoundaryHas(admission.Prerequisites, "selected_contract_binding", runfork.RunForkSelectedContractDispositionPrerequisite) ||
		!executionBoundaryHas(admission.Prerequisites, "replay_resume_admission", runfork.RunForkSelectedContractDispositionPrerequisite) ||
		!executionBoundaryHas(admission.Prerequisites, "selected_contract_route_recovery", runfork.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("prerequisites = %#v, want canonical selected/replay/route owners", admission.Prerequisites)
	}
	if !contractSwapClassificationHas(admission.Classifications, runfork.RunForkReplayResumeFactTimerHistory, runfork.RunForkReplayResumeDispositionFailClosedBlocker) ||
		!contractSwapClassificationHas(admission.Classifications, runfork.RunForkReplayResumeFactContractSwap, runfork.RunForkReplayResumeDispositionFailClosedBlocker) {
		t.Fatalf("classifications = %#v, want timer and contract-swap fail-closed classifications", admission.Classifications)
	}
	if !unsupportedBlockerHas(admission.UnsupportedBlockers, runfork.RunForkBlockerTimerHistoryUnproven) ||
		!unsupportedBlockerHas(admission.UnsupportedBlockers, runfork.RunForkBlockerContractSwapBootResumeAdmissionNonMutating) {
		t.Fatalf("unsupported blockers = %#v, want replay and non-mutating blockers", admission.UnsupportedBlockers)
	}
	if !executionBoundaryHas(admission.InvalidPaths, "copy_source_event_deliveries", runfork.RunForkSelectedContractDispositionInvalid) ||
		!executionBoundaryHas(admission.InvalidPaths, "source_outcome_suppression", runfork.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("invalid paths = %#v, want source rows/outcomes invalid", admission.InvalidPaths)
	}
}

func TestBuildContractSwapBootResumeAdmissionFailsClosedWithoutRouteRecovery(t *testing.T) {
	selectedAdmission := testContractSwapSelectedExecutionAdmission(testContractSwapSelection())
	replayAdmission := runfork.RunForkReplayResumeAdmission{Owner: runfork.RunForkReplayResumeAdmissionOwner}

	admission, err := BuildContractSwapBootResumeAdmission(ContractSwapBootResumeAdmissionRequest{
		SelectedExecutionAdmission: selectedAdmission,
		ReplayResumeAdmission:      replayAdmission,
	})
	if err != nil {
		t.Fatalf("BuildContractSwapBootResumeAdmission: %v", err)
	}
	if admission.RouteRecoveryOwner != "" || admission.RuntimeRouteRecoveryOwner != "" {
		t.Fatalf("route recovery owners = %#v, want absent when route recovery evidence is missing", admission)
	}
	if !unsupportedBlockerHas(admission.UnsupportedBlockers, runfork.RunForkBlockerContractSwapRouteRecoveryMissing) {
		t.Fatalf("unsupported blockers = %#v, want route recovery missing blocker", admission.UnsupportedBlockers)
	}
}

func TestBuildContractSwapBootResumeAdmissionRejectsLocalSelectedAdmissionOwner(t *testing.T) {
	selectedAdmission := testContractSwapSelectedExecutionAdmission(testContractSwapSelection())
	selectedAdmission.Owner = "cmd.swarm.contract_swap_helper"

	_, err := BuildContractSwapBootResumeAdmission(ContractSwapBootResumeAdmissionRequest{
		SelectedExecutionAdmission: selectedAdmission,
		ReplayResumeAdmission:      runfork.RunForkReplayResumeAdmission{Owner: runfork.RunForkReplayResumeAdmissionOwner},
	})
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkSelectedContractExecutionAdmissionOwner) {
		t.Fatalf("error = %v, want canonical selected execution admission owner failure", err)
	}
}

func TestBuildContractSwapBootResumeAdmissionRejectsMismatchedRouteRecovery(t *testing.T) {
	selectedAdmission := testContractSwapSelectedExecutionAdmission(testContractSwapSelection())
	routeRecovery := testContractSwapRouteRecovery(selectedAdmission)
	routeRecovery.ForkRunID = uuid.NewString()

	_, err := BuildContractSwapBootResumeAdmission(ContractSwapBootResumeAdmissionRequest{
		SelectedExecutionAdmission: selectedAdmission,
		ReplayResumeAdmission:      runfork.RunForkReplayResumeAdmission{Owner: runfork.RunForkReplayResumeAdmissionOwner},
		RouteRecovery:              &routeRecovery,
	})
	if err == nil || !strings.Contains(err.Error(), "identity does not match") {
		t.Fatalf("error = %v, want route recovery identity failure", err)
	}
}

func contractSwapClassificationHas(items []runfork.RunForkReplayResumeDisposition, fact, disposition string) bool {
	for _, item := range items {
		if item.Fact == fact && item.Disposition == disposition {
			return true
		}
	}
	return false
}

func testContractSwapSelection() runfork.RunForkContractSelection {
	return runfork.RunForkContractSelection{
		Mode: "selected_contracts",
	}
}

func testContractSwapSelectedExecutionAdmission(selection runfork.RunForkContractSelection) runfork.RunForkSelectedContractExecutionAdmission {
	return runfork.RunForkSelectedContractExecutionAdmission{
		Owner:                      runfork.RunForkSelectedContractExecutionAdmissionOwner,
		FutureExecutionOwner:       runfork.RunForkSelectedContractExecutionOwner,
		NonMutating:                true,
		ExecutionSupported:         false,
		ForkRunID:                  uuid.NewString(),
		SourceRunID:                uuid.NewString(),
		ForkEventID:                uuid.NewString(),
		ContractSelection:          selection,
		ContractBindingOwner:       runfork.RunForkSelectedContractBindingOwner,
		AdmissionOwner:             runfork.RunForkContractFrontierAdmissionOwner,
		AdmissionUse:               runfork.RunForkSelectedContractExecutionAdmissionUseDurableBinding,
		ExecutionModelOwner:        runfork.RunForkSelectedContractExecutionModelOwner,
		DeferredWorkAdmissionOwner: runfork.RunForkSelectedContractDeferredWorkAdmissionOwner,
		SourceWorkflowName:         "workflow",
		SourceWorkflowVersion:      "v1",
		FrontierEventCount:         1,
		RouteTopology: &runfork.RunForkSelectedContractRouteTopology{
			Owner:                         runfork.RunForkSelectedContractRouteTopologyOwner,
			NonMutating:                   true,
			ContractSelection:             selection,
			FrontierEvidenceFingerprint:   "frontier-fingerprint",
			FrontierEventCount:            1,
			DynamicTopologySupported:      true,
			DynamicTopologyDisposition:    runfork.RunForkSelectedContractDispositionForkLocalTruth,
			StaticTopologySupported:       true,
			RouteAdmissionOwner:           runfork.RunForkSelectedContractRouteAdmissionOwner,
			ExecutableRecipientsSupported: false,
		},
		RecipientPlanning: &runfork.RunForkSelectedContractRecipientPlanning{
			Owner:                       runfork.RunForkSelectedContractRecipientPlanningOwner,
			RouteTopologyOwner:          runfork.RunForkSelectedContractRouteTopologyOwner,
			RouteAdmissionOwner:         runfork.RunForkSelectedContractRouteAdmissionOwner,
			FutureExecutionOwner:        runfork.RunForkSelectedContractExecutionOwner,
			NonMutating:                 true,
			RecipientPlanningSupported:  true,
			DeliveryWritesSupported:     false,
			ContractSelection:           selection,
			FrontierEvidenceFingerprint: "frontier-fingerprint",
			FrontierEventCount:          1,
		},
		UnsupportedBlockers: []runfork.RunForkUnsupportedBlocker{{
			Code:    runfork.RunForkBlockerSelectedContractExecutionAdmissionNonMutating,
			Message: "selected-contract admission is non-mutating",
		}},
	}
}

func testContractSwapRouteRecovery(admission runfork.RunForkSelectedContractExecutionAdmission) runfork.RunForkSelectedContractRouteRecovery {
	return runfork.RunForkSelectedContractRouteRecovery{
		Owner:                        runfork.RunForkSelectedContractRoutePersistenceOwner,
		RuntimeRecoveryOwner:         runfork.RunForkSelectedContractRouteRecoveryOwner,
		ForkRunID:                    admission.ForkRunID,
		SourceRunID:                  admission.SourceRunID,
		ForkEventID:                  admission.ForkEventID,
		ContractSelection:            admission.ContractSelection,
		RouteTopologyOwner:           runfork.RunForkSelectedContractRouteTopologyOwner,
		RecipientPlanningOwner:       runfork.RunForkSelectedContractRecipientPlanningOwner,
		FrontierEvidenceFingerprint:  "frontier-fingerprint",
		RouteTopologyFingerprint:     "route-fingerprint",
		RecipientPlanningFingerprint: "recipient-fingerprint",
	}
}
