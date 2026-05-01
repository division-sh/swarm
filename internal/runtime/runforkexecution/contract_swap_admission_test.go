package runforkexecution

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"swarm/internal/store"
)

func TestBuildContractSwapBootResumeAdmissionConsumesCanonicalPrerequisites(t *testing.T) {
	selection := testContractSwapSelection()
	selectedAdmission := testContractSwapSelectedExecutionAdmission(selection)
	replayAdmission := store.RunForkReplayResumeAdmission{
		Owner:                    store.RunForkReplayResumeAdmissionOwner,
		HistoricalReplayRequired: true,
		Dispositions: []store.RunForkReplayResumeDisposition{{
			Fact:        store.RunForkReplayResumeFactTimerHistory,
			Disposition: store.RunForkReplayResumeDispositionFailClosedBlocker,
			BlockerCode: store.RunForkBlockerTimerHistoryUnproven,
			Message:     "timer history remains unproven",
		}},
		UnsupportedBlockers: []store.RunForkUnsupportedBlocker{{
			Code:    store.RunForkBlockerTimerHistoryUnproven,
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
	if admission.Owner != store.RunForkContractSwapBootResumeAdmissionOwner ||
		!admission.NonMutating ||
		admission.BootResumeSupported {
		t.Fatalf("admission ownership = %#v", admission)
	}
	if admission.SelectedBindingOwner != store.RunForkSelectedContractBindingOwner ||
		admission.SelectedExecutionAdmissionOwner != store.RunForkSelectedContractExecutionAdmissionOwner ||
		admission.ReplayResumeAdmissionOwner != store.RunForkReplayResumeAdmissionOwner {
		t.Fatalf("owner consumption = %#v", admission)
	}
	if admission.RouteRecoveryOwner != store.RunForkSelectedContractRoutePersistenceOwner ||
		admission.RuntimeRouteRecoveryOwner != store.RunForkSelectedContractRouteRecoveryOwner {
		t.Fatalf("route recovery owners = store:%q runtime:%q", admission.RouteRecoveryOwner, admission.RuntimeRouteRecoveryOwner)
	}
	if !executionBoundaryHas(admission.Prerequisites, "selected_contract_binding", store.RunForkSelectedContractDispositionPrerequisite) ||
		!executionBoundaryHas(admission.Prerequisites, "replay_resume_admission", store.RunForkSelectedContractDispositionPrerequisite) ||
		!executionBoundaryHas(admission.Prerequisites, "selected_contract_route_recovery", store.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("prerequisites = %#v, want canonical selected/replay/route owners", admission.Prerequisites)
	}
	if !contractSwapClassificationHas(admission.Classifications, store.RunForkReplayResumeFactTimerHistory, store.RunForkReplayResumeDispositionFailClosedBlocker) ||
		!contractSwapClassificationHas(admission.Classifications, store.RunForkReplayResumeFactContractSwap, store.RunForkReplayResumeDispositionFailClosedBlocker) {
		t.Fatalf("classifications = %#v, want timer and contract-swap fail-closed classifications", admission.Classifications)
	}
	if !unsupportedBlockerHas(admission.UnsupportedBlockers, store.RunForkBlockerTimerHistoryUnproven) ||
		!unsupportedBlockerHas(admission.UnsupportedBlockers, store.RunForkBlockerContractSwapBootResumeAdmissionNonMutating) {
		t.Fatalf("unsupported blockers = %#v, want replay and non-mutating blockers", admission.UnsupportedBlockers)
	}
	if !executionBoundaryHas(admission.InvalidPaths, "copy_source_event_deliveries", store.RunForkSelectedContractDispositionInvalid) ||
		!executionBoundaryHas(admission.InvalidPaths, "source_outcome_suppression", store.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("invalid paths = %#v, want source rows/outcomes invalid", admission.InvalidPaths)
	}
}

func TestBuildContractSwapBootResumeAdmissionFailsClosedWithoutRouteRecovery(t *testing.T) {
	selectedAdmission := testContractSwapSelectedExecutionAdmission(testContractSwapSelection())
	replayAdmission := store.RunForkReplayResumeAdmission{Owner: store.RunForkReplayResumeAdmissionOwner}

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
	if !unsupportedBlockerHas(admission.UnsupportedBlockers, store.RunForkBlockerContractSwapRouteRecoveryMissing) {
		t.Fatalf("unsupported blockers = %#v, want route recovery missing blocker", admission.UnsupportedBlockers)
	}
}

func TestBuildContractSwapBootResumeAdmissionRejectsLocalSelectedAdmissionOwner(t *testing.T) {
	selectedAdmission := testContractSwapSelectedExecutionAdmission(testContractSwapSelection())
	selectedAdmission.Owner = "cmd.swarm.contract_swap_helper"

	_, err := BuildContractSwapBootResumeAdmission(ContractSwapBootResumeAdmissionRequest{
		SelectedExecutionAdmission: selectedAdmission,
		ReplayResumeAdmission:      store.RunForkReplayResumeAdmission{Owner: store.RunForkReplayResumeAdmissionOwner},
	})
	if err == nil || !strings.Contains(err.Error(), store.RunForkSelectedContractExecutionAdmissionOwner) {
		t.Fatalf("error = %v, want canonical selected execution admission owner failure", err)
	}
}

func TestBuildContractSwapBootResumeAdmissionRejectsMismatchedRouteRecovery(t *testing.T) {
	selectedAdmission := testContractSwapSelectedExecutionAdmission(testContractSwapSelection())
	routeRecovery := testContractSwapRouteRecovery(selectedAdmission)
	routeRecovery.ForkRunID = uuid.NewString()

	_, err := BuildContractSwapBootResumeAdmission(ContractSwapBootResumeAdmissionRequest{
		SelectedExecutionAdmission: selectedAdmission,
		ReplayResumeAdmission:      store.RunForkReplayResumeAdmission{Owner: store.RunForkReplayResumeAdmissionOwner},
		RouteRecovery:              &routeRecovery,
	})
	if err == nil || !strings.Contains(err.Error(), "identity does not match") {
		t.Fatalf("error = %v, want route recovery identity failure", err)
	}
}

func contractSwapClassificationHas(items []store.RunForkReplayResumeDisposition, fact, disposition string) bool {
	for _, item := range items {
		if item.Fact == fact && item.Disposition == disposition {
			return true
		}
	}
	return false
}

func testContractSwapSelection() store.RunForkContractSelection {
	return store.RunForkContractSelection{
		Mode:            "selected_contracts",
		ContractsRoot:   "/tmp/contracts",
		WorkflowName:    "workflow",
		WorkflowVersion: "v1",
	}
}

func testContractSwapSelectedExecutionAdmission(selection store.RunForkContractSelection) store.RunForkSelectedContractExecutionAdmission {
	return store.RunForkSelectedContractExecutionAdmission{
		Owner:                 store.RunForkSelectedContractExecutionAdmissionOwner,
		FutureExecutionOwner:  store.RunForkSelectedContractExecutionOwner,
		NonMutating:           true,
		ExecutionSupported:    false,
		ForkRunID:             uuid.NewString(),
		SourceRunID:           uuid.NewString(),
		ForkEventID:           uuid.NewString(),
		ContractSelection:     selection,
		ContractBindingOwner:  store.RunForkSelectedContractBindingOwner,
		AdmissionOwner:        store.RunForkContractFrontierAdmissionOwner,
		AdmissionUse:          store.RunForkSelectedContractExecutionAdmissionUseDurableBinding,
		ExecutionModelOwner:   store.RunForkSelectedContractExecutionModelOwner,
		SourceWorkflowName:    selection.WorkflowName,
		SourceWorkflowVersion: selection.WorkflowVersion,
		FrontierEventCount:    1,
		RouteTopology: &store.RunForkSelectedContractRouteTopology{
			Owner:                         store.RunForkSelectedContractRouteTopologyOwner,
			NonMutating:                   true,
			ContractSelection:             selection,
			FrontierEvidenceFingerprint:   "frontier-fingerprint",
			FrontierEventCount:            1,
			DynamicTopologySupported:      true,
			DynamicTopologyDisposition:    store.RunForkSelectedContractDispositionForkLocalTruth,
			StaticTopologySupported:       true,
			RouteAdmissionOwner:           store.RunForkSelectedContractRouteAdmissionOwner,
			ExecutableRecipientsSupported: false,
		},
		RecipientPlanning: &store.RunForkSelectedContractRecipientPlanning{
			Owner:                       store.RunForkSelectedContractRecipientPlanningOwner,
			RouteTopologyOwner:          store.RunForkSelectedContractRouteTopologyOwner,
			RouteAdmissionOwner:         store.RunForkSelectedContractRouteAdmissionOwner,
			FutureExecutionOwner:        store.RunForkSelectedContractExecutionOwner,
			NonMutating:                 true,
			RecipientPlanningSupported:  true,
			DeliveryWritesSupported:     false,
			ContractSelection:           selection,
			FrontierEvidenceFingerprint: "frontier-fingerprint",
			FrontierEventCount:          1,
		},
		UnsupportedBlockers: []store.RunForkUnsupportedBlocker{{
			Code:    store.RunForkBlockerSelectedContractExecutionAdmissionNonMutating,
			Message: "selected-contract admission is non-mutating",
		}},
	}
}

func testContractSwapRouteRecovery(admission store.RunForkSelectedContractExecutionAdmission) store.RunForkSelectedContractRouteRecovery {
	return store.RunForkSelectedContractRouteRecovery{
		Owner:                        store.RunForkSelectedContractRoutePersistenceOwner,
		RuntimeRecoveryOwner:         store.RunForkSelectedContractRouteRecoveryOwner,
		ForkRunID:                    admission.ForkRunID,
		SourceRunID:                  admission.SourceRunID,
		ForkEventID:                  admission.ForkEventID,
		ContractSelection:            admission.ContractSelection,
		RouteTopologyOwner:           store.RunForkSelectedContractRouteTopologyOwner,
		RecipientPlanningOwner:       store.RunForkSelectedContractRecipientPlanningOwner,
		FrontierEvidenceFingerprint:  "frontier-fingerprint",
		RouteTopologyFingerprint:     "route-fingerprint",
		RecipientPlanningFingerprint: "recipient-fingerprint",
	}
}
