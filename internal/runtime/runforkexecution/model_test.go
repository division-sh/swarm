package runforkexecution

import (
	"strings"
	"testing"

	"swarm/internal/store"
)

func TestBuildSelectedContractExecutionModelConsumesAdmissionAsEvidenceOnly(t *testing.T) {
	admission := store.RunForkContractFrontierAdmission{
		Owner:                        store.RunForkContractFrontierAdmissionOwner,
		NonMutating:                  true,
		HistoricalExecutionSupported: false,
		ContractSelection: store.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/contracts",
			WorkflowName:    "workflow",
			WorkflowVersion: "v1",
		},
		FrontierEventCount: 1,
		FrontierEvents: []store.RunForkContractFrontierEvent{{
			SourceEventID:           "source-event",
			EventName:               "work.begin",
			RuntimeEventOwners:      []string{"alpha-intake"},
			WorkflowNodeSubscribers: []string{"beta-intake"},
			DerivedRecipients: []store.RunForkContractFrontierRecipient{{
				SubscriberType: "node",
				SubscriberID:   "alpha-intake",
				Path:           "flow-a/alpha-intake",
				RouteSource:    "selected_contracts",
			}},
		}},
	}

	routeAdmission := testSelectedContractRouteAdmission(admission)
	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractExecutionModel: %v", err)
	}
	if model.Owner != store.RunForkSelectedContractExecutionModelOwner ||
		model.FutureExecutionOwner != store.RunForkSelectedContractExecutionOwner ||
		!model.NonMutating ||
		model.ExecutionSupported {
		t.Fatalf("model ownership = %#v", model)
	}
	if model.AdmissionOwner != store.RunForkContractFrontierAdmissionOwner ||
		model.AdmissionUse != store.RunForkSelectedContractExecutionAdmissionUseEvidenceOnly {
		t.Fatalf("admission use = owner:%q use:%q", model.AdmissionOwner, model.AdmissionUse)
	}
	if model.ContractBinding.Concept != "selected_contract_binding" ||
		model.ContractBinding.Disposition != store.RunForkSelectedContractDispositionPrerequisite ||
		model.ContractBinding.Owner != store.RunForkSelectedContractBindingOwner {
		t.Fatalf("contract binding boundary = %#v, want canonical selected-contract binding owner", model.ContractBinding)
	}
	if model.FrontierEventCount != 1 || len(model.FrontierEvents) != 1 {
		t.Fatalf("frontier events = %#v", model.FrontierEvents)
	}
	if model.FrontierEvents[0].Disposition != store.RunForkSelectedContractDispositionEvidenceOnly {
		t.Fatalf("frontier disposition = %q", model.FrontierEvents[0].Disposition)
	}
	if model.RouteAdmission == nil || model.RouteAdmission.Owner != store.RunForkSelectedContractRouteAdmissionOwner {
		t.Fatalf("route admission = %#v, want canonical selected-contract route admission", model.RouteAdmission)
	}
	if !executionBoundaryHas(model.InvalidPaths, "copy_source_event_deliveries", store.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("invalid paths = %#v, want source delivery copying invalid", model.InvalidPaths)
	}
	if !executionBoundaryHas(model.RequiredConsumers, "handler_execution", store.RunForkSelectedContractDispositionFutureOwnerRequired) {
		t.Fatalf("required consumers = %#v, want handler execution future owner", model.RequiredConsumers)
	}
	if !executionBoundaryHas(model.RequiredConsumers, "safe_agent_delivery_event_replay", store.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("required consumers = %#v, want safe-agent replay as prerequisite", model.RequiredConsumers)
	}
	if !executionBoundaryHas(model.BlockedSiblings, "sessions_turns_audits", store.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("blocked siblings = %#v, want sessions/turns blocked", model.BlockedSiblings)
	}
	if !unsupportedBlockerHas(model.UnsupportedBlockers, store.RunForkBlockerSelectedContractExecutionModelNonMutating) {
		t.Fatalf("unsupported blockers = %#v, want non-mutating model blocker", model.UnsupportedBlockers)
	}
	if !unsupportedBlockerHas(model.UnsupportedBlockers, store.RunForkBlockerSelectedContractRouteAdmissionNonMutating) {
		t.Fatalf("unsupported blockers = %#v, want non-mutating route admission blocker", model.UnsupportedBlockers)
	}
}

func TestBuildSelectedContractExecutionModelCarriesFrontierBlockers(t *testing.T) {
	admission := store.RunForkContractFrontierAdmission{
		Owner:                        store.RunForkContractFrontierAdmissionOwner,
		NonMutating:                  true,
		HistoricalExecutionSupported: false,
		ContractSelection: store.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/contracts",
			WorkflowName:    "workflow",
			WorkflowVersion: "v1",
		},
		FrontierEventCount: 1,
		FrontierEvents: []store.RunForkContractFrontierEvent{{
			SourceEventID: "source-event",
			EventName:     "ghost.event",
		}},
		UnsupportedBlockers: []store.RunForkUnsupportedBlocker{
			{Code: store.RunForkBlockerContractFrontierExecutionUnsupported},
			{Code: store.RunForkBlockerContractFrontierRouteUnresolved},
		},
	}

	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      admission,
		RouteAdmission: testSelectedContractRouteAdmission(admission),
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractExecutionModel: %v", err)
	}
	for _, code := range []string{
		store.RunForkBlockerContractFrontierExecutionUnsupported,
		store.RunForkBlockerContractFrontierRouteUnresolved,
		store.RunForkBlockerSelectedContractRouteAdmissionNonMutating,
		store.RunForkBlockerSelectedContractExecutionModelNonMutating,
	} {
		if !unsupportedBlockerHas(model.UnsupportedBlockers, code) {
			t.Fatalf("unsupported blockers = %#v, want %s", model.UnsupportedBlockers, code)
		}
	}
}

func TestBuildSelectedContractExecutionModelRequiresCanonicalRouteAdmission(t *testing.T) {
	admission := store.RunForkContractFrontierAdmission{
		Owner:                        store.RunForkContractFrontierAdmissionOwner,
		NonMutating:                  true,
		HistoricalExecutionSupported: false,
		ContractSelection: store.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/contracts",
			WorkflowName:    "workflow",
			WorkflowVersion: "v1",
		},
	}
	routeAdmission := testSelectedContractRouteAdmission(admission)
	routeAdmission.Owner = "cmd.swarm.local_route_admission"

	_, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
	})
	if err == nil || !strings.Contains(err.Error(), store.RunForkSelectedContractRouteAdmissionOwner) {
		t.Fatalf("error = %v, want canonical route admission failure", err)
	}
}

func TestBuildSelectedContractExecutionModelFailsClosedOnStaleRouteAdmissionFrontier(t *testing.T) {
	admission := store.RunForkContractFrontierAdmission{
		Owner:                        store.RunForkContractFrontierAdmissionOwner,
		NonMutating:                  true,
		HistoricalExecutionSupported: false,
		ContractSelection: store.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/contracts",
			WorkflowName:    "workflow",
			WorkflowVersion: "v1",
		},
		FrontierEventCount: 1,
		FrontierEvents: []store.RunForkContractFrontierEvent{{
			SourceEventID: "source-event",
			EventName:     "work.begin",
		}},
	}
	routeAdmission := testSelectedContractRouteAdmission(admission)
	admission.FrontierEvents[0].EventName = "work.changed"

	_, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
	})
	if err == nil || !strings.Contains(err.Error(), "frontier fingerprint mismatch") {
		t.Fatalf("error = %v, want stale route admission frontier failure", err)
	}
}

func TestBuildSelectedContractExecutionModelFailsClosedOnStaleRouteAdmissionFlowInstances(t *testing.T) {
	admission := store.RunForkContractFrontierAdmission{
		Owner:                        store.RunForkContractFrontierAdmissionOwner,
		NonMutating:                  true,
		HistoricalExecutionSupported: false,
		ContractSelection: store.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/contracts",
			WorkflowName:    "workflow",
			WorkflowVersion: "v1",
		},
		FrontierEventCount: 1,
		FrontierEvents: []store.RunForkContractFrontierEvent{{
			SourceEventID:         "source-event",
			EventName:             "review/inst-1/task.started",
			SourceClassifications: []string{store.RunForkPendingClassificationPending},
			SourceFlowInstances:   []string{"review/inst-1"},
			SourceSubscriberTypes: []string{"node"},
			SourceSubscriberIDs:   []string{"source-node"},
		}},
	}
	routeAdmission := testSelectedContractRouteAdmission(admission)
	admission.FrontierEvents[0].SourceFlowInstances = []string{"review/inst-2"}

	_, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
	})
	if err == nil || !strings.Contains(err.Error(), "frontier fingerprint mismatch") {
		t.Fatalf("error = %v, want stale route admission flow-instance failure", err)
	}
}

func TestBuildSelectedContractExecutionModelFailsClosedOnExecutableAdmission(t *testing.T) {
	_, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission: store.RunForkContractFrontierAdmission{
			Owner:                        store.RunForkContractFrontierAdmissionOwner,
			NonMutating:                  true,
			HistoricalExecutionSupported: true,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "unexpectedly supports historical execution") {
		t.Fatalf("error = %v, want executable-admission failure", err)
	}
}

func TestBuildSelectedContractExecutionModelRequiresCanonicalAdmissionOwner(t *testing.T) {
	_, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission: store.RunForkContractFrontierAdmission{
			Owner:       "cmd.swarm.local_contracts",
			NonMutating: true,
		},
	})
	if err == nil || !strings.Contains(err.Error(), store.RunForkContractFrontierAdmissionOwner) {
		t.Fatalf("error = %v, want canonical admission owner failure", err)
	}
}

func executionBoundaryHas(items []store.RunForkSelectedContractExecutionBoundary, concept, disposition string) bool {
	for _, item := range items {
		if item.Concept == concept && item.Disposition == disposition {
			return true
		}
	}
	return false
}

func unsupportedBlockerHas(items []store.RunForkUnsupportedBlocker, code string) bool {
	for _, item := range items {
		if item.Code == code {
			return true
		}
	}
	return false
}
