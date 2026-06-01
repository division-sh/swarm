package runforkexecution

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/store"
)

func TestBuildSelectedContractRouteTopologyConsumesRouteAdmissionAsPrerequisite(t *testing.T) {
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
	topology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRouteTopology: %v", err)
	}
	if topology.Owner != store.RunForkSelectedContractRouteTopologyOwner ||
		topology.RouteAdmissionOwner != store.RunForkSelectedContractRouteAdmissionOwner ||
		!topology.NonMutating ||
		topology.RoutePersistenceSupported ||
		topology.ExecutableRecipientsSupported {
		t.Fatalf("topology ownership = %#v", topology)
	}
	if !topology.StaticTopologySupported || !topology.DynamicTopologySupported {
		t.Fatalf("topology support = static:%v dynamic:%v", topology.StaticTopologySupported, topology.DynamicTopologySupported)
	}
	if len(topology.StaticRouteEvents) != len(routeAdmission.SelectedRouteEvents) {
		t.Fatalf("static route events = %#v, want route admission evidence", topology.StaticRouteEvents)
	}
	if !executionBoundaryHas(topology.RequiredEvidence, "selected_contract_route_admission", store.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("required evidence = %#v, want route admission prerequisite", topology.RequiredEvidence)
	}
	if !executionBoundaryHas(topology.InvalidPaths, "copy_source_routing_rules", store.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("invalid paths = %#v, want source routing_rules copy invalid", topology.InvalidPaths)
	}
	if !executionBoundaryHas(topology.BlockedSiblings, "recipient_delivery_writes", store.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("blocked siblings = %#v, want recipient delivery writes blocked", topology.BlockedSiblings)
	}
	if !unsupportedBlockerHas(topology.UnsupportedBlockers, store.RunForkBlockerSelectedContractRouteTopologyNonMutating) {
		t.Fatalf("unsupported blockers = %#v, want topology non-mutating blocker", topology.UnsupportedBlockers)
	}
}

func TestBuildSelectedContractRouteTopologyFailsClosedOnDynamicFlowInstances(t *testing.T) {
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
			SourceFlowInstances:   []string{"review/inst-1"},
			SourceSubscriberTypes: []string{"node"},
			SourceSubscriberIDs:   []string{"source-node"},
		}},
	}
	routeAdmission := testSelectedContractRouteAdmission(admission)
	routeAdmission.DynamicFlowInstances = []string{"review/inst-1"}

	topology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRouteTopology: %v", err)
	}
	if topology.DynamicTopologySupported {
		t.Fatalf("DynamicTopologySupported = true, want false for source dynamic topology")
	}
	if topology.DynamicTopologyDisposition != store.RunForkSelectedContractDispositionFailClosed {
		t.Fatalf("dynamic disposition = %q", topology.DynamicTopologyDisposition)
	}
	if !unsupportedBlockerHas(topology.UnsupportedBlockers, store.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven) {
		t.Fatalf("unsupported blockers = %#v, want dynamic topology blocker", topology.UnsupportedBlockers)
	}
}

func TestBuildSelectedContractRouteTopologyRequiresFrontierCorroborationForDynamicProof(t *testing.T) {
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
			EventName:     "review/inst-1/task.started",
			DerivedRecipients: []store.RunForkContractFrontierRecipient{{
				SubscriberType: "node",
				SubscriberID:   "reviewer-inst-1",
				Path:           "review/inst-1",
				RouteSource:    "selected_contracts",
			}},
		}},
	}
	routeAdmission := testSelectedContractRouteAdmission(admission)
	routeAdmission.DynamicFlowInstances = []string{"review/inst-1"}
	routeAdmission.SelectedRouteEvents = []store.RunForkSelectedContractRouteEvent{{
		SourceEventID: "source-event",
		EventName:     "review/inst-1/task.started",
		DerivedRecipients: []store.RunForkContractFrontierRecipient{{
			SubscriberType: "node",
			SubscriberID:   "reviewer-inst-1",
			Path:           "review/inst-1",
			RouteSource:    "selected_contracts",
		}},
		Disposition: store.RunForkSelectedContractDispositionEvidenceOnly,
	}}

	topology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRouteTopology: %v", err)
	}
	if topology.DynamicTopologySupported {
		t.Fatalf("DynamicTopologySupported = true, want false without frontier flow-instance proof")
	}
	if len(topology.DynamicTopologyProofs) != 0 {
		t.Fatalf("dynamic proofs = %#v, want none without frontier flow-instance proof", topology.DynamicTopologyProofs)
	}
	if !unsupportedBlockerHas(topology.UnsupportedBlockers, store.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven) {
		t.Fatalf("unsupported blockers = %#v, want dynamic topology blocker", topology.UnsupportedBlockers)
	}
}

func TestBuildSelectedContractRouteTopologyProvesDynamicFlowInstancesFromForkLocalEvidence(t *testing.T) {
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
			SourceEventID:       "source-event",
			EventName:           "review/inst-1/task.started",
			SourceFlowInstances: []string{"review/inst-1"},
			DerivedRecipients: []store.RunForkContractFrontierRecipient{{
				SubscriberType: "node",
				SubscriberID:   "reviewer-inst-1",
				Path:           "review/inst-1",
				RouteSource:    "selected_contracts",
			}},
		}},
	}
	routeAdmission := testSelectedContractRouteAdmission(admission)
	routeAdmission.DynamicFlowInstances = []string{"review/inst-1"}
	routeAdmission.SelectedRouteEvents = []store.RunForkSelectedContractRouteEvent{{
		SourceEventID: "source-event",
		EventName:     "review/inst-1/task.started",
		DerivedRecipients: []store.RunForkContractFrontierRecipient{{
			SubscriberType: "node",
			SubscriberID:   "reviewer-inst-1",
			Path:           "review/inst-1",
			RouteSource:    "selected_contracts",
		}},
		Disposition: store.RunForkSelectedContractDispositionEvidenceOnly,
	}}

	topology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRouteTopology: %v", err)
	}
	if !topology.DynamicTopologySupported {
		t.Fatalf("DynamicTopologySupported = false, want fork-local dynamic topology proof")
	}
	if topology.DynamicTopologyOwner != store.RunForkSelectedContractDynamicRouteTopologyOwner {
		t.Fatalf("dynamic topology owner = %q", topology.DynamicTopologyOwner)
	}
	if topology.DynamicTopologyDisposition != store.RunForkSelectedContractDispositionForkLocalTruth {
		t.Fatalf("dynamic disposition = %q", topology.DynamicTopologyDisposition)
	}
	if len(topology.DynamicTopologyProofs) != 1 {
		t.Fatalf("dynamic proofs = %#v, want one proof", topology.DynamicTopologyProofs)
	}
	proof := topology.DynamicTopologyProofs[0]
	if proof.FlowInstance != "review/inst-1" ||
		proof.Disposition != store.RunForkSelectedContractDispositionForkLocalTruth ||
		len(proof.DerivedRecipients) != 1 ||
		proof.DerivedRecipients[0].SubscriberID != "reviewer-inst-1" {
		t.Fatalf("dynamic proof = %#v", proof)
	}
	if !executionBoundaryHas(topology.RequiredEvidence, "selected_contract_dynamic_route_topology", store.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("required evidence = %#v, want dynamic topology prerequisite", topology.RequiredEvidence)
	}
	if unsupportedBlockerHas(topology.UnsupportedBlockers, store.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven) {
		t.Fatalf("unsupported blockers = %#v, want dynamic topology proof to clear blocker", topology.UnsupportedBlockers)
	}
}

func TestBuildSelectedContractRecipientPlanningConsumesRouteTopology(t *testing.T) {
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
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, admission, routeAdmission)

	planning, err := BuildSelectedContractRecipientPlanning(SelectedContractRecipientPlanningRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRecipientPlanning: %v", err)
	}
	if planning.Owner != store.RunForkSelectedContractRecipientPlanningOwner ||
		planning.RouteTopologyOwner != store.RunForkSelectedContractRouteTopologyOwner ||
		!planning.NonMutating ||
		!planning.RecipientPlanningSupported ||
		planning.DeliveryWritesSupported {
		t.Fatalf("recipient planning ownership = %#v", planning)
	}
	if len(planning.RecipientPlanEvents) != 1 ||
		planning.RecipientPlanEvents[0].SourceEventID != "source-event" ||
		planning.RecipientPlanEvents[0].EventName != "work.begin" ||
		len(planning.RecipientPlanEvents[0].Recipients) != 1 ||
		planning.RecipientPlanEvents[0].Recipients[0].SubscriberID != "alpha-intake" {
		t.Fatalf("recipient plan events = %#v", planning.RecipientPlanEvents)
	}
	if !executionBoundaryHas(planning.RequiredEvidence, "selected_contract_route_topology", store.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("required evidence = %#v, want route topology prerequisite", planning.RequiredEvidence)
	}
	if !executionBoundaryHas(planning.RequiredConsumers, "selected_execution_publish_path", store.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("required consumers = %#v, want selected execution publish-path consumer", planning.RequiredConsumers)
	}
	if !executionBoundaryHas(planning.InvalidPaths, "source_event_deliveries_as_recipient_truth", store.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("invalid paths = %#v, want source deliveries invalid", planning.InvalidPaths)
	}
	if !unsupportedBlockerHas(planning.UnsupportedBlockers, store.RunForkBlockerSelectedContractRecipientPlanningNonMutating) {
		t.Fatalf("unsupported blockers = %#v, want recipient-planning non-mutating blocker", planning.UnsupportedBlockers)
	}
}

func TestBuildSelectedContractRecipientPlanningConsumesProvenDynamicTopology(t *testing.T) {
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
			SourceEventID:       "source-event",
			EventName:           "review/inst-1/task.started",
			SourceFlowInstances: []string{"review/inst-1"},
			DerivedRecipients: []store.RunForkContractFrontierRecipient{{
				SubscriberType: "node",
				SubscriberID:   "reviewer-inst-1",
				Path:           "review/inst-1",
				RouteSource:    "selected_contracts",
			}},
		}},
	}
	routeAdmission := testSelectedContractRouteAdmission(admission)
	routeAdmission.DynamicFlowInstances = []string{"review/inst-1"}
	routeAdmission.SelectedRouteEvents = []store.RunForkSelectedContractRouteEvent{{
		SourceEventID: "source-event",
		EventName:     "review/inst-1/task.started",
		DerivedRecipients: []store.RunForkContractFrontierRecipient{{
			SubscriberType: "node",
			SubscriberID:   "reviewer-inst-1",
			Path:           "review/inst-1",
			RouteSource:    "selected_contracts",
		}},
		Disposition: store.RunForkSelectedContractDispositionEvidenceOnly,
	}}
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, admission, routeAdmission)

	planning, err := BuildSelectedContractRecipientPlanning(SelectedContractRecipientPlanningRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRecipientPlanning: %v", err)
	}
	if !planning.RecipientPlanningSupported {
		t.Fatalf("RecipientPlanningSupported = false, want proven dynamic topology to allow planning; blockers=%#v", planning.UnsupportedBlockers)
	}
	if unsupportedBlockerHas(planning.UnsupportedBlockers, store.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven) {
		t.Fatalf("unsupported blockers = %#v, want dynamic blocker cleared", planning.UnsupportedBlockers)
	}
	if !executionBoundaryHas(planning.RequiredEvidence, "selected_contract_dynamic_route_topology", store.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("required evidence = %#v, want dynamic topology evidence consumed", planning.RequiredEvidence)
	}
}

func TestBuildSelectedContractRecipientPlanningKeepsDynamicTopologyBlocked(t *testing.T) {
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
			SourceFlowInstances:   []string{"review/inst-1"},
			SourceSubscriberTypes: []string{"node"},
			SourceSubscriberIDs:   []string{"source-node"},
		}},
	}
	routeAdmission := testSelectedContractRouteAdmission(admission)
	routeAdmission.DynamicFlowInstances = []string{"review/inst-1"}
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, admission, routeAdmission)

	planning, err := BuildSelectedContractRecipientPlanning(SelectedContractRecipientPlanningRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRecipientPlanning: %v", err)
	}
	if planning.RecipientPlanningSupported {
		t.Fatalf("RecipientPlanningSupported = true, want false for unproven dynamic topology")
	}
	if !unsupportedBlockerHas(planning.UnsupportedBlockers, store.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven) {
		t.Fatalf("unsupported blockers = %#v, want dynamic topology blocker", planning.UnsupportedBlockers)
	}
}

func TestBuildSelectedContractExecutionModelConsumesRouteTopologyAsTruth(t *testing.T) {
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
	routeTopology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRouteTopology: %v", err)
	}
	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
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
	if model.RouteTopology == nil || model.RouteTopology.Owner != store.RunForkSelectedContractRouteTopologyOwner {
		t.Fatalf("route topology = %#v, want canonical selected-contract route topology", model.RouteTopology)
	}
	if model.RecipientPlanning == nil || model.RecipientPlanning.Owner != store.RunForkSelectedContractRecipientPlanningOwner {
		t.Fatalf("recipient planning = %#v, want canonical selected-contract recipient planning", model.RecipientPlanning)
	}
	if !executionBoundaryHas(model.RequiredConsumers, "fork_local_recipient_planning", store.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("required consumers = %#v, want recipient planning prerequisite", model.RequiredConsumers)
	}
	if !executionBoundaryHas(model.InvalidPaths, "copy_source_event_deliveries", store.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("invalid paths = %#v, want source delivery copying invalid", model.InvalidPaths)
	}
	if !executionBoundaryHas(model.RequiredConsumers, "fork_local_runtime_container", store.RunForkSelectedContractDispositionPrerequisite) ||
		!executionBoundaryHas(model.RequiredConsumers, "fork_run_id_runtime_context", store.RunForkSelectedContractDispositionPrerequisite) ||
		!executionBoundaryHas(model.RequiredConsumers, "fork_local_event_delivery_writes", store.RunForkSelectedContractDispositionPrerequisite) ||
		!executionBoundaryHas(model.RequiredConsumers, "handler_execution", store.RunForkSelectedContractDispositionPrerequisite) ||
		!executionBoundaryHas(model.RequiredConsumers, "emitted_follow_up_events", store.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("required consumers = %#v, want current runtime container prerequisites", model.RequiredConsumers)
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

	routeAdmission := testSelectedContractRouteAdmission(admission)
	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
		RouteTopology:  testSelectedContractRouteTopologyFromAdmission(t, admission, routeAdmission),
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractExecutionModel: %v", err)
	}
	for _, code := range []string{
		store.RunForkBlockerContractFrontierExecutionUnsupported,
		store.RunForkBlockerContractFrontierRouteUnresolved,
		store.RunForkBlockerSelectedContractRouteAdmissionNonMutating,
		store.RunForkBlockerSelectedContractRouteTopologyNonMutating,
		store.RunForkBlockerSelectedContractRecipientPlanningNonMutating,
		store.RunForkBlockerSelectedContractExecutionModelNonMutating,
	} {
		if !unsupportedBlockerHas(model.UnsupportedBlockers, code) {
			t.Fatalf("unsupported blockers = %#v, want %s", model.UnsupportedBlockers, code)
		}
	}
}

func TestBuildSelectedContractExecutionModelRequiresCanonicalRouteTopology(t *testing.T) {
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
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, admission, routeAdmission)
	routeTopology.Owner = "cmd.swarm.local_route_topology"

	_, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err == nil || !strings.Contains(err.Error(), store.RunForkSelectedContractRouteTopologyOwner) {
		t.Fatalf("error = %v, want canonical route topology failure", err)
	}
}

func TestBuildSelectedContractExecutionModelRejectsForgedDynamicRouteTopology(t *testing.T) {
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
	routeAdmission.DynamicFlowInstances = []string{"review/inst-1"}
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, admission, routeAdmission)
	routeTopology.DynamicFlowInstances = nil
	routeTopology.DynamicTopologySupported = true
	routeTopology.DynamicTopologyDisposition = store.RunForkSelectedContractDispositionForkLocalTruth
	routeTopology.UnsupportedBlockers = removeUnsupportedBlocker(routeTopology.UnsupportedBlockers, store.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven)

	_, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err == nil || !strings.Contains(err.Error(), "canonical route-admission evidence") {
		t.Fatalf("error = %v, want forged dynamic route topology failure", err)
	}
}

func TestBuildSelectedContractExecutionModelRejectsForgedRouteAdmissionBlockerTopology(t *testing.T) {
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
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, admission, routeAdmission)
	routeTopology.UnsupportedBlockers = removeUnsupportedBlocker(routeTopology.UnsupportedBlockers, store.RunForkBlockerSelectedContractRouteAdmissionNonMutating)

	_, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err == nil || !strings.Contains(err.Error(), "canonical route-admission evidence") {
		t.Fatalf("error = %v, want forged route-admission blocker topology failure", err)
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
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, admission, routeAdmission)
	admission.FrontierEvents[0].EventName = "work.changed"

	_, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err == nil || !strings.Contains(err.Error(), "frontier fingerprint mismatch") {
		t.Fatalf("error = %v, want stale route topology frontier failure", err)
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
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, admission, routeAdmission)
	admission.FrontierEvents[0].SourceFlowInstances = []string{"review/inst-2"}

	_, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      admission,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err == nil || !strings.Contains(err.Error(), "frontier fingerprint mismatch") {
		t.Fatalf("error = %v, want stale route topology flow-instance failure", err)
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

func removeUnsupportedBlocker(items []store.RunForkUnsupportedBlocker, code string) []store.RunForkUnsupportedBlocker {
	out := make([]store.RunForkUnsupportedBlocker, 0, len(items))
	for _, item := range items {
		if item.Code != code {
			out = append(out, item)
		}
	}
	return out
}
