package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
)

const (
	RunForkSelectedContractExecutionModelOwner          = "runtime.run_fork.selected_contract_execution_model"
	RunForkSelectedContractExecutionAdmissionOwner      = "runtime.run_fork.selected_contract_execution_admission"
	RunForkSelectedContractExecutionActivationGateOwner = "runtime.run_fork.selected_contract_execution.activation_gate"
	RunForkSelectedContractExecutionOwner               = "runtime.run_fork.selected_contract_execution"
	RunForkSelectedContractRouteAdmissionOwner          = "runtime.run_fork.selected_contract_route_admission"
	RunForkSelectedContractRouteTopologyOwner           = "runtime.run_fork.selected_contract_route_topology"
	RunForkSelectedContractDynamicRouteTopologyOwner    = "runtime.run_fork.selected_contract_dynamic_route_topology"
	RunForkSelectedContractRecipientPlanningOwner       = "runtime.run_fork.selected_contract_recipient_planning"
	RunForkSelectedContractBranchDivergenceOwner        = "store.run_fork.selected_contract_branch_divergence"

	RunForkSelectedContractExecutionAdmissionUseEvidenceOnly   = "prerequisite_evidence_only"
	RunForkSelectedContractExecutionAdmissionUseDurableBinding = "durable_binding_and_frontier_evidence"

	RunForkSelectedContractDispositionEvidenceOnly        = "evidence_only"
	RunForkSelectedContractDispositionFutureOwnerRequired = "future_owner_required"
	RunForkSelectedContractDispositionBlockedSibling      = "blocked_sibling"
	RunForkSelectedContractDispositionPrerequisite        = "prerequisite"
	RunForkSelectedContractDispositionInvalid             = "invalid"
	RunForkSelectedContractDispositionForkLocalTruth      = "fork_local_truth"
	RunForkSelectedContractDispositionFailClosed          = "fail_closed"

	RunForkBlockerSelectedContractExecutionModelNonMutating     = "selected_contract_execution_model_non_mutating"
	RunForkBlockerSelectedContractExecutionAdmissionNonMutating = "selected_contract_execution_admission_non_mutating"
	RunForkBlockerSelectedContractSourceReplayUnsupported       = "selected_contract_source_replay_unsupported"
	RunForkBlockerSelectedContractRouteAdmissionNonMutating     = "selected_contract_route_admission_non_mutating"
	RunForkBlockerSelectedContractRouteTopologyNonMutating      = "selected_contract_route_topology_non_mutating"
	RunForkBlockerSelectedContractDynamicRouteTopologyUnproven  = "selected_contract_dynamic_route_topology_unproven"
	RunForkBlockerSelectedContractRecipientPlanningNonMutating  = "selected_contract_recipient_planning_non_mutating"

	RunForkSelectedContractSourceAdvancedBranchPolicy = "selected_contract_source_advanced_branch"
)

type RunForkSelectedContractExecution struct {
	Owner                string                                     `json:"owner"`
	FutureExecutionOwner string                                     `json:"future_execution_owner"`
	NonMutating          bool                                       `json:"non_mutating"`
	ExecutionSupported   bool                                       `json:"execution_supported"`
	ContractSelection    RunForkContractSelection                   `json:"contract_selection"`
	AdmissionOwner       string                                     `json:"admission_owner"`
	AdmissionUse         string                                     `json:"admission_use"`
	FrontierEventCount   int                                        `json:"frontier_event_count"`
	FrontierEvents       []RunForkSelectedContractFrontierEvent     `json:"frontier_events,omitempty"`
	RouteTopology        *RunForkSelectedContractRouteTopology      `json:"route_topology,omitempty"`
	RecipientPlanning    *RunForkSelectedContractRecipientPlanning  `json:"recipient_planning,omitempty"`
	ContractBinding      RunForkSelectedContractExecutionBoundary   `json:"contract_binding"`
	RequiredConsumers    []RunForkSelectedContractExecutionBoundary `json:"required_consumers,omitempty"`
	BlockedSiblings      []RunForkSelectedContractExecutionBoundary `json:"blocked_siblings,omitempty"`
	InvalidPaths         []RunForkSelectedContractExecutionBoundary `json:"invalid_paths,omitempty"`
	UnsupportedBlockers  []RunForkUnsupportedBlocker                `json:"unsupported_blockers,omitempty"`
}

type RunForkSelectedContractExecutionAdmission struct {
	Owner                 string                                     `json:"owner"`
	FutureExecutionOwner  string                                     `json:"future_execution_owner"`
	NonMutating           bool                                       `json:"non_mutating"`
	ExecutionSupported    bool                                       `json:"execution_supported"`
	ForkRunID             string                                     `json:"fork_run_id"`
	SourceRunID           string                                     `json:"source_run_id"`
	ForkEventID           string                                     `json:"fork_event_id"`
	ContractSelection     RunForkContractSelection                   `json:"contract_selection"`
	ContractBindingOwner  string                                     `json:"contract_binding_owner"`
	AdmissionOwner        string                                     `json:"admission_owner"`
	AdmissionUse          string                                     `json:"admission_use"`
	ExecutionModelOwner   string                                     `json:"execution_model_owner"`
	SourceWorkflowName    string                                     `json:"source_workflow_name"`
	SourceWorkflowVersion string                                     `json:"source_workflow_version"`
	FrontierEventCount    int                                        `json:"frontier_event_count"`
	FrontierEvents        []RunForkSelectedContractFrontierEvent     `json:"frontier_events,omitempty"`
	RouteTopology         *RunForkSelectedContractRouteTopology      `json:"route_topology,omitempty"`
	RecipientPlanning     *RunForkSelectedContractRecipientPlanning  `json:"recipient_planning,omitempty"`
	ContractBinding       RunForkSelectedContractExecutionBoundary   `json:"contract_binding"`
	RequiredConsumers     []RunForkSelectedContractExecutionBoundary `json:"required_consumers,omitempty"`
	BlockedSiblings       []RunForkSelectedContractExecutionBoundary `json:"blocked_siblings,omitempty"`
	InvalidPaths          []RunForkSelectedContractExecutionBoundary `json:"invalid_paths,omitempty"`
	UnsupportedBlockers   []RunForkUnsupportedBlocker                `json:"unsupported_blockers,omitempty"`
}

type RunForkSelectedContractFrontierEvent struct {
	SourceEventID           string                             `json:"source_event_id"`
	EventName               string                             `json:"event_name"`
	RuntimeEventOwners      []string                           `json:"runtime_event_owners,omitempty"`
	WorkflowNodeSubscribers []string                           `json:"workflow_node_subscribers,omitempty"`
	DerivedRecipients       []RunForkContractFrontierRecipient `json:"derived_recipients,omitempty"`
	Disposition             string                             `json:"disposition"`
}

type RunForkSelectedContractRouteAdmission struct {
	Owner                          string                                     `json:"owner"`
	FutureRouteReconstructionOwner string                                     `json:"future_route_reconstruction_owner"`
	NonMutating                    bool                                       `json:"non_mutating"`
	RouteReconstructionSupported   bool                                       `json:"route_reconstruction_supported"`
	ContractSelection              RunForkContractSelection                   `json:"contract_selection"`
	SourceRouteFactsPresent        bool                                       `json:"source_route_facts_present"`
	SelectedRouteEvents            []RunForkSelectedContractRouteEvent        `json:"selected_route_events,omitempty"`
	DynamicFlowInstances           []string                                   `json:"dynamic_flow_instances,omitempty"`
	FrontierAdmissionOwner         string                                     `json:"frontier_admission_owner,omitempty"`
	FrontierEventCount             int                                        `json:"frontier_event_count"`
	FrontierSourceEventIDs         []string                                   `json:"frontier_source_event_ids,omitempty"`
	FrontierEvidenceFingerprint    string                                     `json:"frontier_evidence_fingerprint"`
	RequiredConsumers              []RunForkSelectedContractExecutionBoundary `json:"required_consumers,omitempty"`
	BlockedSiblings                []RunForkSelectedContractExecutionBoundary `json:"blocked_siblings,omitempty"`
	InvalidPaths                   []RunForkSelectedContractExecutionBoundary `json:"invalid_paths,omitempty"`
	UnsupportedBlockers            []RunForkUnsupportedBlocker                `json:"unsupported_blockers,omitempty"`
}

type RunForkSelectedContractRouteTopology struct {
	Owner                          string                                        `json:"owner"`
	RouteAdmissionOwner            string                                        `json:"route_admission_owner"`
	FutureRouteReconstructionOwner string                                        `json:"future_route_reconstruction_owner"`
	NonMutating                    bool                                          `json:"non_mutating"`
	RoutePersistenceSupported      bool                                          `json:"route_persistence_supported"`
	ExecutableRecipientsSupported  bool                                          `json:"executable_recipients_supported"`
	ContractSelection              RunForkContractSelection                      `json:"contract_selection"`
	StaticTopologySupported        bool                                          `json:"static_topology_supported"`
	DynamicTopologySupported       bool                                          `json:"dynamic_topology_supported"`
	DynamicTopologyOwner           string                                        `json:"dynamic_topology_owner,omitempty"`
	SourceRouteFactsPresent        bool                                          `json:"source_route_facts_present"`
	StaticRouteEvents              []RunForkSelectedContractRouteEvent           `json:"static_route_events,omitempty"`
	DynamicFlowInstances           []string                                      `json:"dynamic_flow_instances,omitempty"`
	DynamicTopologyProofs          []RunForkSelectedContractDynamicTopologyProof `json:"dynamic_topology_proofs,omitempty"`
	DynamicTopologyDisposition     string                                        `json:"dynamic_topology_disposition,omitempty"`
	FrontierAdmissionOwner         string                                        `json:"frontier_admission_owner,omitempty"`
	FrontierEventCount             int                                           `json:"frontier_event_count"`
	FrontierSourceEventIDs         []string                                      `json:"frontier_source_event_ids,omitempty"`
	FrontierEvidenceFingerprint    string                                        `json:"frontier_evidence_fingerprint"`
	RequiredEvidence               []RunForkSelectedContractExecutionBoundary    `json:"required_evidence,omitempty"`
	RequiredConsumers              []RunForkSelectedContractExecutionBoundary    `json:"required_consumers,omitempty"`
	BlockedSiblings                []RunForkSelectedContractExecutionBoundary    `json:"blocked_siblings,omitempty"`
	InvalidPaths                   []RunForkSelectedContractExecutionBoundary    `json:"invalid_paths,omitempty"`
	UnsupportedBlockers            []RunForkUnsupportedBlocker                   `json:"unsupported_blockers,omitempty"`
}

type RunForkSelectedContractDynamicTopologyProof struct {
	FlowInstance      string                             `json:"flow_instance"`
	SourceEventIDs    []string                           `json:"source_event_ids,omitempty"`
	EventNames        []string                           `json:"event_names,omitempty"`
	DerivedRecipients []RunForkContractFrontierRecipient `json:"derived_recipients,omitempty"`
	Disposition       string                             `json:"disposition"`
}

type RunForkSelectedContractRouteEvent struct {
	SourceEventID     string                             `json:"source_event_id,omitempty"`
	EventName         string                             `json:"event_name"`
	DerivedRecipients []RunForkContractFrontierRecipient `json:"derived_recipients,omitempty"`
	Disposition       string                             `json:"disposition"`
}

type RunForkSelectedContractRecipientPlanning struct {
	Owner                       string                                      `json:"owner"`
	RouteTopologyOwner          string                                      `json:"route_topology_owner"`
	RouteAdmissionOwner         string                                      `json:"route_admission_owner"`
	FutureExecutionOwner        string                                      `json:"future_execution_owner"`
	NonMutating                 bool                                        `json:"non_mutating"`
	RecipientPlanningSupported  bool                                        `json:"recipient_planning_supported"`
	DeliveryWritesSupported     bool                                        `json:"delivery_writes_supported"`
	ContractSelection           RunForkContractSelection                    `json:"contract_selection"`
	FrontierEventCount          int                                         `json:"frontier_event_count"`
	FrontierSourceEventIDs      []string                                    `json:"frontier_source_event_ids,omitempty"`
	FrontierEvidenceFingerprint string                                      `json:"frontier_evidence_fingerprint"`
	RecipientPlanEvents         []RunForkSelectedContractRecipientPlanEvent `json:"recipient_plan_events,omitempty"`
	RequiredEvidence            []RunForkSelectedContractExecutionBoundary  `json:"required_evidence,omitempty"`
	RequiredConsumers           []RunForkSelectedContractExecutionBoundary  `json:"required_consumers,omitempty"`
	BlockedSiblings             []RunForkSelectedContractExecutionBoundary  `json:"blocked_siblings,omitempty"`
	InvalidPaths                []RunForkSelectedContractExecutionBoundary  `json:"invalid_paths,omitempty"`
	UnsupportedBlockers         []RunForkUnsupportedBlocker                 `json:"unsupported_blockers,omitempty"`
}

type RunForkSelectedContractRecipientPlanEvent struct {
	SourceEventID string                             `json:"source_event_id,omitempty"`
	EventName     string                             `json:"event_name"`
	Recipients    []RunForkContractFrontierRecipient `json:"recipients,omitempty"`
	Disposition   string                             `json:"disposition"`
}

type RunForkSelectedContractExecutionBoundary struct {
	Concept     string `json:"concept"`
	Disposition string `json:"disposition"`
	Owner       string `json:"owner,omitempty"`
	Reason      string `json:"reason"`
}

func RunForkContractFrontierEvidenceBinding(frontier RunForkContractFrontierAdmission) (int, []string, string) {
	type routeRecipient struct {
		SubscriberType string `json:"subscriber_type,omitempty"`
		SubscriberID   string `json:"subscriber_id,omitempty"`
		Path           string `json:"path,omitempty"`
		RouteSource    string `json:"route_source,omitempty"`
	}
	type frontierEvent struct {
		SourceEventID           string           `json:"source_event_id,omitempty"`
		EventName               string           `json:"event_name,omitempty"`
		SourceClassifications   []string         `json:"source_classifications,omitempty"`
		SourceFlowInstances     []string         `json:"source_flow_instances,omitempty"`
		SourceSubscriberTypes   []string         `json:"source_subscriber_types,omitempty"`
		SourceSubscriberIDs     []string         `json:"source_subscriber_ids,omitempty"`
		RuntimeEventOwners      []string         `json:"runtime_event_owners,omitempty"`
		WorkflowNodeSubscribers []string         `json:"workflow_node_subscribers,omitempty"`
		DerivedRecipients       []routeRecipient `json:"derived_recipients,omitempty"`
	}

	events := make([]frontierEvent, 0, len(frontier.FrontierEvents))
	ids := map[string]struct{}{}
	for _, event := range frontier.FrontierEvents {
		sourceEventID := strings.TrimSpace(event.SourceEventID)
		eventName := strings.TrimSpace(event.EventName)
		if sourceEventID != "" {
			ids[sourceEventID] = struct{}{}
		}
		recipients := make([]routeRecipient, 0, len(event.DerivedRecipients))
		for _, recipient := range event.DerivedRecipients {
			recipients = append(recipients, routeRecipient{
				SubscriberType: strings.TrimSpace(recipient.SubscriberType),
				SubscriberID:   strings.TrimSpace(recipient.SubscriberID),
				Path:           strings.TrimSpace(recipient.Path),
				RouteSource:    strings.TrimSpace(recipient.RouteSource),
			})
		}
		sort.Slice(recipients, func(i, j int) bool {
			left := strings.Join([]string{recipients[i].SubscriberType, recipients[i].SubscriberID, recipients[i].Path, recipients[i].RouteSource}, "\x00")
			right := strings.Join([]string{recipients[j].SubscriberType, recipients[j].SubscriberID, recipients[j].Path, recipients[j].RouteSource}, "\x00")
			return left < right
		})
		events = append(events, frontierEvent{
			SourceEventID:           sourceEventID,
			EventName:               eventName,
			SourceClassifications:   sortedTrimmedStrings(event.SourceClassifications),
			SourceFlowInstances:     sortedTrimmedStrings(event.SourceFlowInstances),
			SourceSubscriberTypes:   sortedTrimmedStrings(event.SourceSubscriberTypes),
			SourceSubscriberIDs:     sortedTrimmedStrings(event.SourceSubscriberIDs),
			RuntimeEventOwners:      sortedTrimmedStrings(event.RuntimeEventOwners),
			WorkflowNodeSubscribers: sortedTrimmedStrings(event.WorkflowNodeSubscribers),
			DerivedRecipients:       recipients,
		})
	}
	sort.Slice(events, func(i, j int) bool {
		left := strings.Join([]string{events[i].SourceEventID, events[i].EventName}, "\x00")
		right := strings.Join([]string{events[j].SourceEventID, events[j].EventName}, "\x00")
		return left < right
	})

	sourceEventIDs := make([]string, 0, len(ids))
	for id := range ids {
		sourceEventIDs = append(sourceEventIDs, id)
	}
	sort.Strings(sourceEventIDs)

	payload, _ := json.Marshal(events)
	sum := sha256.Sum256(payload)
	return len(frontier.FrontierEvents), sourceEventIDs, hex.EncodeToString(sum[:])
}

func sortedTrimmedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
