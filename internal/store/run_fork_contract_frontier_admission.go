package store

import "strings"

const (
	RunForkContractFrontierAdmissionOwner = "runtime.run_fork.contract_frontier_admission"

	RunForkSelectedContractDiagnosticPlatformOutcomePolicyOwner = "runtime.run_fork.contract_frontier_admission.selected_contract_diagnostic_platform_outcome_policy"

	RunForkContractFrontierDispositionLineageNoAction = "lineage_no_action"

	RunForkBlockerContractFrontierExecutionUnsupported = "contract_frontier_execution_unsupported"
	RunForkBlockerContractFrontierRouteUnresolved      = "contract_frontier_route_unresolved"
)

type RunForkContractSelection struct {
	Mode            string `json:"mode"`
	ContractsRoot   string `json:"contracts_root,omitempty"`
	WorkflowName    string `json:"workflow_name,omitempty"`
	WorkflowVersion string `json:"workflow_version,omitempty"`
}

type RunForkContractFrontierAdmission struct {
	Owner                        string                                `json:"owner"`
	ContractSelection            RunForkContractSelection              `json:"contract_selection"`
	NonMutating                  bool                                  `json:"non_mutating"`
	HistoricalExecutionSupported bool                                  `json:"historical_execution_supported"`
	FrontierEventCount           int                                   `json:"frontier_event_count"`
	FrontierEvents               []RunForkContractFrontierEvent        `json:"frontier_events,omitempty"`
	LineageOnlyEvents            []RunForkContractFrontierLineageEvent `json:"lineage_only_events,omitempty"`
	UnsupportedBlockers          []RunForkUnsupportedBlocker           `json:"unsupported_blockers,omitempty"`
}

type RunForkContractFrontierLineageEvent struct {
	SourceEventID         string   `json:"source_event_id"`
	EventName             string   `json:"event_name"`
	SourceClassifications []string `json:"source_classifications,omitempty"`
	SourceFlowInstances   []string `json:"source_flow_instances,omitempty"`
	SourceSubscriberTypes []string `json:"source_subscriber_types,omitempty"`
	SourceSubscriberIDs   []string `json:"source_subscriber_ids,omitempty"`
	Owner                 string   `json:"owner"`
	Disposition           string   `json:"disposition"`
	Reason                string   `json:"reason"`
}

type RunForkContractFrontierEvent struct {
	SourceEventID           string                             `json:"source_event_id"`
	EventName               string                             `json:"event_name"`
	SourceClassifications   []string                           `json:"source_classifications,omitempty"`
	SourceFlowInstances     []string                           `json:"source_flow_instances,omitempty"`
	SourceSubscriberTypes   []string                           `json:"source_subscriber_types,omitempty"`
	SourceSubscriberIDs     []string                           `json:"source_subscriber_ids,omitempty"`
	RuntimeEventOwners      []string                           `json:"runtime_event_owners,omitempty"`
	WorkflowNodeSubscribers []string                           `json:"workflow_node_subscribers,omitempty"`
	DerivedRecipients       []RunForkContractFrontierRecipient `json:"derived_recipients,omitempty"`
}

type RunForkContractFrontierRecipient struct {
	SubscriberType string `json:"subscriber_type"`
	SubscriberID   string `json:"subscriber_id"`
	Path           string `json:"path,omitempty"`
	RouteSource    string `json:"route_source,omitempty"`
}

func RunForkSelectedContractDiagnosticPlatformOutcomePolicyApplies(item RunForkPendingWork) bool {
	if strings.TrimSpace(item.Classification) != RunForkPendingClassificationDeadLetter {
		return false
	}
	if strings.TrimSpace(item.SubscriberType) != "platform" {
		return false
	}
	return RunForkSpecDiagnosticPlatformEvent(item.EventName)
}

func RunForkSpecDiagnosticPlatformEvent(eventName string) bool {
	switch strings.TrimSpace(eventName) {
	case "platform.runtime_log", "platform.inbound_recorded":
		return true
	default:
		return false
	}
}
