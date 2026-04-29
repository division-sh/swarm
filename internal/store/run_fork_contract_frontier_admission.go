package store

const (
	RunForkContractFrontierAdmissionOwner = "runtime.run_fork.contract_frontier_admission"

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
	Owner                        string                         `json:"owner"`
	ContractSelection            RunForkContractSelection       `json:"contract_selection"`
	NonMutating                  bool                           `json:"non_mutating"`
	HistoricalExecutionSupported bool                           `json:"historical_execution_supported"`
	FrontierEventCount           int                            `json:"frontier_event_count"`
	FrontierEvents               []RunForkContractFrontierEvent `json:"frontier_events,omitempty"`
	UnsupportedBlockers          []RunForkUnsupportedBlocker    `json:"unsupported_blockers,omitempty"`
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
