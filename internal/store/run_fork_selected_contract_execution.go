package store

const (
	RunForkSelectedContractExecutionModelOwner = "runtime.run_fork.selected_contract_execution_model"
	RunForkSelectedContractExecutionOwner      = "runtime.run_fork.selected_contract_execution"

	RunForkSelectedContractExecutionAdmissionUseEvidenceOnly = "prerequisite_evidence_only"

	RunForkSelectedContractDispositionEvidenceOnly        = "evidence_only"
	RunForkSelectedContractDispositionFutureOwnerRequired = "future_owner_required"
	RunForkSelectedContractDispositionBlockedSibling      = "blocked_sibling"
	RunForkSelectedContractDispositionPrerequisite        = "prerequisite"
	RunForkSelectedContractDispositionInvalid             = "invalid"

	RunForkBlockerSelectedContractExecutionModelNonMutating = "selected_contract_execution_model_non_mutating"
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
	ContractBinding      RunForkSelectedContractExecutionBoundary   `json:"contract_binding"`
	RequiredConsumers    []RunForkSelectedContractExecutionBoundary `json:"required_consumers,omitempty"`
	BlockedSiblings      []RunForkSelectedContractExecutionBoundary `json:"blocked_siblings,omitempty"`
	InvalidPaths         []RunForkSelectedContractExecutionBoundary `json:"invalid_paths,omitempty"`
	UnsupportedBlockers  []RunForkUnsupportedBlocker                `json:"unsupported_blockers,omitempty"`
}

type RunForkSelectedContractFrontierEvent struct {
	SourceEventID           string                             `json:"source_event_id"`
	EventName               string                             `json:"event_name"`
	RuntimeEventOwners      []string                           `json:"runtime_event_owners,omitempty"`
	WorkflowNodeSubscribers []string                           `json:"workflow_node_subscribers,omitempty"`
	DerivedRecipients       []RunForkContractFrontierRecipient `json:"derived_recipients,omitempty"`
	Disposition             string                             `json:"disposition"`
}

type RunForkSelectedContractExecutionBoundary struct {
	Concept     string `json:"concept"`
	Disposition string `json:"disposition"`
	Owner       string `json:"owner,omitempty"`
	Reason      string `json:"reason"`
}
