package store

const (
	RunForkSelectedContractExecutionModelOwner          = "runtime.run_fork.selected_contract_execution_model"
	RunForkSelectedContractExecutionAdmissionOwner      = "runtime.run_fork.selected_contract_execution_admission"
	RunForkSelectedContractExecutionActivationGateOwner = "runtime.run_fork.selected_contract_execution.activation_gate"
	RunForkSelectedContractExecutionOwner               = "runtime.run_fork.selected_contract_execution"
	RunForkSelectedContractBranchDivergenceOwner        = "store.run_fork.selected_contract_branch_divergence"

	RunForkSelectedContractExecutionAdmissionUseEvidenceOnly   = "prerequisite_evidence_only"
	RunForkSelectedContractExecutionAdmissionUseDurableBinding = "durable_binding_and_frontier_evidence"

	RunForkSelectedContractDispositionEvidenceOnly        = "evidence_only"
	RunForkSelectedContractDispositionFutureOwnerRequired = "future_owner_required"
	RunForkSelectedContractDispositionBlockedSibling      = "blocked_sibling"
	RunForkSelectedContractDispositionPrerequisite        = "prerequisite"
	RunForkSelectedContractDispositionInvalid             = "invalid"

	RunForkBlockerSelectedContractExecutionModelNonMutating     = "selected_contract_execution_model_non_mutating"
	RunForkBlockerSelectedContractExecutionAdmissionNonMutating = "selected_contract_execution_admission_non_mutating"
	RunForkBlockerSelectedContractSourceReplayUnsupported       = "selected_contract_source_replay_unsupported"

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

type RunForkSelectedContractExecutionBoundary struct {
	Concept     string `json:"concept"`
	Disposition string `json:"disposition"`
	Owner       string `json:"owner,omitempty"`
	Reason      string `json:"reason"`
}
