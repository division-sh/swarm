package store

const (
	RunForkPlanningOwner                            = "store.run_fork.planning_owner"
	RunForkSelectedContractReadinessClassifierOwner = "runtime.run_fork.selected_contract_readiness_classifier"
)

const (
	RunForkSelectedContractReadinessDispositionExecutableForkWork       = "executable_fork_work"
	RunForkSelectedContractReadinessDispositionLineageNoAction          = "lineage_no_action"
	RunForkSelectedContractReadinessDispositionReconstructedForkState   = "reconstructed_fork_local_state"
	RunForkSelectedContractReadinessDispositionBranchDivergenceEvidence = "branch_divergence_evidence"
	RunForkSelectedContractReadinessDispositionFailClosedBlocker        = "fail_closed_blocker"
	RunForkSelectedContractReadinessDispositionUnsupportedSplitSibling  = "unsupported_split_sibling"
)

const (
	RunForkSelectedContractReadinessFactSourceEvents                = "source_events"
	RunForkSelectedContractReadinessFactForkEvents                  = "fork_events"
	RunForkSelectedContractReadinessFactSourceDeliveries            = "source_deliveries"
	RunForkSelectedContractReadinessFactForkDeliveries              = "fork_deliveries"
	RunForkSelectedContractReadinessFactSelectedRecipientsRoutes    = "selected_recipients_and_route_topology"
	RunForkSelectedContractReadinessFactTimers                      = "timers"
	RunForkSelectedContractReadinessFactSessions                    = "sessions"
	RunForkSelectedContractReadinessFactTurns                       = "turns"
	RunForkSelectedContractReadinessFactAudits                      = "audits"
	RunForkSelectedContractReadinessFactCommittedReplayScopeMarkers = "committed_replay_scope_markers"
	RunForkSelectedContractReadinessFactPlatformRuntimeDiagnostics  = "platform_runtime_diagnostic_control_rows"
	RunForkSelectedContractReadinessFactReceipts                    = "receipts"
	RunForkSelectedContractReadinessFactDeadLetters                 = "dead_letters"
	RunForkSelectedContractReadinessFactRetryIdempotency            = "retry_idempotency"
	RunForkSelectedContractReadinessFactEmittedFollowUps            = "emitted_follow_ups"
	RunForkSelectedContractReadinessFactSourcePostTFacts            = "source_post_t_facts"
	RunForkSelectedContractReadinessFactCurrentStateSnapshots       = "current_state_materialization_snapshots"
	RunForkSelectedContractReadinessFactNonAgentNodeSystemWork      = "non_agent_node_system_work"
	RunForkSelectedContractReadinessFactRestartRecovery             = "restart_recovery"
	RunForkSelectedContractReadinessFactOperatorConsumers           = "cli_api_dashboard_builder_consumers"
)

type RunForkSelectedContractReadiness struct {
	Owner                          string                                     `json:"owner"`
	NonMutating                    bool                                       `json:"non_mutating"`
	ContractSelection              RunForkContractSelection                   `json:"contract_selection"`
	PlannerOwner                   string                                     `json:"planner_owner"`
	ReplayResumeAdmissionOwner     string                                     `json:"replay_resume_admission_owner"`
	ContractFrontierAdmissionOwner string                                     `json:"contract_frontier_admission_owner"`
	RouteAdmissionOwner            string                                     `json:"route_admission_owner,omitempty"`
	RouteTopologyOwner             string                                     `json:"route_topology_owner,omitempty"`
	DynamicRouteTopologyOwner      string                                     `json:"dynamic_route_topology_owner,omitempty"`
	RecipientPlanningOwner         string                                     `json:"recipient_planning_owner,omitempty"`
	SelectedExecutionModelOwner    string                                     `json:"selected_execution_model_owner"`
	FutureExecutionOwner           string                                     `json:"future_execution_owner"`
	FactMatrix                     []RunForkSelectedContractReadinessFact     `json:"fact_matrix"`
	RequiredConsumers              []RunForkSelectedContractExecutionBoundary `json:"required_consumers,omitempty"`
	BlockedSiblings                []RunForkSelectedContractExecutionBoundary `json:"blocked_siblings,omitempty"`
	InvalidPaths                   []RunForkSelectedContractExecutionBoundary `json:"invalid_paths,omitempty"`
	UnsupportedBlockers            []RunForkUnsupportedBlocker                `json:"unsupported_blockers,omitempty"`
}

type RunForkSelectedContractReadinessFact struct {
	Fact        string   `json:"fact"`
	Disposition string   `json:"disposition"`
	Owner       string   `json:"owner,omitempty"`
	SourceOwner string   `json:"source_owner,omitempty"`
	BlockerCode string   `json:"blocker_code,omitempty"`
	Tracker     string   `json:"tracker,omitempty"`
	Evidence    []string `json:"evidence,omitempty"`
	Message     string   `json:"message"`
}
