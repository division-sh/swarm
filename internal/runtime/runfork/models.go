package runfork

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"fmt"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"

	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/google/uuid"
)

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

const (
	RunForkActivatedStatus    = "running"
	RunForkSourceFrozenStatus = "forked"
)

type RunForkActivateRequest struct {
	ForkRunID                         string
	ConfirmSourceFreeze               bool
	HistoricalReplayExecutionAdmitter RunForkHistoricalReplayExecutionAdmitter
}

type RunForkActivation struct {
	SourceRunID               string                                   `json:"source_run_id"`
	ForkRunID                 string                                   `json:"fork_run_id"`
	ForkRunStatus             string                                   `json:"fork_run_status"`
	SourceRunStatus           string                                   `json:"source_run_status"`
	ForkPoint                 RunForkPoint                             `json:"fork_point"`
	Activated                 bool                                     `json:"activated"`
	SourceFrozen              bool                                     `json:"source_frozen"`
	ReplayResumeBlocked       bool                                     `json:"replay_resume_blocked"`
	ReplayResumeAdmission     RunForkReplayResumeAdmission             `json:"replay_resume_admission"`
	UnsupportedBlockers       []RunForkUnsupportedBlocker              `json:"unsupported_blockers,omitempty"`
	MaterializedEntityCount   int                                      `json:"materialized_entity_count"`
	HistoricalReplayExecution *RunForkHistoricalReplayExecution        `json:"historical_replay_execution,omitempty"`
	DeliveryEventReplay       *RunForkDeliveryEventReplayResult        `json:"delivery_event_replay,omitempty"`
	SelectedContractBinding   *RunForkSelectedContractBinding          `json:"selected_contract_binding,omitempty"`
	BranchDivergence          *RunForkSelectedContractBranchDivergence `json:"selected_contract_branch_divergence,omitempty"`
	SourceAdvancedAfterFork   bool                                     `json:"source_advanced_after_fork_point,omitempty"`
	RepeatedActivationFailed  bool                                     `json:"repeated_activation_failed,omitempty"`
}

const (
	RunForkContractFrontierAdmissionOwner = "runtime.run_fork.contract_frontier_admission"

	RunForkSelectedContractDiagnosticPlatformOutcomePolicyOwner = "runtime.run_fork.contract_frontier_admission.selected_contract_diagnostic_platform_outcome_policy"
	RunForkSelectedContractPlatformActivityOwner                = "runtime.run_fork.contract_frontier_admission.platform_activity_runtime"
	RunForkSelectedContractPlatformActivityEvent                = "platform.activity_requested"

	RunForkContractSelectionModeSelectedContracts = "selected_contracts"
	RunForkContractSelectionModeBundleHash        = "bundle_hash"

	RunForkContractFrontierDispositionLineageNoAction = "lineage_no_action"

	RunForkBlockerContractFrontierExecutionUnsupported = "contract_frontier_execution_unsupported"
	RunForkBlockerContractFrontierRouteUnresolved      = "contract_frontier_route_unresolved"
)

func RunForkSelectedContractPlatformRuntimeOwner(eventName string) (string, bool) {
	if strings.TrimSpace(eventName) != RunForkSelectedContractPlatformActivityEvent {
		return "", false
	}
	return RunForkSelectedContractPlatformActivityOwner, true
}

type RunForkContractSelection struct {
	Mode       string `json:"mode"`
	BundleHash string `json:"bundle_hash,omitempty"`
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
	Recipient     events.DeliveryRecipient `json:"-"`
	Path          string                   `json:"path,omitempty"`
	routeSource   string
	AgentIdentity agentidentity.Identity `json:"agent_identity,omitempty"`
}

func NewRunForkContractFrontierRecipient(recipient events.DeliveryRecipient, path, routeSource string, identity agentidentity.Identity) RunForkContractFrontierRecipient {
	if recipient.Empty() {
		return RunForkContractFrontierRecipient{}
	}
	return RunForkContractFrontierRecipient{
		Recipient: recipient, Path: strings.TrimSpace(path), routeSource: strings.TrimSpace(routeSource),
		AgentIdentity: identity.Normalize(),
	}
}

func (r RunForkContractFrontierRecipient) RouteSourceCode() string { return r.routeSource }

type runForkContractFrontierRecipientWire struct {
	SubscriberType string                 `json:"subscriber_type"`
	SubscriberID   string                 `json:"subscriber_id"`
	Path           string                 `json:"path,omitempty"`
	RouteSource    string                 `json:"route_source,omitempty"`
	AgentIdentity  agentidentity.Identity `json:"agent_identity,omitempty"`
}

func (r RunForkContractFrontierRecipient) MarshalJSON() ([]byte, error) {
	if r.Recipient.Empty() {
		return nil, fmt.Errorf("run-fork contract frontier recipient is required")
	}
	return json.Marshal(runForkContractFrontierRecipientWire{
		SubscriberType: r.Recipient.Code(), SubscriberID: r.Recipient.ID(), Path: strings.TrimSpace(r.Path),
		RouteSource: r.RouteSourceCode(), AgentIdentity: r.AgentIdentity.Normalize(),
	})
}

func (r *RunForkContractFrontierRecipient) UnmarshalJSON(raw []byte) error {
	if r == nil {
		return fmt.Errorf("run-fork contract frontier recipient destination is nil")
	}
	var wire runForkContractFrontierRecipientWire
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return fmt.Errorf("decode run-fork contract frontier recipient: %w", err)
	}
	var (
		recipient events.DeliveryRecipient
		err       error
	)
	switch strings.TrimSpace(wire.SubscriberType) {
	case "node":
		var node runtimeidentity.ExecutableNode
		node, err = runtimeidentity.ParseExecutableNodeKey(wire.SubscriberID)
		if err == nil {
			recipient, err = events.NewNodeDeliveryRecipient(node)
		}
	case "agent":
		recipient, err = events.NewAgentDeliveryRecipient(wire.SubscriberID)
	default:
		err = fmt.Errorf("recipient kind is invalid")
	}
	if err != nil {
		return fmt.Errorf("decode run-fork contract frontier recipient: %w", err)
	}
	*r = NewRunForkContractFrontierRecipient(recipient, wire.Path, wire.RouteSource, wire.AgentIdentity)
	return nil
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

const (
	RunForkDeliveryEventReplayOwner = "store.run_fork.delivery_event_replay"
)

type RunForkDeliveryEventReplayResult struct {
	Owner                 string `json:"owner"`
	SourceRunID           string `json:"source_run_id"`
	ForkRunID             string `json:"fork_run_id"`
	ReplayedEventCount    int    `json:"replayed_event_count"`
	ReplayedDeliveryCount int    `json:"replayed_delivery_count"`
}

const (
	RunForkMaterializedEntitySnapshotMetadataOwner = "runtime.run_fork.materialized_entity_snapshot_metadata"

	RunForkMaterializedEntitySnapshotMetadataSourceEvent       = "source_event"
	RunForkMaterializedEntitySnapshotMetadataSourceEntityState = "source_entity_state"
)

type RunForkMaterializedEntitySnapshotMetadata struct {
	Owner        string `json:"owner"`
	FlowInstance string `json:"flow_instance"`
	EntityType   string `json:"entity_type"`
	Slug         string `json:"slug,omitempty"`
	Name         string `json:"name,omitempty"`
	Source       string `json:"source"`
}

const (
	RunForkMaterializedStatus = "paused"
)

type RunForkMaterializeRequest struct {
	SourceRunID             string
	At                      string
	ContractSelection       *RunForkContractSelection
	SourceArtifactFact      runtimecorrelation.SourceArtifactFact
	EffectiveSourceIdentity scenarioexecution.EffectiveSourceIdentity
	DataPinOverrides        []durabledata.ExplicitPin
	FanOutPlanRefs          []runtimecontracts.FanOutPlanRef
}

type RunForkMaterialization struct {
	SourceRunID              string                          `json:"source_run_id"`
	ForkRunID                string                          `json:"fork_run_id"`
	ForkRunStatus            string                          `json:"fork_run_status"`
	ForkPoint                RunForkPoint                    `json:"fork_point"`
	MaterializedEntityCount  int                             `json:"materialized_entity_count"`
	MaterializedFanOutCount  int                             `json:"materialized_fan_out_count"`
	ExecutionReady           bool                            `json:"execution_ready"`
	ReplayResumeAdmission    RunForkReplayResumeAdmission    `json:"replay_resume_admission"`
	SelectedContractBinding  *RunForkSelectedContractBinding `json:"selected_contract_binding,omitempty"`
	UnsupportedBlockers      []RunForkUnsupportedBlocker     `json:"unsupported_blockers,omitempty"`
	DeliveryResumeBlocked    bool                            `json:"delivery_resume_blocked"`
	SourceRunStatusUnchanged bool                            `json:"source_run_status_unchanged"`
	DataPins                 []durabledata.Pin               `json:"data_pins"`
}

const (
	RunForkPendingClassificationDeliveredCompleted = "delivered_completed"
	RunForkPendingClassificationPending            = "pending"
	RunForkPendingClassificationInProgress         = "in_progress"
	RunForkPendingClassificationFailedRetryable    = "failed_retryable"
	RunForkPendingClassificationFailedTerminal     = "failed_terminal"
	RunForkPendingClassificationDeadLetter         = "dead_letter"
	RunForkPendingClassificationCommittedReplay    = "committed_replay_scope"
)

type RunForkPlanRequest struct {
	SourceRunID string
	At          string
}

type RunForkPlan struct {
	SourceRunID               string                            `json:"source_run_id"`
	SourceRunStatus           string                            `json:"source_run_status,omitempty"`
	SourceRunStartedAt        *time.Time                        `json:"source_run_started_at,omitempty"`
	SourceRunEndedAt          *time.Time                        `json:"source_run_ended_at,omitempty"`
	ForkPoint                 RunForkPoint                      `json:"fork_point"`
	EventCountAtFork          int                               `json:"event_count_at_fork"`
	ReconstructedEntityCount  int                               `json:"reconstructed_entity_count"`
	PendingWorkCount          int                               `json:"pending_work_count"`
	FanOutObligationCount     int                               `json:"fan_out_obligation_count"`
	UnsupportedBlockerCount   int                               `json:"unsupported_blocker_count"`
	ExecutionReady            bool                              `json:"execution_ready"`
	ReplayResumeAdmission     RunForkReplayResumeAdmission      `json:"replay_resume_admission"`
	ContractFrontierAdmission *RunForkContractFrontierAdmission `json:"contract_frontier_admission,omitempty"`
	SelectedContractExecution *RunForkSelectedContractExecution `json:"selected_contract_execution,omitempty"`
	SelectedContractReadiness *RunForkSelectedContractReadiness `json:"selected_contract_readiness,omitempty"`
	Entities                  []RunForkEntityState              `json:"entities,omitempty"`
	PendingWork               []RunForkPendingWork              `json:"pending_work,omitempty"`
	FanOutObligations         []RunForkFanOutObligation         `json:"fan_out_obligations,omitempty"`
	UnsupportedBlockers       []RunForkUnsupportedBlocker       `json:"unsupported_blockers,omitempty"`
	RouteHistory              RunForkRouteHistoryProjection     `json:"route_history"`
	historicalRevision        int64
	historicalEventIDs        []string
}

// RunForkFanOutObligation is the exact semantic fan-out progress visible at a
// fixed fork revision. Claim, lease, pacing, and service timestamps are not
// historical authority and are reset when this fact is materialized.
type RunForkFanOutObligation struct {
	Intent         fanoutobligation.Intent      `json:"intent"`
	Outcomes       []fanoutobligation.Outcome   `json:"outcomes,omitempty"`
	PendingReplays []RunForkFanOutPendingReplay `json:"pending_replays,omitempty"`
	Barrier        *fanoutbarrier.Barrier       `json:"barrier,omitempty"`
}

// RunForkFanOutPendingReplay is a fixed-revision nonterminal ordinal that may
// become child-local only through the existing historical delivery replay.
type RunForkFanOutPendingReplay struct {
	Ordinal       int    `json:"ordinal"`
	SourceEventID string `json:"source_event_id"`
}

// ValidateFanOutPendingReplayAdmission proves that every nonterminal fan-out
// ordinal can be reconstructed entirely by the existing historical replay.
func ValidateFanOutPendingReplayAdmission(plan RunForkPlan) error {
	for _, obligation := range plan.FanOutObligations {
		seenOrdinals := make(map[int]struct{}, obligation.Intent.Cursor)
		for _, outcome := range obligation.Outcomes {
			if outcome.Ordinal < 0 || outcome.Ordinal >= obligation.Intent.Cursor {
				return fmt.Errorf("fork fan-out outcome ordinal %d is outside cursor %d", outcome.Ordinal, obligation.Intent.Cursor)
			}
			if _, duplicate := seenOrdinals[outcome.Ordinal]; duplicate {
				return fmt.Errorf("fork fan-out ordinal %d has duplicate fixed-revision facts", outcome.Ordinal)
			}
			seenOrdinals[outcome.Ordinal] = struct{}{}
		}
		seenEvents := make(map[string]struct{}, len(obligation.PendingReplays))
		for _, replay := range obligation.PendingReplays {
			eventID := strings.TrimSpace(replay.SourceEventID)
			if replay.Ordinal < 0 || replay.Ordinal >= obligation.Intent.Cursor {
				return fmt.Errorf("fork fan-out pending ordinal %d is outside cursor %d", replay.Ordinal, obligation.Intent.Cursor)
			}
			if _, duplicate := seenOrdinals[replay.Ordinal]; duplicate {
				return fmt.Errorf("fork fan-out ordinal %d has duplicate fixed-revision facts", replay.Ordinal)
			}
			if _, err := uuid.Parse(eventID); err != nil {
				return fmt.Errorf("fork fan-out pending ordinal %d requires source event UUID: %w", replay.Ordinal, err)
			}
			if _, duplicate := seenEvents[eventID]; duplicate {
				return fmt.Errorf("fork fan-out pending source event %s is bound to multiple ordinals", eventID)
			}
			seenOrdinals[replay.Ordinal] = struct{}{}
			seenEvents[eventID] = struct{}{}

			deliveries := 0
			for _, pending := range plan.PendingWork {
				if strings.TrimSpace(pending.EventID) != eventID {
					continue
				}
				deliveries++
				if !RunForkPendingWorkReplayableForHistoricalReplay(pending) {
					return fmt.Errorf("fork fan-out pending event %s includes unsupported delivery %s", eventID, strings.TrimSpace(pending.DeliveryID))
				}
			}
			if deliveries == 0 {
				return fmt.Errorf("fork fan-out pending event %s has no replayable delivery evidence", eventID)
			}
		}
		if len(seenOrdinals) != obligation.Intent.Cursor {
			return fmt.Errorf("fork fan-out cursor %d has %d exact ordinal facts", obligation.Intent.Cursor, len(seenOrdinals))
		}
	}
	return nil
}

func ValidateFanOutPendingReplayExecution(plan RunForkPlan, work []RunForkHistoricalReplayExecutableWork) error {
	if err := ValidateFanOutPendingReplayAdmission(plan); err != nil {
		return err
	}
	workByDelivery := make(map[string]RunForkHistoricalReplayExecutableWork, len(work))
	for _, item := range work {
		workByDelivery[strings.TrimSpace(item.SourceDeliveryID)] = item
	}
	for _, obligation := range plan.FanOutObligations {
		for _, replay := range obligation.PendingReplays {
			for _, pending := range plan.PendingWork {
				if strings.TrimSpace(pending.EventID) != strings.TrimSpace(replay.SourceEventID) {
					continue
				}
				item, ok := workByDelivery[strings.TrimSpace(pending.DeliveryID)]
				if !ok || strings.TrimSpace(item.SourceEventID) != strings.TrimSpace(replay.SourceEventID) {
					return fmt.Errorf("fork fan-out pending event %s delivery %s is absent from exact replay execution", replay.SourceEventID, pending.DeliveryID)
				}
			}
		}
	}
	return nil
}

func (p RunForkPlan) WithHistoricalEvents(revision int64, eventIDs []string) RunForkPlan {
	p.historicalRevision = revision
	p.historicalEventIDs = append([]string(nil), eventIDs...)
	return p
}

func (p RunForkPlan) HistoricalEventIDs(revision int64) ([]string, bool) {
	if revision <= 0 || p.historicalRevision != revision {
		return nil, false
	}
	return append([]string(nil), p.historicalEventIDs...), true
}

type RunForkPoint struct {
	Input          string               `json:"input"`
	EventID        string               `json:"event_id"`
	EventName      string               `json:"event_name,omitempty"`
	SourceEventID  string               `json:"source_event_id,omitempty"`
	ProducedBy     string               `json:"produced_by,omitempty"`
	ProducedByType string               `json:"produced_by_type,omitempty"`
	RoutingSource  events.RoutingSource `json:"routing_source"`
	Timestamp      time.Time            `json:"timestamp"`
	Revision       int64                `json:"revision"`
}

const (
	RunForkRouteHistoryNotApplicable      = "not_applicable"
	RunForkRouteHistoryUnknownUnversioned = "unknown_unversioned"
)

type RunForkRouteHistoryProjection struct {
	State string `json:"state"`
}

type RunForkEntityState struct {
	EntityID                string                                     `json:"entity_id"`
	CurrentState            string                                     `json:"current_state,omitempty"`
	EnteredStateAt          *time.Time                                 `json:"entered_state_at,omitempty"`
	Fields                  map[string]any                             `json:"fields,omitempty"`
	Bookkeeping             map[string]any                             `json:"bookkeeping,omitempty"`
	Gates                   map[string]any                             `json:"gates,omitempty"`
	Accumulator             map[string]any                             `json:"accumulator,omitempty"`
	MaterializationMetadata *RunForkMaterializedEntitySnapshotMetadata `json:"materialization_metadata,omitempty"`
}

type RunForkPendingWork struct {
	EventID         string               `json:"event_id"`
	EventName       string               `json:"event_name"`
	FlowInstance    string               `json:"flow_instance,omitempty"`
	RoutingSource   events.RoutingSource `json:"routing_source"`
	DeliveryRoute   events.DeliveryRoute `json:"delivery_route,omitempty"`
	DeliveryID      string               `json:"delivery_id,omitempty"`
	SubscriberType  string               `json:"subscriber_type,omitempty"`
	SubscriberID    string               `json:"subscriber_id,omitempty"`
	Classification  string               `json:"classification"`
	Status          string               `json:"status,omitempty"`
	RetryCount      int                  `json:"retry_count,omitempty"`
	ReasonCode      string               `json:"reason_code,omitempty"`
	ActiveSessionID string               `json:"active_session_id,omitempty"`
	CreatedAt       time.Time            `json:"created_at"`
	StartedAt       *time.Time           `json:"started_at,omitempty"`
	DeliveredAt     *time.Time           `json:"delivered_at,omitempty"`
	ReceiptOutcome  string               `json:"receipt_outcome,omitempty"`
	ReceiptAt       *time.Time           `json:"receipt_at,omitempty"`
}

type RunForkUnsupportedBlocker struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

const (
	RunForkReplayResumeAdmissionOwner = "store.run_fork.replay_resume_admission"

	RunForkReplayResumeDispositionReconstruct        = "reconstruct"
	RunForkReplayResumeDispositionForkReplay         = "fork_replay"
	RunForkReplayResumeDispositionLineageOnly        = "lineage_only"
	RunForkReplayResumeDispositionFailClosedBlocker  = "fail_closed_blocker"
	RunForkReplayResumeDispositionSplitSibling       = "split_sibling"
	RunForkReplayResumeDispositionNoHistoricalAction = "no_historical_action"
)

const (
	RunForkReplayResumeFactEntityStateSnapshot       = "entity_state_snapshot"
	RunForkReplayResumeFactDeliveryCompletedHistory  = "delivery_completed_history"
	RunForkReplayResumeFactDeliveryPendingHistory    = "delivery_pending_history"
	RunForkReplayResumeFactDeliveryInProgressHistory = "delivery_in_progress_history"
	RunForkReplayResumeFactDeliveryFailedHistory     = "delivery_failed_history"
	RunForkReplayResumeFactDeliveryDeadLetterHistory = "delivery_dead_letter_history"
	RunForkReplayResumeFactCommittedReplayScope      = "committed_replay_scope"
	RunForkReplayResumeFactTimerHistory              = "timer_history"
	RunForkReplayResumeFactRouteHistory              = "flow_route_history"
	RunForkReplayResumeFactSessionHistory            = "session_history"
	RunForkReplayResumeFactConversationAuditHistory  = "conversation_audit_history"
	RunForkReplayResumeFactActiveTurnHistory         = "active_turn_history"
	RunForkReplayResumeFactSourceAdvanced            = "source_advanced_after_fork_point"
	RunForkReplayResumeFactForkReplayState           = "fork_replay_state"
	RunForkReplayResumeFactContractSwap              = "contract_swap"
	RunForkReplayResumeFactHistoricalReplayExecution = "historical_replay_execution"
	RunForkReplayResumeFactOpenReplyContext          = "open_reply_context"
)

const (
	RunForkBlockerDeliveryHistoryUnproven               = "delivery_history_unproven"
	RunForkBlockerNonAgentDeliveryReplayUnsupported     = "non_agent_delivery_replay_unsupported"
	RunForkBlockerCommittedReplayScopeReplayUnsupported = "committed_replay_scope_replay_unsupported"
	RunForkBlockerTimerHistoryUnproven                  = "timer_history_unproven"
	RunForkBlockerFlowRouteHistoryUnproven              = "flow_route_history_unproven"
	RunForkBlockerSessionHistoryUnproven                = "session_history_unproven"
	RunForkBlockerConversationAuditUnproven             = "conversation_audit_history_unproven"
	RunForkBlockerActiveTurnHistoryUnproven             = "active_turn_history_unproven"
	RunForkBlockerEntitySnapshotMetadataUnproven        = "entity_snapshot_metadata_unproven"
	RunForkBlockerOpenReplyContextUnsupported           = "open_reply_context_unsupported"
)

type RunForkReplayResumeAdmission struct {
	Owner                    string                           `json:"owner"`
	StateOnlyExecutionReady  bool                             `json:"state_only_execution_ready"`
	DeliveryEventReplayReady bool                             `json:"delivery_event_replay_ready"`
	BoundedReplaySupported   bool                             `json:"bounded_replay_supported"`
	ReplayResumeFactsPresent bool                             `json:"replay_resume_facts_present"`
	Dispositions             []RunForkReplayResumeDisposition `json:"dispositions,omitempty"`
	UnsupportedBlockers      []RunForkUnsupportedBlocker      `json:"unsupported_blockers,omitempty"`
}

type RunForkReplayResumeDisposition struct {
	Fact           string `json:"fact"`
	Disposition    string `json:"disposition"`
	Owner          string `json:"owner,omitempty"`
	BlockerCode    string `json:"blocker_code,omitempty"`
	Classification string `json:"classification,omitempty"`
	EntityID       string `json:"entity_id,omitempty"`
	EventID        string `json:"event_id,omitempty"`
	DeliveryID     string `json:"delivery_id,omitempty"`
	SubscriberType string `json:"subscriber_type,omitempty"`
	SubscriberID   string `json:"subscriber_id,omitempty"`
	Message        string `json:"message"`
}

// RunForkReplayResumeAdmissionWithSelectedRouteResolution discharges only the
// unversioned route-history blocker after the selected route topology and its
// persisted fork-local recovery have been validated by the caller.
func RunForkReplayResumeAdmissionWithSelectedRouteResolution(admission RunForkReplayResumeAdmission) RunForkReplayResumeAdmission {
	filtered := make([]RunForkUnsupportedBlocker, 0, len(admission.UnsupportedBlockers))
	for _, blocker := range admission.UnsupportedBlockers {
		if strings.TrimSpace(blocker.Code) == RunForkBlockerFlowRouteHistoryUnproven {
			continue
		}
		filtered = append(filtered, blocker)
	}
	admission.UnsupportedBlockers = filtered
	for i := range admission.Dispositions {
		if strings.TrimSpace(admission.Dispositions[i].Fact) != RunForkReplayResumeFactRouteHistory {
			continue
		}
		admission.Dispositions[i].Disposition = RunForkReplayResumeDispositionReconstruct
		admission.Dispositions[i].BlockerCode = ""
		admission.Dispositions[i].Owner = RunForkSelectedContractRoutePersistenceOwner
		admission.Dispositions[i].Classification = RunForkRouteHistoryUnknownUnversioned
		admission.Dispositions[i].Message = "selected frontier, binding, and static/dynamic topology proof resolve unversioned source routes into persisted fork-local route recovery"
	}
	return runForkReplayResumeAdmissionRecalculateReadiness(admission)
}

// RunForkPendingWorkReplayableForHistoricalReplay is the shared taxonomy predicate
// consumed by the runtime historical replay owner before any fork-local mutation.
func RunForkPendingWorkReplayableForHistoricalReplay(item RunForkPendingWork) bool {
	if item.Classification != RunForkPendingClassificationPending {
		return false
	}
	if strings.TrimSpace(item.DeliveryID) == "" || strings.TrimSpace(item.SubscriberID) == "" {
		return false
	}
	if strings.TrimSpace(item.SubscriberType) != "agent" {
		return false
	}
	return strings.TrimSpace(item.Status) == "pending" &&
		item.RetryCount == 0 &&
		strings.TrimSpace(item.ActiveSessionID) == "" &&
		item.StartedAt == nil &&
		item.DeliveredAt == nil &&
		item.ReceiptAt == nil
}

const (
	RunForkSelectedContractBindingOwner = "store.run_fork.selected_contract_binding"
)

type RunForkSelectedContractBindingRequest struct {
	ForkRunID         string
	SourceRunID       string
	ForkEventID       string
	ContractSelection RunForkContractSelection
}

type RunForkSelectedContractBinding struct {
	Owner             string                   `json:"owner"`
	ForkRunID         string                   `json:"fork_run_id"`
	SourceRunID       string                   `json:"source_run_id"`
	ForkEventID       string                   `json:"fork_event_id"`
	ContractSelection RunForkContractSelection `json:"contract_selection"`
	CreatedAt         time.Time                `json:"created_at"`
}

const (
	RunForkSelectedContractExecutionModelOwner                                 = "runtime.run_fork.selected_contract_execution_model"
	RunForkSelectedContractExecutionAdmissionOwner                             = "runtime.run_fork.selected_contract_execution_admission"
	RunForkSelectedContractDeferredWorkAdmissionOwner                          = "runtime.run_fork.selected_contract_deferred_work_admission"
	RunForkSelectedContractExecutionActivationGateOwner                        = "runtime.run_fork.selected_contract_execution.activation_gate"
	RunForkSelectedContractExecutionOwner                                      = "runtime.run_fork.selected_contract_execution"
	RunForkSelectedContractRouteAdmissionOwner                                 = "runtime.run_fork.selected_contract_route_admission"
	RunForkSelectedContractRouteTopologyOwner                                  = "runtime.run_fork.selected_contract_route_topology"
	RunForkSelectedContractDynamicRouteTopologyOwner                           = "runtime.run_fork.selected_contract_dynamic_route_topology"
	RunForkSelectedContractRecipientPlanningOwner                              = "runtime.run_fork.selected_contract_recipient_planning"
	RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner      = RunForkSelectedContractExecutionOwner + ".authoritative_agent_delivery_materialization"
	RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner      = RunForkSelectedContractExecutionOwner + ".fork_local_agent_runtime_materializer_executor"
	RunForkSelectedContractForkLocalRuntimeContainerOwner                      = RunForkSelectedContractExecutionOwner + ".fork_local_runtime_container"
	RunForkSelectedContractForkLocalRuntimePlatformEventLineagePolicyOwner     = RunForkSelectedContractExecutionOwner + ".fork_local_runtime_platform_event_lineage_policy"
	RunForkSelectedContractForkLocalRuntimeTypedLineageOwner                   = RunForkSelectedContractExecutionOwner + ".fork_local_runtime_typed_lineage"
	RunForkContractSwapBootResumeAdmissionOwner                                = "runtime.run_fork.contract_swap_boot_resume_admission"
	RunForkHistoricalReplayExecutionAdmissionOwner                             = "runtime.run_fork.historical_replay_execution_admission"
	RunForkHistoricalReplayExecutionOwner                                      = "runtime.run_fork.historical_replay_execution"
	RunForkHistoricalReplayContractSwapBootResumeOwner                         = RunForkHistoricalReplayExecutionOwner + ".contract_swap_boot_resume"
	RunForkSelectedContractBranchDivergenceOwner                               = "store.run_fork.selected_contract_branch_divergence"
	RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner        = RunForkSelectedContractExecutionOwner + ".source_advanced_conversation_history_policy"
	RunForkSelectedContractActiveSourceDeliveryConversationCouplingPolicyOwner = RunForkSelectedContractExecutionOwner + ".active_source_delivery_conversation_coupling_policy"

	RunForkSelectedContractExecutionAdmissionUseEvidenceOnly   = "prerequisite_evidence_only"
	RunForkSelectedContractExecutionAdmissionUseDurableBinding = "durable_binding_and_frontier_evidence"

	RunForkSelectedContractDispositionEvidenceOnly        = "evidence_only"
	RunForkSelectedContractDispositionFutureOwnerRequired = "future_owner_required"
	RunForkSelectedContractDispositionBlockedSibling      = "blocked_sibling"
	RunForkSelectedContractDispositionPrerequisite        = "prerequisite"
	RunForkSelectedContractDispositionInvalid             = "invalid"
	RunForkSelectedContractDispositionForkLocalTruth      = "fork_local_truth"
	RunForkSelectedContractDispositionFailClosed          = "fail_closed"

	RunForkBlockerSelectedContractExecutionModelNonMutating              = "selected_contract_execution_model_non_mutating"
	RunForkBlockerSelectedContractExecutionAdmissionNonMutating          = "selected_contract_execution_admission_non_mutating"
	RunForkBlockerSelectedContractSourceReplayUnsupported                = "selected_contract_source_replay_unsupported"
	RunForkBlockerSelectedContractRouteAdmissionNonMutating              = "selected_contract_route_admission_non_mutating"
	RunForkBlockerSelectedContractRouteTopologyNonMutating               = "selected_contract_route_topology_non_mutating"
	RunForkBlockerSelectedContractDynamicRouteTopologyUnproven           = "selected_contract_dynamic_route_topology_unproven"
	RunForkBlockerSelectedContractRecipientPlanningNonMutating           = "selected_contract_recipient_planning_non_mutating"
	RunForkBlockerSelectedContractAgentHandlerMaterializationUnsupported = "selected_contract_agent_handler_materialization_unsupported"
	RunForkBlockerContractSwapBootResumeAdmissionNonMutating             = "contract_swap_boot_resume_admission_non_mutating"
	RunForkBlockerContractSwapRouteRecoveryMissing                       = "contract_swap_route_recovery_missing"
	RunForkBlockerHistoricalReplayExecutionAdmissionNonMutating          = "historical_replay_execution_admission_non_mutating"

	RunForkSelectedContractSourceAdvancedBranchPolicy = "selected_contract_source_advanced_branch"

	RunForkSelectedContractActiveSourceDeliveryConversationCouplingClassification = "same_source_delivery_fork_point_emission"
)

const (
	RunForkHistoricalReplayFactSourceEvents             = "source_events"
	RunForkHistoricalReplayFactEventDeliveries          = "event_deliveries"
	RunForkHistoricalReplayFactReceipts                 = "receipts"
	RunForkHistoricalReplayFactDeadLetters              = "dead_letters"
	RunForkHistoricalReplayFactRetryIdempotency         = "retry_idempotency"
	RunForkHistoricalReplayFactEmittedFollowUps         = "emitted_follow_ups"
	RunForkHistoricalReplayFactTimers                   = "timers"
	RunForkHistoricalReplayFactRoutes                   = "routes"
	RunForkHistoricalReplayFactSessions                 = "sessions"
	RunForkHistoricalReplayFactTurns                    = "turns"
	RunForkHistoricalReplayFactAudits                   = "audits"
	RunForkHistoricalReplayFactNonAgentNodeSystemWork   = "non_agent_node_system_work"
	RunForkHistoricalReplayFactSourceAdvancedPostTFacts = "source_advanced_post_t_facts"
	RunForkHistoricalReplayFactRuntimeRestartRecovery   = "runtime_restart_recovery"
	RunForkHistoricalReplayFactCLIApiDashboardOperator  = "cli_api_dashboard_operator_consumers"

	RunForkHistoricalReplayAdmissionExecutableForkWork     = "executable_fork_work"
	RunForkHistoricalReplayAdmissionReconstructedForkState = "reconstructed_fork_local_state"
	RunForkHistoricalReplayAdmissionLineageOnlyEvidence    = "lineage_only_evidence"
	RunForkHistoricalReplayAdmissionFailClosedBlocker      = "fail_closed_blocker"
	RunForkHistoricalReplayAdmissionSplitSibling           = "split_sibling"
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
	Owner                      string                                     `json:"owner"`
	FutureExecutionOwner       string                                     `json:"future_execution_owner"`
	NonMutating                bool                                       `json:"non_mutating"`
	ExecutionSupported         bool                                       `json:"execution_supported"`
	ForkRunID                  string                                     `json:"fork_run_id"`
	SourceRunID                string                                     `json:"source_run_id"`
	ForkEventID                string                                     `json:"fork_event_id"`
	ContractSelection          RunForkContractSelection                   `json:"contract_selection"`
	ContractBindingOwner       string                                     `json:"contract_binding_owner"`
	AdmissionOwner             string                                     `json:"admission_owner"`
	AdmissionUse               string                                     `json:"admission_use"`
	ExecutionModelOwner        string                                     `json:"execution_model_owner"`
	DeferredWorkAdmissionOwner string                                     `json:"deferred_work_admission_owner"`
	SourceWorkflowName         string                                     `json:"source_workflow_name"`
	SourceWorkflowVersion      string                                     `json:"source_workflow_version"`
	FrontierEventCount         int                                        `json:"frontier_event_count"`
	FrontierEvents             []RunForkSelectedContractFrontierEvent     `json:"frontier_events,omitempty"`
	RouteTopology              *RunForkSelectedContractRouteTopology      `json:"route_topology,omitempty"`
	RecipientPlanning          *RunForkSelectedContractRecipientPlanning  `json:"recipient_planning,omitempty"`
	ContractBinding            RunForkSelectedContractExecutionBoundary   `json:"contract_binding"`
	RequiredConsumers          []RunForkSelectedContractExecutionBoundary `json:"required_consumers,omitempty"`
	BlockedSiblings            []RunForkSelectedContractExecutionBoundary `json:"blocked_siblings,omitempty"`
	InvalidPaths               []RunForkSelectedContractExecutionBoundary `json:"invalid_paths,omitempty"`
	UnsupportedBlockers        []RunForkUnsupportedBlocker                `json:"unsupported_blockers,omitempty"`
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

type RunForkContractSwapBootResumeAdmission struct {
	Owner                           string                                     `json:"owner"`
	NonMutating                     bool                                       `json:"non_mutating"`
	BootResumeSupported             bool                                       `json:"boot_resume_supported"`
	FutureExecutionOwner            string                                     `json:"future_execution_owner"`
	ForkRunID                       string                                     `json:"fork_run_id"`
	SourceRunID                     string                                     `json:"source_run_id"`
	ForkEventID                     string                                     `json:"fork_event_id"`
	ContractSelection               RunForkContractSelection                   `json:"contract_selection"`
	SelectedBindingOwner            string                                     `json:"selected_binding_owner"`
	SelectedExecutionAdmissionOwner string                                     `json:"selected_execution_admission_owner"`
	ReplayResumeAdmissionOwner      string                                     `json:"replay_resume_admission_owner"`
	RouteTopologyOwner              string                                     `json:"route_topology_owner,omitempty"`
	RouteRecoveryOwner              string                                     `json:"route_recovery_owner,omitempty"`
	RuntimeRouteRecoveryOwner       string                                     `json:"runtime_route_recovery_owner,omitempty"`
	RecipientPlanningOwner          string                                     `json:"recipient_planning_owner,omitempty"`
	SourceWorkflowName              string                                     `json:"source_workflow_name"`
	SourceWorkflowVersion           string                                     `json:"source_workflow_version"`
	FrontierEventCount              int                                        `json:"frontier_event_count"`
	Prerequisites                   []RunForkSelectedContractExecutionBoundary `json:"prerequisites,omitempty"`
	Classifications                 []RunForkReplayResumeDisposition           `json:"classifications,omitempty"`
	BlockedSiblings                 []RunForkSelectedContractExecutionBoundary `json:"blocked_siblings,omitempty"`
	InvalidPaths                    []RunForkSelectedContractExecutionBoundary `json:"invalid_paths,omitempty"`
	UnsupportedBlockers             []RunForkUnsupportedBlocker                `json:"unsupported_blockers,omitempty"`
}

type RunForkHistoricalReplayExecutionAdmission struct {
	Owner                           string                                     `json:"owner"`
	NonMutating                     bool                                       `json:"non_mutating"`
	ExecutionSupported              bool                                       `json:"execution_supported"`
	FutureExecutionOwner            string                                     `json:"future_execution_owner"`
	ForkRunID                       string                                     `json:"fork_run_id,omitempty"`
	SourceRunID                     string                                     `json:"source_run_id,omitempty"`
	ForkEventID                     string                                     `json:"fork_event_id,omitempty"`
	ContractSelection               *RunForkContractSelection                  `json:"contract_selection,omitempty"`
	ReplayResumeAdmissionOwner      string                                     `json:"replay_resume_admission_owner"`
	SelectedExecutionAdmissionOwner string                                     `json:"selected_execution_admission_owner,omitempty"`
	SelectedBindingOwner            string                                     `json:"selected_binding_owner,omitempty"`
	RouteTopologyOwner              string                                     `json:"route_topology_owner,omitempty"`
	RouteRecoveryOwner              string                                     `json:"route_recovery_owner,omitempty"`
	RuntimeRouteRecoveryOwner       string                                     `json:"runtime_route_recovery_owner,omitempty"`
	RecipientPlanningOwner          string                                     `json:"recipient_planning_owner,omitempty"`
	ContractSwapAdmissionOwner      string                                     `json:"contract_swap_admission_owner,omitempty"`
	FactAdmissions                  []RunForkHistoricalReplayFactAdmission     `json:"fact_admissions,omitempty"`
	Prerequisites                   []RunForkSelectedContractExecutionBoundary `json:"prerequisites,omitempty"`
	RequiredConsumers               []RunForkSelectedContractExecutionBoundary `json:"required_consumers,omitempty"`
	BlockedSiblings                 []RunForkSelectedContractExecutionBoundary `json:"blocked_siblings,omitempty"`
	InvalidPaths                    []RunForkSelectedContractExecutionBoundary `json:"invalid_paths,omitempty"`
	UnsupportedBlockers             []RunForkUnsupportedBlocker                `json:"unsupported_blockers,omitempty"`
}

type RunForkHistoricalReplayExecution struct {
	Owner                      string                                     `json:"owner"`
	AdmissionOwner             string                                     `json:"admission_owner"`
	ReplayResumeAdmissionOwner string                                     `json:"replay_resume_admission_owner"`
	ForkRunID                  string                                     `json:"fork_run_id"`
	SourceRunID                string                                     `json:"source_run_id"`
	ForkEventID                string                                     `json:"fork_event_id"`
	ClosureLevel               string                                     `json:"closure_level"`
	FullReplayUnsupported      bool                                       `json:"full_replay_unsupported"`
	DeliveryEventReplayReady   bool                                       `json:"delivery_event_replay_ready"`
	EventDeliveriesAdmission   RunForkHistoricalReplayFactAdmission       `json:"event_deliveries_admission"`
	FactAdmissions             []RunForkHistoricalReplayFactAdmission     `json:"fact_admissions,omitempty"`
	DeliveryEventReplayWork    []RunForkHistoricalReplayExecutableWork    `json:"delivery_event_replay_work,omitempty"`
	DeliveryEventReplay        *RunForkDeliveryEventReplayResult          `json:"delivery_event_replay,omitempty"`
	RequiredConsumers          []RunForkSelectedContractExecutionBoundary `json:"required_consumers,omitempty"`
	BlockedSiblings            []RunForkSelectedContractExecutionBoundary `json:"blocked_siblings,omitempty"`
	InvalidPaths               []RunForkSelectedContractExecutionBoundary `json:"invalid_paths,omitempty"`
}

type RunForkHistoricalReplayContractSwapBootResume struct {
	Owner                                   string                                     `json:"owner"`
	ParentHistoricalReplayExecutionOwner    string                                     `json:"parent_historical_replay_execution_owner"`
	HistoricalReplayExecutionAdmissionOwner string                                     `json:"historical_replay_execution_admission_owner"`
	ContractSwapAdmissionOwner              string                                     `json:"contract_swap_admission_owner"`
	SelectedExecutionAdmissionOwner         string                                     `json:"selected_execution_admission_owner"`
	SelectedBindingOwner                    string                                     `json:"selected_binding_owner"`
	RouteTopologyOwner                      string                                     `json:"route_topology_owner"`
	RouteRecoveryOwner                      string                                     `json:"route_recovery_owner"`
	RuntimeRouteRecoveryOwner               string                                     `json:"runtime_route_recovery_owner"`
	RecipientPlanningOwner                  string                                     `json:"recipient_planning_owner"`
	ForkRunID                               string                                     `json:"fork_run_id"`
	SourceRunID                             string                                     `json:"source_run_id"`
	ForkEventID                             string                                     `json:"fork_event_id"`
	ContractSelection                       RunForkContractSelection                   `json:"contract_selection"`
	ClosureLevel                            string                                     `json:"closure_level"`
	DeliveryEventReplayReady                bool                                       `json:"delivery_event_replay_ready"`
	ExecutableWork                          []RunForkHistoricalReplayContractSwapWork  `json:"executable_work,omitempty"`
	FactAdmissions                          []RunForkHistoricalReplayFactAdmission     `json:"fact_admissions,omitempty"`
	RequiredConsumers                       []RunForkSelectedContractExecutionBoundary `json:"required_consumers,omitempty"`
	BlockedSiblings                         []RunForkSelectedContractExecutionBoundary `json:"blocked_siblings,omitempty"`
	InvalidPaths                            []RunForkSelectedContractExecutionBoundary `json:"invalid_paths,omitempty"`
}

type RunForkHistoricalReplayContractSwapWork struct {
	Fact               string                             `json:"fact"`
	SourceEventID      string                             `json:"source_event_id"`
	SourceDeliveryIDs  []string                           `json:"source_delivery_ids"`
	EventName          string                             `json:"event_name"`
	SelectedRecipients []RunForkContractFrontierRecipient `json:"selected_recipients,omitempty"`
	Classification     string                             `json:"classification,omitempty"`
	ReasonCode         string                             `json:"reason_code,omitempty"`
}

type RunForkHistoricalReplayExecutionRequest struct {
	ForkRunID             string
	SourceRunID           string
	ForkEventID           string
	ReplayResumeAdmission RunForkReplayResumeAdmission
	PendingWork           []RunForkPendingWork
}

type RunForkHistoricalReplayExecutionAdmitter interface {
	AdmitRunForkHistoricalReplayExecution(context.Context, RunForkHistoricalReplayExecutionRequest) (RunForkHistoricalReplayExecution, error)
}

type RunForkHistoricalReplayFactAdmission struct {
	Fact        string `json:"fact"`
	Admission   string `json:"admission"`
	SourceOwner string `json:"source_owner,omitempty"`
	BlockerCode string `json:"blocker_code,omitempty"`
	Tracker     string `json:"tracker,omitempty"`
	Message     string `json:"message"`
}

type RunForkHistoricalReplayExecutableWork struct {
	Fact             string `json:"fact"`
	SourceEventID    string `json:"source_event_id"`
	SourceDeliveryID string `json:"source_delivery_id"`
	SubscriberType   string `json:"subscriber_type"`
	SubscriberID     string `json:"subscriber_id"`
	ReasonCode       string `json:"reason_code,omitempty"`
	Classification   string `json:"classification"`
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
				SubscriberType: recipient.Recipient.Code(),
				SubscriberID:   recipient.Recipient.ID(),
				Path:           strings.TrimSpace(recipient.Path),
				RouteSource:    recipient.RouteSourceCode(),
			})
		}
		sort.Slice(recipients, func(i, j int) bool {
			if recipients[i].SubscriberType != recipients[j].SubscriberType {
				return recipients[i].SubscriberType < recipients[j].SubscriberType
			}
			if recipients[i].SubscriberID != recipients[j].SubscriberID {
				return recipients[i].SubscriberID < recipients[j].SubscriberID
			}
			if recipients[i].Path != recipients[j].Path {
				return recipients[i].Path < recipients[j].Path
			}
			return recipients[i].RouteSource < recipients[j].RouteSource
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
		if events[i].SourceEventID != events[j].SourceEventID {
			return events[i].SourceEventID < events[j].SourceEventID
		}
		return events[i].EventName < events[j].EventName
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

const (
	RunForkSelectedContractExecutionLineageOwner = "store.run_fork.selected_contract_execution_lineage"
)

type RunForkSelectedContractExecutionMaterializeRequest struct {
	SourceRunID             string
	At                      string
	ContractSelection       RunForkContractSelection
	SourceArtifactFact      runtimecorrelation.SourceArtifactFact
	EffectiveSourceIdentity scenarioexecution.EffectiveSourceIdentity
	FrontierAdmission       RunForkContractFrontierAdmission
	RouteTopology           RunForkSelectedContractRouteTopology
	RecipientPlanning       RunForkSelectedContractRecipientPlanning
	WorkflowStates          []RunForkSelectedContractWorkflowState
	DataPinOverrides        []durabledata.ExplicitPin
	FanOutPlanRefs          []runtimecontracts.FanOutPlanRef
}

type RunForkSelectedContractWorkflowStateAddressKind string

const (
	RunForkSelectedContractWorkflowStateRunScope RunForkSelectedContractWorkflowStateAddressKind = "run_scope"
	RunForkSelectedContractWorkflowStateExact    RunForkSelectedContractWorkflowStateAddressKind = "exact"
)

type RunForkSelectedContractWorkflowState struct {
	SourceEventID   string
	EntityID        string
	FlowID          string
	WorkflowVersion string
	Mode            string
	AddressKind     RunForkSelectedContractWorkflowStateAddressKind
	Route           runtimeflowidentity.Route
}

type RunForkSelectedContractExecutionActivateRequest struct {
	ForkRunID             string
	ConfirmSourceFreeze   bool
	AllowedSourceEventIDs []string
	FrontierAdmission     RunForkContractFrontierAdmission
	RouteTopology         RunForkSelectedContractRouteTopology
	RecipientPlanning     RunForkSelectedContractRecipientPlanning
}

type RunForkSelectedContractSourceEvent struct {
	SourceEventID string               `json:"source_event_id"`
	EventName     string               `json:"event_name"`
	ExecutionMode executionmode.Mode   `json:"execution_mode"`
	EntityID      string               `json:"entity_id,omitempty"`
	FlowInstance  string               `json:"flow_instance,omitempty"`
	Scope         string               `json:"scope,omitempty"`
	RoutingSource events.RoutingSource `json:"routing_source"`
	Payload       json.RawMessage      `json:"payload,omitempty"`
}

type RunForkSelectedContractExecutionLineage struct {
	ForkRunID          string    `json:"fork_run_id"`
	SourceRunID        string    `json:"source_run_id"`
	SourceEventID      string    `json:"source_event_id"`
	ForkEventID        string    `json:"fork_event_id"`
	EventName          string    `json:"event_name"`
	SelectionAuthority string    `json:"selection_authority"`
	CreatedAt          time.Time `json:"created_at"`
}

type RunForkSelectedContractBranchDivergence struct {
	Owner                          string    `json:"owner"`
	ForkRunID                      string    `json:"fork_run_id"`
	SourceRunID                    string    `json:"source_run_id"`
	ForkEventID                    string    `json:"fork_event_id"`
	Policy                         string    `json:"policy"`
	SourceRunStatusAtActivation    string    `json:"source_run_status_at_activation"`
	SourceRunStatusAfterActivation string    `json:"source_run_status_after_activation"`
	SourceFrozen                   bool      `json:"source_frozen"`
	SourceAdvancedFacts            []string  `json:"source_advanced_facts,omitempty"`
	CreatedAt                      time.Time `json:"created_at"`
}

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
	RunForkSelectedContractReadinessFactOperatorConsumers           = "cli_api_dashboard_operator_consumers"
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

const (
	RunForkSelectedContractRoutePersistenceOwner = "store.run_fork.selected_contract_route_persistence"
	RunForkSelectedContractRouteRecoveryOwner    = "runtime.run_fork.selected_contract_route_recovery"
)

type RunForkSelectedContractRouteRecoveryRequest struct {
	ForkRunID         string
	SourceRunID       string
	ForkEventID       string
	ContractSelection RunForkContractSelection
	RouteTopology     RunForkSelectedContractRouteTopology
	RecipientPlanning RunForkSelectedContractRecipientPlanning
}

type RunForkSelectedContractRouteRecovery struct {
	Owner                        string                   `json:"owner"`
	RuntimeRecoveryOwner         string                   `json:"runtime_recovery_owner"`
	ForkRunID                    string                   `json:"fork_run_id"`
	SourceRunID                  string                   `json:"source_run_id"`
	ForkEventID                  string                   `json:"fork_event_id"`
	ContractSelection            RunForkContractSelection `json:"contract_selection"`
	RouteTopologyOwner           string                   `json:"route_topology_owner"`
	DynamicTopologyOwner         string                   `json:"dynamic_topology_owner,omitempty"`
	RecipientPlanningOwner       string                   `json:"recipient_planning_owner"`
	FrontierEvidenceFingerprint  string                   `json:"frontier_evidence_fingerprint"`
	RouteTopologyFingerprint     string                   `json:"route_topology_fingerprint"`
	RecipientPlanningFingerprint string                   `json:"recipient_planning_fingerprint"`
	StaticRouteEventCount        int                      `json:"static_route_event_count"`
	DynamicTopologyProofCount    int                      `json:"dynamic_topology_proof_count"`
	RecipientPlanEventCount      int                      `json:"recipient_plan_event_count"`
	RouteTopology                json.RawMessage          `json:"route_topology"`
	RecipientPlanning            json.RawMessage          `json:"recipient_planning"`
	CreatedAt                    time.Time                `json:"created_at"`
}

var ErrRunForkSourceFreezeConfirmationRequired = errors.New("run fork source freeze confirmation required")
var ErrRunForkSourceFreezeBusy = errors.New("run fork source has in-flight execution authority")

type RunForkSourceFreezeConfirmationError struct {
	SourceRunID string
	ForkRunID   string
}

func (e *RunForkSourceFreezeConfirmationError) Error() string {
	if e == nil {
		return ErrRunForkSourceFreezeConfirmationRequired.Error()
	}
	return fmt.Sprintf("%s: source_run_id=%s fork_run_id=%s", ErrRunForkSourceFreezeConfirmationRequired, strings.TrimSpace(e.SourceRunID), strings.TrimSpace(e.ForkRunID))
}

func (e *RunForkSourceFreezeConfirmationError) Unwrap() error {
	return ErrRunForkSourceFreezeConfirmationRequired
}

type RunForkSourceFreezeBusyError struct {
	SourceRunID string
	Blockers    []string
}

func (e *RunForkSourceFreezeBusyError) Error() string {
	if e == nil {
		return ErrRunForkSourceFreezeBusy.Error()
	}
	return fmt.Sprintf("%s: source_run_id=%s blockers=%s", ErrRunForkSourceFreezeBusy, strings.TrimSpace(e.SourceRunID), strings.Join(e.Blockers, ","))
}

func (e *RunForkSourceFreezeBusyError) Unwrap() error {
	return ErrRunForkSourceFreezeBusy
}

type SelectedContractRuntimeExecutionIssueRequest struct {
	Admission                  RunForkSelectedContractExecutionAdmission
	ContainerPlanFingerprint   string
	ActorCensusFingerprint     string
	EffectiveConfigFingerprint string
	ExecutionMode              executionmode.Mode
	Now                        time.Time
}

type SelectedContractRuntimeExecution struct {
	ExecutionID                     string
	ForkRunID                       string
	SourceRunID                     string
	ForkEventID                     string
	Generation                      uint64
	ExecutableCoordinateFingerprint string
	AdmissionFingerprint            string
	ContainerPlanFingerprint        string
	ActorCensusFingerprint          string
	EffectiveConfigFingerprint      string
	State                           string
	ExecutionOwner                  string
	LeaseExpiresAt                  time.Time
	FenceGeneration                 uint64
	ExecutionMode                   executionmode.Mode
}

func RunForkSelectedContractRuntimeFingerprint(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal selected-contract runtime fingerprint: %w", err)
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
