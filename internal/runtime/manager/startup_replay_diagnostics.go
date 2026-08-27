package manager

import (
	"github.com/division-sh/swarm/internal/events"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type startupManagerReplayOutcome string

const (
	startupManagerReplayOutcomeReplayed startupManagerReplayOutcome = "replayed"
	startupManagerReplayOutcomeSkipped  startupManagerReplayOutcome = "skipped"
	startupManagerReplayOutcomeDropped  startupManagerReplayOutcome = "dropped"
)

type startupManagerReplayReasonCode string

const (
	startupManagerReplayReasonReplayed             startupManagerReplayReasonCode = "persisted_event_replayed"
	startupManagerReplayReasonBudgetSuppressed     startupManagerReplayReasonCode = "budget_suppressed"
	startupManagerReplayReasonDirectiveIntercepted startupManagerReplayReasonCode = "directive_intercepted"
	startupManagerReplayReasonSessionLeased        startupManagerReplayReasonCode = "session_currently_leased"
	startupManagerReplayReasonBudgetEmergency      startupManagerReplayReasonCode = "budget_emergency"
	startupManagerReplayReasonTransientAgentError  startupManagerReplayReasonCode = "transient_agent_error"
	startupManagerReplayReasonProcessFailed        startupManagerReplayReasonCode = "event_processing_failed"
	startupManagerReplayReasonDeliveryStartFailed  startupManagerReplayReasonCode = "delivery_start_failed"
	startupManagerReplayReasonPublishFailed        startupManagerReplayReasonCode = "publish_output_failed"
	startupManagerReplayReasonBacklogLoadFailed    startupManagerReplayReasonCode = "pending_backlog_load_failed"
)

type startupManagerReplayRecord struct {
	Event      events.Event
	AgentID    string
	Outcome    startupManagerReplayOutcome
	ReasonCode startupManagerReplayReasonCode
	Failure    *runtimefailures.Envelope
}

type StartupReplaySummary struct {
	ReplayedCount       int
	SkippedCount        int
	DroppedCount        int
	FirstDroppedFailure *runtimefailures.Envelope
}
