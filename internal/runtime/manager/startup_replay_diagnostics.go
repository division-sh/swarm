package manager

import (
	"context"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

const startupManagerReplayAction = "startup_recovery_manager_replay_aftermath"

type startupManagerReplayContextKey struct{}

func withStartupManagerReplayDiagnostics(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, startupManagerReplayContextKey{}, true)
}

func startupManagerReplayDiagnosticsEnabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	enabled, _ := ctx.Value(startupManagerReplayContextKey{}).(bool)
	return enabled
}

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

func (r startupManagerReplayRecord) detail() map[string]any {
	detail := map[string]any{
		"decision_family":      "startup_manager_replay",
		"decision_outcome":     string(r.Outcome),
		"decision_reason_code": string(r.ReasonCode),
		"event_id":             strings.TrimSpace(r.Event.ID()),
		"event_type":           strings.TrimSpace(string(r.Event.Type())),
		"agent_id":             strings.TrimSpace(r.AgentID),
		"entity_id":            r.Event.EntityID(),
		"flow_instance":        r.Event.FlowInstance(),
		"parent_event_id":      strings.TrimSpace(r.Event.ParentEventID()),
		"persisted_run_id":     strings.TrimSpace(r.Event.RunID()),
	}
	if r.Failure != nil {
		detail["failure"] = *r.Failure
	}
	return detail
}

func (r startupManagerReplayRecord) level() diaglog.Level {
	if r.Outcome == startupManagerReplayOutcomeDropped {
		return diaglog.LevelWarn
	}
	return diaglog.LevelInfo
}

func (r startupManagerReplayRecord) message() string {
	switch r.Outcome {
	case startupManagerReplayOutcomeDropped:
		return "Startup recovery dropped persisted manager replay event"
	case startupManagerReplayOutcomeSkipped:
		return "Startup recovery skipped persisted manager replay event"
	default:
		return "Startup recovery replayed persisted manager event"
	}
}

func logStartupManagerReplayAftermath(ctx context.Context, bus Bus, record startupManagerReplayRecord) {
	if bus == nil {
		return
	}
	_ = bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
		Level:     record.level(),
		Component: "agent-manager",
		Action:    startupManagerReplayAction,
		Message:   record.message(),
		EventID:   strings.TrimSpace(record.Event.ID()),
		EventType: strings.TrimSpace(string(record.Event.Type())),
		AgentID:   strings.TrimSpace(record.AgentID),
		EntityID:  record.Event.EntityID(),
		Detail:    record.detail(),
	})
}

type StartupReplaySummary struct {
	ReplayedCount       int
	SkippedCount        int
	DroppedCount        int
	FirstDroppedFailure *runtimefailures.Envelope
}

func (s *StartupReplaySummary) observe(record startupManagerReplayRecord) {
	if s == nil {
		return
	}
	switch record.Outcome {
	case startupManagerReplayOutcomeReplayed:
		s.ReplayedCount++
	case startupManagerReplayOutcomeSkipped:
		s.SkippedCount++
	case startupManagerReplayOutcomeDropped:
		s.DroppedCount++
		if s.FirstDroppedFailure == nil {
			if record.Failure != nil {
				s.FirstDroppedFailure = runtimefailures.CloneEnvelope(record.Failure)
			} else {
				fallback := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassInternalFailure, "startup_replay_dropped_without_failure", "agent-manager", "startup_replay", map[string]any{"agent_id": strings.TrimSpace(record.AgentID)}), "agent-manager", "startup_replay")
				s.FirstDroppedFailure = &fallback
			}
		}
	}
}

func (s *StartupReplaySummary) merge(other StartupReplaySummary) {
	if s == nil {
		return
	}
	s.ReplayedCount += other.ReplayedCount
	s.SkippedCount += other.SkippedCount
	s.DroppedCount += other.DroppedCount
	if s.FirstDroppedFailure == nil {
		s.FirstDroppedFailure = runtimefailures.CloneEnvelope(other.FirstDroppedFailure)
	}
}
