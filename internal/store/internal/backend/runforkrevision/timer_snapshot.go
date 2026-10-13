package runforkrevision

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
)

// TimerSnapshot is the native timer fact at an exact historical revision.
// It carries inherited arming separately from the child row's birth.
type TimerSnapshot struct {
	TimerID                 string          `json:"timer_id"`
	TimerName               string          `json:"timer_name"`
	ScheduleScope           string          `json:"schedule_scope"`
	ScheduleKey             string          `json:"schedule_key"`
	ImmutableHash           string          `json:"immutable_hash"`
	RunID                   string          `json:"run_id"`
	SourceTimerID           string          `json:"source_timer_id"`
	ForkedFromRunID         string          `json:"forked_from_run_id"`
	ForkedFromEventID       string          `json:"forked_from_event_id"`
	ReconstructionOwner     string          `json:"reconstruction_owner"`
	ForkedFromPointKind     forkpoint.Kind  `json:"forked_from_point_kind"`
	ForkedFromPointRevision int64           `json:"forked_from_point_revision"`
	SourceArmedAt           *time.Time      `json:"source_armed_at"`
	EntityID                string          `json:"entity_id"`
	FlowScopeKey            string          `json:"flow_scope_key"`
	FlowInstanceID          string          `json:"flow_instance_id"`
	FlowInstance            string          `json:"flow_instance"`
	FireEvent               string          `json:"fire_event"`
	FirePayload             json.RawMessage `json:"fire_payload"`
	RoutingSource           json.RawMessage `json:"routing_source"`
	ExecutionMode           string          `json:"execution_mode"`
	FireAt                  time.Time       `json:"fire_at"`
	InitialFireAt           *time.Time      `json:"initial_fire_at"`
	Recurring               bool            `json:"recurring"`
	RecurrenceInterval      string          `json:"recurrence_interval"`
	OwnerNode               string          `json:"owner_node"`
	OwnerAgent              string          `json:"owner_agent"`
	OwnerKind               string          `json:"owner_kind"`
	AgentNameOwner          string          `json:"agent_name_owner"`
	AgentNameSource         string          `json:"agent_name_source"`
	AgentRoutePresence      string          `json:"agent_route_presence"`
	AgentFlowScopeKey       string          `json:"agent_flow_scope_key"`
	AgentFlowInstanceID     string          `json:"agent_flow_instance_id"`
	ReplyContextID          string          `json:"reply_context_id"`
	TaskID                  string          `json:"task_id"`
	DueBasisKind            string          `json:"due_basis_kind"`
	DueBasisAbsolute        *time.Time      `json:"due_basis_absolute"`
	DueBasisDuration        string          `json:"due_basis_duration"`
	DueBasisCron            string          `json:"due_basis_cron"`
	OccurrenceEventID       string          `json:"occurrence_event_id"`
	OccurrenceAdmittedAt    *time.Time      `json:"occurrence_admitted_at"`
	AcceptedAt              *time.Time      `json:"accepted_at"`
	CancelCause             string          `json:"cancel_cause"`
	CancelledAt             *time.Time      `json:"cancelled_at"`
	FailureCode             string          `json:"failure_code"`
	FailureMessage          string          `json:"failure_message"`
	FailedAt                *time.Time      `json:"failed_at"`
	ClockSuspension         json.RawMessage `json:"clock_suspension"`
	TaskType                string          `json:"task_type"`
	Status                  string          `json:"status"`
	FiredAt                 *time.Time      `json:"fired_at"`
	CreatedAt               time.Time       `json:"created_at"`
}

func DecodeTimerSnapshot(raw []byte) (TimerSnapshot, error) {
	var snapshot TimerSnapshot
	if _, err := canonicaljson.Decode(raw); err != nil {
		return snapshot, fmt.Errorf("historical timer: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot); err != nil {
		return TimerSnapshot{}, fmt.Errorf("decode historical timer: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return TimerSnapshot{}, fmt.Errorf("historical timer requires one JSON value")
	}
	if snapshot.TimerID == "" {
		return TimerSnapshot{}, fmt.Errorf("historical timer identity is required")
	}
	switch snapshot.TaskType {
	case "timer", "scheduled_task", "global_recurring", "workflow_timer":
	default:
		return TimerSnapshot{}, fmt.Errorf("historical timer requires an explicit canonical task family")
	}
	if snapshot.TaskType == "workflow_timer" {
		if (snapshot.CancelCause == "") != (snapshot.CancelledAt == nil) || (snapshot.CancelledAt != nil && snapshot.CancelledAt.IsZero()) {
			return TimerSnapshot{}, fmt.Errorf("historical workflow timer cancellation fields must be absent or an exact pair")
		}
	}
	return snapshot, nil
}
