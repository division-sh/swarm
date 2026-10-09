package timerobligation

import (
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

type WorkflowTimerCancelCause string

const WorkflowTimerCancelCauseRuleRemoved WorkflowTimerCancelCause = "rule_removed"

func (c WorkflowTimerCancelCause) Valid() bool { return c == WorkflowTimerCancelCauseRuleRemoved }

// This primitive carrier may cross fixed-cut and persistence boundaries without
// importing an executor. Workflow activation admission remains in pipeline.
type WorkflowTimerActivationRecord struct {
	ActivationID            string
	TaskID                  string
	RunID                   string
	EntityID                string
	Route                   flowidentity.Route
	RoutingSource           events.RoutingSource
	EventType               string
	ExecutionMode           executionmode.Mode
	Payload                 []byte
	FireAt                  time.Time
	Recurring               bool
	RecurrenceInterval      string
	OwnerNode               string
	OwnerAgent              string
	TaskType                string
	Status                  string
	FiredAt                 time.Time
	CancelCause             WorkflowTimerCancelCause
	CancelledAt             time.Time
	CreatedAt               time.Time
	SourceTimerID           string
	ForkedFromRunID         string
	ForkedFromPointKind     forkpoint.Kind
	ForkedFromPointRevision int64
	ForkedFromEventID       string
	SourceArmedAt           time.Time
	ReconstructionOwner     string
}
