package engine

import (
	"time"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/identity"
)

const DefaultMaxChainDepth = 50

type StateSnapshot struct {
	EntityID        identity.EntityID
	WorkflowName    string
	WorkflowVersion string
	CurrentState    string
	EnteredStateAt  time.Time
	Metadata        map[string]any
	StateBuckets    map[string]any
	TimerState      []TimerState
}

type TimerState struct {
	TimerID   string
	EventType string
	CreatedAt time.Time
	FiresAt   time.Time
	StartedBy string
	Recurring bool
	Cancelled bool
}

type ExecutionRequest struct {
	ExecutionID string
	EntityID    identity.EntityID
	NodeID      identity.NodeID
	FlowID      identity.FlowID
	Event       events.Event
	Handler     runtimecontracts.SystemNodeEventHandler
	State       StateSnapshot
	ChainDepth  int
	MaxDepth    int
	Preview     bool
}

type ExecutionContext struct {
	Request   ExecutionRequest
	Base      BaseContext
	Step      Step
	Completed []Step
}

type ExecutionState struct {
	State       StateSnapshot
	Computed    map[string]any
	Accumulated map[string]any
	FanOut      map[string]any
	Transformed map[string]any
}

type EmitIntent struct {
	Event          events.Event
	Recipients     []string
	ChainDepth     int
	ParentEventID  string
	DeadLetterHint string
}

type TimerIntent struct {
	Operation    TimerOperation
	TimerID      string
	Owner        identity.NodeID
	EventType    string
	FireAt       time.Time
	Recurring    bool
	FromState    string
	ToState      string
	TriggerEvent string
}

type StateMutation struct {
	NextState        string
	Metadata         map[string]any
	StateBuckets     map[string]any
	ClearGates       []string
	SetGate          string
	DataAccumulation runtimecontracts.WorkflowDataAccumulation
}

type RuleMatch struct {
	ID         string
	AdvancesTo string
	SetsGate   string
	ActionID   string
}

type ExecutionResult struct {
	Status          OutcomeStatus
	FailureClass    FailureClass
	ExecutedSteps   []Step
	CurrentState    string
	NextState       string
	GuardsEvaluated []string
	ActionsExecuted []string
	ClearGates      []string
	SetsGate        string
	RuleID          string
	FanOutCount     int
	Computed        map[string]any
	StateMutation   StateMutation
	TimerIntents    []TimerIntent
	EmitIntents     []EmitIntent
	ChainDepth      int
}
