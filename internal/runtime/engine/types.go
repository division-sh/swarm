package engine

import (
	"strings"
	"time"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/core/identity"
	"empireai/internal/runtime/core/values"
)

const DefaultMaxChainDepth = 50

type StateSnapshot struct {
	EntityID        identity.EntityID
	WorkflowName    string
	WorkflowVersion string
	CurrentState    string
	EnteredStateAt  time.Time
	Metadata        map[string]any
	Gates           map[string]bool
	StateBuckets    map[string]any
	TimerState      []TimerState
}

func (s StateSnapshot) MetadataBucket() values.Bucket {
	return values.Wrap(s.Metadata)
}

func (s StateSnapshot) StateBucketsBucket() values.Bucket {
	return values.Wrap(s.StateBuckets)
}

func (s StateSnapshot) GatesBucket() values.Bucket {
	return values.Wrap(boolMapToAnyMap(s.Gates))
}

func (s StateSnapshot) EntityContext() map[string]any {
	out := cloneStringAnyMap(s.Metadata)
	if out == nil {
		out = map[string]any{}
	}
	out["entity_id"] = s.EntityID.String()
	out["current_state"] = strings.TrimSpace(s.CurrentState)
	out["workflow_name"] = strings.TrimSpace(s.WorkflowName)
	out["workflow_version"] = strings.TrimSpace(s.WorkflowVersion)
	out["gates"] = boolMapToAnyMap(s.Gates)
	return out
}

func (s *StateSnapshot) EnsureStateBucketsBucket() values.Bucket {
	if s.StateBuckets == nil {
		s.StateBuckets = map[string]any{}
	}
	return values.Wrap(s.StateBuckets)
}

func (s *StateSnapshot) SetGate(name string, value bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	if s.Gates == nil {
		s.Gates = map[string]bool{}
	}
	s.Gates[name] = value
	if s.Metadata == nil {
		s.Metadata = map[string]any{}
	}
	s.Metadata["gates"] = boolMapToAnyMap(s.Gates)
}

func (s *StateSnapshot) EnsureGatesMap() map[string]bool {
	if s.Gates == nil {
		s.Gates = map[string]bool{}
	}
	if s.Metadata == nil {
		s.Metadata = map[string]any{}
	}
	metadataGates := map[string]bool{}
	if raw, ok := s.Metadata["gates"].(map[string]any); ok {
		metadataGates = anyMapToBoolMap(raw)
	}
	for key, value := range metadataGates {
		if _, ok := s.Gates[key]; !ok {
			s.Gates[key] = value
		}
	}
	s.Metadata["gates"] = boolMapToAnyMap(s.Gates)
	return s.Gates
}

func (s *StateSnapshot) SetMetadata(key string, value any) {
	if s.Metadata == nil {
		s.Metadata = map[string]any{}
	}
	values.Wrap(s.Metadata).Set(key, value)
}

func (s *StateSnapshot) EnsureMetadataMap(key string) values.Bucket {
	if s.Metadata == nil {
		s.Metadata = map[string]any{}
	}
	return values.Wrap(s.Metadata).EnsureMap(key)
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

func (s ExecutionState) ComputedBucket() values.Bucket {
	return values.Wrap(s.Computed)
}

func (s ExecutionState) AccumulatedBucket() values.Bucket {
	return values.Wrap(s.Accumulated)
}

func (s ExecutionState) FanOutBucket() values.Bucket {
	return values.Wrap(s.FanOut)
}

func (s *ExecutionState) SetComputed(key string, value any) {
	if s.Computed == nil {
		s.Computed = map[string]any{}
	}
	values.Wrap(s.Computed).Set(key, value)
}

func (s *ExecutionState) SetAccumulated(key string, value any) {
	if s.Accumulated == nil {
		s.Accumulated = map[string]any{}
	}
	values.Wrap(s.Accumulated).Set(key, value)
}

func (s *ExecutionState) SetFanOut(key string, value any) {
	if s.FanOut == nil {
		s.FanOut = map[string]any{}
	}
	values.Wrap(s.FanOut).Set(key, value)
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
	Gates            map[string]bool
	StateBuckets     map[string]any
	ClearGates       []string
	SetGate          string
	DataAccumulation runtimecontracts.WorkflowDataAccumulation
}

func (m StateMutation) MetadataBucket() values.Bucket {
	return values.Wrap(m.Metadata)
}

func (m StateMutation) StateBucketsBucket() values.Bucket {
	return values.Wrap(m.StateBuckets)
}

func (m StateMutation) GatesBucket() values.Bucket {
	return values.Wrap(boolMapToAnyMap(m.Gates))
}

func (m *StateMutation) SetMetadata(key string, value any) {
	if m.Metadata == nil {
		m.Metadata = map[string]any{}
	}
	values.Wrap(m.Metadata).Set(key, value)
}

func (m *StateMutation) EnsureMetadataMap(key string) values.Bucket {
	if m.Metadata == nil {
		m.Metadata = map[string]any{}
	}
	return values.Wrap(m.Metadata).EnsureMap(key)
}

func (m *StateMutation) SetGateValue(name string, value bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	if m.Gates == nil {
		m.Gates = map[string]bool{}
	}
	m.Gates[name] = value
	if m.Metadata == nil {
		m.Metadata = map[string]any{}
	}
	m.Metadata["gates"] = boolMapToAnyMap(m.Gates)
}

func (m *StateMutation) SetStateBuckets(raw map[string]any) {
	m.StateBuckets = cloneStringAnyMap(raw)
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

func (r ExecutionResult) ComputedBucket() values.Bucket {
	return values.Wrap(r.Computed)
}

func (r *ExecutionResult) SetComputed(key string, value any) {
	if r.Computed == nil {
		r.Computed = map[string]any{}
	}
	values.Wrap(r.Computed).Set(key, value)
}
