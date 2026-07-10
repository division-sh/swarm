package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/computemodule"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	"github.com/division-sh/swarm/internal/runtime/platformcontext"
)

const DefaultMaxChainDepth = 50

type StateCarrier struct {
	Metadata     map[string]any
	Gates        map[string]bool
	StateBuckets map[string]map[string]any
}

func NewStateCarrier(metadata map[string]any, gates map[string]bool, stateBuckets map[string]map[string]any) StateCarrier {
	return StateCarrier{
		Metadata:     cloneStringAnyMap(metadata),
		Gates:        mapsClone(gates),
		StateBuckets: cloneStateBucketSet(stateBuckets),
	}
}

func StateCarrierFromPersisted(metadata map[string]any, stateBuckets map[string]any) (StateCarrier, error) {
	fields := cloneStringAnyMap(metadata)
	gates, err := stateCarrierGatesFromMetadata(fields)
	if err != nil {
		return StateCarrier{}, err
	}
	delete(fields, "gates")
	buckets, err := stateBucketSetFromRaw(stateBuckets)
	if err != nil {
		return StateCarrier{}, err
	}
	return StateCarrier{
		Metadata:     fields,
		Gates:        gates,
		StateBuckets: buckets,
	}, nil
}

func (c StateCarrier) MetadataBucket() values.Bucket {
	return values.Wrap(c.Metadata)
}

func (c StateCarrier) GatesBucket() values.Bucket {
	return values.Wrap(boolMapToAnyMap(c.Gates))
}

func (c StateCarrier) StateBucketsBucket() values.Bucket {
	return values.Wrap(stateBucketSetAsRaw(c.StateBuckets))
}

func (c StateCarrier) EntityContext(entityID identity.EntityID, currentState, workflowName, workflowVersion string) map[string]any {
	out := cloneStringAnyMap(c.Metadata)
	if out == nil {
		out = map[string]any{}
	}
	delete(out, "subject_id")
	return out
}

func (c StateCarrier) PersistedMetadata() map[string]any {
	out := cloneStringAnyMap(c.Metadata)
	if out == nil {
		out = map[string]any{}
	}
	delete(out, "subject_id")
	if len(c.Gates) == 0 {
		delete(out, "gates")
		return out
	}
	out["gates"] = boolMapToAnyMap(c.Gates)
	return out
}

func (c StateCarrier) PersistedStateBuckets() map[string]any {
	return stateBucketSetAsRaw(c.StateBuckets)
}

func (c *StateCarrier) SetGate(name string, value bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	if c.Gates == nil {
		c.Gates = map[string]bool{}
	}
	c.Gates[name] = value
}

func (c *StateCarrier) EnsureGatesMap() map[string]bool {
	if c.Gates == nil {
		c.Gates = map[string]bool{}
	}
	return c.Gates
}

func (c *StateCarrier) SetMetadata(key string, value any) {
	if c.Metadata == nil {
		c.Metadata = map[string]any{}
	}
	values.Wrap(c.Metadata).Set(key, value)
}

func (c *StateCarrier) EnsureMetadataMap(key string) values.Bucket {
	if c.Metadata == nil {
		c.Metadata = map[string]any{}
	}
	return values.Wrap(c.Metadata).EnsureMap(key)
}

func (c *StateCarrier) EnsureStateBucket(name string) values.Bucket {
	name = strings.TrimSpace(name)
	if name == "" {
		return values.Wrap(map[string]any{})
	}
	if c.StateBuckets == nil {
		c.StateBuckets = map[string]map[string]any{}
	}
	if c.StateBuckets[name] == nil {
		c.StateBuckets[name] = map[string]any{}
	}
	return values.Wrap(c.StateBuckets[name])
}

func (c StateCarrier) StateBucket(name string) (values.Bucket, bool) {
	name = strings.TrimSpace(name)
	if name == "" || c.StateBuckets == nil {
		return values.Bucket{}, false
	}
	bucket, ok := c.StateBuckets[name]
	if !ok || bucket == nil {
		return values.Bucket{}, false
	}
	return values.Wrap(bucket), true
}

type StateSnapshot struct {
	EntityID        identity.EntityID
	WorkflowName    string
	WorkflowVersion string
	CurrentState    string
	EnteredStateAt  time.Time
	StateCarrier
	TimerState []TimerState
}

func (s StateSnapshot) MetadataBucket() values.Bucket {
	return s.StateCarrier.MetadataBucket()
}

func (s StateSnapshot) GatesBucket() values.Bucket {
	return s.StateCarrier.GatesBucket()
}

func (s StateSnapshot) StateBucketsBucket() values.Bucket {
	return s.StateCarrier.StateBucketsBucket()
}

func (s StateSnapshot) EntityContext() map[string]any {
	return s.StateCarrier.EntityContext(s.EntityID, s.CurrentState, s.WorkflowName, s.WorkflowVersion)
}

func (s StateSnapshot) PlatformEntityContext(flowInstance string) map[string]any {
	return platformcontext.EntityMetadata(s.EntityID.String(), s.CurrentState, flowInstance, s.StateCarrier.Gates)
}

func (s *StateSnapshot) SetGate(name string, value bool) {
	s.StateCarrier.SetGate(name, value)
}

func (s *StateSnapshot) EnsureGatesMap() map[string]bool {
	return s.StateCarrier.EnsureGatesMap()
}

func (s *StateSnapshot) SetMetadata(key string, value any) {
	s.StateCarrier.SetMetadata(key, value)
}

func (s *StateSnapshot) EnsureMetadataMap(key string) values.Bucket {
	return s.StateCarrier.EnsureMetadataMap(key)
}

func (s *StateSnapshot) EnsureStateBucket(name string) values.Bucket {
	return s.StateCarrier.EnsureStateBucket(name)
}

func (s StateSnapshot) StateBucket(name string) (values.Bucket, bool) {
	return s.StateCarrier.StateBucket(name)
}

type TimerState struct {
	TimerID   string
	EventType string
	CreatedAt time.Time
	FiresAt   time.Time
	StartedBy string
	Recurring bool
	Cancelled bool
	Fired     bool
}

type ExecutionRequest struct {
	ExecutionID string
	EntityID    identity.EntityID
	NodeID      identity.NodeID
	FlowID      identity.FlowID
	Event       events.Event
	// ProducerRoute is the admitted handler/action route that owns runtime
	// action result events emitted during this execution.
	ProducerRoute events.RouteIdentity
	// HandlerEventKey is the matched authored handler event key selected by
	// runtime dispatch. Concrete Event.Type remains event provenance.
	HandlerEventKey string
	Handler         runtimecontracts.SystemNodeEventHandler
	State           StateSnapshot
	// ExpectedComputeModuleTraces carries prior deterministic module evidence
	// for supported replay. Nil means normal execution; a non-nil empty slice
	// means replay mode with zero expected module executions. When present,
	// module execution re-runs and compares identity/profile, semantic
	// outcome, and resource evidence against this ordered evidence.
	ExpectedComputeModuleTraces []ComputeModuleTrace
	ChainDepth                  int
	MaxDepth                    int
	Preview                     bool
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
	Context        events.DeliveryContext
	Recipients     []string
	ChainDepth     int
	ParentEventID  string
	DeadLetterHint string
}

type TimerIntent struct {
	Context      events.DeliveryContext
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

type ActivityIntent struct {
	Context          events.DeliveryContext
	ActivityID       string
	Tool             string
	Input            map[string]any
	EffectClass      runtimecontracts.ActivityEffectClass
	SuccessEvent     string
	FailureEvent     string
	RetryMaxAttempts int
	RetryBackoff     string
	ForkPolicy       runtimecontracts.ActivityForkPolicy
	EntityID         identity.EntityID
	NodeID           identity.NodeID
	FlowID           identity.FlowID
	HandlerEventKey  string
	SourceEventID    string
	SourceRunID      string
	SourceTaskID     string
	ParentEventID    string
	ChainDepth       int
	Attempt          int
}

func (i ActivityIntent) Normalized() ActivityIntent {
	i.Context = i.Context.Normalized()
	i.ActivityID = strings.TrimSpace(i.ActivityID)
	i.Tool = strings.TrimSpace(i.Tool)
	i.SuccessEvent = strings.TrimSpace(i.SuccessEvent)
	i.FailureEvent = strings.TrimSpace(i.FailureEvent)
	i.RetryBackoff = strings.TrimSpace(i.RetryBackoff)
	i.HandlerEventKey = strings.TrimSpace(i.HandlerEventKey)
	i.SourceEventID = strings.TrimSpace(i.SourceEventID)
	i.ParentEventID = strings.TrimSpace(i.ParentEventID)
	if i.Attempt <= 0 {
		i.Attempt = 1
	}
	if i.Input == nil {
		i.Input = map[string]any{}
	}
	return i
}

type StateMutation struct {
	NextState string
	StateCarrier
	ClearGates       []string
	SetGate          string
	DataAccumulation runtimecontracts.WorkflowDataAccumulation
}

func (m StateMutation) MetadataBucket() values.Bucket {
	return m.StateCarrier.MetadataBucket()
}

func (m StateMutation) GatesBucket() values.Bucket {
	return m.StateCarrier.GatesBucket()
}

func (m StateMutation) StateBucketsBucket() values.Bucket {
	return m.StateCarrier.StateBucketsBucket()
}

func (m *StateMutation) SetMetadata(key string, value any) {
	m.StateCarrier.SetMetadata(key, value)
}

func (m *StateMutation) EnsureMetadataMap(key string) values.Bucket {
	return m.StateCarrier.EnsureMetadataMap(key)
}

func (m *StateMutation) SetGateValue(name string, value bool) {
	m.StateCarrier.SetGate(name, value)
}

func (m *StateMutation) SetStateBuckets(raw map[string]map[string]any) {
	m.StateCarrier.StateBuckets = cloneStateBucketSet(raw)
}

type AccumulatorCompletionEvaluationOutcome string

const (
	AccumulatorCompletionEvaluationNotAttempted AccumulatorCompletionEvaluationOutcome = "not_attempted"
	AccumulatorCompletionEvaluationSucceeded    AccumulatorCompletionEvaluationOutcome = "succeeded"
	AccumulatorCompletionEvaluationFailed       AccumulatorCompletionEvaluationOutcome = "failed"
)

type AccumulatorCompletionCommitOutcome string

const (
	AccumulatorCompletionCommitUnknown    AccumulatorCompletionCommitOutcome = "unknown"
	AccumulatorCompletionCommitCommitted  AccumulatorCompletionCommitOutcome = "committed"
	AccumulatorCompletionCommitRolledBack AccumulatorCompletionCommitOutcome = "rolled_back"
)

type AccumulatorCompletionDiagnostics struct {
	Relevant           bool
	CompletionReached  bool
	ReceivedCount      int
	ExpectedCount      int
	CompletionMode     string
	OnCompleteDeclared bool
	EvaluationOutcome  AccumulatorCompletionEvaluationOutcome
	SelectedRuleID     string
	CommitOutcome      AccumulatorCompletionCommitOutcome
}

type RuleMatch struct {
	ID         string
	AdvancesTo string
	SetsGate   string
	ActionID   string
}

type ExecutionResult struct {
	Status                           OutcomeStatus
	FailureClass                     FailureClass
	ExecutedSteps                    []Step
	CurrentState                     string
	NextState                        string
	GuardsEvaluated                  []string
	ActionsExecuted                  []string
	ClearGates                       []string
	SetsGate                         string
	RuleID                           string
	FanOutCount                      int
	Computed                         map[string]any
	StateMutation                    StateMutation
	TimerIntents                     []TimerIntent
	EmitIntents                      []EmitIntent
	ActivityIntents                  []ActivityIntent
	ComputeModuleTraces              []ComputeModuleTrace
	DeadLetterIntents                []EmitIntent
	ChainDepth                       int
	AccumulatorCompletionDiagnostics AccumulatorCompletionDiagnostics
}

type ComputeModuleTrace = computemodule.ReplayEnvelope

func (r ExecutionResult) ComputedBucket() values.Bucket {
	return values.Wrap(r.Computed)
}

func (r *ExecutionResult) SetComputed(key string, value any) {
	if r.Computed == nil {
		r.Computed = map[string]any{}
	}
	values.Wrap(r.Computed).Set(key, value)
}

func cloneStateBucketSet(in map[string]map[string]any) map[string]map[string]any {
	if len(in) == 0 {
		return map[string]map[string]any{}
	}
	out := make(map[string]map[string]any, len(in))
	for key, bucket := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = cloneStringAnyMap(bucket)
	}
	return out
}

func stateCarrierGatesFromMetadata(metadata map[string]any) (map[string]bool, error) {
	if len(metadata) == 0 {
		return map[string]bool{}, nil
	}
	raw, ok := metadata["gates"]
	if !ok || raw == nil {
		return map[string]bool{}, nil
	}
	switch typed := raw.(type) {
	case map[string]any:
		out := make(map[string]bool, len(typed))
		for key, rawValue := range typed {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			value, ok := rawValue.(bool)
			if !ok {
				return nil, fmt.Errorf("invalid workflow gates shape: gate %q = %T", key, rawValue)
			}
			out[key] = value
		}
		return out, nil
	case map[string]bool:
		return mapsClone(typed), nil
	default:
		return nil, fmt.Errorf("invalid workflow gates shape: %T", raw)
	}
}

func stateBucketSetFromRaw(raw map[string]any) (map[string]map[string]any, error) {
	if len(raw) == 0 {
		return map[string]map[string]any{}, nil
	}
	out := make(map[string]map[string]any, len(raw))
	for key, value := range raw {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		bucket, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid workflow state bucket %q: %T", key, value)
		}
		out[key] = cloneStringAnyMap(bucket)
	}
	return out, nil
}

func stateBucketSetAsRaw(in map[string]map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for key, bucket := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = cloneStringAnyMap(bucket)
	}
	return out
}
