package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/computemodule"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/division-sh/swarm/internal/runtime/platformcontext"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

const DefaultMaxChainDepth = 50

type StateCarrier struct {
	Fields       map[string]any
	Bookkeeping  map[string]any
	Control      StateControl
	Gates        map[string]bool
	StateBuckets map[string]map[string]any
}

type StateControl struct {
	FlowPath           string
	InstanceID         string
	StorageRef         string
	EntityType         string
	InstanceKind       string
	TemplateVersion    string
	ParentFlowID       string
	ParentFlowInstance string
	ParentEntityID     string
}

func NewStateCarrier(fields map[string]any, gates map[string]bool, stateBuckets map[string]map[string]any) StateCarrier {
	return StateCarrier{
		Fields:       cloneStringAnyMap(fields),
		Bookkeeping:  map[string]any{},
		Gates:        mapsClone(gates),
		StateBuckets: cloneStateBucketSet(stateBuckets),
	}
}

func NewStateCarrierWithOwners(fields, bookkeeping map[string]any, control StateControl, gates map[string]bool, stateBuckets map[string]map[string]any) StateCarrier {
	carrier := NewStateCarrier(fields, gates, stateBuckets)
	carrier.Bookkeeping = cloneStringAnyMap(bookkeeping)
	carrier.Control = control
	return carrier
}

func StateCarrierFromPersisted(fields, bookkeeping map[string]any, gates map[string]bool, stateBuckets map[string]any) (StateCarrier, error) {
	buckets, err := stateBucketSetFromRaw(stateBuckets)
	if err != nil {
		return StateCarrier{}, err
	}
	return StateCarrier{
		Fields:       cloneStringAnyMap(fields),
		Bookkeeping:  cloneStringAnyMap(bookkeeping),
		Gates:        mapsClone(gates),
		StateBuckets: buckets,
	}, nil
}

func (c StateCarrier) FieldsBucket() values.Bucket {
	return values.Wrap(c.Fields)
}

func (c StateCarrier) BookkeepingBucket() values.Bucket {
	return values.Wrap(c.Bookkeeping)
}

func (c StateCarrier) GatesBucket() values.Bucket {
	return values.Wrap(boolMapToAnyMap(c.Gates))
}

func (c StateCarrier) StateBucketsBucket() values.Bucket {
	return values.Wrap(stateBucketSetAsRaw(c.StateBuckets))
}

func (c StateCarrier) EntityContext(entityID identity.EntityID, currentState, workflowName, workflowVersion string) map[string]any {
	out := cloneStringAnyMap(c.Fields)
	if out == nil {
		out = map[string]any{}
	}
	delete(out, "subject_id")
	return out
}

func (c StateCarrier) PersistedFields() map[string]any {
	return cloneStringAnyMap(c.Fields)
}

func (c StateCarrier) PersistedBookkeeping() map[string]any {
	return cloneStringAnyMap(c.Bookkeeping)
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

func (c *StateCarrier) SetField(key string, value any) {
	if c.Fields == nil {
		c.Fields = map[string]any{}
	}
	values.Wrap(c.Fields).Set(key, value)
}

func (c *StateCarrier) EnsureFieldMap(key string) values.Bucket {
	if c.Fields == nil {
		c.Fields = map[string]any{}
	}
	return values.Wrap(c.Fields).EnsureMap(key)
}

func (c *StateCarrier) SetBookkeeping(key string, value any) {
	if c.Bookkeeping == nil {
		c.Bookkeeping = map[string]any{}
	}
	values.Wrap(c.Bookkeeping).Set(key, value)
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
}

func (s StateSnapshot) FieldsBucket() values.Bucket {
	return s.StateCarrier.FieldsBucket()
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

func (s *StateSnapshot) SetField(key string, value any) {
	s.StateCarrier.SetField(key, value)
}

func (s *StateSnapshot) EnsureFieldMap(key string) values.Bucket {
	return s.StateCarrier.EnsureFieldMap(key)
}

func (s *StateSnapshot) SetBookkeeping(key string, value any) {
	s.StateCarrier.SetBookkeeping(key, value)
}

func (s *StateSnapshot) EnsureStateBucket(name string) values.Bucket {
	return s.StateCarrier.EnsureStateBucket(name)
}

func (s StateSnapshot) StateBucket(name string) (values.Bucket, bool) {
	return s.StateCarrier.StateBucket(name)
}

type ExecutionRequest struct {
	ExecutionID string
	EntityID    identity.EntityID
	Node        identity.ExecutableNode
	// ExecutionFlowID is the runtime flow scope selected for this exact node.
	// It is distinct from Node.FlowID(): root declarations intentionally carry
	// an empty owning flow while executing in the bundle's root flow.
	ExecutionFlowID identity.FlowID
	// Route is the exact workflow-instance persistence identity selected by
	// the runtime boundary. ProducerSource remains event-source authority.
	Route runtimeflowidentity.Route
	Event events.Event
	// ProducerSource is admitted once at the handler boundary and copied by
	// every runtime event produced by this execution.
	ProducerSource events.RoutingSource
	// HandlerEventKey is the matched authored handler event key selected by
	// runtime dispatch. Concrete Event.Type remains event provenance.
	HandlerEventKey string
	Handler         runtimecontracts.SystemNodeEventHandler
	// JoinDeclaration is the exact authored join identity selected before
	// execution. Internal timer occurrences carry the same declaration plus
	// their durable window and generation.
	JoinDeclaration timeridentity.JoinRef
	State           StateSnapshot
	// InitialFieldValues is the exact authored create-entity projection that the
	// persistence owner records separately from subsequent handler mutations.
	InitialFieldValues map[string]any
	// ExpectedComputeModuleTraces carries prior deterministic module evidence
	// for supported replay. Nil means normal execution; a non-nil empty slice
	// means replay mode with zero expected module executions. When present,
	// module execution re-runs and compares identity/profile, semantic
	// outcome, and resource evidence against this ordered evidence.
	ExpectedComputeModuleTraces []ComputeModuleTrace
	ChainDepth                  int
	MaxDepth                    int
	Preview                     bool
	// DeferCommittedDispatch returns committed emission and activity evidence to
	// the caller instead of dispatching it. Intercepting runtimes use this to
	// compose one immutable evaluation result without context-carried collectors.
	DeferCommittedDispatch bool
}

func (r ExecutionRequest) StateAddress() StateAddress {
	return StateAddress{
		FlowID:   identity.NormalizeFlowID(r.ExecutionFlowID.String()),
		Route:    r.Route,
		EntityID: identity.NormalizeEntityID(r.EntityID.String()),
	}
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
	Join        map[string]any
	Loop        map[string]any
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

func (s ExecutionState) JoinBucket() values.Bucket {
	return values.Wrap(s.Join)
}

func (s ExecutionState) LoopBucket() values.Bucket {
	return values.Wrap(s.Loop)
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

func (s *ExecutionState) SetJoin(key string, value any) {
	if s.Join == nil {
		s.Join = map[string]any{}
	}
	values.Wrap(s.Join).Set(key, value)
}

func (s *ExecutionState) SetLoop(values map[string]any) {
	s.Loop = cloneStringAnyMap(values)
}

type EmitIntent struct {
	Event          events.Event
	Context        events.DeliveryContext
	Recipients     []string
	ChainDepth     int
	ParentEventID  string
	DeadLetterHint string
}

type ActivityIntent struct {
	Context          events.DeliveryContext
	RoutingSource    events.RoutingSource
	ActivityID       string
	Tool             string
	PlanGeneration   plangeneration.Generation
	BundleHash       string
	WorkflowVersion  string
	Input            semanticvalue.Value
	ApprovalDecision string
	EffectClass      runtimecontracts.ActivityEffectClass
	SuccessEvent     string
	FailureEvent     string
	RevisionEvent    string
	RejectedEvent    string
	RetryMaxAttempts int
	RetryBackoff     string
	ForkPolicy       runtimecontracts.ActivityForkPolicy
	EntityID         identity.EntityID
	Owner            activityidentity.Owner
	ExecutionFlowID  identity.FlowID
	FlowInstance     string
	HandlerEventKey  string
	SourceEventID    string
	SourceRunID      string
	SourceTaskID     string
	ParentEventID    string
	ChainDepth       int
	Attempt          int
	Generation       attemptgeneration.Generation
	LoopStage        string
	ExecutionMode    executionmode.Mode
}

func (i ActivityIntent) Normalized() ActivityIntent {
	i.Context = i.Context.Normalized()
	i.ActivityID = strings.TrimSpace(i.ActivityID)
	i.Tool = strings.TrimSpace(i.Tool)
	i.BundleHash = strings.TrimSpace(i.BundleHash)
	i.WorkflowVersion = strings.TrimSpace(i.WorkflowVersion)
	i.SuccessEvent = strings.TrimSpace(i.SuccessEvent)
	i.FailureEvent = strings.TrimSpace(i.FailureEvent)
	i.RevisionEvent = strings.TrimSpace(i.RevisionEvent)
	i.RejectedEvent = strings.TrimSpace(i.RejectedEvent)
	i.RetryBackoff = strings.TrimSpace(i.RetryBackoff)
	i.HandlerEventKey = strings.TrimSpace(i.HandlerEventKey)
	i.ApprovalDecision = strings.TrimSpace(i.ApprovalDecision)
	i.SourceEventID = strings.TrimSpace(i.SourceEventID)
	i.ParentEventID = strings.TrimSpace(i.ParentEventID)
	i.FlowInstance = strings.Trim(strings.TrimSpace(i.FlowInstance), "/")
	i.Generation = i.Generation.Normalize()
	i.LoopStage = strings.TrimSpace(i.LoopStage)
	i.ExecutionMode = executionmode.Mode(strings.TrimSpace(string(i.ExecutionMode)))
	if i.Attempt <= 0 {
		i.Attempt = 1
	}
	if i.Input.Kind() == semanticvalue.KindNull {
		i.Input = semanticvalue.EmptyObject()
	}
	return i
}

type StateMutation struct {
	NextState        string
	TriggerEventID   string
	TriggerEventType string
	TriggeredAt      time.Time
	StateCarrier
	ClearGates         []string
	SetGate            string
	DataAccumulation   runtimecontracts.WorkflowDataAccumulation
	InitialFieldValues map[string]any
}

func (m StateMutation) FieldsBucket() values.Bucket {
	return m.StateCarrier.FieldsBucket()
}

func (m StateMutation) GatesBucket() values.Bucket {
	return m.StateCarrier.GatesBucket()
}

func (m StateMutation) StateBucketsBucket() values.Bucket {
	return m.StateCarrier.StateBucketsBucket()
}

func (m *StateMutation) SetField(key string, value any) {
	m.StateCarrier.SetField(key, value)
}

func (m *StateMutation) EnsureFieldMap(key string) values.Bucket {
	return m.StateCarrier.EnsureFieldMap(key)
}

func (m *StateMutation) SetBookkeeping(key string, value any) {
	m.StateCarrier.SetBookkeeping(key, value)
}

func (m *StateMutation) SetGateValue(name string, value bool) {
	m.StateCarrier.SetGate(name, value)
}

func (m *StateMutation) SetStateBuckets(raw map[string]map[string]any) {
	m.StateCarrier.StateBuckets = cloneStateBucketSet(raw)
}

type RuleMatch struct {
	ID         string
	AdvancesTo string
	SetsGate   string
	ActionID   string
}

type ExecutionResult struct {
	Status               OutcomeStatus
	Failure              *failures.Envelope
	FailureDisposition   FailureDisposition
	ExecutedSteps        []Step
	CurrentState         string
	NextState            string
	GuardsEvaluated      []string
	ActionsExecuted      []string
	ClearGates           []string
	SetsGate             string
	HandlerRuleSelection handlerselection.HandlerRuleSelectionFact
	FanOutCount          int
	Computed             map[string]any
	StateMutation        StateMutation
	EmitIntents          []EmitIntent
	ActivityIntents      []ActivityIntent
	ComputeModuleTraces  []ComputeModuleTrace
	DeadLetterIntents    []EmitIntent
	ChainDepth           int
	LoopTrace            *LoopExecutionTrace
}

type LoopExecutionTrace struct {
	LoopID       string                  `json:"loop_id"`
	Operation    string                  `json:"operation"`
	RevisionID   string                  `json:"revision_id"`
	Attempt      int                     `json:"attempt"`
	MaxAttempts  int                     `json:"max_attempts"`
	CurrentStage string                  `json:"current_stage"`
	Status       loopruntime.Status      `json:"status"`
	CloseReason  loopruntime.CloseReason `json:"close_reason,omitempty"`
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
