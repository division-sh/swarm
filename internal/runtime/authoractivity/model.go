package authoractivity

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

const Version = 2

type ScopeKind string

const (
	ScopeBundle  ScopeKind = "bundle"
	ScopeRuntime ScopeKind = "runtime"
	ScopeGlobal  ScopeKind = "global"
)

type Scope struct {
	Kind              ScopeKind `json:"kind"`
	RuntimeInstanceID string    `json:"runtime_instance_id,omitempty"`
	BundleHash        string    `json:"bundle_hash,omitempty"`
}

func BundleScope(runtimeInstanceID, bundleHash string) Scope {
	return Scope{Kind: ScopeBundle, RuntimeInstanceID: strings.TrimSpace(runtimeInstanceID), BundleHash: strings.TrimSpace(bundleHash)}
}

func RuntimeScope(runtimeInstanceID string) Scope {
	return Scope{Kind: ScopeRuntime, RuntimeInstanceID: strings.TrimSpace(runtimeInstanceID)}
}

type Kind string

const (
	KindInboundReceived    Kind = "inbound.received"
	KindEventEmitted       Kind = "event.emitted"
	KindEntityLifecycle    Kind = "entity.lifecycle"
	KindDeliveryLifecycle  Kind = "delivery.lifecycle"
	KindDeadLetterRecorded Kind = "dead_letter.recorded"
	KindActivityLifecycle  Kind = "activity.lifecycle"
	KindEffectLifecycle    Kind = "effect.lifecycle"
	KindTurnLifecycle      Kind = "turn.lifecycle"
	KindTurnToolCompleted  Kind = "turn.tool_completed"
	KindCardLifecycle      Kind = "card.lifecycle"
	KindAgentLifecycle     Kind = "agent.lifecycle"
	KindDirectiveLifecycle Kind = "directive.lifecycle"
	KindRunLifecycle       Kind = "run.lifecycle"
	KindPlatformSignal     Kind = "platform.signal"
)

// Projection is the closed, metadata-only persistence shape for author activity.
// It deliberately has no generic payload, evidence, prompt, response, or result field.
type Projection struct {
	SubjectType        string     `json:"subject_type,omitempty"`
	SubjectID          string     `json:"subject_id,omitempty"`
	Provider           string     `json:"provider,omitempty"`
	EventType          string     `json:"event_type,omitempty"`
	ProducerType       string     `json:"producer_type,omitempty"`
	ProducerID         string     `json:"producer_id,omitempty"`
	OldState           string     `json:"old_state,omitempty"`
	NewState           string     `json:"new_state,omitempty"`
	WriterType         string     `json:"writer_type,omitempty"`
	WriterID           string     `json:"writer_id,omitempty"`
	SubscriberType     string     `json:"subscriber_type,omitempty"`
	SubscriberID       string     `json:"subscriber_id,omitempty"`
	RetryCount         *int       `json:"retry_count,omitempty"`
	ReasonCode         string     `json:"reason_code,omitempty"`
	NodeID             string     `json:"node_id,omitempty"`
	Activity           string     `json:"activity,omitempty"`
	Tool               string     `json:"tool,omitempty"`
	EffectClass        string     `json:"effect_class,omitempty"`
	Attempt            *int       `json:"attempt,omitempty"`
	Adapter            string     `json:"adapter,omitempty"`
	Transport          string     `json:"transport,omitempty"`
	AuthorityKind      string     `json:"authority_kind,omitempty"`
	AuthorityID        string     `json:"authority_id,omitempty"`
	ExecutionMode      string     `json:"execution_mode,omitempty"`
	TurnID             string     `json:"turn_id,omitempty"`
	DurationMS         *int       `json:"duration_ms,omitempty"`
	ParseOK            *bool      `json:"parse_ok,omitempty"`
	UsageExactness     string     `json:"usage_exactness,omitempty"`
	InputTokens        *int64     `json:"input_tokens,omitempty"`
	OutputTokens       *int64     `json:"output_tokens,omitempty"`
	ToolName           string     `json:"tool_name,omitempty"`
	ToolUseID          string     `json:"tool_use_id,omitempty"`
	CardID             string     `json:"card_id,omitempty"`
	AnchorKind         string     `json:"anchor_kind,omitempty"`
	AnchorID           string     `json:"anchor_id,omitempty"`
	DecisionID         string     `json:"decision_id,omitempty"`
	Verdict            string     `json:"verdict,omitempty"`
	DeferUntil         *time.Time `json:"defer_until,omitempty"`
	SupersedeReason    string     `json:"supersede_reason,omitempty"`
	PreviousPhase      string     `json:"previous_phase,omitempty"`
	NextPhase          string     `json:"next_phase,omitempty"`
	PreviousGeneration *uint64    `json:"previous_generation,omitempty"`
	NextGeneration     *uint64    `json:"next_generation,omitempty"`
	RunMode            string     `json:"run_mode,omitempty"`
	Method             string     `json:"method,omitempty"`
	Source             string     `json:"source,omitempty"`
	ParentRunID        string     `json:"parent_run_id,omitempty"`
	ForkRunID          string     `json:"fork_run_id,omitempty"`
	TriggerEventType   string     `json:"trigger_event_type,omitempty"`
	ControlReason      string     `json:"control_reason,omitempty"`
	Level              string     `json:"level,omitempty"`
	Spend              string     `json:"spend,omitempty"`
	Cap                string     `json:"cap,omitempty"`
	Percentage         string     `json:"percentage,omitempty"`
	Period             string     `json:"period,omitempty"`
	OperationalState   string     `json:"operational_state,omitempty"`
	BlockingLayer      string     `json:"blocking_layer,omitempty"`
	AuthorSubjectType  string     `json:"author_subject_type,omitempty"`
	AuthorSubjectID    string     `json:"author_subject_id,omitempty"`
}

type Draft struct {
	OccurrenceID      string
	Kind              Kind
	Version           int
	Transition        string
	SourceOwner       string
	SourceIdentity    string
	DedupKey          string
	OccurredAt        time.Time
	RunID             string
	EntityID          string
	AgentID           string
	FlowID            string
	Scope             Scope
	AuthorSafeSummary string
	Projection        Projection
	Failure           *runtimefailures.Envelope
}

type Occurrence struct {
	OccurrenceID      string                    `json:"occurrence_id"`
	Sequence          int64                     `json:"sequence"`
	Kind              Kind                      `json:"kind"`
	Version           int                       `json:"version"`
	Transition        string                    `json:"transition"`
	SourceOwner       string                    `json:"source_owner"`
	SourceIdentity    string                    `json:"source_identity"`
	DedupKey          string                    `json:"dedup_key"`
	OccurredAt        time.Time                 `json:"occurred_at"`
	RunID             string                    `json:"run_id,omitempty"`
	EntityID          string                    `json:"entity_id,omitempty"`
	AgentID           string                    `json:"agent_id,omitempty"`
	FlowID            string                    `json:"flow_id,omitempty"`
	Scope             Scope                     `json:"scope"`
	AuthorSafeSummary string                    `json:"author_safe_summary,omitempty"`
	Projection        Projection                `json:"projection"`
	Failure           *runtimefailures.Envelope `json:"failure,omitempty"`
}

type ListOptions struct {
	AfterSequence       int64
	Limit               int
	RunID               string
	EntityID            string
	AgentID             string
	FlowID              string
	RuntimeInstanceID   string
	BundleHashes        []string
	IncludeRuntimeScope bool
}

type ListResult struct {
	Occurrences []Occurrence `json:"occurrences"`
	NextCursor  int64        `json:"next_cursor,omitempty"`
}

type subjectStrategy uint8

const (
	subjectTypedIdentity subjectStrategy = iota + 1
	subjectProducer
	subjectAdapter
)

type kindContract struct {
	Transitions              map[string]struct{}
	SourceOwner              string
	SourceIdentityRequired   bool
	AllowedProjectionFields  map[string]struct{}
	RequiredProjectionFields map[string]struct{}
	FailureTransitions       map[string]struct{}
	SubjectStrategy          subjectStrategy
	SubjectTypes             map[string]struct{}
	ScopeByTransition        map[string]ScopeKind
	HumanVisibleTransitions  map[string]struct{}
	Actions                  map[string]string
	SubjectRenderer          func(Occurrence) string
	ActionRenderer           func(Occurrence, string) string
}

var kindContracts = map[Kind]kindContract{
	KindInboundReceived: {
		Transitions: set("received"), SourceOwner: "events", SourceIdentityRequired: true,
		AllowedProjectionFields:  set("subject_type", "subject_id", "provider", "author_subject_type", "author_subject_id"),
		RequiredProjectionFields: set("subject_type", "subject_id"),
		SubjectStrategy:          subjectTypedIdentity, SubjectTypes: set("entity"),
		ScopeByTransition: scopeAll(ScopeBundle, "received"), HumanVisibleTransitions: set("received"),
		Actions: map[string]string{"received": "message received"}, SubjectRenderer: renderInboundSubject, ActionRenderer: renderInboundAction,
	},
	KindEventEmitted: {
		Transitions: set("emitted"), SourceOwner: "events", SourceIdentityRequired: true,
		AllowedProjectionFields:  set("event_type", "producer_type", "producer_id", "execution_mode"),
		RequiredProjectionFields: set("event_type", "producer_type", "producer_id"),
		SubjectStrategy:          subjectProducer,
		ScopeByTransition:        scopeAll(ScopeBundle, "emitted"), HumanVisibleTransitions: set("emitted"),
		Actions: map[string]string{"emitted": "event"}, ActionRenderer: renderEventAction,
	},
	KindEntityLifecycle: {
		Transitions: set("created", "stage_changed"), SourceOwner: "entity_mutations", SourceIdentityRequired: true,
		AllowedProjectionFields:  set("subject_type", "subject_id", "old_state", "new_state", "writer_type", "writer_id", "author_subject_type", "author_subject_id"),
		RequiredProjectionFields: set("subject_type", "subject_id"),
		SubjectStrategy:          subjectTypedIdentity, SubjectTypes: set("entity"),
		ScopeByTransition: scopeAll(ScopeBundle, "created", "stage_changed"), HumanVisibleTransitions: set("created", "stage_changed"),
		Actions: map[string]string{"created": "created", "stage_changed": "stage changed"}, SubjectRenderer: renderEntitySubject, ActionRenderer: renderEntityAction,
	},
	KindDeliveryLifecycle: {
		Transitions: set("in_progress", "delivered", "failed", "dead_letter", "terminalized"), SourceOwner: "event_deliveries", SourceIdentityRequired: true,
		AllowedProjectionFields:  set("subject_type", "subject_id", "event_type", "subscriber_type", "subscriber_id", "retry_count", "reason_code"),
		RequiredProjectionFields: set("subject_type", "subject_id"), FailureTransitions: set("failed", "dead_letter"),
		SubjectStrategy: subjectTypedIdentity, SubjectTypes: set("agent", "node"),
		ScopeByTransition: scopeAll(ScopeBundle, "in_progress", "delivered", "failed", "dead_letter", "terminalized"), HumanVisibleTransitions: set("delivered", "failed", "dead_letter"),
		Actions: map[string]string{"in_progress": "in flight", "delivered": "✓ sent", "failed": "✗ failed", "dead_letter": "✗ failed", "terminalized": "terminalized"},
	},
	KindDeadLetterRecorded: {
		Transitions: set("recorded"), SourceOwner: "dead_letters", SourceIdentityRequired: true,
		AllowedProjectionFields:  set("subject_type", "subject_id", "event_type", "retry_count", "reason_code", "node_id"),
		RequiredProjectionFields: set("subject_type", "subject_id"), FailureTransitions: set("recorded"),
		SubjectStrategy: subjectTypedIdentity, SubjectTypes: set("event"),
		ScopeByTransition: scopeAll(ScopeBundle, "recorded"), HumanVisibleTransitions: set("recorded"),
		Actions: map[string]string{"recorded": "✗ event failed"}, SubjectRenderer: renderDeadLetterSubject,
	},
	KindActivityLifecycle: {
		Transitions: set("started", "succeeded", "failed", "uncertain"), SourceOwner: "activity_attempts", SourceIdentityRequired: true,
		AllowedProjectionFields:  set("subject_type", "subject_id", "node_id", "activity", "tool", "effect_class", "attempt", "event_type", "execution_mode"),
		RequiredProjectionFields: set("subject_type", "subject_id", "execution_mode"), FailureTransitions: set("failed", "uncertain"),
		SubjectStrategy: subjectTypedIdentity, SubjectTypes: set("activity"),
		ScopeByTransition: scopeAll(ScopeBundle, "started", "succeeded", "failed", "uncertain"), HumanVisibleTransitions: set("succeeded", "failed", "uncertain"),
		Actions: map[string]string{"started": "in flight", "succeeded": "completed", "failed": "✗ failed", "uncertain": "outcome uncertain"}, SubjectRenderer: renderActivitySubject,
	},
	KindEffectLifecycle: {
		Transitions: set("launched", "terminal_failure", "outcome_uncertain"), SourceOwner: "runtime_external_effect_attempts", SourceIdentityRequired: true,
		AllowedProjectionFields:  set("adapter", "transport", "authority_kind", "authority_id", "effect_class", "attempt", "execution_mode"),
		RequiredProjectionFields: set("adapter", "transport", "authority_kind", "authority_id"), FailureTransitions: set("terminal_failure", "outcome_uncertain"),
		SubjectStrategy:   subjectAdapter,
		ScopeByTransition: scopeAll(ScopeBundle, "launched", "terminal_failure", "outcome_uncertain"), HumanVisibleTransitions: set("terminal_failure", "outcome_uncertain"),
		Actions: map[string]string{"launched": "in flight", "terminal_failure": "✗ failed", "outcome_uncertain": "outcome uncertain"},
	},
	KindTurnLifecycle: {
		Transitions: set("completed", "failed"), SourceOwner: "agent_turns", SourceIdentityRequired: true,
		AllowedProjectionFields:  set("subject_type", "subject_id", "turn_id", "duration_ms", "parse_ok", "usage_exactness", "input_tokens", "output_tokens", "retry_count", "event_type", "execution_mode"),
		RequiredProjectionFields: set("subject_type", "subject_id"), FailureTransitions: set("failed"),
		SubjectStrategy: subjectTypedIdentity, SubjectTypes: set("agent"),
		ScopeByTransition: scopeAll(ScopeBundle, "completed", "failed"), HumanVisibleTransitions: set("completed", "failed"),
		Actions: map[string]string{"completed": "turn completed", "failed": "turn failed"},
	},
	KindTurnToolCompleted: {
		Transitions: set("completed"), SourceOwner: "agent_turns", SourceIdentityRequired: true,
		AllowedProjectionFields:  set("subject_type", "subject_id", "turn_id", "tool_name", "tool_use_id", "execution_mode"),
		RequiredProjectionFields: set("subject_type", "subject_id"),
		SubjectStrategy:          subjectTypedIdentity, SubjectTypes: set("agent"),
		ScopeByTransition: scopeAll(ScopeBundle, "completed"), HumanVisibleTransitions: set(),
		Actions: map[string]string{"completed": "tool completed"}, ActionRenderer: renderTurnToolAction,
	},
	KindCardLifecycle: {
		Transitions: set("created", "decided", "deferred", "expired", "superseded"), SourceOwner: "decision_card_changes", SourceIdentityRequired: true,
		AllowedProjectionFields:  set("subject_type", "subject_id", "card_id", "anchor_kind", "anchor_id", "decision_id", "verdict", "defer_until", "supersede_reason", "execution_mode"),
		RequiredProjectionFields: set("subject_type", "subject_id"),
		SubjectStrategy:          subjectTypedIdentity, SubjectTypes: set("card"),
		ScopeByTransition: scopeAll(ScopeBundle, "created", "decided", "deferred", "expired", "superseded"), HumanVisibleTransitions: set("created", "decided", "deferred", "expired", "superseded"),
		Actions: map[string]string{"created": "created", "decided": "decided", "deferred": "deferred", "expired": "expired", "superseded": "superseded"},
	},
	KindAgentLifecycle: {
		Transitions: set("registered", "running", "terminated", "failed"), SourceOwner: "agent_lifecycle_transition_facts", SourceIdentityRequired: true,
		AllowedProjectionFields:  set("subject_type", "subject_id", "previous_phase", "next_phase", "previous_generation", "next_generation", "run_mode"),
		RequiredProjectionFields: set("subject_type", "subject_id"),
		SubjectStrategy:          subjectTypedIdentity, SubjectTypes: set("agent"),
		ScopeByTransition: scopeAll(ScopeBundle, "registered", "running", "terminated", "failed"), HumanVisibleTransitions: set("failed"),
		Actions: map[string]string{"registered": "registered", "running": "running", "terminated": "terminated", "failed": "failed"},
	},
	KindDirectiveLifecycle: {
		Transitions: set("received", "in_flight", "completed", "failed", "outcome_uncertain"), SourceOwner: "agent_directive_operations", SourceIdentityRequired: true,
		AllowedProjectionFields:  set("subject_type", "subject_id", "method", "source"),
		RequiredProjectionFields: set("subject_type", "subject_id"), FailureTransitions: set("failed", "outcome_uncertain"),
		SubjectStrategy: subjectTypedIdentity, SubjectTypes: set("agent"),
		ScopeByTransition: scopeAll(ScopeBundle, "received", "in_flight", "completed", "failed", "outcome_uncertain"), HumanVisibleTransitions: set("completed", "failed", "outcome_uncertain"),
		Actions: map[string]string{"received": "directive received", "in_flight": "directive in flight", "completed": "directive completed", "failed": "directive failed", "outcome_uncertain": "directive outcome uncertain"},
	},
	KindRunLifecycle: {
		Transitions: set("started", "fork_prepared", "paused", "resumed", "fork_started", "completed", "failed", "cancelled", "forked"), SourceOwner: "runs", SourceIdentityRequired: true,
		AllowedProjectionFields:  set("subject_type", "subject_id", "parent_run_id", "fork_run_id", "trigger_event_type", "control_reason", "source"),
		RequiredProjectionFields: set("subject_type", "subject_id"), FailureTransitions: set("failed"),
		SubjectStrategy: subjectTypedIdentity, SubjectTypes: set("run"),
		ScopeByTransition: scopeAll(ScopeBundle, "started", "fork_prepared", "paused", "resumed", "fork_started", "completed", "failed", "cancelled", "forked"), HumanVisibleTransitions: set("started", "fork_prepared", "paused", "resumed", "fork_started", "completed", "failed", "cancelled", "forked"),
		Actions: map[string]string{"started": "started", "fork_prepared": "fork prepared", "paused": "paused", "resumed": "resumed", "fork_started": "fork started", "completed": "completed", "failed": "failed", "cancelled": "cancelled", "forked": "forked"},
	},
	KindPlatformSignal: {
		Transitions: set("agent_failed_retrying", "agent_failed", "event_quarantined", "dead_letters_escalated", "run_stalled", "runtime_reset", "authorization_required", "budget_warning", "budget_throttle", "budget_emergency", "budget_ok", "runtime_paused", "runtime_resumed", "recovery_failed"),
		SourceOwner: "events", SourceIdentityRequired: true,
		AllowedProjectionFields:  set("subject_type", "subject_id", "event_type", "retry_count", "reason_code", "tool", "level", "spend", "cap", "percentage", "period", "source", "operational_state", "blocking_layer"),
		RequiredProjectionFields: set("subject_type", "subject_id"), FailureTransitions: set("agent_failed_retrying", "agent_failed", "authorization_required", "recovery_failed"),
		SubjectStrategy: subjectTypedIdentity, SubjectTypes: set("agent", "entity", "run", "event", "platform"),
		ScopeByTransition: map[string]ScopeKind{
			"agent_failed_retrying": ScopeBundle, "agent_failed": ScopeBundle, "event_quarantined": ScopeBundle,
			"dead_letters_escalated": ScopeBundle, "run_stalled": ScopeBundle, "authorization_required": ScopeBundle,
			"budget_warning": ScopeBundle, "budget_throttle": ScopeBundle, "budget_emergency": ScopeBundle, "budget_ok": ScopeBundle,
			"runtime_reset": ScopeRuntime, "runtime_paused": ScopeRuntime, "runtime_resumed": ScopeRuntime, "recovery_failed": ScopeRuntime,
		},
		HumanVisibleTransitions: set("agent_failed_retrying", "agent_failed", "event_quarantined", "dead_letters_escalated", "run_stalled", "runtime_reset", "authorization_required", "budget_warning", "budget_throttle", "budget_emergency", "runtime_paused", "runtime_resumed", "recovery_failed"),
		Actions: map[string]string{
			"agent_failed_retrying": "failed, retrying", "agent_failed": "failed", "event_quarantined": "event quarantined",
			"dead_letters_escalated": "dead letters escalated", "run_stalled": "run stalled", "runtime_reset": "runtime reset",
			"authorization_required": "authorization required", "budget_warning": "budget warning", "budget_throttle": "budget throttled",
			"budget_emergency": "budget emergency", "budget_ok": "budget ok", "runtime_paused": "runtime paused",
			"runtime_resumed": "runtime resumed", "recovery_failed": "recovery failed",
		}, SubjectRenderer: renderPlatformSignalSubject,
	},
}

func set(values ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

func scopeAll(kind ScopeKind, transitions ...string) map[string]ScopeKind {
	out := make(map[string]ScopeKind, len(transitions))
	for _, transition := range transitions {
		out[transition] = kind
	}
	return out
}

func Kinds() []Kind {
	out := make([]Kind, 0, len(kindContracts))
	for kind := range kindContracts {
		out = append(out, kind)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func ValidateDraft(d Draft) error {
	d.Kind = Kind(strings.TrimSpace(string(d.Kind)))
	contract, ok := kindContracts[d.Kind]
	if !ok {
		return fmt.Errorf("author activity kind %q is not registered", d.Kind)
	}
	if d.Version == 0 {
		d.Version = Version
	}
	if d.Version != Version {
		return fmt.Errorf("author activity %s version %d is not registered", d.Kind, d.Version)
	}
	transition := strings.TrimSpace(d.Transition)
	if _, ok := contract.Transitions[transition]; !ok {
		return fmt.Errorf("author activity %s transition %q is not registered", d.Kind, transition)
	}
	sourceOwner := strings.TrimSpace(d.SourceOwner)
	if sourceOwner != contract.SourceOwner {
		return fmt.Errorf("author activity %s source_owner %q is not registered; expected %q", d.Kind, sourceOwner, contract.SourceOwner)
	}
	if contract.SourceIdentityRequired && strings.TrimSpace(d.SourceIdentity) == "" {
		return fmt.Errorf("author activity %s source_identity is required", d.Kind)
	}
	if strings.TrimSpace(d.DedupKey) == "" {
		return fmt.Errorf("author activity %s dedup_key is required", d.Kind)
	}
	if d.OccurredAt.IsZero() {
		return fmt.Errorf("author activity %s occurred_at is required", d.Kind)
	}
	if err := validateScope(d.Kind, transition, d.Scope); err != nil {
		return err
	}
	if _, err := NormalizeAuthorSafeSummary(d.AuthorSafeSummary); err != nil {
		return fmt.Errorf("author activity %s/%s author_safe_summary: %w", d.Kind, transition, err)
	}
	if err := validateProjection(d.Kind, contract, d.Projection); err != nil {
		return err
	}
	if failureRequired(d.Kind, transition) && d.Failure == nil {
		return fmt.Errorf("author activity %s/%s requires canonical failure", d.Kind, transition)
	}
	if d.Failure != nil {
		if err := runtimefailures.ValidateEnvelope(*d.Failure); err != nil {
			return fmt.Errorf("author activity %s/%s failure: %w", d.Kind, transition, err)
		}
	}
	return nil
}

func validateScope(kind Kind, transition string, scope Scope) error {
	contract, ok := kindContracts[kind]
	if !ok {
		return fmt.Errorf("author activity kind %q is not registered", kind)
	}
	want, ok := contract.ScopeByTransition[strings.TrimSpace(transition)]
	if !ok {
		return fmt.Errorf("author activity %s/%s scope policy is not registered", kind, transition)
	}
	scope.Kind = ScopeKind(strings.TrimSpace(string(scope.Kind)))
	scope.RuntimeInstanceID = strings.TrimSpace(scope.RuntimeInstanceID)
	scope.BundleHash = strings.TrimSpace(scope.BundleHash)
	if scope.Kind != want {
		return fmt.Errorf("author activity %s/%s scope kind %q is not registered; expected %q", kind, transition, scope.Kind, want)
	}
	switch scope.Kind {
	case ScopeBundle:
		if scope.RuntimeInstanceID == "" || scope.BundleHash == "" {
			return fmt.Errorf("author activity %s/%s bundle scope requires runtime_instance_id and bundle_hash", kind, transition)
		}
	case ScopeRuntime:
		if scope.RuntimeInstanceID == "" || scope.BundleHash != "" {
			return fmt.Errorf("author activity %s/%s runtime scope requires runtime_instance_id and forbids bundle_hash", kind, transition)
		}
	case ScopeGlobal:
		if scope.RuntimeInstanceID != "" || scope.BundleHash != "" {
			return fmt.Errorf("author activity %s/%s global scope forbids runtime_instance_id and bundle_hash", kind, transition)
		}
	default:
		return fmt.Errorf("author activity %s/%s scope kind %q is not registered", kind, transition, scope.Kind)
	}
	return nil
}

func NormalizeAuthorSafeSummary(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if !utf8.ValidString(value) {
		return "", fmt.Errorf("must be valid UTF-8")
	}
	var normalized strings.Builder
	spacePending := false
	for _, r := range value {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			spacePending = normalized.Len() > 0
			continue
		}
		if spacePending {
			normalized.WriteByte(' ')
			spacePending = false
		}
		normalized.WriteRune(r)
	}
	runes := []rune(strings.TrimSpace(normalized.String()))
	if len(runes) > 24 {
		runes = runes[:24]
	}
	return string(runes), nil
}

func HumanVisible(kind Kind, transition string) bool {
	contract, ok := kindContracts[kind]
	if !ok {
		return false
	}
	_, visible := contract.HumanVisibleTransitions[strings.TrimSpace(transition)]
	return visible
}

func failureRequired(kind Kind, transition string) bool {
	contract, ok := kindContracts[kind]
	if !ok {
		return false
	}
	_, required := contract.FailureTransitions[strings.TrimSpace(transition)]
	return required
}

func validateProjection(kind Kind, contract kindContract, projection Projection) error {
	raw, err := json.Marshal(projection)
	if err != nil {
		return fmt.Errorf("marshal author activity %s projection: %w", kind, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return fmt.Errorf("decode author activity %s projection: %w", kind, err)
	}
	for field := range fields {
		if _, ok := contract.AllowedProjectionFields[field]; !ok {
			return fmt.Errorf("author activity %s projection field %q is not registered", kind, field)
		}
	}
	for _, field := range sortedSet(contract.RequiredProjectionFields) {
		if !projectionFieldPopulated(fields[field]) {
			return fmt.Errorf("author activity %s projection field %q is required", kind, field)
		}
	}
	if contract.SubjectStrategy == subjectTypedIdentity {
		subjectType := strings.TrimSpace(projection.SubjectType)
		if _, ok := contract.SubjectTypes[subjectType]; !ok {
			return fmt.Errorf("author activity %s subject_type %q is not registered", kind, subjectType)
		}
	}
	if mode := strings.TrimSpace(projection.ExecutionMode); mode != "" && mode != "live" && mode != "mock" {
		return fmt.Errorf("author activity %s execution_mode %q is not registered", kind, mode)
	}
	return nil
}

func projectionFieldPopulated(raw json.RawMessage) bool {
	if len(raw) == 0 || string(raw) == "null" {
		return false
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return false
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text) != ""
	}
	return true
}

func sortedSet(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func cloneDraft(d Draft) Draft {
	d.SourceOwner = strings.TrimSpace(d.SourceOwner)
	d.SourceIdentity = strings.TrimSpace(d.SourceIdentity)
	d.DedupKey = strings.TrimSpace(d.DedupKey)
	d.Transition = strings.TrimSpace(d.Transition)
	d.RunID = strings.TrimSpace(d.RunID)
	d.EntityID = strings.TrimSpace(d.EntityID)
	d.AgentID = strings.TrimSpace(d.AgentID)
	d.FlowID = strings.TrimSpace(d.FlowID)
	d.OccurredAt = d.OccurredAt.UTC()
	d.Failure = runtimefailures.CloneEnvelope(d.Failure)
	return d
}

func draftsEqual(left, right Draft) bool {
	left.OccurrenceID = ""
	right.OccurrenceID = ""
	return reflect.DeepEqual(cloneDraft(left), cloneDraft(right))
}
