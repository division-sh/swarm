package fanoutobligation

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

var ErrStaleClaim = errors.New("stale fan-out claim")

const (
	InitialChunkSize = MaxChunkSize
	MinChunkSize     = 1
	MaxChunkSize     = 32
)

// RetryChunkSize applies only after a typed retry failure, never after a slow success.
func RetryChunkSize(current int) int {
	return max(MinChunkSize, (current+1)/2)
}

type SourceKind string

const (
	SourceEventPayloadField SourceKind = "event_payload_field"
	SourceEntityField       SourceKind = "entity_field_revision"
	SourceResourceVersion   SourceKind = "resource_version"
)

type SourceRef struct {
	Kind        SourceKind                 `json:"kind"`
	EventID     string                     `json:"event_id,omitempty"`
	RunID       string                     `json:"run_id,omitempty"`
	EntityID    string                     `json:"entity_id,omitempty"`
	Field       string                     `json:"field,omitempty"`
	MutationID  string                     `json:"mutation_id,omitempty"`
	Declaration durabledata.DeclarationRef `json:"declaration,omitempty"`
	VersionID   durabledata.VersionID      `json:"version_id,omitempty"`
}

func (r SourceRef) Validate(persisted bool) error {
	if strings.TrimSpace(r.Field) == "" && r.Kind != SourceResourceVersion {
		return errors.New("fan-out source requires exact top-level field")
	}
	switch r.Kind {
	case SourceEventPayloadField:
		if _, err := uuid.Parse(strings.TrimSpace(r.EventID)); err != nil || r.RunID != "" || r.EntityID != "" || r.MutationID != "" || r.Declaration.Validate() == nil || r.VersionID != "" {
			return errors.New("fan-out payload source requires only exact event and field")
		}
	case SourceEntityField:
		if _, err := uuid.Parse(strings.TrimSpace(r.RunID)); err != nil {
			return errors.New("fan-out entity source requires exact source run identity")
		}
		if _, err := uuid.Parse(strings.TrimSpace(r.EntityID)); err != nil || r.EventID != "" || r.Declaration.Validate() == nil || r.VersionID != "" {
			return errors.New("fan-out entity source requires only exact entity, field, and mutation revision")
		}
		if persisted {
			if _, err := uuid.Parse(strings.TrimSpace(r.MutationID)); err != nil {
				return errors.New("persisted fan-out entity source requires exact mutation revision")
			}
		} else if r.MutationID != "" {
			return errors.New("fan-out entity source request cannot author a mutation revision")
		}
	case SourceResourceVersion:
		if err := r.Declaration.Validate(); err != nil {
			return err
		}
		if err := r.VersionID.Validate(); err != nil {
			return err
		}
		if r.EventID != "" || r.RunID != "" || r.EntityID != "" || r.Field != "" || r.MutationID != "" {
			return errors.New("fan-out resource source carries only declaration and exact version")
		}
	default:
		return fmt.Errorf("fan-out source kind %q is invalid", r.Kind)
	}
	return nil
}

// ExecutionReceiver is the receiver context of an already admitted handler,
// not a publication obligation. Initialization and dependency authority cannot
// be inherited by persisting this projection.
type ExecutionReceiver struct {
	Node   identity.ExecutableNode        `json:"node"`
	Target events.DeliveryTargetOwnership `json:"target"`
}

func ProjectExecutionReceiver(route events.DeliveryRoute) (ExecutionReceiver, error) {
	if _, err := route.Identity(); err != nil {
		return ExecutionReceiver{}, err
	}
	node, ok := route.Recipient.Node()
	if !ok {
		return ExecutionReceiver{}, fmt.Errorf("fan-out execution receiver requires a node")
	}
	return ExecutionReceiver{Node: node, Target: route.Target}, nil
}

type Capsule struct {
	NodeKey          string                    `json:"node_key"`
	ExecutionFlowID  string                    `json:"execution_flow_id"`
	Route            runtimeflowidentity.Route `json:"route"`
	EntityID         string                    `json:"entity_id,omitempty"`
	HandlerEventKey  string                    `json:"handler_event_key"`
	CurrentState     string                    `json:"current_state,omitempty"`
	ChainDepth       int                       `json:"chain_depth"`
	ProducerSource   events.RoutingSource      `json:"producer_source"`
	Receiver         *ExecutionReceiver        `json:"receiver,omitempty"`
	Lineage          events.EventLineage       `json:"lineage"`
	Entity           map[string]any            `json:"entity,omitempty"`
	PlatformEntity   map[string]any            `json:"platform_entity,omitempty"`
	Computed         map[string]any            `json:"computed,omitempty"`
	Accumulated      map[string]any            `json:"accumulated,omitempty"`
	Join             map[string]any            `json:"join,omitempty"`
	Loop             map[string]any            `json:"loop,omitempty"`
	StateFields      map[string]any            `json:"state_fields,omitempty"`
	StateBookkeeping map[string]any            `json:"state_bookkeeping,omitempty"`
	StateGates       map[string]bool           `json:"state_gates,omitempty"`
}

// Capsule business values are frozen JSON, not floating-point approximations.
// Owning decoding here gives live, fixed-revision, and fork readers one law.
func (c *Capsule) UnmarshalJSON(raw []byte) error {
	type wire Capsule
	var decoded wire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	*c = Capsule(decoded)
	return nil
}

func (c Capsule) Equal(other Capsule) bool {
	left, err := json.Marshal(c)
	if err != nil {
		return false
	}
	right, err := json.Marshal(other)
	return err == nil && bytes.Equal(left, right)
}

// MarshalCapsule preserves the admitted integer-versus-double carrier kind
// needed when the capsule is evaluated after its originating handler returns.
func MarshalCapsule(c Capsule) ([]byte, error) {
	return canonicaljson.MarshalPreservingNumberKinds(c)
}

func (c Capsule) Validate() error {
	if strings.TrimSpace(c.NodeKey) == "" || strings.TrimSpace(c.HandlerEventKey) == "" || strings.TrimSpace(c.Lineage.RunID) == "" || strings.TrimSpace(c.Lineage.ParentEventID) == "" {
		return errors.New("fan-out capsule requires exact node, handler, run, and parent event identity")
	}
	if !c.Route.Valid() || c.ChainDepth < 0 || c.ProducerSource.Empty() {
		return errors.New("fan-out capsule requires route, producer source, and nonnegative chain depth")
	}
	if c.Receiver != nil {
		r := c.Receiver
		if err := r.Target.Validate(); err != nil {
			return fmt.Errorf("fan-out capsule receiver: %w", err)
		}
		target := r.Target.Route()
		if !r.Node.Valid() || r.Node.Key() != c.NodeKey || r.Node.FlowPath() != c.ExecutionFlowID || target.FlowID != c.ExecutionFlowID || target.FlowInstance != c.Route.InstancePath || target.EntityID != c.EntityID {
			return fmt.Errorf("fan-out capsule receiver contradicts its execution owner")
		}
	}
	if !c.Lineage.ExecutionMode.Valid() {
		return errors.New("fan-out capsule requires a valid execution mode")
	}
	return nil
}

type IntentKey struct {
	RunID                string                            `json:"run_id"`
	TriggeringDeliveryID string                            `json:"triggering_delivery_id"`
	ElementRef           runtimecontracts.FanOutElementRef `json:"element_ref"`
}

func (k IntentKey) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(k.RunID)); err != nil {
		return errors.New("fan-out intent requires canonical run identity")
	}
	if _, err := uuid.Parse(strings.TrimSpace(k.TriggeringDeliveryID)); err != nil {
		return errors.New("fan-out intent requires canonical triggering delivery identity")
	}
	if _, err := k.ElementRef.DeclarationIdentity(); err != nil {
		return err
	}
	return nil
}

func (k IntentKey) String() string {
	identity, _ := k.ElementRef.DeclarationIdentity()
	return strings.Join([]string{strings.TrimSpace(k.RunID), strings.TrimSpace(k.TriggeringDeliveryID), identity.Key()}, "|")
}

type IntentRequest struct {
	Key         IntentKey                      `json:"key"`
	PlanRef     runtimecontracts.FanOutPlanRef `json:"plan_ref"`
	Source      SourceRef                      `json:"source"`
	Cardinality int                            `json:"cardinality"`
	Capsule     Capsule                        `json:"capsule"`
}

func (r IntentRequest) Validate() error {
	if err := r.Key.Validate(); err != nil {
		return err
	}
	if r.Key.ElementRef != r.PlanRef.ElementRef || strings.TrimSpace(r.PlanRef.BundleHash) == "" || strings.TrimSpace(r.PlanRef.SemanticDigest) == "" {
		return errors.New("fan-out intent plan identity is incomplete or contradictory")
	}
	if err := r.Source.Validate(false); err != nil {
		return err
	}
	if r.Source.Kind == SourceEventPayloadField && strings.TrimSpace(r.Source.EventID) != strings.TrimSpace(r.Capsule.Lineage.ParentEventID) {
		return errors.New("fan-out payload source must be the exact triggering event")
	}
	// A same-run capture reads the executing entity. A retained ancestor source
	// remains immutable; its exact revision and lineage are admitted by the reader.
	if r.Source.Kind == SourceEntityField && r.Source.RunID == r.Key.RunID && strings.TrimSpace(r.Source.EntityID) != strings.TrimSpace(r.Capsule.EntityID) {
		return errors.New("fan-out entity source must be the exact selected execution entity")
	}
	if r.Cardinality < 0 {
		return errors.New("fan-out cardinality cannot be negative")
	}
	return r.Capsule.Validate()
}

type Status string

const (
	StatusOpen     Status = "open"
	StatusClosed   Status = "closed"
	StatusCanceled Status = "canceled"
	StatusBlocked  Status = "blocked"
)

type Intent struct {
	Request         IntentRequest `json:"request"`
	Source          SourceRef     `json:"source"`
	Cursor          int           `json:"cursor"`
	Status          Status        `json:"status"`
	NextChunkSize   int           `json:"next_chunk_size"`
	LastServedAt    time.Time     `json:"last_served_at,omitempty"`
	Retry           *RetryWait    `json:"retry,omitempty"`
	CreatedAt       time.Time     `json:"created_at"`
	UpdatedAt       time.Time     `json:"updated_at"`
	ClaimOwner      string        `json:"claim_owner,omitempty"`
	ClaimGeneration uint64        `json:"claim_generation,omitempty"`
	LeaseExpiresAt  time.Time     `json:"lease_expires_at,omitempty"`
	BlockedReason   string        `json:"blocked_reason,omitempty"`
}

// ChunkEndOrdinal bounds an already validated intent's budget by remaining work.
func (i Intent) ChunkEndOrdinal() int {
	return i.Cursor + min(i.NextChunkSize, i.Request.Cardinality-i.Cursor)
}

func (i Intent) Validate() error {
	if err := i.Request.Validate(); err != nil {
		return err
	}
	if err := i.Source.Validate(true); err != nil {
		return err
	}
	wantSource := i.Request.Source
	if wantSource.Kind == SourceEntityField {
		wantSource.MutationID = i.Source.MutationID
	}
	if i.Source != wantSource {
		return errors.New("fan-out persisted source disagrees with request")
	}
	if i.Cursor < 0 || i.Cursor > i.Request.Cardinality || i.NextChunkSize < MinChunkSize || i.NextChunkSize > MaxChunkSize {
		return errors.New("fan-out intent cursor or chunk size is invalid")
	}
	if i.CreatedAt.IsZero() || i.UpdatedAt.Before(i.CreatedAt) {
		return errors.New("fan-out intent timestamps are invalid")
	}
	if i.Retry != nil {
		if err := i.Retry.Validate(); err != nil {
			return err
		}
		if i.Status != StatusOpen || i.ClaimOwner != "" {
			return errors.New("fan-out retry wait requires open unclaimed work")
		}
	}
	if strings.TrimSpace(i.ClaimOwner) == "" {
		if !i.LeaseExpiresAt.IsZero() {
			return errors.New("unclaimed fan-out intent cannot retain a lease")
		}
	} else if i.ClaimGeneration == 0 || i.LeaseExpiresAt.IsZero() {
		return errors.New("claimed fan-out intent requires generation and lease")
	}
	switch i.Status {
	case StatusOpen:
		if i.Cursor >= i.Request.Cardinality || strings.TrimSpace(i.BlockedReason) != "" {
			return errors.New("open fan-out intent must owe an ordinal")
		}
	case StatusClosed:
		if i.Cursor != i.Request.Cardinality || strings.TrimSpace(i.BlockedReason) != "" || strings.TrimSpace(i.ClaimOwner) != "" {
			return errors.New("closed fan-out intent must consume every ordinal")
		}
	case StatusCanceled:
		if strings.TrimSpace(i.BlockedReason) == "" || strings.TrimSpace(i.ClaimOwner) != "" {
			return errors.New("canceled fan-out intent requires a reason and no active claim")
		}
	case StatusBlocked:
		if i.Cursor >= i.Request.Cardinality || strings.TrimSpace(i.BlockedReason) == "" || strings.TrimSpace(i.ClaimOwner) != "" {
			return errors.New("blocked fan-out intent must owe an ordinal, carry typed failure evidence, and have no active claim")
		}
		failure, err := runtimefailures.UnmarshalEnvelope([]byte(i.BlockedReason))
		if err != nil {
			return fmt.Errorf("blocked fan-out intent failure evidence: %w", err)
		}
		if err := runtimefailures.ValidateEnvelope(failure); err != nil {
			return fmt.Errorf("blocked fan-out intent failure evidence: %w", err)
		}
	default:
		return fmt.Errorf("fan-out status %q is invalid", i.Status)
	}
	return nil
}

type RetryWait struct {
	ReadyAt time.Time                `json:"ready_at"`
	Failure runtimefailures.Envelope `json:"failure"`
}

func (r RetryWait) Validate() error {
	if r.ReadyAt.IsZero() {
		return errors.New("fan-out retry wait requires due time")
	}
	if err := runtimefailures.ValidateEnvelope(r.Failure); err != nil {
		return err
	}
	if !r.Failure.Retryable || r.Failure.Class == runtimefailures.ClassOutcomeUncertain {
		return errors.New("fan-out retry wait requires a safely retryable failure")
	}
	return nil
}

type ServingState uint8

const (
	ServingEligible ServingState = iota + 1
	ServingLeased
	ServingRetryWait
	ServingBlocked
	ServingClosed
	ServingCanceled
)

// ServingAt projects durable facts only. Runtime/source admission and shared
// capacity are separate evidence; absence of either cannot mean completion.
func (i Intent) ServingAt(now time.Time) (ServingState, error) {
	if err := i.Validate(); err != nil {
		return 0, err
	}
	if now.IsZero() {
		return 0, errors.New("fan-out serving projection requires observation time")
	}
	switch i.Status {
	case StatusClosed:
		return ServingClosed, nil
	case StatusCanceled:
		return ServingCanceled, nil
	case StatusBlocked:
		return ServingBlocked, nil
	case StatusOpen:
		if i.ClaimOwner != "" && i.LeaseExpiresAt.After(now) {
			return ServingLeased, nil
		}
		if i.Retry != nil && i.Retry.ReadyAt.After(now) {
			return ServingRetryWait, nil
		}
		return ServingEligible, nil
	default:
		return 0, errors.New("fan-out serving status is invalid")
	}
}

type Claim struct {
	Key        IntentKey `json:"key"`
	Owner      string    `json:"owner"`
	Generation uint64    `json:"generation"`
	LeaseUntil time.Time `json:"lease_until"`
}

func (c Claim) Validate() error {
	if err := c.Key.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(c.Owner) == "" || c.Generation == 0 || c.LeaseUntil.IsZero() {
		return errors.New("fan-out claim requires owner, generation, and lease")
	}
	return nil
}

// AdmitClaim checks persisted ownership at the selected store's admission time.
// A caller's audit timestamp and claimed lease deadline cannot extend authority.
func (i Intent) AdmitClaim(claim Claim, admittedAt time.Time) error {
	if err := i.Validate(); err != nil {
		return err
	}
	if err := claim.Validate(); err != nil {
		return err
	}
	if admittedAt.IsZero() || i.Request.Key != claim.Key || i.Status != StatusOpen ||
		i.ClaimOwner != claim.Owner || i.ClaimGeneration != claim.Generation ||
		!i.LeaseExpiresAt.After(admittedAt) {
		return ErrStaleClaim
	}
	return nil
}

type OutcomeKind string

const (
	OutcomeCommitted        OutcomeKind = "committed"
	OutcomeSemanticRejected OutcomeKind = "semantic_rejected"
)

type Outcome struct {
	Ordinal              int                          `json:"ordinal"`
	Kind                 OutcomeKind                  `json:"kind"`
	EventID              string                       `json:"event_id,omitempty"`
	SourceEventID        string                       `json:"source_event_id,omitempty"`
	InheritedDisposition InheritedTerminalDisposition `json:"inherited_disposition,omitempty"`
	Failure              json.RawMessage              `json:"failure,omitempty"`
	CreatedAt            time.Time                    `json:"created_at"`
}

type InheritedTerminalDisposition string

const (
	InheritedSucceeded    InheritedTerminalDisposition = "succeeded"
	InheritedDeadLettered InheritedTerminalDisposition = "dead_lettered"
	InheritedNoRoute      InheritedTerminalDisposition = "no_route"
)

func (d InheritedTerminalDisposition) Valid() bool {
	return d == InheritedSucceeded || d == InheritedDeadLettered || d == InheritedNoRoute
}

func (o Outcome) Validate() error {
	if o.Ordinal < 0 || o.CreatedAt.IsZero() {
		return errors.New("fan-out outcome requires ordinal and creation time")
	}
	switch o.Kind {
	case OutcomeCommitted:
		if (strings.TrimSpace(o.EventID) == "") == (strings.TrimSpace(o.SourceEventID) == "") || len(o.Failure) != 0 {
			return errors.New("committed fan-out outcome requires exactly one owned or inherited event")
		}
		if strings.TrimSpace(o.SourceEventID) == "" && o.InheritedDisposition != "" || strings.TrimSpace(o.SourceEventID) != "" && !o.InheritedDisposition.Valid() {
			return errors.New("fan-out inherited event requires its exact terminal disposition")
		}
	case OutcomeSemanticRejected:
		if o.EventID != "" || o.SourceEventID != "" || o.InheritedDisposition != "" {
			return errors.New("rejected fan-out outcome requires only typed failure evidence")
		}
		if err := ValidateSemanticRejection(o.Failure); err != nil {
			return fmt.Errorf("rejected fan-out outcome: %w", err)
		}
	default:
		return fmt.Errorf("fan-out outcome kind %q is invalid", o.Kind)
	}
	return nil
}

func ValidateSemanticRejection(raw json.RawMessage) error {
	failure, err := runtimefailures.UnmarshalEnvelope(raw)
	if err != nil {
		return fmt.Errorf("semantic rejection requires a typed failure envelope: %w", err)
	}
	if failure.Class != runtimefailures.ClassSchemaInvalid || failure.Detail.Code != "emit_payload_contract_violation" || failure.Retryable || !failure.Deterministic {
		return errors.New("semantic rejection requires exact emit-contract failure evidence")
	}
	attributes := failure.Detail.Attributes
	for _, name := range []string{"event", "kind", "path", "constraint", "expected", "detail"} {
		value, present := attributes[name].(string)
		if !present || strings.TrimSpace(value) == "" {
			return fmt.Errorf("semantic rejection emit-contract attribute %q is required", name)
		}
	}
	actual, actualPresent := attributes["actual"].(string)
	if !actualPresent {
		return errors.New("semantic rejection emit-contract attribute \"actual\" is required")
	}
	switch attributes["kind"] {
	case "schema_mismatch":
		// Empty and whitespace-only strings are legitimate rejected values. The
		// typed attribute's presence distinguishes them from missing evidence.
		return nil
	case "schema_unresolved", "authored_envelope_field":
		if strings.TrimSpace(actual) == "" {
			return errors.New("semantic rejection emit-contract attribute \"actual\" is required")
		}
		return nil
	default:
		return errors.New("semantic rejection requires a recognized emit-contract rejection kind")
	}
}

type RunSummary struct {
	RunID                   string                         `json:"run_id"`
	Intents                 int                            `json:"intents"`
	Open                    int                            `json:"open"`
	Blocked                 int                            `json:"blocked"`
	BlockedIntents          []BlockedIntentDiagnosis       `json:"blocked_intents"`
	Cardinality             int                            `json:"cardinality"`
	Cursor                  int                            `json:"cursor"`
	Owed                    int                            `json:"owed"`
	Committed               int                            `json:"committed"`
	SemanticRejected        int                            `json:"semantic_rejected"`
	SemanticRejectionSample *FanOutSemanticRejectionSample `json:"semantic_rejection_sample"`
	Canceled                int                            `json:"canceled"`
	Settled                 int                            `json:"settled"`
	Unsettled               int                            `json:"unsettled"`
	BarrierArmed            int                            `json:"barrier_armed"`
	BarrierPending          int                            `json:"barrier_closed_pending"`
	BarrierTerminal         int                            `json:"barrier_terminal"`
	MinNextChunk            int                            `json:"min_next_chunk"`
	MaxNextChunk            int                            `json:"max_next_chunk"`
	OldestAgeMS             int64                          `json:"oldest_age_ms"`
}

type FanOutSemanticRejectionSample struct {
	TriggeringDeliveryID string                   `json:"triggering_delivery_id"`
	FlowPath             string                   `json:"flow_path"`
	Family               string                   `json:"family"`
	SemanticPath         string                   `json:"semantic_path"`
	Ordinal              int                      `json:"ordinal"`
	Failure              runtimefailures.Envelope `json:"failure"`
}

func (s FanOutSemanticRejectionSample) Validate() error {
	if s.Family != "fan_out" {
		return errors.New("fan-out semantic rejection sample requires fan_out declaration family")
	}
	if _, err := uuid.Parse(strings.TrimSpace(s.TriggeringDeliveryID)); err != nil {
		return errors.New("fan-out semantic rejection sample requires triggering delivery identity")
	}
	if _, err := (runtimecontracts.FanOutElementRef{FlowPath: s.FlowPath, Family: s.Family, SemanticPath: s.SemanticPath}).DeclarationIdentity(); err != nil {
		return err
	}
	if s.Ordinal < 0 {
		return errors.New("fan-out semantic rejection sample ordinal cannot be negative")
	}
	raw, err := runtimefailures.MarshalEnvelope(s.Failure)
	if err != nil {
		return err
	}
	return ValidateSemanticRejection(raw)
}

type BlockedIntentDiagnosis struct {
	TriggeringDeliveryID string                   `json:"triggering_delivery_id"`
	FlowPath             string                   `json:"flow_path"`
	Family               string                   `json:"family"`
	SemanticPath         string                   `json:"semantic_path"`
	Cursor               int                      `json:"cursor"`
	Owed                 int                      `json:"owed"`
	Failure              runtimefailures.Envelope `json:"failure"`
}

func (d BlockedIntentDiagnosis) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(d.TriggeringDeliveryID)); err != nil {
		return errors.New("blocked fan-out diagnosis requires triggering delivery identity")
	}
	if _, err := (runtimecontracts.FanOutElementRef{FlowPath: d.FlowPath, Family: d.Family, SemanticPath: d.SemanticPath}).DeclarationIdentity(); err != nil {
		return err
	}
	if d.Cursor < 0 || d.Owed <= 0 {
		return errors.New("blocked fan-out diagnosis requires nonnegative cursor and positive owed count")
	}
	return runtimefailures.ValidateEnvelope(d.Failure)
}

func (s RunSummary) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(s.RunID)); err != nil {
		return errors.New("fan-out run summary requires canonical run identity")
	}
	if s.Intents < 0 || s.Open < 0 || s.Blocked < 0 || s.Cardinality < 0 || s.Cursor < 0 || s.Owed < 0 || s.Committed < 0 || s.SemanticRejected < 0 || s.Canceled < 0 || s.Settled < 0 || s.Unsettled < 0 || s.BarrierArmed < 0 || s.BarrierPending < 0 || s.BarrierTerminal < 0 || s.MinNextChunk < 0 || s.MaxNextChunk < 0 || s.OldestAgeMS < 0 {
		return errors.New("fan-out run summary counts cannot be negative")
	}
	if s.Cursor != s.Committed+s.SemanticRejected || s.Cardinality != s.Cursor+s.Owed+s.Canceled || s.Committed != s.Settled+s.Unsettled {
		return errors.New("fan-out run summary progress facts are contradictory")
	}
	if (s.SemanticRejected == 0) != (s.SemanticRejectionSample == nil) {
		return errors.New("fan-out semantic rejection count disagrees with deterministic sample")
	}
	if s.SemanticRejectionSample != nil {
		if s.SemanticRejectionSample.Ordinal >= s.Cardinality {
			return errors.New("fan-out semantic rejection sample ordinal is outside run cardinality")
		}
		if err := s.SemanticRejectionSample.Validate(); err != nil {
			return fmt.Errorf("fan-out semantic rejection sample: %w", err)
		}
	}
	if (s.Intents == 0) != (s.MinNextChunk == 0 && s.MaxNextChunk == 0) || s.MinNextChunk > s.MaxNextChunk || s.MaxNextChunk > MaxChunkSize {
		return errors.New("fan-out run summary adaptive chunk facts are contradictory")
	}
	if s.Open+s.Blocked == 0 && s.Owed != 0 {
		return errors.New("fan-out run summary cannot owe work without an open or blocked intent")
	}
	if len(s.BlockedIntents) != s.Blocked {
		return errors.New("fan-out blocked count disagrees with typed diagnoses")
	}
	for index, diagnosis := range s.BlockedIntents {
		if err := diagnosis.Validate(); err != nil {
			return fmt.Errorf("fan-out blocked diagnosis %d: %w", index, err)
		}
	}
	return nil
}

func (s RunSummary) BlocksCompletion() bool {
	return s.Open > 0 || s.Blocked > 0 || s.Owed > 0 || s.BarrierArmed > 0 || s.BarrierPending > 0
}
