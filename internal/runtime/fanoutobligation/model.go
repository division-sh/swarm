package fanoutobligation

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

var ErrStaleClaim = errors.New("stale fan-out claim")

const (
	InitialChunkSize = 4
	MinChunkSize     = 1
	MaxChunkSize     = 32
)

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

type Capsule struct {
	NodeKey          string                    `json:"node_key"`
	ExecutionFlowID  string                    `json:"execution_flow_id"`
	Route            runtimeflowidentity.Route `json:"route"`
	EntityID         string                    `json:"entity_id,omitempty"`
	HandlerEventKey  string                    `json:"handler_event_key"`
	CurrentState     string                    `json:"current_state,omitempty"`
	ChainDepth       int                       `json:"chain_depth"`
	ProducerSource   events.RoutingSource      `json:"producer_source"`
	DeliveryRoute    *events.DeliveryRoute     `json:"delivery_route,omitempty"`
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

func (c Capsule) Validate() error {
	if strings.TrimSpace(c.NodeKey) == "" || strings.TrimSpace(c.HandlerEventKey) == "" || strings.TrimSpace(c.Lineage.RunID) == "" || strings.TrimSpace(c.Lineage.ParentEventID) == "" {
		return errors.New("fan-out capsule requires exact node, handler, run, and parent event identity")
	}
	if !c.Route.Valid() || c.ChainDepth < 0 || c.ProducerSource.Empty() {
		return errors.New("fan-out capsule requires route, producer source, and nonnegative chain depth")
	}
	if c.DeliveryRoute != nil {
		if _, err := c.DeliveryRoute.Identity(); err != nil {
			return fmt.Errorf("fan-out capsule delivery route: %w", err)
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
	if _, err := k.ElementRef.ContractElementRef(); err != nil {
		return err
	}
	return nil
}

func (k IntentKey) String() string {
	return strings.Join([]string{strings.TrimSpace(k.RunID), strings.TrimSpace(k.TriggeringDeliveryID), strings.TrimSpace(k.ElementRef.PackageKey), strings.TrimSpace(k.ElementRef.ElementID)}, "|")
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
	if r.Source.Kind == SourceEntityField && strings.TrimSpace(r.Source.EntityID) != strings.TrimSpace(r.Capsule.EntityID) {
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
	LastChunkMS     int64         `json:"last_chunk_ms"`
	LastServedAt    time.Time     `json:"last_served_at,omitempty"`
	CreatedAt       time.Time     `json:"created_at"`
	UpdatedAt       time.Time     `json:"updated_at"`
	ClaimOwner      string        `json:"claim_owner,omitempty"`
	ClaimGeneration uint64        `json:"claim_generation,omitempty"`
	LeaseExpiresAt  time.Time     `json:"lease_expires_at,omitempty"`
	BlockedReason   string        `json:"blocked_reason,omitempty"`
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
	if i.Cursor < 0 || i.Cursor > i.Request.Cardinality || i.NextChunkSize < MinChunkSize || i.NextChunkSize > MaxChunkSize || i.LastChunkMS < 0 {
		return errors.New("fan-out intent cursor or chunk size is invalid")
	}
	if i.CreatedAt.IsZero() || i.UpdatedAt.Before(i.CreatedAt) {
		return errors.New("fan-out intent timestamps are invalid")
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
	if failure.Retryable || !failure.Deterministic || failure.Class == runtimefailures.ClassInternalFailure || failure.Class == runtimefailures.ClassOutcomeUncertain {
		return errors.New("semantic rejection requires deterministic non-retryable item failure evidence")
	}
	return nil
}

type RunSummary struct {
	RunID           string                   `json:"run_id"`
	Intents         int                      `json:"intents"`
	Open            int                      `json:"open"`
	Blocked         int                      `json:"blocked"`
	BlockedIntents  []BlockedIntentDiagnosis `json:"blocked_intents"`
	Cardinality     int                      `json:"cardinality"`
	Cursor          int                      `json:"cursor"`
	Owed            int                      `json:"owed"`
	Committed       int                      `json:"committed"`
	Rejected        int                      `json:"rejected"`
	Canceled        int                      `json:"canceled"`
	Settled         int                      `json:"settled"`
	Unsettled       int                      `json:"unsettled"`
	BarrierArmed    int                      `json:"barrier_armed"`
	BarrierPending  int                      `json:"barrier_closed_pending"`
	BarrierTerminal int                      `json:"barrier_terminal"`
	MinNextChunk    int                      `json:"min_next_chunk"`
	MaxNextChunk    int                      `json:"max_next_chunk"`
	LastChunkMaxMS  int64                    `json:"last_chunk_max_ms"`
	OldestAgeMS     int64                    `json:"oldest_age_ms"`
}

type BlockedIntentDiagnosis struct {
	TriggeringDeliveryID string                   `json:"triggering_delivery_id"`
	PackageKey           string                   `json:"package_key"`
	ElementID            string                   `json:"element_id"`
	Cursor               int                      `json:"cursor"`
	Owed                 int                      `json:"owed"`
	Failure              runtimefailures.Envelope `json:"failure"`
}

func (d BlockedIntentDiagnosis) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(d.TriggeringDeliveryID)); err != nil {
		return errors.New("blocked fan-out diagnosis requires triggering delivery identity")
	}
	if _, err := (runtimecontracts.FanOutElementRef{PackageKey: d.PackageKey, ElementID: d.ElementID}).ContractElementRef(); err != nil {
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
	if s.Intents < 0 || s.Open < 0 || s.Blocked < 0 || s.Cardinality < 0 || s.Cursor < 0 || s.Owed < 0 || s.Committed < 0 || s.Rejected < 0 || s.Canceled < 0 || s.Settled < 0 || s.Unsettled < 0 || s.BarrierArmed < 0 || s.BarrierPending < 0 || s.BarrierTerminal < 0 || s.MinNextChunk < 0 || s.MaxNextChunk < 0 || s.LastChunkMaxMS < 0 || s.OldestAgeMS < 0 {
		return errors.New("fan-out run summary counts cannot be negative")
	}
	if s.Cursor != s.Committed+s.Rejected || s.Cardinality != s.Cursor+s.Owed+s.Canceled || s.Committed != s.Settled+s.Unsettled {
		return errors.New("fan-out run summary progress facts are contradictory")
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
