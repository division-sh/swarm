package fanoutobligation

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

const (
	DefaultListLimit  = 50
	MaxListLimit      = 500
	ListIdentityOrder = "intent_identity_asc"
)

var ErrInvalidListCursor = errors.New("invalid fan-out list cursor")

// ListFilter selects persisted facts, not inferred runtime admission.
type ListFilter struct {
	Status               Status `json:"status,omitempty"`
	TriggeringDeliveryID string `json:"triggering_delivery_id,omitempty"`
	FlowPath             string `json:"flow_path,omitempty"`
	SemanticPath         string `json:"semantic_path,omitempty"`
}

type ListQuery struct {
	RunID  string     `json:"run_id"`
	Limit  int        `json:"limit,omitempty"`
	Cursor string     `json:"cursor,omitempty"`
	Filter ListFilter `json:"filter,omitempty"`
}

func (q ListQuery) Validate() error {
	if id, err := uuid.Parse(q.RunID); err != nil || id.String() != q.RunID {
		return errors.New("run_id must be a canonical UUID")
	}
	if q.Limit < 0 || q.Limit > MaxListLimit {
		return fmt.Errorf("limit must be between 1 and %d", MaxListLimit)
	}
	switch q.Filter.Status {
	case "", StatusOpen, StatusClosed, StatusCanceled, StatusBlocked:
	default:
		return errors.New("invalid fan-out status filter")
	}
	if raw := q.Filter.TriggeringDeliveryID; raw != "" {
		if id, err := uuid.Parse(raw); err != nil || id.String() != raw {
			return errors.New("triggering_delivery_id must be a canonical UUID")
		}
	}
	if len(q.Cursor) > 16384 {
		return ErrInvalidListCursor
	}
	for _, value := range []string{q.Filter.FlowPath, q.Filter.SemanticPath} {
		if len(value) > 4096 || value != strings.TrimSpace(value) {
			return errors.New("fan-out identity filters must be exact paths of at most 4096 bytes")
		}
	}
	if q.Filter.FlowPath != "" {
		if _, err := runtimeidentity.AdmitFlowIdentity(q.Filter.FlowPath); err != nil {
			return err
		}
	}
	if q.Filter.SemanticPath != "" {
		if _, err := runtimeidentity.AdmitDeclarationIdentity(".", "fan_out", q.Filter.SemanticPath); err != nil {
			return err
		}
	}
	return nil
}

// RuntimeReadback is occurrence-local evidence. Nil metrics are unavailable,
// never zero measurements. Durable eligibility does not grant execution.
type RuntimeReadback struct {
	ObservedAt    *time.Time `json:"observed_at"`
	Availability  string     `json:"availability"`
	Reason        string     `json:"reason"`
	Eligible      *bool      `json:"eligible"`
	Workers       *int       `json:"workers"`
	ActiveWorkers *int       `json:"active_workers"`
	LastCommitMS  *float64   `json:"last_commit_ms"`
}

func UnavailableRuntimeReadback() RuntimeReadback {
	return RuntimeReadback{Availability: "unavailable", Reason: "runtime_observation_unavailable"}
}

func (r RuntimeReadback) Validate() error {
	if strings.TrimSpace(r.Reason) == "" {
		return errors.New("fan-out runtime observation requires its owner's reason")
	}
	switch r.Availability {
	case "unavailable", "retired":
		if r.Reason == "" || r.ObservedAt != nil || r.Eligible != nil || r.Workers != nil || r.ActiveWorkers != nil || r.LastCommitMS != nil {
			return errors.New("unavailable fan-out runtime evidence must have a reason and null metrics/eligibility")
		}
	case "available":
		if r.ObservedAt == nil || r.ObservedAt.IsZero() {
			return errors.New("available fan-out runtime evidence requires its own observation time")
		}
		if r.Eligible == nil || r.Workers == nil || r.ActiveWorkers == nil || *r.Workers < 1 || *r.ActiveWorkers < 0 || *r.ActiveWorkers > *r.Workers {
			return errors.New("available fan-out runtime evidence requires exact eligibility and capacity")
		}
		if r.LastCommitMS != nil && (*r.LastCommitMS < 0 || math.IsNaN(*r.LastCommitMS) || math.IsInf(*r.LastCommitMS, 0)) {
			return errors.New("fan-out runtime latency must be finite and nonnegative or unavailable")
		}
	default:
		return errors.New("invalid fan-out runtime availability")
	}
	return nil
}

type IntentReadback struct {
	Key                IntentKey                 `json:"key"`
	BundleHash         string                    `json:"bundle_hash"`
	Status             Status                    `json:"status"`
	DurableState       string                    `json:"durable_state"`
	Cardinality        int                       `json:"cardinality"`
	Cursor             int                       `json:"cursor"`
	Owed               int                       `json:"owed"`
	NextChunkSize      int                       `json:"next_chunk_size"`
	LastServedAt       *time.Time                `json:"last_served_at"`
	Retry              *RetryWait                `json:"retry"`
	ClaimOwner         string                    `json:"claim_owner"`
	ClaimGeneration    uint64                    `json:"claim_generation"`
	LeaseExpiresAt     *time.Time                `json:"lease_expires_at"`
	Failure            *runtimefailures.Envelope `json:"failure"`
	CancellationReason string                    `json:"cancellation_reason"`
	CreatedAt          time.Time                 `json:"created_at"`
	UpdatedAt          time.Time                 `json:"updated_at"`
	Runtime            RuntimeReadback           `json:"runtime"`
}

type ListPage struct {
	RunID      string           `json:"run_id"`
	RunStatus  string           `json:"run_status"`
	ObservedAt time.Time        `json:"observed_at"`
	Order      string           `json:"order"`
	Intents    []IntentReadback `json:"intents"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

func (p ListPage) Validate(q ListQuery) error {
	if err := q.Validate(); err != nil {
		return err
	}
	if p.RunID != q.RunID || p.ObservedAt.IsZero() || p.Order != ListIdentityOrder || p.Intents == nil {
		return errors.New("invalid fan-out page identity, time, order or rows")
	}
	switch p.RunStatus {
	case "running", "paused", "completed", "failed", "cancelled", "forked":
	default:
		return errors.New("invalid fan-out page run status")
	}
	limit := q.Limit
	if limit == 0 {
		limit = DefaultListLimit
	}
	if len(p.Intents) > limit || (p.NextCursor != "" && (len(p.Intents) != limit || p.NextCursor == q.Cursor)) {
		return errors.New("invalid fan-out page bound or continuation")
	}
	for index, row := range p.Intents {
		if row.Key.RunID != p.RunID || row.Key.Validate() != nil || row.BundleHash == "" || row.Cardinality < 0 || row.Cursor < 0 || row.Cursor > row.Cardinality || row.Owed < 0 || row.Owed > row.Cardinality-row.Cursor || row.NextChunkSize < MinChunkSize || row.NextChunkSize > MaxChunkSize || row.CreatedAt.IsZero() || row.UpdatedAt.Before(row.CreatedAt) {
			return fmt.Errorf("invalid fan-out row %d", index)
		}
		if err := row.Runtime.Validate(); err != nil {
			return err
		}
		if q.Filter.Status != "" && row.Status != q.Filter.Status || q.Filter.TriggeringDeliveryID != "" && row.Key.TriggeringDeliveryID != q.Filter.TriggeringDeliveryID || q.Filter.FlowPath != "" && row.Key.ElementRef.FlowPath != q.Filter.FlowPath || q.Filter.SemanticPath != "" && row.Key.ElementRef.SemanticPath != q.Filter.SemanticPath {
			return errors.New("fan-out row contradicts requested filters")
		}
		if index > 0 && compareReadbackKeys(p.Intents[index-1].Key, row.Key) >= 0 {
			return errors.New("fan-out page keys must be unique and ascending")
		}
		if row.Retry != nil {
			if err := row.Retry.Validate(); err != nil {
				return err
			}
		}
		if row.Failure != nil {
			if err := runtimefailures.ValidateEnvelope(*row.Failure); err != nil {
				return err
			}
		}
		switch row.DurableState {
		case "eligible", "leased", "retry_wait":
			if row.Status != StatusOpen || row.Owed != row.Cardinality-row.Cursor {
				return errors.New("fan-out open progress contradicts state")
			}
		case "blocked":
			if row.Status != StatusBlocked || row.Failure == nil || row.Owed != row.Cardinality-row.Cursor {
				return errors.New("fan-out blockage contradicts progress or failure")
			}
		case "closed":
			if row.Status != StatusClosed || row.Owed != 0 || row.Cursor != row.Cardinality {
				return errors.New("fan-out closed progress contradicts state")
			}
		case "canceled":
			if row.Status != StatusCanceled || row.Owed != 0 || row.CancellationReason == "" {
				return errors.New("fan-out canceled progress contradicts state")
			}
		default:
			return errors.New("invalid fan-out durable state")
		}
	}
	return nil
}

func compareReadbackKeys(a, b IntentKey) int {
	for _, pair := range [][2]string{{a.TriggeringDeliveryID, b.TriggeringDeliveryID}, {a.ElementRef.FlowPath, b.ElementRef.FlowPath}, {a.ElementRef.Family, b.ElementRef.Family}, {a.ElementRef.SemanticPath, b.ElementRef.SemanticPath}} {
		if order := strings.Compare(pair[0], pair[1]); order != 0 {
			return order
		}
	}
	return 0
}

func (i Intent) ReadbackAt(now time.Time) (IntentReadback, error) {
	state, err := i.ServingAt(now)
	if err != nil {
		return IntentReadback{}, err
	}
	row := IntentReadback{
		Key: i.Request.Key, BundleHash: i.Request.PlanRef.BundleHash, Status: i.Status,
		Cardinality: i.Request.Cardinality, Cursor: i.Cursor, NextChunkSize: i.NextChunkSize,
		Retry: i.Retry, ClaimOwner: i.ClaimOwner, ClaimGeneration: i.ClaimGeneration,
		CreatedAt: i.CreatedAt, UpdatedAt: i.UpdatedAt, Runtime: UnavailableRuntimeReadback(),
	}
	switch state {
	case ServingEligible:
		row.DurableState = "eligible"
	case ServingLeased:
		row.DurableState = "leased"
	case ServingRetryWait:
		row.DurableState = "retry_wait"
	case ServingBlocked:
		row.DurableState = "blocked"
	case ServingClosed:
		row.DurableState = "closed"
	case ServingCanceled:
		row.DurableState = "canceled"
	default:
		return IntentReadback{}, errors.New("unknown fan-out serving state")
	}
	if i.Status == StatusOpen || i.Status == StatusBlocked {
		row.Owed = i.Request.Cardinality - i.Cursor
	}
	if !i.LastServedAt.IsZero() {
		at := i.LastServedAt
		row.LastServedAt = &at
	}
	if !i.LeaseExpiresAt.IsZero() {
		at := i.LeaseExpiresAt
		row.LeaseExpiresAt = &at
	}
	if i.Status == StatusBlocked {
		failure, err := runtimefailures.UnmarshalEnvelope([]byte(i.BlockedReason))
		if err != nil {
			return IntentReadback{}, err
		}
		row.Failure = &failure
	}
	if i.Status == StatusCanceled {
		row.CancellationReason = i.BlockedReason
	}
	return row, nil
}
