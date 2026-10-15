package deliverylifecycle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

// SourceExecutionAtCut retains refusal predicates, never live execution authority.
type SourceExecutionAtCut struct {
	InProgress   bool `json:"in_progress"`
	Started      bool `json:"started"`
	SessionBound bool `json:"session_bound"`
}

func (s *SourceExecutionAtCut) UnmarshalJSON(raw []byte) error {
	var fields struct {
		InProgress   *bool `json:"in_progress"`
		Started      *bool `json:"started"`
		SessionBound *bool `json:"session_bound"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fields); err != nil {
		return err
	}
	if fields.InProgress == nil || fields.Started == nil || fields.SessionBound == nil {
		return fmt.Errorf("%w: source execution predicates must all be present", ErrConflict)
	}
	*s = SourceExecutionAtCut{*fields.InProgress, *fields.Started, *fields.SessionBound}
	return nil
}

// HistoricalFact has no live claim, lease, session identifier or physical clock.
// RetryDecision is a committed outcome even when a later claim is in progress.
type HistoricalFact struct {
	DeliveryID          string
	EventID             string
	RunID               string
	RouteIdentity       events.DeliveryRouteIdentity
	Route               events.DeliveryRoute
	SubscriberClass     SubscriberClass
	SubscriberID        string
	Status              Status
	FinalSelection      SelectionPresence
	RetryCount          int
	MaxRetries          int
	RetryDecision       *CommittedRetryDecision
	SourceExecution     SourceExecutionAtCut
	SettledClaimVersion int64
	ReasonCode          string
	Failure             *runtimefailures.Envelope
	SettledAt           time.Time
	CreatedAt           time.Time
}

func (f HistoricalFact) Terminal() bool {
	return f.Status == StatusDelivered || f.Status == StatusDeadLetter || f.Status == StatusCanceled
}

// DecodeHistoricalSnapshot validates the single canonical fixed-cut representation.
// Its result deliberately cannot be passed to a live Snapshot consumer.
func DecodeHistoricalSnapshot(raw []byte) (HistoricalFact, error) {
	var fact struct {
		DeliveryID                string                           `json:"delivery_id"`
		EventID                   string                           `json:"event_id"`
		RunID                     string                           `json:"run_id"`
		RouteIdentity             string                           `json:"route_identity"`
		SubscriberType            string                           `json:"subscriber_type"`
		SubscriberID              string                           `json:"subscriber_id"`
		AgentIdentity             agentidentity.Identity           `json:"agent_identity"`
		DeliveryTargetOwnership   events.DeliveryTargetOwnership   `json:"delivery_target_ownership"`
		DeliveryContext           events.DeliveryContext           `json:"delivery_context"`
		DeliveryPayloadProjection events.DeliveryPayloadProjection `json:"delivery_payload_projection"`
		ConnectClaim              events.ConnectExecutionClaim     `json:"connect_execution_claim"`
		ReceiverMaterialization   json.RawMessage                  `json:"receiver_materialization_plan"`
		Status                    string                           `json:"status"`
		FinalSelection            SelectionPresence                `json:"final_selection"`
		RetryCount                int                              `json:"retry_count"`
		MaxRetries                int                              `json:"max_retries"`
		RetryDecision             *CommittedRetryDecision          `json:"retry_decision"`
		SettledClaimVersion       *int64                           `json:"settled_claim_version"`
		SourceExecution           *SourceExecutionAtCut            `json:"source_execution_at_cut"`
		ReasonCode                string                           `json:"reason_code"`
		Failure                   *runtimefailures.Envelope        `json:"failure"`
		SettledAt                 *time.Time                       `json:"settled_at"`
		CreatedAt                 time.Time                        `json:"created_at"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fact); err != nil {
		return HistoricalFact{}, fmt.Errorf("decode historical delivery snapshot: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return HistoricalFact{}, fmt.Errorf("%w: historical delivery requires exactly one fact", ErrConflict)
	}
	if fact.SourceExecution == nil {
		return HistoricalFact{}, fmt.Errorf("%w: historical delivery lacks source execution predicates", ErrConflict)
	}
	if _, err := uuid.Parse(strings.TrimSpace(fact.DeliveryID)); err != nil {
		return HistoricalFact{}, fmt.Errorf("historical delivery id: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(fact.EventID)); err != nil {
		return HistoricalFact{}, fmt.Errorf("historical delivery event id: %w", err)
	}
	if fact.RunID = strings.TrimSpace(fact.RunID); fact.RunID != "" {
		if _, err := uuid.Parse(fact.RunID); err != nil {
			return HistoricalFact{}, fmt.Errorf("historical delivery run id: %w", err)
		}
	}
	identity, err := events.ParseDeliveryRouteIdentity(fact.RouteIdentity)
	if err != nil {
		return HistoricalFact{}, err
	}
	class, err := ParseSubscriberClass(fact.SubscriberType)
	if err != nil {
		return HistoricalFact{}, err
	}
	status, err := ParseStatus(fact.Status)
	if err != nil {
		return HistoricalFact{}, err
	}
	if status == StatusCanceled {
		if _, err := ParseCancellationReason(fact.ReasonCode); err != nil {
			return HistoricalFact{}, err
		}
	}
	recipient, err := deliveryRecipientForClass(class, fact.SubscriberID)
	if err != nil {
		return HistoricalFact{}, err
	}
	route := events.DeliveryRoute{
		Recipient:     recipient,
		AgentIdentity: fact.AgentIdentity, Target: fact.DeliveryTargetOwnership, Context: fact.DeliveryContext,
		PayloadProjection: fact.DeliveryPayloadProjection,
		ConnectClaim:      fact.ConnectClaim,
	}.Normalized()
	route, err = events.RestoreReceiverMaterializationRecord(route, fact.ReceiverMaterialization)
	if err != nil {
		return HistoricalFact{}, err
	}
	if !route.Initialization.Empty() {
		if err := route.Initialization.ValidatePublication(fact.RunID, fact.EventID); err != nil {
			return HistoricalFact{}, err
		}
	}
	derived, err := route.Identity()
	if err != nil || derived != identity {
		return HistoricalFact{}, fmt.Errorf("%w: historical delivery route identity mismatch", ErrConflict)
	}
	if err := validateDeliveryRouteOwningRun(fact.RunID, route); err != nil {
		return HistoricalFact{}, fmt.Errorf("%w: historical delivery ownership: %v", ErrConflict, err)
	}
	snapshot := HistoricalFact{
		DeliveryID: fact.DeliveryID, EventID: fact.EventID, RunID: fact.RunID,
		RouteIdentity: identity, Route: route, SubscriberClass: class, SubscriberID: route.Recipient.ID(),
		Status: status, RetryCount: fact.RetryCount, MaxRetries: fact.MaxRetries,
		FinalSelection: fact.FinalSelection,
		RetryDecision:  fact.RetryDecision, SourceExecution: *fact.SourceExecution,
		ReasonCode: strings.TrimSpace(fact.ReasonCode), Failure: runtimefailures.CloneEnvelope(fact.Failure),
		CreatedAt: fact.CreatedAt.UTC(),
	}
	if fact.SettledAt != nil {
		snapshot.SettledAt = fact.SettledAt.UTC()
	}
	if (fact.SettledClaimVersion != nil) != !snapshot.SettledAt.IsZero() {
		return HistoricalFact{}, fmt.Errorf("%w: historical claim generation requires exact settlement evidence", ErrConflict)
	}
	if fact.SettledClaimVersion != nil {
		snapshot.SettledClaimVersion = *fact.SettledClaimVersion
	}
	if err := validateHistoricalSnapshot(snapshot); err != nil {
		return HistoricalFact{}, err
	}
	return snapshot, nil
}

func validateHistoricalSnapshot(snapshot HistoricalFact) error {
	if snapshot.Status == StatusCanceled {
		if _, err := ParseCancellationReason(snapshot.ReasonCode); err != nil || snapshot.Failure != nil {
			return fmt.Errorf("%w: historical cancellation requires exact intent without failure", ErrConflict)
		}
	}
	if err := ValidateSelectionPresence(snapshot.Status, snapshot.FinalSelection); err != nil {
		return fmt.Errorf("%w: historical final selection: %v", ErrConflict, err)
	}
	if snapshot.SubscriberID == "" || snapshot.MaxRetries != snapshot.SubscriberClass.MaxRetries() ||
		snapshot.RetryCount < 0 || snapshot.RetryCount > snapshot.MaxRetries ||
		snapshot.CreatedAt.IsZero() || snapshot.SettledClaimVersion < 0 {
		return fmt.Errorf("%w: historical delivery snapshot violates structural policy", ErrConflict)
	}
	cut := snapshot.SourceExecution
	if cut.InProgress != (snapshot.Status == StatusInProgress) ||
		(cut.InProgress && !cut.Started) || (cut.SessionBound && !cut.InProgress) {
		return fmt.Errorf("%w: historical source execution predicates contradict lifecycle", ErrConflict)
	}
	return validateHistoricalLifecycleShape(snapshot)
}

func validateHistoricalLifecycleShape(snapshot HistoricalFact) error {
	if snapshot.RetryDecision != nil {
		if err := snapshot.RetryDecision.Validate(); err != nil {
			return err
		}
	}
	switch snapshot.Status {
	case StatusPending:
		if snapshot.RetryCount != 0 || snapshot.RetryDecision != nil || !snapshot.SettledAt.IsZero() {
			return fmt.Errorf("%w: historical pending delivery has conflicting lifecycle facts", ErrConflict)
		}
	case StatusInProgress:
		if !snapshot.SettledAt.IsZero() || (snapshot.RetryCount > 0) != (snapshot.RetryDecision != nil) {
			return fmt.Errorf("%w: historical in-progress delivery has conflicting lifecycle facts", ErrConflict)
		}
	case StatusFailed:
		if snapshot.RetryCount == 0 || snapshot.RetryDecision == nil || !snapshot.SettledAt.IsZero() {
			return fmt.Errorf("%w: historical failed delivery has conflicting lifecycle facts", ErrConflict)
		}
	case StatusDelivered, StatusDeadLetter, StatusCanceled:
		if snapshot.SettledAt.IsZero() || snapshot.RetryDecision != nil {
			return fmt.Errorf("%w: historical terminal delivery has conflicting lifecycle facts", ErrConflict)
		}
	default:
		return fmt.Errorf("%w: historical delivery status is unsupported", ErrConflict)
	}
	return nil
}
