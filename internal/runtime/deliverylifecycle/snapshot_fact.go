package deliverylifecycle

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

// DecodeHistoricalSnapshot validates the canonical persisted delivery fact
// captured by run-fork revisioning. It deliberately excludes claim tokens;
// historical readback is evidence, never a capability.
func DecodeHistoricalSnapshot(raw []byte) (Snapshot, error) {
	var fact struct {
		DeliveryID                string                           `json:"delivery_id"`
		EventID                   string                           `json:"event_id"`
		RunID                     string                           `json:"run_id"`
		RouteIdentity             string                           `json:"route_identity"`
		SubscriberType            string                           `json:"subscriber_type"`
		SubscriberID              string                           `json:"subscriber_id"`
		DeliveryTargetRoute       events.RouteIdentity             `json:"delivery_target_route"`
		DeliveryContext           events.DeliveryContext           `json:"delivery_context"`
		DeliveryPayloadProjection events.DeliveryPayloadProjection `json:"delivery_payload_projection"`
		Status                    string                           `json:"status"`
		RetryCount                int                              `json:"retry_count"`
		MaxRetries                int                              `json:"max_retries"`
		NextEligibleAt            *time.Time                       `json:"next_eligible_at"`
		ClaimVersion              int64                            `json:"claim_version"`
		ClaimExpiresAt            *time.Time                       `json:"claim_expires_at"`
		ActiveSessionID           string                           `json:"active_session_id"`
		ReasonCode                string                           `json:"reason_code"`
		Failure                   *runtimefailures.Envelope        `json:"failure"`
		StartedAt                 *time.Time                       `json:"started_at"`
		SettledAt                 *time.Time                       `json:"settled_at"`
		CreatedAt                 time.Time                        `json:"created_at"`
		UpdatedAt                 time.Time                        `json:"updated_at"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fact); err != nil {
		return Snapshot{}, fmt.Errorf("decode historical delivery snapshot: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(fact.DeliveryID)); err != nil {
		return Snapshot{}, fmt.Errorf("historical delivery id: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(fact.EventID)); err != nil {
		return Snapshot{}, fmt.Errorf("historical delivery event id: %w", err)
	}
	if fact.RunID = strings.TrimSpace(fact.RunID); fact.RunID != "" {
		if _, err := uuid.Parse(fact.RunID); err != nil {
			return Snapshot{}, fmt.Errorf("historical delivery run id: %w", err)
		}
	}
	identity, err := events.ParseDeliveryRouteIdentity(fact.RouteIdentity)
	if err != nil {
		return Snapshot{}, err
	}
	class, err := ParseSubscriberClass(fact.SubscriberType)
	if err != nil {
		return Snapshot{}, err
	}
	status, err := ParseStatus(fact.Status)
	if err != nil {
		return Snapshot{}, err
	}
	route := events.DeliveryRoute{
		SubscriberType: string(class), SubscriberID: fact.SubscriberID,
		Target: fact.DeliveryTargetRoute, Context: fact.DeliveryContext,
		PayloadProjection: fact.DeliveryPayloadProjection,
	}.Normalized()
	derived, err := route.Identity()
	if err != nil || derived != identity {
		return Snapshot{}, fmt.Errorf("%w: historical delivery route identity mismatch", ErrConflict)
	}
	snapshot := Snapshot{
		DeliveryID: fact.DeliveryID, EventID: fact.EventID, RunID: fact.RunID,
		RouteIdentity: identity, Route: route, SubscriberClass: class, SubscriberID: route.SubscriberID,
		Status: status, RetryCount: fact.RetryCount, MaxRetries: fact.MaxRetries,
		ClaimVersion: fact.ClaimVersion, ActiveSessionID: strings.TrimSpace(fact.ActiveSessionID),
		ReasonCode: strings.TrimSpace(fact.ReasonCode), Failure: runtimefailures.CloneEnvelope(fact.Failure),
		CreatedAt: fact.CreatedAt.UTC(), UpdatedAt: fact.UpdatedAt.UTC(),
	}
	if fact.NextEligibleAt != nil {
		snapshot.NextEligibleAt = fact.NextEligibleAt.UTC()
	}
	if fact.ClaimExpiresAt != nil {
		snapshot.ClaimExpiresAt = fact.ClaimExpiresAt.UTC()
	}
	if fact.StartedAt != nil {
		snapshot.StartedAt = fact.StartedAt.UTC()
	}
	if fact.SettledAt != nil {
		snapshot.SettledAt = fact.SettledAt.UTC()
	}
	if err := validateHistoricalSnapshot(snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func validateHistoricalSnapshot(snapshot Snapshot) error {
	if snapshot.SubscriberID == "" || snapshot.MaxRetries != snapshot.SubscriberClass.MaxRetries() ||
		snapshot.RetryCount < 0 || snapshot.RetryCount > snapshot.MaxRetries ||
		snapshot.CreatedAt.IsZero() || snapshot.UpdatedAt.IsZero() || snapshot.ClaimVersion < 0 {
		return fmt.Errorf("%w: historical delivery snapshot violates structural policy", ErrConflict)
	}
	switch snapshot.Status {
	case StatusPending:
		if snapshot.RetryCount != 0 || snapshot.NextEligibleAt.IsZero() || !snapshot.SettledAt.IsZero() || !snapshot.ClaimExpiresAt.IsZero() {
			return fmt.Errorf("%w: historical pending delivery has conflicting lifecycle facts", ErrConflict)
		}
	case StatusInProgress:
		if snapshot.ClaimVersion == 0 || snapshot.ClaimExpiresAt.IsZero() || snapshot.StartedAt.IsZero() || !snapshot.NextEligibleAt.IsZero() || !snapshot.SettledAt.IsZero() {
			return fmt.Errorf("%w: historical in-progress delivery has conflicting lifecycle facts", ErrConflict)
		}
	case StatusFailed:
		if snapshot.RetryCount == 0 || snapshot.NextEligibleAt.IsZero() || !snapshot.ClaimExpiresAt.IsZero() || !snapshot.SettledAt.IsZero() {
			return fmt.Errorf("%w: historical failed delivery has conflicting lifecycle facts", ErrConflict)
		}
	case StatusDelivered, StatusDeadLetter:
		if snapshot.SettledAt.IsZero() || !snapshot.NextEligibleAt.IsZero() || !snapshot.ClaimExpiresAt.IsZero() {
			return fmt.Errorf("%w: historical terminal delivery has conflicting lifecycle facts", ErrConflict)
		}
	default:
		return fmt.Errorf("%w: historical delivery status is unsupported", ErrConflict)
	}
	return nil
}
