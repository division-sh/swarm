package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

const (
	DefaultAgentDeliveryDiagnosticsLimit = 50
	MaxAgentDeliveryDiagnosticsLimit     = 200
)

var ErrInvalidAgentDeliveryDiagnosticsCursor = errors.New("invalid agent delivery diagnostics cursor")

type AgentDeliveryDiagnosticsCursorError struct {
	Field string
}

func (e AgentDeliveryDiagnosticsCursorError) Error() string {
	field := strings.TrimSpace(e.Field)
	if field == "" {
		field = "cursor"
	}
	return fmt.Sprintf("invalid agent delivery diagnostics %s", field)
}

func (e AgentDeliveryDiagnosticsCursorError) Unwrap() error {
	return ErrInvalidAgentDeliveryDiagnosticsCursor
}

type OperatorAgentDeliveryDiagnosticsOptions struct {
	FailureLimit     int
	FailureCursor    string
	DeadLetterLimit  int
	DeadLetterCursor string
}

type OperatorAgentDeliveryDiagnostics struct {
	AgentID               string                                  `json:"agent_id"`
	Summary               OperatorAgentDeliveryDiagnosticsSummary `json:"summary"`
	Failures              []OperatorAgentDeliveryFailure          `json:"failures"`
	FailuresNextCursor    string                                  `json:"failures_next_cursor,omitempty"`
	DeadLetters           []OperatorAgentDeadLetterDelivery       `json:"dead_letters"`
	DeadLettersNextCursor string                                  `json:"dead_letters_next_cursor,omitempty"`
}

type OperatorAgentDeliveryDiagnosticsSummary struct {
	Failures24h    int `json:"failures_24h"`
	DeadLetters24h int `json:"dead_letters_24h"`
}

type OperatorAgentDeliveryFailure struct {
	DeliveryID string                    `json:"delivery_id"`
	EventID    string                    `json:"event_id"`
	EventName  string                    `json:"event_name"`
	RunID      string                    `json:"run_id,omitempty"`
	EntityID   string                    `json:"entity_id,omitempty"`
	Status     string                    `json:"status"`
	ReasonCode string                    `json:"reason_code,omitempty"`
	Failure    *runtimefailures.Envelope `json:"failure,omitempty"`
	RetryCount int                       `json:"retry_count"`
	OccurredAt time.Time                 `json:"occurred_at"`
}

type OperatorAgentDeadLetterDelivery struct {
	DeliveryID        string                     `json:"delivery_id"`
	EventID           string                     `json:"event_id"`
	EventName         string                     `json:"event_name"`
	RunID             string                     `json:"run_id,omitempty"`
	EntityID          string                     `json:"entity_id,omitempty"`
	Status            string                     `json:"status"`
	ReasonCode        string                     `json:"reason_code,omitempty"`
	Failure           *runtimefailures.Envelope  `json:"failure,omitempty"`
	RetryCount        int                        `json:"retry_count"`
	OccurredAt        time.Time                  `json:"occurred_at"`
	DeadLetterRecords []OperatorDeadLetterRecord `json:"dead_letter_records"`
}

type agentDeliveryDiagnosticsCursor struct {
	Kind       string `json:"kind"`
	OccurredAt string `json:"occurred_at"`
	DeliveryID string `json:"delivery_id"`
}

type agentDeliveryDiagnosticSnapshotReader interface {
	deliveryDiagnosticSnapshotPageForAgent(context.Context, runtimedelivery.AgentDiagnosticPageQuery) (runtimedelivery.SnapshotPage, error)
	deliveryDiagnosticCountsForAgentSince(context.Context, string, time.Time) (runtimedelivery.AgentDiagnosticCounts, error)
}

func (s *PostgresStore) LoadOperatorAgentDeliveryDiagnostics(ctx context.Context, agentID string, opts OperatorAgentDeliveryDiagnosticsOptions) (OperatorAgentDeliveryDiagnostics, error) {
	return NewOperatorAgentConversationReadSurface(s.DB, s, 0).LoadOperatorAgentDeliveryDiagnostics(ctx, agentID, opts)
}

func (r *OperatorAgentConversationReadSurface) LoadOperatorAgentDeliveryDiagnostics(ctx context.Context, agentID string, opts OperatorAgentDeliveryDiagnosticsOptions) (OperatorAgentDeliveryDiagnostics, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return OperatorAgentDeliveryDiagnostics{}, ErrAgentNotFound
	}
	if err := r.requireAgentDeliveryDiagnosticsAccess(); err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}
	if err := r.ensureAgentDeliveryDiagnosticsAgentExists(ctx, agentID); err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}

	opts = defaultOperatorAgentDeliveryDiagnosticsOptions(opts)
	reader, ok := r.owner.(agentDeliveryDiagnosticSnapshotReader)
	if !ok {
		return OperatorAgentDeliveryDiagnostics{}, fmt.Errorf("operator agent delivery diagnostics requires canonical bounded delivery snapshots")
	}
	counts, failures, deadLetters, err := loadAgentDeliveryDiagnosticSnapshotPages(ctx, reader, agentID, opts)
	if err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}
	observability := NewOperatorObservabilityReadSurface(r.db, r.owner)
	return buildAgentDeliveryDiagnostics(agentID, counts, failures, deadLetters,
		func(eventID string) (deliveryLifecycleEventMetadata, error) {
			record, found, err := loadPostgresEventIdentity(ctx, r.db, eventID)
			if err != nil {
				return deliveryLifecycleEventMetadata{}, err
			}
			if !found {
				return deliveryLifecycleEventMetadata{}, fmt.Errorf("delivery event %s not found", eventID)
			}
			admitted, err := decodeEventRecord(record)
			if err != nil {
				return deliveryLifecycleEventMetadata{}, err
			}
			event := admitted.Event()
			return deliveryLifecycleEventMetadata{EventName: string(event.Type()), RunID: event.RunID(), EntityID: event.EntityID()}, nil
		},
		func(deliveryID string, claimVersion int64) ([]OperatorDeadLetterRecord, error) {
			return observability.loadOperatorDeliveryDeadLetters(ctx, deliveryID, claimVersion)
		})
}

func defaultOperatorAgentDeliveryDiagnosticsOptions(opts OperatorAgentDeliveryDiagnosticsOptions) OperatorAgentDeliveryDiagnosticsOptions {
	if opts.FailureLimit <= 0 {
		opts.FailureLimit = DefaultAgentDeliveryDiagnosticsLimit
	}
	if opts.FailureLimit > MaxAgentDeliveryDiagnosticsLimit {
		opts.FailureLimit = MaxAgentDeliveryDiagnosticsLimit
	}
	if opts.DeadLetterLimit <= 0 {
		opts.DeadLetterLimit = DefaultAgentDeliveryDiagnosticsLimit
	}
	if opts.DeadLetterLimit > MaxAgentDeliveryDiagnosticsLimit {
		opts.DeadLetterLimit = MaxAgentDeliveryDiagnosticsLimit
	}
	opts.FailureCursor = strings.TrimSpace(opts.FailureCursor)
	opts.DeadLetterCursor = strings.TrimSpace(opts.DeadLetterCursor)
	return opts
}

func (r *OperatorAgentConversationReadSurface) requireAgentDeliveryDiagnosticsAccess() error {
	if r == nil || r.db == nil {
		return fmt.Errorf("operator agent delivery diagnostics read owner requires postgres store")
	}
	return r.owner.requireCurrentSchema()
}

func (r *OperatorAgentConversationReadSurface) ensureAgentDeliveryDiagnosticsAgentExists(ctx context.Context, agentID string) error {
	var exists bool
	if err := r.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM agents
			WHERE agent_id = $1
			  AND status NOT IN ('terminated', 'ephemeral')
		)
	`, agentID).Scan(&exists); err != nil {
		return fmt.Errorf("load agent delivery diagnostics agent: %w", err)
	}
	if !exists {
		return ErrAgentNotFound
	}
	return nil
}

func encodeAgentDeliveryDiagnosticsCursor(kind string, occurredAt time.Time, deliveryID string) string {
	raw, _ := json.Marshal(agentDeliveryDiagnosticsCursor{
		Kind:       strings.TrimSpace(kind),
		OccurredAt: occurredAt.UTC().Format(time.RFC3339Nano),
		DeliveryID: strings.TrimSpace(deliveryID),
	})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeAgentDeliveryDiagnosticsCursor(raw, kind, field string) (agentDeliveryDiagnosticsCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return agentDeliveryDiagnosticsCursor{}, AgentDeliveryDiagnosticsCursorError{Field: field}
	}
	var cursor agentDeliveryDiagnosticsCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return agentDeliveryDiagnosticsCursor{}, AgentDeliveryDiagnosticsCursorError{Field: field}
	}
	if strings.TrimSpace(cursor.Kind) != strings.TrimSpace(kind) {
		return agentDeliveryDiagnosticsCursor{}, AgentDeliveryDiagnosticsCursorError{Field: field}
	}
	return cursor, nil
}

func buildAgentDeliveryDiagnostics(
	agentID string,
	counts runtimedelivery.AgentDiagnosticCounts,
	failures runtimedelivery.SnapshotPage,
	deadLetters runtimedelivery.SnapshotPage,
	loadEvent func(string) (deliveryLifecycleEventMetadata, error),
	loadDeliveryDeadLetters func(string, int64) ([]OperatorDeadLetterRecord, error),
) (OperatorAgentDeliveryDiagnostics, error) {
	result := OperatorAgentDeliveryDiagnostics{
		AgentID:  agentID,
		Summary:  OperatorAgentDeliveryDiagnosticsSummary{Failures24h: counts.Failures, DeadLetters24h: counts.DeadLetters},
		Failures: []OperatorAgentDeliveryFailure{}, DeadLetters: []OperatorAgentDeadLetterDelivery{},
	}
	for _, snapshot := range failures.Snapshots {
		if snapshot.Status != runtimedelivery.StatusFailed {
			return OperatorAgentDeliveryDiagnostics{}, fmt.Errorf("canonical failure page returned delivery status %q", snapshot.Status)
		}
		occurredAt := deliveryDiagnosticOccurredAt(snapshot)
		metadata, err := loadEvent(snapshot.EventID)
		if err != nil {
			return OperatorAgentDeliveryDiagnostics{}, err
		}
		runID := snapshot.RunID
		if runID == "" {
			runID = metadata.RunID
		}
		result.Failures = append(result.Failures, OperatorAgentDeliveryFailure{
			DeliveryID: snapshot.DeliveryID, EventID: snapshot.EventID, EventName: metadata.EventName,
			RunID: runID, EntityID: metadata.EntityID, Status: string(snapshot.Status),
			ReasonCode: snapshot.ReasonCode, Failure: runtimefailures.CloneEnvelope(snapshot.Failure),
			RetryCount: snapshot.RetryCount, OccurredAt: occurredAt,
		})
	}
	for _, snapshot := range deadLetters.Snapshots {
		if snapshot.Status != runtimedelivery.StatusDeadLetter {
			return OperatorAgentDeliveryDiagnostics{}, fmt.Errorf("canonical dead-letter page returned delivery status %q", snapshot.Status)
		}
		occurredAt := deliveryDiagnosticOccurredAt(snapshot)
		metadata, err := loadEvent(snapshot.EventID)
		if err != nil {
			return OperatorAgentDeliveryDiagnostics{}, err
		}
		runID := snapshot.RunID
		if runID == "" {
			runID = metadata.RunID
		}
		records, err := loadDeliveryDeadLetters(snapshot.DeliveryID, snapshot.ClaimVersion)
		if err != nil {
			return OperatorAgentDeliveryDiagnostics{}, err
		}
		result.DeadLetters = append(result.DeadLetters, OperatorAgentDeadLetterDelivery{
			DeliveryID: snapshot.DeliveryID, EventID: snapshot.EventID, EventName: metadata.EventName,
			RunID: runID, EntityID: metadata.EntityID, Status: string(snapshot.Status),
			ReasonCode: snapshot.ReasonCode, Failure: runtimefailures.CloneEnvelope(snapshot.Failure),
			RetryCount: snapshot.RetryCount, OccurredAt: occurredAt, DeadLetterRecords: records,
		})
	}
	if failures.HasMore && len(result.Failures) > 0 {
		last := result.Failures[len(result.Failures)-1]
		result.FailuresNextCursor = encodeAgentDeliveryDiagnosticsCursor("agent.delivery_diagnostics.failures", last.OccurredAt, last.DeliveryID)
	}
	if deadLetters.HasMore && len(result.DeadLetters) > 0 {
		last := result.DeadLetters[len(result.DeadLetters)-1]
		result.DeadLettersNextCursor = encodeAgentDeliveryDiagnosticsCursor("agent.delivery_diagnostics.dead_letters", last.OccurredAt, last.DeliveryID)
	}
	return result, nil
}

func loadAgentDeliveryDiagnosticSnapshotPages(
	ctx context.Context,
	reader agentDeliveryDiagnosticSnapshotReader,
	agentID string,
	opts OperatorAgentDeliveryDiagnosticsOptions,
) (runtimedelivery.AgentDiagnosticCounts, runtimedelivery.SnapshotPage, runtimedelivery.SnapshotPage, error) {
	failureCursorAt, failureCursorID, err := decodeDeliveryDiagnosticsPosition(opts.FailureCursor, "agent.delivery_diagnostics.failures", "failure_cursor")
	if err != nil {
		return runtimedelivery.AgentDiagnosticCounts{}, runtimedelivery.SnapshotPage{}, runtimedelivery.SnapshotPage{}, err
	}
	deadCursorAt, deadCursorID, err := decodeDeliveryDiagnosticsPosition(opts.DeadLetterCursor, "agent.delivery_diagnostics.dead_letters", "dead_letter_cursor")
	if err != nil {
		return runtimedelivery.AgentDiagnosticCounts{}, runtimedelivery.SnapshotPage{}, runtimedelivery.SnapshotPage{}, err
	}
	counts, err := reader.deliveryDiagnosticCountsForAgentSince(ctx, agentID, time.Now().UTC().Add(-24*time.Hour))
	if err != nil {
		return runtimedelivery.AgentDiagnosticCounts{}, runtimedelivery.SnapshotPage{}, runtimedelivery.SnapshotPage{}, err
	}
	failures, err := reader.deliveryDiagnosticSnapshotPageForAgent(ctx, runtimedelivery.AgentDiagnosticPageQuery{
		AgentID: agentID, Status: runtimedelivery.StatusFailed,
		BeforeOccurredAt: failureCursorAt, BeforeDeliveryID: failureCursorID, Limit: opts.FailureLimit,
	})
	if err != nil {
		return runtimedelivery.AgentDiagnosticCounts{}, runtimedelivery.SnapshotPage{}, runtimedelivery.SnapshotPage{}, err
	}
	deadLetters, err := reader.deliveryDiagnosticSnapshotPageForAgent(ctx, runtimedelivery.AgentDiagnosticPageQuery{
		AgentID: agentID, Status: runtimedelivery.StatusDeadLetter,
		BeforeOccurredAt: deadCursorAt, BeforeDeliveryID: deadCursorID, Limit: opts.DeadLetterLimit,
	})
	if err != nil {
		return runtimedelivery.AgentDiagnosticCounts{}, runtimedelivery.SnapshotPage{}, runtimedelivery.SnapshotPage{}, err
	}
	return counts, failures, deadLetters, nil
}

func deliveryDiagnosticOccurredAt(snapshot runtimedelivery.Snapshot) time.Time {
	if !snapshot.SettledAt.IsZero() {
		return snapshot.SettledAt
	}
	if !snapshot.UpdatedAt.IsZero() {
		return snapshot.UpdatedAt
	}
	return snapshot.CreatedAt
}

func decodeDeliveryDiagnosticsPosition(raw, kind, field string) (time.Time, string, error) {
	if strings.TrimSpace(raw) == "" {
		return time.Time{}, "", nil
	}
	cursor, err := decodeAgentDeliveryDiagnosticsCursor(raw, kind, field)
	if err != nil {
		return time.Time{}, "", err
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, cursor.OccurredAt)
	if err != nil || strings.TrimSpace(cursor.DeliveryID) == "" {
		return time.Time{}, "", AgentDeliveryDiagnosticsCursorError{Field: field}
	}
	return occurredAt.UTC(), strings.TrimSpace(cursor.DeliveryID), nil
}
