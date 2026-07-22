package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
	reader, ok := r.owner.(interface {
		deliverySnapshotsForAgent(context.Context, string, time.Time) ([]runtimedelivery.Snapshot, error)
	})
	if !ok {
		return OperatorAgentDeliveryDiagnostics{}, fmt.Errorf("operator agent delivery diagnostics requires canonical delivery snapshots")
	}
	snapshots, err := reader.deliverySnapshotsForAgent(ctx, agentID, time.Unix(0, 0).UTC())
	if err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}
	observability := NewOperatorObservabilityReadSurface(r.db, r.owner)
	return buildAgentDeliveryDiagnostics(agentID, snapshots, opts,
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
		func(eventID string) ([]OperatorDeadLetterRecord, error) {
			return observability.loadOperatorEventDeadLetters(ctx, eventID)
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
	snapshots []runtimedelivery.Snapshot,
	opts OperatorAgentDeliveryDiagnosticsOptions,
	loadEvent func(string) (deliveryLifecycleEventMetadata, error),
	loadDeadLetters func(string) ([]OperatorDeadLetterRecord, error),
) (OperatorAgentDeliveryDiagnostics, error) {
	result := OperatorAgentDeliveryDiagnostics{
		AgentID: agentID, Failures: []OperatorAgentDeliveryFailure{}, DeadLetters: []OperatorAgentDeadLetterDelivery{},
	}
	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	for _, snapshot := range snapshots {
		occurredAt := deliveryDiagnosticOccurredAt(snapshot)
		switch snapshot.Status {
		case runtimedelivery.StatusFailed:
			if !occurredAt.Before(cutoff) {
				result.Summary.Failures24h++
			}
		case runtimedelivery.StatusDeadLetter:
			if !occurredAt.Before(cutoff) {
				result.Summary.DeadLetters24h++
			}
		}
	}

	failureCursorAt, failureCursorID, err := decodeDeliveryDiagnosticsPosition(opts.FailureCursor, "agent.delivery_diagnostics.failures", "failure_cursor")
	if err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}
	deadCursorAt, deadCursorID, err := decodeDeliveryDiagnosticsPosition(opts.DeadLetterCursor, "agent.delivery_diagnostics.dead_letters", "dead_letter_cursor")
	if err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}
	for _, snapshot := range snapshots {
		if snapshot.Status != runtimedelivery.StatusFailed && snapshot.Status != runtimedelivery.StatusDeadLetter {
			continue
		}
		occurredAt := deliveryDiagnosticOccurredAt(snapshot)
		if snapshot.Status == runtimedelivery.StatusFailed && !deliveryDiagnosticsAfterCursor(occurredAt, snapshot.DeliveryID, failureCursorAt, failureCursorID) {
			continue
		}
		if snapshot.Status == runtimedelivery.StatusDeadLetter && !deliveryDiagnosticsAfterCursor(occurredAt, snapshot.DeliveryID, deadCursorAt, deadCursorID) {
			continue
		}
		metadata, err := loadEvent(snapshot.EventID)
		if err != nil {
			return OperatorAgentDeliveryDiagnostics{}, err
		}
		runID := snapshot.RunID
		if runID == "" {
			runID = metadata.RunID
		}
		if snapshot.Status == runtimedelivery.StatusFailed {
			result.Failures = append(result.Failures, OperatorAgentDeliveryFailure{
				DeliveryID: snapshot.DeliveryID, EventID: snapshot.EventID, EventName: metadata.EventName,
				RunID: runID, EntityID: metadata.EntityID, Status: string(snapshot.Status),
				ReasonCode: snapshot.ReasonCode, Failure: runtimefailures.CloneEnvelope(snapshot.Failure),
				RetryCount: snapshot.RetryCount, OccurredAt: occurredAt,
			})
			continue
		}
		records, err := loadDeadLetters(snapshot.EventID)
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
	sort.Slice(result.Failures, func(i, j int) bool {
		if !result.Failures[i].OccurredAt.Equal(result.Failures[j].OccurredAt) {
			return result.Failures[i].OccurredAt.After(result.Failures[j].OccurredAt)
		}
		return result.Failures[i].DeliveryID > result.Failures[j].DeliveryID
	})
	sort.Slice(result.DeadLetters, func(i, j int) bool {
		if !result.DeadLetters[i].OccurredAt.Equal(result.DeadLetters[j].OccurredAt) {
			return result.DeadLetters[i].OccurredAt.After(result.DeadLetters[j].OccurredAt)
		}
		return result.DeadLetters[i].DeliveryID > result.DeadLetters[j].DeliveryID
	})
	if len(result.Failures) > opts.FailureLimit {
		last := result.Failures[opts.FailureLimit-1]
		result.FailuresNextCursor = encodeAgentDeliveryDiagnosticsCursor("agent.delivery_diagnostics.failures", last.OccurredAt, last.DeliveryID)
		result.Failures = result.Failures[:opts.FailureLimit]
	}
	if len(result.DeadLetters) > opts.DeadLetterLimit {
		last := result.DeadLetters[opts.DeadLetterLimit-1]
		result.DeadLettersNextCursor = encodeAgentDeliveryDiagnosticsCursor("agent.delivery_diagnostics.dead_letters", last.OccurredAt, last.DeliveryID)
		result.DeadLetters = result.DeadLetters[:opts.DeadLetterLimit]
	}
	return result, nil
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

func deliveryDiagnosticsAfterCursor(occurredAt time.Time, deliveryID string, cursorAt time.Time, cursorID string) bool {
	if cursorAt.IsZero() {
		return true
	}
	return occurredAt.Before(cursorAt) || (occurredAt.Equal(cursorAt) && deliveryID < cursorID)
}
