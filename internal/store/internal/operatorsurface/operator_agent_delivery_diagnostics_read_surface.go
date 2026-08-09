package operatorsurface

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type agentDeliveryDiagnosticsCursor struct {
	Kind       string `json:"kind"`
	OccurredAt string `json:"occurred_at"`
	DeliveryID string `json:"delivery_id"`
}

func (s *AgentPostgres) LoadOperatorAgentDeliveryDiagnostics(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDeliveryDiagnosticsOptions) (operatorread.OperatorAgentDeliveryDiagnostics, error) {
	return NewOperatorAgentReadSurface(s.backend, s, 0).LoadOperatorAgentDeliveryDiagnostics(ctx, identity, opts)
}

func (r *OperatorAgentReadSurface) LoadOperatorAgentDeliveryDiagnostics(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDeliveryDiagnosticsOptions) (operatorread.OperatorAgentDeliveryDiagnostics, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return operatorread.OperatorAgentDeliveryDiagnostics{}, operatorread.ErrAgentNotFound
	}
	if err := r.requireAgentDeliveryDiagnosticsAccess(); err != nil {
		return operatorread.OperatorAgentDeliveryDiagnostics{}, err
	}
	if err := r.ensureAgentDeliveryDiagnosticsAgentExists(ctx, identity); err != nil {
		return operatorread.OperatorAgentDeliveryDiagnostics{}, err
	}

	opts = defaultOperatorAgentDeliveryDiagnosticsOptions(opts)
	counts, failures, deadLetters, err := loadAgentDeliveryDiagnosticSnapshotPages(ctx, r.source, identity, opts)
	if err != nil {
		return operatorread.OperatorAgentDeliveryDiagnostics{}, err
	}
	return buildAgentDeliveryDiagnostics(identity.AgentID(), counts, failures, deadLetters,
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
		func(deliveryID string, claimVersion int64) ([]operatorread.OperatorDeadLetterRecord, error) {
			return r.source.LoadOperatorDeliveryDeadLetters(ctx, deliveryID, claimVersion)
		})
}

func defaultOperatorAgentDeliveryDiagnosticsOptions(opts operatorread.OperatorAgentDeliveryDiagnosticsOptions) operatorread.OperatorAgentDeliveryDiagnosticsOptions {
	if opts.FailureLimit <= 0 {
		opts.FailureLimit = operatorread.DefaultAgentDeliveryDiagnosticsLimit
	}
	if opts.FailureLimit > operatorread.MaxAgentDeliveryDiagnosticsLimit {
		opts.FailureLimit = operatorread.MaxAgentDeliveryDiagnosticsLimit
	}
	if opts.DeadLetterLimit <= 0 {
		opts.DeadLetterLimit = operatorread.DefaultAgentDeliveryDiagnosticsLimit
	}
	if opts.DeadLetterLimit > operatorread.MaxAgentDeliveryDiagnosticsLimit {
		opts.DeadLetterLimit = operatorread.MaxAgentDeliveryDiagnosticsLimit
	}
	opts.FailureCursor = strings.TrimSpace(opts.FailureCursor)
	opts.DeadLetterCursor = strings.TrimSpace(opts.DeadLetterCursor)
	return opts
}

func (r *OperatorAgentReadSurface) requireAgentDeliveryDiagnosticsAccess() error {
	if r == nil || r.db == nil {
		return fmt.Errorf("operator agent delivery diagnostics read owner requires postgres store")
	}
	return r.source.RequireCurrentSchema()
}

func (r *OperatorAgentReadSurface) ensureAgentDeliveryDiagnosticsAgentExists(ctx context.Context, identity agentidentity.Identity) error {
	fields, err := agentIdentityFields(identity)
	if err != nil {
		return operatorread.ErrAgentNotFound
	}
	var exists bool
	if err := r.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM agents
			WHERE agent_id = $1
			  AND agent_name_owner = $2
			  AND agent_name_source = $3
			  AND agent_route_presence = $4
			  AND flow_scope_key = $5
			  AND flow_instance_id = $6
			  AND flow_instance = $7
			  AND status NOT IN ('terminated', 'ephemeral')
		)
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&exists); err != nil {
		return fmt.Errorf("load agent delivery diagnostics agent: %w", err)
	}
	if !exists {
		return operatorread.ErrAgentNotFound
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
		return agentDeliveryDiagnosticsCursor{}, operatorread.AgentDeliveryDiagnosticsCursorError{Field: field}
	}
	var cursor agentDeliveryDiagnosticsCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return agentDeliveryDiagnosticsCursor{}, operatorread.AgentDeliveryDiagnosticsCursorError{Field: field}
	}
	if strings.TrimSpace(cursor.Kind) != strings.TrimSpace(kind) {
		return agentDeliveryDiagnosticsCursor{}, operatorread.AgentDeliveryDiagnosticsCursorError{Field: field}
	}
	return cursor, nil
}

func buildAgentDeliveryDiagnostics(
	agentID string,
	counts runtimedelivery.AgentDiagnosticCounts,
	failures runtimedelivery.SnapshotPage,
	deadLetters runtimedelivery.SnapshotPage,
	loadEvent func(string) (deliveryLifecycleEventMetadata, error),
	loadDeliveryDeadLetters func(string, int64) ([]operatorread.OperatorDeadLetterRecord, error),
) (operatorread.OperatorAgentDeliveryDiagnostics, error) {
	result := operatorread.OperatorAgentDeliveryDiagnostics{
		AgentID:  agentID,
		Summary:  operatorread.OperatorAgentDeliveryDiagnosticsSummary{Failures24h: counts.Failures, DeadLetters24h: counts.DeadLetters},
		Failures: []operatorread.OperatorAgentDeliveryFailure{}, DeadLetters: []operatorread.OperatorAgentDeadLetterDelivery{},
	}
	for _, snapshot := range failures.Snapshots {
		if snapshot.Status != runtimedelivery.StatusFailed {
			return operatorread.OperatorAgentDeliveryDiagnostics{}, fmt.Errorf("canonical failure page returned delivery status %q", snapshot.Status)
		}
		occurredAt := deliveryDiagnosticOccurredAt(snapshot)
		metadata, err := loadEvent(snapshot.EventID)
		if err != nil {
			return operatorread.OperatorAgentDeliveryDiagnostics{}, err
		}
		runID := snapshot.RunID
		if runID == "" {
			runID = metadata.RunID
		}
		result.Failures = append(result.Failures, operatorread.OperatorAgentDeliveryFailure{
			DeliveryID: snapshot.DeliveryID, EventID: snapshot.EventID, EventName: metadata.EventName,
			RunID: runID, EntityID: metadata.EntityID, Status: string(snapshot.Status),
			ReasonCode: snapshot.ReasonCode, Failure: runtimefailures.CloneEnvelope(snapshot.Failure),
			RetryCount: snapshot.RetryCount, OccurredAt: occurredAt,
		})
	}
	for _, snapshot := range deadLetters.Snapshots {
		if snapshot.Status != runtimedelivery.StatusDeadLetter {
			return operatorread.OperatorAgentDeliveryDiagnostics{}, fmt.Errorf("canonical dead-letter page returned delivery status %q", snapshot.Status)
		}
		occurredAt := deliveryDiagnosticOccurredAt(snapshot)
		metadata, err := loadEvent(snapshot.EventID)
		if err != nil {
			return operatorread.OperatorAgentDeliveryDiagnostics{}, err
		}
		runID := snapshot.RunID
		if runID == "" {
			runID = metadata.RunID
		}
		records, err := loadDeliveryDeadLetters(snapshot.DeliveryID, snapshot.ClaimVersion)
		if err != nil {
			return operatorread.OperatorAgentDeliveryDiagnostics{}, err
		}
		result.DeadLetters = append(result.DeadLetters, operatorread.OperatorAgentDeadLetterDelivery{
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
	reader OperatorAgentReadSource,
	identity agentidentity.Identity,
	opts operatorread.OperatorAgentDeliveryDiagnosticsOptions,
) (runtimedelivery.AgentDiagnosticCounts, runtimedelivery.SnapshotPage, runtimedelivery.SnapshotPage, error) {
	failureCursorAt, failureCursorID, err := decodeDeliveryDiagnosticsPosition(opts.FailureCursor, "agent.delivery_diagnostics.failures", "failure_cursor")
	if err != nil {
		return runtimedelivery.AgentDiagnosticCounts{}, runtimedelivery.SnapshotPage{}, runtimedelivery.SnapshotPage{}, err
	}
	deadCursorAt, deadCursorID, err := decodeDeliveryDiagnosticsPosition(opts.DeadLetterCursor, "agent.delivery_diagnostics.dead_letters", "dead_letter_cursor")
	if err != nil {
		return runtimedelivery.AgentDiagnosticCounts{}, runtimedelivery.SnapshotPage{}, runtimedelivery.SnapshotPage{}, err
	}
	counts, err := reader.DeliveryDiagnosticCountsForAgentSince(ctx, identity, time.Now().UTC().Add(-24*time.Hour))
	if err != nil {
		return runtimedelivery.AgentDiagnosticCounts{}, runtimedelivery.SnapshotPage{}, runtimedelivery.SnapshotPage{}, err
	}
	failures, err := reader.DeliveryDiagnosticSnapshotPageForAgent(ctx, runtimedelivery.AgentDiagnosticPageQuery{
		AgentIdentity: identity, Status: runtimedelivery.StatusFailed,
		BeforeOccurredAt: failureCursorAt, BeforeDeliveryID: failureCursorID, Limit: opts.FailureLimit,
	})
	if err != nil {
		return runtimedelivery.AgentDiagnosticCounts{}, runtimedelivery.SnapshotPage{}, runtimedelivery.SnapshotPage{}, err
	}
	deadLetters, err := reader.DeliveryDiagnosticSnapshotPageForAgent(ctx, runtimedelivery.AgentDiagnosticPageQuery{
		AgentIdentity: identity, Status: runtimedelivery.StatusDeadLetter,
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
		return time.Time{}, "", operatorread.AgentDeliveryDiagnosticsCursorError{Field: field}
	}
	return occurredAt.UTC(), strings.TrimSpace(cursor.DeliveryID), nil
}
