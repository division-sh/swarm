package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	RunForkDeliveryEventReplayOwner = "store.run_fork.delivery_event_replay"
	runForkDeliveryEventReplayTable = "run_fork_delivery_event_replays"
	runForkDeliveryReplayReasonCode = "fork_replay"
)

type RunForkDeliveryEventReplayResult struct {
	Owner                 string `json:"owner"`
	SourceRunID           string `json:"source_run_id"`
	ForkRunID             string `json:"fork_run_id"`
	ReplayedEventCount    int    `json:"replayed_event_count"`
	ReplayedDeliveryCount int    `json:"replayed_delivery_count"`
}

type runForkReplaySourceEvent struct {
	EventID        string
	EventName      string
	EntityID       sql.NullString
	FlowInstance   sql.NullString
	Scope          string
	Payload        json.RawMessage
	ProducedBy     sql.NullString
	ProducedByType sql.NullString
	HandlerNode    sql.NullString
}

func applyRunForkDeliveryEventReplay(ctx context.Context, tx *sql.Tx, lineage runForkActivationLineage, execution RunForkHistoricalReplayExecution, now time.Time) (RunForkDeliveryEventReplayResult, error) {
	result := RunForkDeliveryEventReplayResult{
		Owner:       RunForkDeliveryEventReplayOwner,
		SourceRunID: lineage.SourceRunID,
		ForkRunID:   lineage.ForkRunID,
	}
	if strings.TrimSpace(execution.Owner) != RunForkHistoricalReplayExecutionOwner ||
		strings.TrimSpace(execution.AdmissionOwner) != RunForkHistoricalReplayExecutionAdmissionOwner ||
		!execution.DeliveryEventReplayReady ||
		execution.EventDeliveriesAdmission.Fact != RunForkHistoricalReplayFactEventDeliveries ||
		execution.EventDeliveriesAdmission.Admission != RunForkHistoricalReplayAdmissionExecutableForkWork {
		return result, fmt.Errorf("store.run_fork.delivery_event_replay requires %s owner-authorized executable event_deliveries", RunForkHistoricalReplayExecutionOwner)
	}
	replayable := execution.DeliveryEventReplayWork
	if len(replayable) == 0 {
		return result, nil
	}

	sourceEvents := map[string]runForkReplaySourceEvent{}
	insertedEvents := map[string]string{}
	for _, item := range replayable {
		sourceEventID := strings.TrimSpace(item.SourceEventID)
		sourceDeliveryID := strings.TrimSpace(item.SourceDeliveryID)
		if item.Fact != RunForkHistoricalReplayFactEventDeliveries || sourceEventID == "" || sourceDeliveryID == "" {
			return result, fmt.Errorf("store.run_fork.delivery_event_replay requires owner-authorized source event and delivery identity")
		}
		sourceEvent, ok := sourceEvents[sourceEventID]
		if !ok {
			loaded, err := loadRunForkReplaySourceEvent(ctx, tx, lineage.SourceRunID, sourceEventID)
			if err != nil {
				return result, err
			}
			sourceEvent = loaded
			sourceEvents[sourceEventID] = sourceEvent
		}
		forkEventID, ok := insertedEvents[sourceEventID]
		if !ok {
			forkEventID = deterministicRunForkReplayEventID(lineage.ForkRunID, sourceEventID)
			inserted, err := insertRunForkReplayEvent(ctx, tx, lineage.ForkRunID, forkEventID, sourceEvent, now)
			if err != nil {
				return result, err
			}
			if err := insertRunForkReplayScopeMarker(ctx, tx, lineage.ForkRunID, forkEventID, now); err != nil {
				return result, err
			}
			if inserted {
				result.ReplayedEventCount++
			}
			insertedEvents[sourceEventID] = forkEventID
		}
		forkDeliveryID := deterministicRunForkReplayDeliveryID(lineage.ForkRunID, sourceDeliveryID)
		inserted, err := insertRunForkReplayDelivery(ctx, tx, lineage, item, sourceEventID, forkEventID, forkDeliveryID, now)
		if err != nil {
			return result, err
		}
		if inserted {
			result.ReplayedDeliveryCount++
		}
	}
	if err := syncRunForkReplayEventCount(ctx, tx, lineage.ForkRunID); err != nil {
		return result, err
	}
	return result, nil
}

func validateRunForkDeliveryEventReplayWorkAgainstPlan(pending []RunForkPendingWork, work []RunForkHistoricalReplayExecutableWork) error {
	evidenceByDeliveryID := make(map[string]RunForkPendingWork, len(pending))
	for _, item := range pending {
		deliveryID := strings.TrimSpace(item.DeliveryID)
		if deliveryID == "" {
			continue
		}
		if _, exists := evidenceByDeliveryID[deliveryID]; exists {
			return fmt.Errorf("store.run_fork.delivery_event_replay current pending evidence has duplicate source delivery %s", deliveryID)
		}
		evidenceByDeliveryID[deliveryID] = item
	}

	seenWork := make(map[string]struct{}, len(work))
	for _, item := range work {
		sourceDeliveryID := strings.TrimSpace(item.SourceDeliveryID)
		if sourceDeliveryID == "" {
			return fmt.Errorf("store.run_fork.delivery_event_replay owner work missing source delivery identity")
		}
		if _, exists := seenWork[sourceDeliveryID]; exists {
			return fmt.Errorf("store.run_fork.delivery_event_replay owner work has duplicate source delivery %s", sourceDeliveryID)
		}
		seenWork[sourceDeliveryID] = struct{}{}

		evidence, ok := evidenceByDeliveryID[sourceDeliveryID]
		if !ok {
			return fmt.Errorf("store.run_fork.delivery_event_replay owner work source delivery %s is not in current pending evidence", sourceDeliveryID)
		}
		if item.Fact != RunForkHistoricalReplayFactEventDeliveries {
			return fmt.Errorf("store.run_fork.delivery_event_replay owner work for source delivery %s has fact %q; want %q", sourceDeliveryID, item.Fact, RunForkHistoricalReplayFactEventDeliveries)
		}
		if !RunForkPendingWorkReplayableForHistoricalReplay(evidence) {
			return fmt.Errorf("store.run_fork.delivery_event_replay owner work source delivery %s is not replayable pending agent work", sourceDeliveryID)
		}
		if strings.TrimSpace(item.SourceEventID) != strings.TrimSpace(evidence.EventID) ||
			strings.TrimSpace(item.SubscriberType) != strings.TrimSpace(evidence.SubscriberType) ||
			strings.TrimSpace(item.SubscriberID) != strings.TrimSpace(evidence.SubscriberID) ||
			strings.TrimSpace(item.Classification) != strings.TrimSpace(evidence.Classification) ||
			strings.TrimSpace(item.ReasonCode) != strings.TrimSpace(evidence.ReasonCode) {
			return fmt.Errorf("store.run_fork.delivery_event_replay owner work source delivery %s does not exactly match current pending evidence", sourceDeliveryID)
		}
	}
	return nil
}

func loadRunForkReplaySourceEvent(ctx context.Context, tx *sql.Tx, sourceRunID, sourceEventID string) (runForkReplaySourceEvent, error) {
	var event runForkReplaySourceEvent
	err := tx.QueryRowContext(ctx, `
		SELECT
			event_id::text,
			event_name,
			entity_id::text,
			flow_instance,
			scope,
			COALESCE(payload, '{}'::jsonb),
			produced_by,
			produced_by_type,
			handler_node
		FROM events
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
	`, sourceRunID, sourceEventID).Scan(
		&event.EventID,
		&event.EventName,
		&event.EntityID,
		&event.FlowInstance,
		&event.Scope,
		&event.Payload,
		&event.ProducedBy,
		&event.ProducedByType,
		&event.HandlerNode,
	)
	if err == sql.ErrNoRows {
		return runForkReplaySourceEvent{}, fmt.Errorf("fork delivery/event replay source event %s not found in run %s", sourceEventID, sourceRunID)
	}
	if err != nil {
		return runForkReplaySourceEvent{}, fmt.Errorf("load fork delivery/event replay source event: %w", err)
	}
	if !json.Valid(event.Payload) {
		return runForkReplaySourceEvent{}, fmt.Errorf("source event %s payload is not valid json", sourceEventID)
	}
	return event, nil
}

func insertRunForkReplayEvent(ctx context.Context, tx *sql.Tx, forkRunID, forkEventID string, event runForkReplaySourceEvent, now time.Time) (bool, error) {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO events (
			event_id, run_id, event_name, entity_id, flow_instance, scope, payload,
			chain_depth, produced_by, produced_by_type, handler_node, source_event_id, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3, NULLIF($4, '')::uuid, NULLIF($5, ''), $6, $7::jsonb,
			0, NULLIF($8, ''), NULLIF($9, ''), NULLIF($10, ''), NULL, $11
		)
		ON CONFLICT (event_id) DO NOTHING
	`, forkEventID, forkRunID, event.EventName, nullStringText(event.EntityID), nullStringText(event.FlowInstance), event.Scope, string(event.Payload), nullStringText(event.ProducedBy), nullStringText(event.ProducedByType), nullStringText(event.HandlerNode), now)
	if err != nil {
		return false, fmt.Errorf("insert fork replay event %s from source event %s: %w", forkEventID, event.EventID, err)
	}
	return rowsAffected(res)
}

func insertRunForkReplayScopeMarker(ctx context.Context, tx *sql.Tx, forkRunID, forkEventID string, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id,
			status, retry_count, reason_code, last_error, active_session_id,
			started_at, delivered_at, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4, $5,
			'delivered', 0, $6, NULL, NULL,
			NULL, $7, $7
		)
		ON CONFLICT (delivery_id) DO NOTHING
	`, deterministicRunForkReplayScopeMarkerDeliveryID(forkRunID, forkEventID), forkRunID, forkEventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID, replayScopeReasonDirect, now)
	if err != nil {
		return fmt.Errorf("insert fork replay committed scope marker for fork event %s: %w", forkEventID, err)
	}
	return nil
}

func insertRunForkReplayDelivery(ctx context.Context, tx *sql.Tx, lineage runForkActivationLineage, item RunForkHistoricalReplayExecutableWork, sourceEventID, forkEventID, forkDeliveryID string, now time.Time) (bool, error) {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id,
			status, retry_count, reason_code, last_error, active_session_id,
			started_at, delivered_at, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4, $5,
			'pending', 0, $6, NULL, NULL,
			NULL, NULL, $7
		)
		ON CONFLICT (delivery_id) DO NOTHING
	`, forkDeliveryID, lineage.ForkRunID, forkEventID, item.SubscriberType, item.SubscriberID, runForkReplayReasonCode(item), now)
	if err != nil {
		return false, fmt.Errorf("insert fork replay delivery %s from source delivery %s: %w", forkDeliveryID, item.SourceDeliveryID, err)
	}
	inserted, err := rowsAffected(res)
	if err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_fork_delivery_event_replays (
			fork_run_id, source_run_id, source_event_id, source_delivery_id,
			fork_event_id, fork_delivery_id, subscriber_type, subscriber_id, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4::uuid,
			$5::uuid, $6::uuid, $7, $8, $9
		)
		ON CONFLICT (fork_run_id, source_delivery_id) DO NOTHING
	`, lineage.ForkRunID, lineage.SourceRunID, sourceEventID, item.SourceDeliveryID, forkEventID, forkDeliveryID, item.SubscriberType, item.SubscriberID, now); err != nil {
		return false, fmt.Errorf("insert fork delivery/event replay lineage for source delivery %s: %w", item.SourceDeliveryID, err)
	}
	return inserted, nil
}

func runForkReplayReasonCode(item RunForkHistoricalReplayExecutableWork) string {
	if reason := strings.TrimSpace(item.ReasonCode); reason != "" {
		return "fork_replay:" + reason
	}
	return runForkDeliveryReplayReasonCode
}

func syncRunForkReplayEventCount(ctx context.Context, tx *sql.Tx, forkRunID string) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET event_count = (
			SELECT COUNT(*)::integer
			FROM events
			WHERE run_id = $1::uuid
		)
		WHERE run_id = $1::uuid
	`, forkRunID); err != nil {
		return fmt.Errorf("sync fork replay event count: %w", err)
	}
	return nil
}

func deterministicRunForkReplayEventID(forkRunID, sourceEventID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm/run-fork/delivery-event-replay/event/"+strings.TrimSpace(forkRunID)+"/"+strings.TrimSpace(sourceEventID))).String()
}

func deterministicRunForkReplayDeliveryID(forkRunID, sourceDeliveryID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm/run-fork/delivery-event-replay/delivery/"+strings.TrimSpace(forkRunID)+"/"+strings.TrimSpace(sourceDeliveryID))).String()
}

func deterministicRunForkReplayScopeMarkerDeliveryID(forkRunID, forkEventID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm/run-fork/delivery-event-replay/scope/"+strings.TrimSpace(forkRunID)+"/"+strings.TrimSpace(forkEventID))).String()
}

func nullStringText(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return strings.TrimSpace(value.String)
}

func rowsAffected(res sql.Result) (bool, error) {
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read affected rows: %w", err)
	}
	return rows > 0, nil
}
