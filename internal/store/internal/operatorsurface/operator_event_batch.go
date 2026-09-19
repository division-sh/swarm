package operatorsurface

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
)

// Never hydrate beyond the possible matching lookahead. A larger raw window
// could expose corruption that scalar pagination would not consume.
func operatorEventBatchSize(limit, collected int) int {
	return min(128, limit+1-collected)
}

func (r *ObservabilityPostgres) loadOperatorEventBatch(ctx context.Context, tx *sql.Tx, ids []string) ([]operatorread.OperatorEventFull, error) {
	admitted, err := eventrecordpostgres.LoadAdmittedMany(ctx, tx, ids)
	if err != nil {
		return nil, operatorEventBatchError(err)
	}
	deliveries, err := operatorPostgresDelivery.SnapshotsForEvents(ctx, tx, ids)
	if err != nil {
		return nil, fmt.Errorf("load operator event deliveries: %w", err)
	}
	deadLetters, err := loadOperatorEventDeadLetterBatch(ctx, tx, true, ids)
	if err != nil {
		return nil, err
	}
	return assembleOperatorEventBatch(admitted, deliveries, deadLetters)
}

func (s *ObservabilitySQLite) loadOperatorEventBatch(ctx context.Context, tx *sql.Tx, ids []string) ([]operatorread.OperatorEventFull, error) {
	admitted, err := eventrecordsqlite.LoadAdmittedMany(ctx, tx, ids)
	if err != nil {
		return nil, operatorEventBatchError(err)
	}
	deliveries, err := operatorSQLiteDelivery.SnapshotsForEvents(ctx, tx, ids)
	if err != nil {
		return nil, fmt.Errorf("load sqlite operator event deliveries: %w", err)
	}
	deadLetters, err := loadOperatorEventDeadLetterBatch(ctx, tx, false, ids)
	if err != nil {
		return nil, err
	}
	return assembleOperatorEventBatch(admitted, deliveries, deadLetters)
}

func operatorEventBatchError(err error) error {
	if errors.Is(err, eventrecord.ErrMissing) {
		return operatorread.ErrEventNotFound
	}
	return fmt.Errorf("load operator event: %w", err)
}

func assembleOperatorEventBatch(
	admitted []eventrecord.AdmittedRecord,
	snapshots map[string][]runtimedelivery.Snapshot,
	deadLetters map[string][]operatorread.OperatorDeadLetterRecord,
) ([]operatorread.OperatorEventFull, error) {
	out := make([]operatorread.OperatorEventFull, 0, len(admitted))
	for _, record := range admitted {
		id := record.Event.ID()
		deliveries := make([]operatorread.OperatorEventDelivery, 0, len(snapshots[id]))
		for _, snapshot := range snapshots[id] {
			deliveries = append(deliveries, operatorEventDeliveryFromSnapshot(snapshot))
		}
		event, err := assembleOperatorEvent(record.Event, record.Settlement, deliveries, deadLetters[id])
		if err != nil {
			return nil, err
		}
		out = append(out, event)
	}
	return out, nil
}

func loadOperatorEventDeadLetterBatch(ctx context.Context, db interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, postgres bool, ids []string) (map[string][]operatorread.OperatorDeadLetterRecord, error) {
	out := make(map[string][]operatorread.OperatorDeadLetterRecord, len(ids))
	for _, id := range ids {
		out[id] = []operatorread.OperatorDeadLetterRecord{}
	}
	for start := 0; start < len(ids); start += 128 {
		batch := ids[start:min(start+128, len(ids))]
		args, binds := make([]any, len(batch)), make([]string, len(batch))
		for i, id := range batch {
			args[i], binds[i] = id, "?"
			if postgres {
				binds[i] = fmt.Sprintf("$%d::uuid", i+1)
			}
		}
		rows, err := db.QueryContext(ctx, `
			SELECT CAST(original_event_id AS TEXT), CAST(dead_letter_id AS TEXT),
			       COALESCE(CAST(delivery_id AS TEXT), ''), COALESCE(claim_version, 0), failure,
			       COALESCE(retry_count, 0), COALESCE(chain_depth, 0), COALESCE(handler_node, ''), created_at
			FROM dead_letters
			WHERE original_event_id IN (`+strings.Join(binds, ",")+`)
			ORDER BY created_at ASC, CAST(dead_letter_id AS TEXT) ASC`, args...)
		if err != nil {
			return nil, fmt.Errorf("load operator event dead letters: %w", err)
		}
		for rows.Next() {
			var eventID string
			var item operatorread.OperatorDeadLetterRecord
			var rawFailure, createdRaw any
			if err := rows.Scan(&eventID, &item.DeadLetterID, &item.DeliveryID, &item.ClaimVersion, &rawFailure, &item.RetryCount, &item.ChainDepth, &item.HandlerNode, &createdRaw); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan operator event dead letter: %w", err)
			}
			if _, requested := out[eventID]; !requested {
				rows.Close()
				return nil, fmt.Errorf("operator dead letter belongs to an unrequested event %s", eventID)
			}
			failure, err := decodeStoredFailure(rawFailure)
			if err != nil || failure == nil {
				rows.Close()
				return nil, fmt.Errorf("decode operator event dead letter failure: %w", err)
			}
			item.Failure = *failure
			if at, present, err := sqliteTimeValue(createdRaw); err != nil {
				rows.Close()
				return nil, err
			} else if present {
				item.CreatedAt = at
			} else if postgres {
				rows.Close()
				return nil, fmt.Errorf("operator event dead letter requires created_at")
			}
			out[eventID] = append(out[eventID], item)
		}
		readErr := rows.Err()
		closeErr := rows.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read operator event dead letters: %w", readErr)
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	return out, nil
}

func assembleOperatorEvent(
	admitted events.AdmittedEvent,
	settlement events.RouteSettlement,
	deliveries []operatorread.OperatorEventDelivery,
	deadLetters []operatorread.OperatorDeadLetterRecord,
) (operatorread.OperatorEventFull, error) {
	event, err := operatorread.NewOperatorEventFull(admitted.Event())
	if err != nil {
		return operatorread.OperatorEventFull{}, err
	}
	event.Deliveries = operatorread.EnrichOperatorDeliveryFailureEvidence(deliveries, deadLetters)
	event.DeadLetters = deadLetters
	if err := applyRouteSettlement(&event, settlement); err != nil {
		return operatorread.OperatorEventFull{}, fmt.Errorf("load operator event settlement: %w", err)
	}
	if event.Deliveries == nil {
		event.Deliveries = []operatorread.OperatorEventDelivery{}
	}
	if event.DeadLetters == nil {
		event.DeadLetters = []operatorread.OperatorDeadLetterRecord{}
	}
	return event, nil
}
