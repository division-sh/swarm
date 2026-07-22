package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

const (
	RunForkDeliveryEventReplayOwner = "store.run_fork.delivery_event_replay"
	runForkDeliveryEventReplayTable = "run_fork_delivery_event_replays"
)

type RunForkDeliveryEventReplayResult struct {
	Owner                 string `json:"owner"`
	SourceRunID           string `json:"source_run_id"`
	ForkRunID             string `json:"fork_run_id"`
	ReplayedEventCount    int    `json:"replayed_event_count"`
	ReplayedDeliveryCount int    `json:"replayed_delivery_count"`
}

func applyRunForkDeliveryEventReplay(ctx context.Context, tx *sql.Tx, store *PostgresStore, lineage runForkActivationLineage, execution RunForkHistoricalReplayExecution, now time.Time) (RunForkDeliveryEventReplayResult, error) {
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
	if err := runtimeauthoractivity.Require(ctx); err != nil {
		return result, err
	}
	replayable := execution.DeliveryEventReplayWork
	if len(replayable) == 0 {
		return result, nil
	}

	if err := store.requireCurrentSchema(); err != nil {
		return result, err
	}
	sourceEvents := map[string]events.Event{}
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
			replayed, err := projectRunForkReplayEvent(sourceEvent, lineage, forkEventID, now)
			if err != nil {
				return result, err
			}
			outcome, err := store.appendEventSpec(ctx, tx, replayed)
			if err != nil {
				return result, err
			}
			if err := store.upsertCommittedReplayScopeSpec(ctx, tx, forkEventID, "direct"); err != nil {
				return result, err
			}
			if outcome == runtimebus.EventAppendInserted {
				result.ReplayedEventCount++
			}
			insertedEvents[sourceEventID] = forkEventID
		}
		sourceDelivery, err := postgresDeliveryAdapter.Snapshot(ctx, tx, sourceDeliveryID)
		if err != nil {
			return result, fmt.Errorf("load source delivery %s for fork replay: %w", sourceDeliveryID, err)
		}
		if sourceDelivery.EventID != sourceEventID || string(sourceDelivery.SubscriberClass) != item.SubscriberType || sourceDelivery.SubscriberID != item.SubscriberID {
			return result, fmt.Errorf("source delivery %s does not exactly match authorized fork replay work", sourceDeliveryID)
		}
		obligation, err := runtimedelivery.NewObligation(forkEventID, lineage.ForkRunID, sourceDelivery.Route)
		if err != nil {
			return result, err
		}
		inserted, err := insertRunForkReplayDelivery(ctx, tx, lineage, item, sourceEventID, forkEventID, obligation, now)
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

func loadRunForkReplaySourceEvent(ctx context.Context, tx *sql.Tx, sourceRunID, sourceEventID string) (events.Event, error) {
	row, found, err := loadPostgresEventIdentity(ctx, tx, sourceEventID)
	if err != nil {
		var event events.Event
		return event, fmt.Errorf("load fork delivery/event replay source event: %w", err)
	}
	if !found || row.RunID != strings.TrimSpace(sourceRunID) {
		var event events.Event
		return event, fmt.Errorf("fork delivery/event replay source event %s not found in run %s", sourceEventID, sourceRunID)
	}
	event, err := decodeEventRecord(row)
	if err != nil {
		var empty events.Event
		return empty, fmt.Errorf("load fork delivery/event replay source event: %w", err)
	}
	return event.Event(), nil
}

func projectRunForkReplayEvent(source events.Event, lineage runForkActivationLineage, forkEventID string, now time.Time) (events.AdmittedEvent, error) {
	selected, err := events.NewSelectedForkLineage(
		lineage.ForkRunID,
		lineage.SourceRunID,
		source.ID(),
		RunForkDeliveryEventReplayOwner,
		source.TaskID(),
		source.ExecutionMode(),
	)
	if err != nil {
		return events.AdmittedEvent{}, err
	}
	replayed, err := events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{
		Facts: events.EventFacts{
			ID: forkEventID, Type: source.Type(),
			Producer: events.ProducerClaim{Type: source.ProducerType(), ID: source.SourceAgent()},
			TaskID:   source.TaskID(), Payload: source.Payload(), Envelope: source.Envelope(),
			RoutingSource: source.RoutingSource(), CreatedAt: now, ExecutionMode: source.ExecutionMode(),
		},
		Lineage: selected,
	})
	if err != nil {
		return events.AdmittedEvent{}, err
	}
	admitted, err := events.AdmitForPersistence(replayed, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return events.AdmittedEvent{}, fmt.Errorf("project fork replay event %s from source event %s: %w", forkEventID, source.ID(), err)
	}
	return admitted, nil
}

func insertRunForkReplayDelivery(ctx context.Context, tx *sql.Tx, lineage runForkActivationLineage, item RunForkHistoricalReplayExecutableWork, sourceEventID, forkEventID string, obligation runtimedelivery.Obligation, now time.Time) (bool, error) {
	if _, err := postgresDeliveryAdapter.CommitInitial(ctx, tx, forkEventID, lineage.ForkRunID, []events.DeliveryRoute{obligation.Route()}); err != nil {
		return false, fmt.Errorf("insert fork replay delivery %s from source delivery %s: %w", obligation.DeliveryID(), item.SourceDeliveryID, err)
	}
	res, err := tx.ExecContext(ctx, `
		INSERT INTO run_fork_delivery_event_replays (
			replay_id, fork_run_id, source_run_id, source_event_id, source_delivery_id,
			fork_event_id, fork_delivery_id, subscriber_type, subscriber_id,
			selection_authority, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::uuid,
			$6::uuid, $7::uuid, $8, $9, $10, $11
		)
		ON CONFLICT (fork_run_id, source_delivery_id) DO NOTHING
	`, deterministicRunForkReplayLineageID(lineage.ForkRunID, item.SourceDeliveryID), lineage.ForkRunID, lineage.SourceRunID,
		sourceEventID, item.SourceDeliveryID, forkEventID, obligation.DeliveryID(), item.SubscriberType, item.SubscriberID,
		RunForkDeliveryEventReplayOwner, now)
	if err != nil {
		return false, fmt.Errorf("insert fork delivery/event replay lineage for source delivery %s: %w", item.SourceDeliveryID, err)
	}
	return rowsAffected(res)
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

func deterministicRunForkReplayLineageID(forkRunID, sourceDeliveryID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm/run-fork/delivery-event-replay/lineage/"+strings.TrimSpace(forkRunID)+"/"+strings.TrimSpace(sourceDeliveryID))).String()
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
