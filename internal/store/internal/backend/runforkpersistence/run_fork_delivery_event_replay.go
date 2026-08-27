package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func applyRunForkDeliveryEventReplay(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *runforkrevision.Effects, store *RunForkPostgresOwner, lineage runForkActivationLineage, execution runfork.RunForkHistoricalReplayExecution, now time.Time) (runfork.RunForkDeliveryEventReplayResult, error) {
	result := runfork.RunForkDeliveryEventReplayResult{
		Owner:       runfork.RunForkDeliveryEventReplayOwner,
		SourceRunID: lineage.SourceRunID,
		ForkRunID:   lineage.ForkRunID,
	}
	if strings.TrimSpace(execution.Owner) != runfork.RunForkHistoricalReplayExecutionOwner ||
		strings.TrimSpace(execution.AdmissionOwner) != runfork.RunForkHistoricalReplayExecutionAdmissionOwner ||
		!execution.DeliveryEventReplayReady ||
		execution.EventDeliveriesAdmission.Fact != runfork.RunForkHistoricalReplayFactEventDeliveries ||
		execution.EventDeliveriesAdmission.Admission != runfork.RunForkHistoricalReplayAdmissionExecutableForkWork {
		return result, fmt.Errorf("store.run_fork.delivery_event_replay requires %s owner-authorized executable event_deliveries", runfork.RunForkHistoricalReplayExecutionOwner)
	}
	if story == nil {
		return result, fmt.Errorf("run fork delivery/event replay requires private story ownership")
	}
	replayable := execution.DeliveryEventReplayWork
	if len(replayable) == 0 {
		return result, fmt.Errorf("store.run_fork.delivery_event_replay requires at least one owner-authorized delivery")
	}

	if err := store.requireCurrentSchema(); err != nil {
		return result, err
	}
	sourceEvents := map[string]events.Event{}
	bundleSource, err := runtimecorrelation.NewPersistedBundleSourceFact(lineage.ForkBundleHash)
	if err != nil {
		return result, fmt.Errorf("construct fork replay bundle source: %w", err)
	}
	deliveryAuthority, err := runtimedelivery.NewNormalExecutionAuthority(
		bundleSource, runfork.RunForkDeliveryEventReplayOwner+":"+lineage.ForkRunID, 1,
	)
	if err != nil {
		return result, fmt.Errorf("construct fork replay activation delivery authority: %w", err)
	}
	type preparedReplayDelivery struct {
		item          runfork.RunForkHistoricalReplayExecutableWork
		sourceEventID string
		forkEventID   string
		obligation    runtimedelivery.Obligation
	}
	preparedEvents := make(map[string]events.AdmittedEvent, len(replayable))
	preparedRoutes := make(map[string][]events.DeliveryRoute, len(replayable))
	eventOrder := make([]string, 0, len(replayable))
	preparedDeliveries := make([]preparedReplayDelivery, 0, len(replayable))
	for _, item := range replayable {
		sourceEventID := strings.TrimSpace(item.SourceEventID)
		sourceDeliveryID := strings.TrimSpace(item.SourceDeliveryID)
		if item.Fact != runfork.RunForkHistoricalReplayFactEventDeliveries || sourceEventID == "" || sourceDeliveryID == "" {
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
		forkEventID := deterministicRunForkReplayEventID(lineage.ForkRunID, sourceEventID)
		if _, ok := preparedEvents[forkEventID]; !ok {
			replayed, err := projectRunForkReplayEvent(sourceEvent, lineage, forkEventID, now)
			if err != nil {
				return result, err
			}
			preparedEvents[forkEventID] = replayed
			eventOrder = append(eventOrder, forkEventID)
		}
		sourceDelivery, err := postgresDeliveryAdapter.Snapshot(ctx, tx, sourceDeliveryID)
		if err != nil {
			return result, fmt.Errorf("load source delivery %s for fork replay: %w", sourceDeliveryID, err)
		}
		if sourceDelivery.EventID != sourceEventID || string(sourceDelivery.SubscriberClass) != item.SubscriberType || sourceDelivery.SubscriberID != item.SubscriberID {
			return result, fmt.Errorf("source delivery %s does not exactly match authorized fork replay work", sourceDeliveryID)
		}
		obligation, err := runtimedelivery.NewObligation(forkEventID, lineage.ForkRunID, sourceDelivery.Route, deliveryAuthority)
		if err != nil {
			return result, err
		}
		preparedRoutes[forkEventID] = append(preparedRoutes[forkEventID], obligation.Route())
		preparedDeliveries = append(preparedDeliveries, preparedReplayDelivery{item: item, sourceEventID: sourceEventID, forkEventID: forkEventID, obligation: obligation})
	}
	for _, forkEventID := range eventOrder {
		routes := events.NormalizeDeliveryRoutes(preparedRoutes[forkEventID])
		settlement, err := events.NewDeliverySettlement(events.EventWriteHistoricalRunForkReplay, events.ConnectEvaluationLedger{})
		if err != nil {
			return result, err
		}
		if err := settlement.Validate(routes); err != nil {
			return result, err
		}
		admitted := preparedEvents[forkEventID]
		projected, changed, err := runtimebus.ResolvePreparedPublishEventTargetProjection(admitted.Event(), routes)
		if err != nil {
			return result, fmt.Errorf("project fork replay event %s from exact delivery routes: %w", forkEventID, err)
		}
		if changed {
			admitted, err = admitRunForkReplayEventTargetProjection(projected)
			if err != nil {
				return result, fmt.Errorf("admit fork replay event %s target projection: %w", forkEventID, err)
			}
			preparedEvents[forkEventID] = admitted
		}
		if err := (runtimebus.PreparedPublishEvent{Event: admitted, Settlement: settlement, DeliveryRoutes: routes}).Validate(); err != nil {
			return result, fmt.Errorf("validate fork replay event %s aggregate: %w", forkEventID, err)
		}
		outcome, err := store.events.AppendAdmittedEventTxOutcome(ctx, tx, story, effects, preparedEvents[forkEventID], settlement)
		if err != nil {
			return result, err
		}
		if err := store.PipelinePostgresOwner.CommitScopeAtTx(ctx, tx, effects, forkEventID, runtimepipelineobligation.ScopeDirect, now); err != nil {
			return result, err
		}
		if outcome == runtimebus.EventAppendInserted {
			result.ReplayedEventCount++
		}
	}
	for _, prepared := range preparedDeliveries {
		inserted, err := insertRunForkReplayDelivery(ctx, tx, lineage, prepared.item, prepared.sourceEventID, prepared.forkEventID, prepared.obligation, now)
		if err != nil {
			return result, err
		}
		if inserted {
			result.ReplayedDeliveryCount++
		}
	}
	if err := syncRunForkReplayEventCount(ctx, tx, store, lineage.ForkRunID); err != nil {
		return result, err
	}
	return result, nil
}

func admitRunForkReplayEventTargetProjection(projected events.Event) (events.AdmittedEvent, error) {
	return events.AdmitForPersistence(projected, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
}

func validateRunForkDeliveryEventReplayWorkAgainstPlan(pending []runfork.RunForkPendingWork, work []runfork.RunForkHistoricalReplayExecutableWork) error {
	evidenceByDeliveryID := make(map[string]runfork.RunForkPendingWork, len(pending))
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
		if item.Fact != runfork.RunForkHistoricalReplayFactEventDeliveries {
			return fmt.Errorf("store.run_fork.delivery_event_replay owner work for source delivery %s has fact %q; want %q", sourceDeliveryID, item.Fact, runfork.RunForkHistoricalReplayFactEventDeliveries)
		}
		if !runfork.RunForkPendingWorkReplayableForHistoricalReplay(evidence) {
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

func ValidateRunForkDeliveryEventReplayWorkAgainstPlan(pending []runfork.RunForkPendingWork, work []runfork.RunForkHistoricalReplayExecutableWork) error {
	return validateRunForkDeliveryEventReplayWorkAgainstPlan(pending, work)
}

func loadRunForkReplaySourceEvent(ctx context.Context, tx *sql.Tx, sourceRunID, sourceEventID string) (events.Event, error) {
	row, found, err := eventrecordpostgres.Load(ctx, tx, sourceEventID)
	if err != nil {
		var event events.Event
		return event, fmt.Errorf("load fork delivery/event replay source event: %w", err)
	}
	if !found || row.RunID != strings.TrimSpace(sourceRunID) {
		var event events.Event
		return event, fmt.Errorf("fork delivery/event replay source event %s not found in run %s", sourceEventID, sourceRunID)
	}
	event, err := row.Decode()
	if err != nil {
		var empty events.Event
		return empty, fmt.Errorf("load fork delivery/event replay source event: %w", err)
	}
	return event.Event(), nil
}

func LoadRunForkReplaySourceEvent(ctx context.Context, tx *sql.Tx, sourceRunID, sourceEventID string) (events.Event, error) {
	return loadRunForkReplaySourceEvent(ctx, tx, sourceRunID, sourceEventID)
}

func projectRunForkReplayEvent(source events.Event, lineage runForkActivationLineage, forkEventID string, now time.Time) (events.AdmittedEvent, error) {
	selected, err := events.NewSelectedForkLineage(
		lineage.ForkRunID,
		lineage.SourceRunID,
		source.ID(),
		runfork.RunForkDeliveryEventReplayOwner,
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

func ProjectRunForkReplayEvent(source events.Event, lineage RunForkActivationLineage, forkEventID string, now time.Time) (events.AdmittedEvent, error) {
	return projectRunForkReplayEvent(source, lineage, forkEventID, now)
}

func insertRunForkReplayDelivery(ctx context.Context, tx *sql.Tx, lineage runForkActivationLineage, item runfork.RunForkHistoricalReplayExecutableWork, sourceEventID, forkEventID string, obligation runtimedelivery.Obligation, now time.Time) (bool, error) {
	if _, err := postgresDeliveryAdapter.CommitInitial(ctx, tx, forkEventID, lineage.ForkRunID, []events.DeliveryRoute{obligation.Route()}, obligation.Authority()); err != nil {
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
		runfork.RunForkDeliveryEventReplayOwner, now)
	if err != nil {
		return false, fmt.Errorf("insert fork delivery/event replay lineage for source delivery %s: %w", item.SourceDeliveryID, err)
	}
	return rowsAffected(res)
}

func InsertRunForkReplayDelivery(ctx context.Context, tx *sql.Tx, lineage RunForkActivationLineage, item runfork.RunForkHistoricalReplayExecutableWork, sourceEventID, forkEventID string, obligation runtimedelivery.Obligation, now time.Time) (bool, error) {
	return insertRunForkReplayDelivery(ctx, tx, lineage, item, sourceEventID, forkEventID, obligation, now)
}

func syncRunForkReplayEventCount(ctx context.Context, tx *sql.Tx, store *RunForkPostgresOwner, forkRunID string) error {
	if err := store.RunLifecyclePostgresOwner.SyncCountersTx(ctx, tx, nil, forkRunID); err != nil {
		return fmt.Errorf("sync fork replay event count: %w", err)
	}
	return nil
}

func deterministicRunForkReplayEventID(forkRunID, sourceEventID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm/run-fork/delivery-event-replay/event/"+strings.TrimSpace(forkRunID)+"/"+strings.TrimSpace(sourceEventID))).String()
}

func DeterministicRunForkReplayEventID(forkRunID, sourceEventID string) string {
	return deterministicRunForkReplayEventID(forkRunID, sourceEventID)
}

func deterministicRunForkReplayLineageID(forkRunID, sourceDeliveryID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm/run-fork/delivery-event-replay/lineage/"+strings.TrimSpace(forkRunID)+"/"+strings.TrimSpace(sourceDeliveryID))).String()
}

func rowsAffected(res sql.Result) (bool, error) {
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read affected rows: %w", err)
	}
	return rows > 0, nil
}
