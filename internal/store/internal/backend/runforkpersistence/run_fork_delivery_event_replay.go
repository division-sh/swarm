package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"

	"github.com/division-sh/swarm/internal/runtime/runfork"

	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"

	"github.com/google/uuid"
)

// Replay consumes fixed-cut construction independently of the actor's entity.
// It never runs a constructor or borrows source-run attachment authority.
func projectRunForkReplayInitializedReceiver(snapshot *runForkRevisionSnapshot, event events.Event, delivery runtimedelivery.HistoricalFact, forkRunID string) (events.DeliveryRoute, error) {
	source := delivery.Route.Normalized()
	if !source.Recipient.IsAgent() {
		return events.DeliveryRoute{}, fmt.Errorf("ordinary replay receiver requires an admitted agent delivery")
	}
	if snapshot == nil || snapshot.RunID != event.RunID() || delivery.RunID != snapshot.RunID || delivery.EventID != event.ID() {
		return events.DeliveryRoute{}, fmt.Errorf("replay receiver requires exact source-at-revision publication ownership")
	}
	sourceID, err := source.Identity()
	if err != nil {
		return events.DeliveryRoute{}, err
	}
	publication := make([]events.DeliveryRoute, 0)
	matched := false
	for _, fact := range snapshot.Deliveries {
		candidate := fact.Snapshot
		if candidate.EventID != event.ID() {
			continue
		}
		if candidate.RunID != snapshot.RunID {
			return events.DeliveryRoute{}, fmt.Errorf("source publication contains a foreign-run delivery")
		}
		publication = append(publication, candidate.Route)
		id, err := candidate.Route.Identity()
		if err != nil {
			return events.DeliveryRoute{}, err
		}
		if candidate.DeliveryID == delivery.DeliveryID {
			if matched || id != sourceID || candidate.RunID != snapshot.RunID {
				return events.DeliveryRoute{}, fmt.Errorf("replay delivery contradicts fixed-revision route")
			}
			matched = true
		}
	}
	if !matched {
		return events.DeliveryRoute{}, fmt.Errorf("replay delivery is absent from fixed revision")
	}
	if err := events.ValidateReceiverMaterializations(event, publication); err != nil {
		return events.DeliveryRoute{}, fmt.Errorf("source replay publication: %w", err)
	}
	if source.Target.Empty() {
		return projectRunForkReplayDeliveryRoute(source, snapshot.RunID, forkRunID)
	}
	construction, err := replayReceiverConstructionTarget(event, source)
	if err != nil {
		return events.DeliveryRoute{}, err
	}
	target := construction.Route()
	state, err := loadRunForkConstructedEntityState(snapshot, target.EntityID)
	if err != nil {
		return events.DeliveryRoute{}, fmt.Errorf("agent-only replay requires a reconstructed receiver, not future initialization: %w", err)
	}
	metadata := state.MaterializationMetadata
	if target.FlowInstance != metadata.FlowInstance || target.FlowID != metadata.FlowTemplate {
		return events.DeliveryRoute{}, fmt.Errorf("replay receiver contradicts fixed-revision entity ownership")
	}
	projection, err := runfork.ProjectEntityOwnership(snapshot.RunID, forkRunID, target.EntityID, metadata.FlowInstance)
	if err != nil {
		return events.DeliveryRoute{}, err
	}
	target.EntityID, target.FlowInstance = projection.Fork.EntityID, projection.Fork.FlowInstance
	source.Target, err = projectRunForkReplayActorTarget(snapshot.RunID, forkRunID, source.Target, target.FlowInstance)
	if err != nil {
		return events.DeliveryRoute{}, err
	}
	projected, err := projectRunForkReplayDeliveryRoute(source, snapshot.RunID, forkRunID)
	if err != nil {
		return events.DeliveryRoute{}, err
	}
	constructed, err := events.NewExistingEntityTarget(target)
	if err != nil {
		return events.DeliveryRoute{}, err
	}
	projected.Initialization, err = events.AdmitHistoricalFlowReceiverInitialization(forkRunID, constructed, projected.AgentIdentity.Route,
		events.HistoricalReceiverConstruction{SourceRunID: snapshot.RunID, SourceRevision: snapshot.Revision, SourceReceiver: source.AgentIdentity.Route})
	if err != nil {
		return events.DeliveryRoute{}, err
	}
	return projected, nil
}

func replayReceiverConstructionTarget(event events.Event, source events.DeliveryRoute) (events.DeliveryTargetOwnership, error) {
	construction := source.Target
	if !source.Initialization.Empty() {
		if err := source.Initialization.ValidateEvent(event); err != nil {
			return events.DeliveryTargetOwnership{}, err
		}
		construction = source.Initialization.ConstructionTarget()
	}
	if !construction.ExistingEntity() && !construction.MaterializingEntity() {
		return events.DeliveryTargetOwnership{}, fmt.Errorf("agent replay requires exact flow construction independent of actor ownership")
	}
	return construction, nil
}

func projectRunForkReplayActorTarget(sourceRunID, forkRunID string, target events.DeliveryTargetOwnership, constructedPath string) (events.DeliveryTargetOwnership, error) {
	actor := target.Route()
	if target.EntitylessReceiver() {
		actor.FlowInstance = constructedPath
		return events.NewEntitylessReceiverTarget(actor)
	}
	projection, err := runfork.ProjectEntityOwnership(sourceRunID, forkRunID, actor.EntityID, actor.FlowInstance)
	if err != nil {
		return events.DeliveryTargetOwnership{}, err
	}
	actor.EntityID, actor.FlowInstance = projection.Fork.EntityID, projection.Fork.FlowInstance
	return events.NewExistingEntityTarget(actor)
}

func projectRunForkReplayDeliveryRoute(source events.DeliveryRoute, sourceRunID, forkRunID string) (events.DeliveryRoute, error) {
	source = source.Normalized()
	identity := source.AgentIdentity.Normalize()
	if identity.RunID != strings.TrimSpace(sourceRunID) {
		return events.DeliveryRoute{}, fmt.Errorf("source agent route run %q does not match source run %q", identity.RunID, sourceRunID)
	}
	forkIdentity, err := agentidentity.New(forkRunID, identity.Name, identity.Route)
	if err != nil {
		return events.DeliveryRoute{}, fmt.Errorf("construct fork agent identity: %w", err)
	}
	source.AgentIdentity = forkIdentity
	return source, nil
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

func insertRunForkReplayDeliveryLineage(ctx context.Context, tx *sql.Tx, lineage runForkActivationLineage, item runfork.RunForkHistoricalReplayExecutableWork, sourceEventID, forkEventID string, obligation runtimedelivery.Obligation, now time.Time) (bool, error) {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO run_fork_delivery_event_replays (
			replay_id, fork_run_id, source_run_id, source_event_id, source_delivery_id,
			fork_event_id, fork_delivery_id, subscriber_type, subscriber_id,
			selection_authority, created_at
		)
		VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10, $11
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

func InsertRunForkReplayDelivery(ctx context.Context, attempt *mutationprotocol.Attempt, lineage RunForkActivationLineage, item runfork.RunForkHistoricalReplayExecutableWork, sourceEventID, forkEventID string, obligation runtimedelivery.Obligation, now time.Time) (bool, error) {
	if attempt == nil {
		return false, fmt.Errorf("fork replay delivery requires a mutation attempt")
	}
	if _, err := postgresDeliveryAdapter.CommitInitial(ctx, attempt, forkEventID, lineage.ForkRunID, []events.DeliveryRoute{obligation.Route()}, obligation.Authority()); err != nil {
		return false, fmt.Errorf("insert fork replay delivery %s from source delivery %s: %w", obligation.DeliveryID(), item.SourceDeliveryID, err)
	}
	var inserted bool
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		inserted, err = insertRunForkReplayDeliveryLineage(ctx, tx, lineage, item, sourceEventID, forkEventID, obligation, now)
		return err
	})
	return inserted, err
}

func deterministicRunForkReplayEventID(forkRunID, sourceEventID string) string {
	return runfork.ProjectReplayEventID(forkRunID, sourceEventID)
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
