package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func (s *RunForkPostgresOwner) RequireSelectedForkExecutionEvidenceTx(ctx context.Context, attempt *mutationprotocol.Attempt, lineage runfork.RunForkSelectedContractExecutionLineage) error {
	if attempt == nil {
		return fmt.Errorf("selected fork publication readback requires its mutation attempt")
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return requireSelectedForkExecutionEvidenceTx(ctx, tx, lineage, true)
	})
}

func (s *RunForkSQLiteOwner) RequireSelectedForkExecutionEvidenceTx(ctx context.Context, attempt *mutationprotocol.Attempt, lineage runfork.RunForkSelectedContractExecutionLineage) error {
	if attempt == nil {
		return fmt.Errorf("selected fork publication readback requires its mutation attempt")
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return requireSelectedForkExecutionEvidenceTx(ctx, tx, lineage, false)
	})
}

// Duplicate publication observes the original evidence; it cannot insert a
// missing lineage or ordinal merely because the event already exists.
func requireSelectedForkExecutionEvidenceTx(ctx context.Context, tx *sql.Tx, want runfork.RunForkSelectedContractExecutionLineage, postgres bool) error {
	stored, found, err := readSelectedForkInputLineageTx(ctx, tx, want.ForkRunID, want.SourceEventID, want.ForkEventID)
	if err != nil {
		return err
	}
	if !found || stored.ForkRunID != want.ForkRunID || stored.SourceRunID != want.SourceRunID || stored.SourceEventID != want.SourceEventID ||
		stored.ForkEventID != want.ForkEventID || stored.EventName != want.EventName || stored.SelectionAuthority != want.SelectionAuthority {
		return fmt.Errorf("selected fork duplicate publication lacks its exact original lineage")
	}
	var admitted events.AdmittedEvent
	if postgres {
		admitted, _, found, err = eventpostgres.LoadAdmitted(ctx, tx, stored.ForkEventID)
	} else {
		admitted, _, found, err = eventsqlite.LoadAdmitted(ctx, tx, stored.ForkEventID)
	}
	if err != nil {
		return err
	}
	if !found {
		return eventrecord.Missing(want.ForkEventID)
	}
	event := admitted.Event()
	lineage, selected := event.SelectedForkLineage()
	if !selected || event.AdmissionClass() != events.EventAdmissionSelectedForkReplay || event.ID() != stored.ForkEventID || event.RunID() != stored.ForkRunID ||
		string(event.Type()) != stored.EventName || lineage.SourceRunID() != stored.SourceRunID || lineage.SourceEventID() != stored.SourceEventID ||
		lineage.AuthorityStamp() != stored.SelectionAuthority || !stored.CreatedAt.Equal(event.CreatedAt()) {
		return eventrecord.Corrupt(want.ForkEventID, fmt.Errorf("selected fork duplicate publication changed its retained event/lineage binding"))
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, stored.ForkRunID)
	if err != nil {
		return err
	}
	if binding.SourceRunID != stored.SourceRunID {
		return fmt.Errorf("selected duplicate publication has foreign source binding")
	}
	if err := requireSelectedActivityResultPublishedTx(ctx, tx, binding, stored.SourceEventID, postgres); err != nil {
		return err
	}
	return requireSelectedForkFanOutPublishedTx(ctx, tx, runfork.RunForkSelectedContractBinding{ForkRunID: stored.ForkRunID, SourceRunID: stored.SourceRunID}, stored.SourceEventID, postgres)
}

func (s *RunForkPostgresOwner) CommitSelectedForkExecutionEvidenceTx(ctx context.Context, attempt *mutationprotocol.Attempt, lineage runfork.RunForkSelectedContractExecutionLineage, publication events.AdmittedEvent, routes []events.DeliveryRoute, replies []pinrouting.ReplyRequestContinuation, returns []pinrouting.ReplyReturnContinuation) error {
	if attempt == nil {
		return fmt.Errorf("selected fork execution evidence requires its mutation attempt")
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := requireRunForkReplyReturnEvidenceTx(ctx, tx, lineage, routes, returns, true); err != nil {
			return err
		}
		if err := requireRunForkRequestReplyEvidenceTx(ctx, tx, lineage, routes, replies, true); err != nil {
			return err
		}
		if err := insertPostgresSelectedForkExecutionLineageTx(ctx, tx, lineage, publication, routes); err != nil {
			return err
		}
		if err := requireSelectedActivityResultPublicationTx(ctx, tx, lineage, routes, true); err != nil {
			return err
		}
		return bindSelectedForkFanOutEvidenceTx(ctx, tx, attempt, lineage, publication, routes, true)
	})
}

func (s *RunForkSQLiteOwner) CommitSelectedForkExecutionEvidenceTx(ctx context.Context, attempt *mutationprotocol.Attempt, lineage runfork.RunForkSelectedContractExecutionLineage, publication events.AdmittedEvent, routes []events.DeliveryRoute, replies []pinrouting.ReplyRequestContinuation, returns []pinrouting.ReplyReturnContinuation) error {
	if attempt == nil {
		return fmt.Errorf("selected fork execution evidence requires its mutation attempt")
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := requireRunForkReplyReturnEvidenceTx(ctx, tx, lineage, routes, returns, false); err != nil {
			return err
		}
		if err := requireRunForkRequestReplyEvidenceTx(ctx, tx, lineage, routes, replies, false); err != nil {
			return err
		}
		if err := insertSQLiteSelectedForkExecutionLineageTx(ctx, tx, lineage, publication, routes); err != nil {
			return err
		}
		if err := requireSelectedActivityResultPublicationTx(ctx, tx, lineage, routes, false); err != nil {
			return err
		}
		return bindSelectedForkFanOutEvidenceTx(ctx, tx, attempt, lineage, publication, routes, false)
	})
}

// The fixed source partition determines which ordinal is continuing. The
// selected publication and its fresh delivery authority still own execution.
func bindSelectedForkFanOutEvidenceTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, lineage runfork.RunForkSelectedContractExecutionLineage, publication events.AdmittedEvent, routes []events.DeliveryRoute, postgres bool) error {
	snapshot, plan, err := loadSelectedForkFanOutSourceEvidenceTx(ctx, tx, lineage.ForkRunID, lineage.SourceRunID, postgres)
	if err != nil || snapshot == nil {
		return err
	}
	key, replay, found, err := selectedForkFanOutContinuationTx(ctx, tx, lineage.ForkRunID, plan, lineage.SourceEventID)
	if err != nil || !found {
		return err
	}
	if err := requireSelectedFanOutRecipientPartition(snapshot, replay, publication, routes); err != nil {
		return err
	}
	return bindRunForkContinuedOutcomeTx(ctx, tx, attempt, key, replay, publication.Event().ID(), publication.Event().CreatedAt())
}

func selectedForkFanOutContinuationTx(ctx context.Context, tx *sql.Tx, childRun string, plan runfork.RunForkPlan, sourceEvent string) (fanoutobligation.IntentKey, runfork.RunForkFanOutPendingReplay, bool, error) {
	for _, obligation := range plan.FanOutObligations {
		if obligation.Intent.Request.Deployment == nil {
			for _, replay := range obligation.PendingReplays {
				if replay.SourceEventID == sourceEvent {
					key := obligation.Intent.Request.Key
					key.RunID = childRun
					return key, replay, true, nil
				}
			}
			continue
		}
		for _, pending := range obligation.PendingDeployment {
			if pending.SourceEventID != sourceEvent {
				continue
			}
			key, err := loadRunForkChildDeploymentFeedKeyTx(ctx, tx, childRun, obligation.Intent.Source.Declaration)
			if err != nil {
				return key, runfork.RunForkFanOutPendingReplay{}, false, err
			}
			var versionID string
			if err := tx.QueryRowContext(ctx, `SELECT source_resource_version_id FROM fan_out_intents WHERE run_id=$1 AND deployment_feed_id=$2 AND origin_kind='deployment'`, childRun, key.DeploymentFeedID).Scan(&versionID); err != nil {
				return key, runfork.RunForkFanOutPendingReplay{}, false, err
			}
			if versionID != string(obligation.Intent.Source.VersionID) {
				// The old publication still has lineage, not an ordinal in the
				// new version's fresh feed.
				return key, runfork.RunForkFanOutPendingReplay{}, false, nil
			}
			return key, runfork.RunForkFanOutPendingReplay{Ordinal: pending.Ordinal, SourceEventID: pending.SourceEventID,
				SourceDeliveryIDs: pending.SourceDeliveryIDs, InheritedDisposition: pending.InheritedDisposition}, true, nil
		}
	}
	return fanoutobligation.IntentKey{}, runfork.RunForkFanOutPendingReplay{}, false, nil
}

func loadSelectedForkFanOutSourceEvidenceTx(ctx context.Context, tx *sql.Tx, childRun, sourceRun string, postgres bool) (*runForkRevisionSnapshot, runfork.RunForkPlan, error) {
	var hasFanOut bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fan_out_intents WHERE run_id=$1)`, childRun).Scan(&hasFanOut); err != nil {
		return nil, runfork.RunForkPlan{}, err
	}
	if !hasFanOut {
		return nil, runfork.RunForkPlan{}, nil
	}
	validate, resolve := runforkrevision.ValidateCompleteSQLite, resolveSQLiteRunForkRevisionPoint
	if postgres {
		validate, resolve = runforkrevision.ValidateCompletePostgres, resolveRunForkRevisionPoint
	}
	admission, err := loadRunForkSourceStateAdmission(ctx, tx, childRun, validate, resolve, postgres)
	if err != nil {
		return nil, runfork.RunForkPlan{}, err
	}
	if admission.snapshot.RunID != sourceRun {
		return nil, runfork.RunForkPlan{}, fmt.Errorf("selected fan-out evidence disagrees with bound source run")
	}
	pending, err := loadRunForkPendingWorkFromRevision(admission.snapshot)
	if err != nil {
		return nil, runfork.RunForkPlan{}, err
	}
	obligations, err := loadRunForkFanOutObligationsFromRevision(admission.snapshot, pending)
	if err != nil {
		return nil, runfork.RunForkPlan{}, err
	}
	plan := runfork.RunForkPlan{SourceRunID: sourceRun, PendingWork: pending, FanOutObligations: obligations}
	if err := runfork.ValidateFanOutPendingReplayAdmission(plan); err != nil {
		return nil, runfork.RunForkPlan{}, err
	}
	return admission.snapshot, plan, nil
}

func requireSelectedFanOutRecipientPartition(snapshot *runForkRevisionSnapshot, replay runfork.RunForkFanOutPendingReplay, publication events.AdmittedEvent, routes []events.DeliveryRoute) error {
	event := publication.Event()
	lineage, ok := event.SelectedForkLineage()
	if snapshot == nil || !ok || lineage.SourceRunID() != snapshot.RunID || lineage.DestinationRunID() != event.RunID() || lineage.SourceEventID() != replay.SourceEventID ||
		event.ID() != activityidentity.ForkLineageEventID(event.RunID(), replay.SourceEventID) || len(routes) != len(replay.SourceDeliveryIDs) {
		return fmt.Errorf("continued fan-out publication lacks its exact source/child recipient partition")
	}
	cut, err := newRunForkReplayCut(snapshot)
	if err != nil {
		return err
	}
	sourceEvent, err := cut.event(replay.SourceEventID)
	if err != nil {
		return err
	}
	if event.Type() != sourceEvent.Type() || event.ExecutionMode() != sourceEvent.ExecutionMode() {
		return fmt.Errorf("continued fan-out publication changed its source event or execution mode")
	}
	matched := make(map[events.DeliveryRouteIdentity]bool, len(routes))
	for _, sourceID := range replay.SourceDeliveryIDs {
		delivery, err := cut.delivery(replay.SourceEventID, sourceID)
		if err != nil {
			return err
		}
		projected, err := projectSelectedFanOutRecipient(snapshot, sourceEvent, delivery, event.RunID())
		if err != nil {
			return err
		}
		identity, err := projected.Identity()
		if err != nil || matched[identity] {
			return fmt.Errorf("continued fan-out source partition has contradictory projected routes")
		}
		found := false
		for _, route := range routes {
			found = found || events.SameDeliveryRouteIdentity(route, projected)
		}
		if !found {
			return fmt.Errorf("continued fan-out publication omitted or substituted source delivery %s", sourceID)
		}
		matched[identity] = true
	}
	return nil
}

// Transfer the exact unfinished receiver bindings through the existing native
// projector. The publication fence independently checks them at commit.
func projectContinuedFanOutSourceRoutes(snapshot *runForkRevisionSnapshot, eventID, childRun string) ([]events.DeliveryRoute, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("continued source preparation lacks its fixed cut")
	}
	if len(snapshot.FanOutFacts) == 0 {
		return nil, nil
	}
	pending, err := loadRunForkPendingWorkFromRevision(snapshot)
	if err != nil {
		return nil, err
	}
	obligations, err := loadRunForkFanOutObligationsFromRevision(snapshot, pending)
	if err != nil {
		return nil, err
	}
	plan := runfork.RunForkPlan{SourceRunID: snapshot.RunID, PendingWork: pending, FanOutObligations: obligations}
	partition, found, err := plan.FanOutPendingRecipientPartition(eventID)
	if err != nil || !found {
		return nil, err
	}
	cut, err := newRunForkReplayCut(snapshot)
	if err != nil {
		return nil, err
	}
	event, err := cut.event(eventID)
	if err != nil {
		return nil, err
	}
	routes := make([]events.DeliveryRoute, 0, len(partition.SourceDeliveryIDs))
	for _, id := range partition.SourceDeliveryIDs {
		delivery, err := cut.delivery(eventID, id)
		if err != nil {
			return nil, err
		}
		route, err := projectSelectedFanOutRecipient(snapshot, event, delivery, childRun)
		if err != nil {
			return nil, err
		}
		routes = append(routes, route)
	}
	return routes, nil
}

func projectSelectedFanOutRecipient(snapshot *runForkRevisionSnapshot, event events.Event, delivery deliverylifecycle.Snapshot, childRun string) (events.DeliveryRoute, error) {
	if snapshot == nil || event.RunID() != snapshot.RunID || delivery.RunID != snapshot.RunID || delivery.EventID != event.ID() || delivery.Terminal() {
		return events.DeliveryRoute{}, fmt.Errorf("continued fan-out requires exact unfinished cut evidence")
	}
	var projected events.DeliveryRoute
	var err error
	if delivery.Route.Recipient.IsAgent() {
		projected, err = projectRunForkReplayInitializedReceiver(snapshot, event, delivery, childRun)
	} else if delivery.Route.Recipient.IsNode() {
		projected, err = projectSelectedFanOutNodeReceiver(snapshot, event, delivery.Route, childRun)
	} else {
		return events.DeliveryRoute{}, fmt.Errorf("continued fan-out has an unsupported receiver kind")
	}
	if err != nil {
		return events.DeliveryRoute{}, err
	}
	if projected.Context.Reply != nil || len(projected.Context.Joins) != 0 {
		entities, err := loadRunForkEntityStates(snapshot)
		if err != nil {
			return events.DeliveryRoute{}, err
		}
		entities, _, err = attachRunForkMaterializedEntitySnapshotMetadata(snapshot, entities)
		if err != nil {
			return events.DeliveryRoute{}, err
		}
		plan := runfork.RunForkPlan{SourceRunID: snapshot.RunID, Entities: entities}
		for _, reply := range snapshot.ReplyContexts {
			plan.ReplyContexts = append(plan.ReplyContexts, reply.Record)
		}
		projected.Context, err = projectRunForkConstructionContext(plan, childRun, projected.Context)
		if err != nil {
			return events.DeliveryRoute{}, err
		}
	}
	if _, err := projected.Identity(); err != nil {
		return events.DeliveryRoute{}, err
	}
	return projected, nil
}

func projectSelectedFanOutNodeReceiver(snapshot *runForkRevisionSnapshot, event events.Event, source events.DeliveryRoute, childRun string) (events.DeliveryRoute, error) {
	var publication []events.DeliveryRoute
	for _, delivery := range snapshot.Deliveries {
		if delivery.Snapshot.EventID == event.ID() {
			publication = append(publication, delivery.Snapshot.Route)
		}
	}
	if err := events.ValidateReceiverMaterializations(event, publication); err != nil {
		return events.DeliveryRoute{}, err
	}
	target := source.Target.Route()
	if source.Target.EntitylessReceiver() {
		route, err := runfork.ProjectExecutionRoute(snapshot.RunID, childRun, target.FlowID,
			flowidentity.StoredRoute(target.FlowID, target.FlowInstance, target.FlowInstance))
		if err != nil {
			return events.DeliveryRoute{}, err
		}
		target.FlowInstance = route.InstancePath
		source.Target, err = events.NewEntitylessReceiverTarget(target)
		return source, err
	}
	state, err := loadRunForkConstructedEntityState(snapshot, target.EntityID)
	if err != nil {
		return events.DeliveryRoute{}, err
	}
	owner := state.MaterializationMetadata
	if target.FlowID != owner.FlowTemplate || target.FlowInstance != owner.FlowInstance {
		return events.DeliveryRoute{}, fmt.Errorf("continued node receiver contradicts its fixed owner")
	}
	projection, err := runfork.ProjectEntityOwnership(snapshot.RunID, childRun, target.EntityID, owner.FlowInstance)
	if err != nil {
		return events.DeliveryRoute{}, err
	}
	target.EntityID, target.FlowInstance = projection.Fork.EntityID, projection.Fork.FlowInstance
	source.Target, err = events.NewExistingEntityTarget(target)
	if err != nil {
		return events.DeliveryRoute{}, err
	}
	source.Initialization = events.ReceiverInitialization{}
	return source, nil
}
