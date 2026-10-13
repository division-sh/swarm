package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// Cause membership is read through each actual binding and immutable cut. A
// pending intermediate publication is never loaded or fabricated as an event.
func requireRunForkTransferredJoinCauses(ctx context.Context, tx *sql.Tx, plan runfork.RunForkPlan) error {
	sources, err := runForkRetainedJoinSources(plan)
	if err != nil {
		return err
	}
	for _, source := range sources {
		if err := requireRunForkTransferredJoinCause(ctx, tx, source, make(map[string]bool), nil); err != nil {
			return err
		}
	}
	return nil
}

func requireRunForkTransferredJoinCause(ctx context.Context, tx *sql.Tx, source genericschedule.TransferredJoinOccurrence, visiting map[string]bool, projectedRoute *events.DeliveryRoute) error {
	if err := source.Validate(); err != nil {
		return err
	}
	var present bool
	if tx == nil {
		return fmt.Errorf("transferred join cause requires a native transaction")
	}
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE event_id=$1 AND run_id=$2)`,
		source.Publication.CommittedCause.EventID, source.Publication.CommittedCause.RunID).Scan(&present); err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("transferred join lacks its retained committed cause event")
	}
	key := source.Command.RunID + "/" + source.Publication.EventID
	if tx == nil || visiting[key] {
		return fmt.Errorf("transferred join cause requires acyclic native binding evidence")
	}
	visiting[key] = true
	defer delete(visiting, key)
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, source.Command.RunID)
	if err != nil {
		return err
	}
	if binding.SourceRunID != source.Publication.SourceRunID || binding.ForkRunID != source.Command.RunID ||
		source.Publication.AuthorityStamp != runfork.RunForkSelectedContractExecutionOwner {
		return fmt.Errorf("transferred join cause contradicts its immediate retained binding")
	}
	snapshot, err := loadRunForkPointSnapshot(ctx, tx, binding.SourceRunID, binding.ForkPoint)
	if err != nil {
		return err
	}
	parent, err := loadRunForkJoinObligationPlan(snapshot, binding.ForkPoint)
	if err != nil {
		return err
	}
	var expected genericschedule.JoinContinuation
	var route events.DeliveryRoute
	if publication, published := parent.HistoricalJoinPublication(source.Publication.SourceEventID); published {
		ancestors, err := runForkRetainedJoinSources(parent)
		if err != nil {
			return err
		}
		for _, ancestor := range ancestors {
			if ancestor.Publication.EventID == source.Publication.SourceEventID {
				if err := requireRunForkTransferredJoinCause(ctx, tx, ancestor, visiting, nil); err != nil {
					return err
				}
			}
		}
		route, err = requireRunForkCommittedJoinRoute(snapshot, parent, publication)
		if err != nil {
			return err
		}
		expected, err = publication.Continuation(source.Command)
	} else {
		sources, err := runForkRetainedJoinSources(parent)
		if err != nil {
			return err
		}
		found := false
		for _, previous := range sources {
			if previous.Publication.EventID != source.Publication.SourceEventID {
				continue
			}
			if found {
				return fmt.Errorf("transferred join cause repeats an immediate source obligation")
			}
			found = true
			if err := requireRunForkTransferredJoinCause(ctx, tx, previous, visiting, &route); err != nil {
				return err
			}
			expected, err = genericschedule.ProjectPendingJoinContinuation(previous, source.Command)
			if err != nil {
				return err
			}
		}
		if !found {
			return fmt.Errorf("transferred join lacks its exact committed cause or pending source obligation")
		}
	}
	if err != nil {
		return err
	}
	publication, err := expected.RetainedPublication(source.Publication.AuthorityStamp)
	if err != nil {
		return err
	}
	if publication != source.Publication {
		return fmt.Errorf("transferred join contradicts its exact retained committed cause")
	}
	if projectedRoute != nil {
		*projectedRoute, err = projectRunForkPublishedJoinRoute(parent, source.Command, route)
		if err != nil {
			return err
		}
	}
	return nil
}

func requireRunForkCommittedJoinRoute(snapshot *runForkRevisionSnapshot, plan runfork.RunForkPlan, publication runfork.PublishedJoinOccurrence) (events.DeliveryRoute, error) {
	command := publication.Command()
	_, ref, valid := timeridentity.ParseJoinHandle(command.Payload.Interface().(map[string]any))
	if !valid {
		return events.DeliveryRoute{}, fmt.Errorf("committed join lacks its exact handle")
	}
	if ref.Mode() == timeridentity.JoinRefModeArrival {
		return requireRunForkPublishedJoinDelivery(snapshot, command, publication.Event(), false)
	}
	for _, obligation := range plan.FanOutObligations {
		if obligation.Barrier != nil && obligation.Barrier.Registration.Handle.TaskID() == command.TaskID {
			return requireRunForkPublishedBarrierDelivery(snapshot, *obligation.Barrier, publication.Event(), false)
		}
	}
	return events.DeliveryRoute{}, fmt.Errorf("committed barrier lacks its exact retained owner")
}

// These are the same canonical reconstruction owners used by full planning and
// source preparation, restricted to the join obligation needed by this read.
func loadRunForkJoinObligationPlan(snapshot *runForkRevisionSnapshot, point runfork.RunForkPoint) (runfork.RunForkPlan, error) {
	if snapshot == nil || snapshot.Revision != point.Revision || snapshot.RunID == "" {
		return runfork.RunForkPlan{}, fmt.Errorf("join obligations require their exact native cut")
	}
	plan := runfork.RunForkPlan{SourceRunID: snapshot.RunID, ForkPoint: point}
	var err error
	plan.Entities, err = loadRunForkEntityStates(snapshot)
	if err != nil {
		return plan, err
	}
	plan.Entities, _, err = attachRunForkMaterializedEntitySnapshotMetadata(snapshot, plan.Entities)
	if err != nil {
		return plan, err
	}
	plan.JoinSchedules, err = loadRunForkArrivalJoinSchedules(snapshot, plan.Entities)
	if err != nil {
		return plan, err
	}
	if err := attachRunForkPublishedArrivals(snapshot, plan.Entities, plan.JoinSchedules); err != nil {
		return plan, err
	}
	plan.TransferredJoins = runForkTransferredJoins(plan.Entities)
	plan.PendingWork, err = loadRunForkPendingWorkFromRevision(snapshot)
	if err != nil {
		return plan, err
	}
	plan.FanOutObligations, err = loadRunForkFanOutObligationsFromRevision(snapshot, plan.PendingWork)
	if err != nil {
		return plan, err
	}
	for _, reply := range snapshot.ReplyContexts {
		plan.ReplyContexts = append(plan.ReplyContexts, reply.Record)
	}
	ids := make([]string, 0, len(snapshot.Events))
	for _, event := range snapshot.Events {
		ids = append(ids, event.EventID)
	}
	plan = plan.WithHistoricalEvents(snapshot.Revision, ids)
	publications, err := runForkAllJoinPublications(snapshot, plan)
	if err != nil {
		return plan, err
	}
	return plan.WithHistoricalJoinPublications(snapshot.Revision, publications)
}

func runForkRetainedJoinSources(plan runfork.RunForkPlan) ([]genericschedule.TransferredJoinOccurrence, error) {
	sources := append([]genericschedule.TransferredJoinOccurrence(nil), plan.TransferredJoins...)
	for _, obligation := range plan.FanOutObligations {
		if obligation.Barrier == nil || obligation.Barrier.TransferredOccurrence == nil {
			continue
		}
		source, err := genericschedule.NewTransferredFanOutBarrierOccurrence(*obligation.Barrier)
		if err != nil {
			return nil, err
		}
		sources = append(sources, source)
	}
	seen := make(map[string]bool, len(sources))
	for _, source := range sources {
		if source.Command.RunID != plan.SourceRunID || seen[source.Publication.EventID] {
			return nil, fmt.Errorf("retained join inventory repeats or borrows a source obligation")
		}
		seen[source.Publication.EventID] = true
	}
	return sources, nil
}
