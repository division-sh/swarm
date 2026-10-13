package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func insertSelectedPendingJoinLineage(ctx context.Context, tx *sql.Tx, lineage runfork.RunForkSelectedContractExecutionLineage, cause joinruntime.CommittedPublicationCause) error {
	query := `INSERT INTO run_fork_selected_contract_executions (
		execution_id,fork_run_id,source_run_id,source_event_id,fork_event_id,event_name,selection_authority,created_at,
		source_obligation_event_id,committed_cause_run_id)
		SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10 FROM events cause WHERE cause.event_id=$4 AND cause.run_id=$10`
	result, err := tx.ExecContext(ctx, query, uuid.NewString(), lineage.ForkRunID, lineage.SourceRunID, cause.EventID,
		lineage.ForkEventID, lineage.EventName, lineage.SelectionAuthority, lineage.CreatedAt, lineage.SourceEventID, cause.RunID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return fmt.Errorf("pending publication lost its exact committed cause: %w", err)
	}
	return nil
}

func admitSelectedPendingJoinLineage(ctx context.Context, tx *sql.Tx, lineage runfork.RunForkSelectedContractExecutionLineage, publication events.AdmittedEvent, routes []events.DeliveryRoute, postgres bool) (joinruntime.CommittedPublicationCause, bool, error) {
	if lineage.EventName != "platform.join_complete" && lineage.EventName != "platform.join_timeout" {
		return joinruntime.CommittedPublicationCause{}, false, nil
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, lineage.ForkRunID)
	if err != nil {
		return joinruntime.CommittedPublicationCause{}, false, err
	}
	if binding.SourceRunID != lineage.SourceRunID {
		return joinruntime.CommittedPublicationCause{}, false, fmt.Errorf("pending publication has foreign immediate source binding")
	}
	snapshot, err := loadRunForkPointSnapshot(ctx, tx, binding.SourceRunID, binding.ForkPoint)
	if err != nil {
		return joinruntime.CommittedPublicationCause{}, false, err
	}
	admission := runForkSourceStateAdmission{forkRunID: binding.ForkRunID, snapshot: snapshot, point: binding.ForkPoint, postgres: postgres}
	_, inputs, err := loadRunForkPendingJoinInputs(ctx, tx, admission, []string{lineage.SourceEventID})
	if err != nil || len(inputs) == 0 {
		return joinruntime.CommittedPublicationCause{}, false, err
	}
	input := inputs[0]
	if len(inputs) != 1 || input.SourceEventID != lineage.SourceEventID || input.EventName != lineage.EventName ||
		publication.Event().ID() != lineage.ForkEventID || publication.Event().RunID() != lineage.ForkRunID {
		return joinruntime.CommittedPublicationCause{}, false, fmt.Errorf("pending publication changes exact input identity")
	}
	if err := input.JoinContinuation.ValidateEvent(publication.Event(), lineage.SelectionAuthority); err != nil {
		return joinruntime.CommittedPublicationCause{}, false, err
	}
	if err := requireExactRunForkRequestRoutes(routes, []events.DeliveryRoute{input.JoinRoute}); err != nil {
		return joinruntime.CommittedPublicationCause{}, false, err
	}
	source, pending := input.JoinContinuation.PendingSource()
	if !pending {
		return joinruntime.CommittedPublicationCause{}, false, fmt.Errorf("pending publication lost its exact source obligation")
	}
	return source.Publication.CommittedCause, true, nil
}

// Pending inputs are admitted from the exact source obligation and the child's
// immutable materialization, never a later source publication or fresh timer.
func loadRunForkPendingJoinInputs(ctx context.Context, tx *sql.Tx, admission runForkSourceStateAdmission, ids []string) ([]string, []runfork.RunForkSelectedContractSourceEvent, error) {
	plan, err := loadRunForkJoinObligationPlan(admission.snapshot, admission.point)
	if err != nil {
		return nil, nil, err
	}
	pending, err := plan.PendingJoinOccurrences()
	if err != nil {
		return nil, nil, err
	}
	byID := make(map[string]genericschedule.TransferredJoinOccurrence, len(pending))
	for _, source := range pending {
		byID[source.Publication.EventID] = source
	}
	physical := make([]string, 0, len(ids))
	var inputs []runfork.RunForkSelectedContractSourceEvent
	for _, id := range ids {
		source, found := byID[id]
		if !found {
			physical = append(physical, id)
			continue
		}
		input, err := admitRunForkPendingJoinInput(ctx, tx, admission, plan, source)
		if err != nil {
			return nil, nil, err
		}
		inputs = append(inputs, input)
	}
	return physical, inputs, nil
}

func admitRunForkPendingJoinInput(ctx context.Context, tx *sql.Tx, admission runForkSourceStateAdmission, plan runfork.RunForkPlan, source genericschedule.TransferredJoinOccurrence) (runfork.RunForkSelectedContractSourceEvent, error) {
	var route events.DeliveryRoute
	if err := requireRunForkTransferredJoinCause(ctx, tx, source, make(map[string]bool), &route); err != nil {
		return runfork.RunForkSelectedContractSourceEvent{}, err
	}
	start, err := resolveRunForkStartPoint(ctx, tx, admission.forkRunID)
	if err != nil {
		return runfork.RunForkSelectedContractSourceEvent{}, err
	}
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: start.Revision}
	snapshot, err := loadRunForkPointSnapshot(ctx, tx, admission.forkRunID, point)
	if err != nil {
		return runfork.RunForkSelectedContractSourceEvent{}, err
	}
	child, err := loadRunForkJoinObligationPlan(snapshot, point)
	if err != nil {
		return runfork.RunForkSelectedContractSourceEvent{}, err
	}
	id := activityidentity.ForkLineageEventID(admission.forkRunID, source.Publication.EventID)
	materialized, found, err := child.PendingJoinOccurrence(id)
	if err != nil || !found {
		return runfork.RunForkSelectedContractSourceEvent{}, fmt.Errorf("pending join lacks exact child materialization: found=%t: %w", found, err)
	}
	continuation, err := genericschedule.ProjectPendingJoinContinuation(source, materialized.Command)
	if err != nil {
		return runfork.RunForkSelectedContractSourceEvent{}, err
	}
	publication, err := continuation.RetainedPublication(runfork.RunForkSelectedContractExecutionOwner)
	if err != nil || publication != materialized.Publication {
		return runfork.RunForkSelectedContractSourceEvent{}, fmt.Errorf("pending join contradicts materialized lineage: %w", err)
	}
	projected, err := continuation.Event(id, runfork.RunForkSelectedContractExecutionOwner)
	if err != nil {
		return runfork.RunForkSelectedContractSourceEvent{}, err
	}
	route, err = projectRunForkPublishedJoinRoute(plan, materialized.Command, route)
	if err != nil {
		return runfork.RunForkSelectedContractSourceEvent{}, err
	}
	return runfork.RunForkSelectedContractSourceEvent{SourceEventID: source.Publication.EventID,
		EventName: source.Command.EventType, ExecutionMode: source.Command.ExecutionMode,
		Scope: string(projected.Scope()), RoutingSource: projected.RoutingSource(), Payload: projected.Payload(),
		JoinContinuation: continuation, JoinRoute: route}, nil
}

// Pending inputs retain their original due, while physical inputs retain their
// captured publication time. Appending one variant must not reorder the other.
func orderRunForkSourceJoinInputs(inputs []runfork.RunForkSelectedContractSourceEvent, snapshot *runForkRevisionSnapshot) error {
	hasPending := false
	for _, input := range inputs {
		_, pending := input.JoinContinuation.PendingSource()
		hasPending = hasPending || pending
	}
	if !hasPending {
		return nil
	}
	if snapshot == nil {
		return fmt.Errorf("mixed join inputs require their exact source cut")
	}
	physical := make(map[string]time.Time, len(snapshot.Events))
	for _, event := range snapshot.Events {
		if event.RunID != snapshot.RunID {
			return fmt.Errorf("mixed join inputs contain a foreign captured publication")
		}
		if _, duplicate := physical[event.EventID]; duplicate {
			return fmt.Errorf("mixed join inputs repeat a captured publication")
		}
		physical[event.EventID] = event.CreatedAt
	}
	times := make(map[string]time.Time, len(inputs))
	for _, input := range inputs {
		if _, duplicate := times[input.SourceEventID]; duplicate {
			return fmt.Errorf("mixed join inputs repeat an execution coordinate")
		}
		at, published := physical[input.SourceEventID]
		if source, pending := input.JoinContinuation.PendingSource(); pending {
			if published || source.Command.RunID != snapshot.RunID || source.Publication.EventID != input.SourceEventID {
				return fmt.Errorf("pending join input contradicts its captured source coordinate")
			}
			at = source.Command.Due.Absolute
		}
		if at.IsZero() {
			return fmt.Errorf("mixed join input lacks its captured time")
		}
		times[input.SourceEventID] = at
	}
	sort.Slice(inputs, func(i, j int) bool {
		left, right := inputs[i].SourceEventID, inputs[j].SourceEventID
		if times[left].Equal(times[right]) {
			return left < right
		}
		return times[left].Before(times[right])
	})
	return nil
}
