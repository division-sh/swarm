package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"slices"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

type runForkReplyContextOwner interface {
	CreateWithinTransaction(context.Context, *mutationprotocol.Attempt, replycontext.Record) error
	LoadWithinTransaction(context.Context, *sql.Tx, string) (replycontext.Record, error)
}

// Reply authority is projected from the captured semantic record. In particular,
// old return entries and carried correlation are not selected again at child birth.
func projectRunForkReplyContext(plan runfork.RunForkPlan, childRunID string, source replycontext.Record) (replycontext.Record, error) {
	if err := validateRunForkReplyProjection(plan, childRunID, source); err != nil {
		return replycontext.Record{}, err
	}
	origin, err := projectRunForkReplyOrigin(plan, childRunID, source.Origin)
	if err != nil {
		return replycontext.Record{}, err
	}
	context, err := projectRunForkConstructionContext(plan, childRunID, events.DeliveryContext{Joins: source.ReturnJoins})
	if err != nil {
		return replycontext.Record{}, fmt.Errorf("project reply return admissions: %w", err)
	}
	child := source
	child.ID, child.RunID = deterministicRunForkReplyContextID(childRunID, source.ID), childRunID
	child.RequestEventID = deterministicRunForkReplayEventID(childRunID, source.RequestEventID)
	child.Origin, child.ReturnJoins = origin, context.Joins
	if source.AcceptedReplyEventID != "" {
		child.AcceptedReplyEventID = deterministicRunForkReplayEventID(childRunID, source.AcceptedReplyEventID)
	}
	if source.TerminalAt != nil {
		terminalAt := *source.TerminalAt
		child.TerminalAt = &terminalAt
	}
	return child, child.Validate()
}

func validateRunForkReplyProjection(plan runfork.RunForkPlan, childRunID string, source replycontext.Record) error {
	for _, runID := range []string{plan.SourceRunID, childRunID} {
		id, err := uuid.Parse(runID)
		if err != nil || id == uuid.Nil || id.String() != runID {
			return fmt.Errorf("fork reply projection requires exact nonzero run identities")
		}
	}
	if childRunID == plan.SourceRunID || source.RunID != plan.SourceRunID {
		return fmt.Errorf("fork reply projection requires distinct source and child ownership")
	}
	if err := plan.ForkPoint.Validate(); err != nil {
		return err
	}
	if err := validateRunForkReplyCanonicalRecord(source); err != nil {
		return err
	}
	history, admitted := plan.HistoricalEventIDs(plan.ForkPoint.Revision)
	if !admitted {
		return fmt.Errorf("fork reply projection requires exact fixed-cut event membership")
	}
	for _, eventID := range []string{source.RequestEventID, source.AcceptedReplyEventID} {
		if eventID == "" {
			continue
		}
		id, err := uuid.Parse(eventID)
		if err != nil || id == uuid.Nil || id.String() != eventID || !slices.Contains(history, eventID) {
			return fmt.Errorf("fork reply event reference is outside exact fixed-cut history")
		}
	}
	matched := false
	for _, captured := range plan.ReplyContexts {
		if captured.ID != source.ID {
			continue
		}
		if matched || !sameRunForkReplyRecord(captured, source) {
			return fmt.Errorf("fork reply projection has contradictory captured reply authority")
		}
		matched = true
	}
	if !matched {
		return fmt.Errorf("fork reply projection requires captured reply authority")
	}
	return nil
}

func projectRunForkReplyOrigin(plan runfork.RunForkPlan, childRunID string, source events.RouteIdentity) (events.RouteIdentity, error) {
	var entity *runfork.RunForkEntityState
	for i := range plan.Entities {
		if plan.Entities[i].EntityID != source.EntityID {
			continue
		}
		if entity != nil {
			return events.RouteIdentity{}, fmt.Errorf("fork reply origin repeats its captured construction owner")
		}
		entity = &plan.Entities[i]
	}
	if entity == nil || entity.MaterializationMetadata == nil {
		return events.RouteIdentity{}, fmt.Errorf("fork reply origin lacks captured construction authority")
	}
	metadata := entity.MaterializationMetadata
	receipt, err := pipeline.DecodeStoredFlowConstructionReceipt(metadata.InitialMaterialization,
		plan.SourceRunID, entity.EntityID, metadata.FlowInstance, metadata.FlowTemplate)
	if err != nil {
		return events.RouteIdentity{}, err
	}
	identity := receipt.Identity
	if identity.ScopeKey != source.FlowID || identity.InstancePath != source.FlowInstance || identity.EntityID != source.EntityID {
		return events.RouteIdentity{}, fmt.Errorf("fork reply origin contradicts exact captured construction identity")
	}
	child, err := runfork.ProjectConstructionIdentity(plan.SourceRunID, childRunID, identity)
	if err != nil {
		return events.RouteIdentity{}, err
	}
	return events.RouteIdentity{FlowID: child.ScopeKey, FlowInstance: child.InstancePath, EntityID: child.EntityID}, nil
}

func projectRunForkReplyContexts(plan runfork.RunForkPlan, childRunID string) ([]replycontext.Record, error) {
	projected := make([]replycontext.Record, 0, len(plan.ReplyContexts))
	for _, source := range plan.ReplyContexts {
		child, err := projectRunForkReplyContext(plan, childRunID, source)
		if err != nil {
			return nil, err
		}
		projected = append(projected, child)
	}
	return projected, nil
}

func materializeRunForkReplyContexts(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, owner runForkReplyContextOwner, plan runfork.RunForkPlan, childRunID string, postgres bool, admission runfork.RunForkReplayResumeAdmission) (runfork.RunForkReplayResumeAdmission, error) {
	if err := ctx.Err(); err != nil {
		return admission, err
	}
	if tx == nil || attempt == nil || owner == nil {
		return admission, fmt.Errorf("fork reply materialization requires the existing transaction, attempt and reply owner")
	}
	records, err := projectRunForkReplyContexts(plan, childRunID)
	if err != nil {
		return admission, err
	}
	for _, record := range records {
		if err := requireRunForkReplyEvents(ctx, tx, postgres, plan, record); err != nil {
			return admission, err
		}
	}
	for _, record := range records {
		if err := owner.CreateWithinTransaction(ctx, attempt, record); err != nil {
			return admission, err
		}
	}
	return requireMaterializedRunForkReplyContexts(ctx, tx, owner, plan, childRunID, postgres, admission)
}

func requireMaterializedRunForkReplyContexts(ctx context.Context, tx *sql.Tx, owner runForkReplyContextOwner, plan runfork.RunForkPlan, childRunID string, postgres bool, admission runfork.RunForkReplayResumeAdmission) (runfork.RunForkReplayResumeAdmission, error) {
	if err := ctx.Err(); err != nil {
		return admission, err
	}
	if tx == nil || owner == nil {
		return admission, fmt.Errorf("fork reply readback requires the existing transaction and reply owner")
	}
	records, err := projectRunForkReplyContexts(plan, childRunID)
	if err != nil {
		return admission, err
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM reply_contexts WHERE run_id=$1`, childRunID).Scan(&count); err != nil {
		return admission, err
	}
	if count != len(records) {
		return admission, fmt.Errorf("fork reply inventory differs from exact captured reply authority")
	}
	for _, expected := range records {
		if err := requireRunForkReplyEvents(ctx, tx, postgres, plan, expected); err != nil {
			return admission, err
		}
		actual, err := owner.LoadWithinTransaction(ctx, tx, expected.ID)
		if err != nil {
			return admission, err
		}
		if err := validateRunForkReplyCanonicalRecord(actual); err != nil {
			return admission, err
		}
		if !sameRunForkReplyRecord(actual, expected) {
			return admission, fmt.Errorf("materialized reply context changed immutable or fixed-cut lifecycle facts")
		}
	}
	return dischargeMaterializedRunForkReplyAdmission(admission, records)
}

func requireRunForkReplyEvents(ctx context.Context, tx *sql.Tx, postgres bool, plan runfork.RunForkPlan, record replycontext.Record) error {
	var source replycontext.Record
	for _, captured := range plan.ReplyContexts {
		if deterministicRunForkReplyContextID(record.RunID, captured.ID) == record.ID {
			source = captured
			break
		}
	}
	if source.ID == "" {
		return fmt.Errorf("fork reply event admission requires captured source authority")
	}
	request, err := loadRunForkReplyChildEvent(ctx, tx, postgres, record.RunID, record.RequestEventID)
	if err != nil {
		return err
	}
	if err := validateRunForkReplyChildEventLineage(request, plan.SourceRunID, source.RequestEventID); err != nil {
		return err
	}
	if err := validateRunForkReplyChildProducer(request, record.RequesterFlowID, &record.Origin); err != nil {
		return err
	}
	if record.State == replycontext.StateTerminal {
		reply, err := loadRunForkReplyChildEvent(ctx, tx, postgres, record.RunID, record.AcceptedReplyEventID)
		if err != nil {
			return err
		}
		if err := validateRunForkReplyChildEventLineage(reply, plan.SourceRunID, source.AcceptedReplyEventID); err != nil {
			return err
		}
		if err := validateRunForkReplyChildProducer(reply, record.ProviderFlowID, nil); err != nil {
			return err
		}
	}
	return nil
}

func validateRunForkReplyChildEventLineage(event events.Event, sourceRunID, sourceEventID string) error {
	lineage, present := event.SelectedForkLineage()
	if !present || lineage.SourceRunID() != sourceRunID || lineage.SourceEventID() != sourceEventID {
		return fmt.Errorf("fork reply event lacks its exact projected source request/reply lineage")
	}
	return nil
}

func loadRunForkReplyChildEvent(ctx context.Context, tx *sql.Tx, postgres bool, childRunID, eventID string) (events.Event, error) {
	if postgres {
		return loadRunForkReplaySourceEvent(ctx, tx, childRunID, eventID)
	}
	return loadSQLiteRunForkReplaySourceEvent(ctx, tx, childRunID, eventID)
}

func validateRunForkReplyChildProducer(event events.Event, flowID string, origin *events.RouteIdentity) error {
	source := event.RoutingSource()
	if source.Empty() {
		return fmt.Errorf("fork reply event contradicts its captured producer declaration")
	}
	route := source.Route()
	if source.Kind() == events.RoutingSourceRoot {
		if flowID != "." || route.EntityID != event.RunID() {
			return fmt.Errorf("fork reply event contradicts its captured producer declaration")
		}
		if origin != nil && (origin.FlowID != "." || origin.EntityID != event.RunID() || origin.FlowInstance != event.RunID() || route.EntityID != origin.EntityID) {
			return fmt.Errorf("fork reply request contradicts its exact root origin")
		}
		return nil
	}
	if route.FlowID != flowID {
		return fmt.Errorf("fork reply event contradicts its captured producer declaration")
	}
	if origin != nil && route != *origin {
		return fmt.Errorf("fork reply request contradicts its exact captured origin")
	}
	return nil
}

func sameRunForkReplyRecord(left, right replycontext.Record) bool {
	return left.SameIdentity(right) && left.State == right.State && left.AcceptedReplyEventID == right.AcceptedReplyEventID &&
		left.CreatedAt.Equal(right.CreatedAt) && left.UpdatedAt.Equal(right.UpdatedAt) && reflect.DeepEqual(left.TerminalAt, right.TerminalAt)
}

func validateRunForkReplyCanonicalRecord(record replycontext.Record) error {
	if err := record.Validate(); err != nil {
		return err
	}
	if !reflect.DeepEqual(record, record.Normalized()) || record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() ||
		(record.TerminalAt != nil && record.TerminalAt.IsZero()) {
		return fmt.Errorf("fork reply projection/readback requires canonical captured lifecycle facts")
	}
	return nil
}
