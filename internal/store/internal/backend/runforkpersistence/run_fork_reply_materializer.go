package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"

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

// Only an exact captured row can name a child continuation. The reference is
// projection evidence, not permission to reply or execute against that row.
func projectRunForkReplyReference(sourceRunID, childRunID string, captured []replycontext.Record, replyID string) (string, error) {
	if replyID == "" {
		return "", nil
	}
	if sourceRunID == "" || childRunID == "" || sourceRunID == childRunID {
		return "", fmt.Errorf("reply carrier requires distinct captured source and child ownership")
	}
	found := false
	for _, record := range captured {
		if record.ID != replyID {
			continue
		}
		if found || record.RunID != sourceRunID || validateRunForkReplyCanonicalRecord(record) != nil {
			return "", fmt.Errorf("reply carrier has contradictory captured reply evidence")
		}
		found = true
	}
	if !found {
		return "", fmt.Errorf("reply carrier has no fixed-cut reply evidence")
	}
	return deterministicRunForkReplyContextID(childRunID, replyID), nil
}

// This checks only whether this one blocker has a complete projection. Actual
// admission is discharged later, after the child rows are written and read back.
func runForkReplyProjectionBlockers(plan runfork.RunForkPlan, childRunID string, blockers []runfork.RunForkUnsupportedBlocker) ([]runfork.RunForkUnsupportedBlocker, error) {
	records, err := projectRunForkReplyContexts(plan, childRunID)
	if err != nil {
		return nil, err
	}
	hasOpen := false
	for _, record := range records {
		hasOpen = hasOpen || record.State == replycontext.StateOpen
	}
	filtered := make([]runfork.RunForkUnsupportedBlocker, 0, len(blockers))
	for _, blocker := range blockers {
		if blocker.Code == runfork.RunForkBlockerOpenReplyContextUnsupported && hasOpen {
			continue
		}
		filtered = append(filtered, blocker)
	}
	return filtered, nil
}

func requireMaterializedRunForkReplyPlan(ctx context.Context, tx *sql.Tx, owner runForkReplyContextOwner, plan runfork.RunForkPlan, childRunID string, postgres bool) (runfork.RunForkPlan, error) {
	admission, err := requireMaterializedRunForkReplyContexts(ctx, tx, owner, plan, childRunID, postgres, plan.ReplayResumeAdmission)
	if err != nil {
		return plan, err
	}
	plan.ReplayResumeAdmission = admission
	plan.ExecutionReady = admission.StateOnlyExecutionReady || admission.DeliveryEventReplayReady
	plan.UnsupportedBlockers = admission.UnsupportedBlockers
	plan.UnsupportedBlockerCount = len(admission.UnsupportedBlockers)
	return plan, nil
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
	if source.EnclosingReplyContextID != "" {
		if _, err := capturedRunForkEnclosingReply(plan, source); err != nil {
			return replycontext.Record{}, err
		}
		child.EnclosingReplyContextID = deterministicRunForkReplyContextID(childRunID, source.EnclosingReplyContextID)
	}
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
	_, admitted := plan.HistoricalEventIDs(plan.ForkPoint.Revision)
	if !admitted {
		return fmt.Errorf("fork reply projection requires admitted fixed-cut history")
	}
	for _, eventID := range []string{source.RequestEventID, source.AcceptedReplyEventID} {
		if eventID == "" {
			continue
		}
		id, err := uuid.Parse(eventID)
		if err != nil || id == uuid.Nil || id.String() != eventID {
			return fmt.Errorf("fork reply requires exact logical event coordinates")
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
	ordered, err := orderRunForkReplyContexts(plan)
	if err != nil {
		return nil, err
	}
	for _, source := range ordered {
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
		if err := owner.CreateWithinTransaction(ctx, attempt, record); err != nil {
			return admission, err
		}
	}
	return requireMaterializedRunForkReplyContexts(ctx, tx, owner, plan, childRunID, postgres, admission)
}

func requireMaterializedRunForkReplyContexts(ctx context.Context, tx *sql.Tx, owner runForkReplyContextOwner, plan runfork.RunForkPlan, childRunID string, postgres bool, admission runfork.RunForkReplayResumeAdmission) (runfork.RunForkReplayResumeAdmission, error) {
	return requireRunForkReplyContexts(ctx, tx, owner, plan, childRunID, admission, false, time.Time{})
}

func requireContinuingRunForkReplyContexts(ctx context.Context, tx *sql.Tx, owner runForkReplyContextOwner, plan runfork.RunForkPlan, childRunID string, bornAt time.Time, admission runfork.RunForkReplayResumeAdmission) (runfork.RunForkReplayResumeAdmission, error) {
	return requireRunForkReplyContexts(ctx, tx, owner, plan, childRunID, admission, true, bornAt)
}

func requireRunForkReplyContexts(ctx context.Context, tx *sql.Tx, owner runForkReplyContextOwner, plan runfork.RunForkPlan, childRunID string, admission runfork.RunForkReplayResumeAdmission, continuing bool, bornAt time.Time) (runfork.RunForkReplayResumeAdmission, error) {
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
	if !continuing {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM reply_contexts WHERE run_id=$1`, childRunID).Scan(&count); err != nil {
			return admission, err
		}
		if count != len(records) {
			return admission, fmt.Errorf("fork reply inventory differs from exact captured reply authority")
		}
	}
	for _, expected := range records {
		actual, err := owner.LoadWithinTransaction(ctx, tx, expected.ID)
		if err != nil {
			return admission, err
		}
		if err := validateRunForkReplyCanonicalRecord(actual); err != nil {
			return admission, err
		}
		if continuing {
			if err := actual.ValidateContinuationFrom(expected, bornAt); err != nil {
				return admission, err
			}
		} else if !sameRunForkReplyRecord(actual, expected) {
			return admission, fmt.Errorf("materialized reply context changed immutable or fixed-cut lifecycle facts")
		}
	}
	// The source-open fact is discharged by exact continuation evidence even
	// when the child has since settled it. New child requests are not inherited
	// evidence and never authorize or replace these captured obligations.
	return dischargeMaterializedRunForkReplyAdmission(admission, records)
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
	for _, value := range []string{record.RunID, record.RequestEventID, record.AcceptedReplyEventID, record.SourceRequestEventID, record.SourceRunID} {
		if value == "" {
			continue
		}
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return fmt.Errorf("fork reply requires exact original evidence coordinates")
		}
	}
	return nil
}
