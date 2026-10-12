package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func historicalReplyReturnPublications(ctx context.Context, tx *sql.Tx, snapshot *runForkRevisionSnapshot) (map[string]replycontext.Record, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("reply return correspondence lacks its fixed cut")
	}
	a := runForkSourceStateAdmission{snapshot: snapshot}
	rows := make(map[string]replycontext.Record)
	for _, event := range snapshot.Events {
		row, err := a.capturedReplyReturn(ctx, tx, event.EventID, make(map[string]bool))
		if err != nil {
			return nil, err
		}
		if row != nil {
			rows[event.EventID] = row.Normalized()
		}
	}
	return rows, nil
}

// A consumed reply reference is no longer on the output event. Only the native
// cut can associate that accepted occurrence with the context owning its return.
func (a runForkSourceStateAdmission) capturedReplyReturn(ctx context.Context, tx *sql.Tx, eventID string, visiting map[string]bool) (*replycontext.Record, error) {
	if len(a.snapshot.ReplyContexts) == 0 {
		return nil, nil
	}
	key := a.snapshot.RunID + "/" + eventID
	if visiting[key] {
		return nil, fmt.Errorf("retained reply return lineage contains a cycle")
	}
	visiting[key] = true
	defer delete(visiting, key)
	cut, err := newRunForkReplayCut(a.snapshot)
	if err != nil {
		return nil, err
	}
	event, err := cut.event(eventID)
	if err != nil {
		return nil, err
	}
	if _, selected := event.SelectedForkLineage(); selected {
		return a.inheritedReplyReturn(ctx, tx, event, visiting)
	}
	var matched *replycontext.Record
	for _, captured := range a.snapshot.ReplyContexts {
		record := captured.Record
		if record.State != replycontext.StateTerminal || record.AcceptedReplyEventID != event.ID() {
			continue
		}
		if matched != nil || record.RunID != a.snapshot.RunID {
			return nil, fmt.Errorf("accepted reply has ambiguous or foreign context ownership")
		}
		if err := record.ValidateContinuationFrom(record, record.CreatedAt); err != nil {
			return nil, err
		}
		matched = &record
	}
	return matched, nil
}

func (a runForkSourceStateAdmission) inheritedReplyReturn(ctx context.Context, tx *sql.Tx, event events.Event, visiting map[string]bool) (*replycontext.Record, error) {
	if tx == nil {
		return nil, fmt.Errorf("inherited reply return requires its native retained binding")
	}
	lineage, _ := event.SelectedForkLineage()
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, a.snapshot.RunID)
	if err != nil {
		return nil, err
	}
	if binding.SourceRunID != lineage.SourceRunID() {
		return nil, fmt.Errorf("inherited reply return has foreign source binding")
	}
	if binding.ForkPoint.Kind == runfork.RunForkPointRunStart {
		return nil, nil
	}
	parent, err := loadRunForkPointSnapshot(ctx, tx, binding.SourceRunID, binding.ForkPoint)
	if err != nil {
		return nil, err
	}
	parentAdmission := runForkSourceStateAdmission{snapshot: parent, point: binding.ForkPoint, forkRunID: binding.ForkRunID, postgres: a.postgres}
	record, err := parentAdmission.capturedReplyReturn(ctx, tx, lineage.SourceEventID(), visiting)
	if err != nil || record == nil {
		return nil, err
	}
	parentCut, err := newRunForkReplayCut(parent)
	if err != nil {
		return nil, err
	}
	previous, err := parentCut.event(lineage.SourceEventID())
	if err != nil {
		return nil, err
	}
	if err := requireSelectedPublicationCutLineage(ctx, tx, binding, event, previous, nil); err != nil {
		return nil, err
	}
	projected, err := projectRunForkCapturedReplies(parent, binding.ForkPoint, binding.ForkRunID, []replycontext.Record{*record})
	if err != nil {
		return nil, err
	}
	var actual *replycontext.Record
	for _, captured := range a.snapshot.ReplyContexts {
		if captured.Record.ID != projected[0].ID {
			continue
		}
		if actual != nil || !sameRunForkReplyRecord(captured.Record, projected[0]) {
			return nil, fmt.Errorf("inherited reply return changed its captured own context")
		}
		row := captured.Record
		actual = &row
	}
	if actual == nil {
		return nil, fmt.Errorf("inherited reply return lacks its captured own context")
	}
	return actual, nil
}

func (a runForkSourceStateAdmission) projectReplyReturn(ctx context.Context, tx *sql.Tx, eventID string) (*replycontext.Record, error) {
	if a.firstTurn != nil {
		original, present := a.firstTurn.Event()
		if present && original.ID() == eventID {
			return nil, nil
		}
	}
	record, err := a.capturedReplyReturn(ctx, tx, eventID, make(map[string]bool))
	if err != nil || record == nil {
		return nil, err
	}
	projected, err := projectRunForkCapturedReplies(a.snapshot, a.point, a.forkRunID, []replycontext.Record{*record})
	if err != nil {
		return nil, err
	}
	return &projected[0], nil
}

func projectReplyReturnRoutes(snapshot *runForkRevisionSnapshot, eventID, child string) ([]events.DeliveryRoute, error) {
	cut, err := newRunForkReplayCut(snapshot)
	if err != nil {
		return nil, err
	}
	event, err := cut.event(eventID)
	if err != nil {
		return nil, err
	}
	var routes []events.DeliveryRoute
	for _, captured := range snapshot.Deliveries {
		delivery := captured.Snapshot
		if delivery.EventID != eventID || delivery.Terminal() {
			continue
		}
		route, err := projectSelectedFanOutRecipient(snapshot, event, delivery, child)
		if err != nil {
			return nil, err
		}
		routes = append(routes, route)
	}
	return routes, nil
}

func requireRunForkReplyReturnEvidenceTx(ctx context.Context, tx *sql.Tx, lineage runfork.RunForkSelectedContractExecutionLineage, routes []events.DeliveryRoute, returns []pinrouting.ReplyReturnContinuation, postgres bool) error {
	validate, resolve := runforkrevision.ValidateCompleteSQLite, resolveSQLiteRunForkRevisionPoint
	if postgres {
		validate, resolve = runforkrevision.ValidateCompletePostgres, resolveRunForkRevisionPoint
	}
	a, err := loadRunForkSourceStateAdmission(ctx, tx, lineage.ForkRunID, validate, resolve, postgres)
	if err != nil {
		return err
	}
	if a.snapshot.RunID != lineage.SourceRunID {
		return fmt.Errorf("retained return has foreign source binding")
	}
	expected, err := a.projectReplyReturn(ctx, tx, lineage.SourceEventID)
	if err != nil {
		return err
	}
	if expected == nil {
		if len(returns) != 0 {
			return fmt.Errorf("retained return invented an accepted source obligation")
		}
		return nil
	}
	if len(returns) != 1 || returns[0].Validate() != nil || !sameRunForkReplyRecord(returns[0].Record, *expected) {
		return fmt.Errorf("retained return omitted or changed its exact captured terminal context")
	}
	source, err := a.capturedReplyReturn(ctx, tx, lineage.SourceEventID, make(map[string]bool))
	if err != nil {
		return err
	}
	sourceReturn := returns[0].Normalized()
	sourceReturn.Record = *source
	if err := requireCapturedReplyReturnClaims(a.snapshot, lineage.SourceEventID, sourceReturn); err != nil {
		return err
	}
	expectedRoutes, err := projectReplyReturnRoutes(a.snapshot, lineage.SourceEventID, lineage.ForkRunID)
	if err != nil {
		return err
	}
	if err := bindReplyReturnRecipientClaims(expectedRoutes, returns[0]); err != nil {
		return err
	}
	return requireExactRunForkRequestRoutes(expectedRoutes, routes)
}

func requireCapturedReplyReturnClaims(snapshot *runForkRevisionSnapshot, eventID string, continuation pinrouting.ReplyReturnContinuation) error {
	if err := continuation.Validate(); err != nil {
		return err
	}
	matched := false
	for _, captured := range snapshot.Deliveries {
		delivery := captured.Snapshot
		if delivery.EventID != eventID || delivery.Terminal() {
			continue
		}
		response := continuation.MatchesResponseRecipient(delivery.Route)
		claim, present, err := retainedReturnRecipientClaim(continuation, delivery.Route)
		if !present && delivery.Route.ConnectClaim.Empty() {
			continue
		}
		if err != nil || !claim.Equal(delivery.Route.ConnectClaim) {
			return fmt.Errorf("retained return changed its captured complete response claim: %v", err)
		}
		matched = matched || response
	}
	if !matched {
		return fmt.Errorf("retained return lacks its unfinished captured requester")
	}
	return nil
}

func bindReplyReturnRecipientClaims(routes []events.DeliveryRoute, continuation pinrouting.ReplyReturnContinuation) error {
	if err := continuation.Validate(); err != nil {
		return err
	}
	for i := range routes {
		claim, present, err := retainedReturnRecipientClaim(continuation, routes[i])
		if !present && routes[i].ConnectClaim.Empty() {
			continue
		}
		if err != nil || !present {
			if err == nil {
				return fmt.Errorf("retained return lacks its ordinary compiled recipient")
			}
			return err
		}
		routes[i].ConnectClaim = claim
	}
	return nil
}

func retainedReturnRecipientClaim(continuation pinrouting.ReplyReturnContinuation, route events.DeliveryRoute) (events.ConnectExecutionClaim, bool, error) {
	if continuation.MatchesResponseRecipient(route) {
		claim, err := continuation.ClaimForDelivery(route)
		return claim, true, err
	}
	return continuation.OrdinaryClaimForDelivery(route)
}
