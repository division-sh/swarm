package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

// A physical selected publication and its projected logical request have
// different IDs. Only the admitted immediate-source event supplies this link.
func capturedRunForkRequestReplies(snapshot *runForkRevisionSnapshot, eventID string) ([]replycontext.Record, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("inherited request lacks its fixed cut")
	}
	if len(snapshot.ReplyContexts) == 0 {
		return nil, nil
	}
	cut, err := newRunForkReplayCut(snapshot)
	if err != nil {
		return nil, err
	}
	event, err := cut.event(eventID)
	if err != nil {
		return nil, err
	}
	var records []replycontext.Record
	for _, captured := range snapshot.ReplyContexts {
		record := captured.Record
		if !capturedReplyRequestPublication(cut, event, record) {
			continue
		}
		if record.RunID != snapshot.RunID || record.State != replycontext.StateOpen {
			return nil, fmt.Errorf("inherited request has a foreign or settled captured reply")
		}
		records = append(records, record)
	}
	return records, nil
}

func capturedReplyRequestPublication(cut runForkReplayCut, event events.Event, record replycontext.Record) bool {
	if _, selected := event.SelectedForkLineage(); !selected {
		return record.RequestEventID == event.ID()
	}
	// Recursive physical occurrences do not alias recursively projected logical
	// request IDs. The immediate cut's admitted delivery retains the exact row.
	path := record.Origin.FlowInstance
	if record.RequesterFlowID == "." {
		path = "."
	}
	name := eventidentity.ExternalizeForFlow(path, []string{record.RequestOutputPin}, record.RequestOutputPin)
	if string(event.Type()) != name {
		return false
	}
	for _, captured := range cut.deliveries {
		if captured.Snapshot.EventID == event.ID() && captured.Snapshot.Route.Context.ReplyContextID() == record.ID {
			return true
		}
	}
	return false
}

func (a runForkSourceStateAdmission) projectRequestReplies(eventID string) ([]replycontext.Record, error) {
	if a.firstTurn != nil {
		original, present := a.firstTurn.Event()
		if present && original.ID() == eventID {
			// The start cut excludes the original first publication. It cannot
			// supply an already-published request obligation for that new work.
			return nil, nil
		}
	}
	return projectRunForkRequestReplies(a.snapshot, a.point, eventID, a.forkRunID)
}

func requireRunForkRequestReplyEvidenceTx(ctx context.Context, tx *sql.Tx, lineage runfork.RunForkSelectedContractExecutionLineage, routes []events.DeliveryRoute, replies []pinrouting.ReplyRequestContinuation, postgres bool) error {
	validate, resolve := runforkrevision.ValidateCompleteSQLite, resolveSQLiteRunForkRevisionPoint
	if postgres {
		validate, resolve = runforkrevision.ValidateCompletePostgres, resolveRunForkRevisionPoint
	}
	admission, err := loadRunForkSourceStateAdmission(ctx, tx, lineage.ForkRunID, validate, resolve, postgres)
	if err != nil {
		return err
	}
	if admission.snapshot.RunID != lineage.SourceRunID {
		return fmt.Errorf("request continuation has foreign source binding")
	}
	expected, err := admission.projectRequestReplies(lineage.SourceEventID)
	if err != nil {
		return err
	}
	if err := requireRunForkRequestReplyEvidence(expected, replies, routes); err != nil {
		return err
	}
	if len(expected) == 0 {
		return nil
	}
	return requireRunForkRequestRecipientPartition(admission.snapshot, lineage.SourceEventID, lineage.ForkRunID, replies, routes)
}

func requireRunForkRequestReplyEvidence(expected []replycontext.Record, replies []pinrouting.ReplyRequestContinuation, routes []events.DeliveryRoute) error {
	if len(expected) != len(replies) {
		return fmt.Errorf("request continuation omitted or invented a captured obligation")
	}
	byID := make(map[string]replycontext.Record, len(expected))
	for _, record := range expected {
		if _, duplicate := byID[record.ID]; duplicate {
			return fmt.Errorf("request continuation has duplicate captured obligations")
		}
		byID[record.ID] = record
	}
	for _, continuation := range replies {
		if err := continuation.Validate(); err != nil {
			return err
		}
		actual := continuation.Record
		retained, found := byID[actual.ID]
		if !found || actual.State != replycontext.StateOpen {
			return fmt.Errorf("request continuation changed its projected obligation")
		}
		if err := actual.ValidateContinuationFrom(retained, retained.CreatedAt); err != nil {
			return err
		}
		if !runForkReplyHasRecipient(actual.ID, routes) {
			return fmt.Errorf("request continuation omitted its projected recipient context")
		}
		delete(byID, actual.ID)
	}
	return nil
}

// The original complete claim admits the supplied compiled plan. That same
// claim owner computes the projected child claim; no digest field is discarded.
func requireRunForkRequestRecipientPartition(snapshot *runForkRevisionSnapshot, eventID, child string, replies []pinrouting.ReplyRequestContinuation, actual []events.DeliveryRoute) error {
	cut, err := newRunForkReplayCut(snapshot)
	if err != nil {
		return err
	}
	event, err := cut.event(eventID)
	if err != nil {
		return err
	}
	var expected []events.DeliveryRoute
	for _, captured := range cut.deliveries {
		delivery := captured.Snapshot
		if delivery.EventID != eventID || delivery.Terminal() {
			continue
		}
		projected, err := projectSelectedFanOutRecipient(snapshot, event, delivery, child)
		if err != nil {
			return err
		}
		if err := bindRunForkRequestRecipientClaim(delivery.Route, &projected, replies); err != nil {
			return err
		}
		expected = append(expected, projected)
	}
	return requireExactRunForkRequestRoutes(expected, actual)
}

func bindRunForkRequestRecipientClaim(source events.DeliveryRoute, projected *events.DeliveryRoute, replies []pinrouting.ReplyRequestContinuation) error {
	for _, reply := range replies {
		if projected.Context.ReplyContextID() != reply.Record.ID {
			continue
		}
		original, err := reply.ClaimForDelivery(source)
		if err != nil || !original.Equal(source.ConnectClaim) {
			return fmt.Errorf("continued request changed its captured compiled claim: %v", err)
		}
		projected.ConnectClaim, err = reply.ClaimForDelivery(*projected)
		return err
	}
	return fmt.Errorf("continued request recipient lacks its exact projected reply binding")
}

func requireExactRunForkRequestRoutes(expected, actual []events.DeliveryRoute) error {
	if len(expected) == 0 || len(expected) != len(actual) {
		return fmt.Errorf("continued request changed its complete unfinished recipient set")
	}
	seen := make(map[events.DeliveryRouteIdentity]bool, len(actual))
	for _, route := range actual {
		identity, err := route.Identity()
		if err != nil || seen[identity] {
			return fmt.Errorf("continued request has an invalid or duplicate child recipient: %v", err)
		}
		seen[identity] = true
	}
	for _, route := range expected {
		identity, err := route.Identity()
		if err != nil || !seen[identity] {
			return fmt.Errorf("continued request omitted or substituted an exact child recipient: %v", err)
		}
		delete(seen, identity)
	}
	return nil
}

func runForkReplyHasRecipient(replyID string, routes []events.DeliveryRoute) bool {
	for _, route := range routes {
		if route.Context.ReplyContextID() == replyID {
			return true
		}
	}
	return false
}

func projectRunForkRequestReplies(snapshot *runForkRevisionSnapshot, point runfork.RunForkPoint, eventID, child string) ([]replycontext.Record, error) {
	requested, err := capturedRunForkRequestReplies(snapshot, eventID)
	if err != nil || len(requested) == 0 {
		return nil, err
	}
	if err := point.Validate(); err != nil || point.Revision != snapshot.Revision {
		return nil, fmt.Errorf("inherited request requires its exact admitted cut: %v", err)
	}
	entities, err := loadRunForkEntityStates(snapshot)
	if err != nil {
		return nil, err
	}
	entities, _, err = attachRunForkMaterializedEntitySnapshotMetadata(snapshot, entities)
	if err != nil {
		return nil, err
	}
	plan := runfork.RunForkPlan{SourceRunID: snapshot.RunID, ForkPoint: point, Entities: entities}
	var eventIDs []string
	for _, event := range snapshot.Events {
		eventIDs = append(eventIDs, event.EventID)
	}
	plan = plan.WithHistoricalEvents(snapshot.Revision, eventIDs)
	for _, captured := range snapshot.ReplyContexts {
		plan.ReplyContexts = append(plan.ReplyContexts, captured.Record)
	}
	var projected []replycontext.Record
	for _, record := range requested {
		childRecord, err := projectRunForkReplyContext(plan, child, record)
		if err != nil {
			return nil, err
		}
		projected = append(projected, childRecord)
	}
	return projected, nil
}
