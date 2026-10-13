package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/activityresult"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

// ValidateRecordedActivityPublication consumes the canonical activity lineage,
// not names or payload similarity. The caller supplies only facts at its cut.
func ValidateRecordedActivityPublication(request, result events.Event, journalResultID string, journalPayload json.RawMessage) error {
	intent, err := activityIntentFromRequestEvent(request)
	if err != nil {
		return err
	}
	_, selected := request.SelectedForkLineage()
	if intent.SourceRunID != request.RunID() || (selected && intent.SourceEventID != request.ID()) || (!selected && activityRequestEventID(intent) != request.ID()) {
		return fmt.Errorf("recorded activity request has foreign physical or journal identity")
	}
	lineage := ActivityRequestLineage{request: request, intent: intent}
	if err := lineage.ValidateResult(result); err != nil {
		return err
	}
	physical, err := canonicaljson.Canonicalize(result.Payload())
	if err != nil {
		return err
	}
	journal, err := canonicaljson.Canonicalize(journalPayload)
	if err != nil {
		return err
	}
	if result.ID() != journalResultID || !bytes.Equal(physical, journal) {
		return fmt.Errorf("published activity result contradicts its recorded journal")
	}
	return nil
}

func ActivityRequestInputHash(request events.Event) (string, error) {
	intent, err := activityIntentFromRequestEvent(request)
	if err != nil {
		return "", err
	}
	return activityInputHash(intent.Input), nil
}

func ActivityRequestResultPublications(request events.Event) ([]activityresult.Record, error) {
	intent, err := activityIntentFromRequestEvent(request)
	if err != nil {
		return nil, err
	}
	var out []activityresult.Record
	for _, declaration := range []string{intent.SuccessEvent, intent.FailureEvent} {
		publication, err := admitActivityPublication(intent, declaration)
		if err != nil {
			return nil, err
		}
		out = append(out, activityresult.Record{EventID: publication.eventID, EventType: string(publication.eventType)})
	}
	return out, nil
}

func ValidateSelectedActivityResultPayload(request, result events.Event) error {
	lineage, err := SelectedActivityRequestLineage(request)
	if err != nil {
		return err
	}
	if result.RunID() != request.RunID() || result.RoutingSource() != request.RoutingSource() || result.ExecutionMode() != request.ExecutionMode() {
		return fmt.Errorf("selected activity result has foreign request coordinates")
	}
	return lineage.ValidateResultPayload(result)
}

func selectedActivityInputPublication(ctx context.Context, intent runtimeengine.ActivityIntent) (activityresult.PublishedContinuation, bool, error) {
	publication, found := activityresult.PublishedContinuationFromContext(ctx, activityRequestEventID(intent))
	if !found {
		return activityresult.PublishedContinuation{}, false, nil
	}
	if err := ctx.Err(); err != nil {
		return activityresult.PublishedContinuation{}, false, err
	}
	authority, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok || authority.Kind != runtimeeffects.AuthoritySelectedContractFork || authority.SelectedFork.ForkRunID != intent.SourceRunID {
		return activityresult.PublishedContinuation{}, false, fmt.Errorf("published activity accounting is outside its current child scope")
	}
	if err := publication.RequireInput(activityRequestEventID(intent), intent.SourceRunID, activityInputHash(intent.Input), intent.ExecutionMode, intent.EffectClass); err != nil {
		return activityresult.PublishedContinuation{}, false, err
	}
	return publication, true, nil
}

func WithSelectedActivityPublications(ctx context.Context, childRun string, publications []activityresult.PublishedContinuation) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	authority, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok || authority.Kind != runtimeeffects.AuthoritySelectedContractFork || authority.SelectedFork.ForkRunID != childRun {
		return nil, fmt.Errorf("selected activity accounting requires its current child execution scope")
	}
	inventory, err := activityresult.NewPublishedInventory(childRun, publications)
	if err != nil {
		return nil, err
	}
	return inventory.Bind(ctx)
}

func selectedActivityPublicationOwnsResult(ctx context.Context, intent runtimeengine.ActivityIntent, record ActivityAttemptRecord) (bool, error) {
	publication, found, err := selectedActivityInputPublication(ctx, intent)
	if err != nil || !found {
		return false, err
	}
	if err := ValidateActivityAttemptTerminal(record); err != nil {
		return false, err
	}
	if err := ValidateActivityAttemptClaimIdentity(record, activityAttemptStartRecord(intent, activityInputHash(intent.Input))); err != nil {
		return false, err
	}
	if record.Status == ActivityAttemptStatusUncertain || record.CompletedAt == nil {
		return false, fmt.Errorf("published activity accounting requires unambiguous completed evidence")
	}
	payload, err := canonicaljson.MarshalPreservingNumberKinds(record.ResultPayload)
	if err != nil {
		return false, err
	}
	return true, publication.RequireJournal(record.RequestEventID, record.ResultEventID, record.ResultEventType, payload,
		record.ExecutionMode, record.InputHash, record.ReplyContextID, *record.CompletedAt)
}
