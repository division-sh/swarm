package authoractivity

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// AdmitDraft validates and completes one immutable story fact before it
// crosses into a selected-store operation. Persistence adapters never infer
// scope or repair an incomplete draft.
func AdmitDraft(ctx context.Context, draft Draft) (Draft, error) {
	draft = cloneDraft(draft)
	if draft.Version == 0 {
		draft.Version = Version
	}
	if draft.OccurrenceID == "" {
		draft.OccurrenceID = uuid.NewString()
	}
	scope, err := scopeForDraft(ctx, draft.Kind, draft.Transition, draft.Scope)
	if err != nil {
		return Draft{}, err
	}
	draft.Scope = scope
	draft.AuthorSafeSummary, err = NormalizeAuthorSafeSummary(draft.AuthorSafeSummary)
	if err != nil {
		return Draft{}, fmt.Errorf("normalize author activity summary: %w", err)
	}
	if err := ValidateDraft(draft); err != nil {
		return Draft{}, err
	}
	return draft, nil
}

// DraftsEqual compares the complete immutable occurrence projection. It is
// exported for private selected-store duplicate validation only.
func DraftsEqual(left, right Draft) bool {
	return draftsEqual(left, right)
}

// DraftFromOccurrence projects a persisted occurrence back to the immutable
// command shape used for exact duplicate comparison.
func DraftFromOccurrence(occurrence Occurrence) Draft {
	return Draft{
		OccurrenceID: occurrence.OccurrenceID,
		Kind:         occurrence.Kind, Version: occurrence.Version, Transition: occurrence.Transition,
		SourceOwner: occurrence.SourceOwner, SourceIdentity: occurrence.SourceIdentity, DedupKey: occurrence.DedupKey,
		OccurredAt: occurrence.OccurredAt, RunID: occurrence.RunID, EntityID: occurrence.EntityID,
		AgentID: occurrence.AgentID, FlowID: occurrence.FlowID, Scope: occurrence.Scope,
		AuthorSafeSummary: occurrence.AuthorSafeSummary, Projection: occurrence.Projection,
		Failure: occurrence.Failure,
	}
}
