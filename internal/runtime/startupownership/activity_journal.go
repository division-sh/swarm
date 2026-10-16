package startupownership

import (
	"context"
	"errors"
	"reflect"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// ActivityJournalGrantBinder is supplied by the retained native session.
// Binding captures attribution; every writer still proves current permission
// inside its original native transaction.
type ActivityJournalGrantBinder interface {
	BindActivityGrant(GrantEvidence) (pipeline.ActivityAttemptJournal, error)
}

// CaptureActivityJournal binds one installed generation to its own retained
// session. Dispatch keeps this value; callbacks never look up a newer role.
func CaptureActivityJournal(ctx context.Context, grant GenerationGrant, expected GrantEvidence) (pipeline.ActivityAttemptJournal, error) {
	var original *generationGrant
	switch value := grant.(type) {
	case *generationGrant:
		original = value
	case *liveGenerationGrant:
		if value != nil {
			original = value.generationGrant
		}
	}
	if ctx == nil || original == nil || original.owner == nil {
		return nil, errors.New("activity journal requires its original installed generation grant")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	owner := original.owner
	owner.opMu.Lock()
	defer owner.opMu.Unlock()
	if err := owner.proveCurrent(ctx); err != nil {
		return nil, err
	}
	evidence, err := original.Evidence()
	if err != nil {
		return nil, err
	}
	if evidence.State != GrantAdmitted || !reflect.DeepEqual(evidence, expected) {
		return nil, errors.New("activity journal differs from the exact installed admitted generation")
	}
	if err := original.requireExecutionAuthorityLocked(ctx, evidence); err != nil {
		return nil, err
	}
	role, ok := owner.session.(interface {
		ActivityJournalBinder() (ActivityJournalGrantBinder, error)
	})
	if !ok {
		return nil, errors.New("retained process session has no activity journal binder")
	}
	binder, err := role.ActivityJournalBinder()
	if err != nil {
		return nil, err
	}
	if binder == nil {
		return nil, errors.New("retained process session returned no activity journal binder")
	}
	return binder.BindActivityGrant(evidence)
}
