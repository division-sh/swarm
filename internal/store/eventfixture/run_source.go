package eventfixture

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

// RunSourceMutation is a semantic fixture handle, not transaction access.
type RunSourceMutation = sourceadmission.FixtureSourceMutation

// RunSource pairs source admission with the fixture's registered native story.
// Existing event fixture mutations do not opt in to this lifecycle bridge.
func RunSource(ctx context.Context, dialect authoractivityfixture.Dialect) (RunSourceMutation, error) {
	mutation, ok := authoractivityfixture.Mutation(ctx)
	if !ok {
		return RunSourceMutation{}, errors.New("fixture run source requires registered native author activity")
	}
	return sourceadmission.NewFixtureSourceMutation(mutation, string(dialect))
}
