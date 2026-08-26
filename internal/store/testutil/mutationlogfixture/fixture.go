// Package mutationlogfixture exposes the selected-store mutation-log adapter
// only to tests that assemble an explicit transaction fixture.
package mutationlogfixture

import (
	"context"
	"database/sql"
	"fmt"

	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	privatemutationlog "github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

func Insert(ctx context.Context, tx *sql.Tx, runLifecycle privatemutationlog.ActiveRunSourceOwner, record runtimemutationlog.Record) error {
	story, ok := authoractivityfixture.Mutation(ctx)
	if !ok {
		return fmt.Errorf("test mutation log fixture requires an active author activity mutation")
	}
	return privatemutationlog.InsertWithStory(ctx, tx, runLifecycle, story, privaterunforkrevision.NewEffects(), record)
}

func InsertEntityStateDiff(
	ctx context.Context,
	tx *sql.Tx,
	runLifecycle privatemutationlog.ActiveRunSourceOwner,
	entityID string,
	before, after runtimemutationlog.EntityStateProjection,
	writer runtimemutationlog.Writer,
) error {
	story, ok := authoractivityfixture.Mutation(ctx)
	if !ok {
		return fmt.Errorf("test mutation log fixture requires an active author activity mutation")
	}
	return privatemutationlog.InsertEntityStateDiffWithStory(ctx, tx, runLifecycle, story, privaterunforkrevision.NewEffects(), entityID, before, after, writer)
}
