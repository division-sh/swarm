// Package mutationlogfixture exposes the selected-store mutation-log adapter
// only to tests that assemble an explicit transaction fixture.
package mutationlogfixture

import (
	"context"
	"time"

	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	privatemutationlog "github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func Insert(ctx context.Context, attempt *mutationprotocol.Attempt, runLifecycle privatemutationlog.ActiveRunSourceOwner, record runtimemutationlog.Record) error {
	return privatemutationlog.Insert(ctx, attempt, runLifecycle, record)
}

func InsertEntityStateDiff(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	runLifecycle privatemutationlog.ActiveRunSourceOwner,
	entityID string,
	before, after runtimemutationlog.EntityStateProjection,
	writer runtimemutationlog.Writer,
) error {
	return privatemutationlog.InsertEntityStateDiff(ctx, attempt, runLifecycle, entityID, before, after, writer)
}

func InsertSQLiteEntityStateDiff(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	runLifecycle privatemutationlog.ActiveRunSourceOwner,
	entityID string,
	before, after runtimemutationlog.EntityStateProjection,
	writer runtimemutationlog.Writer,
	occurredAt time.Time,
) error {
	return privatemutationlog.InsertSQLiteEntityStateDiff(ctx, attempt, runLifecycle, entityID, before, after, writer, occurredAt)
}
