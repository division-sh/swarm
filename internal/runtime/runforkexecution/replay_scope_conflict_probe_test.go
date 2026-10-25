package runforkexecution

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type selectedReplayScopeConflictObservation struct {
	request       bus.CommitSelectedForkEventRequest
	err           error
	before, after map[string]storetest.SelectedForkStorageTableSnapshot
	readErr       error
}

type selectedReplayScopeConflictProbe struct {
	SelectedContractReplayPersistence
	selected    any
	publication chan selectedReplayScopeConflictObservation
}

func (p *selectedReplayScopeConflictProbe) CommitSelectedForkEvent(ctx context.Context, request bus.CommitSelectedForkEventRequest) (bus.CommittedSelectedForkEvent, error) {
	before, beforeErr := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, p.selected)
	committed, err := p.SelectedContractReplayPersistence.CommitSelectedForkEvent(ctx, request)
	after, afterErr := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, p.selected)
	p.publication <- selectedReplayScopeConflictObservation{request: request, err: err, before: before, after: after, readErr: errors.Join(beforeErr, afterErr)}
	return committed, err
}
