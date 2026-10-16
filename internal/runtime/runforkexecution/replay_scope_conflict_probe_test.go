package runforkexecution

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/effects"
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
	SelectedContractRuntimeExecutionLifecycle
	selected            any
	publication         chan selectedReplayScopeConflictObservation
	mu                  sync.Mutex
	failure             json.RawMessage
	failureAcknowledged bool
}

func (p *selectedReplayScopeConflictProbe) CommitSelectedForkEvent(ctx context.Context, request bus.CommitSelectedForkEventRequest) (bus.CommittedSelectedForkEvent, error) {
	before, beforeErr := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, p.selected)
	committed, err := p.SelectedContractReplayPersistence.CommitSelectedForkEvent(ctx, request)
	after, afterErr := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, p.selected)
	p.publication <- selectedReplayScopeConflictObservation{request: request, err: err, before: before, after: after, readErr: errors.Join(beforeErr, afterErr)}
	return committed, err
}

func (p *selectedReplayScopeConflictProbe) FailActivatedRunForkSelectedContractRuntimeExecution(ctx context.Context, authority effects.Authority, raw json.RawMessage) (bool, error) {
	acknowledged, err := p.SelectedContractRuntimeExecutionLifecycle.FailActivatedRunForkSelectedContractRuntimeExecution(ctx, authority, raw)
	p.mu.Lock()
	p.failure, p.failureAcknowledged = append(json.RawMessage(nil), raw...), acknowledged
	p.mu.Unlock()
	return acknowledged, err
}
