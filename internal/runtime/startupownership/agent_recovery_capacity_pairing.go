package startupownership

import (
	"context"
	"errors"
	"math"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
)

// ObserveSQLiteCapacityInnerStartContext retains SQLite's original physical
// write observation. Only the exact capacity invocation pairs it with its outer
// scope; a second physical attempt refuses before pool or SQL admission.
func (o *AgentRecoveryWriteObserver) ObserveSQLiteCapacityInnerStartContext(ctx context.Context) (*AgentRecoveryWriteObservation, error) {
	if ctx == nil || ctx.Value(agentRecoveryClaimContextKey{}) == nil {
		return o.ObserveWriteStart(), nil
	}
	entry, ok := ctx.Value(agentRecoveryClaimContextKey{}).(agentRecoveryClaimEntry)
	if !ok || entry.intent == nil || entry.intent.process == nil || entry.intent.life == nil || o == nil || entry.intent.observation.writes != o {
		return nil, errors.New("SQLite inner write requires its original bound invocation")
	}
	intent := entry.intent
	if intent.capacity == nil {
		return o.ObserveWriteStart(), nil
	}
	p, capacity := intent.process, intent.capacity
	o.mu.Lock()
	defer o.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := errors.Join(ctx.Err(), intent.life.Err()); err != nil {
		return nil, err
	}
	if intent.observation.authority.Backend != processbinding.SQLiteLeaseBackend || !intent.capacitySeedCurrentLocked() ||
		!capacity.outerIdentityCurrentLocked(intent.observation) || capacity.inner != nil ||
		o.epoch != capacity.epoch || o.active != 1 || o.epoch == math.MaxUint64 {
		return nil, errAgentRecoverySeedChanged
	}
	inner := o.startWriteLocked()
	inner.capacity = capacity
	capacity.inner = inner
	return inner, nil
}
