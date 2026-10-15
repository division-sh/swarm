package startupownership

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
)

var errAgentRecoverySeedChanged = errors.New("agent recovery seed requires a complete fresh reconstruction after native writes join")

// AgentRecoveryWriteObserver belongs to one original native backend, including
// before process acquisition. Its zero value observes no writes. It records
// private-cache freshness only, never provider occupancy or transaction rights.
type AgentRecoveryWriteObserver struct {
	mu      sync.Mutex
	epoch   uint64
	active  uint64
	invalid bool
	process *processCapability
}

type agentRecoveryWriteObserverOwner interface {
	AgentRecoveryWriteObserver() *AgentRecoveryWriteObserver
}

// AgentRecoveryWriteObservation is retained only by the original native write
// owner until its existing transaction and cleanup join. It carries no SQL,
// process capability, commit evidence or right to begin another operation.
type AgentRecoveryWriteObservation struct {
	observer *AgentRecoveryWriteObserver
	ended    atomic.Bool
	capacity *agentRecoveryCapacityAttempt
}

// ObserveWriteStart precedes native pool/session/mutation admission. Even a
// failed or unchanged write conservatively revokes a private observation.
func (o *AgentRecoveryWriteObserver) ObserveWriteStart() *AgentRecoveryWriteObservation {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.process != nil {
		o.process.mu.Lock()
		defer o.process.mu.Unlock()
	}
	return o.startWriteLocked()
}

func (o *AgentRecoveryWriteObserver) startWriteLocked() *AgentRecoveryWriteObservation {
	if o.epoch == math.MaxUint64 || o.active == math.MaxUint64 {
		o.invalid = true
	} else {
		o.epoch++
		o.active++
	}
	if o.process != nil {
		o.process.agentRecoverySeed = nil
	}
	return &AgentRecoveryWriteObservation{observer: o}
}

// End is called only after the original COMMIT/rollback/discard/connection
// cleanup joins. It never infers rollback or changes durable claim occupancy.
func (w *AgentRecoveryWriteObservation) End() {
	if w == nil || w.observer == nil || w.ended.Swap(true) {
		return
	}
	o := w.observer
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.active == 0 {
		o.invalid = true
		return
	}
	o.active--
}

func (o *AgentRecoveryWriteObserver) bindProcess(p *processCapability) error {
	if o == nil || p == nil {
		return errors.New("agent recovery writes require the original backend observer and process")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.process != nil && o.process != p {
		o.process.mu.Lock()
		live := o.process.terminal.Cause == ""
		o.process.mu.Unlock()
		if live {
			return errors.New("agent recovery write observer is already bound to a live process")
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.terminal.Cause != "" || p.agentRecoveryWrites != nil && p.agentRecoveryWrites != o {
		return errors.New("agent recovery write observer cannot replace the original process binding")
	}
	// Do not reset epoch or active: a pre-bind native writer must still join.
	o.process = p
	p.agentRecoveryWrites = o
	return nil
}

func (o *AgentRecoveryWriteObserver) seedEpoch(p *processCapability) (uint64, error) {
	if o == nil {
		return 0, errors.New("agent recovery seed requires its original backend write observer")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.invalid || o.process != p || o.active != 0 {
		return 0, errAgentRecoverySeedChanged
	}
	return o.epoch, nil
}

func (o *AgentRecoveryWriteObserver) publishSeed(ctx context.Context, p *processCapability, epoch uint64, seed *agentRecoverySeed) error {
	if o == nil {
		return errors.New("agent recovery seed requires its original backend write observer")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.invalid || o.process != p || o.active != 0 || o.epoch != epoch {
		return errAgentRecoverySeedChanged
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// The observer -> process mutex order is also used by write revocation.
	// Neither mutex is held during native work or process opMu acquisition.
	if p.agentRecoveryWrites != o || p.terminal.Cause != "" || ctx.Err() != nil {
		return errors.Join(errors.New("agent recovery seed lost its live process before publication"), ctx.Err())
	}
	p.agentRecoverySeed = seed
	return nil
}
