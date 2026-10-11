package worklifetime

import (
	"context"
	"errors"
)

const PublicCompletionLimit = 32

var ErrPublicCompletionCapacity = errors.New("public_completion_capacity_busy")

func (p *Process) PublicCompletionCount() uint64 {
	if p == nil {
		return 0
	}
	p.occurrence.gate.mu.Lock()
	defer p.occurrence.gate.mu.Unlock()
	return p.occurrence.gate.publicCompletions
}

// BeginPublicCompletion reserves process capacity before construction or COMMIT.
// Its lease survives the HTTP waiter, but not the exact occurrence's retirement.
func BeginPublicCompletion(ctx context.Context, owner Occurrence) (*Lease, error) {
	if ctx == nil || owner == nil {
		return nil, errors.New("public completion requires a context and exact occurrence")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	process := publicCompletionProcess(owner)
	if process == nil {
		return nil, errors.New("public completion requires its occurrence's process owner")
	}
	accepted := WithProcess(context.WithoutCancel(ctx), process)
	reservation, err := process.occurrence.gate.beginOwned(accepted, true, nil, true)
	if err != nil {
		return nil, err
	}
	lease, err := owner.Begin(accepted)
	if err != nil {
		return nil, errors.Join(err, reservation.Done())
	}
	if projected, ok := ProcessFromContext(lease.Context()); !ok || projected != process {
		return nil, errors.Join(errors.New("public completion occurrence has a foreign process companion"), lease.Done(), reservation.Done())
	}
	// Manager admission can compose another exact occurrence. Append the process
	// reservation, preserving every existing companion and its joined settlement.
	tail := lease
	for tail.parent != nil {
		tail = tail.parent
	}
	tail.parent = reservation
	return lease, nil
}

func publicCompletionProcess(owner Occurrence) *Process {
	switch typed := owner.(type) {
	case *RuntimeOccurrence:
		if typed != nil {
			return typed.process
		}
	case *SelectedForkOccurrence:
		if typed != nil {
			return typed.process
		}
	case *StandingOccurrence:
		if typed != nil && typed.occurrence != nil && typed.occurrence.parentLease != nil {
			process, _ := ProcessFromContext(typed.occurrence.parentLease.Context())
			return process
		}
	case *ManagerWorkOccurrence:
		if typed != nil && typed.manager != nil {
			return typed.manager.process
		}
	}
	return nil
}
