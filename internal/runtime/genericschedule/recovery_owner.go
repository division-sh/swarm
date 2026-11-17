package genericschedule

import (
	"context"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
)

var errInstanceExecutionOwner = errors.New("instance schedule requires its runtime work occurrence and exact run")

func validateInstanceExecutionOwner(ctx context.Context, command AdmissionCommand) error {
	if command.OwnerKind != OwnerInstance {
		return nil
	}
	owner, _ := worklifetime.OccurrenceFromContext(ctx)
	if owner == nil || correlation.RunIDFromContext(ctx) != command.RunID {
		return errInstanceExecutionOwner
	}
	return nil
}

func instanceExecutionLease(ctx context.Context, command AdmissionCommand) (*worklifetime.Lease, error) {
	if err := validateInstanceExecutionOwner(ctx, command); err != nil {
		return nil, err
	}
	if command.OwnerKind != OwnerInstance {
		return nil, nil
	}
	owner, _ := worklifetime.OccurrenceFromContext(ctx)
	return owner.Begin(ctx)
}

// An idle generic clock is standing custody. Only its firing callback admits
// finite work for a run, so a selected fork joins started work, not its timer.
func firingExecutionLease(ctx context.Context, command AdmissionCommand) (*worklifetime.Lease, error) {
	if command.OwnerKind == OwnerInstance {
		return instanceExecutionLease(ctx, command)
	}
	if command.RunID == "" {
		return nil, nil
	}
	owner, ok := worklifetime.OccurrenceFromContext(ctx)
	if !ok || owner == nil {
		return nil, errors.New("run-scoped schedule fire requires its runtime work occurrence")
	}
	if existing := correlation.RunIDFromContext(ctx); existing != "" && existing != command.RunID {
		return nil, errors.New("run-scoped schedule fire has a foreign run")
	}
	return owner.Begin(correlation.WithRunID(ctx, command.RunID))
}

func recoveryOwnerRefused(err error) bool {
	return errors.Is(err, errInstanceExecutionOwner) || errors.Is(err, worklifetime.ErrRetired) || errors.Is(err, worklifetime.ErrAdmissionFenced)
}

func (l *Lifecycle) queueProjectionRecovery(ctx context.Context, activationID string, projectionErr error) (bool, error) {
	if recoveryOwnerRefused(projectionErr) {
		return false, projectionErr
	}
	queued, ownerErr := l.startRecovery(ctx, activationID)
	return queued, errors.Join(projectionErr, ownerErr)
}

func (l *Lifecycle) runProjectionRecovery(ctx context.Context, activationID string) {
	delay := 50 * time.Millisecond
	for {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		err := l.ReconcileWakeup(ctx, activationID)
		if err == nil {
			return
		}
		l.log(ctx, "recovery", activationID, err)
		if ctx.Err() != nil || recoveryOwnerRefused(err) {
			return
		}
		if delay < time.Second {
			delay *= 2
		}
	}
}
