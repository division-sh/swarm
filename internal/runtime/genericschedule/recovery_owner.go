package genericschedule

import (
	"context"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

var errInstanceExecutionOwner = errors.New("instance schedule requires its exact standing work occurrence")

func validateInstanceExecutionOwner(ctx context.Context, command AdmissionCommand) error {
	if command.OwnerKind != OwnerInstance {
		return nil
	}
	owner, _ := worklifetime.OccurrenceFromContext(ctx)
	standing, ok := worklifetime.StandingProjection(owner)
	if !ok || standing.Identity().RunID != command.RunID {
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
