package runtime

import (
	"context"
	"errors"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
)

func (*runtimeTestRetainedSession) AdmitResetOperation(context.Context, destructivereset.Request) (destructivereset.Operation, error) {
	return destructivereset.Operation{}, errors.New("reset admission is not part of this lifecycle fixture")
}
func (*runtimeTestRetainedSession) ReadResetOperation(context.Context, string) (destructivereset.Operation, error) {
	return destructivereset.Operation{}, errors.New("reset read is not part of this lifecycle fixture")
}
func (*runtimeTestRetainedSession) PendingResetOperations(context.Context) ([]destructivereset.Operation, error) {
	return nil, errors.New("reset recovery is not part of this lifecycle fixture")
}
func (*runtimeTestRetainedSession) AdvanceResetOperation(context.Context, destructivereset.Operation, destructivereset.Operation) error {
	return errors.New("reset transition is not part of this lifecycle fixture")
}
