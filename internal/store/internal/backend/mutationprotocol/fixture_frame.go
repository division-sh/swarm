package mutationprotocol

import (
	"context"
	"errors"
)

// RequireExistingSQLFrame validates this native attempt's already-bound frame.
// Unlike WithSQL, it cannot bind a missing frame or lend transaction authority.
func (a *Attempt) RequireExistingSQLFrame(ctx context.Context) error {
	if err := a.requireActive(); err != nil {
		return err
	}
	current, err := runAdmissionAttempt(ctx, a.tx)
	if err != nil {
		return err
	}
	if current != a {
		return errors.New("mutation SQL context requires an existing native SQL frame for this attempt")
	}
	return nil
}
