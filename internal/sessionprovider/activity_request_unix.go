//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/operatorchannel"
)

// Request validation never seals an account or grants an external write.
func (c *RuntimeConnection) ValidateActivityRequestAccount(ctx context.Context, expected operatorchannel.SessionAccountAdmission, parent string, revision int64) (err error) {
	work, err := c.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, work.Done()) }()
	if err := c.lockLifecycle(work.Context()); err != nil {
		return err
	}
	defer func() { <-c.lifecycle }()
	op, err := c.currentOperation(work.Context())
	if err != nil {
		return err
	}
	if expected.Validate() != nil || op.SessionAccount != expected || op.OperationID != parent || revision < 1 || op.Revision < revision ||
		op.DeploymentRegistration && !op.AccountConfirmed || c.state == nil {
		return errSessionAccount
	}
	device, err := c.state.device(work.Context())
	if err != nil {
		return err
	}
	if device.ID == nil || device.ID.ToNonAD().String() != expected.AccountRef {
		return errSessionAccount
	}
	occurrence := c.state.currentOccurrence()
	if occurrence == nil {
		return errClientOccurrenceFenced
	}
	current, release, err := occurrence.acquire(work.Context())
	if err != nil {
		if cause := context.Cause(work.Context()); cause != nil {
			return errors.Join(err, cause)
		}
		if occurrence.transientAdmissionRefusal(err) {
			return nil
		}
		return err
	}
	defer release()
	if c.state.ownsConnectedOccurrence(current, occurrence) {
		return current.Err()
	}
	return errClientOccurrenceFenced
}
