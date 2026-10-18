//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/google/uuid"
)

// CheckpointPairing freezes only the genuine SDK account in this reserved private
// directory. It does not confirm a human, admit a target or publish an activation.
func (c *RuntimeConnection) CheckpointPairing(ctx context.Context, expectedRevision int64) (result channelonboarding.Operation, paired bool, err error) {
	work, err := c.begin(ctx)
	if err != nil {
		return result, false, err
	}
	defer func() { err = errors.Join(err, work.Done()) }()
	ctx = work.Context()
	if err := c.lockLifecycle(ctx); err != nil {
		return result, false, err
	}
	defer func() { <-c.lifecycle }()
	op, err := c.currentOperationScope(ctx)
	if err != nil {
		return result, false, err
	}
	if op.Revision != expectedRevision || (op.Phase != channelonboarding.PhaseActivatingProvider && op.Phase != channelonboarding.PhaseAwaitingExternalIdentity) {
		return result, false, channelonboarding.ErrRevisionConflict
	}
	occurrence := c.state.currentOccurrence()
	if !c.state.ownsConnectedOccurrence(ctx, occurrence) {
		return op, false, nil
	}
	owned, release, err := occurrence.acquire(ctx)
	if err != nil {
		return result, false, err
	}
	defer release()
	device, err := c.state.device(owned)
	if err != nil {
		return result, false, err
	}
	if device.ID == nil || owned.Err() != nil || !c.state.ownsConnectedOccurrence(owned, occurrence) {
		return result, false, errSessionAccount
	}
	account := op.SessionAccount
	if account == (operatorchannel.SessionAccountAdmission{}) {
		account = operatorchannel.SessionAccountAdmission{Provider: op.Provider, ConnectionID: op.SessionConnectionID,
			AccountRef: device.ID.ToNonAD().String(), AdmissionID: uuid.NewString(), Revision: 1}
	}
	if account.Validate() != nil || account.AccountRef != device.ID.ToNonAD().String() ||
		account.ConnectionID != c.operation.SessionConnectionID ||
		c.sessionAccount() != (operatorchannel.SessionAccountAdmission{}) && c.sessionAccount() != account {
		return result, false, errSessionAccount
	}
	if op.Phase == channelonboarding.PhaseActivatingProvider {
		op, err = c.store.AdvanceChannelOnboarding(owned, channelonboarding.AdvanceRequest{OperationID: op.OperationID,
			ExpectedRevision: expectedRevision, Phase: channelonboarding.PhaseAwaitingExternalIdentity,
			SessionAccount: &account, Now: time.Now().UTC()})
		if err != nil {
			return result, false, err
		}
	}
	// The selected commit is authoritative even if its caller stops waiting.
	// Publish only this occurrence's verified original account; a fresh request
	// still has to obtain native admission and pass the lifetime fences.
	c.account.Store(&account)
	if owned.Err() != nil || c.ctx.Err() != nil {
		return result, false, errors.Join(errClientOccurrenceFenced, context.Cause(owned), context.Cause(c.ctx))
	}
	return op, true, nil
}

// PairingReadback accepts the API owner's selected principal, not a provider
// sender. The private QR owner rechecks the operation at the disclosure boundary.
func (c *RuntimeConnection) PairingReadback(ctx context.Context, principal operatorchannel.Principal) (result channelonboarding.PairingReadback, err error) {
	work, err := c.begin(ctx)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, work.Done()) }()
	op, err := c.currentOperation(work.Context())
	if err != nil {
		return result, err
	}
	if principal.Validate() != nil || principal.ID != op.PrincipalID || op.Phase.Terminal() ||
		op.Phase != channelonboarding.PhaseActivatingProvider || op.SessionAccount != (operatorchannel.SessionAccountAdmission{}) {
		return result, errPairingScope
	}
	occurrence := c.state.currentOccurrence()
	if occurrence == nil {
		return result, errPairingStopped
	}
	occurrence.mu.Lock()
	pairing := occurrence.pairing
	occurrence.mu.Unlock()
	if pairing == nil {
		// A paired private database after an interrupted checkpoint is not QR
		// material or new executable admission. Recovery owns its next action.
		return channelonboarding.PairingReadback{Status: "recovery_required"}, nil
	}
	view, err := pairing.readAuthorized(work.Context(), principal, c.store, op.Coordinate)
	if err != nil {
		return result, err
	}
	return channelonboarding.PairingReadback{Status: string(view.Status), Code: view.Code,
		ExpiresAt: view.ExpiresAt, Paired: view.Paired, Connected: view.Connected}, nil
}
