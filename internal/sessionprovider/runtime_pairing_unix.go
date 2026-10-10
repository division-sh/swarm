//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
)

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
