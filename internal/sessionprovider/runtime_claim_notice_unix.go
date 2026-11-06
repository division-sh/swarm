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

type claimCodeObservation struct {
	notice  channelonboarding.ClaimCodeNotice
	expires time.Time
}

func (c *RuntimeConnection) observeClaimCodeMiss(identity operatorchannel.Operation) {
	// Replacement coalesces arrivals without retaining their code, body or sender.
	c.claimCodeNotice.Store(&claimCodeObservation{notice: channelonboarding.ClaimCodeNotice{
		IdentityOperationID: identity.OperationID, Observation: uuid.NewString()}, expires: identity.ExpiresAt})
}

func (c *RuntimeConnection) ReadClaimCodeNotice(ctx context.Context, expected channelonboarding.Operation) (notice *channelonboarding.ClaimCodeNotice, err error) {
	observed := c.claimCodeNotice.Load()
	if observed == nil || !observed.expires.After(time.Now()) {
		return nil, nil
	}
	work, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, work.Done()) }()
	op, err := c.currentOperation(work.Context())
	if err != nil {
		return nil, err
	}
	if op.OperationID != expected.OperationID || op.PrincipalID != expected.PrincipalID || op.SessionAccount != expected.SessionAccount ||
		op.IdentityOperationID != observed.notice.IdentityOperationID || op.Phase != channelonboarding.PhaseAwaitingExternalIdentity {
		return nil, nil
	}
	if err := c.CheckBootstrap(work.Context()); err != nil {
		return nil, err
	}
	if c.incoming == nil {
		return nil, nil
	}
	identities, err := c.incoming.store.ListOperatorChannelOperations(work.Context(), op.PrincipalID)
	if err != nil {
		return nil, err
	}
	for _, identity := range identities {
		if pendingClaimIdentityMatches(op, identity) && identity.State == operatorchannel.StateAwaitingClaim && identity.ExpiresAt.Equal(observed.expires) {
			copy := observed.notice
			return &copy, nil
		}
	}
	return nil, nil
}
