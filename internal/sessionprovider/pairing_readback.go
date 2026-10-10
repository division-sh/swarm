package sessionprovider

import (
	"context"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
)

type pairingOperationReader interface {
	GetChannelOnboarding(context.Context, string) (channelonboarding.Operation, error)
}

// principal is the selected principal resolved by the authenticated API owner,
// not a provider sender or a caller-authored field. This checks disclosure scope;
// it neither authenticates the request nor installs session execution.
func (q *pairingQR) readAuthorized(ctx context.Context, principal operatorchannel.Principal,
	operations pairingOperationReader, current channelonboarding.ChannelRuntimeContextCoordinate,
) (pairingQRSnapshot, error) {
	if q == nil || ctx == nil || ctx.Err() != nil {
		return pairingQRSnapshot{}, errPairingStopped
	}
	if operations == nil || principal.Validate() != nil || principal.ID != q.scope.PrincipalID || current != q.scope.Coordinate {
		return pairingQRSnapshot{}, errPairingScope
	}
	op, err := operations.GetChannelOnboarding(ctx, q.scope.OperationID)
	if ctx.Err() != nil {
		return pairingQRSnapshot{}, errPairingStopped
	}
	if err != nil {
		return pairingQRSnapshot{}, err
	}
	if op.OperationID != q.scope.OperationID || op.PrincipalID != principal.ID || op.Provider != "whatsapp" ||
		op.SessionConnectionID != q.scope.ConnectionID ||
		op.Posture != channelonboarding.ActivationSessionConnection || op.Ceremony != channelonboarding.CeremonyAuthenticatedTextChallenge ||
		op.Coordinate != q.scope.Coordinate || op.Phase != channelonboarding.PhaseActivatingProvider ||
		op.Revision < 1 || op.RequestedAt.IsZero() || !op.CompletedAt.IsZero() {
		return pairingQRSnapshot{}, errPairingScope
	}
	result, err := q.read(q.scope)
	if ctx.Err() != nil {
		return pairingQRSnapshot{}, errPairingStopped
	}
	return result, err
}
