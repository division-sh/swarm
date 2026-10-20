//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
)

type revokedPairing struct {
	connection *RuntimeConnection
	occurrence *clientOccurrence
	operation  channelonboarding.Operation
}

func (*revokedPairing) Error() string { return "WhatsApp revoked this unadmitted pairing" }

func (r *revokedPairing) MatchesOperation(op channelonboarding.Operation) bool {
	return r.connection.state.currentOccurrence() == r.occurrence && r.connection.ctx.Err() == nil &&
		op.OperationID == r.operation.OperationID && op.Revision == r.operation.Revision &&
		op.PrincipalID == r.operation.PrincipalID && op.Interface == r.operation.Interface &&
		op.Coordinate.MatchesRuntimeContext(r.operation.Coordinate) && op.TargetSelector == r.operation.TargetSelector &&
		op.SessionConnectionID == r.operation.SessionConnectionID && op.Posture == channelonboarding.ActivationSessionConnection &&
		op.Phase == channelonboarding.PhaseActivatingProvider && op.SessionAccount == (operatorchannel.SessionAccountAdmission{}) &&
		op.IdentityOperationID == "" && op.BindingRevision == 0 && op.ActivationRevision == 0
}

func (r *revokedPairing) Join(ctx context.Context) error {
	op, err := r.connection.store.GetChannelOnboarding(ctx, r.operation.OperationID)
	if err != nil {
		return err
	}
	if op.Phase != channelonboarding.PhaseFailed || op.Revision != r.operation.Revision+1 ||
		op.FailureCode != "session_pairing_revoked" || op.PrincipalID != r.operation.PrincipalID ||
		!op.Coordinate.MatchesRuntimeContext(r.operation.Coordinate) || op.SessionConnectionID != r.operation.SessionConnectionID ||
		op.SessionAccount != (operatorchannel.SessionAccountAdmission{}) || op.IdentityOperationID != "" ||
		op.BindingRevision != 0 || op.ActivationRevision != 0 {
		return channelonboarding.ErrRevisionConflict
	}
	return r.connection.Close(ctx)
}

// RevokedSession observes only this explicit caller's original login. It never
// dials, deletes private state, checkpoints an account, or publishes authority.
func (c *RuntimeConnection) RevokedSession(ctx context.Context, expected channelonboarding.Operation) (fact channelonboarding.SessionRevocation, err error) {
	work, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, work.Done()) }()
	op, err := c.currentOperation(work.Context())
	if err != nil {
		return nil, err
	}
	if op.Phase != channelonboarding.PhaseActivatingProvider || op.SessionAccount != (operatorchannel.SessionAccountAdmission{}) {
		return nil, nil
	}
	occurrence := c.state.currentOccurrence()
	if occurrence == nil {
		return nil, nil
	}
	occurrence.mu.Lock()
	wait := occurrence.started && occurrence.pairing == nil && !occurrence.fenced
	occurrence.mu.Unlock()
	if wait {
		_ = occurrence.awaitConnected(work.Context())
		if work.Context().Err() != nil {
			return nil, context.Cause(work.Context())
		}
	}
	occurrence.mu.Lock()
	revoked := occurrence.revoked != nil
	occurrence.mu.Unlock()
	if !revoked {
		return nil, nil
	}
	op, err = c.currentOperation(work.Context())
	if err != nil {
		return nil, err
	}
	r := &revokedPairing{connection: c, occurrence: occurrence, operation: op}
	if !r.MatchesOperation(expected) {
		return nil, channelonboarding.ErrRevisionConflict
	}
	return r, nil
}
