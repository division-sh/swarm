//go:build linux || darwin

package sessionprovider

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"github.com/division-sh/swarm/internal/sessionprovider/internal/authorityfact"
)

type sessionAuthorityOwner struct {
	state       *sessionState
	store       channelonboarding.Store
	operation   channelonboarding.Operation
	credentials operatorchannel.CredentialCurrentness
}

func newSessionAuthorityOwner(state *sessionState, store channelonboarding.Store, operation channelonboarding.Operation, credentials operatorchannel.CredentialCurrentness) (*sessionAuthorityOwner, error) {
	if state == nil || state.directory == nil || store == nil || operation.Provider != "whatsapp" || operation.ValidateSessionAccount() != nil ||
		operation.SessionAccount.ConnectionID != state.directory.connectionID || operation.Coordinate.ValidateContext() != nil {
		return nil, fmt.Errorf("session authority requires its original declared operation and private state")
	}
	return &sessionAuthorityOwner{state: state, store: store, operation: operation, credentials: credentials}, nil
}

func (o *sessionAuthorityOwner) CurrentValueMatchesSeal(ctx context.Context, expected runtimecredentials.ValueEvidence) (bool, error) {
	if o.credentials == nil {
		return false, fmt.Errorf("this session has no credential snapshot owner")
	}
	return o.credentials.CurrentValueMatchesSeal(ctx, expected)
}

func (o *sessionAuthorityOwner) AdmitSessionAccount(ctx context.Context, expected operatorchannel.SessionAccountAdmission) (authority.Admission, error) {
	if ctx == nil || ctx.Err() != nil || expected != o.operation.SessionAccount {
		return authority.Admission{}, errSessionAccount
	}
	op, err := o.store.GetChannelOnboarding(ctx, o.operation.OperationID)
	if err != nil {
		return authority.Admission{}, err
	}
	if op.Phase == channelonboarding.PhaseFailed || op.Phase == channelonboarding.PhaseRetired ||
		op.SessionAccount != expected || op.Posture != channelonboarding.ActivationSessionConnection ||
		op.Provider != expected.Provider || op.PrincipalID != o.operation.PrincipalID ||
		op.TargetSelector != o.operation.TargetSelector || !op.Coordinate.MatchesDeclaration(o.operation.Coordinate) {
		return authority.Admission{}, errCaptureScopeChanged
	}
	occurrence := o.state.currentOccurrence()
	if occurrence == nil {
		return authority.Admission{}, errClientOccurrenceFenced
	}
	workCtx, release, err := occurrence.acquire(ctx)
	if err != nil {
		return authority.Admission{}, err
	}
	retained := false
	defer func() {
		if !retained {
			release()
		}
	}()
	device, err := o.state.device(workCtx)
	if err != nil {
		return authority.Admission{}, err
	}
	if device.ID == nil || device.ID.ToNonAD().String() != expected.AccountRef || workCtx.Err() != nil {
		return authority.Admission{}, errSessionAccount
	}
	current := func() bool {
		return o.state.ownsConnectedOccurrence(workCtx, occurrence)
	}
	if !current() {
		return authority.Admission{}, errClientOccurrenceFenced
	}
	retained = true
	return authorityfact.SealOwnedAccount(expected, op.OperationID, op.Revision, workCtx, current, release), nil
}
