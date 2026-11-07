//go:build linux || darwin

package sessionprovider

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/channelprovider"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"github.com/division-sh/swarm/internal/sessionprovider/internal/authorityfact"
)

type sessionAuthorityOwner struct {
	mu          sync.Mutex
	releaseErr  error
	parent      sessionWorkParent
	state       retainedSessionAccountReader
	store       channelonboarding.Store
	operations  channelonboarding.OperationStore
	operation   channelonboarding.Operation
	credentials operatorchannel.CredentialCurrentness
}

type sessionWorkParent interface {
	Begin(context.Context) (*worklifetime.Lease, error)
	BeginStanding(context.Context) (*worklifetime.Lease, error)
}

type retainedSessionAccountReader interface {
	sessionAccountScope() (provider, connection string, occurrence context.Context, err error)
	acquireSessionAccount(context.Context, operatorchannel.SessionAccountAdmission) (context.Context, func() bool, func(), error)
	failSessionAuthority(error)
}

func newSessionAuthorityOwner(state retainedSessionAccountReader, store channelonboarding.OperationStore, operation channelonboarding.Operation, credentials operatorchannel.CredentialCurrentness, invocation sessionWorkParent) (*sessionAuthorityOwner, error) {
	if state == nil || reflect.ValueOf(state).Kind() == reflect.Pointer && reflect.ValueOf(state).IsNil() || store == nil ||
		operation.ValidateSessionAccount() != nil || !operation.DeploymentRegistration && operation.Coordinate.ValidateContext() != nil {
		return nil, fmt.Errorf("session authority requires its original declared operation and private state")
	}
	provider, connection, occurrence, err := state.sessionAccountScope()
	if !channelprovider.Session(provider) || operation.Provider != provider || operation.SessionAccount.ConnectionID != connection {
		return nil, fmt.Errorf("session authority requires its original declared operation and private state")
	}
	if err != nil {
		return nil, err
	}
	if occurrence == nil {
		return nil, errClientOccurrenceFenced
	}
	if operation.DeploymentRegistration {
		if invocation == nil || !operation.AccountConfirmed {
			return nil, errSessionAccount
		}
		return &sessionAuthorityOwner{state: state, operations: store, operation: operation, credentials: credentials, parent: invocation}, nil
	}
	runtimeStore, ok := store.(channelonboarding.Store)
	if !ok {
		return nil, errRuntimeConnection
	}
	parent, ok := worklifetime.RuntimeOccurrenceFromContext(occurrence)
	if !ok || parent.Identity().RuntimeInstanceID != operation.Coordinate.RuntimeInstanceID || parent.Identity().BundleHash != operation.Coordinate.BundleHash {
		return nil, fmt.Errorf("session authority requires its owning runtime occurrence")
	}
	return &sessionAuthorityOwner{state: state, store: runtimeStore, operations: store, operation: operation, credentials: credentials, parent: parent}, nil
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
	o.mu.Lock()
	previous := o.releaseErr
	o.mu.Unlock()
	if previous != nil {
		return authority.Admission{}, previous
	}
	work, err := o.parent.Begin(ctx)
	if err != nil {
		return authority.Admission{}, err
	}
	retainedRuntime := false
	defer func() {
		if !retainedRuntime {
			o.releaseRuntime(work)
		}
	}()
	ctx = work.Context()
	op, err := o.operations.GetChannelOnboarding(ctx, o.operation.OperationID)
	if err != nil {
		return authority.Admission{}, err
	}
	if op.Phase == channelonboarding.PhaseFailed || op.Phase == channelonboarding.PhaseRetired ||
		op.SessionAccount != expected || op.Posture != channelonboarding.ActivationSessionConnection ||
		op.Provider != expected.Provider || op.PrincipalID != o.operation.PrincipalID ||
		op.TargetSelector != o.operation.TargetSelector || op.DeploymentRegistration != o.operation.DeploymentRegistration ||
		!op.DeploymentRegistration && !op.Coordinate.MatchesDeclaration(o.operation.Coordinate) || op.DeploymentRegistration && !op.AccountConfirmed {
		return authority.Admission{}, errCaptureScopeChanged
	}
	workCtx, current, release, err := o.state.acquireSessionAccount(ctx, expected)
	if err != nil {
		return authority.Admission{}, err
	}
	if release == nil {
		return authority.Admission{}, errSessionAccount
	}
	retained := false
	defer func() {
		if !retained {
			release()
		}
	}()
	if workCtx == nil || workCtx.Err() != nil || current == nil {
		return authority.Admission{}, errSessionAccount
	}
	if !current() {
		return authority.Admission{}, errClientOccurrenceFenced
	}
	retained = true
	retainedRuntime = true
	scope := authority.RuntimeParent
	if op.DeploymentRegistration {
		scope = authority.DeploymentParent
	}
	return authorityfact.SealOwnedAccount(expected, op.OperationID, op.Revision, scope, workCtx, current, func() {
		release()
		o.releaseRuntime(work)
	}), nil
}

func (o *sessionAuthorityOwner) releaseRuntime(work *worklifetime.Lease) {
	if err := work.Done(); err != nil {
		o.mu.Lock()
		o.releaseErr = err
		o.mu.Unlock()
		o.state.failSessionAuthority(err)
	}
}
