//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
)

// Connections belong to retained onboarding operations and the selected runtime,
// not a provider-name registry or an independent process/retry supervisor.
type serveSessionBootstrap struct {
	mu            sync.Mutex
	connections   map[string]*serveSessionBootstrapAttempt
	selectRuntime serveSessionBootstrapRuntime
	store         channelonboarding.Store
	credentials   operatorchannel.CredentialCurrentness
	directory     string
}

type serveSessionBootstrapAttempt struct {
	connection *sessionprovider.RuntimeConnection
	done       chan struct{}
	err        error
}

func (a *serveSessionBootstrapAttempt) current(ctx context.Context) (*sessionprovider.RuntimeConnection, error) {
	if ctx == nil {
		return nil, channelonboarding.ErrInvalidRequest
	}
	select {
	case <-a.done:
		if a.err != nil {
			return nil, a.err
		}
		if a.connection == nil {
			return nil, channelonboarding.ErrRevisionConflict
		}
		if err := a.connection.CheckBootstrap(ctx); err != nil {
			return nil, err
		}
		return a.connection, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func (s *serveSessionBootstrap) currentConnection(ctx context.Context, operationID string) (*sessionprovider.RuntimeConnection, error) {
	s.mu.Lock()
	attempt := s.connections[operationID]
	s.mu.Unlock()
	if attempt == nil {
		return nil, channelonboarding.ErrNotFound
	}
	return attempt.current(ctx)
}

func init() {
	newServeSessionBootstrap = newNativeServeSessionBootstrap
}

func newNativeServeSessionBootstrap(selectRuntime serveSessionBootstrapRuntime, store channelonboarding.Store,
	credentials operatorchannel.CredentialCurrentness, directory string,
) (channelonboarding.SessionBootstrapOwner, error) {
	return &serveSessionBootstrap{connections: make(map[string]*serveSessionBootstrapAttempt),
		selectRuntime: selectRuntime, store: store, credentials: credentials, directory: directory}, nil
}

func (s *serveSessionBootstrap) QualifySessionPlan(candidate channelonboarding.Candidate) error {
	if s == nil || s.selectRuntime == nil || s.store == nil || s.credentials == nil || s.directory == "" {
		return fmt.Errorf("session bootstrap requires its serve runtime and selected owners")
	}
	if err := candidate.ValidateDeclaration(); err != nil {
		return err
	}
	if candidate.Posture != channelonboarding.ActivationSessionConnection {
		return &operatorchannel.SessionProviderUnavailableError{Provider: candidate.Provider}
	}
	return sessionprovider.QualifyBootstrapPlan(candidate.Plan)
}

func (s *serveSessionBootstrap) connection(operationID string) *sessionprovider.RuntimeConnection {
	s.mu.Lock()
	defer s.mu.Unlock()
	if attempt := s.connections[operationID]; attempt != nil {
		return attempt.connection
	}
	return nil
}

func (s *serveSessionBootstrap) BootstrapSession(ctx context.Context, op channelonboarding.Operation, candidate channelonboarding.Candidate) (err error) {
	if err := s.QualifySessionPlan(candidate); err != nil {
		return err
	}
	if !op.Coordinate.MatchesDeclaration(candidate.Coordinate) || op.TargetSelector != candidate.Target.Selector ||
		op.Phase != channelonboarding.PhaseActivatingProvider || op.SessionAccount != (operatorchannel.SessionAccountAdmission{}) {
		return channelonboarding.ErrRevisionConflict
	}
	if ctx == nil {
		return channelonboarding.ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	s.mu.Lock()
	attempt, reused := s.connections[op.OperationID]
	if !reused {
		attempt = &serveSessionBootstrapAttempt{done: make(chan struct{})}
		s.connections[op.OperationID] = attempt
	}
	s.mu.Unlock()
	if reused {
		_, err := attempt.current(ctx)
		return err
	}
	return s.openSessionAttempt(ctx, op, candidate, attempt, true)
}

// Resume is an explicit onboarding/recovery instruction, not cache reuse. The
// original owner's complete join precedes construction on the same reservation.
func (s *serveSessionBootstrap) ResumeSession(ctx context.Context, op channelonboarding.Operation, candidate channelonboarding.Candidate) error {
	if err := s.validateResumeSession(ctx, op, candidate); err != nil {
		return err
	}
	s.mu.Lock()
	previous := s.connections[op.OperationID]
	s.mu.Unlock()
	if previous != nil {
		select {
		case <-previous.done:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		if previous.connection != nil {
			if previous.err == nil {
				reused, err := previous.connection.CheckSessionReuse(ctx, op)
				if err != nil || reused {
					return err
				}
			}
			if err := previous.connection.Close(ctx); err != nil {
				return err
			}
		}
	}
	s.mu.Lock()
	if current := s.connections[op.OperationID]; current != previous {
		s.mu.Unlock()
		_, err := current.current(ctx)
		return err
	}
	attempt := &serveSessionBootstrapAttempt{done: make(chan struct{})}
	s.connections[op.OperationID] = attempt
	s.mu.Unlock()
	return s.openSessionAttempt(ctx, op, candidate, attempt, op.SessionAccount == (operatorchannel.SessionAccountAdmission{}))
}

func (s *serveSessionBootstrap) validateResumeSession(ctx context.Context, op channelonboarding.Operation, candidate channelonboarding.Candidate) error {
	if ctx == nil {
		return channelonboarding.ErrInvalidRequest
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if err := s.QualifySessionPlan(candidate); err != nil {
		return err
	}
	if op.Provider != candidate.Provider || op.Posture != candidate.Posture || op.Interface.Normalized() != candidate.Interface.Normalized() ||
		!op.Coordinate.MatchesRuntimeContext(candidate.Coordinate) || op.TargetSelector != candidate.Target.Selector ||
		op.Phase == channelonboarding.PhaseFailed || op.Phase == channelonboarding.PhaseRetired || op.ValidateSessionAccount() != nil ||
		op.SessionAccount == (operatorchannel.SessionAccountAdmission{}) && op.Phase != channelonboarding.PhaseActivatingProvider {
		return channelonboarding.ErrRevisionConflict
	}
	current, err := s.store.GetChannelOnboarding(ctx, op.OperationID)
	if err != nil {
		return err
	}
	if current.Revision != op.Revision || current.Phase != op.Phase || current.RequestHash != op.RequestHash ||
		current.Provider != op.Provider || current.Posture != op.Posture ||
		current.PrincipalID != op.PrincipalID || current.SessionAccount != op.SessionAccount || current.SessionConnectionID != op.SessionConnectionID ||
		current.TargetSelector != op.TargetSelector || current.Interface.Normalized() != op.Interface.Normalized() ||
		!current.Coordinate.MatchesDeclaration(op.Coordinate) {
		return channelonboarding.ErrRevisionConflict
	}
	if op.Phase == channelonboarding.PhaseSucceeded {
		eligible, err := channelonboarding.RetainedSessionCurrent(ctx, s.store, op)
		if err != nil {
			return err
		}
		if !eligible {
			return channelonboarding.ErrRevisionConflict
		}
	}
	return nil
}

func (s *serveSessionBootstrap) openSessionAttempt(ctx context.Context, op channelonboarding.Operation, candidate channelonboarding.Candidate, attempt *serveSessionBootstrapAttempt, bootstrap bool) (err error) {
	// The retained entry owns partial construction and cleanup even when the
	// caller stops waiting. It is never an instruction to retry Connect.
	defer func() {
		attempt.err = err
		close(attempt.done)
	}()
	owned, incoming, release, err := s.selectRuntime(ctx, candidate)
	if err != nil {
		return err
	}
	if release == nil {
		return channelonboarding.ErrInvalidRequest
	}
	defer func() { err = errors.Join(err, release()) }()
	if owned == nil {
		return channelonboarding.ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	options := sessionprovider.RuntimeConnectionOptions{Directory: s.directory, OperationID: op.OperationID,
		Store: s.store, Credentials: s.credentials, Plan: candidate.Plan, Incoming: incoming}
	var connection *sessionprovider.RuntimeConnection
	if bootstrap {
		connection, err = sessionprovider.OpenRuntimeBootstrap(owned, options)
	} else {
		connection, err = sessionprovider.OpenRuntimeConnection(owned, options)
	}
	s.mu.Lock()
	attempt.connection = connection
	s.mu.Unlock()
	if err == nil {
		err = connection.Connect(ctx)
	}
	if err != nil && connection != nil {
		err = errors.Join(err, connection.Close(ctx))
	}
	if err == nil {
		err = connection.CheckBootstrap(ctx)
	}
	if err == nil && incoming != nil {
		err = connection.ReconcileIncoming(ctx)
		if err != nil {
			err = errors.Join(err, connection.Close(ctx))
		}
	}
	return err
}

func (s *serveSessionBootstrap) CheckpointSessionPairing(ctx context.Context, op channelonboarding.Operation) (channelonboarding.Operation, bool, error) {
	connection, err := s.currentConnection(ctx, op.OperationID)
	if err != nil {
		return op, false, err
	}
	return connection.CheckpointPairing(ctx, op.Revision)
}

func (s *serveSessionBootstrap) CurrentValueMatchesSeal(ctx context.Context, expected credentials.ValueEvidence) (bool, error) {
	return s.credentials.CurrentValueMatchesSeal(ctx, expected)
}

func (s *serveSessionBootstrap) AdmitSessionAccount(ctx context.Context, account operatorchannel.SessionAccountAdmission) (authority.Admission, error) {
	if s == nil || ctx == nil || ctx.Err() != nil || account.Validate() != nil || account.Provider != "whatsapp" {
		return authority.Admission{}, channelonboarding.ErrInvalidRequest
	}
	s.mu.Lock()
	var selected *serveSessionBootstrapAttempt
	for _, attempt := range s.connections {
		connection := attempt.connection
		if connection != nil && connection.ConnectionID() == account.ConnectionID {
			if selected != nil {
				s.mu.Unlock()
				return authority.Admission{}, channelonboarding.ErrConflict
			}
			selected = attempt
		}
	}
	s.mu.Unlock()
	if selected == nil {
		return authority.Admission{}, &operatorchannel.SessionProviderUnavailableError{Provider: account.Provider}
	}
	connection, err := selected.current(ctx)
	if err != nil {
		return authority.Admission{}, err
	}
	return connection.AdmitSessionAccount(ctx, account)
}

func (s *serveSessionBootstrap) ReadSessionPairing(ctx context.Context, op channelonboarding.Operation, principal operatorchannel.Principal) (channelonboarding.PairingReadback, error) {
	if s == nil || ctx == nil || ctx.Err() != nil || principal.Validate() != nil || principal.ID != op.PrincipalID {
		return channelonboarding.PairingReadback{}, channelonboarding.ErrInvalidRequest
	}
	connection, err := s.currentConnection(ctx, op.OperationID)
	if errors.Is(err, channelonboarding.ErrNotFound) {
		return channelonboarding.PairingReadback{Status: "not_started"}, nil
	}
	if err != nil {
		return channelonboarding.PairingReadback{}, err
	}
	return connection.PairingReadback(ctx, principal)
}
