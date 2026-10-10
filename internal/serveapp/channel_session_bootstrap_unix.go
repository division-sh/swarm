//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
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
		select {
		case <-attempt.done:
			if attempt.err != nil {
				return attempt.err
			}
			return attempt.connection.CheckBootstrap(ctx)
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	// The retained entry owns partial construction and cleanup even when the
	// caller stops waiting. It is never an instruction to retry Connect.
	defer func() {
		attempt.err = err
		close(attempt.done)
	}()
	owned, release, err := s.selectRuntime(ctx, op.Coordinate)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, release()) }()
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	connection, err := sessionprovider.OpenRuntimeBootstrap(owned, sessionprovider.RuntimeConnectionOptions{
		Directory: s.directory, OperationID: op.OperationID, Store: s.store, Credentials: s.credentials, Plan: candidate.Plan})
	s.mu.Lock()
	attempt.connection = connection
	s.mu.Unlock()
	if err == nil {
		err = connection.Connect(ctx)
	}
	if err != nil && connection != nil {
		err = errors.Join(err, connection.Close(context.WithoutCancel(ctx)))
	}
	if err == nil {
		err = connection.CheckBootstrap(ctx)
	}
	return err
}

func (s *serveSessionBootstrap) CheckpointSessionPairing(ctx context.Context, op channelonboarding.Operation) (channelonboarding.Operation, bool, error) {
	connection := s.connection(op.OperationID)
	if connection == nil {
		return op, false, channelonboarding.ErrNotFound
	}
	return connection.CheckpointPairing(ctx, op.Revision)
}

func (s *serveSessionBootstrap) AdmitSessionAccount(ctx context.Context, account operatorchannel.SessionAccountAdmission) (authority.Admission, error) {
	if s == nil || ctx == nil || ctx.Err() != nil || account.Validate() != nil || account.Provider != "whatsapp" {
		return authority.Admission{}, channelonboarding.ErrInvalidRequest
	}
	s.mu.Lock()
	var selected *sessionprovider.RuntimeConnection
	for _, attempt := range s.connections {
		connection := attempt.connection
		if connection != nil && connection.ConnectionID() == account.ConnectionID {
			if selected != nil {
				s.mu.Unlock()
				return authority.Admission{}, channelonboarding.ErrConflict
			}
			selected = connection
		}
	}
	s.mu.Unlock()
	if selected == nil {
		return authority.Admission{}, &operatorchannel.SessionProviderUnavailableError{Provider: account.Provider}
	}
	return selected.AdmitSessionAccount(ctx, account)
}

func (s *serveSessionBootstrap) ReadSessionPairing(ctx context.Context, op channelonboarding.Operation, principal operatorchannel.Principal) (channelonboarding.PairingReadback, error) {
	if s == nil || ctx == nil || ctx.Err() != nil || principal.Validate() != nil || principal.ID != op.PrincipalID {
		return channelonboarding.PairingReadback{}, channelonboarding.ErrInvalidRequest
	}
	connection := s.connection(op.OperationID)
	if connection == nil {
		return channelonboarding.PairingReadback{Status: "not_started"}, nil
	}
	return connection.PairingReadback(ctx, principal)
}
