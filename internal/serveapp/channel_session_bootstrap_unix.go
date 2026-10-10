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
)

// Connections belong to retained onboarding operations and the selected runtime,
// not a provider-name registry or an independent process/retry supervisor.
type serveSessionBootstrap struct {
	mu            sync.Mutex
	connections   map[string]*sessionprovider.RuntimeConnection
	selectRuntime serveSessionBootstrapRuntime
	store         channelonboarding.Store
	credentials   operatorchannel.CredentialCurrentness
	directory     string
}

func init() {
	newServeSessionBootstrap = newNativeServeSessionBootstrap
}

func newNativeServeSessionBootstrap(selectRuntime serveSessionBootstrapRuntime, store channelonboarding.Store,
	credentials operatorchannel.CredentialCurrentness, directory string,
) (channelonboarding.SessionBootstrapOwner, error) {
	return &serveSessionBootstrap{connections: make(map[string]*sessionprovider.RuntimeConnection),
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
	return s.connections[operationID]
}

func (s *serveSessionBootstrap) BootstrapSession(ctx context.Context, op channelonboarding.Operation, candidate channelonboarding.Candidate) (err error) {
	if err := s.QualifySessionPlan(candidate); err != nil {
		return err
	}
	if !op.Coordinate.MatchesDeclaration(candidate.Coordinate) || op.TargetSelector != candidate.Target.Selector ||
		op.Phase != channelonboarding.PhaseActivatingProvider || op.SessionAccount != (operatorchannel.SessionAccountAdmission{}) {
		return channelonboarding.ErrRevisionConflict
	}
	if s.connection(op.OperationID) != nil {
		return nil
	}
	owned, release, err := s.selectRuntime(ctx, op.Coordinate)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, release()) }()
	connection, err := sessionprovider.OpenRuntimeBootstrap(owned, sessionprovider.RuntimeConnectionOptions{
		Directory: s.directory, OperationID: op.OperationID, Store: s.store, Credentials: s.credentials, Plan: candidate.Plan})
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.connections[op.OperationID] != nil {
		s.mu.Unlock()
		return errors.Join(channelonboarding.ErrConflict, connection.Close(context.WithoutCancel(ctx)))
	}
	s.connections[op.OperationID] = connection
	s.mu.Unlock()
	return connection.Connect(ctx)
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
