//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	sessionexecution "github.com/division-sh/swarm/internal/sessionprovider/execution"
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
	operation  channelonboarding.Operation
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
) (serveSessionBootstrapOwner, error) {
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
		attempt = &serveSessionBootstrapAttempt{operation: op, done: make(chan struct{})}
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
		default:
			_, err := previous.current(ctx)
			return err
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
	attempt := &serveSessionBootstrapAttempt{operation: op, done: make(chan struct{})}
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
	if err := s.validateRetainedSessionOperation(ctx, op); err != nil {
		return err
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

func (s *serveSessionBootstrap) validateRetainedSessionOperation(ctx context.Context, op channelonboarding.Operation) error {
	if s == nil || s.store == nil || ctx == nil {
		return channelonboarding.ErrInvalidRequest
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if op.Posture != channelonboarding.ActivationSessionConnection || op.Phase == channelonboarding.PhaseFailed ||
		op.Phase == channelonboarding.PhaseRetired || op.ValidateSessionAccount() != nil {
		return channelonboarding.ErrRevisionConflict
	}
	current, err := s.store.GetChannelOnboarding(ctx, op.OperationID)
	if err != nil {
		return err
	}
	if current.Revision != op.Revision || current.Phase != op.Phase || current.RequestHash != op.RequestHash ||
		current.Provider != op.Provider || current.Posture != op.Posture ||
		current.SlotKey != op.SlotKey || current.BindingRevision != op.BindingRevision || current.ActivationRevision != op.ActivationRevision ||
		current.PrincipalID != op.PrincipalID || current.SessionAccount != op.SessionAccount || current.SessionConnectionID != op.SessionConnectionID ||
		current.TargetSelector != op.TargetSelector || current.Interface.Normalized() != op.Interface.Normalized() ||
		current.Coordinate.TargetGeneration != op.Coordinate.TargetGeneration || !current.Coordinate.MatchesDeclaration(op.Coordinate) {
		return channelonboarding.ErrRevisionConflict
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return nil
}

// Observation neither opens a connection nor adopts a later selected parent.
// Cached ownership is distinct from a successfully connected SDK occurrence.
func (s *serveSessionBootstrap) ObserveSession(ctx context.Context, op channelonboarding.Operation) (operatorchannel.ProviderAuthority, operatorchannel.SessionConnectionObservation, bool, error) {
	var empty operatorchannel.SessionConnectionObservation
	if err := s.validateRetainedSessionOperation(ctx, op); err != nil {
		return operatorchannel.ProviderAuthority{}, empty, false, err
	}
	if op.SessionAccount == (operatorchannel.SessionAccountAdmission{}) {
		return operatorchannel.ProviderAuthority{}, empty, false, nil
	}
	s.mu.Lock()
	attempt := s.connections[op.OperationID]
	s.mu.Unlock()
	if attempt == nil {
		return operatorchannel.ProviderAuthority{}, empty, false, nil
	}
	select {
	case <-attempt.done:
	case <-ctx.Done():
		return operatorchannel.ProviderAuthority{}, empty, false, context.Cause(ctx)
	}
	if attempt.err != nil {
		return operatorchannel.ProviderAuthority{}, empty, false, attempt.err
	}
	if attempt.connection == nil {
		return operatorchannel.ProviderAuthority{}, empty, false, channelonboarding.ErrRevisionConflict
	}
	current, err := attempt.connection.CheckSessionReuse(ctx, op)
	if err != nil || !current {
		return operatorchannel.ProviderAuthority{}, empty, false, err
	}
	provider, observed, err := attempt.connection.ObserveSession(ctx)
	if err != nil {
		return operatorchannel.ProviderAuthority{}, empty, false, err
	}
	parentID, parentRevision := provider.SessionParent()
	if parentID != op.OperationID || parentRevision != op.Revision || observed.Admission != op.SessionAccount || ctx.Err() != nil {
		provider.CloseExecution()
		return operatorchannel.ProviderAuthority{}, empty, false, errors.Join(channelonboarding.ErrRevisionConflict, context.Cause(ctx))
	}
	return provider, observed, true, nil
}

func (s *serveSessionBootstrap) ChannelExecution(ctx context.Context, op channelonboarding.Operation) (sessionexecution.Channel, error) {
	if err := s.validateRetainedSessionOperation(ctx, op); err != nil {
		return sessionexecution.Channel{}, err
	}
	// Capture the original connection before observing it. Concurrent explicit
	// recovery cannot substitute a successor connection into this selection.
	connection, err := s.currentConnection(ctx, op.OperationID)
	if err != nil {
		return sessionexecution.Channel{}, err
	}
	provider, _, current, err := s.ObserveSession(ctx, op)
	defer provider.CloseExecution()
	if err != nil || !current {
		return sessionexecution.Channel{}, errors.Join(channelonboarding.ErrRevisionConflict, err)
	}
	return connection.ChannelExecution(ctx)
}

func (s *serveSessionBootstrap) RetireInactiveSessions(ctx context.Context) error {
	if s == nil || s.store == nil || ctx == nil {
		return channelonboarding.ErrInvalidRequest
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	type retained struct {
		id      string
		attempt *serveSessionBootstrapAttempt
	}
	s.mu.Lock()
	entries := make([]retained, 0, len(s.connections))
	for id, attempt := range s.connections {
		entries = append(entries, retained{id: id, attempt: attempt})
	}
	s.mu.Unlock()
	sort.Slice(entries, func(i, j int) bool { return entries[i].id < entries[j].id })
	var result error
	for _, entry := range entries {
		if ctx.Err() != nil {
			return errors.Join(result, context.Cause(ctx))
		}
		op, err := s.store.GetChannelOnboarding(ctx, entry.id)
		if ctx.Err() != nil {
			return errors.Join(result, context.Cause(ctx))
		}
		removed := errors.Is(err, channelonboarding.ErrNotFound)
		if err != nil && !removed {
			result = errors.Join(result, fmt.Errorf("observe retained session %s: %w", entry.id, err))
			continue
		}
		retired := removed
		if removed {
			// Construction evidence identifies cleanup, not executable authority.
			op = entry.attempt.operation
			err = nil
		} else {
			retired, err = serveSessionRetirementRequired(ctx, s.store, op)
		}
		if err != nil {
			result = errors.Join(result, fmt.Errorf("observe session retirement %s: %w", entry.id, err))
			continue
		}
		if !retired {
			continue
		}
		// Keep the original construction/cleanup entry even after joining. A
		// failed bootstrap can still own partial state; its error is not a join.
		select {
		case <-entry.attempt.done:
		case <-ctx.Done():
			return errors.Join(result, context.Cause(ctx))
		}
		if entry.attempt.connection != nil {
			if op.OperationID != entry.id || op.Posture != channelonboarding.ActivationSessionConnection ||
				op.SessionConnectionID != entry.attempt.connection.ConnectionID() {
				result = errors.Join(result, channelonboarding.ErrRevisionConflict)
				continue
			}
			if err := entry.attempt.connection.Close(ctx); err != nil {
				result = errors.Join(result, fmt.Errorf("join retained session %s: %w", entry.id, err))
			}
		}
	}
	return errors.Join(result, context.Cause(ctx))
}

func serveSessionRetirementRequired(ctx context.Context, store channelonboarding.Store, op channelonboarding.Operation) (bool, error) {
	if ctx.Err() != nil {
		return false, context.Cause(ctx)
	}
	switch op.Phase {
	case channelonboarding.PhaseFailed, channelonboarding.PhaseRetired:
		return true, nil
	case channelonboarding.PhaseSucceeded:
		current, err := channelonboarding.RetainedSessionCurrent(ctx, store, op)
		if ctx.Err() != nil {
			return false, errors.Join(err, context.Cause(ctx))
		}
		return !current, err
	default:
		return false, nil
	}
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
	if err == nil && !bootstrap {
		err = connection.AwaitSessionAccount(ctx)
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
