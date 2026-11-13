//go:build linux || darwin

package serveapp

import (
	"context"
	"errors"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packs"
	"time"
)

func (s *serveSessionBootstrap) sessionRecovery(ctx context.Context, op channelonboarding.Operation) (time.Time, bool, error) {
	if err := s.validateRetainedSessionOperation(ctx, op); err != nil {
		return time.Time{}, false, err
	}
	s.mu.Lock()
	attempt := s.connections[op.OperationID]
	s.mu.Unlock()
	if attempt == nil {
		return time.Time{}, false, channelonboarding.ErrNotFound
	}
	if _, err := attempt.finishedConnection(ctx); err != nil && !channelonboarding.SessionUnavailableOnly(err) {
		return time.Time{}, false, err
	}
	if channelonboarding.SessionUnavailableOnly(attempt.err) {
		return time.Time{}, false, attempt.err
	}
	return attempt.connection.SessionRecovery(ctx, op)
}

func (s *serveSessionBootstrap) sessionPublicationCurrent(ctx context.Context, activation channelonboarding.CompiledActivation) (bool, error) {
	if activation.Plan.Transport() != packs.ChannelTransportSession ||
		activation.Source != channelonboarding.ActivationSourceLearned && activation.Source != channelonboarding.ActivationSourceRegistered {
		return true, nil
	}
	responsibility, err := activation.AdmissionResponsibility()
	if err != nil {
		return false, err
	}
	current, err := channelonboarding.AdmissionResponsibilityCurrent(ctx, s.store, responsibility, true)
	if err != nil || !current {
		return false, errors.Join(channelonboarding.ErrRevisionConflict, err)
	}
	operations, err := s.operationsFor(channelonboarding.Operation{DeploymentRegistration: activation.Source == channelonboarding.ActivationSourceRegistered})
	if err != nil {
		return false, err
	}
	op, err := operations.GetChannelOnboarding(ctx, activation.OnboardingOperationID)
	if err != nil {
		return false, err
	}
	if op.Phase != channelonboarding.PhaseSucceeded {
		return true, nil // Existing initial-enrollment publication keeps its native admission.
	}
	at, connected, err := s.sessionRecovery(ctx, op)
	if channelonboarding.SessionUnavailableOnly(err) && errors.Is(err, channelonboarding.ErrSessionNotLinked) && !errors.Is(err, channelonboarding.ErrSessionUnavailable) {
		return false, nil // Terminal Not linked, never a deferred or retryable channel.
	}
	if err != nil {
		return false, err
	}
	if !connected && at.IsZero() {
		return false, channelonboarding.ErrRevisionConflict
	}
	return connected, nil
}
