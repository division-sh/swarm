//go:build linux || darwin

package serveapp

import (
	"context"

	"github.com/division-sh/swarm/internal/channelonboarding"
)

func (s *serveSessionBootstrap) readSessionDiagnostic(ctx context.Context, op channelonboarding.Operation) (serveSessionDiagnostic, error) {
	item := serveSessionDiagnostic{Operation: op}
	if err := s.validateRetainedSessionOperation(ctx, op); err != nil {
		return item, err
	}
	if connection := s.connection(op.OperationID); connection != nil && !connection.Closed() {
		var err error
		item.Fact, item.Account, err = connection.SessionDiagnostic(ctx)
		if err != nil {
			return item, err
		}
		if item.Fact.ConnectionID != "" && item.Fact.ConnectionID != op.SessionConnectionID {
			return item, channelonboarding.ErrRevisionConflict
		}
	}
	return item, nil
}

// Reconnect must reuse this exact discovery; observation grants no eligibility.
func (s *serveSessionBootstrap) registeredSessionConnections(ctx context.Context) ([]serveSessionDiagnostic, error) {
	scoped, ok := s.store.(interface {
		DeploymentChannelOperations() channelonboarding.OperationStore
	})
	if !ok || ctx == nil {
		return nil, channelonboarding.ErrInvalidRequest
	}
	operations, err := scoped.DeploymentChannelOperations().ListChannelOnboardingOperations(ctx)
	if err != nil {
		return nil, err
	}
	var out []serveSessionDiagnostic
	accounts, connections := map[string]bool{}, map[string]bool{}
	for _, op := range channelonboarding.RetainedConfirmedAccounts(operations, "whatsapp") {
		if !op.DeploymentRegistration || op.ValidateSessionAccount() != nil || accounts[op.AccountReference] || connections[op.SessionConnectionID] {
			return nil, channelonboarding.ErrRevisionConflict
		}
		accounts[op.AccountReference], connections[op.SessionConnectionID] = true, true
		item, err := s.readSessionDiagnostic(ctx, op)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
}

func (s *serveSessionBootstrap) sessionDiagnostics(ctx context.Context) ([]serveSessionDiagnostic, error) {
	out, err := s.registeredSessionConnections(ctx)
	if err != nil {
		return nil, err
	}
	activations, err := s.store.ListCurrentConnectedChannelActivations(ctx)
	if err != nil {
		return nil, err
	}
	for _, activation := range activations {
		if activation.Provider == "whatsapp" {
			op, err := s.store.GetChannelOnboarding(ctx, activation.OperationID)
			if err != nil {
				return nil, err
			}
			item, err := s.readSessionDiagnostic(ctx, op)
			if err != nil {
				return nil, err
			}
			out = append(out, item)
		}
	}
	return out, nil
}
