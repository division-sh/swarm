package serveapp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/sessionprovider"
)

func newServeChannelProviderOwners(manager *runtime.RuntimeContextManager, store channelonboarding.Store,
	current *credentials.SnapshotOwner, directory string, invocationState *sessionprovider.InvocationState,
) (serveSessionBootstrapOwner, operatorchannel.CredentialCurrentness, error) {
	sessions, err := newServeSessionBootstrap(serveSessionBootstrapRuntimeSelector(manager), store, current, directory, invocationState, serveChannelTargetEnablementReader(manager))
	if err != nil {
		var unsupported *operatorchannel.SessionProviderUnavailableError
		if errors.As(err, &unsupported) {
			// Unsupported hosts retain native refusal, not a credential substitute.
			return nil, current, nil
		}
		return nil, nil, err
	}
	return sessions, sessions, nil
}

// The existing joined channel worker owns bounded recovery, through the same
// fenced native owner as startup/connect. SDK auto-reconnect stays disabled.
func (d *serveChannelDeliveryDispatcher) reconcileSessionConnections(ctx context.Context, activations []channelonboarding.ConnectedChannelActivation) error {
	if d.onboarding == nil {
		return nil
	}
	for _, activation := range activations {
		if activation.Provider != "whatsapp" {
			continue
		}
		result, err := d.onboarding.Get(ctx, activation.OperationID)
		if err != nil {
			return err
		}
		op := result.Operation
		if op.Phase != channelonboarding.PhaseSucceeded || result.Connection == nil {
			continue
		}
		state := result.Connection.State
		if state == "connected" {
			if d.sessionStates[op.OperationID] != state {
				fmt.Fprintf(d.output, "WhatsApp: reconnected (%s)\n", result.Connection.AccountPresentation)
			}
			d.sessionStates[op.OperationID], d.sessionAttempts[op.OperationID] = state, 0
			continue
		}
		if d.sessionStates[op.OperationID] != "disconnected" {
			reason := result.Connection.Reason
			if reason == "" {
				reason = "connection unavailable"
			} else if reason == "network unreachable" {
				reason += ", reconnecting..."
			}
			fmt.Fprintf(d.output, "WhatsApp disconnected: %s\n", reason)
			d.sessionStates[op.OperationID] = "disconnected"
		}
		if d.sessionAttempts[op.OperationID] >= 3 || d.now().Before(d.sessionNextAttempt[op.OperationID]) {
			continue
		}
		d.sessionAttempts[op.OperationID]++
		d.sessionNextAttempt[op.OperationID] = d.now().Add(5 * time.Second)
		if _, err := d.onboarding.Retry(ctx, channelonboarding.RetryInput{OperationID: op.OperationID}); err != nil {
			return err
		}
	}
	return nil
}

func restoreServeChannelStartup(ctx context.Context, manager *runtime.RuntimeContextManager, store channelonboarding.Store,
	teardown *channelonboarding.DestructiveService, onboarding *channelonboarding.Service, identities *operatorchannel.Service, now time.Time,
) ([]operatorchannel.Binding, error) {
	if err := onboarding.RollSessionSources(ctx); err != nil {
		return nil, err
	}
	if err := reconcileRetiredConnectedChannelContexts(ctx, manager, store, teardown); err != nil {
		return nil, err
	}
	if err := onboarding.RestoreSessions(ctx); err != nil {
		return nil, fmt.Errorf("restore native ownership before retained channel proofs: %w", err)
	}
	_, bindings, err := identities.Bootstrap(ctx, now)
	return bindings, err
}
