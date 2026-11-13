package serveapp

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	owner, ok := d.sessions.(interface {
		registeredSessionConnections(context.Context) ([]serveSessionDiagnostic, error)
		sessionRecovery(context.Context, channelonboarding.Operation) (time.Time, bool, error)
	})
	if !ok {
		return nil
	}
	var operations []channelonboarding.Operation
	presentations := map[string]string{}
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
		operations = append(operations, op)
		presentations[op.OperationID] = result.Connection.AccountPresentation
	}
	registered, err := owner.registeredSessionConnections(ctx)
	if err != nil {
		return err
	}
	for _, item := range registered {
		if item.Fact.ConnectionID == "" {
			continue
		}
		operations = append(operations, item.Operation)
		presentations[item.Operation.OperationID] = item.Account
	}
	for _, op := range operations {
		at, connected, err := owner.sessionRecovery(ctx, op)
		if channelonboarding.SessionUnavailableOnly(err) && errors.Is(err, channelonboarding.ErrSessionNotLinked) {
			if d.sessionStates[op.OperationID] != "not_linked" {
				fmt.Fprintln(d.output, channelonboarding.ErrSessionNotLinked)
			}
			d.sessionStates[op.OperationID] = "not_linked"
			continue
		}
		if err != nil {
			return err
		}
		state := "disconnected"
		if connected {
			state = "connected"
		}
		if state == "connected" {
			if d.sessionStates[op.OperationID] != state {
				fmt.Fprintf(d.output, "WhatsApp: reconnected (%s)\n", presentations[op.OperationID])
			}
			d.sessionStates[op.OperationID], d.sessionAttempts[op.OperationID] = state, 0
			continue
		}
		if d.sessionStates[op.OperationID] != "disconnected" {
			fmt.Fprintln(d.output, "WhatsApp disconnected: network unreachable, reconnecting...")
			d.sessionStates[op.OperationID] = "disconnected"
			d.sessionAttempts[op.OperationID] = 5
			d.sessionNextAttempt[op.OperationID] = at.Add(5 * time.Second)
		}
		if at.IsZero() || d.now().Before(d.sessionNextAttempt[op.OperationID]) {
			continue
		}
		if op.DeploymentRegistration {
			if d.deployment == nil || d.deployment.resume == nil {
				return channelonboarding.ErrInvalidRequest
			}
			err = d.deployment.resume(ctx, op)
		} else {
			_, err = d.onboarding.Retry(ctx, channelonboarding.RetryInput{OperationID: op.OperationID})
		}
		if err != nil && !channelonboarding.SessionUnavailableOnly(err) {
			return err
		}
		if errors.Is(err, channelonboarding.ErrSessionNotLinked) {
			fmt.Fprintln(d.output, channelonboarding.ErrSessionNotLinked)
			d.sessionStates[op.OperationID] = "not_linked"
			continue
		}
		d.sessionAttempts[op.OperationID] = min(d.sessionAttempts[op.OperationID]*2, 7200)
		d.sessionNextAttempt[op.OperationID] = d.now().Add(time.Duration(d.sessionAttempts[op.OperationID]) * time.Second)
		if err == nil {
			_, connected, err := owner.sessionRecovery(ctx, op)
			if err != nil {
				return err
			}
			if connected {
				fmt.Fprintf(d.output, "WhatsApp: reconnected (%s)\n", presentations[op.OperationID])
				d.sessionStates[op.OperationID], d.sessionAttempts[op.OperationID] = "connected", 0
			}
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

type serveSessionDiagnostic struct {
	Operation channelonboarding.Operation
	Fact      sessionprovider.SessionDiagnostic
	Account   string
}

func serveSessionDiagnostics(sessions serveSessionBootstrapOwner, hitl *channelonboarding.AccountReference, out, errOut io.Writer, verbose bool) func(context.Context) error {
	owner, ok := sessions.(interface {
		sessionDiagnostics(context.Context) ([]serveSessionDiagnostic, error)
	})
	if !ok || hitl == nil && !verbose {
		return nil
	}
	previous := map[[2]string]sessionprovider.SessionDiagnostic{}
	return func(ctx context.Context) error {
		items, err := owner.sessionDiagnostics(ctx)
		if err != nil {
			return err
		}
		next := map[[2]string]sessionprovider.SessionDiagnostic{}
		for _, item := range items {
			fact := item.Fact
			if fact.OccurrenceID == "" {
				continue
			}
			key := [2]string{fact.ConnectionID, fact.OccurrenceID}
			old := previous[key]
			next[key] = fact
			previous[key] = fact // One owner can appear in both learned and registered views.
			if fact.Down && !old.Down && hitl != nil && hitl.Provider == item.Operation.Provider && hitl.Account == item.Account {
				fmt.Fprintf(out, "ERROR WhatsApp disconnected: %s\n", fact.Cause())
			}
			if verbose && fact != old && (fact.Down || fact.Socket != "" || fact.Lifecycle != "") {
				fmt.Fprintf(errOut, "ERROR whatsapp occurrence=%s socket=%s errno=%d close_status=%d lifecycle=%q\n", fact.OccurrenceID, fact.Socket, fact.Errno, fact.CloseStatus, fact.Lifecycle)
			}
			if verbose && fact.KeepAliveTimeout && !old.KeepAliveTimeout {
				fmt.Fprintln(errOut, "OBSERVATION WhatsApp: SDK KeepAliveTimeout observed")
			}
			if verbose && fact.KeepAliveRestored && !old.KeepAliveRestored {
				fmt.Fprintln(errOut, "OBSERVATION WhatsApp: SDK KeepAliveRestored observed")
			}
		}
		previous = next
		return nil
	}
}
