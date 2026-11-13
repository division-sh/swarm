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
func (d *serveChannelDeliveryDispatcher) reconcileSessionConnections(ctx context.Context, activations []channelonboarding.ConnectedChannelActivation) (accountErr, fatalErr error) {
	if d.onboarding == nil {
		return nil, nil
	}
	owner, ok := d.sessions.(interface {
		registeredSessionConnections(context.Context) ([]serveSessionDiagnostic, error)
		sessionRecovery(context.Context, channelonboarding.Operation) (time.Time, bool, error)
	})
	if !ok {
		return nil, nil
	}
	var operations []channelonboarding.Operation
	presentations := map[string]string{}
	for _, activation := range activations {
		if activation.Provider != "whatsapp" {
			continue
		}
		result, err := d.onboarding.Get(ctx, activation.OperationID)
		if err != nil {
			return nil, err
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
		return nil, err
	}
	for _, item := range registered {
		operations = append(operations, item.Operation)
		presentations[item.Operation.OperationID] = item.Account
	}
	var result error
	for _, op := range operations {
		if ctx.Err() != nil {
			return result, context.Cause(ctx)
		}
		err := d.reconcileSessionConnection(ctx, op, presentations[op.OperationID], owner.sessionRecovery)
		if d.linkNotices != nil {
			d.linkNotices.observe(op, err)
		}
		result = errors.Join(result, err)
	}
	return result, nil
}

func (d *serveChannelDeliveryDispatcher) reconcileSessionConnection(ctx context.Context, op channelonboarding.Operation, presentation string, recovery func(context.Context, channelonboarding.Operation) (time.Time, bool, error)) error {
	at, connected, err := recovery(ctx, op)
	if channelonboarding.SessionUnavailableOnly(err) && errors.Is(err, channelonboarding.ErrSessionNotLinked) {
		if d.sessionStates[op.OperationID] != "not_linked" {
			fmt.Fprintln(d.output, channelonboarding.ErrSessionNotLinked)
		}
		d.sessionStates[op.OperationID] = "not_linked"
		return nil
	}
	if err != nil {
		return err
	}
	state := "disconnected"
	if connected {
		state = "connected"
	}
	if state == "connected" {
		if presentation == "" {
			return nil // Wait for a fresh validated presentation, never print a cached empty label.
		}
		if d.sessionStates[op.OperationID] != state {
			if d.sessionStates[op.OperationID] != "recovered" && d.refreshSessions != nil {
				if err := d.refreshSessions(ctx); err != nil {
					return err
				}
			}
			fmt.Fprintf(d.output, "WhatsApp: reconnected (%s)\n", presentation)
		}
		d.sessionStates[op.OperationID], d.sessionAttempts[op.OperationID] = state, 0
		return nil
	}
	if d.sessionStates[op.OperationID] != "disconnected" {
		fmt.Fprintln(d.output, "WhatsApp disconnected: network unreachable, reconnecting...")
		d.sessionStates[op.OperationID] = "disconnected"
		d.sessionAttempts[op.OperationID] = 5
		d.sessionNextAttempt[op.OperationID] = at.Add(5 * time.Second)
	}
	if at.IsZero() || d.now().Before(d.sessionNextAttempt[op.OperationID]) {
		return nil
	}
	return d.retrySessionRecovery(ctx, op, recovery)
}

func (d *serveChannelDeliveryDispatcher) retrySessionRecovery(ctx context.Context, op channelonboarding.Operation, recovery func(context.Context, channelonboarding.Operation) (time.Time, bool, error)) error {
	var err error
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
		return nil
	}
	d.sessionAttempts[op.OperationID] = min(d.sessionAttempts[op.OperationID]*2, 7200)
	d.sessionNextAttempt[op.OperationID] = d.now().Add(time.Duration(d.sessionAttempts[op.OperationID]) * time.Second)
	if err == nil {
		_, connected, err := recovery(ctx, op)
		if err != nil {
			return err
		}
		if connected {
			if d.refreshSessions != nil {
				if err := d.refreshSessions(ctx); err != nil {
					return err
				}
			}
			d.sessionStates[op.OperationID], d.sessionAttempts[op.OperationID] = "recovered", 0
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

func serveSessionDiagnostics(sessions serveSessionBootstrapOwner, hitl *channelonboarding.AccountReference, out, errOut io.Writer, verbose bool, observed ...func([]serveSessionDiagnostic)) func(context.Context) error {
	owner, ok := sessions.(interface {
		sessionDiagnostics(context.Context) ([]serveSessionDiagnostic, error)
	})
	if !ok || hitl == nil && !verbose && len(observed) == 0 {
		return nil
	}
	previous := map[[2]string]sessionprovider.SessionDiagnostic{}
	boot := true
	return func(ctx context.Context) error {
		items, err := owner.sessionDiagnostics(ctx)
		if err != nil {
			return err
		}
		for _, observe := range observed {
			observe(items)
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
			if err := writeSessionDiagnosticDown(ctx, sessions, item, old, boot, hitl, out); err != nil {
				return err
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
		previous, boot = next, false
		return nil
	}
}

func writeSessionDiagnosticDown(ctx context.Context, sessions serveSessionBootstrapOwner, item serveSessionDiagnostic, old sessionprovider.SessionDiagnostic, boot bool, hitl *channelonboarding.AccountReference, out io.Writer) error {
	if hitl == nil || hitl.Provider != item.Operation.Provider || hitl.Account != item.Account {
		return nil
	}
	fact := item.Fact
	down := fact.Failed() && !old.Failed()
	disconnected := fact.Disconnected
	if boot && !fact.Down && old.OccurrenceID == "" {
		if owner, ok := sessions.(interface {
			sessionRecovery(context.Context, channelonboarding.Operation) (time.Time, bool, error)
		}); ok {
			at, connected, err := owner.sessionRecovery(ctx, item.Operation)
			if err != nil && !channelonboarding.SessionUnavailableOnly(err) {
				return err
			}
			if !down {
				down = err == nil && !connected && !at.IsZero()
				disconnected = down
			}
		}
	}
	if down {
		if disconnected {
			fmt.Fprintf(out, "ERROR WhatsApp disconnected: %s\n", fact.Cause())
		} else {
			fmt.Fprintf(out, "ERROR WhatsApp: %s\n", fact.Cause())
		}
	}
	return nil
}
