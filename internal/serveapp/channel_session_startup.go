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
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/sessionprovider"
)

func newServeChannelProviderOwners(manager *runtime.RuntimeContextManager, store channelonboarding.Store,
	current *credentials.SnapshotOwner, directory string, invocationState *sessionprovider.InvocationState,
) (serveSessionBootstrapOwner, operatorchannel.CredentialCurrentness, error) {
	sessions, err := newServeSessionBootstrap(serveSessionBootstrapRuntimeSelector(manager), store, current, directory, invocationState)
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

// Recovery uses the same fenced, joined session owner as startup and explicit
// connect. The SDK's independent auto-reconnect remains disabled.
func startServeChannelSessionRecovery(ctx context.Context, owner *worklifetime.Process, manager *runtime.RuntimeContextManager, store channelonboarding.Store, onboarding *channelonboarding.Service, out io.Writer) (func(), error) {
	catalog, err := serveChannelOnboardingCatalog(manager)
	if err != nil {
		return nil, err
	}
	installed := false
	for _, candidate := range catalog.Candidates() {
		installed = installed || candidate.Provider == "whatsapp"
	}
	if !installed {
		return func() {}, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	lease, err := owner.Begin(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = lease.Done() }()
		ctx := lease.Context()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		states := map[string]string{}
		attempts := map[string]int{}
		for {
			operations, err := store.ListChannelOnboardingOperations(ctx)
			restore := false
			if err == nil {
				for _, op := range operations {
					if op.Provider != "whatsapp" || op.Phase != channelonboarding.PhaseSucceeded || op.SessionAccount.Validate() != nil {
						continue
					}
					if current, err := channelonboarding.RetainedSessionCurrent(ctx, store, op); err != nil || !current {
						continue
					}
					result, err := onboarding.Get(ctx, op.OperationID)
					if err != nil || result.Connection == nil {
						continue
					}
					state := result.Connection.State
					if state != states[op.OperationID] {
						if state == "connected" {
							fmt.Fprintf(out, "WhatsApp: reconnected (%s)\n", result.Connection.AccountPresentation)
							attempts[op.OperationID] = 0
						} else {
							fmt.Fprintln(out, "WhatsApp disconnected: network unreachable, reconnecting...")
						}
						states[op.OperationID] = state
					}
					if state == "disconnected" && attempts[op.OperationID] < 3 {
						attempts[op.OperationID]++
						restore = true
					}
				}
			}
			if restore {
				_ = onboarding.RestoreSessions(ctx)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() { cancel(); <-done }, nil
}

func restoreServeChannelStartup(ctx context.Context, manager *runtime.RuntimeContextManager, store channelonboarding.Store,
	teardown *channelonboarding.DestructiveService, onboarding *channelonboarding.Service, identities *operatorchannel.Service, now time.Time,
) ([]operatorchannel.Binding, error) {
	if err := reconcileRetiredConnectedChannelContexts(ctx, manager, store, teardown); err != nil {
		return nil, err
	}
	if err := onboarding.RestoreSessions(ctx); err != nil {
		return nil, fmt.Errorf("restore native ownership before retained channel proofs: %w", err)
	}
	_, bindings, err := identities.Bootstrap(ctx, now)
	return bindings, err
}
