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
)

func newServeChannelProviderOwners(manager *runtime.RuntimeContextManager, store channelonboarding.Store,
	current *credentials.SnapshotOwner, directory string,
) (serveSessionBootstrapOwner, operatorchannel.CredentialCurrentness, error) {
	sessions, err := newServeSessionBootstrap(serveSessionBootstrapRuntimeSelector(manager), store, current, directory)
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
