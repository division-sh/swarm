package serveapp

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
)

// Platform code receives the existing owner's held construction context, not
// the runtime manager's unrelated persistence-bearing implementation fields.
type serveSessionBootstrapRuntime func(context.Context, channelonboarding.ChannelRuntimeContextCoordinate) (context.Context, func() error, error)

// One private platform constructor, initialized only by the compiled native
// implementation. There is no registration API or pack-selected Go callback.
var newServeSessionBootstrap = unavailableServeSessionBootstrap

func unavailableServeSessionBootstrap(serveSessionBootstrapRuntime, channelonboarding.Store,
	operatorchannel.CredentialCurrentness, string,
) (channelonboarding.SessionBootstrapOwner, error) {
	return nil, &operatorchannel.SessionProviderUnavailableError{Provider: "whatsapp"}
}

func serveSessionBootstrapRuntimeSelector(manager *runtime.RuntimeContextManager) serveSessionBootstrapRuntime {
	if manager == nil {
		return nil
	}
	return func(ctx context.Context, coordinate channelonboarding.ChannelRuntimeContextCoordinate) (context.Context, func() error, error) {
		use, lookup, err := manager.AcquireBundleHash(ctx, coordinate.BundleHash)
		if err != nil || !lookup.Loaded() || use == nil {
			return nil, nil, errors.Join(channelonboarding.ErrRevisionConflict, err)
		}
		if !coordinate.MatchesContextOccurrence(use.Context.RuntimeInstanceID, use.Context.PublicationGeneration) {
			return nil, nil, errors.Join(channelonboarding.ErrRevisionConflict, use.Done())
		}
		// Construction holds this use until the connection owns its real runtime
		// work. Settling the use cannot cancel the durable connection context.
		owned := correlation.WithSourceArtifactFact(context.WithoutCancel(use.WorkContext()), use.Context.SourceArtifactFact)
		return owned, use.Done, nil
	}
}
