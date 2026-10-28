package sessionprovider

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	inbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
)

var ErrAccountUnclaimed = errors.New("account_unclaimed")

// RuntimeIncomingOptions supplies existing runtime owners, not raw capture,
// a connected flag or a caller-defined authority issuer.
type RuntimeIncomingOptions struct {
	Alias   string
	Trigger providertriggers.InboundAdmissionPlan
	Bus     *bus.EventBus
	Posture executionposture.Posture
}

// RegisteredBusinessBinding borrows the existing current run and source. The
// SDK owner still validates its original deployment account and occurrence.
type RegisteredBusinessBinding struct {
	Context        context.Context
	Incoming       RuntimeIncomingOptions
	Coordinate     channelonboarding.ChannelRuntimeContextCoordinate
	TargetSelector string
	Publication    inbound.BindingGeneration
	Current        func(context.Context) error
	Release        func() error
}

type RegisteredBusinessSelector func(context.Context, channelonboarding.Operation) (RegisteredBusinessBinding, error)
