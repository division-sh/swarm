package cliapp

import (
	"context"
	"net/http"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/credentials"
)

// ChannelRegistrationSession is the invocation-owned composition of existing
// session/identity ports. It grants no runtime activation or business execution.
type ChannelRegistrationSession interface {
	channelonboarding.SessionBootstrapOwner
	operatorchannel.CredentialCurrentness
	operatorchannel.SessionAdmissionOwner
	channelonboarding.DeploymentSessionOwner
	PollOperatorClaim(context.Context, channelonboarding.Operation) error
	ConsumeWebhookClaim(context.Context, channelonboarding.AccountReference, func(channelonboarding.CredentialAdmission) bool, func() (providertriggers.Delivery, error), func() bool) (bool, error)
	ReadSessionConnection(context.Context, channelonboarding.Operation) (channelonboarding.SessionConnectionReadback, error)
	Close(context.Context) error
}

type ChannelRegistrationOptions struct {
	Directory      string
	Principal      operatorchannel.Principal
	Process        *worklifetime.Process
	Operations     channelonboarding.OperationStore
	Store          channelonboarding.Store
	Credentials    *credentials.SnapshotOwner
	Identities     operatorchannel.Store
	HTTP           *http.Client
	Plan           packs.SatisfactionPlan
	Trigger        providertriggers.InboundAdmissionPlan
	CurrentWebhook func(context.Context, string, string) bool
}

type ChannelRegistrationFactory func(context.Context, ChannelRegistrationOptions) (ChannelRegistrationSession, error)
