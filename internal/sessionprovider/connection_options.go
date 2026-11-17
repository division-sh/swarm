package sessionprovider

import (
	"context"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

type RuntimeConnectionOptions struct {
	operatorAccount    channelonboarding.AccountReference
	operatorBinding    func(context.Context, channelonboarding.Operation) (channelonboarding.Result, error)
	Directory          string
	InvocationState    *InvocationState
	OperationID        string
	Store              channelonboarding.Store
	Credentials        operatorchannel.CredentialCurrentness
	Plan               packs.SatisfactionPlan
	Incoming           *RuntimeIncomingOptions
	RegisteredBusiness RegisteredBusinessSelector
	ProcessBinding     processbinding.Binding
}

type DeploymentConnectionOptions struct {
	OperatorAccount    channelonboarding.AccountReference
	OperatorBinding    func(context.Context, channelonboarding.Operation) (channelonboarding.Result, error)
	RegisteredBusiness RegisteredBusinessSelector
	Directory          string
	OperationID        string
	Principal          operatorchannel.Principal
	Process            *worklifetime.Process
	Operations         channelonboarding.OperationStore
	Store              channelonboarding.Store
	Credentials        operatorchannel.CredentialCurrentness
	Plan               packs.SatisfactionPlan
	Trigger            providertriggers.InboundAdmissionPlan
}
