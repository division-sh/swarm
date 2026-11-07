package channeldelivery

import (
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
)

// DeploymentInput retains existing provider-owned admission. Exported account
// or parent values alone cannot construct an authenticated request.
type DeploymentInput struct {
	Parent          channelnative.TransportParent
	Account         channelonboarding.AccountReference
	PrincipalID     string
	BindingRevision int64
	Session         operatorchannel.SessionAccountAdmission
	Channel         packs.SatisfactionPlan
	Trigger         providertriggers.InboundAdmissionPlan
	Input           providertriggers.AdmittedRequest
	ReceivedAt      time.Time
}

type DeploymentInputReceipt struct {
	Parent          channelnative.TransportParent
	ProviderEventID string
	Fingerprint     string
}
