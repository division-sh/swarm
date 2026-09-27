package channeldelivery

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
)

// Candidate is a selected-store projection for one current destination.
// Dispatch must still acquire current activation and effect authority.
type Candidate struct {
	DeliveryID       string
	SourceKind       string
	SourceID         string
	BindingRevision  int64
	Audience         Audience
	State            string
	CurrentRenderID  string
	CurrentReceiptID string
}

type PreparedRender struct {
	RenderID   string
	DeliveryID string
	Frozen     Frozen
	Actions    []Action
}

type Action struct {
	Token   string
	Kind    string
	Verdict string
	Label   string
}

type ResolvedAction struct {
	Action             Action
	DeliveryID         string
	RenderID           string
	RenderHash         string
	ReceiptOperationID string
	SourceKind         string
	SourceID           string
	PrincipalID        string
	BindingRevision    int64
	ActivationID       string
	ActivationRevision int64
	CurrentRender      bool
}

type CardActionDemand struct {
	CardID             string
	PrincipalID        string
	Method             string
	Verdict            string
	ReceiptOperationID string
	RenderHash         string
}

type PendingAction struct {
	PublicationID string
	Fact          operatorchannel.InboundAction
	ReceivedAt    time.Time
}

type ActionDisposition string

const (
	ActionApplied      ActionDisposition = "applied"
	ActionInputStarted ActionDisposition = "input_started"
	ActionStale        ActionDisposition = "stale"
	ActionRejected     ActionDisposition = "rejected"
	ActionUnsupported  ActionDisposition = "unsupported"
)

func (d ActionDisposition) Valid() bool {
	switch d {
	case ActionApplied, ActionInputStarted, ActionStale, ActionRejected, ActionUnsupported:
		return true
	default:
		return false
	}
}

type Store interface {
	CurrentChannelDeliveryActivationID(context.Context) (string, bool, error)
	ListCurrentChannelDeliveryPlans(context.Context, string, int) ([]Candidate, error)
	GetCurrentChannelDeliveryPlan(context.Context, string) (Candidate, bool, error)
	PlanOpenChannelCard(context.Context, string) (bool, error)
	FreezeAndPersistChannelRender(context.Context, string) (PreparedRender, error)
	ResolveChannelActionFact(context.Context, operatorchannel.ActionFact) (ResolvedAction, bool, error)
	ListPendingChannelActions(context.Context, string, int) ([]PendingAction, error)
}
