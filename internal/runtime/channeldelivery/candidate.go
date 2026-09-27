package channeldelivery

import "context"

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

type Store interface {
	CurrentChannelDeliveryActivationID(context.Context) (string, bool, error)
	ListCurrentChannelDeliveryPlans(context.Context, string, int) ([]Candidate, error)
	GetCurrentChannelDeliveryPlan(context.Context, string) (Candidate, bool, error)
	PlanOpenChannelCard(context.Context, string) (bool, error)
	FreezeAndPersistChannelRender(context.Context, string) (PreparedRender, error)
}
