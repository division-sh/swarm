package channeldelivery

import "context"

type IntentKind string

const (
	IntentText   IntentKind = "text"
	IntentAction IntentKind = "action"
)

type IntentObservationQuery struct {
	Kind            IntentKind
	Provider        string
	ProviderEventID string
	InterfaceKey    string
}

// IntentObservation contains settlement metadata, never provider credentials,
// private entered answers or executable action authority.
type IntentObservation struct {
	State       string
	Disposition string
}

type Observer interface {
	ObserveChannelIntent(context.Context, IntentObservationQuery) (IntentObservation, bool, error)
	ListCurrentChannelDeliveryPlans(context.Context, string, int) ([]Candidate, error)
	GetCurrentChannelSentReceipt(context.Context, string, string) (SentReceipt, bool, error)
}
