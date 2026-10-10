package channeldelivery

import (
	"context"
	"fmt"
	"strings"
)

type IntentKind string

const (
	IntentText   IntentKind = "text"
	IntentAction IntentKind = "action"
)

type IntentObservationQuery struct {
	Kind                  IntentKind
	Provider              string
	ProviderEventID       string
	MessageReference      string
	ConversationReference string
	InterfaceKey          string
}

func (q IntentObservationQuery) Validate() error {
	byMessage := q.MessageReference != ""
	if strings.TrimSpace(q.Provider) == "" || q.InterfaceKey == "" ||
		q.Kind != IntentText && q.Kind != IntentAction ||
		byMessage && (q.ProviderEventID != "" || q.ConversationReference == "" || q.Kind != IntentText) ||
		!byMessage && (strings.TrimSpace(q.ProviderEventID) == "" || q.ConversationReference != "") {
		return fmt.Errorf("channel intent observation requires exact provider/interface and one event or text-message/conversation selector")
	}
	return nil
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
