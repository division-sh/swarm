package serveapp

import (
	"context"
	"reflect"
	"testing"

	runtimechanneldelivery "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
)

type channelDeliveryWorkerCards struct {
	decisioncard.Store
}

func (channelDeliveryWorkerCards) ListDecisionCards(_ context.Context, opts decisioncard.ListOptions) ([]decisioncard.ListItem, string, error) {
	switch {
	case opts.Status == decisioncard.StatusPending && opts.Cursor == "":
		return []decisioncard.ListItem{{CardID: "pending-a"}}, "next", nil
	case opts.Status == decisioncard.StatusPending && opts.Cursor == "next":
		return []decisioncard.ListItem{{CardID: "pending-b"}}, "", nil
	case opts.Status == "deferred" && opts.Cursor == "":
		return []decisioncard.ListItem{{CardID: "deferred-c"}}, "", nil
	default:
		panic("unexpected card page")
	}
}

type channelDeliveryWorkerStore struct {
	runtimechanneldelivery.Store
	planned []string
}

func (s *channelDeliveryWorkerStore) PlanOpenChannelCard(_ context.Context, cardID string) (bool, error) {
	s.planned = append(s.planned, cardID)
	return true, nil
}

func (*channelDeliveryWorkerStore) CurrentChannelDeliveryActivationID(context.Context) (string, bool, error) {
	return "activation", true, nil
}

func (*channelDeliveryWorkerStore) ListCurrentChannelDeliveryPlans(context.Context, string, int) ([]runtimechanneldelivery.Candidate, error) {
	return []runtimechanneldelivery.Candidate{
		{DeliveryID: "sent", State: "sent", CurrentReceiptID: "receipt"},
		{DeliveryID: "uncertain", State: "uncertain"},
		{DeliveryID: "editing", State: "rendered", CurrentReceiptID: "receipt"},
	}, nil
}

func TestChannelDeliveryReconciliationPlansOpenCardsWithoutResending(t *testing.T) {
	selected := &channelDeliveryWorkerStore{}
	d := &serveChannelDeliveryDispatcher{store: selected, cards: channelDeliveryWorkerCards{}}
	if err := d.reconcileInitialDeliveries(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"pending-a", "pending-b", "deferred-c"}
	if !reflect.DeepEqual(selected.planned, want) {
		t.Fatalf("planned cards = %v, want %v", selected.planned, want)
	}
}
