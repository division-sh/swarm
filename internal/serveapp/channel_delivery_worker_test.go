package serveapp

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimechanneldelivery "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/google/uuid"
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
		{DeliveryID: "sent", State: "sent", CurrentRenderID: "render", CurrentReceiptID: "receipt"},
		{DeliveryID: "uncertain", State: "uncertain"},
		{DeliveryID: "editing", State: "rendered", CurrentRenderID: "render", CurrentReceiptID: "receipt"},
	}, nil
}

func (*channelDeliveryWorkerStore) FreezeAndPersistChannelRender(_ context.Context, deliveryID string) (runtimechanneldelivery.PreparedRender, error) {
	return runtimechanneldelivery.PreparedRender{DeliveryID: deliveryID, RenderID: "render"}, nil
}

func (s *channelDeliveryWorkerStore) GetCurrentChannelDeliveryPlan(_ context.Context, deliveryID string) (runtimechanneldelivery.Candidate, bool, error) {
	plans, _ := s.ListCurrentChannelDeliveryPlans(context.Background(), "", 200)
	for _, plan := range plans {
		if plan.DeliveryID == deliveryID {
			return plan, true, nil
		}
	}
	return runtimechanneldelivery.Candidate{}, false, nil
}

func (*channelDeliveryWorkerStore) GetCurrentChannelSentReceipt(_ context.Context, deliveryID, operationID string) (runtimechanneldelivery.SentReceipt, bool, error) {
	return runtimechanneldelivery.SentReceipt{DeliveryID: deliveryID, OperationID: operationID, RenderID: "render", DeliveryReference: map[string]any{"id": 1}}, true, nil
}

func TestChannelDeliveryReconciliationPlansOpenCardsWithoutResending(t *testing.T) {
	selected := &channelDeliveryWorkerStore{}
	d := &serveChannelDeliveryDispatcher{store: selected, cards: channelDeliveryWorkerCards{}}
	if err := d.reconcileDeliveries(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"pending-a", "pending-b", "deferred-c"}
	if !reflect.DeepEqual(selected.planned, want) {
		t.Fatalf("planned cards = %v, want %v", selected.planned, want)
	}
}

type rejectedChannelActionStore struct {
	runtimechanneldelivery.Store
	pending []runtimechanneldelivery.PendingAction
	settled []runtimechanneldelivery.ActionDisposition
}

func (s *rejectedChannelActionStore) ListPendingChannelActions(context.Context, string, int) ([]runtimechanneldelivery.PendingAction, error) {
	return s.pending, nil
}

func (*rejectedChannelActionStore) ResolveChannelActionFact(context.Context, operatorchannel.ActionFact) (runtimechanneldelivery.ResolvedAction, bool, error) {
	return runtimechanneldelivery.ResolvedAction{}, false, nil
}

func (s *rejectedChannelActionStore) SettleUnappliedChannelAction(_ context.Context, _ operatorchannel.InboundAction, disposition runtimechanneldelivery.ActionDisposition) error {
	s.settled = append(s.settled, disposition)
	return nil
}

func TestChannelActionWorkerRejectsUnresolvedIntentWithoutMutation(t *testing.T) {
	store := &rejectedChannelActionStore{pending: []runtimechanneldelivery.PendingAction{{
		PublicationID: uuid.NewString(), ReceivedAt: time.Now().UTC(),
	}}}
	d := &serveChannelDeliveryDispatcher{store: store, cards: channelDeliveryWorkerCards{}}
	if err := d.reconcileCardActions(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(store.settled, []runtimechanneldelivery.ActionDisposition{runtimechanneldelivery.ActionRejected}) {
		t.Fatalf("unresolved callback dispositions = %v", store.settled)
	}
}
