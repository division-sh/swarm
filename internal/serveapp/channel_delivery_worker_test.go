package serveapp

import (
	"context"
	"errors"
	"fmt"
	"github.com/division-sh/swarm/internal/packs"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimechanneldelivery "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/google/uuid"
)

type channelDraftChoiceDispositionStore struct {
	runtimechanneldelivery.Store
	previewError error
	disposition  runtimechanneldelivery.ActionDisposition
	teaching     bool
}

func (s *channelDraftChoiceDispositionStore) PreviewChosenChannelInputDraftText(context.Context, operatorchannel.InboundAction, time.Time) (runtimechanneldelivery.InputDraftCandidate, runtimechanneldelivery.PendingText, decisioncard.InputFieldProgress, string, error) {
	return runtimechanneldelivery.InputDraftCandidate{}, runtimechanneldelivery.PendingText{}, decisioncard.InputFieldProgress{}, "", s.previewError
}

func (s *channelDraftChoiceDispositionStore) SettleUnappliedChannelAction(_ context.Context, _ operatorchannel.InboundAction, disposition runtimechanneldelivery.ActionDisposition) error {
	s.disposition = disposition
	return nil
}

func (s *channelDraftChoiceDispositionStore) PlanChannelActionResponse(context.Context, operatorchannel.InboundAction, runtimechanneldelivery.ResolvedAction, string) (string, error) {
	s.teaching = true
	return "teaching", nil
}

func TestChannelDraftChoiceDispositionPreservesOwnerErrors(t *testing.T) {
	storageFailure := errors.New("selected-store unavailable")
	for _, test := range []struct {
		name            string
		err             error
		stale, teaching bool
	}{
		{"completed_or_expired", fmt.Errorf("chosen draft unavailable: %w", decisioncard.ErrDraftNotAuthority), true, false},
		{"invalid_answer", decisioncard.ErrInvalidInput, false, true},
		{"storage_failure", storageFailure, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &channelDraftChoiceDispositionStore{previewError: test.err}
			d := &serveChannelDeliveryDispatcher{store: store}
			err := d.processDraftChoice(context.Background(), runtimechanneldelivery.PendingAction{}, runtimechanneldelivery.ResolvedAction{})
			if test.stale && (err != nil || store.disposition != runtimechanneldelivery.ActionStale) {
				t.Fatalf("obsolete choice not settled stale: %v/%s", err, store.disposition)
			}
			if store.teaching != test.teaching || !test.stale && store.disposition != "" {
				t.Fatal("choice classification changed its owner disposition")
			}
			if test.name == "storage_failure" && !errors.Is(err, storageFailure) {
				t.Fatal("storage failure was swallowed as staleness")
			}
		})
	}
}

type channelDeliveryWorkerCards struct {
	decisioncard.Store
	changes []decisioncard.Change
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

func (c channelDeliveryWorkerCards) ListDecisionCardChanges(_ context.Context, opts decisioncard.SubscriptionOptions) ([]decisioncard.Change, error) {
	var out []decisioncard.Change
	for _, change := range c.changes {
		if change.Sequence > opts.After {
			out = append(out, change)
			if len(out) == opts.Limit {
				break
			}
		}
	}
	return out, nil
}

type channelDeliveryWorkerStore struct {
	runtimechanneldelivery.Store
	planned         []string
	changeCursor    int64
	changed         []string
	failOnSequence  int64
	freezeFailure   string
	freezes         []string
	readReceipts    []string
	currentOverride *runtimechanneldelivery.Candidate
	absent          bool
}

func (s *channelDeliveryWorkerStore) PlanOpenChannelCard(_ context.Context, cardID string) (bool, error) {
	s.planned = append(s.planned, cardID)
	return true, nil
}

func (s *channelDeliveryWorkerStore) CurrentChannelCardChangeCursor(context.Context) (int64, bool, error) {
	return s.changeCursor, true, nil
}

func (s *channelDeliveryWorkerStore) PlanChangedChannelCard(_ context.Context, sequence int64, cardID string) error {
	if sequence == s.failOnSequence {
		return fmt.Errorf("injected selected-store failure")
	}
	s.changeCursor = sequence
	s.changed = append(s.changed, cardID)
	return nil
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

func (s *channelDeliveryWorkerStore) FreezeAndPersistChannelRender(_ context.Context, deliveryID string, _ packs.PresentationBounds) (runtimechanneldelivery.PreparedRender, error) {
	s.freezes = append(s.freezes, deliveryID)
	if deliveryID == s.freezeFailure {
		return runtimechanneldelivery.PreparedRender{}, fmt.Errorf("injected plan-local failure")
	}
	return runtimechanneldelivery.PreparedRender{DeliveryID: deliveryID, RenderID: "render"}, nil
}

func (s *channelDeliveryWorkerStore) GetCurrentChannelDeliveryPlan(_ context.Context, deliveryID string) (runtimechanneldelivery.Candidate, bool, error) {
	if s.absent {
		return runtimechanneldelivery.Candidate{}, false, nil
	}
	if s.currentOverride != nil {
		return *s.currentOverride, true, nil
	}
	plans, _ := s.ListCurrentChannelDeliveryPlans(context.Background(), "", 200)
	for _, plan := range plans {
		if plan.DeliveryID == deliveryID {
			return plan, true, nil
		}
	}
	return runtimechanneldelivery.Candidate{}, false, nil
}

func TestChannelDeliveryWorkerRechecksResponsibilityBeforeRender(t *testing.T) {
	for _, cut := range []string{"source_lost", "accepted_work"} {
		t.Run(cut, func(t *testing.T) {
			selected := &channelDeliveryWorkerStore{absent: cut == "source_lost"}
			if cut == "accepted_work" {
				selected.currentOverride = &runtimechanneldelivery.Candidate{DeliveryID: "stale", State: "rendered", RecoveryPending: true}
			}
			d := &serveChannelDeliveryDispatcher{store: selected}
			if err := d.reconcileDelivery(context.Background(), runtimechanneldelivery.Candidate{DeliveryID: "stale", State: "planned"}); err != nil {
				t.Fatal(err)
			}
			if len(selected.freezes) != 0 || len(selected.readReceipts) != 0 {
				t.Fatal("stale or recovery-owned candidate reached render/dispatch")
			}
		})
	}
}

func (s *channelDeliveryWorkerStore) GetCurrentChannelSentReceipt(_ context.Context, deliveryID, operationID string) (runtimechanneldelivery.SentReceipt, bool, error) {
	s.readReceipts = append(s.readReceipts, deliveryID)
	return runtimechanneldelivery.SentReceipt{DeliveryID: deliveryID, OperationID: operationID, RenderID: "render", DeliveryReference: map[string]any{"id": 1}}, true, nil
}

func TestChannelDeliveryReconciliationPlansOpenCardsButRejectsUncompiledBounds(t *testing.T) {
	selected := &channelDeliveryWorkerStore{}
	d := &serveChannelDeliveryDispatcher{store: selected, cards: channelDeliveryWorkerCards{}}
	if err := d.reconcileDeliveries(context.Background()); err == nil || !strings.Contains(err.Error(), "binding owners are unavailable") {
		t.Fatalf("render admitted without its compiled bounds owner: %v", err)
	}
	want := []string{"pending-a", "pending-b", "deferred-c"}
	if !reflect.DeepEqual(selected.planned, want) {
		t.Fatalf("planned cards = %v, want %v", selected.planned, want)
	}
	if len(selected.freezes) != 0 || len(selected.readReceipts) != 0 {
		t.Fatal("uncompiled plan reached rendering or delivery")
	}
}

func TestChannelDeliveryReconciliationContinuesAfterPlanLocalAdmissionFailure(t *testing.T) {
	selected := &channelDeliveryWorkerStore{}
	d := &serveChannelDeliveryDispatcher{store: selected, cards: channelDeliveryWorkerCards{}}
	err := d.reconcileDeliveries(context.Background())
	if err == nil || !strings.Contains(err.Error(), "select channel delivery sent presentation bounds") ||
		!strings.Contains(err.Error(), "select channel delivery editing presentation bounds") {
		t.Fatalf("one plan's admission failure starved another: %v", err)
	}
	if len(selected.freezes) != 0 || len(selected.readReceipts) != 0 {
		t.Fatal("uncompiled plan reached rendering or delivery")
	}
}

func TestChannelDeliveryChangeCursorResumesAfterPlanningFailure(t *testing.T) {
	selected := &channelDeliveryWorkerStore{failOnSequence: 2}
	d := &serveChannelDeliveryDispatcher{store: selected, cards: channelDeliveryWorkerCards{changes: []decisioncard.Change{
		{Sequence: 1, CardID: "first"}, {Sequence: 2, CardID: "second"},
	}}}
	if err := d.reconcileCardChanges(context.Background()); err == nil {
		t.Fatal("second change failure was ignored")
	}
	if selected.changeCursor != 1 || !reflect.DeepEqual(selected.changed, []string{"first"}) {
		t.Fatalf("committed progress after failure = cursor %d, cards %v", selected.changeCursor, selected.changed)
	}
	selected.failOnSequence = 0
	if err := d.reconcileCardChanges(context.Background()); err != nil {
		t.Fatal(err)
	}
	if selected.changeCursor != 2 || !reflect.DeepEqual(selected.changed, []string{"first", "second"}) {
		t.Fatalf("resumed progress = cursor %d, cards %v", selected.changeCursor, selected.changed)
	}
}

func TestChannelDeliveryChangeCursorCrossesPageBoundaryAndResumes(t *testing.T) {
	changes := make([]decisioncard.Change, 205)
	for i := range changes {
		changes[i] = decisioncard.Change{Sequence: int64(i + 1), CardID: fmt.Sprintf("card-%d", i+1)}
	}
	selected := &channelDeliveryWorkerStore{failOnSequence: 201}
	d := &serveChannelDeliveryDispatcher{store: selected, cards: channelDeliveryWorkerCards{changes: changes}}
	if err := d.reconcileCardChanges(context.Background()); err == nil {
		t.Fatal("failure at the second page was ignored")
	}
	if selected.changeCursor != 200 || len(selected.changed) != 200 {
		t.Fatalf("first committed page = cursor %d, planned %d", selected.changeCursor, len(selected.changed))
	}
	selected.failOnSequence = 0
	if err := d.reconcileCardChanges(context.Background()); err != nil {
		t.Fatal(err)
	}
	if selected.changeCursor != 205 || len(selected.changed) != 205 || selected.changed[200] != "card-201" {
		t.Fatalf("resumed second page = cursor %d, planned %d", selected.changeCursor, len(selected.changed))
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
