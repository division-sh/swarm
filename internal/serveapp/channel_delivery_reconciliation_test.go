package serveapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
)

type channelWorkerControlStore struct {
	render.Store
	signal  render.ReconcileSignal
	actions func(context.Context) error
	texts   func() error
}

func (s *channelWorkerControlStore) SubscribeChannelReconciliation(ctx context.Context) (*render.ReconcileSubscription, error) {
	return s.signal.Subscribe(ctx)
}

func (s *channelWorkerControlStore) CurrentChannelCardChangeCursor(context.Context) (int64, bool, error) {
	return 0, false, nil
}

func (s *channelWorkerControlStore) CurrentChannelDeliveryActivationID(context.Context) (string, bool, error) {
	return "", false, nil
}

func (s *channelWorkerControlStore) ListPendingChannelTexts(context.Context, string, int) ([]render.PendingText, error) {
	if s.texts != nil {
		return nil, s.texts()
	}
	return nil, nil
}

func (s *channelWorkerControlStore) ListPendingChannelActions(ctx context.Context, _ string, _ int) ([]render.PendingAction, error) {
	if s.actions != nil {
		return nil, s.actions(ctx)
	}
	return nil, nil
}

type channelWorkerControlCards struct{ decisioncard.Store }

func (channelWorkerControlCards) ListDecisionCards(context.Context, decisioncard.ListOptions) ([]decisioncard.ListItem, string, error) {
	return nil, "", nil
}

type channelWorkerControlMailbox struct{ apiv1.MailboxAPIStore }
type channelWorkerControlProposed struct {
	decisioncard.ProposedEffectStore
}

func channelWorkerControlDispatcher(store *channelWorkerControlStore) *serveChannelDeliveryDispatcher {
	return &serveChannelDeliveryDispatcher{
		store: store, cards: channelWorkerControlCards{},
		mailbox: channelWorkerControlMailbox{}, proposedEffects: channelWorkerControlProposed{},
	}
}

func TestChannelWorkerKeepsInPassHintAndJoinsExactProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	process := worklifetime.NewProcess()
	t.Cleanup(func() {
		process.Retire()
		joinctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := process.Wait(joinctx); err != nil {
			t.Error(err)
		}
	})
	entered, release := make(chan struct{}), make(chan struct{})
	first := true
	store := &channelWorkerControlStore{actions: func(ctx context.Context) error {
		if first {
			first = false
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}}
	passes := make(chan render.ReconcilePass, 8)
	if err := startServeChannelDelivery(ctx, process, channelWorkerControlDispatcher(store), channelDeliveryWorkerOptions{
		cadence: render.ReconcileCadence{Ordinary: time.Hour, Native: time.Hour},
		passed:  func(pass render.ReconcilePass) { passes <- pass },
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cut, active := store.signal.Mark()
	if !active || cut.Pass != 1 {
		t.Fatalf("initial pass mark=%#v active=%t", cut, active)
	}
	// The action phase is after delivery scanning. Its commit must leave a
	// subsequent pass pending rather than being cleared at the first pass's end.
	if err := store.signal.PublishAcknowledged(true, render.ReconcileOrdinary); err != nil {
		t.Fatal(err)
	}
	close(release)
	for _, afterCut := range []bool{false, true} {
		select {
		case pass := <-passes:
			if pass.Scope != render.ReconcileOrdinary || pass.Start.StartedAfter(cut) != afterCut {
				t.Fatalf("pass=%#v cut=%#v afterCut=%t", pass, cut, afterCut)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	process.Retire()
	if err := process.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if _, active := store.signal.Mark(); active || process.ActiveCount() != 0 {
		t.Fatal("worker subscription or lease survived process join")
	}
	if err := store.signal.PublishAcknowledged(true, render.ReconcileOrdinary|render.ReconcileNative); err != nil {
		t.Fatal(err)
	}
	select {
	case pass := <-passes:
		t.Fatalf("retired worker credited a pass: %#v", pass)
	default:
	}
}

func TestChannelWorkerDoesNotCreditFailedPass(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	process := worklifetime.NewProcess()
	defer func() {
		process.Retire()
		if err := process.Wait(ctx); err != nil {
			t.Error(err)
		}
	}()
	first := true
	failed := errors.New("controlled text scan failure")
	failedPass := make(chan struct{})
	store := &channelWorkerControlStore{actions: func(context.Context) error {
		if first {
			first = false
			close(failedPass)
			return failed
		}
		return nil
	}}
	passes := make(chan render.ReconcilePass, 8)
	if err := startServeChannelDelivery(ctx, process, channelWorkerControlDispatcher(store), channelDeliveryWorkerOptions{
		cadence: render.ReconcileCadence{Ordinary: time.Hour, Native: time.Hour},
		passed:  func(pass render.ReconcilePass) { passes <- pass },
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-failedPass:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cut, _ := store.signal.Mark()
	if err := store.signal.PublishAcknowledged(true, render.ReconcileOrdinary); err != nil {
		t.Fatal(err)
	}
	select {
	case pass := <-passes:
		if pass.Scope != render.ReconcileOrdinary || !pass.Start.StartedAfter(cut) {
			t.Fatalf("failed or pre-cut pass credited: %#v, cut=%#v", pass, cut)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
