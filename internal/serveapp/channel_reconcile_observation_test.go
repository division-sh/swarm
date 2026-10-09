package serveapp

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/servedparity"
)

type channelReconcileObservation struct {
	mu     sync.Mutex
	store  render.Store
	mark   func() (render.ReconcileMark, bool)
	passes map[render.ReconcileDemand][2]render.ReconcileMark
}

func (o *channelReconcileObservation) configure(opts *cliapp.ServeOptions) {
	opts.TestChannelReconcileCadence.Ordinary = 25 * time.Millisecond
	opts.TestChannelReconcilePass = o.passed
	opts.TestChannelReconcileStarted = o.started
}

func (o *channelReconcileObservation) started(store render.Store, mark func() (render.ReconcileMark, bool)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.store, o.mark = store, mark
	o.passes = make(map[render.ReconcileDemand][2]render.ReconcileMark)
}

func (o *channelReconcileObservation) passed(pass render.ReconcilePass) {
	o.mu.Lock()
	defer o.mu.Unlock()
	prior := o.passes[pass.Scope]
	o.passes[pass.Scope] = [2]render.ReconcileMark{prior[1], pass.Start}
}

func (o *channelReconcileObservation) planCut(t *testing.T, predicate func(render.Candidate) bool) render.ReconcileMark {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for {
		o.mu.Lock()
		store, mark := o.store, o.mark
		o.mu.Unlock()
		if store == nil || mark == nil {
			t.Fatal("channel observer has no exact running Process subscription")
		}
		cursor := ""
		for {
			plans, err := store.ListCurrentChannelDeliveryPlans(ctx, cursor, 200)
			if err != nil {
				t.Fatal(err)
			}
			for _, plan := range plans {
				if predicate(plan) {
					cut, active := mark()
					if !active {
						t.Fatal("channel Process retired before the selected-store cut")
					}
					return cut
				}
			}
			if len(plans) < 200 {
				break
			}
			cursor = plans[len(plans)-1].DeliveryID
		}
		select {
		case <-ctx.Done():
			t.Fatal("expected exact channel settlement was not observable", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (o *channelReconcileObservation) twoAfter(t *testing.T, cut render.ReconcileMark, scope render.ReconcileDemand) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2200*time.Millisecond)
	defer cancel()
	for {
		o.mu.Lock()
		passes, mark := o.passes[scope], o.mark
		o.mu.Unlock()
		if _, active := mark(); !active {
			t.Fatal("channel Process retired before successful pass observation")
		}
		if passes[0].StartedAfter(cut) && passes[1].StartedAfter(cut) && passes[1].Pass > passes[0].Pass {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no two successful scope %d passes after exact cut %#v: %#v", scope, cut, passes)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (o *channelReconcileObservation) completedCurrentOrdinary(t *testing.T) (render.Store, render.ReconcileMark) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for {
		o.mu.Lock()
		store, mark, passes := o.store, o.mark, o.passes[render.ReconcileOrdinary]
		o.mu.Unlock()
		cut, active := mark()
		if !active {
			t.Fatal("exact channel Process is not running")
		}
		if passes[1] == cut {
			return store, cut
		}
		select {
		case <-ctx.Done():
			t.Fatalf("earlier channel hints did not finish a successful ordinary pass: %#v/%#v", cut, passes)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (o *channelReconcileObservation) oneAfter(t *testing.T, cut render.ReconcileMark, scope render.ReconcileDemand) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2200*time.Millisecond)
	defer cancel()
	for {
		o.mu.Lock()
		last, mark := o.passes[scope][1], o.mark
		o.mu.Unlock()
		if _, active := mark(); !active {
			t.Fatal("channel Process retired before successful pass observation")
		}
		if last.StartedAfter(cut) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no successful scope %d pass after exact cut %#v: %#v", scope, cut, last)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestChannelOnboardingSucceededWakeWithoutBackstopBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			h := newChannelOnboardingE2EHarness(t, backend, true)
			observation := &channelReconcileObservation{}
			observation.configure(&h.opts)
			// No repair tick can release the held final completion in this proof.
			h.opts.TestChannelReconcileCadence = render.ReconcileCadence{Ordinary: time.Hour, Native: time.Hour}
			h.start(t)
			t.Cleanup(func() { h.stop(t) })
			begun := startChannelOnboardingRPC(t, h, channelonboarding.VerbConnect, "succeeded-wake-token", map[string]string{"client_language": "en"})
			callback, signing, _ := h.provider.Registration()
			requireChannelClaimDisposition(t, "wake claimant", submitChannelOnboardingClaimAs(t, callback, signing,
				begun.IdentityOperation.Challenge, 70931, 7000, 1001, "wake_operator"), "consumed_by_binding")
			awaiting := retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "")
			var confirmed map[string]any
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{
				"operation_id": awaiting.IdentityOperation.OperationID, "expected_revision": awaiting.IdentityOperation.Revision, "approve": true,
			}, &confirmed)
			arrived, release := h.provider.PauseDeliveryResponseMatching(func(message map[string]any) bool {
				text, _ := message["text"].(string)
				return strings.HasPrefix(text, "Swarm channel connected.")
			})
			t.Cleanup(release)
			command := startChannelOnboardingCLICommand(t, h.opts.ConfigPath, h.endpoint,
				[]string{"channel", "resume", begun.Operation.OperationID, "--yes"}, "")
			select {
			case <-arrived:
			case <-time.After(15 * time.Second):
				t.Fatal("confirmation did not reach the held real HTTP response")
			}
			pending := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
			if pending.Operation.Phase != channelonboarding.PhaseDeliveringConfirmation {
				t.Fatalf("confirmation cut phase=%s", pending.Operation.Phase)
			}
			store, cut := observation.completedCurrentOrdinary(t)
			if id, found, err := store.CurrentChannelDeliveryActivationID(context.Background()); err != nil || found || id != "" {
				t.Fatalf("pre-completion activation=%s/%t error=%v", id, found, err)
			}
			if len(h.provider.CommandWrites()) != 0 {
				t.Fatal("native install ran before the succeeded commit")
			}
			release()
			requireChannelOnboardingCommandSuccess(t, command)
			succeeded := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
			if succeeded.Operation.Phase != channelonboarding.PhaseSucceeded {
				t.Fatalf("public completion phase=%s", succeeded.Operation.Phase)
			}
			activationID, found, err := store.CurrentChannelDeliveryActivationID(context.Background())
			if err != nil || !found || activationID == "" {
				t.Fatalf("post-completion activation=%s/%t error=%v", activationID, found, err)
			}
			entry := waitNativeInboxCommand(t, h.provider, "chat", "1001", "")
			callback, signing, _ = h.provider.Registration()
			postChannelTelegramUpdate(t, callback, signing, map[string]any{
				"update_id": 70932, "message": map[string]any{
					"message_id": 70932, "from": map[string]any{"id": 7000},
					"chat": map[string]any{"id": 1001, "type": "private"}, "text": "/" + entry,
				},
			})
			observation.planCut(t, func(plan render.Candidate) bool {
				return plan.SourceKind == "response" && plan.State == "sent" && plan.CurrentReceiptID != "" &&
					plan.RequestActivationID == activationID && !plan.RecoveryPending
			})
			observation.oneAfter(t, cut, render.ReconcileNative)
			observation.oneAfter(t, cut, render.ReconcileOrdinary)
		})
	}
}
