package serveapp

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type inputPromptAuthorizationCut struct {
	*credentials.FileStore
	armed   atomic.Bool
	entered chan effects.Authority
	release chan struct{}
}

func (s *inputPromptAuthorizationCut) Snapshot(ctx context.Context, key string) (credentials.AtomicSnapshot, error) {
	a, ok := effects.AuthorityFromContext(ctx)
	if ok && a.Kind == effects.AuthorityChannelDelivery && a.ChannelDelivery.PreviousReceiptOperationID == "" && s.armed.CompareAndSwap(true, false) {
		s.entered <- a
		select {
		case <-s.release:
		case <-ctx.Done():
			return credentials.AtomicSnapshot{}, ctx.Err()
		}
	}
	return s.FileStore.Snapshot(ctx, key)
}

func proveObsoleteInputPromptAtOrdinaryStop(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	h := prepareChannelAnchorPublicJourney(t, backend, "draft-stop-token", false, 0, nil)
	cut := &inputPromptAuthorizationCut{entered: make(chan effects.Authority, 1), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(cut.release) })
	var mark func() (render.ReconcileMark, bool)
	h.opts.TestChannelReconcileStarted = func(_ render.Store, markCurrent func() (render.ReconcileMark, bool)) { mark = markCurrent }
	var selected *selectedStoreOwner
	previous := projectRuntimePersistenceForServe
	projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
		selected = owner
		return previous(owner)
	}
	t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
	var diagnostics lockedBuffer
	logOutput := log.Writer()
	log.SetOutput(io.MultiWriter(logOutput, &diagnostics))
	t.Cleanup(func() { log.SetOutput(logOutput) })
	h.process = startRuntimeTestProcessWithRunner(t, repoRootForTest(), h.opts, func(ctx context.Context, root string, opts cliapp.ServeOptions) int {
		code, err := runOwnedLifecycle(ctx, root, t.TempDir(), opts,
			apiv1.AuthTokenResolution{Tokens: []string{apiv1.DefaultLoopbackAPIToken}, Explicit: true},
			executionposture.Live, "", func(base credentials.Store) credentials.Store {
				cut.FileStore = base.(*credentials.FileStore)
				return cut
			})
		if err != nil {
			fmt.Fprintln(opts.Output, err)
		}
		return code
	})
	defer release()
	h.process.waitForReadyLine()
	h.endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, h.process.outputString())
	observation := openChannelAnchorObservation(t, h)
	seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
		"event_name": "work.requested", "bundle_hash": channelAnchorBundleHash(t, h.rpcEndpoint()),
		"run_id": seedServedPublicationRun(t, h.process.runtime), "payload": map[string]any{"seed": true},
		"idempotency_key": "draft-stop-seed",
	})
	cardID := waitChannelPublicCard(t, h, seed.RunID, decisioncard.AnchorKindStageGate, "chat/reviews")
	message := waitChannelAnchorObservedReceipt(t, observation, cardID)
	reject, found := telegramCallbackToken(h.provider.Delivery(message-1), "Reject")
	if !found {
		t.Fatal("card has no required-input verdict")
	}
	cut.armed.Store(true)
	postUncertainCopyCallback(t, h, message, reject, 902001)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	var authority effects.Authority
	select {
	case authority = <-cut.entered:
	case <-ctx.Done():
		t.Fatal("exact ordinary send did not reach HTTP preflight")
	}
	d := authority.ChannelDelivery
	eventID := channelFixtureProviderEventID(t, "902001")
	intent, found, err := observation.ObserveChannelIntent(ctx, render.IntentObservationQuery{
		Kind: render.IntentAction, Provider: "telegram", ProviderEventID: eventID, InterfaceKey: d.InterfaceKey,
	})
	if err != nil || !found || intent.State != "settled" || intent.Disposition != "input_started" {
		t.Fatalf("held send lacks its exact started input: %+v %t %v", intent, found, err)
	}
	source := uuid.NewSHA1(uuid.MustParse(h.registration.OperationID), []byte("deployment-operator-input:"+eventID)).String()
	plan, found, err := selected.ChannelDelivery().GetCurrentChannelDeliveryPlan(ctx, d.DeliveryID)
	if err != nil || !found || plan.SourceKind != "response" || plan.SourceID != source || plan.RequestParent != d.Parent ||
		plan.State != "rendered" || plan.CurrentRenderID != d.RenderID || plan.CurrentReceiptID != "" || plan.RecoveryPending {
		t.Fatalf("held send is not the exact queued prompt: %+v %t %v", plan, found, err)
	}
	cards := selected.RuntimeDeps().DecisionCards
	draft := requireDraftChange(t, ctx, cards, cardID, decisioncard.ChangeDraftStarted)
	if current, err := selected.Effects().IsExternalEffectAuthorityCurrent(ctx, authority); err != nil || !current {
		t.Fatalf("held prompt was not current before decision: %t %v", current, err)
	}
	_, providerSends := h.provider.Counts()
	requireServedOKJSONRPC(t, h.rpcEndpoint(), "mailbox.decide", lifecycleDecisionParamsFromEndpoint(t, h.rpcEndpoint(), cardID, "approve"))
	if cancelled := requireDraftChange(t, ctx, cards, cardID, decisioncard.ChangeDraftCancelled); cancelled != draft {
		t.Fatal("decision did not cancel the exact held draft")
	}
	if current, err := selected.Effects().IsExternalEffectAuthorityCurrent(ctx, authority); err != nil || current {
		t.Fatalf("decision did not obsolete the held prompt: %t %v", current, err)
	}
	if _, active := mark(); !active {
		t.Fatal("ordinary source retired before test requested stop")
	}
	h.process.cancel()
	for {
		if _, active := mark(); !active {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("ordinary selection did not close with the send held")
		case <-time.After(time.Millisecond):
		}
	}
	release()
	code := h.process.stop()
	requireNoPromptEffect(t, ctx, h.opts.StoreMode, h.storeDSN, observation, authority)
	if _, sends := h.provider.Counts(); sends != providerSends {
		t.Fatalf("refused prompt reached provider: sends=%d/%d", sends, providerSends)
	}
	const refusal = "platform.superseded_generation external_effect_authority_stale component=external-effects operation=check_authority"
	if !strings.Contains(diagnostics.String()+h.process.outputString(), refusal) {
		t.Fatalf("exact pre-authorization refusal not observed: %s", h.process.outputString())
	}
	if code != 0 {
		t.Fatalf("obsoleted ordinary prompt failed graceful stop: exit=%d\n%s", code, h.process.outputString())
	}
}

func requireDraftChange(t *testing.T, ctx context.Context, cards decisioncard.Store, cardID, kind string) string {
	t.Helper()
	changes, err := cards.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 200})
	if err != nil {
		t.Fatal(err)
	}
	var draft string
	for _, change := range changes {
		if change.CardID == cardID && change.ChangeType == kind {
			payload, _ := change.Payload.Interface().(map[string]any)
			next, _ := payload["input_draft_id"].(string)
			if draft != "" || uuid.Validate(next) != nil {
				t.Fatalf("ambiguous or invalid %s for exact card", kind)
			}
			draft = next
		}
	}
	if draft == "" {
		t.Fatalf("missing %s for exact card", kind)
	}
	return draft
}

func requireNoPromptEffect(t *testing.T, ctx context.Context, driver, location string, observation render.Observer, authority effects.Authority) {
	t.Helper()
	inspect, err := storetest.OpenReleaseProcessReadOnlyInspection(driver, location)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inspect.Close() })
	if err := inspect.InspectSnapshot(ctx, func(readCtx context.Context) error {
		outcome, found, err := inspect.(effects.OutcomeStore).GetExternalEffectOutcome(readCtx, authority.ID)
		if err != nil || found {
			return fmt.Errorf("refused pre-authorization prompt has journal evidence: %+v %t %v", outcome, found, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if receipt, found, err := observation.GetCurrentChannelSentReceipt(ctx, authority.ChannelDelivery.DeliveryID, authority.ID); err != nil || found {
		t.Fatalf("refused prompt has a receipt: %+v %t %v", receipt, found, err)
	}
}
