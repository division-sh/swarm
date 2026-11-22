package serveapp

import (
	"context"
	"database/sql"
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
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/servedparity"
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
	var native effects.Store
	previous := projectRuntimePersistenceForServe
	projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
		native = owner.Effects()
		return previous(owner)
	}
	t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
	var diagnostics lockedBuffer
	logOutput := log.Writer()
	log.SetOutput(io.MultiWriter(logOutput, &diagnostics))
	t.Cleanup(func() { log.SetOutput(logOutput) })
	h.process = startRuntimeTestProcessWithRunner(t, repoRootForTest(), h.opts, func(ctx context.Context, root string, opts cliapp.ServeOptions) int {
		code, err := runOwnedLifecycleWithProviderStore(ctx, root, t.TempDir(), opts,
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
	driver := "sqlite"
	if backend == servedparity.BackendExplicitPostgres {
		driver = "postgres"
	}
	db, err := sql.Open(driver, h.storeDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	seed := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
		"event_name": "work.requested", "bundle_hash": channelAnchorBundleHash(t, h.rpcEndpoint()),
		"run_id": seedServedPublicationRun(t, h.process.runtime), "payload": map[string]any{"seed": true},
		"idempotency_key": "draft-stop-seed",
	})
	cardID := waitChannelAnchorCard(t, db, seed.RunID, decisioncard.AnchorKindStageGate, "chat/reviews")
	message := waitChannelAnchorReceipt(t, db, cardID)
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
	requireHeldInputPrompt(t, ctx, db, d, cardID)
	if current, err := native.IsExternalEffectAuthorityCurrent(ctx, authority); err != nil || !current {
		t.Fatalf("held prompt was not current before decision: %t %v", current, err)
	}
	_, providerSends := h.provider.Counts()
	params := lifecycleDecisionParamsForCard(t, servedControlProofRuntime{Endpoint: h.rpcEndpoint()}, cardID, "approve")
	var response map[string]any
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "mailbox.decide", params, &response)
	waitChannelDraftStatus(t, db, cardID, "cancelled")
	if current, err := native.IsExternalEffectAuthorityCurrent(ctx, authority); err != nil || current {
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
	var receipts, attempts int
	err = db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM channel_delivery_receipts WHERE delivery_id=$1),
		(SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE operation_id=$2)`, d.DeliveryID, authority.ID).Scan(&receipts, &attempts)
	if _, sends := h.provider.Counts(); err != nil || receipts != 0 || attempts != 0 || sends != providerSends {
		t.Fatalf("refused prompt reached provider or journal: sends=%d/%d receipts=%d attempts=%d err=%v", sends, providerSends, receipts, attempts, err)
	}
	const refusal = "platform.superseded_generation external_effect_authority_stale component=external-effects operation=check_authority"
	if !strings.Contains(diagnostics.String()+h.process.outputString(), refusal) {
		t.Fatalf("exact pre-authorization refusal not observed: %s", h.process.outputString())
	}
	if code != 0 {
		t.Fatalf("obsoleted ordinary prompt failed graceful stop: exit=%d\n%s", code, h.process.outputString())
	}
}

func requireHeldInputPrompt(t *testing.T, ctx context.Context, db *sql.DB, d effects.ChannelDeliveryAuthority, cardID string) {
	t.Helper()
	var raw []byte
	err := db.QueryRowContext(ctx, `SELECT CAST(r.render_input AS TEXT)
		FROM channel_delivery_plans p JOIN channel_delivery_renders r ON r.render_id=p.current_render_id
		WHERE p.delivery_id=$1 AND r.render_id=$2 AND r.render_hash=$3
		AND p.source_kind='response' AND p.state='rendered' AND p.current_receipt_operation_id IS NULL`,
		d.DeliveryID, d.RenderID, d.RenderHash).Scan(&raw)
	if err != nil {
		t.Fatalf("held send is not the exact queued response: %v", err)
	}
	raw, err = canonicaljson.Canonicalize(raw)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := render.Decode(raw, d.RenderHash)
	if err != nil || frozen.InputCard == nil || frozen.InputCard.CardID != cardID || frozen.Prompt == nil {
		t.Fatalf("held response is not the exact card input prompt: %v", err)
	}
}
