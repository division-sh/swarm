package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/registration"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	"github.com/division-sh/swarm/tests/support/testutil/packfixture"
)

func proveChannelFirstSendEligibility(t *testing.T, ctx context.Context, mode string, selected selectedChannelDeliveryTestStore,
	runTx func(func(context.Context, *sql.Tx) error) error, current func(runtimeeffects.Authority) bool,
	authority runtimeeffects.Authority, postgres bool) {
	t.Helper()
	id := authority.ChannelDelivery.DeliveryID
	var original channeldelivery.StoredRender
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
		var err error
		original, _, err = channeldelivery.LoadRender(ctx, tx, authority.ChannelDelivery.RenderID, postgres)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	complete := func() {
		t.Helper()
		if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `UPDATE mailbox SET status='decided',notified=true WHERE item_id=$1`, original.Frozen.SourceID)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	responsibility := func(want, pending bool) {
		t.Helper()
		var plan channeldelivery.Plan
		var found bool
		if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
			var err error
			plan, found, err = channeldelivery.LoadCurrentPlan(ctx, tx, id, postgres)
			return err
		}); err != nil || found != want || found && plan.RecoveryPending != pending {
			t.Fatalf("responsibility found=%t pending=%t want=%t/%t err=%v", found, plan.RecoveryPending, want, pending, err)
		}
		page, err := selected.ListCurrentChannelDeliveryPlans(ctx, "", 500)
		if err != nil {
			t.Fatal(err)
		}
		listed := false
		for _, item := range page {
			if item.DeliveryID == id {
				listed = true
				if item.RecoveryPending != pending {
					t.Fatal("list/get accepted responsibility differs")
				}
			}
		}
		if listed != want {
			t.Fatalf("listed=%t want=%t", listed, want)
		}
	}
	responsibility(true, false)
	recover := func(want string) {
		t.Helper()
		if _, err := selected.(runtimeeffects.RecoveryStore).ReconcileExternalEffectAttempts(ctx,
			liveExternalEffectRecoveryRequest(time.Now().Add(time.Hour))); err != nil {
			t.Fatal(err)
		}
		if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
			var state string
			if err := tx.QueryRowContext(ctx, `SELECT state FROM runtime_external_effect_operations WHERE operation_id=$1`, authority.ID).Scan(&state); err != nil {
				return err
			}
			if state != want {
				t.Fatalf("source-loss recovery state=%s want=%s", state, want)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		responsibility(false, false)
	}
	if mode == "eligibility_before_authorize" {
		complete()
		responsibility(false, false)
		if current(authority) {
			t.Fatal("completed notice retained first-send authority")
		}
		effectCtx := runtimeeffects.WithController(runtimeeffects.WithAuthority(testAuthorActivityContextForBundle(authority.ChannelDelivery.BundleHash), authority),
			runtimeeffects.NewController(selected).WithExecutionPosture(executionposture.Live))
		effectCtx = startupownership.CarryMutationAuthority(effectCtx, ctx)
		if _, err := runtimeeffects.BeginChannelDelivery(effectCtx, []byte(authority.ID), nil); err == nil {
			t.Fatal("completed notice authorized")
		} else if _, ok := err.(*runtimeeffects.ChannelDeliveryRenderSuperseded); !ok {
			t.Fatalf("completed notice did not report a confirmed pre-authorization render change: %v", err)
		}
		return
	}
	if mode == "eligibility_http_launch_refusal" {
		err := proveSettledHTTPLaunchRefusal(t, ctx, selected, runTx, authority, complete)
		responsibility(false, false)
		failure, ok := errors.Unwrap(err).(*failures.Error)
		if !ok || failure.Unwrap() == nil {
			t.Fatalf("settled launch refusal lost its direct failure and cause: %T: %v", err, err)
		}
		return
	}
	effectCtx, handle := authorizeResponseTestEffect(t, ctx, selected, authority)
	responsibility(true, true)
	if mode == "eligibility_recover_authorized" {
		complete()
		retireFirstSendDestination(t, runTx)
		responsibility(true, true)
		recover("terminal_failure")
		return
	}
	if mode == "eligibility_before_launch" {
		complete()
		responsibility(true, true)
		if err := handle.MarkLaunched(effectCtx); err == nil {
			t.Fatal("source loss before launch was admitted")
		} else if _, ok := err.(*runtimeeffects.ChannelDeliveryRenderSuperseded); ok {
			t.Fatalf("already-authorized attempt was reported as unselected: %v", err)
		}
		failure, _ := failures.EnvelopeFromError(failures.New(failures.ClassLifecycleConflict, "test_source_closed_prelaunch", "test", "launch", nil))
		if err := handle.Settle(effectCtx, runtimeeffects.StateTerminalFailure, &failure, nil); err != nil {
			t.Fatal(err)
		}
		responsibility(false, false)
		return
	}
	if err := handle.MarkLaunched(effectCtx); err != nil {
		t.Fatal(err)
	}
	if mode == "eligibility_after_observation" || mode == "eligibility_recover_observed" {
		if err := handle.MarkResponseObserved(effectCtx, map[string]any{"provider": "accepted"}); err != nil {
			t.Fatal(err)
		}
	}
	complete()
	if strings.HasPrefix(mode, "eligibility_recover_") {
		retireFirstSendDestination(t, runTx)
	}
	responsibility(true, true)
	if current(authority) {
		t.Fatal("terminal first-send source remained current after launch")
	}
	if strings.HasPrefix(mode, "eligibility_recover_") {
		recover("outcome_uncertain")
		if _, err := runtimeeffects.BeginChannelDelivery(effectCtx, []byte(authority.ID), nil); err == nil {
			t.Fatal("recovered source-loss operation redispatched")
		}
		return
	}
	if mode == "eligibility_uncertain" {
		failure, _ := failures.EnvelopeFromError(failures.New(failures.ClassOutcomeUncertain, "test_first_send_uncertain", "test", "settle", nil))
		if err := handle.Settle(effectCtx, runtimeeffects.StateOutcomeUncertain, &failure, nil); err != nil {
			t.Fatal(err)
		}
		responsibility(false, false)
		if _, err := runtimeeffects.BeginChannelDelivery(effectCtx, []byte(authority.ID), nil); err == nil {
			t.Fatal("uncertain first send redispatched")
		}
		return
	}
	if err := handle.Succeed(effectCtx, map[string]any{"projected_output": map[string]any{"delivery_reference": map[string]any{"id": 73}}}); err != nil {
		t.Fatal(err)
	}
	responsibility(true, false)
	if mode == "eligibility_edit" {
		prepared, err := selected.FreezeAndPersistChannelRender(ctx, id, packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64})
		if err != nil || prepared.RenderID == original.RenderID {
			t.Fatalf("terminal source edit render=%s original=%s err=%v", prepared.RenderID, original.RenderID, err)
		}
		previous := authority.ID
		authority = responseTestAuthority(t, authority, id, prepared)
		authority.ChannelDelivery.PreviousReceiptOperationID = previous
		if !current(authority) {
			t.Fatal("exact SENT predecessor lost terminal edit authority")
		}
		editCtx, edit := authorizeResponseTestEffect(t, ctx, selected, authority)
		if err := edit.MarkLaunched(editCtx); err != nil {
			t.Fatal(err)
		}
		if err := edit.Succeed(editCtx, map[string]any{"projected_output": map[string]any{"delivery_receipt": map[string]any{"edited": true}}}); err != nil {
			t.Fatal(err)
		}
		if _, found, err := selected.GetCurrentChannelSentReceipt(ctx, id, previous); err != nil || found {
			t.Fatalf("old SENT predecessor remained current: %t %v", found, err)
		}
	}
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
		stored, found, err := channeldelivery.LoadRender(ctx, tx, original.RenderID, postgres)
		if err != nil || !found || string(stored.Frozen.Input) != string(original.Frozen.Input) {
			t.Fatalf("source loss rewrote immutable render: found=%t err=%v", found, err)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

type channelLaunchRefusalTransport struct{ calls int }

func (transport *channelLaunchRefusalTransport) RoundTrip(*http.Request) (*http.Response, error) {
	transport.calls++
	return nil, errors.New("unexpected provider dispatch")
}

func proveSettledHTTPLaunchRefusal(t *testing.T, original context.Context, selected selectedChannelDeliveryTestStore,
	runTx func(func(context.Context, *sql.Tx) error) error, authority runtimeeffects.Authority, complete func()) error {
	t.Helper()
	const toolID = "telegram.send_interactive"
	tool := packfixture.ConnectorTool(t, "telegram", toolID).Tool
	tool, err := tool.WithCompiledResult(contracts.CompiledResultProjection{
		Fields: map[string]contracts.CompiledResultField{"id": {From: "result.message_id"}},
		OutputSchema: contracts.MustToolInputSchema(contracts.ToolSchemaObject,
			contracts.ToolSchemaProperties(map[string]contracts.ToolInputSchema{"id": contracts.MustToolInputSchema(contracts.ToolSchemaInteger)}),
			contracts.ToolSchemaRequired("id")),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtimeeffects.WithController(runtimeeffects.WithAuthority(testAuthorActivityContextForBundle(authority.ChannelDelivery.BundleHash), authority),
		runtimeeffects.NewController(selected).WithExecutionPosture(executionposture.Live))
	ctx = startupownership.CarryMutationAuthority(ctx, original)
	transport := &channelLaunchRefusalTransport{}
	preflights := 0
	executor := registration.HTTPExecutor{Client: &http.Client{Transport: transport}, Preflight: func(context.Context) error {
		preflights++
		if preflights == 2 {
			complete()
		}
		return nil
	}}
	result, refusal := executor.DeliverChannelMessage(ctx, toolID, tool,
		map[string]any{"chat_id": "42", "text": "Notice", "reply_markup": map[string]any{"inline_keyboard": []any{}}},
		map[string]any{"telegram.telegram_bot_token": "test-token"}, nil)
	if refusal == nil || result.OperationID != authority.ID || result.Output != nil || preflights != 2 || transport.calls != 0 {
		t.Fatalf("result=%+v refusal=%v preflights=%d provider calls=%d", result, refusal, preflights, transport.calls)
	}
	var operation, attempt string
	var launched bool
	var receipts int
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT o.state, a.state, a.launched_at IS NOT NULL,
			(SELECT count(*) FROM channel_delivery_receipts r WHERE r.effect_operation_id=o.operation_id)
			FROM runtime_external_effect_operations o JOIN runtime_external_effect_attempts a USING (operation_id)
			WHERE o.operation_id=$1`, authority.ID).Scan(&operation, &attempt, &launched, &receipts)
	}); err != nil || operation != "terminal_failure" || attempt != operation || launched || receipts != 0 {
		t.Fatalf("operation=%s attempt=%s launched=%t receipts=%d err=%v", operation, attempt, launched, receipts, err)
	}
	return refusal
}

func retireFirstSendDestination(t *testing.T, runTx func(func(context.Context, *sql.Tx) error) error) {
	t.Helper()
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE channel_delivery_defaults SET state='retired' WHERE singleton_id=1`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
