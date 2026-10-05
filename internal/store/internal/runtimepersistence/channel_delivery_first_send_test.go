package runtimepersistence

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packs"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
)

func proveChannelFirstSendEligibility(t *testing.T, mode string, selected selectedChannelDeliveryTestStore,
	runTx func(func(context.Context, *sql.Tx) error) error, current func(runtimeeffects.Authority) bool,
	authority runtimeeffects.Authority, postgres bool) {
	t.Helper()
	ctx := context.Background()
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
		if _, err := selected.(runtimeeffects.RecoveryStore).ReconcileExternalEffectAttempts(testAuthorActivityContext(),
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
		if _, err := runtimeeffects.BeginChannelDelivery(effectCtx, []byte(authority.ID), nil); err == nil {
			t.Fatal("completed notice authorized")
		}
		return
	}
	effectCtx, handle := authorizeResponseTestEffect(t, selected, authority)
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
		editCtx, edit := authorizeResponseTestEffect(t, selected, authority)
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

func retireFirstSendDestination(t *testing.T, runTx func(func(context.Context, *sql.Tx) error) error) {
	t.Helper()
	if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE channel_delivery_defaults SET state='retired' WHERE singleton_id=1`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
