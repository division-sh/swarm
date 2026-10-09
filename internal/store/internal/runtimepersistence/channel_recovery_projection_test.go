package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	"github.com/google/uuid"
)

func channelRecoveryProofModes() []string {
	var modes []string
	for _, source := range []string{"notice", "response"} {
		for _, operation := range []string{"send", "edit"} {
			for _, cut := range []string{"authorized", "launched", "observed", "settled", "retired", "renderadvanced", "predecessorconflict", "authorityconflict", "evidenceconflict", "renderconflict", "retryauthorized", "retrylaunched", "retryobserved", "retrysettled", "retryownerconflict", "retryattemptevidenceconflict", "retrymissingconflict", "retryevidenceconflict"} {
				modes = append(modes, "recovery_"+source+"_"+operation+"_"+cut)
			}
		}
	}
	for _, cut := range []string{"authorized", "launched", "observed", "settled", "retired", "generationconflict", "installconflict", "authorityconflict", "evidenceconflict", "retryauthorized", "retrylaunched", "retryobserved", "retrysettled", "retryownerconflict", "retryattemptevidenceconflict", "retrymissingconflict", "retryevidenceconflict"} {
		modes = append(modes, "recovery_native_install_"+cut)
	}
	return modes
}

// This is a selected-store state matrix, not public producer or OS-death proof.
// It reuses the admitted binding/render fixture and actual managed effect owner.
func proveChannelRecoveryProjection(t *testing.T, mode string, selected selectedChannelDeliveryTestStore,
	db interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	},
	runTx func(func(context.Context, *sql.Tx) error) error,
	delivery, native runtimeeffects.Authority, text operatorchannel.InboundText, postgres bool) {
	t.Helper()
	parts := strings.Split(mode, "_")
	source, operation, cut := parts[1], parts[2], parts[3]
	ctx := context.Background()
	bounds := packs.PresentationBounds{Actions: 8, TextRunes: 4096, LabelRunes: 64}
	exec := func(query string, args ...any) {
		t.Helper()
		if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, query, args...)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	if source == "response" {
		bounds = packs.PresentationBounds{Actions: 2, TextRunes: 512, LabelRunes: 24}
		text.PublicationID, text.ProviderEventID = uuid.NewString(), uuid.NewString()
		var id string
		if err := runTx(func(ctx context.Context, tx *sql.Tx) error {
			if err := channeldelivery.InsertTextIntentTx(ctx, tx, text, time.Now(), postgres); err != nil {
				return err
			}
			var err error
			id, _, err = channeldelivery.PlanTextResponseTx(ctx, tx, text, "Exact recovery response", "teaching", postgres)
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `UPDATE operator_channel_text_intents SET state='settled',disposition='teaching',settled_at=$1 WHERE publication_id=$2`, time.Now(), text.PublicationID)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		prepared, err := selected.FreezeAndPersistChannelRender(ctx, id, bounds)
		if err != nil {
			t.Fatal(err)
		}
		delivery = responseTestAuthority(t, delivery, id, prepared)
	}
	advance := func() {
		t.Helper()
		exec(`UPDATE channel_delivery_plans SET action_page_index=action_page_index+1 WHERE delivery_id=$1`, delivery.ChannelDelivery.DeliveryID)
		prepared, err := selected.FreezeAndPersistChannelRender(ctx, delivery.ChannelDelivery.DeliveryID, bounds)
		if err != nil {
			t.Fatal(err)
		}
		previous := delivery.ID
		delivery = responseTestAuthority(t, delivery, delivery.ChannelDelivery.DeliveryID, prepared)
		delivery.ChannelDelivery.PreviousReceiptOperationID = previous
	}
	if operation == "edit" {
		effectCtx, first := authorizeResponseTestEffect(t, selected, delivery)
		if err := first.MarkLaunched(effectCtx); err != nil {
			t.Fatal(err)
		}
		if err := first.Succeed(effectCtx, map[string]any{"projected_output": map[string]any{"delivery_reference": map[string]any{"id": 73}}}); err != nil {
			t.Fatal(err)
		}
		advance()
	}
	authority := delivery
	if source == "native" {
		authority = native
	}
	effectCtx := runtimeeffects.WithController(runtimeeffects.WithAuthority(testAuthorActivityContextForBundle(delivery.ChannelDelivery.BundleHash), authority),
		runtimeeffects.NewController(selected).WithExecutionPosture(executionposture.Live))
	var handle *runtimeeffects.Handle
	var err error
	if source == "native" {
		handle, err = runtimeeffects.BeginChannelNativeSetting(effectCtx, []byte("install recovery"), nil)
	} else {
		handle, err = runtimeeffects.BeginChannelDelivery(effectCtx, []byte(authority.ID), nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	var originalEvidence []byte
	var originalAttemptID string
	recovery := selected.(interface {
		ReconcileExternalEffectAttempts(context.Context, runtimeeffects.RecoveryRequest) (runtimeeffects.RecoverySummary, error)
	})
	if strings.HasPrefix(cut, "retry") {
		cut = strings.TrimPrefix(cut, "retry")
		originalAttemptID = handle.Attempt().AttemptID
		if err := db.QueryRowContext(ctx, `SELECT authority_evidence FROM runtime_external_effect_operations WHERE operation_id=$1`, authority.ID).Scan(&originalEvidence); err != nil {
			t.Fatal(err)
		}
		if _, err := recovery.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(time.Now().Add(time.Hour))); err != nil {
			t.Fatal(err)
		}
		authority.ExecutionOwner = "admitted-retry-owner"
		effectCtx = runtimeeffects.WithAuthority(effectCtx, authority)
		if source == "native" {
			handle, err = runtimeeffects.BeginChannelNativeSetting(effectCtx, []byte("install recovery"), nil)
		} else {
			handle, err = runtimeeffects.BeginChannelDelivery(effectCtx, []byte(authority.ID), nil)
		}
		if err != nil || handle.Attempt().Ordinal != 2 {
			t.Fatalf("exact prelaunch retry was not admitted: %v", err)
		}
	}
	initial, want := "authorized", "terminal_failure"
	if cut != "authorized" {
		if err := handle.MarkLaunched(effectCtx); err != nil {
			t.Fatal(err)
		}
		initial, want = "launched", "outcome_uncertain"
	}
	if cut == "observed" || cut == "settled" {
		if err := handle.MarkResponseObserved(effectCtx, map[string]any{"provider_applied": true}); err != nil {
			t.Fatal(err)
		}
		initial = "response_observed"
	}
	if cut == "settled" {
		evidence := map[string]any{"projected_output": map[string]any{"delivery_reference": map[string]any{"id": 73}}}
		if operation == "edit" {
			evidence = map[string]any{"projected_output": map[string]any{"delivery_receipt": map[string]any{"id": 91}}}
		}
		if source == "native" {
			desired, err := channelnative.DesiredCommands(native.ChannelNativeSetting.SettingID, native.ChannelNativeSetting.SettingGeneration)
			if err != nil {
				t.Fatal(err)
			}
			evidence = map[string]any{"readback_hash": runtimeeffects.Fingerprint(desired)}
		}
		if err := handle.Succeed(effectCtx, evidence); err != nil {
			t.Fatal(err)
		}
		initial, want = "settled", "settled"
	}
	if cut == "retired" {
		_, _, err := selected.UnbindOperatorChannel(ctx, operatorchannel.UnbindRequest{
			OperationID: uuid.NewString(), PrincipalID: delivery.ChannelDelivery.PrincipalID, Interface: text.Interface,
			ExpectedRevision: delivery.ChannelDelivery.BindingRevision, RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(), RequestedAt: time.Now(),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if cut == "renderadvanced" {
		advance()
	}
	if cut == "predecessorconflict" {
		// Deliberately corrupted owner pointer must roll back the journal change.
		exec(`UPDATE channel_delivery_plans SET current_receipt_operation_id=NULL,state='planned' WHERE delivery_id=$1`, delivery.ChannelDelivery.DeliveryID)
	}
	if cut == "generationconflict" {
		exec(`UPDATE channel_native_settings SET generation=generation+1 WHERE setting_id=$1`, native.ChannelNativeSetting.SettingID)
	}
	if cut == "installconflict" {
		exec(`UPDATE channel_native_settings SET install_operation_id=$1 WHERE setting_id=$2`, uuid.NewString(), native.ChannelNativeSetting.SettingID)
	}
	if cut == "authorityconflict" {
		exec(`UPDATE runtime_external_effect_attempts SET fence_generation=fence_generation+1 WHERE attempt_id=$1`, handle.Attempt().AttemptID)
	}
	if cut == "ownerconflict" {
		exec(`UPDATE runtime_external_effect_attempts SET execution_owner='unadmitted-owner' WHERE attempt_id=$1`, handle.Attempt().AttemptID)
	}
	if cut == "missingconflict" {
		exec(`UPDATE runtime_external_effect_attempts SET authority_evidence='{}' WHERE attempt_id=$1`, handle.Attempt().AttemptID)
	}
	if cut == "attemptevidenceconflict" {
		evidence := authority.Evidence()
		evidence["unadmitted_attempt_authority"] = "successor"
		raw, err := canonicaljson.Bytes(evidence)
		if err != nil {
			t.Fatal(err)
		}
		exec(`UPDATE runtime_external_effect_attempts SET authority_evidence=$1 WHERE attempt_id=$2`, string(raw), handle.Attempt().AttemptID)
	}
	if cut == "evidenceconflict" {
		evidence := authority.Evidence()
		// Unknown evidence cannot be dropped by decoding into a typed struct.
		evidence["unadmitted_current_authority"] = "successor"
		raw, err := canonicaljson.Bytes(evidence)
		if err != nil {
			t.Fatal(err)
		}
		exec(`UPDATE runtime_external_effect_operations SET authority_evidence=$1 WHERE operation_id=$2`, string(raw), authority.ID)
	}
	if cut == "renderconflict" {
		exec(`UPDATE channel_delivery_renders SET render_hash=$1 WHERE render_id=$2`, strings.Repeat("0", 64), authority.ChannelDelivery.RenderID)
	}
	var frozenBefore []byte
	if source != "native" {
		if err := db.QueryRowContext(ctx, `SELECT render_input FROM channel_delivery_renders WHERE render_id=$1`, authority.ChannelDelivery.RenderID).Scan(&frozenBefore); err != nil {
			t.Fatal(err)
		}
	}
	_, err = recovery.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(time.Now().Add(time.Hour)))
	conflict := strings.HasSuffix(cut, "conflict")
	if conflict != (err != nil) {
		t.Fatalf("recovery cut=%s error=%v, want conflict=%t", cut, err, conflict)
	}
	if conflict {
		envelope, ok := runtimefailures.EnvelopeFromError(err)
		if !ok || envelope.Detail.Code != "channel_source_recovery_settlement_conflict" {
			t.Fatalf("recovery conflict lost typed failure: %v", err)
		}
	}
	var attemptState, operationState string
	if err := db.QueryRowContext(ctx, `SELECT a.state,o.state FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE a.attempt_id=$1`, handle.Attempt().AttemptID).Scan(&attemptState, &operationState); err != nil {
		t.Fatal(err)
	}
	if conflict {
		want = initial
	}
	if attemptState != want || operationState != want {
		t.Fatalf("journal mismatch attempt=%s operation=%s want=%s", attemptState, operationState, want)
	}
	if originalAttemptID != "" {
		var after []byte
		var originalState string
		if err := db.QueryRowContext(ctx, `SELECT o.authority_evidence,a.state FROM runtime_external_effect_operations o JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id WHERE a.attempt_id=$1`, originalAttemptID).Scan(&after, &originalState); err != nil {
			t.Fatal(err)
		}
		if cut != "evidenceconflict" && !bytes.Equal(originalEvidence, after) || originalState != "terminal_failure" {
			t.Fatal("retry rewrote original operation or predecessor attempt evidence")
		}
	}
	if cut == "observed" {
		var raw []byte
		if err := db.QueryRowContext(ctx, `SELECT evidence FROM runtime_external_effect_attempts WHERE attempt_id=$1`, handle.Attempt().AttemptID).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var evidence map[string]any
		if err := json.Unmarshal(raw, &evidence); err != nil || evidence["provider_applied"] != true {
			t.Fatalf("recovery lost observed provider facts: %s %v", raw, err)
		}
	}
	if source == "native" {
		var state string
		var readback sql.NullString
		if err := db.QueryRowContext(ctx, `SELECT state,readback_hash FROM channel_native_settings WHERE setting_id=$1`, native.ChannelNativeSetting.SettingID).Scan(&state, &readback); err != nil {
			t.Fatal(err)
		}
		wantState := "uncertain"
		if cut == "authorized" || conflict {
			wantState = "planned"
		} else if cut == "settled" {
			wantState = "installed"
		}
		if state != wantState || readback.Valid != (cut == "settled") {
			t.Fatalf("native recovered state=%s readback=%v want=%s", state, readback, wantState)
		}
	} else {
		var frozenAfter []byte
		if err := db.QueryRowContext(ctx, `SELECT render_input FROM channel_delivery_renders WHERE render_id=$1`, authority.ChannelDelivery.RenderID).Scan(&frozenAfter); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(frozenBefore, frozenAfter) {
			t.Fatal("recovery changed the original frozen render")
		}
		var count int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_delivery_receipts WHERE effect_operation_id=$1`, authority.ID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		wantCount := 1
		if cut == "authorized" || conflict {
			wantCount = 0
		}
		if count != wantCount {
			t.Fatalf("recovery receipts=%d want=%d", count, wantCount)
		}
		if count == 1 {
			var state string
			var reference sql.NullString
			if err := db.QueryRowContext(ctx, `SELECT state,provider_reference FROM channel_delivery_receipts WHERE effect_operation_id=$1`, authority.ID).Scan(&state, &reference); err != nil {
				t.Fatal(err)
			}
			if state != "uncertain" && state != "sent" || reference.Valid != (cut == "settled") {
				t.Fatalf("fabricated recovery acknowledgment state=%s ref=%v", state, reference)
			}
			if operation == "edit" {
				var original []byte
				if err := db.QueryRowContext(ctx, `SELECT provider_reference FROM channel_delivery_receipts WHERE effect_operation_id=$1`, authority.ChannelDelivery.PreviousReceiptOperationID).Scan(&original); err != nil {
					t.Fatal(err)
				}
				var value map[string]any
				if err := json.Unmarshal(original, &value); err != nil || fmt.Sprint(value["delivery_reference"]) != "map[id:73]" {
					t.Fatalf("recovery erased original sent reference: %s %v", original, err)
				}
			}
		}
	}
	if !conflict {
		summary, err := recovery.ReconcileExternalEffectAttempts(testAuthorActivityContext(), liveExternalEffectRecoveryRequest(time.Now().Add(2*time.Hour)))
		if err != nil || summary != (runtimeeffects.RecoverySummary{}) {
			t.Fatalf("repeat recovery changed terminal evidence: %#v %v", summary, err)
		}
	}
}
