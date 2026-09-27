package effectpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func requireChannelDeliverySettlementAuthorityTx(ctx context.Context, tx *sql.Tx, settlement runtimeeffects.Settlement, postgres bool) error {
	authority := settlement.Authority
	if !authority.Valid() || authority.Kind != runtimeeffects.AuthorityChannelDelivery ||
		settlement.OperationID != authority.ChannelDelivery.EffectOperationID {
		return fmt.Errorf("channel delivery settlement authority is invalid")
	}
	query := `SELECT operation.authority_evidence, operation.authority_kind, operation.effect_kind,
		operation.bundle_hash, attempt.state, attempt.execution_owner, attempt.fence_generation
		FROM runtime_external_effect_operations operation
		JOIN runtime_external_effect_attempts attempt ON attempt.operation_id=operation.operation_id
		WHERE operation.operation_id=? AND attempt.attempt_id=?`
	if postgres {
		query = `SELECT operation.authority_evidence, operation.authority_kind, operation.effect_kind,
			operation.bundle_hash, attempt.state, attempt.execution_owner, attempt.fence_generation
			FROM runtime_external_effect_operations operation
			JOIN runtime_external_effect_attempts attempt ON attempt.operation_id=operation.operation_id
			WHERE operation.operation_id=$1::uuid AND attempt.attempt_id=$2::uuid
			FOR UPDATE OF operation, attempt`
	}
	var storedEvidence []byte
	var kind, effectKind, bundleHash, state, owner string
	var fence uint64
	if err := tx.QueryRowContext(ctx, query, settlement.OperationID, settlement.AttemptID).Scan(
		&storedEvidence, &kind, &effectKind, &bundleHash, &state, &owner, &fence,
	); err != nil {
		return fmt.Errorf("load exact channel delivery settlement attempt: %w", err)
	}
	if kind != string(runtimeeffects.AuthorityChannelDelivery) || effectKind != string(runtimeeffects.KindChannelDelivery) ||
		bundleHash != authority.ChannelDelivery.BundleHash || owner != authority.ExecutionOwner || fence != authority.FenceGeneration {
		return fmt.Errorf("channel delivery settlement attempt contradicts authority")
	}
	wantRaw, err := json.Marshal(authority.Evidence())
	if err != nil {
		return err
	}
	want, err := canonicaljson.Canonicalize(wantRaw)
	if err != nil {
		return err
	}
	got, err := canonicaljson.Canonicalize(storedEvidence)
	if err != nil || !bytes.Equal(want, got) {
		return fmt.Errorf("channel delivery settlement evidence contradicts original authority")
	}
	switch settlement.State {
	case runtimeeffects.StateSettled, runtimeeffects.StateOutcomeUncertain:
		if state != string(runtimeeffects.StateLaunched) && state != string(runtimeeffects.StateResponseObserved) && state != string(settlement.State) {
			return fmt.Errorf("channel delivery cannot settle %s from %s", settlement.State, state)
		}
	case runtimeeffects.StateTerminalFailure:
		if state != string(runtimeeffects.StateAuthorized) && state != string(runtimeeffects.StateTerminalFailure) {
			return fmt.Errorf("channel delivery prelaunch failure contradicts %s", state)
		}
	default:
		return fmt.Errorf("unsupported channel delivery settlement %s", settlement.State)
	}
	return nil
}

// Project the typed effect result before committing its journal settlement.
// The effect journal owns transport truth; this row owns delivery readback.
func projectChannelDeliverySettlementTx(ctx context.Context, tx *sql.Tx, settlement runtimeeffects.Settlement, postgres bool) error {
	if settlement.Authority.Kind != runtimeeffects.AuthorityChannelDelivery {
		return nil
	}
	authority := settlement.Authority.ChannelDelivery
	if !settlement.Authority.Valid() || settlement.OperationID != authority.EffectOperationID {
		return fmt.Errorf("channel delivery settlement has contradictory authority")
	}
	if settlement.State != runtimeeffects.StateSettled && settlement.State != runtimeeffects.StateOutcomeUncertain {
		return nil
	}
	state := "uncertain"
	var providerReference any
	if settlement.State == runtimeeffects.StateSettled {
		value, ok := settlement.Evidence["projected_output"]
		if !ok {
			return fmt.Errorf("settled channel delivery has no compiled receipt")
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("encode channel delivery receipt: %w", err)
		}
		canonical, err := canonicaljson.Canonicalize(raw)
		if err != nil {
			return fmt.Errorf("canonicalize channel delivery receipt: %w", err)
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(canonical, &object); err != nil || len(object) == 0 {
			return fmt.Errorf("channel delivery receipt is not a nonempty object")
		}
		providerReference = string(canonical)
		state = "sent"
	}
	query := `INSERT INTO channel_delivery_receipts
		(effect_operation_id, attempt_id, delivery_id, render_id, state, provider_reference, settled_at)
		VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (effect_operation_id) DO NOTHING`
	if postgres {
		query = `INSERT INTO channel_delivery_receipts
			(effect_operation_id, attempt_id, delivery_id, render_id, state, provider_reference, settled_at)
			VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5, $6::jsonb, $7)
			ON CONFLICT (effect_operation_id) DO NOTHING`
	}
	result, err := tx.ExecContext(ctx, query, settlement.OperationID, settlement.AttemptID,
		authority.DeliveryID, authority.RenderID, state, providerReference, settlement.Now.UTC())
	if err != nil {
		return fmt.Errorf("persist channel delivery receipt: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 0 {
		query = `SELECT attempt_id, delivery_id, render_id, state, provider_reference FROM channel_delivery_receipts WHERE effect_operation_id=?`
		if postgres {
			query = `SELECT attempt_id::text, delivery_id::text, render_id::text, state, provider_reference FROM channel_delivery_receipts WHERE effect_operation_id=$1::uuid`
		}
		var attemptID, deliveryID, renderID, storedState string
		var storedReference sql.NullString
		if err := tx.QueryRowContext(ctx, query, settlement.OperationID).Scan(&attemptID, &deliveryID, &renderID, &storedState, &storedReference); err != nil {
			return err
		}
		matchingReference := !storedReference.Valid && providerReference == nil
		if storedReference.Valid && providerReference != nil {
			canonical, canonicalErr := canonicaljson.Canonicalize([]byte(storedReference.String))
			matchingReference = canonicalErr == nil && string(canonical) == providerReference
		}
		if attemptID != settlement.AttemptID || deliveryID != authority.DeliveryID || renderID != authority.RenderID ||
			storedState != state || !matchingReference {
			return fmt.Errorf("existing channel delivery receipt contradicts settled effect")
		}
	} else if inserted != 1 {
		return fmt.Errorf("channel delivery receipt insert affected %d rows", inserted)
	}
	if state == "sent" {
		query = `UPDATE channel_delivery_plans SET current_receipt_operation_id=?,
			state=CASE WHEN current_render_id=? THEN 'sent' ELSE 'rendered' END
			WHERE delivery_id=? AND current_receipt_operation_id IS NULL AND state='rendered'`
		if postgres {
			query = `UPDATE channel_delivery_plans SET current_receipt_operation_id=$1::uuid,
				state=CASE WHEN current_render_id=$2::uuid THEN 'sent' ELSE 'rendered' END
				WHERE delivery_id=$3::uuid AND current_receipt_operation_id IS NULL AND state='rendered'`
		}
		result, err = tx.ExecContext(ctx, query, settlement.OperationID, authority.RenderID, authority.DeliveryID)
	} else {
		query = `UPDATE channel_delivery_plans SET state='uncertain'
			WHERE delivery_id=? AND current_receipt_operation_id IS NULL AND state='rendered'`
		if postgres {
			query = `UPDATE channel_delivery_plans SET state='uncertain'
				WHERE delivery_id=$1::uuid AND current_receipt_operation_id IS NULL AND state='rendered'`
		}
		result, err = tx.ExecContext(ctx, query, authority.DeliveryID)
	}
	if err != nil {
		return fmt.Errorf("settle channel delivery plan: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if updated == 1 {
		return nil
	}
	query = `SELECT state, COALESCE(current_receipt_operation_id, ''), COALESCE(current_render_id, '') FROM channel_delivery_plans WHERE delivery_id=?`
	if postgres {
		query = `SELECT state, COALESCE(current_receipt_operation_id::text, ''), COALESCE(current_render_id::text, '') FROM channel_delivery_plans WHERE delivery_id=$1::uuid`
	}
	var storedState, currentReceiptID, currentRenderID string
	if err := tx.QueryRowContext(ctx, query, authority.DeliveryID).Scan(&storedState, &currentReceiptID, &currentRenderID); err != nil {
		return err
	}
	wantState := state
	if state == "sent" && currentRenderID != authority.RenderID {
		wantState = "rendered"
	}
	if storedState != wantState || (state == "sent" && currentReceiptID != settlement.OperationID) {
		return fmt.Errorf("channel delivery plan state %q contradicts settled receipt %q", storedState, state)
	}
	return nil
}
