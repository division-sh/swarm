package channeldelivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/google/uuid"
)

var errControlCopySuperseded = errors.New("requested control page already has a successor")

// A verified More choices request may create one linked physical copy without
// edit. It preserves the semantic source and does not retry the original send.
func planRequestedControlCopyTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction,
	resolved render.ResolvedAction, parent Plan, postgres bool) error {
	frozen, err := FreezeCurrentSourceTx(ctx, tx, parent, postgres)
	if err != nil {
		return err
	}
	if frozen.Hash != resolved.RenderHash {
		return fmt.Errorf("requested control copy source changed")
	}
	frozen, err = render.WithPresentation(frozen, parent.Bounds, parent.ActionPageIndex+1)
	if err != nil {
		return err
	}
	deliveryID, renderID := uuid.NewString(), uuid.NewString()
	if err := insertControlCopyPlanTx(ctx, tx, action, parent, deliveryID, renderID, postgres); err != nil {
		return err
	}
	query := `INSERT INTO channel_delivery_renders (render_id, delivery_id, source_revision,
		projection_version, render_input, render_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`
	if postgres {
		query = `INSERT INTO channel_delivery_renders (render_id, delivery_id, source_revision,
			projection_version, render_input, render_hash, created_at) VALUES ($1::uuid, $2::uuid, $3, $4, $5::jsonb, $6, $7)`
	}
	if _, err := tx.ExecContext(ctx, query, renderID, deliveryID, frozen.Revision,
		render.ProjectionVersion, string(frozen.Input), frozen.Hash, time.Now().UTC()); err != nil {
		return fmt.Errorf("freeze requested control copy: %w", err)
	}
	_, err = EnsureRenderActionsTx(ctx, tx, renderID, frozen, postgres)
	return err
}

func insertControlCopyPlanTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction,
	parent Plan, deliveryID, renderID string, postgres bool) error {
	query := `SELECT resend_generation FROM channel_delivery_plans WHERE delivery_id=?
		AND NOT EXISTS (SELECT 1 FROM channel_delivery_plans child WHERE child.resend_of_delivery_id=?)`
	if postgres {
		query = `SELECT resend_generation FROM channel_delivery_plans WHERE delivery_id=$1::uuid
			AND NOT EXISTS (SELECT 1 FROM channel_delivery_plans child WHERE child.resend_of_delivery_id=$2::uuid)`
	}
	var generation int64
	if err := tx.QueryRowContext(ctx, query, parent.DeliveryID, parent.DeliveryID).Scan(&generation); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errControlCopySuperseded
		}
		return err
	}
	if generation == math.MaxInt64 {
		return fmt.Errorf("requested control copy generation exhausted")
	}
	query = `INSERT INTO channel_delivery_plans (delivery_id, source_kind, source_id, request_activation_id,
		summary_count, principal_id, interface_key, binding_revision, delivery_epoch, external_account_reference,
		conversation_reference, conversation_scope, state, current_render_id, action_capacity, text_capacity,
		label_capacity, action_page_index, resend_generation, resend_of_delivery_id, resend_action_publication_id, created_at)
		SELECT ?, source_kind, source_id, request_activation_id, summary_count, principal_id, interface_key,
		binding_revision, delivery_epoch, external_account_reference, conversation_reference, conversation_scope,
		'rendered', ?, action_capacity, text_capacity, label_capacity, action_page_index+1, ?, delivery_id, ?, ?
		FROM channel_delivery_plans WHERE delivery_id=? AND state='sent' AND current_render_id=? AND current_receipt_operation_id=?`
	if postgres {
		query = `INSERT INTO channel_delivery_plans (delivery_id, source_kind, source_id, request_activation_id,
			summary_count, principal_id, interface_key, binding_revision, delivery_epoch, external_account_reference,
			conversation_reference, conversation_scope, state, current_render_id, action_capacity, text_capacity,
			label_capacity, action_page_index, resend_generation, resend_of_delivery_id, resend_action_publication_id, created_at)
			SELECT $1::uuid, source_kind, source_id, request_activation_id, summary_count, principal_id, interface_key,
			binding_revision, delivery_epoch, external_account_reference, conversation_reference, conversation_scope,
			'rendered', $2::uuid, action_capacity, text_capacity, label_capacity, action_page_index+1, $3, delivery_id, $4::uuid, $5
			FROM channel_delivery_plans WHERE delivery_id=$6::uuid AND state='sent' AND current_render_id=$7::uuid AND current_receipt_operation_id=$8::uuid`
	}
	result, err := tx.ExecContext(ctx, query, deliveryID, renderID, generation+1, action.PublicationID,
		time.Now().UTC(), parent.DeliveryID, parent.CurrentRenderID, parent.CurrentReceiptID)
	if err != nil {
		return fmt.Errorf("plan requested control copy: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return fmt.Errorf("requested control copy lost its exact parent: rows=%d: %w", rows, err)
	}
	return nil
}
