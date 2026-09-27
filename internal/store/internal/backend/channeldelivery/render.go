package channeldelivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/google/uuid"
)

type StoredRender struct {
	RenderID   string
	DeliveryID string
	Frozen     render.Frozen
}

func PersistRenderTx(ctx context.Context, tx *sql.Tx, deliveryID string, frozen render.Frozen, postgres bool) (string, bool, error) {
	if tx == nil || uuid.Validate(deliveryID) != nil {
		return "", false, fmt.Errorf("channel render requires a transaction and delivery id")
	}
	verified, err := render.Decode(frozen.Input, frozen.Hash)
	if err != nil {
		return "", false, err
	}
	if verified.SourceKind != frozen.SourceKind || verified.SourceID != frozen.SourceID ||
		verified.Revision != frozen.Revision || verified.Audience != frozen.Audience || verified.FullText != frozen.FullText {
		return "", false, fmt.Errorf("channel render metadata contradicts frozen input")
	}
	principalID, found, err := LockCurrentPrincipalTx(ctx, tx, postgres)
	if err != nil {
		return "", false, err
	}
	if !found {
		return "", false, fmt.Errorf("channel render principal is unavailable")
	}
	plan, found, err := LoadCurrentPlan(ctx, tx, deliveryID, postgres)
	if err != nil {
		return "", false, err
	}
	if !found || plan.PrincipalID != principalID || !renderMatchesPlan(verified, plan) {
		return "", false, fmt.Errorf("channel render plan is not exact-current")
	}
	if err := requireExactSourceRenderTx(ctx, tx, plan, verified, postgres); err != nil {
		return "", false, err
	}
	id := uuid.NewString()
	query := `INSERT INTO channel_delivery_renders (render_id, delivery_id, source_revision,
		projection_version, render_input, render_hash, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (delivery_id, render_hash) DO NOTHING`
	if postgres {
		query = `INSERT INTO channel_delivery_renders (render_id, delivery_id, source_revision,
			projection_version, render_input, render_hash, created_at)
			VALUES ($1::uuid, $2::uuid, $3, $4, $5::jsonb, $6, $7)
			ON CONFLICT (delivery_id, render_hash) DO NOTHING`
	}
	result, err := tx.ExecContext(ctx, query, id, deliveryID, verified.Revision, render.ProjectionVersion,
		string(verified.Input), verified.Hash, time.Now().UTC())
	if err != nil {
		return "", false, fmt.Errorf("persist channel render: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return "", false, err
	}
	if rows == 1 {
		query = `UPDATE channel_delivery_plans SET current_render_id = ?,
			state = CASE WHEN state = 'uncertain' THEN 'uncertain' ELSE 'rendered' END
			WHERE delivery_id = ? AND state IN ('planned','rendered','sent','uncertain')`
		if postgres {
			query = `UPDATE channel_delivery_plans SET current_render_id = $1::uuid,
				state = CASE WHEN state = 'uncertain' THEN 'uncertain' ELSE 'rendered' END
				WHERE delivery_id = $2::uuid AND state IN ('planned','rendered','sent','uncertain')`
		}
		updated, err := tx.ExecContext(ctx, query, id, deliveryID)
		if err != nil {
			return "", false, err
		}
		count, err := updated.RowsAffected()
		if err != nil {
			return "", false, err
		}
		if count != 1 {
			return "", false, fmt.Errorf("current channel render plan did not advance")
		}
		return id, true, nil
	}
	if rows != 0 {
		return "", false, fmt.Errorf("channel render insert affected %d rows", rows)
	}
	query = `SELECT render_id FROM channel_delivery_renders WHERE delivery_id = ? AND render_hash = ?`
	if postgres {
		query = `SELECT render_id::text FROM channel_delivery_renders WHERE delivery_id = $1::uuid AND render_hash = $2`
	}
	if err := tx.QueryRowContext(ctx, query, deliveryID, verified.Hash).Scan(&id); err != nil {
		return "", false, err
	}
	stored, found, err := LoadRender(ctx, tx, id, postgres)
	if err != nil {
		return "", false, fmt.Errorf("read existing channel render: %w", err)
	}
	if !found {
		return "", false, fmt.Errorf("existing channel render disappeared")
	}
	if stored.DeliveryID != deliveryID || stored.Frozen.Hash != verified.Hash ||
		stored.Frozen.Revision != verified.Revision || string(stored.Frozen.Input) != string(verified.Input) {
		return "", false, fmt.Errorf("existing channel render contradicts immutable input")
	}
	return id, false, nil
}

func LoadRender(ctx context.Context, db queryer, renderID string, postgres bool) (StoredRender, bool, error) {
	if db == nil || uuid.Validate(renderID) != nil {
		return StoredRender{}, false, fmt.Errorf("channel render read requires a store and render id")
	}
	query := `SELECT render_id, delivery_id, source_revision, projection_version, render_input, render_hash
		FROM channel_delivery_renders WHERE render_id = ?`
	if postgres {
		query = `SELECT render_id::text, delivery_id::text, source_revision, projection_version, render_input, render_hash
			FROM channel_delivery_renders WHERE render_id = $1::uuid`
	}
	var stored StoredRender
	var sourceRevision int64
	var version, hash string
	var raw []byte
	err := db.QueryRowContext(ctx, query, renderID).Scan(&stored.RenderID, &stored.DeliveryID,
		&sourceRevision, &version, &raw, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredRender{}, false, nil
	}
	if err != nil {
		return StoredRender{}, false, err
	}
	canonical, err := canonicaljson.Canonicalize(raw)
	if err != nil {
		return StoredRender{}, false, err
	}
	stored.Frozen, err = render.Decode(canonical, hash)
	if err != nil {
		return StoredRender{}, false, err
	}
	if version != render.ProjectionVersion || sourceRevision != stored.Frozen.Revision {
		return StoredRender{}, false, fmt.Errorf("stored channel render projection metadata contradicts input")
	}
	plan, found, err := LoadPlan(ctx, db, stored.DeliveryID, postgres)
	if err != nil {
		return StoredRender{}, false, err
	}
	if !found || !renderMatchesPlan(stored.Frozen, plan) {
		return StoredRender{}, false, fmt.Errorf("stored channel render contradicts delivery plan")
	}
	return stored, true, nil
}

func renderMatchesPlan(frozen render.Frozen, plan Plan) bool {
	audience := frozen.Audience
	return frozen.SourceKind == plan.SourceKind && frozen.SourceID == plan.SourceID &&
		audience.PrincipalID == plan.PrincipalID && audience.InterfaceKey == plan.InterfaceKey &&
		audience.DeliveryEpoch == plan.DeliveryEpoch && audience.ExternalAccountRef == plan.ExternalAccountRef &&
		audience.ConversationRef == plan.ConversationRef && audience.ConversationScope == plan.ConversationScope
}
