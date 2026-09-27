package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
)

func (s *PostgresStore) ListCurrentChannelDeliveryPlans(ctx context.Context, cursor string, limit int) ([]channeldelivery.Plan, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var plans []channeldelivery.Plan
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		plans, err = channeldelivery.ListCurrentPlans(txctx, tx, cursor, limit, true)
		return err
	})
	return plans, err
}

func (s *SQLiteRuntimeStore) ListCurrentChannelDeliveryPlans(ctx context.Context, cursor string, limit int) ([]channeldelivery.Plan, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	var plans []channeldelivery.Plan
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		plans, err = channeldelivery.ListCurrentPlans(txctx, tx, cursor, limit, false)
		return err
	})
	return plans, err
}

func (s *PostgresStore) FreezeAndPersistChannelRender(ctx context.Context, deliveryID string) (channeldelivery.StoredRender, error) {
	if s == nil || s.backend == nil {
		return channeldelivery.StoredRender{}, fmt.Errorf("postgres channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return channeldelivery.StoredRender{}, err
	}
	var stored channeldelivery.StoredRender
	err := s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		stored, err = freezeAndPersistChannelRenderTx(txctx, tx, deliveryID, true)
		return err
	})
	return stored, err
}

func (s *SQLiteRuntimeStore) FreezeAndPersistChannelRender(ctx context.Context, deliveryID string) (channeldelivery.StoredRender, error) {
	if s == nil || s.backend == nil {
		return channeldelivery.StoredRender{}, fmt.Errorf("sqlite channel delivery store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return channeldelivery.StoredRender{}, err
	}
	var stored channeldelivery.StoredRender
	err := s.backend.RunTransaction(ctx, "freeze channel delivery render", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		stored, err = freezeAndPersistChannelRenderTx(txctx, tx, deliveryID, false)
		return err
	})
	return stored, err
}

func freezeAndPersistChannelRenderTx(ctx context.Context, tx *sql.Tx, deliveryID string, postgres bool) (channeldelivery.StoredRender, error) {
	plan, found, err := channeldelivery.LoadPlan(ctx, tx, deliveryID, postgres)
	if err != nil {
		return channeldelivery.StoredRender{}, err
	}
	if !found {
		return channeldelivery.StoredRender{}, fmt.Errorf("channel delivery plan %s is missing", deliveryID)
	}
	var frozen render.Frozen
	frozen, err = channeldelivery.FreezeCurrentSourceTx(ctx, tx, plan, postgres)
	if err != nil {
		return channeldelivery.StoredRender{}, err
	}
	id, _, err := channeldelivery.PersistRenderTx(ctx, tx, deliveryID, frozen, postgres)
	if err != nil {
		return channeldelivery.StoredRender{}, err
	}
	return channeldelivery.StoredRender{RenderID: id, DeliveryID: deliveryID, Frozen: frozen}, nil
}
