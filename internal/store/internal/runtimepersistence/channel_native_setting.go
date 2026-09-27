package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
)

func (s *PostgresStore) AttachNativeInboxSetting(ctx context.Context, admission channelnative.Admission) (channelnative.Setting, error) {
	if s == nil || s.backend == nil {
		return channelnative.Setting{}, fmt.Errorf("postgres native inbox setting store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return channelnative.Setting{}, err
	}
	var setting channelnative.Setting
	err := s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		setting, err = channeldelivery.AttachNativeInboxSettingTx(txctx, tx, admission, true)
		return err
	})
	return setting, err
}

func (s *SQLiteRuntimeStore) AttachNativeInboxSetting(ctx context.Context, admission channelnative.Admission) (channelnative.Setting, error) {
	if s == nil || s.backend == nil {
		return channelnative.Setting{}, fmt.Errorf("sqlite native inbox setting store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return channelnative.Setting{}, err
	}
	var setting channelnative.Setting
	err := s.backend.RunTransaction(ctx, "attach native inbox setting", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		setting, err = channeldelivery.AttachNativeInboxSettingTx(txctx, tx, admission, false)
		return err
	})
	return setting, err
}

func (s *PostgresStore) RetireStaleNativeInboxConsumers(ctx context.Context) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres native inbox setting store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return channeldelivery.RetireStaleNativeInboxConsumersTx(txctx, tx, true)
	})
}

func (s *SQLiteRuntimeStore) RetireStaleNativeInboxConsumers(ctx context.Context) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite native inbox setting store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, "retire native inbox setting consumers", func(txctx context.Context, tx *sql.Tx) error {
		return channeldelivery.RetireStaleNativeInboxConsumersTx(txctx, tx, false)
	})
}
