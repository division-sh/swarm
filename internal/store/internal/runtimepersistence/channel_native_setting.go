package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
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
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		setting, changed, err = channeldelivery.AttachNativeInboxSettingTx(txctx, tx, admission, true)
		return err
	})
	return setting, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary|render.ReconcileNative))
}

func (s *SQLiteRuntimeStore) AttachNativeInboxSetting(ctx context.Context, admission channelnative.Admission) (channelnative.Setting, error) {
	if s == nil || s.backend == nil {
		return channelnative.Setting{}, fmt.Errorf("sqlite native inbox setting store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return channelnative.Setting{}, err
	}
	var setting channelnative.Setting
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "attach native inbox setting", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		setting, changed, err = channeldelivery.AttachNativeInboxSettingTx(txctx, tx, admission, false)
		return err
	})
	return setting, errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary|render.ReconcileNative))
}

func (s *PostgresStore) RetireStaleNativeInboxConsumers(ctx context.Context) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres native inbox setting store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = channeldelivery.RetireStaleNativeInboxConsumersTx(txctx, tx, true)
		return err
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary|render.ReconcileNative))
}

func (s *SQLiteRuntimeStore) RetireStaleNativeInboxConsumers(ctx context.Context) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite native inbox setting store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "retire native inbox setting consumers", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = channeldelivery.RetireStaleNativeInboxConsumersTx(txctx, tx, false)
		return err
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary|render.ReconcileNative))
}

func (s *PostgresStore) MarkNativeInboxSettingUnavailable(ctx context.Context, settingID string, generation int64) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres native inbox setting store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return channeldelivery.MarkNativeInboxSettingUnavailableTx(txctx, tx, settingID, generation, true)
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) MarkNativeInboxSettingUnavailable(ctx context.Context, settingID string, generation int64) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite native inbox setting store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "mark native inbox setting unavailable", func(txctx context.Context, tx *sql.Tx) error {
		return channeldelivery.MarkNativeInboxSettingUnavailableTx(txctx, tx, settingID, generation, false)
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged, render.ReconcileOrdinary))
}

func (s *PostgresStore) RecordNativeInboxQualification(ctx context.Context, req channelnative.QualificationRequest) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres native inbox qualification store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = channeldelivery.RecordNativeInboxQualificationTx(txctx, tx, req, true)
		return err
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *SQLiteRuntimeStore) RecordNativeInboxQualification(ctx context.Context, req channelnative.QualificationRequest) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite native inbox qualification store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	var changed bool
	acknowledged, err := s.backend.RunTransactionOutcome(ctx, "record native inbox qualification", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		changed, err = channeldelivery.RecordNativeInboxQualificationTx(txctx, tx, req, false)
		return err
	})
	return errors.Join(err, s.channelChanges.PublishAcknowledged(acknowledged && changed, render.ReconcileOrdinary))
}

func (s *PostgresStore) ReadNativeInboxQualification(ctx context.Context, activationID string) (channelnative.Qualification, error) {
	if s == nil || s.backend == nil {
		return channelnative.Qualification{}, fmt.Errorf("postgres native inbox qualification store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return channelnative.Qualification{}, err
	}
	var out channelnative.Qualification
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = channeldelivery.ReadNativeInboxQualificationTx(txctx, tx, activationID, true)
		return err
	})
	return out, err
}

func (s *SQLiteRuntimeStore) ReadNativeInboxQualification(ctx context.Context, activationID string) (channelnative.Qualification, error) {
	if s == nil || s.backend == nil {
		return channelnative.Qualification{}, fmt.Errorf("sqlite native inbox qualification store is unavailable")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return channelnative.Qualification{}, err
	}
	var out channelnative.Qualification
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = channeldelivery.ReadNativeInboxQualificationTx(txctx, tx, activationID, false)
		return err
	})
	return out, err
}
