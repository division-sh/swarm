package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	"github.com/google/uuid"
)

// ChannelInputBindingContradiction is a receipt for one downstream-admission
// negative. It cannot issue selected execution or generation authority.
type ChannelInputBindingContradiction struct {
	run, binding string
	ordinary     runlifecycle.ServedSetupRunStorage
	created      time.Time
}

func CaptureChannelInputBindingContradictionTx(ctx context.Context, tx *sql.Tx, run string) (*ChannelInputBindingContradiction, error) {
	if tx == nil || uuid.Validate(run) != nil {
		return nil, errors.New("channel binding fault requires its exact native run")
	}
	ordinary, err := runlifecycle.ReadServedSetupRunStorageTx(ctx, tx, run)
	if err != nil {
		return nil, err
	}
	if ordinary.OriginKind != "event" || ordinary.Status != "running" || ordinary.Trigger == "" {
		return nil, errors.New("channel binding fault requires the original ordinary event run")
	}
	if _, err := loadRunForkSelectedContractBinding(ctx, tx, run); err == nil {
		return nil, errors.New("channel binding fault refuses an already-bound run")
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	return &ChannelInputBindingContradiction{run: run, binding: uuid.NewString(), ordinary: ordinary, created: time.Now().UTC()}, nil
}

func (r *ChannelInputBindingContradiction) InstallTx(ctx context.Context, tx *sql.Tx) error {
	return r.applyTx(ctx, tx, true)
}

func (r *ChannelInputBindingContradiction) RestoreTx(ctx context.Context, tx *sql.Tx) error {
	return r.applyTx(ctx, tx, false)
}

func (r *ChannelInputBindingContradiction) applyTx(ctx context.Context, tx *sql.Tx, install bool) error {
	if r == nil || tx == nil || uuid.Validate(r.run) != nil || uuid.Validate(r.binding) != nil {
		return errors.New("channel binding fault requires its captured ordinary run")
	}
	var result sql.Result
	var err error
	if install {
		result, err = tx.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_bindings
			(binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,mode,created_at)
			SELECT $1,r.run_id,r.run_id,'deployment_revision',1,'selected_contracts',$3
			FROM runs r WHERE r.run_id=$2 AND r.status=$4 AND r.origin_kind=$5 AND r.bundle_hash=$6 AND r.trigger_event_type=$7
			AND NOT EXISTS (SELECT 1 FROM run_fork_selected_contract_bindings b WHERE b.fork_run_id=r.run_id)`,
			r.binding, r.run, r.created, r.ordinary.Status, r.ordinary.OriginKind, r.ordinary.BundleHash, r.ordinary.Trigger)
	} else {
		result, err = tx.ExecContext(ctx, `DELETE FROM run_fork_selected_contract_bindings
			WHERE binding_id=$1 AND fork_run_id=$2 AND source_run_id=$2 AND fork_point_kind='deployment_revision'
			AND fork_revision=1 AND mode='selected_contracts' AND created_at=$3 AND fork_event_id IS NULL AND bundle_hash IS NULL`, r.binding, r.run, r.created)
	}
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("channel binding fault refused a changed run or receipt: affected=%d", count)
	}
	return nil
}
