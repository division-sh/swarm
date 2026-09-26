package startupownership

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

type selectedFanOutAuthoritySource struct{ authority runtimeownership.Authority }

func (s selectedFanOutAuthoritySource) Authority() (runtimeownership.Authority, error) {
	return s.authority, nil
}

func selectedFanOutAdmission(ctx context.Context, tx *sql.Tx, sqlite bool, grant runtimeownership.GrantEvidence) (*fanOutAdmission, error) {
	if err := grant.Validate(); err != nil {
		return nil, err
	}
	if grant.State != runtimeownership.GrantAdmitted || grant.SelectedFork == nil {
		return nil, errors.New("selected deployment serving requires an admitted selected-fork grant")
	}
	backend := "postgres_retained_session"
	if sqlite {
		backend = "sqlite_retained_owner"
	}
	authority, exists, err := loadAuthorityHeadModeTx(ctx, tx, backend, sqlite, false)
	if err != nil {
		return nil, err
	}
	if !exists || authority.State != runtimeownership.StateActive ||
		grant.ProcessAuthorityID != authority.AuthorityID || grant.ProcessOwnerID != authority.OwnerID || grant.ProcessBootID != authority.BootID {
		return nil, errors.New("selected deployment grant differs from current process authority")
	}
	return &fanOutAdmission{session: selectedFanOutAuthoritySource{authority: authority}, sqlite: sqlite}, nil
}

func (s *StartupPostgresOwner) BindSelectedDeploymentFanOutGrant(grant runtimeownership.GrantEvidence) (owner pipeline.FanOutObligationOwner, err error) {
	if s == nil || s.fanOutPipeline == nil {
		return nil, errors.New("selected deployment serving requires assembled PostgreSQL startup owner")
	}
	err = s.backend.RunTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		admission, err := selectedFanOutAdmission(ctx, tx, false, grant)
		if err != nil {
			return err
		}
		owned, err := admission.AdmitFanOutRunTx(ctx, tx, grant, grant.SelectedFork.ForkRunID)
		if err != nil {
			return err
		}
		if !owned {
			return fanoutobligation.ErrStaleClaim
		}
		owner, err = s.fanOutPipeline.BindSelectedDeploymentFanOutGrant(admission, grant)
		return err
	})
	return owner, err
}

func (s *StartupSQLiteOwner) BindSelectedDeploymentFanOutGrant(grant runtimeownership.GrantEvidence) (owner pipeline.FanOutObligationOwner, err error) {
	if s == nil || s.fanOutPipeline == nil {
		return nil, errors.New("selected deployment serving requires assembled SQLite startup owner")
	}
	err = s.backend.RunTransaction(context.Background(), "bind selected deployment fan-out grant", func(ctx context.Context, tx *sql.Tx) error {
		admission, err := selectedFanOutAdmission(ctx, tx, true, grant)
		if err != nil {
			return err
		}
		owned, err := admission.AdmitFanOutRunTx(ctx, tx, grant, grant.SelectedFork.ForkRunID)
		if err != nil {
			return err
		}
		if !owned {
			return fanoutobligation.ErrStaleClaim
		}
		owner, err = s.fanOutPipeline.BindSelectedDeploymentFanOutGrant(admission, grant)
		return err
	})
	return owner, err
}

func (s *StartupPostgresOwner) ListSelectedDeploymentFeeds(ctx context.Context, grant runtimeownership.GrantEvidence) ([]fanoutobligation.Intent, error) {
	if s == nil || s.fanOutPipeline == nil {
		return nil, errors.New("selected deployment listing requires assembled PostgreSQL startup owner")
	}
	var admission *fanOutAdmission
	if err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		admission, err = selectedFanOutAdmission(ctx, tx, false, grant)
		return err
	}); err != nil {
		return nil, err
	}
	return s.fanOutPipeline.ListSelectedDeploymentFeeds(ctx, admission, grant)
}

func (s *StartupSQLiteOwner) ListSelectedDeploymentFeeds(ctx context.Context, grant runtimeownership.GrantEvidence) ([]fanoutobligation.Intent, error) {
	if s == nil || s.fanOutPipeline == nil {
		return nil, errors.New("selected deployment listing requires assembled SQLite startup owner")
	}
	var admission *fanOutAdmission
	if err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		admission, err = selectedFanOutAdmission(ctx, tx, true, grant)
		return err
	}); err != nil {
		return nil, err
	}
	return s.fanOutPipeline.ListSelectedDeploymentFeeds(ctx, admission, grant)
}
