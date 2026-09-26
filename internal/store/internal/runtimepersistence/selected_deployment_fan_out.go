package runtimepersistence

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

func (s *PostgresStore) BindSelectedDeploymentFanOutGrant(grant startupownership.GrantEvidence) (pipeline.FanOutObligationOwner, error) {
	if s == nil || s.startupPostgresOwner == nil {
		return nil, errors.New("selected deployment serving requires assembled PostgreSQL store")
	}
	return s.startupPostgresOwner.BindSelectedDeploymentFanOutGrant(grant)
}

func (s *SQLiteRuntimeStore) BindSelectedDeploymentFanOutGrant(grant startupownership.GrantEvidence) (pipeline.FanOutObligationOwner, error) {
	if s == nil || s.startupSQLiteOwner == nil {
		return nil, errors.New("selected deployment serving requires assembled SQLite store")
	}
	return s.startupSQLiteOwner.BindSelectedDeploymentFanOutGrant(grant)
}

func (s *PostgresStore) ListSelectedDeploymentFeeds(ctx context.Context, grant startupownership.GrantEvidence) ([]fanoutobligation.Intent, error) {
	if s == nil || s.startupPostgresOwner == nil {
		return nil, errors.New("selected deployment listing requires assembled PostgreSQL store")
	}
	return s.startupPostgresOwner.ListSelectedDeploymentFeeds(ctx, grant)
}

func (s *SQLiteRuntimeStore) ListSelectedDeploymentFeeds(ctx context.Context, grant startupownership.GrantEvidence) ([]fanoutobligation.Intent, error) {
	if s == nil || s.startupSQLiteOwner == nil {
		return nil, errors.New("selected deployment listing requires assembled SQLite store")
	}
	return s.startupSQLiteOwner.ListSelectedDeploymentFeeds(ctx, grant)
}
