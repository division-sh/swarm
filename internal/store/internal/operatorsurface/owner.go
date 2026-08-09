package operatorsurface

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type RuntimeDiagnosticsSource interface {
	PipelineObligations() runtimepipelineobligation.Store
	ReadTimerObligations(context.Context, runtimetimerobligation.Scope, time.Time) (runtimetimerobligation.Snapshot, error)
	ListOperatorConversationTurns(context.Context, OperatorConversationTurnListOptions) (OperatorConversationTurnListResult, error)
	LoadOperatorPublicConversationTurn(context.Context, string, string) (OperatorPublicConversationTurnDetail, error)
	LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error)
}

func (s *OperatorPostgres) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	return s.runtime.LoadAgents(ctx)
}

func (s *OperatorSQLite) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	return s.runtime.LoadAgents(ctx)
}

func (s *OperatorPostgres) ListOperatorConversationTurns(ctx context.Context, opts OperatorConversationTurnListOptions) (OperatorConversationTurnListResult, error) {
	return s.runtime.ListOperatorConversationTurns(ctx, opts)
}

func (s *OperatorPostgres) LoadOperatorPublicConversationTurn(ctx context.Context, sessionID, turnID string) (OperatorPublicConversationTurnDetail, error) {
	return s.runtime.LoadOperatorPublicConversationTurn(ctx, sessionID, turnID)
}

func (s *OperatorSQLite) ListOperatorConversationTurns(ctx context.Context, opts OperatorConversationTurnListOptions) (OperatorConversationTurnListResult, error) {
	return s.runtime.ListOperatorConversationTurns(ctx, opts)
}

func (s *OperatorSQLite) LoadOperatorPublicConversationTurn(ctx context.Context, sessionID, turnID string) (OperatorPublicConversationTurnDetail, error) {
	return s.runtime.LoadOperatorPublicConversationTurn(ctx, sessionID, turnID)
}

type OperatorPostgres struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
	runtime     RuntimeDiagnosticsSource
}

func NewPostgres(backend *postgresbackend.Backend, schemaGuard func() error, runtime RuntimeDiagnosticsSource) (*OperatorPostgres, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("operator postgres backend is required")
	}
	return &OperatorPostgres{backend: backend, schemaGuard: schemaGuard, runtime: runtime}, nil
}

func (s *OperatorPostgres) requireCurrentSchema() error {
	if s == nil || s.schemaGuard == nil {
		return fmt.Errorf("operator postgres schema guard is required")
	}
	return s.schemaGuard()
}

func (s *OperatorPostgres) RequireCurrentSchema() error {
	return s.requireCurrentSchema()
}

func (s *OperatorPostgres) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.backend.QueryContext(ctx, query, args...)
}

func (s *OperatorPostgres) queryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return s.backend.QueryRowContext(ctx, query, args...)
}

type OperatorSQLite struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
	nowFn       func() time.Time
	runtime     RuntimeDiagnosticsSource
}

func NewSQLite(backend *sqlitebackend.Backend, schemaGuard func() error, nowFn func() time.Time, runtime RuntimeDiagnosticsSource) (*OperatorSQLite, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("operator sqlite backend is required")
	}
	return &OperatorSQLite{backend: backend, schemaGuard: schemaGuard, nowFn: nowFn, runtime: runtime}, nil
}

func (s *OperatorSQLite) requireCurrentSchema() error {
	if s == nil || s.schemaGuard == nil {
		return fmt.Errorf("operator sqlite schema guard is required")
	}
	return s.schemaGuard()
}

func (s *OperatorSQLite) RequireCurrentSchema() error {
	return s.requireCurrentSchema()
}

func (s *OperatorSQLite) now() time.Time {
	if s == nil || s.nowFn == nil {
		return time.Now().UTC()
	}
	return s.nowFn().UTC()
}

func (s *OperatorSQLite) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.backend.QueryContext(ctx, query, args...)
}

func (s *OperatorSQLite) queryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return s.backend.QueryRowContext(ctx, query, args...)
}

var (
	operatorPostgresDelivery = mustDeliveryAdapter(deliveryadapter.DialectPostgres)
	operatorSQLiteDelivery   = mustDeliveryAdapter(deliveryadapter.DialectSQLite)
)

func mustDeliveryAdapter(dialect deliveryadapter.Dialect) *deliveryadapter.Adapter {
	adapter, err := deliveryadapter.NewAdapter(dialect)
	if err != nil {
		panic(err)
	}
	return adapter
}
