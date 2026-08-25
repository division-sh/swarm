package operatorsurface

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type PipelineObligationSource interface {
	PipelineObligations() runtimepipelineobligation.Store
}

type TimerObligationSource interface {
	ReadTimerObligations(context.Context, runtimetimerobligation.Scope, time.Time) (runtimetimerobligation.Snapshot, error)
}

type DeadLetterProjection interface {
	LoadOperatorDeliveryDeadLetters(context.Context, string, int64) ([]operatorread.OperatorDeadLetterRecord, error)
}

type agentDeliveryProjection interface {
	DeliveryLifecycleSnapshotPageForAgent(context.Context, runtimedelivery.AgentLifecyclePageQuery) (runtimedelivery.SnapshotPage, error)
	DeliveryDiagnosticSnapshotPageForAgent(context.Context, runtimedelivery.AgentDiagnosticPageQuery) (runtimedelivery.SnapshotPage, error)
	DeliveryDiagnosticCountsForAgentSince(context.Context, agentidentity.Identity, time.Time) (runtimedelivery.AgentDiagnosticCounts, error)
}

type RunPostgres struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
	pipeline    PipelineObligationSource
	timers      TimerObligationSource
	deadLetters DeadLetterProjection
}

type EntityPostgres struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
}

type AgentPostgres struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
	delivery    agentDeliveryProjection
	deadLetters DeadLetterProjection
}

type ConversationPostgres struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
}

type ObservabilityPostgres struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
}

func requirePostgresBackend(backend *postgresbackend.Backend) error {
	if backend == nil || !backend.Valid() {
		return fmt.Errorf("operator postgres backend is required")
	}
	return nil
}

func NewRunPostgres(backend *postgresbackend.Backend, schemaGuard func() error, pipeline PipelineObligationSource, timers TimerObligationSource, deadLetters DeadLetterProjection) (*RunPostgres, error) {
	if err := requirePostgresBackend(backend); err != nil {
		return nil, err
	}
	if pipeline == nil || timers == nil || deadLetters == nil {
		return nil, fmt.Errorf("run diagnostics sources are required")
	}
	return &RunPostgres{backend: backend, schemaGuard: schemaGuard, pipeline: pipeline, timers: timers, deadLetters: deadLetters}, nil
}

func NewEntityPostgres(backend *postgresbackend.Backend, schemaGuard func() error) (*EntityPostgres, error) {
	if err := requirePostgresBackend(backend); err != nil {
		return nil, err
	}
	return &EntityPostgres{backend: backend, schemaGuard: schemaGuard}, nil
}

func NewAgentPostgres(backend *postgresbackend.Backend, schemaGuard func() error, deadLetters DeadLetterProjection) (*AgentPostgres, error) {
	if err := requirePostgresBackend(backend); err != nil {
		return nil, err
	}
	if deadLetters == nil {
		return nil, fmt.Errorf("agent read sources are required")
	}
	owner := &AgentPostgres{backend: backend, schemaGuard: schemaGuard, deadLetters: deadLetters}
	owner.delivery = owner
	return owner, nil
}

func NewConversationPostgres(backend *postgresbackend.Backend, schemaGuard func() error) (*ConversationPostgres, error) {
	if err := requirePostgresBackend(backend); err != nil {
		return nil, err
	}
	return &ConversationPostgres{backend: backend, schemaGuard: schemaGuard}, nil
}

func NewObservabilityPostgres(backend *postgresbackend.Backend, schemaGuard func() error) (*ObservabilityPostgres, error) {
	if err := requirePostgresBackend(backend); err != nil {
		return nil, err
	}
	return &ObservabilityPostgres{backend: backend, schemaGuard: schemaGuard}, nil
}

func (s *RunPostgres) requireCurrentSchema() error {
	if s == nil {
		return fmt.Errorf("operator run postgres schema guard is required")
	}
	return requireSchema("run postgres", s.schemaGuard)
}
func (s *EntityPostgres) requireCurrentSchema() error {
	if s == nil {
		return fmt.Errorf("operator entity postgres schema guard is required")
	}
	return requireSchema("entity postgres", s.schemaGuard)
}
func (s *AgentPostgres) requireCurrentSchema() error {
	if s == nil {
		return fmt.Errorf("operator agent postgres schema guard is required")
	}
	return requireSchema("agent postgres", s.schemaGuard)
}
func (s *ConversationPostgres) requireCurrentSchema() error {
	if s == nil {
		return fmt.Errorf("operator conversation postgres schema guard is required")
	}
	return requireSchema("conversation postgres", s.schemaGuard)
}
func (s *ObservabilityPostgres) requireCurrentSchema() error {
	if s == nil {
		return fmt.Errorf("operator observability postgres schema guard is required")
	}
	return requireSchema("observability postgres", s.schemaGuard)
}

func (s *RunPostgres) RequireCurrentSchema() error          { return s.requireCurrentSchema() }
func (s *AgentPostgres) RequireCurrentSchema() error        { return s.requireCurrentSchema() }
func (s *ConversationPostgres) RequireCurrentSchema() error { return s.requireCurrentSchema() }

func (s *AgentPostgres) LoadOperatorDeliveryDeadLetters(ctx context.Context, deliveryID string, claimVersion int64) ([]operatorread.OperatorDeadLetterRecord, error) {
	return s.deadLetters.LoadOperatorDeliveryDeadLetters(ctx, deliveryID, claimVersion)
}

type RunSQLite struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
	nowFn       func() time.Time
	pipeline    PipelineObligationSource
	timers      TimerObligationSource
	deadLetters DeadLetterProjection
}

type EntitySQLite struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
}

type AgentSQLite struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
	delivery    agentDeliveryProjection
	deadLetters DeadLetterProjection
}

type ConversationSQLite struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
}

type ObservabilitySQLite struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
}

func requireSQLiteBackend(backend *sqlitebackend.Backend) error {
	if backend == nil || !backend.Valid() {
		return fmt.Errorf("operator sqlite backend is required")
	}
	return nil
}

func NewRunSQLite(backend *sqlitebackend.Backend, schemaGuard func() error, nowFn func() time.Time, pipeline PipelineObligationSource, timers TimerObligationSource, deadLetters DeadLetterProjection) (*RunSQLite, error) {
	if err := requireSQLiteBackend(backend); err != nil {
		return nil, err
	}
	if pipeline == nil || timers == nil || deadLetters == nil {
		return nil, fmt.Errorf("run diagnostics sources are required")
	}
	return &RunSQLite{backend: backend, schemaGuard: schemaGuard, nowFn: nowFn, pipeline: pipeline, timers: timers, deadLetters: deadLetters}, nil
}

func NewEntitySQLite(backend *sqlitebackend.Backend, schemaGuard func() error) (*EntitySQLite, error) {
	if err := requireSQLiteBackend(backend); err != nil {
		return nil, err
	}
	return &EntitySQLite{backend: backend, schemaGuard: schemaGuard}, nil
}

func NewAgentSQLite(backend *sqlitebackend.Backend, schemaGuard func() error, deadLetters DeadLetterProjection) (*AgentSQLite, error) {
	if err := requireSQLiteBackend(backend); err != nil {
		return nil, err
	}
	if deadLetters == nil {
		return nil, fmt.Errorf("agent read sources are required")
	}
	owner := &AgentSQLite{backend: backend, schemaGuard: schemaGuard, deadLetters: deadLetters}
	owner.delivery = owner
	return owner, nil
}

func NewConversationSQLite(backend *sqlitebackend.Backend, schemaGuard func() error) (*ConversationSQLite, error) {
	if err := requireSQLiteBackend(backend); err != nil {
		return nil, err
	}
	return &ConversationSQLite{backend: backend, schemaGuard: schemaGuard}, nil
}

func NewObservabilitySQLite(backend *sqlitebackend.Backend, schemaGuard func() error) (*ObservabilitySQLite, error) {
	if err := requireSQLiteBackend(backend); err != nil {
		return nil, err
	}
	return &ObservabilitySQLite{backend: backend, schemaGuard: schemaGuard}, nil
}

func (s *RunSQLite) requireCurrentSchema() error {
	if s == nil {
		return fmt.Errorf("operator run sqlite schema guard is required")
	}
	return requireSchema("run sqlite", s.schemaGuard)
}
func (s *EntitySQLite) requireCurrentSchema() error {
	if s == nil {
		return fmt.Errorf("operator entity sqlite schema guard is required")
	}
	return requireSchema("entity sqlite", s.schemaGuard)
}
func (s *AgentSQLite) requireCurrentSchema() error {
	if s == nil {
		return fmt.Errorf("operator agent sqlite schema guard is required")
	}
	return requireSchema("agent sqlite", s.schemaGuard)
}
func (s *ConversationSQLite) requireCurrentSchema() error {
	if s == nil {
		return fmt.Errorf("operator conversation sqlite schema guard is required")
	}
	return requireSchema("conversation sqlite", s.schemaGuard)
}
func (s *ObservabilitySQLite) requireCurrentSchema() error {
	if s == nil {
		return fmt.Errorf("operator observability sqlite schema guard is required")
	}
	return requireSchema("observability sqlite", s.schemaGuard)
}

func (s *RunSQLite) RequireCurrentSchema() error          { return s.requireCurrentSchema() }
func (s *AgentSQLite) RequireCurrentSchema() error        { return s.requireCurrentSchema() }
func (s *ConversationSQLite) RequireCurrentSchema() error { return s.requireCurrentSchema() }

func (s *RunSQLite) now() time.Time { return ownerNow(s.nowFn) }

func (s *AgentSQLite) LoadOperatorDeliveryDeadLetters(ctx context.Context, deliveryID string, claimVersion int64) ([]operatorread.OperatorDeadLetterRecord, error) {
	return s.deadLetters.LoadOperatorDeliveryDeadLetters(ctx, deliveryID, claimVersion)
}

func requireSchema(name string, guard func() error) error {
	if guard == nil {
		return fmt.Errorf("operator %s schema guard is required", name)
	}
	return guard()
}

func ownerNow(nowFn func() time.Time) time.Time {
	if nowFn == nil {
		return time.Now().UTC()
	}
	return nowFn().UTC()
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
