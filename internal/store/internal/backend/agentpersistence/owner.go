package agentpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type DirectiveEventCommitter interface {
	CommitDirectiveEventTx(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, *privaterunforkrevision.Effects, events.AdmittedEvent) (runtimebus.EventAppendOutcome, error)
	LoadDirectiveEventTx(context.Context, *sql.Tx, string) (events.AdmittedEvent, bool, error)
}

type DirectivePipelineOwner interface {
	TerminalizePipelineObligationTx(context.Context, *sql.Tx, *privaterunforkrevision.Effects, string, runtimepipelineobligation.Disposition, time.Time) error
}

type ProviderAttemptDrainPostgresCapturer interface {
	CaptureProviderAttemptDrainsPostgresTx(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, *privaterunforkrevision.Effects, runtimeeffects.ProviderAttemptDrainCapture) (runtimeeffects.ProviderAttemptDrainCaptureResult, error)
}

type ProviderAttemptDrainSQLiteCapturer interface {
	CaptureProviderAttemptDrainsSQLiteTx(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, *privaterunforkrevision.Effects, runtimeeffects.ProviderAttemptDrainCapture) (runtimeeffects.ProviderAttemptDrainCaptureResult, error)
}

type AgentSource interface {
	LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error)
}

type AgentPostgresOwner struct {
	backend        *postgresbackend.Backend
	schemaGuard    func() error
	agents         AgentSource
	events         DirectiveEventCommitter
	pipeline       DirectivePipelineOwner
	providerDrains ProviderAttemptDrainPostgresCapturer
}

func NewPostgres(backend *postgresbackend.Backend, schemaGuard func() error, agents AgentSource) (*AgentPostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("agent postgres backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("agent postgres schema guard is required")
	}
	if agents == nil {
		return nil, fmt.Errorf("agent source is required")
	}
	return &AgentPostgresOwner{backend: backend, schemaGuard: schemaGuard, agents: agents}, nil
}

func (s *AgentPostgresOwner) requireCurrentSchema() error {
	if s == nil || s.schemaGuard == nil {
		return fmt.Errorf("agent postgres schema guard is required")
	}
	return s.schemaGuard()
}

type AgentSQLiteOwner struct {
	backend        *sqlitebackend.Backend
	schemaGuard    func() error
	agents         AgentSource
	events         DirectiveEventCommitter
	pipeline       DirectivePipelineOwner
	providerDrains ProviderAttemptDrainSQLiteCapturer
}

func (s *AgentPostgresOwner) BindProviderAttemptDrains(owner ProviderAttemptDrainPostgresCapturer) error {
	if s == nil || owner == nil {
		return fmt.Errorf("agent lifecycle PostgreSQL provider-drain owner is required")
	}
	if s.providerDrains != nil {
		return fmt.Errorf("agent lifecycle PostgreSQL provider-drain owner is already bound")
	}
	s.providerDrains = owner
	return nil
}

func (s *AgentSQLiteOwner) BindProviderAttemptDrains(owner ProviderAttemptDrainSQLiteCapturer) error {
	if s == nil || owner == nil {
		return fmt.Errorf("agent lifecycle SQLite provider-drain owner is required")
	}
	if s.providerDrains != nil {
		return fmt.Errorf("agent lifecycle SQLite provider-drain owner is already bound")
	}
	s.providerDrains = owner
	return nil
}

func (s *AgentPostgresOwner) BindDirectiveDependencies(events DirectiveEventCommitter, pipeline DirectivePipelineOwner) error {
	if s == nil || events == nil || pipeline == nil {
		return fmt.Errorf("agent directive PostgreSQL dependencies are required")
	}
	if s.events != nil || s.pipeline != nil {
		return fmt.Errorf("agent directive PostgreSQL dependencies are already bound")
	}
	s.events, s.pipeline = events, pipeline
	return nil
}

func (s *AgentSQLiteOwner) BindDirectiveDependencies(events DirectiveEventCommitter, pipeline DirectivePipelineOwner) error {
	if s == nil || events == nil || pipeline == nil {
		return fmt.Errorf("agent directive SQLite dependencies are required")
	}
	if s.events != nil || s.pipeline != nil {
		return fmt.Errorf("agent directive SQLite dependencies are already bound")
	}
	s.events, s.pipeline = events, pipeline
	return nil
}

func NewSQLite(backend *sqlitebackend.Backend, schemaGuard func() error, agents AgentSource) (*AgentSQLiteOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("agent sqlite backend is required")
	}
	if schemaGuard == nil {
		return nil, fmt.Errorf("agent sqlite schema guard is required")
	}
	if agents == nil {
		return nil, fmt.Errorf("agent source is required")
	}
	return &AgentSQLiteOwner{backend: backend, schemaGuard: schemaGuard, agents: agents}, nil
}

func (s *AgentSQLiteOwner) requireCurrentSchema() error {
	if s == nil || s.schemaGuard == nil {
		return fmt.Errorf("agent sqlite schema guard is required")
	}
	return s.schemaGuard()
}
