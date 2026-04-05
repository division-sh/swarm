package tools

import (
	"context"
	"database/sql"

	"swarm/internal/config"
	"swarm/internal/events"
	runtimeauthority "swarm/internal/runtime/authority"
	models "swarm/internal/runtime/core/actors"
	runtimecredentials "swarm/internal/runtime/credentials"
	runtimemcp "swarm/internal/runtime/mcp"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	workspace "swarm/internal/runtime/workspace"
)

type Schedule = runtimepipeline.Schedule

type SchedulePersistence = runtimepipeline.SchedulePersistence

type EventPublisher interface {
	Publish(ctx context.Context, evt events.Event) error
	PublishDirect(ctx context.Context, evt events.Event, recipients []string) error
}

type Scheduler interface {
	Register(runtimepipeline.Schedule) error
	Stop()
}

type Manager interface {
	GetAgentConfig(agentID string) (models.AgentConfig, bool)
	SpawnAgentForEntity(entityID string, cfg models.AgentConfig) error
	TeardownAgent(agentID string) error
	ReconfigureAgent(agentID string, cfg models.AgentConfig) error
}

type ManagerProvider func() Manager

type ExecutorOptions struct {
	Manager           Manager
	ManagerProvider   ManagerProvider
	Config            *config.Config
	Credentials       runtimecredentials.Store
	MailboxStore      MailboxPersistence
	MCPClient         *runtimemcp.Client
	SQLDB             *sql.DB
	WorkflowSource    semanticview.Source
	FlowActivator     runtimepipeline.FlowInstanceActivator
	WorkspaceResolver workspace.Resolver
	AuthorityProvider runtimeauthority.Provider
	EmitRegistry      *EmitRegistry
}
