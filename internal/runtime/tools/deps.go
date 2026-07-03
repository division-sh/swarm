package tools

import (
	"context"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type Schedule = runtimepipeline.Schedule

type SchedulePersistence = runtimepipeline.SchedulePersistence

type WorkflowInstanceLoader interface {
	Enabled() bool
	Load(ctx context.Context, instanceID string) (runtimepipeline.WorkflowInstance, bool, error)
}

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
	Manager            Manager
	ManagerProvider    ManagerProvider
	Config             *config.Config
	Credentials        runtimecredentials.Store
	ManagedCredentials runtimemanagedcredentials.Store
	MailboxStore       MailboxPersistence
	EntityStore        EntityPersistence
	HumanTaskStore     HumanTaskPersistence
	WorkflowInstances  WorkflowInstanceLoader
	MCPClient          *runtimemcp.Client
	WorkflowSource     semanticview.Source
	WorkspaceResolver  workspace.Resolver
	ModelRuntime       llm.Runtime
	AuthorityProvider  runtimeauthority.Provider
	EmitRegistry       *EmitRegistry
	// Trusted runtime/test escape hatch for exercising retained legacy handlers.
	// Actor-authored config must never enable legacy entity tools for normal agents.
	AllowInternalLegacyEntityTools bool
}
