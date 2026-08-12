package tools

import (
	"context"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packs"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type GenericScheduleAdmission interface {
	Admit(context.Context, runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.AdmissionResult, error)
}

type WorkflowInstanceLoader interface {
	Load(ctx context.Context, route runtimeflowidentity.Route) (runtimepipeline.WorkflowInstance, bool, error)
}

type EventPublisher interface {
	Publish(ctx context.Context, evt events.Event) error
	PublishDirect(ctx context.Context, evt events.Event, recipients []string) error
	PublishDirectRoutes(ctx context.Context, evt events.Event, routes []events.DeliveryRoute) error
}

type Manager interface {
	ResolveAgentConfig(agentID, flowInstance string) (models.AgentConfig, error)
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
	HumanTaskStore     HumanTaskCardStore
	WorkflowInstances  WorkflowInstanceLoader
	MCPClient          *runtimemcp.Client
	WorkflowSource     semanticview.Source
	ChannelBindings    []packs.OutboundBindingPlan
	ActivityExecutor   DurableActivityExecutor
	WorkspaceResolver  workspace.Resolver
	ModelRuntimes      llm.AgentRuntimeResolver
	AuthorityProvider  runtimeauthority.Provider
	EmitRegistry       *EmitRegistry
	GenericSchedules   GenericScheduleAdmission
	// Trusted runtime/test escape hatch for exercising retained legacy handlers.
	// Actor-authored config must never enable legacy entity tools for normal agents.
	AllowInternalLegacyEntityTools bool
}
