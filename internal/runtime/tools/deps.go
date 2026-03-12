package tools

import (
	"context"
	"database/sql"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/models"
	runtimepipeline "empireai/internal/runtime/pipeline"
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
	SpawnAgentFor(verticalID string, cfg models.AgentConfig) error
	TeardownAgent(agentID string) error
	ReconfigureAgent(agentID string, cfg models.AgentConfig) error
}

type ManagerProvider func() Manager

type ExecutorOptions struct {
	Manager         Manager
	ManagerProvider ManagerProvider
	Config          *config.Config
	MailboxStore    MailboxPersistence
	SQLDB           *sql.DB
}
