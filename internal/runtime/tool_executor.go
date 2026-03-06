package runtime

import (
	"context"
	"database/sql"

	"empireai/internal/config"
	"empireai/internal/models"
	llm "empireai/internal/runtime/llm"
	runtimetools "empireai/internal/runtime/tools"
)

type RuntimeToolExecutor struct {
	inner     *runtimetools.Executor
	scheduler *Scheduler
}

func NewRuntimeToolExecutor(bus *EventBus, scheduler *Scheduler, manager *AgentManager, stores ...SchedulePersistence) *RuntimeToolExecutor {
	var publisher runtimetools.EventPublisher
	if bus != nil {
		publisher = bus
	}
	var sched runtimetools.Scheduler
	if scheduler != nil {
		sched = scheduler
	}
	var mgr runtimetools.Manager
	if manager != nil {
		mgr = manager
	}
	return &RuntimeToolExecutor{
		inner:     runtimetools.NewExecutor(publisher, sched, mgr, stores...),
		scheduler: scheduler,
	}
}

func (e *RuntimeToolExecutor) SetManager(manager runtimetools.Manager) {
	if e == nil || e.inner == nil {
		return
	}
	e.inner.SetManager(manager)
}

func (e *RuntimeToolExecutor) SetConfig(cfg *config.Config) {
	if e == nil || e.inner == nil {
		return
	}
	e.inner.SetConfig(cfg)
}

func (e *RuntimeToolExecutor) ToolDefinitions() []llm.ToolDefinition {
	if e == nil || e.inner == nil {
		return nil
	}
	return e.inner.ToolDefinitions()
}

func (e *RuntimeToolExecutor) Execute(ctx context.Context, name string, input any) (any, error) {
	if e == nil || e.inner == nil {
		return nil, nil
	}
	return e.inner.Execute(ctx, name, input)
}

func (e *RuntimeToolExecutor) SetMailboxStore(store MailboxPersistence) {
	if e == nil || e.inner == nil {
		return
	}
	e.inner.SetMailboxStore(store)
}

func (e *RuntimeToolExecutor) SetSQLDB(db *sql.DB) {
	if e == nil || e.inner == nil {
		return
	}
	e.inner.SetSQLDB(db)
}

func (e *RuntimeToolExecutor) execAgentMessage(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	return e.inner.ExecAgentMessageDirect(ctx, actor, input)
}

func (e *RuntimeToolExecutor) execSchedule(actor models.AgentConfig, input any) (any, error) {
	return e.inner.ExecScheduleDirect(actor, input)
}

func (e *RuntimeToolExecutor) execConfigureRouting(actor models.AgentConfig, input any) (any, error) {
	return e.inner.ExecConfigureRoutingDirect(actor, input)
}

func (e *RuntimeToolExecutor) execAgentHire(actor models.AgentConfig, input any) (any, error) {
	return e.inner.ExecAgentHireDirect(actor, input)
}

func (e *RuntimeToolExecutor) execAgentFire(actor models.AgentConfig, input any) (any, error) {
	return e.inner.ExecAgentFireDirect(actor, input)
}

func (e *RuntimeToolExecutor) execAgentReconfigure(actor models.AgentConfig, input any) (any, error) {
	return e.inner.ExecAgentReconfigureDirect(actor, input)
}

func (e *RuntimeToolExecutor) execMailboxSend(actor models.AgentConfig, input any) (any, error) {
	return e.inner.ExecMailboxSendDirect(actor, input)
}

func (e *RuntimeToolExecutor) execHumanTaskRequest(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	return e.inner.ExecHumanTaskRequestDirect(ctx, actor, input)
}

func (e *RuntimeToolExecutor) execHumanTaskDecide(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	return e.inner.ExecHumanTaskDecideDirect(ctx, actor, input)
}

func (e *RuntimeToolExecutor) execSQLExecute(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	return e.inner.ExecSQLExecuteDirect(ctx, actor, input)
}

func (e *RuntimeToolExecutor) execNginxReload(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	return e.inner.ExecNginxReloadDirect(ctx, actor, input)
}

func (e *RuntimeToolExecutor) execSystemdControl(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	return e.inner.ExecSystemdControlDirect(ctx, actor, input)
}

func (e *RuntimeToolExecutor) execCertbotExecute(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	return e.inner.ExecCertbotExecuteDirect(ctx, actor, input)
}

func (e *RuntimeToolExecutor) execInstagramHandleCheck(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	return e.inner.ExecInstagramHandleCheckDirect(ctx, actor, input)
}

func (e *RuntimeToolExecutor) execEmailAPI(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	return e.inner.ExecEmailAPIDirect(ctx, actor, input)
}

func (e *RuntimeToolExecutor) decryptCredentialValue(ctx context.Context, value any) any {
	if e == nil || e.inner == nil {
		return value
	}
	return e.inner.DecryptCredentialValueForTest(ctx, value)
}

func (e *RuntimeToolExecutor) decryptCredentialMap(ctx context.Context, in map[string]any) map[string]any {
	if e == nil || e.inner == nil {
		return in
	}
	return e.inner.DecryptCredentialMapForTest(ctx, in)
}

func (e *RuntimeToolExecutor) loadVerticalCredentials(ctx context.Context, verticalID string) (map[string]any, error) {
	if e == nil || e.inner == nil {
		return nil, nil
	}
	return e.inner.LoadVerticalCredentialsForTest(ctx, verticalID)
}

func (e *RuntimeToolExecutor) loadExternalCredentials(ctx context.Context, verticalID, toolName string) (map[string]any, error) {
	if e == nil || e.inner == nil {
		return nil, nil
	}
	return e.inner.LoadExternalCredentialsForTest(ctx, verticalID, toolName)
}
