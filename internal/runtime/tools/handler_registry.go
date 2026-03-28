package tools

import (
	"context"

	models "swarm/internal/runtime/core/actors"
)

func (e *Executor) buildToolHandlers() map[string]ToolHandler {
	handlers := map[string]ToolHandler{}
	e.registerAgentHandlers(handlers)
	e.registerFlowHandlers(handlers)
	e.registerEntityHandlers(handlers)
	e.registerMailboxHandlers(handlers)
	e.registerHumanTaskHandlers(handlers)
	e.registerNativeToolHandlers(handlers)
	return handlers
}

func (e *Executor) registerAgentHandlers(handlers map[string]ToolHandler) {
	handlers["agent_message"] = e.execAgentMessage
	handlers["schedule"] = e.execSchedule
	handlers["configure_routing"] = e.execConfigureRouting
	handlers["agent_hire"] = func(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
		return e.execAgentHire(actor, input)
	}
	handlers["agent_fire"] = func(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
		return e.execAgentFire(actor, input)
	}
	handlers["agent_reconfigure"] = func(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
		return e.execAgentReconfigure(actor, input)
	}
}

func (e *Executor) registerMailboxHandlers(handlers map[string]ToolHandler) {
	handlers["mailbox_send"] = e.execMailboxSend
}

func (e *Executor) registerEntityHandlers(handlers map[string]ToolHandler) {
	handlers["get_entity"] = e.execGetEntity
	handlers["save_entity_field"] = e.execSaveEntityField
	handlers["create_entity"] = e.execCreateEntity
	handlers["query_entities"] = e.execQueryEntities
	handlers["search_entities"] = e.execSearchEntities
	handlers["query_metrics"] = e.execQueryMetrics
}

func (e *Executor) registerFlowHandlers(handlers map[string]ToolHandler) {
	handlers["create_flow_instance"] = e.execCreateFlowInstance
}

func (e *Executor) registerHumanTaskHandlers(handlers map[string]ToolHandler) {
	handlers["human_task_request"] = e.execHumanTaskRequest
	handlers["human_task_decide"] = e.execHumanTaskDecide
}

func (e *Executor) registerNativeToolHandlers(handlers map[string]ToolHandler) {
	handlers["bash"] = e.execNativeBash
	handlers["web_search"] = e.execNativeWebSearch
	handlers["read_file"] = e.execNativeReadFile
	handlers["write_file"] = e.execNativeWriteFile
}
