package tools

import (
	"context"

	models "empireai/internal/runtime/actors"
)

func (e *Executor) buildToolHandlers() map[string]ToolHandler {
	handlers := map[string]ToolHandler{}
	e.registerAgentHandlers(handlers)
	e.registerMailboxHandlers(handlers)
	e.registerHumanTaskHandlers(handlers)
	e.registerInfraHandlers(handlers)
	e.registerExternalHandlers(handlers)
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

func (e *Executor) registerHumanTaskHandlers(handlers map[string]ToolHandler) {
	handlers["human_task_request"] = e.execHumanTaskRequest
	handlers["human_task_decide"] = e.execHumanTaskDecide
}

func (e *Executor) registerInfraHandlers(handlers map[string]ToolHandler) {
	handlers["nginx_reload"] = e.execNginxReload
	handlers["systemd_control"] = e.execSystemdControl
	handlers["certbot_execute"] = e.execCertbotExecute
}

func (e *Executor) registerExternalHandlers(handlers map[string]ToolHandler) {
	handlers["whatsapp_business_api"] = func(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
		return e.execExternalProxy(ctx, actor, "whatsapp_business_api", input)
	}
	handlers["email_api"] = e.execEmailAPI
	handlers["instagram_api"] = func(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
		return e.execExternalProxy(ctx, actor, "instagram_api", input)
	}
	handlers["domain_purchase"] = func(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
		return e.execExternalProxy(ctx, actor, "domain_purchase", input)
	}
	handlers["domain_availability_check"] = func(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
		return e.execExternalProxy(ctx, actor, "domain_availability_check", input)
	}
	handlers["dns_configure"] = func(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
		return e.execExternalProxy(ctx, actor, "dns_configure", input)
	}
	handlers["instagram_handle_check"] = e.execInstagramHandleCheck
	handlers["whatsapp_name_check"] = func(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
		return e.execExternalProxy(ctx, actor, "whatsapp_name_check", input)
	}
}
