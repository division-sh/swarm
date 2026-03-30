package tools

import (
	"context"
	"fmt"
	"strings"

	models "swarm/internal/runtime/core/actors"
)

type ToolHandler func(ctx context.Context, actor models.AgentConfig, input any) (any, error)
type EmitToolHandler func(ctx context.Context, actor models.AgentConfig, name string, input any) (any, error)
type HTTPToolHandler func(ctx context.Context, actor models.AgentConfig, tool RegisteredTool, input any) (any, error)
type MCPToolHandler func(ctx context.Context, actor models.AgentConfig, tool RegisteredTool, input any) (any, error)
type ToolResolver func(actor models.AgentConfig, name string) (RegisteredTool, bool, error)

type ToolDispatcher struct {
	emitHandler EmitToolHandler
	resolver    ToolResolver
	httpHandler HTTPToolHandler
	mcpHandler  MCPToolHandler
	handlers    map[string]ToolHandler
}

func NewToolDispatcher(emitHandler EmitToolHandler, resolver ToolResolver, httpHandler HTTPToolHandler, mcpHandler MCPToolHandler, handlers map[string]ToolHandler) *ToolDispatcher {
	copied := make(map[string]ToolHandler, len(handlers))
	for name, handler := range handlers {
		if strings.TrimSpace(name) == "" || handler == nil {
			continue
		}
		copied[strings.TrimSpace(name)] = handler
	}
	return &ToolDispatcher{
		emitHandler: emitHandler,
		resolver:    resolver,
		httpHandler: httpHandler,
		mcpHandler:  mcpHandler,
		handlers:    copied,
	}
}

func (d *ToolDispatcher) Dispatch(ctx context.Context, actor models.AgentConfig, name string, input any) (any, error) {
	if d == nil {
		return nil, fmt.Errorf("tool dispatcher is not configured")
	}
	name = normalizeNativeToolName(name)
	if strings.HasPrefix(name, "emit_") {
		if d.emitHandler == nil {
			return nil, fmt.Errorf("emit tool handler is not configured")
		}
		return d.emitHandler(ctx, actor, name, input)
	}
	if d.resolver == nil {
		return nil, fmt.Errorf("tool resolver is not configured")
	}
	tool, ok, err := d.resolver(actor, name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("unsupported runtime tool: %s", name)
	}
	switch tool.HandlerType {
	case implementationPlatformBuiltin:
		handler, ok := d.handlers[name]
		if !ok || handler == nil {
			return nil, fmt.Errorf("missing platform builtin handler: %s", name)
		}
		return handler(ctx, actor, input)
	case implementationHTTP:
		if d.httpHandler == nil {
			return nil, fmt.Errorf("http tool handler is not configured")
		}
		return d.httpHandler(ctx, actor, tool, input)
	case implementationMCP:
		if d.mcpHandler == nil {
			return nil, fmt.Errorf("mcp tool handler is not configured")
		}
		return d.mcpHandler(ctx, actor, tool, input)
	default:
		return nil, fmt.Errorf("unsupported tool handler type for %s: %s", name, tool.HandlerType)
	}
}
