package tools

import (
	"context"
	"fmt"
	"strings"

	models "swarm/internal/runtime/core/actors"
)

type ToolHandler func(ctx context.Context, actor models.AgentConfig, input any) (any, error)
type EmitToolHandler func(ctx context.Context, actor models.AgentConfig, name string, input any) (any, error)

type ToolDispatcher struct {
	emitHandler EmitToolHandler
	handlers    map[string]ToolHandler
}

func NewToolDispatcher(emitHandler EmitToolHandler, handlers map[string]ToolHandler) *ToolDispatcher {
	copied := make(map[string]ToolHandler, len(handlers))
	for name, handler := range handlers {
		if strings.TrimSpace(name) == "" || handler == nil {
			continue
		}
		copied[strings.TrimSpace(name)] = handler
	}
	return &ToolDispatcher{
		emitHandler: emitHandler,
		handlers:    copied,
	}
}

func (d *ToolDispatcher) Dispatch(ctx context.Context, actor models.AgentConfig, name string, input any) (any, error) {
	if d == nil {
		return nil, fmt.Errorf("tool dispatcher is not configured")
	}
	name = strings.TrimSpace(name)
	if strings.HasPrefix(name, "emit_") {
		if d.emitHandler == nil {
			return nil, fmt.Errorf("emit tool handler is not configured")
		}
		return d.emitHandler(ctx, actor, name, input)
	}
	handler, ok := d.handlers[name]
	if !ok || handler == nil {
		return nil, fmt.Errorf("unsupported runtime tool: %s", name)
	}
	return handler(ctx, actor, input)
}
