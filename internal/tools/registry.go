package tools

import (
	"context"
	"fmt"
	"sync"
)

type ToolFunc func(ctx context.Context, input any) (any, error)

type Registry struct {
	mu    sync.RWMutex
	tools map[string]ToolFunc
}

func NewRegistry() *Registry {
	return &Registry{tools: make(map[string]ToolFunc)}
}

func (r *Registry) Register(name string, fn ToolFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools[name] = fn
}

func (r *Registry) Get(name string) (ToolFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.tools[name]
	return fn, ok
}

func (r *Registry) Execute(ctx context.Context, name string, input any) (any, error) {
	fn, ok := r.Get(name)
	if !ok {
		return nil, fmt.Errorf("tool not found: %s", name)
	}
	return fn(ctx, input)
}
