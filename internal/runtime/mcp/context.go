package mcp

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

type actorResolverFn func(context.Context) (models.AgentConfig, bool)

type TurnContext struct {
	Actor             models.AgentConfig
	Inbound           events.Event
	HasInbound        bool
	RuntimeLineage    runtimecorrelation.RuntimeLineage
	HasRuntimeLineage bool
	Allowed           map[string]struct{}
	Recorder          *runtimebus.EmittedEventsRecorder
	Emitted           map[string]struct{}
	CreatedAt         time.Time
	ExpiresAt         time.Time
}

type TurnContextRegistry struct {
	mu   sync.RWMutex
	data map[string]TurnContext

	actorResolver actorResolverFn
	defaultTTL    time.Duration
}

func NewTurnContextRegistry(resolve actorResolverFn) *TurnContextRegistry {
	return &TurnContextRegistry{
		data:          make(map[string]TurnContext),
		actorResolver: resolve,
		defaultTTL:    2 * time.Hour,
	}
}

func (r *TurnContextRegistry) RegisterTurnContext(ctx context.Context) string {
	if r == nil {
		return ""
	}
	return r.RegisterTurnContextWithTTL(ctx, r.defaultTTL)
}

func (r *TurnContextRegistry) RegisterTurnContextWithTTL(ctx context.Context, ttl time.Duration) string {
	return r.RegisterTurnContextWithAllowedTools(ctx, ttl, nil)
}

func (r *TurnContextRegistry) RegisterTurnContextWithAllowedTools(ctx context.Context, ttl time.Duration, allowedTools []string) string {
	if r == nil || r.actorResolver == nil {
		return ""
	}
	actor, ok := r.actorResolver(ctx)
	if !ok || strings.TrimSpace(actor.ID) == "" {
		return ""
	}
	if ttl <= 0 {
		ttl = r.defaultTTL
	}
	now := time.Now().UTC()
	token := uuid.NewString()
	recorder, _ := runtimebus.EmittedEventsRecorderFromContext(ctx)
	inbound, hasInbound := runtimebus.InboundEventFromContext(ctx)
	lineage, hasLineage := runtimecorrelation.RuntimeLineageFromContext(ctx)
	r.put(token, TurnContext{
		Actor:             actor,
		Inbound:           inbound,
		HasInbound:        hasInbound,
		RuntimeLineage:    lineage,
		HasRuntimeLineage: hasLineage,
		Allowed:           normalizeAllowedTools(allowedTools),
		Recorder:          recorder,
		CreatedAt:         now,
		ExpiresAt:         now.Add(ttl),
	})
	return token
}

func (r *TurnContextRegistry) ResolveTurnContext(token string) (TurnContext, bool) {
	if r == nil {
		return TurnContext{}, false
	}
	return r.get(strings.TrimSpace(token))
}

func (r *TurnContextRegistry) MarkEmitUsed(token string) bool {
	if r == nil {
		return false
	}
	return r.markEmitKeyUsed(strings.TrimSpace(token), "__default__")
}

func (r *TurnContextRegistry) MarkEmitKeyUsed(token, key string) bool {
	if r == nil {
		return false
	}
	return r.markEmitKeyUsed(strings.TrimSpace(token), strings.TrimSpace(key))
}

func (r *TurnContextRegistry) UnregisterTurnContext(token string) {
	if r == nil {
		return
	}
	r.delete(strings.TrimSpace(token))
}

func (r *TurnContextRegistry) Reset() {
	if r == nil {
		return
	}
	r.reset()
}

func (r *TurnContextRegistry) PutTurnContextForTest(token string, data TurnContext) {
	if r == nil {
		return
	}
	r.put(strings.TrimSpace(token), data)
}

func (r *TurnContextRegistry) PruneTurnContextsBefore(now time.Time) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(now)
}

func (r *TurnContextRegistry) put(token string, data TurnContext) {
	if strings.TrimSpace(token) == "" {
		return
	}
	now := time.Now().UTC()
	if data.CreatedAt.IsZero() {
		data.CreatedAt = now
	}
	if data.ExpiresAt.IsZero() {
		data.ExpiresAt = data.CreatedAt.Add(r.defaultTTL)
	}
	data.Allowed = copyAllowedTools(data.Allowed)
	if data.Emitted != nil {
		cloned := make(map[string]struct{}, len(data.Emitted))
		for key := range data.Emitted {
			cloned[key] = struct{}{}
		}
		data.Emitted = cloned
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(now)
	r.data[token] = data
}

func (r *TurnContextRegistry) get(token string) (TurnContext, bool) {
	if strings.TrimSpace(token) == "" {
		return TurnContext{}, false
	}
	now := time.Now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(now)
	v, ok := r.data[token]
	v.Allowed = copyAllowedTools(v.Allowed)
	if v.Emitted != nil {
		cloned := make(map[string]struct{}, len(v.Emitted))
		for key := range v.Emitted {
			cloned[key] = struct{}{}
		}
		v.Emitted = cloned
	}
	return v, ok
}

func (r *TurnContextRegistry) markEmitKeyUsed(token, key string) bool {
	if strings.TrimSpace(token) == "" {
		return false
	}
	if strings.TrimSpace(key) == "" {
		key = "__default__"
	}
	now := time.Now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(now)
	v, ok := r.data[token]
	if !ok {
		return false
	}
	if v.Emitted == nil {
		v.Emitted = map[string]struct{}{}
	}
	if _, ok := v.Emitted[key]; ok {
		return true
	}
	v.Emitted[key] = struct{}{}
	r.data[token] = v
	return false
}

func (r *TurnContextRegistry) delete(token string) {
	if strings.TrimSpace(token) == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.data, token)
}

func (r *TurnContextRegistry) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data = make(map[string]TurnContext)
}

func (r *TurnContextRegistry) pruneLocked(now time.Time) {
	for k, v := range r.data {
		if !v.ExpiresAt.IsZero() {
			if !v.ExpiresAt.After(now) {
				delete(r.data, k)
			}
			continue
		}
		if v.CreatedAt.Before(now.Add(-r.defaultTTL)) {
			delete(r.data, k)
		}
	}
}

func normalizeAllowedTools(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(values))
	for _, raw := range values {
		name := toolidentity.CanonicalName(raw)
		if name == "" {
			continue
		}
		out[name] = struct{}{}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func copyAllowedTools(values map[string]struct{}) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(values))
	for key := range values {
		out[key] = struct{}{}
	}
	return out
}
