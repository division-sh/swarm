package mcp

import (
	"context"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
	models "empireai/internal/runtime/actors"
	runtimebus "empireai/internal/runtime/bus"
	"github.com/google/uuid"
)

type actorResolverFn func(context.Context) (models.AgentConfig, bool)

var actorResolver actorResolverFn

type TurnContext struct {
	Actor      models.AgentConfig
	Inbound    events.Event
	HasInbound bool
	Recorder   *runtimebus.EmittedEventsRecorder
	Epoch      int64
	CreatedAt  time.Time
	ExpiresAt  time.Time
}

type mcpTurnRegistry struct {
	mu   sync.RWMutex
	data map[string]TurnContext
}

func newMCPTurnRegistry() *mcpTurnRegistry {
	return &mcpTurnRegistry{
		data: make(map[string]TurnContext),
	}
}

var globalMCPTurnRegistry = newMCPTurnRegistry()
var defaultMCPTurnContextTTL = 2 * time.Hour

func init() {
	runtimebus.SetRuntimeResetHook(ResetTurnContexts)
}

func SetActorResolver(resolve actorResolverFn) {
	actorResolver = resolve
}

func RegisterTurnContext(ctx context.Context) string {
	return RegisterTurnContextWithTTL(ctx, defaultMCPTurnContextTTL)
}

func RegisterTurnContextWithTTL(ctx context.Context, ttl time.Duration) string {
	if actorResolver == nil {
		return ""
	}
	actor, ok := actorResolver(ctx)
	if !ok || strings.TrimSpace(actor.ID) == "" {
		return ""
	}
	if ttl <= 0 {
		ttl = defaultMCPTurnContextTTL
	}
	now := time.Now().UTC()
	token := uuid.NewString()
	recorder, _ := runtimebus.EmittedEventsRecorderFromContext(ctx)
	inbound, hasInbound := runtimebus.InboundEventFromContext(ctx)
	epoch := runtimebus.CurrentRuntimeEpoch()
	if scoped, ok := runtimebus.RuntimeEpochFromContext(ctx); ok && scoped > 0 {
		epoch = scoped
	}
	globalMCPTurnRegistry.put(token, TurnContext{
		Actor:      actor,
		Inbound:    inbound,
		HasInbound: hasInbound,
		Recorder:   recorder,
		Epoch:      epoch,
		CreatedAt:  now,
		ExpiresAt:  now.Add(ttl),
	})
	return token
}

func ResolveTurnContext(token string) (TurnContext, bool) {
	return globalMCPTurnRegistry.get(strings.TrimSpace(token))
}

func UnregisterTurnContext(token string) {
	globalMCPTurnRegistry.delete(strings.TrimSpace(token))
}

func ResetTurnContexts() {
	globalMCPTurnRegistry.reset()
}

func PutTurnContextForTest(token string, data TurnContext) {
	globalMCPTurnRegistry.put(strings.TrimSpace(token), data)
}

func PruneTurnContextsBefore(now time.Time) {
	globalMCPTurnRegistry.mu.Lock()
	defer globalMCPTurnRegistry.mu.Unlock()
	globalMCPTurnRegistry.pruneLocked(now)
}

func (r *mcpTurnRegistry) put(token string, data TurnContext) {
	if strings.TrimSpace(token) == "" {
		return
	}
	now := time.Now().UTC()
	if data.CreatedAt.IsZero() {
		data.CreatedAt = now
	}
	if data.ExpiresAt.IsZero() {
		data.ExpiresAt = data.CreatedAt.Add(defaultMCPTurnContextTTL)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(now)
	r.data[token] = data
}

func (r *mcpTurnRegistry) get(token string) (TurnContext, bool) {
	if strings.TrimSpace(token) == "" {
		return TurnContext{}, false
	}
	now := time.Now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(now)
	v, ok := r.data[token]
	return v, ok
}

func (r *mcpTurnRegistry) delete(token string) {
	if strings.TrimSpace(token) == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.data, token)
}

func (r *mcpTurnRegistry) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data = make(map[string]TurnContext)
}

func (r *mcpTurnRegistry) pruneLocked(now time.Time) {
	for k, v := range r.data {
		if !v.ExpiresAt.IsZero() {
			if !v.ExpiresAt.After(now) {
				delete(r.data, k)
			}
			continue
		}
		if v.CreatedAt.Before(now.Add(-defaultMCPTurnContextTTL)) {
			delete(r.data, k)
		}
	}
}
