package runtime

import (
	"context"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	"github.com/google/uuid"
)

type mcpTurnContext struct {
	Actor      models.AgentConfig
	Inbound    events.Event
	HasInbound bool
	Recorder   *EmittedEventsRecorder
	Epoch      int64
	CreatedAt  time.Time
	ExpiresAt  time.Time
}

type mcpTurnRegistry struct {
	mu   sync.RWMutex
	data map[string]mcpTurnContext
}

func newMCPTurnRegistry() *mcpTurnRegistry {
	return &mcpTurnRegistry{
		data: make(map[string]mcpTurnContext),
	}
}

var globalMCPTurnRegistry = newMCPTurnRegistry()
var defaultMCPTurnContextTTL = 2 * time.Hour

func registerMCPTurnContext(ctx context.Context) string {
	return registerMCPTurnContextWithTTL(ctx, defaultMCPTurnContextTTL)
}

func registerMCPTurnContextWithTTL(ctx context.Context, ttl time.Duration) string {
	actor, ok := ActorFromContext(ctx)
	if !ok || strings.TrimSpace(actor.ID) == "" {
		return ""
	}
	if ttl <= 0 {
		ttl = defaultMCPTurnContextTTL
	}
	now := time.Now().UTC()
	token := uuid.NewString()
	recorder, _ := EmittedEventsRecorderFromContext(ctx)
	inbound, hasInbound := InboundEventFromContext(ctx)
	epoch := CurrentRuntimeEpoch()
	if scoped, ok := RuntimeEpochFromContext(ctx); ok && scoped > 0 {
		epoch = scoped
	}
	globalMCPTurnRegistry.put(token, mcpTurnContext{
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

func resolveMCPTurnContext(token string) (mcpTurnContext, bool) {
	return globalMCPTurnRegistry.get(strings.TrimSpace(token))
}

func unregisterMCPTurnContext(token string) {
	globalMCPTurnRegistry.delete(strings.TrimSpace(token))
}

func resetMCPTurnContexts() {
	globalMCPTurnRegistry.reset()
}

func (r *mcpTurnRegistry) put(token string, data mcpTurnContext) {
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

func (r *mcpTurnRegistry) get(token string) (mcpTurnContext, bool) {
	if strings.TrimSpace(token) == "" {
		return mcpTurnContext{}, false
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
	r.data = make(map[string]mcpTurnContext)
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
