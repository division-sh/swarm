package server

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store"
)

type SQLAgentReader struct {
	source *dashboardAgentReadSource
	owner  *store.OperatorAgentConversationReadSurface
}

func NewSQLAgentReader(db *sql.DB, source any, turnLimit int) *SQLAgentReader {
	adapter := &dashboardAgentReadSource{source: source}
	owner := store.NewOperatorAgentConversationReadSurface(db, adapter, turnLimit)
	if owner == nil {
		return nil
	}
	return &SQLAgentReader{source: adapter, owner: owner}
}

func (r *SQLAgentReader) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	if r == nil || r.source == nil {
		return nil, nil
	}
	return r.source.LoadAgents(ctx)
}

func (r *SQLAgentReader) ListOperatorAgents(ctx context.Context, opts store.OperatorAgentListOptions) (store.OperatorAgentListResult, error) {
	if r == nil || r.owner == nil {
		return store.OperatorAgentListResult{}, nil
	}
	return r.owner.ListOperatorAgents(ctx, opts)
}

func (r *SQLAgentReader) LoadOperatorAgent(ctx context.Context, agentID string) (store.OperatorAgentDetail, error) {
	if r == nil || r.owner == nil {
		return store.OperatorAgentDetail{}, store.ErrAgentNotFound
	}
	return r.owner.LoadOperatorAgent(ctx, agentID)
}

func (r *SQLAgentReader) ListGenericAgents(ctx context.Context) ([]genericAgent, error) {
	result, err := r.ListOperatorAgents(ctx, store.OperatorAgentListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]genericAgent, 0, len(result.Agents))
	for _, item := range result.Agents {
		out = append(out, genericAgentFromOperatorSummary(item))
	}
	return out, nil
}

func (r *SQLAgentReader) GetGenericAgent(ctx context.Context, id string) (genericAgent, bool, error) {
	item, err := r.LoadOperatorAgent(ctx, strings.TrimSpace(id))
	if errors.Is(err, store.ErrAgentNotFound) {
		return genericAgent{}, false, nil
	}
	if err != nil {
		return genericAgent{}, false, err
	}
	return genericAgentFromOperatorSummary(item.Agent), true, nil
}

type dashboardAgentReadSource struct {
	source any
}

type dashboardLoadAgentsSource interface {
	LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error)
}

type pendingAgentDeliveryFactSource interface {
	ListPendingAgentDeliveryFacts(ctx context.Context, agentIDs []string, since time.Time) (map[string]store.PendingAgentDeliveryFacts, error)
}

type pendingAgentDeliveryDetailSource interface {
	ListPendingAgentDeliveryDetails(ctx context.Context, opts store.PendingAgentDeliveryListOptions) (store.PendingAgentDeliveryPage, error)
}

type agentLifecycleFactSource interface {
	ListAgentLifecycleFacts(ctx context.Context, agentIDs []string) (map[string]store.AgentLifecycleFacts, error)
}

func (s *dashboardAgentReadSource) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	source, ok := s.source.(dashboardLoadAgentsSource)
	if !ok || source == nil {
		return nil, nil
	}
	return source.LoadAgents(ctx)
}

func (s *dashboardAgentReadSource) ResolveSchemaCapabilities(ctx context.Context) (store.StoreSchemaCapabilities, error) {
	source, ok := s.source.(schemaCapabilitySource)
	if !ok || source == nil {
		return store.StoreSchemaCapabilities{}, missingDashboardCapabilityOwner("agent reader")
	}
	return source.ResolveSchemaCapabilities(ctx)
}

func (s *dashboardAgentReadSource) ListPendingAgentDeliveryFacts(ctx context.Context, agentIDs []string, since time.Time) (map[string]store.PendingAgentDeliveryFacts, error) {
	source, ok := s.source.(pendingAgentDeliveryFactSource)
	if !ok || source == nil {
		return nil, errors.New("missing pending agent delivery fact source")
	}
	return source.ListPendingAgentDeliveryFacts(ctx, agentIDs, since)
}

func (s *dashboardAgentReadSource) ListPendingAgentDeliveryDetails(ctx context.Context, opts store.PendingAgentDeliveryListOptions) (store.PendingAgentDeliveryPage, error) {
	source, ok := s.source.(pendingAgentDeliveryDetailSource)
	if !ok || source == nil {
		return store.PendingAgentDeliveryPage{}, errors.New("missing pending agent delivery detail source")
	}
	return source.ListPendingAgentDeliveryDetails(ctx, opts)
}

func (s *dashboardAgentReadSource) ListAgentLifecycleFacts(ctx context.Context, agentIDs []string) (map[string]store.AgentLifecycleFacts, error) {
	source, ok := s.source.(agentLifecycleFactSource)
	if !ok || source == nil {
		return nil, errors.New("missing agent lifecycle fact source")
	}
	return source.ListAgentLifecycleFacts(ctx, agentIDs)
}
