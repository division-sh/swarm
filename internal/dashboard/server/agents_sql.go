package server

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store"
)

type SQLAgentReader struct {
	owner     dashboardAgentReadOwner
	turnLimit int
}

type dashboardAgentReadOwner interface {
	LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error)
	ListOperatorAgents(context.Context, store.OperatorAgentListOptions) (store.OperatorAgentListResult, error)
}

func NewSQLAgentReader(db *sql.DB, source any, turnLimit int) *SQLAgentReader {
	owner, ok := source.(dashboardAgentReadOwner)
	if db == nil || !ok || owner == nil {
		return nil
	}
	if turnLimit < 0 {
		turnLimit = 0
	}
	return &SQLAgentReader{owner: owner, turnLimit: turnLimit}
}

func (r *SQLAgentReader) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	if r == nil || r.owner == nil {
		return nil, nil
	}
	return r.owner.LoadAgents(ctx)
}

func (r *SQLAgentReader) ListOperatorAgents(ctx context.Context, opts store.OperatorAgentListOptions) (store.OperatorAgentListResult, error) {
	if r == nil || r.owner == nil {
		return store.OperatorAgentListResult{}, nil
	}
	opts.TurnLimit = r.turnLimit
	return r.owner.ListOperatorAgents(ctx, opts)
}

func (r *SQLAgentReader) LoadOperatorAgent(ctx context.Context, agentID string) (store.OperatorAgentDetail, error) {
	if r == nil || r.owner == nil {
		return store.OperatorAgentDetail{}, store.ErrAgentNotFound
	}
	agentID = strings.TrimSpace(agentID)
	result, err := r.ListOperatorAgents(ctx, store.OperatorAgentListOptions{})
	if err != nil {
		return store.OperatorAgentDetail{}, err
	}
	for _, agent := range result.Agents {
		if strings.TrimSpace(agent.AgentID) == agentID {
			return store.OperatorAgentDetail{
				Agent:             agent,
				CurrentSessionRef: agent.CurrentSessionRef,
				LastTurnRef:       agent.LastTurnRef,
			}, nil
		}
	}
	return store.OperatorAgentDetail{}, store.ErrAgentNotFound
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
