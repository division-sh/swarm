package tools

import (
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func actorOwnedReadTargetContracts(source semanticview.Source, actor *models.AgentConfig) []entityruntime.Contract {
	contracts := entityruntime.ReadTargetContracts(source)
	if actor == nil {
		return contracts
	}
	if strings.TrimSpace(actor.ID) == "" {
		return nil
	}
	out := make([]entityruntime.Contract, 0, len(contracts))
	for _, contract := range contracts {
		if entityReadContractOwnedByActor(source, *actor, contract) {
			out = append(out, contract)
		}
	}
	return out
}

func entityReadContractOwnedByActor(source semanticview.Source, actorConfig models.AgentConfig, target entityruntime.Contract) bool {
	actor, ok := entityruntime.ResolveForActor(source, actorConfig)
	if !ok {
		return false
	}
	actorRoot := entityReadFlowScopeRoot(source, actor)
	targetRoot := entityReadFlowScopeRoot(source, target)
	if actorRoot == "" {
		return targetRoot == "" && strings.EqualFold(actor.EntityType, target.EntityType)
	}
	if targetRoot == "" {
		return false
	}
	return entityFlowOwnedBy(actorRoot, targetRoot)
}

func entityReadRowOwnedByActor(source semanticview.Source, actor models.AgentConfig, row map[string]any) bool {
	contract, ok := entityruntime.ResolveForEntityRow(source, row)
	if !ok {
		return false
	}
	return entityReadContractOwnedByActor(source, actor, contract)
}

func filterEntityReadRowsForActor(source semanticview.Source, actor models.AgentConfig, rows []map[string]any) []map[string]any {
	if len(rows) == 0 {
		return rows
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		if entityReadRowOwnedByActor(source, actor, row) {
			out = append(out, row)
		}
	}
	return out
}

func enforceEntityReadOwnership(source semanticview.Source, actor models.AgentConfig, entityID string, row map[string]any, operation string) error {
	if entityReadRowOwnedByActor(source, actor, row) {
		return nil
	}
	return failures.NewDetail(
		"cross_flow_read_forbidden",
		"tool-executor",
		operation,
		map[string]any{
			"action":          "entity_read",
			"actor_id":        strings.TrimSpace(actor.ID),
			"entity_id":       strings.TrimSpace(entityID),
			"owner_flow_path": strings.TrimSpace(asString(row["flow_instance"])),
		},
	)
}
