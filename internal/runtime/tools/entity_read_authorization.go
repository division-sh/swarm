package tools

import (
	"strings"

	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/entityruntime"
	"swarm/internal/runtime/semanticview"
)

func actorOwnedReadTargetContracts(source semanticview.Source, actor *models.AgentConfig) []entityruntime.Contract {
	contracts := entityruntime.ReadTargetContracts(source)
	if actor == nil {
		return contracts
	}
	actorID := strings.TrimSpace(actor.ID)
	if actorID == "" {
		return nil
	}
	out := make([]entityruntime.Contract, 0, len(contracts))
	for _, contract := range contracts {
		if entityReadContractOwnedByActor(source, actorID, contract) {
			out = append(out, contract)
		}
	}
	return out
}

func entityReadContractOwnedByActor(source semanticview.Source, actorID string, target entityruntime.Contract) bool {
	actor, ok := entityruntime.ResolveForActor(source, actorID)
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

func entityReadRowOwnedByActor(source semanticview.Source, actorID string, row map[string]any) bool {
	contract, ok := entityruntime.ResolveForEntityRow(source, row)
	if !ok {
		return false
	}
	return entityReadContractOwnedByActor(source, actorID, contract)
}

func enforceEntityReadOwnership(source semanticview.Source, actor models.AgentConfig, entityID string, row map[string]any, operation string) error {
	if entityReadRowOwnedByActor(source, actor.ID, row) {
		return nil
	}
	return NewRuntimeError(
		"cross_flow_read_forbidden",
		"tool-executor",
		operation,
		false,
		"actor %s cannot read entity %s owned by flow_instance %s",
		strings.TrimSpace(actor.ID),
		strings.TrimSpace(entityID),
		strings.TrimSpace(asString(row["flow_instance"])),
	)
}
