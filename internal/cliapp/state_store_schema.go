package cliapp

import (
	"fmt"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store"
)

type StateStoreSchemaPlanSet struct {
	Platform []store.SchemaTableDDL
	State    []store.SchemaTableDDL
}

func (p StateStoreSchemaPlanSet) All() []store.SchemaTableDDL {
	plans := append([]store.SchemaTableDDL{}, p.Platform...)
	return append(plans, p.State...)
}

func StateStoreSchemaPlans(bundle *runtimecontracts.WorkflowContractBundle) (StateStoreSchemaPlanSet, error) {
	if bundle == nil {
		return StateStoreSchemaPlanSet{}, fmt.Errorf("workflow contract bundle is required")
	}
	platformPlans, err := store.GeneratePlatformTableDDLs(bundle.Platform)
	if err != nil {
		return StateStoreSchemaPlanSet{}, fmt.Errorf("Platform-owned tables: %w", err)
	}
	statePlans, err := store.GenerateNodeStateTableDDLs(bundle.NodeEntries())
	if err != nil {
		return StateStoreSchemaPlanSet{}, fmt.Errorf("state_schema tables: %w", err)
	}
	return StateStoreSchemaPlanSet{Platform: platformPlans, State: statePlans}, nil
}
