package cliapp

import (
	"fmt"
	"strings"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/versionmetadata"
)

type StateStoreSchemaPlanSet struct {
	Platform []store.SchemaTableDDL
	State    []store.SchemaTableDDL
}

// SchemaBootstrapRequest describes the same boot schema without preparing it.
func SchemaBootstrapRequest(spec runtimecontracts.PlatformSpecDocument, platformPlans, statePlans []store.SchemaTableDDL) (store.SchemaBootstrapRequest, error) {
	metadata, err := versionmetadata.Resolve(InjectedBuildMetadata())
	if err != nil {
		return store.SchemaBootstrapRequest{}, fmt.Errorf("resolve schema bootstrap build identity: %w", err)
	}
	return store.SchemaBootstrapRequest{
		PlatformPlans: platformPlans, StatePlans: statePlans,
		Origin: store.RuntimeStoreOrigin{
			SwarmVersion: metadata.BinaryVersion, PlatformVersion: strings.TrimSpace(spec.Platform.Version), CreatedAt: time.Now().UTC(),
		},
	}, nil
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
	statePlans, err := store.GenerateNodeStateTableDDLs(bundle.ScopedNodeRecords())
	if err != nil {
		return StateStoreSchemaPlanSet{}, fmt.Errorf("state_schema tables: %w", err)
	}
	return StateStoreSchemaPlanSet{Platform: platformPlans, State: statePlans}, nil
}
