package runforkadmission

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestCapturedHistoricalConstructionHasOneSharedAdmission(t *testing.T) {
	source := testContractFrontierTemplateSource(t)
	original := withConstructedHeader(t, testRunForkPlan("review/task.started", runfork.RunForkPendingClassificationPending, "node", "source-node"), source, "review", "inst-1")
	base := original.Entities[0]
	receipt, err := pipeline.DecodeStoredFlowConstructionReceipt(base.MaterializationMetadata.InitialMaterialization, original.SourceRunID, base.EntityID, base.MaterializationMetadata.FlowInstance, "review")
	if err != nil {
		t.Fatal(err)
	}
	projected := projectedCapturedConstructorForTest(t, original.SourceRunID, eventtest.UUID("captured-intermediate"), 7, base, receipt.Identity)
	plan := original
	plan.SourceRunID, plan.Entities = eventtest.UUID("captured-intermediate"), []runfork.RunForkEntityState{projected}
	identity, _, found, err := FixedConstructionEvidenceForRoute(plan, receipt.Identity.Route())
	if err != nil || !found {
		t.Fatalf("historical constructor: found=%t err=%v", found, err)
	}
	expected, err := runfork.ProjectCapturedConstructionIdentity(original.SourceRunID, plan.SourceRunID, receipt.Identity)
	if err != nil {
		t.Fatal(err)
	}
	if len(projected.MaterializationMetadata.InitialMaterialization) != 0 || identity != expected ||
		projected.MaterializationMetadata.HistoricalConstruction.SourceRunID != original.SourceRunID {
		t.Fatal("history became native or lost its immediate cut")
	}
	evidence, err := fixedConstructionEvidence(plan, projected)
	if err != nil || evidence.Original.Identity != receipt.Identity || evidence.Original.RunID != original.SourceRunID || len(evidence.Lineage) != 1 || evidence.Lineage[0].SourceRevision != 7 {
		t.Fatalf("original native evidence was relabeled or lost: %+v %v", evidence, err)
	}
	if instances, err := ConstructedInstances(source, plan); err != nil || len(instances) != 1 || instances[0] != identity {
		t.Fatalf("shared constructed census: %+v %v", instances, err)
	}
	for _, cut := range []string{"missing_lineage", "missing_revision", "source_run", "source_key", "parent", "mode", "type", "imported_state", "native_conflict", "cycle"} {
		t.Run(cut, func(t *testing.T) {
			mutant := plan
			mutant.Entities = nil
			raw, err := json.Marshal(plan.Entities)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &mutant.Entities); err != nil {
				t.Fatalf("clone captured proof: %v", err)
			}
			meta := mutant.Entities[0].MaterializationMetadata
			switch cut {
			case "missing_lineage":
				meta.HistoricalConstruction = nil
			case "missing_revision":
				meta.HistoricalConstruction.SourceRevision = 0
			case "source_run":
				meta.HistoricalConstruction.SourceRunID = eventtest.UUID("foreign-captured-source")
			case "source_key":
				meta.HistoricalConstruction.SourceEntity.MaterializationMetadata.InstanceKey = "foreign-key"
			case "parent":
				meta.ParentInstance = "foreign-parent"
			case "mode":
				meta.Mode = "static"
			case "type":
				meta.EntityType = "foreign-type"
			case "imported_state":
				meta.HistoricalConstruction.SourceEntity.MaterializationMetadata.Source = runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState
			case "native_conflict":
				conflict, err := pipeline.ProjectFlowConstructionReceipt(receipt, flowidentity.RunScopedFlowInstance{RunID: mutant.SourceRunID, Route: identity.Route()}, identity, receipt.CreatingInput, nil)
				if err != nil {
					t.Fatal(err)
				}
				meta.InitialMaterialization, err = pipeline.EncodeFlowConstructionReceipt(conflict)
				if err != nil {
					t.Fatal(err)
				}
			case "cycle":
				meta.HistoricalConstruction.SourceRunID = mutant.SourceRunID
			}
			before, _ := json.Marshal(mutant)
			if _, _, found, err := FixedConstructionEvidenceForRoute(mutant, identity.Route()); err == nil || found {
				t.Fatalf("%s became constructor evidence: found=%t err=%v", cut, found, err)
			}
			after, _ := json.Marshal(mutant)
			if !bytes.Equal(before, after) {
				t.Fatal("admission rewrote captured evidence")
			}
		})
	}
	currentRun, currentEntity, currentIdentity := original.SourceRunID, base, receipt.Identity
	for i := 0; i < 64; i++ {
		nextRun := eventtest.UUID(fmt.Sprintf("captured-hop-%d", i))
		currentEntity = projectedCapturedConstructorForTest(t, currentRun, nextRun, int64(i+1), currentEntity, currentIdentity)
		currentIdentity, err = runfork.ProjectCapturedConstructionIdentity(currentRun, nextRun, currentIdentity)
		if err != nil {
			t.Fatal(err)
		}
		currentRun = nextRun
	}
	plan.SourceRunID, plan.Entities = currentRun, []runfork.RunForkEntityState{currentEntity}
	if identity, _, found, err := FixedConstructionEvidenceForRoute(plan, currentIdentity.Route()); err != nil || !found || identity != currentIdentity {
		t.Fatalf("deep lineage lost its exact constructor: found=%t err=%v", found, err)
	}
	evidence, err = fixedConstructionEvidence(plan, currentEntity)
	if err != nil || evidence.Original.RunID != original.SourceRunID || evidence.Original.Identity != receipt.Identity || len(evidence.Lineage) != 64 || evidence.Lineage[0].SourceRunID != original.SourceRunID || evidence.Lineage[63].RunID != plan.SourceRunID {
		t.Fatalf("deep lineage replaced original provenance: %+v %v", evidence, err)
	}
	evidence.Original.Persisted.Control.EntityType = "changed-detached-observation"
	fresh, err := fixedConstructionEvidence(plan, currentEntity)
	if err != nil || fresh.Original.Persisted.Control.EntityType != receipt.Persisted.Control.EntityType {
		t.Fatalf("returned original evidence aliases captured storage: %+v %v", fresh, err)
	}
}

func projectedCapturedConstructorForTest(t testing.TB, sourceRunID, targetRunID string, revision int64, source runfork.RunForkEntityState, identity flowidentity.Instance) runfork.RunForkEntityState {
	t.Helper()
	projected, err := runfork.ProjectCapturedConstructionIdentity(sourceRunID, targetRunID, identity)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := pipeline.WorkflowInstanceHeaderPayloadForIdentity(projected, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	config, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	metadata := *source.MaterializationMetadata
	metadata.InitialMaterialization = nil
	metadata.FlowConfig, metadata.FlowInstance, metadata.ParentInstance = config, projected.InstancePath, projected.ParentRoute.FlowInstance
	metadata.HistoricalConstruction = &runfork.RunForkCapturedHistoricalConstruction{SourceRunID: sourceRunID, SourceRevision: revision, SourceEntity: source}
	result := source
	result.EntityID, result.MaterializationMetadata = projected.EntityID, &metadata
	return result
}
