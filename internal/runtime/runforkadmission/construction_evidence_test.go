package runforkadmission

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestFixedConstructionEvidenceRetainsCapturedIdentityWithoutCurrentSource(t *testing.T) {
	source := testContractFrontierTemplateSource(t)
	plan := withConstructedHeader(t, testRunForkPlan("review/task.started", runfork.RunForkPendingClassificationPending, "node", "source-node"), source, "review", "inst-1")
	entity := plan.Entities[0]
	meta := entity.MaterializationMetadata
	receipt, err := pipeline.DecodeStoredFlowConstructionReceipt(meta.InitialMaterialization, plan.SourceRunID, entity.EntityID, meta.FlowInstance, meta.FlowTemplate)
	if err != nil {
		t.Fatal(err)
	}
	before, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	got, selected, found, err := FixedConstructionEvidenceForRoute(plan, receipt.Identity.Route())
	if err != nil || !found || got != receipt.Identity || !reflect.DeepEqual(selected, entity) {
		t.Fatalf("captured evidence was reconstructed or lost: %+v found=%t err=%v", got, found, err)
	}
	changedSource := testContractFrontierSource("different-current-node")
	if _, _, found, err := FixedConstructionForRoute(changedSource, plan, receipt.Identity.Route()); err == nil || found {
		t.Fatalf("evidence bypassed current executable declaration: found=%t err=%v", found, err)
	}
	if after, err := json.Marshal(plan); err != nil || !bytes.Equal(before, after) {
		t.Fatalf("evidence or executable refusal mutated its fixed cut: %v", err)
	}
}

func TestFixedConstructionEvidenceRefusesMissingAndCrossedCapturedCoordinates(t *testing.T) {
	source := testContractFrontierTemplateSource(t)
	plan := withConstructedHeader(t, testRunForkPlan("review/task.started", runfork.RunForkPendingClassificationPending, "node", "source-node"), source, "review", "inst-1")
	entity := plan.Entities[0]
	meta := entity.MaterializationMetadata
	receipt, err := pipeline.DecodeStoredFlowConstructionReceipt(meta.InitialMaterialization, plan.SourceRunID, entity.EntityID, meta.FlowInstance, meta.FlowTemplate)
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []struct {
		name    string
		mutate  func(*runfork.RunForkPlan)
		capture func(*pipeline.FlowConstructionReceipt)
	}{
		{name: "missing_receipt", mutate: func(p *runfork.RunForkPlan) { p.Entities[0].MaterializationMetadata.InitialMaterialization = nil }},
		{name: "malformed_receipt", mutate: func(p *runfork.RunForkPlan) {
			p.Entities[0].MaterializationMetadata.InitialMaterialization = []byte(`{`)
		}},
		{name: "foreign_recorded_run", mutate: func(p *runfork.RunForkPlan) { p.SourceRunID = eventtest.UUID("foreign-fixed-run") }},
		{name: "foreign_recorded_entity", mutate: func(p *runfork.RunForkPlan) { p.Entities[0].EntityID = eventtest.UUID("foreign-fixed-entity") }},
		{name: "foreign_recorded_template", mutate: func(p *runfork.RunForkPlan) { p.Entities[0].MaterializationMetadata.FlowTemplate = "unrelated" }},
		{name: "foreign_parent", mutate: func(p *runfork.RunForkPlan) { p.Entities[0].MaterializationMetadata.ParentInstance = "foreign-parent" }},
		{name: "foreign_key", mutate: func(p *runfork.RunForkPlan) { p.Entities[0].MaterializationMetadata.InstanceKey = "foreign-key" }},
		{name: "foreign_config_parent", mutate: func(p *runfork.RunForkPlan) {
			p.Entities[0].MaterializationMetadata.FlowConfig = []byte(`{"instance_id":"inst-1","storage_ref":"review/inst-1","flow_path":"review/inst-1","parent_flow_id":"foreign","parent_flow_instance":"foreign/parent","parent_entity_id":"foreign"}`)
		}},
		{name: "crossed_captured_path", capture: func(r *pipeline.FlowConstructionReceipt) { r.Identity.InstancePath = "review/foreign" }},
		{name: "crossed_captured_parent", capture: func(r *pipeline.FlowConstructionReceipt) {
			r.Identity.ParentEntityID = eventtest.UUID("foreign-captured-parent")
		}},
		{name: "crossed_captured_key", capture: func(r *pipeline.FlowConstructionReceipt) { r.InstanceKey = "foreign-captured-key" }},
		{name: "duplicate_entity", mutate: func(p *runfork.RunForkPlan) { p.Entities = append(p.Entities, p.Entities[0]) }},
		{name: "duplicate_route", mutate: func(p *runfork.RunForkPlan) {
			copy := p.Entities[0]
			copy.EntityID = eventtest.UUID("duplicate-route-owner")
			p.Entities = append(p.Entities, copy)
		}},
	} {
		t.Run(cut.name, func(t *testing.T) {
			mutant := plan
			mutant.Entities = append([]runfork.RunForkEntityState(nil), plan.Entities...)
			metadata := *meta
			metadata.InitialMaterialization = bytes.Clone(meta.InitialMaterialization)
			mutant.Entities[0].MaterializationMetadata = &metadata
			if cut.mutate != nil {
				cut.mutate(&mutant)
			}
			if cut.capture != nil {
				changed := receipt
				cut.capture(&changed)
				metadata.InitialMaterialization, err = json.Marshal(changed)
				if err != nil {
					t.Fatal(err)
				}
			}
			before := fixedConstructionEvidenceSnapshotForTest(t, mutant)
			if _, _, found, err := FixedConstructionEvidenceForRoute(mutant, receipt.Identity.Route()); err == nil || found {
				t.Fatalf("invalid fixed evidence became authority: found=%t err=%v", found, err)
			}
			if after := fixedConstructionEvidenceSnapshotForTest(t, mutant); !bytes.Equal(before, after) {
				t.Fatal("fixed evidence refusal repaired its input")
			}
		})
	}
}

func fixedConstructionEvidenceSnapshotForTest(t testing.TB, plan runfork.RunForkPlan) []byte {
	t.Helper()
	plan.Entities = append([]runfork.RunForkEntityState(nil), plan.Entities...)
	var receipts []string
	for i := range plan.Entities {
		metadata := *plan.Entities[i].MaterializationMetadata
		receipts = append(receipts, string(metadata.InitialMaterialization))
		// Preserve malformed wire bytes without asking RawMessage to serialize them.
		metadata.InitialMaterialization = nil
		plan.Entities[i].MaterializationMetadata = &metadata
	}
	raw, err := json.Marshal(struct {
		Plan     runfork.RunForkPlan
		Receipts []string
	}{plan, receipts})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
