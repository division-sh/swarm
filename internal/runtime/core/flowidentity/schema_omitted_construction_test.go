package flowidentity_test

import (
	"bytes"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestA9SchemaOmittedConstructionUsesAdmittedTreeAndRetainedSource(t *testing.T) {
	root := canonicalrouting.CopySchemaOmittedConstructionTree(t)
	repo := canonicalrouting.RepoRoot(t)
	disk, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, "")
	if err != nil {
		t.Fatal(err)
	}
	blob := disk.SourceArtifact.LogicalBlob()
	retained, err := sourceartifact.DecodeLogical(blob)
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := contracts.LoadWorkflowContractBundleFromArtifact(repo, retained, "", contracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for name, bundle := range map[string]*contracts.WorkflowContractBundle{"disk": disk, "retained": reloaded} {
		t.Run(name, func(t *testing.T) {
			bundle.FlowSchemas["dead"] = contracts.FlowSchemaDocument{Name: "not-an-admitted-node"}
			source := semanticview.Wrap(bundle)
			if bundle.RootSchema != nil {
				t.Fatal("effective schema fabricated an authored root declaration")
			}
			for _, flow := range []string{".", "parent/middle", "resources"} {
				if _, authored := bundle.FlowSchemas[flow]; authored {
					t.Fatalf("effective schema fabricated an authored declaration at %s", flow)
				}
				schema, found := source.FlowSchemaByID(flow)
				if !found || !schema.Instance.Empty() || len(schema.Pins.Inputs.EventPins) != 0 || len(schema.Pins.Outputs.EventPins) != 0 {
					t.Fatalf("admitted schema-omitted node %s has invented or missing shape: %+v found=%t", flow, schema, found)
				}
				constructor, err := pipeline.CompileFlowConstructor(source, flow, "")
				if err != nil || !constructor.Eligible() || constructor.KeyField() != "" {
					t.Fatalf("shared constructor rejected effective keyless node %s: %+v err=%v", flow, constructor, err)
				}
				catalog, found := semanticview.WorkflowStageTopology(source, flow)
				if !found || !catalog.ValidStageCatalog() || catalog.StageCount() != 0 || len(catalog.FinalStageIDs()) != 0 {
					t.Fatalf("admitted stageless node %s has missing or invented catalog: %+v found=%t", flow, catalog, found)
				}
			}
			const runID = "11111111-1111-4111-8111-111111111111"
			rootOwner := flowidentity.Stored(source, ".", runID, runID, runID, "")
			if err := rootOwner.ValidateConstruction(source, runID); err != nil {
				t.Fatalf("schema-omitted root: %v", err)
			}
			resourceOwner, err := flowidentity.KeylessChild(source, rootOwner, "resources")
			if err != nil {
				t.Fatal(err)
			}
			parent, err := flowidentity.KeyedChild(source, rootOwner, "parent", "stored-parent")
			if err != nil {
				t.Fatal(err)
			}
			middle, err := flowidentity.KeylessChild(source, parent, "parent/middle")
			if err != nil {
				t.Fatal(err)
			}
			leaf, err := flowidentity.KeyedChild(source, middle, "parent/middle/leaf", "stored-leaf")
			if err != nil {
				t.Fatal(err)
			}
			if middle.InstancePath != "parent/stored-parent/middle" || leaf.ParentRoute.FlowInstance != middle.InstancePath {
				t.Fatalf("schema-omitted ancestry discarded its keyed parent: middle=%+v leaf=%+v", middle, leaf)
			}
			for _, instance := range []flowidentity.Instance{resourceOwner, parent, middle, leaf} {
				if err := instance.ValidateConstruction(source, runID); err != nil {
					t.Fatalf("effective constructor refused %+v: %v", instance, err)
				}
			}
			for _, unknown := range []string{"missing", "dead", "resources/data", "../outside"} {
				if _, found := semanticview.WorkflowStageTopology(source, unknown); found {
					t.Fatalf("non-admitted node %s acquired a compiled catalog", unknown)
				}
				if _, found := source.FlowSchemaByID(unknown); found {
					t.Fatalf("non-admitted node %s acquired a schema", unknown)
				}
				if _, err := pipeline.CompileFlowConstructor(source, unknown, ""); err == nil {
					t.Fatalf("non-admitted node %s acquired a constructor", unknown)
				}
			}
			if _, err := flowidentity.KeylessChild(source, rootOwner, "parent/middle"); err == nil {
				t.Fatal("schema omission bypassed the immediate-parent requirement")
			}
			if _, err := flowidentity.KeylessChild(source, flowidentity.Instance{}, "resources"); err == nil {
				t.Fatal("schema omission admitted a missing structural parent")
			}
			if _, err := flowidentity.KeyedChild(source, rootOwner, "resources", "invented-key"); err == nil {
				t.Fatal("schema omission invented a keyed constructor")
			}
			if !bytes.Equal(blob, bundle.SourceArtifact.LogicalBlob()) || bundle.SourceArtifact.BundleHash() != disk.SourceArtifact.BundleHash() {
				t.Fatal("effective constructor changed authored bytes or retained identity")
			}
		})
	}
}
