package pipeline_test

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func activationAncestrySource(t *testing.T) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, map[string]string{
		"schema.yaml":                    "name: activation-ancestry\nstages:\n  created: {}\n",
		"operating/schema.yaml":          "name: operating\ninstance: instance_key\nstages:\n  created: {}\n",
		"operating/entities.yaml":        "item:\n  instance_key: text\n",
		"operating/review/schema.yaml":   "name: review\ninstance: instance_key\nstages:\n  created: {}\n",
		"operating/review/entities.yaml": "item:\n  instance_key: text\n",
		"operating/scout/schema.yaml":    "name: scout\nstages:\n  created: {}\n",
		"scout/schema.yaml":              "name: scout\nstages:\n  created: {}\n",
		"scout/review/schema.yaml":       "name: review\ninstance: instance_key\nstages:\n  created: {}\n",
		"scout/review/entities.yaml":     "item:\n  instance_key: text\n",
		"scout/detail/schema.yaml":       "name: detail\nstages:\n  created: {}\n",
	}))
}

func activationAncestryTree(t *testing.T, source semanticview.Source) pipeline.FlowInstanceActivationPlan {
	t.Helper()
	runID := "11111111-1111-4111-8111-111111111111"
	at := time.Date(2026, time.October, 14, 16, 0, 0, 0, time.UTC)
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("ancestry proof requires its admitted source")
	}
	makePlan := func(identity flowidentity.Instance) pipeline.FlowInstanceActivationPlan {
		instance := pipeline.WorkflowInstance{
			InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityID: identity.EntityID,
			WorkflowName: identity.TemplateID, WorkflowVersion: source.WorkflowVersion(), Mode: "static",
			ParentFlowID: identity.ParentRoute.FlowID, ParentFlowInstance: identity.ParentRoute.FlowInstance, ParentEntityID: identity.ParentEntityID,
			CurrentState: "created", StageDefined: true, CreatedAt: at, EnteredStageAt: at,
		}
		schema, found := source.FlowSchemaByID(identity.TemplateID)
		if !found {
			t.Fatal("unknown proof flow")
		}
		if !schema.Instance.Empty() {
			instance.Mode, instance.EntityType = "template", "item"
			key := "business-" + identity.InstanceID
			var err error
			instance.InstanceKey, err = pipeline.AdmitFlowInstanceKey(source, identity.TemplateID, key)
			if err != nil {
				t.Fatal(err)
			}
			instance.Fields = map[string]any{"instance_key": key}
		}
		if err := identity.ValidateConstruction(source, runID); err != nil {
			t.Fatalf("independently admit component identity: %v", err)
		}
		return pipeline.FlowInstanceActivationPlan{
			Identity: identity, Instance: instance, OccurredAt: at,
			CreatingInput: pipeline.FlowConstructionInput{EventID: "22222222-2222-4222-8222-222222222222", Input: "started"},
			Readiness: pipeline.DynamicFlowRuntimeReadinessPlan{
				Identity: identity, RunID: runID, BundleHash: bundle.SourceArtifact.BundleHash(),
				WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live,
			},
		}
	}
	root := flowidentity.Stored(source, ".", runID, runID, runID, "")
	child := func(parent flowidentity.Instance, flowID string, keyed bool) flowidentity.Instance {
		var identity flowidentity.Instance
		var err error
		if keyed {
			identity, err = flowidentity.KeyedChild(source, parent, flowID, "local-1")
		} else {
			identity, err = flowidentity.KeylessChild(source, parent, flowID)
		}
		if err != nil {
			t.Fatal(err)
		}
		return identity
	}
	keyed := child(root, "operating", true)
	keyed.EntityID = eventtest.UUID("stored-parent-entity")
	keyless := child(root, "scout", false)
	plan := makePlan(root)
	plan.Children = []pipeline.FlowInstanceActivationPlan{makePlan(keyed), makePlan(keyless)}
	plan.Children[0].Children = []pipeline.FlowInstanceActivationPlan{makePlan(child(keyed, "operating/review", true)), makePlan(child(keyed, "operating/scout", false))}
	plan.Children[1].Children = []pipeline.FlowInstanceActivationPlan{makePlan(child(keyless, "scout/review", true)), makePlan(child(keyless, "scout/detail", false))}
	return plan
}

func TestActivationPlanExactImmediateAncestry(t *testing.T) {
	source := activationAncestrySource(t)
	plan := activationAncestryTree(t, source)
	if err := plan.Validate(); err != nil {
		t.Fatalf("independently admitted keyed/keyless tree: %v", err)
	}
	if len(plan.ConstructionPlans()) != 7 {
		t.Fatal("matrix omitted an immediate or nested keyed/keyless component")
	}
	for _, descendant := range plan.Children[0].Children {
		if descendant.Identity.ParentEntityID != eventtest.UUID("stored-parent-entity") {
			t.Fatalf("descendant re-derived its admitted parent: %+v", descendant.Identity)
		}
	}
	children, err := flowidentity.KeylessChildFlowIDs(source, ".")
	if err != nil || len(children) != 1 || children[0] != "scout" {
		t.Fatalf("composed keyed child became an eager child: %v %v", children, err)
	}
}

func TestActivationPlanRefusesHostileImmediateAncestry(t *testing.T) {
	source := activationAncestrySource(t)
	cases := []struct {
		name   string
		change func(*pipeline.FlowInstanceActivationPlan)
	}{
		{"foreign_run", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Children[0].Readiness.RunID = "33333333-3333-4333-8333-333333333333"
		}},
		{"foreign_parent_flow", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Children[0].Readiness.Identity.ParentRoute.FlowID = "other"
		}},
		{"foreign_parent_path", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Children[0].Readiness.Identity.ParentRoute.FlowInstance = "other/parent"
		}},
		{"foreign_parent_entity", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Children[0].Readiness.Identity.ParentRoute.EntityID = "foreign-parent"
		}},
		{"foreign_stored_parent_entity", func(p *pipeline.FlowInstanceActivationPlan) { p.Children[0].Instance.ParentEntityID = "foreign-parent" }},
		{"absent_key", func(p *pipeline.FlowInstanceActivationPlan) { p.Children[0].Instance.InstanceKey = "" }},
		{"key_on_keyless", func(p *pipeline.FlowInstanceActivationPlan) { p.Children[1].Instance.InstanceKey = "foreign-key" }},
		{"keyed_mode_changed", func(p *pipeline.FlowInstanceActivationPlan) { p.Children[0].Instance.Mode = "static" }},
		{"unknown_mode", func(p *pipeline.FlowInstanceActivationPlan) { p.Children[0].Instance.Mode = "unknown" }},
		{"mismatched_discriminator", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Children[0].Readiness.Identity.InstanceID, p.Children[0].Instance.InstanceID = "other", "other"
		}},
		{"keyless_discriminator", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Children[1].Readiness.Identity.InstanceID, p.Children[1].Instance.InstanceID = "other", "other"
		}},
		{"prefix_lookalike", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Children[0].Readiness.Identity.InstancePath, p.Children[0].Instance.StorageRef = "operating-extra/local-1", "operating-extra/local-1"
		}},
		{"extra_path_segment", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Children[0].Readiness.Identity.InstancePath, p.Children[0].Instance.StorageRef = "operating/local-1/extra", "operating/local-1/extra"
		}},
		{"skipped_root_child", func(p *pipeline.FlowInstanceActivationPlan) {
			child := p.Children[0].Children[0]
			child.Readiness.Identity.ParentRoute = p.Children[0].Readiness.Identity.ParentRoute
			child.Instance.ParentEntityID = p.Identity.EntityID
			p.Children = []pipeline.FlowInstanceActivationPlan{child}
		}},
		{"nested_unrelated_scope", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Children[0].Children[0].Readiness.Identity.ScopeKey = "operating-extra/review"
		}},
		{"nested_prefix_lookalike", func(p *pipeline.FlowInstanceActivationPlan) {
			child := &p.Children[0].Children[0]
			child.Readiness.Identity.InstancePath, child.Instance.StorageRef = "operating/local-1-extra/review/local-1", "operating/local-1-extra/review/local-1"
		}},
		{"nested_skipped_parent", func(p *pipeline.FlowInstanceActivationPlan) {
			child := &p.Children[0].Children[0]
			child.Readiness.Identity.ScopeKey = "operating/review/deep"
			child.Readiness.Identity.InstancePath, child.Instance.StorageRef = "operating/local-1/review/deep/local-1", "operating/local-1/review/deep/local-1"
		}},
		{"duplicate_descendant", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Children[0].Children = append(p.Children[0].Children, p.Children[0].Children[0])
		}},
		{"source_hash", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Children[0].Readiness.BundleHash = "bundle-v2:sha256:" + strings.Repeat("d", 64)
		}},
		{"source_version", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Children[0].Readiness.WorkflowVersion = "other-version"
		}},
		{"execution_mode", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Children[0].Readiness.ExecutionMode = executionmode.Mock
		}},
		{"occurrence", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Children[0].OccurredAt = p.Children[0].OccurredAt.Add(time.Second)
		}},
		{"creating_publication", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Children[0].CreatingInput.EventID = "33333333-3333-4333-8333-333333333333"
		}},
		{"nested_creating_publication", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Children[0].Children[0].CreatingInput.EventID = "33333333-3333-4333-8333-333333333333"
		}},
		{"foreign_root_coordinate", func(p *pipeline.FlowInstanceActivationPlan) {
			p.Readiness.Identity.InstancePath, p.Instance.StorageRef = "33333333-3333-4333-8333-333333333333", "33333333-3333-4333-8333-333333333333"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := activationAncestryTree(t, source)
			tc.change(&plan)
			if err := plan.Validate(); err == nil || (!strings.Contains(err.Error(), "disagrees with its exact parent/source/occurrence") && !strings.Contains(err.Error(), "construction repeats descendant")) {
				t.Fatalf("hostile ancestry did not reach the exact tree refusal: %v", err)
			}
		})
	}
}
