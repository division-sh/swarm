package semanticview

import (
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"gopkg.in/yaml.v3"
)

func TestSemanticSourceEventProofConsumesEffectiveReceiverCarries(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	root := filepath.Join(repoRoot, "examples", "routing", "template-create-minted-key")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatal(err)
	}
	source := Wrap(bundle)

	flowProof := ResolveFlowEventProof(source, "validator", "validation.requested")
	if !flowProof.HasSchema || flowProof.Entry.Payload.Properties["validation_case_id"].Type != "uuid" {
		t.Fatalf("flow proof omitted effective receiver carry: %#v", flowProof)
	}
	node := identitytest.ExecutableNode(t, "validator", "validator-node")
	nodeProof := ResolveExecutableNodeEventProof(source, node, "validation.requested")
	if !nodeProof.HasSchema || nodeProof.Entry.Payload.Properties["validation_case_id"].Type != "uuid" {
		t.Fatalf("executable-node proof omitted effective receiver carry: %#v", nodeProof)
	}
}

func TestResolveEventSchemaBindsConcreteTemplateInstanceToAuthoredDeclaration(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	root := filepath.Join(repoRoot, "examples", "routing", "fan-in", "stream")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatal(err)
	}
	resolution := ResolveEventSchema(Wrap(bundle), "operating", "operating/ti-proof/operating.reported")
	if !resolution.HasCompiled || !resolution.HasClassification || resolution.Classification != runtimecontracts.CompiledEventSchemaAuthored {
		t.Fatalf("template-instance schema binding = %#v, want compiled authored declaration", resolution)
	}
	if resolution.CompiledSchema.FlowPath() != "operating" {
		t.Fatalf("schema owner = %q, want declared flow path", resolution.CompiledSchema.FlowPath())
	}
	invalid := ResolveEventSchema(Wrap(bundle), "operating/ti-proof", "operating/ti-proof/operating.reported")
	if invalid.HasSchema || invalid.HasCompiled {
		t.Fatalf("instance path became a schema owner: %#v", invalid)
	}
}

func TestCompiledEventSchemaRejectsUnresolvedTypesAtAdmission(t *testing.T) {
	root := &runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "."},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"handoff.completed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"evidence": {Type: "NotDeclared"},
					},
					Required: []string{"evidence"},
				},
			},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: root,
			ByID: map[string]*runtimecontracts.FlowContractView{".": root},
		},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err == nil || !strings.Contains(err.Error(), "NotDeclared") {
		t.Fatalf("admission error = %v, want unresolved NotDeclared", err)
	}
}

func TestResolveEventSchema_UsesPlatformEmittedCatalogEntry(t *testing.T) {
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(`
payload:
  mailbox_id: uuid
  mailbox_payload: object
  decided_at: timestamp
`), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	bundle := &runtimecontracts.WorkflowContractBundle{}
	root := &runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}}
	bundle.FlowTree = flowmodel.Tree[runtimecontracts.FlowContractView]{Root: root, ByID: map[string]*runtimecontracts.FlowContractView{".": root}}
	bundle.Platform = admittedCatalogTestSpec(t, map[string]yaml.Node{
		"mailbox.card_decided": *doc.Content[0],
	})

	source := Wrap(bundle)
	if scope, found := source.FlowScopeByID("."); !found || scope.ID != "." {
		t.Fatal("platform fixture requires an actual dot-root flow")
	}
	if resolution := ResolveEventSchema(source, "", "mailbox.card_decided"); resolution.HasSchema {
		t.Fatal("empty coordinate gained a platform payload schema")
	}
	resolution := ResolveEventSchema(source, ".", "mailbox.card_decided")
	if !resolution.HasSchema {
		t.Fatal("expected platform event schema resolution")
	}
	properties, ok := resolution.Schema.Schema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties = %#v, want map", resolution.Schema.Schema["properties"])
	}
	if _, ok := properties["mailbox_payload"]; !ok {
		t.Fatalf("properties missing mailbox_payload: %#v", properties)
	}
	if resolution.EventKey != "mailbox.card_decided" {
		t.Fatalf("EventKey = %q, want mailbox.card_decided", resolution.EventKey)
	}
	if !resolution.HasClassification || resolution.Classification != runtimecontracts.CompiledEventSchemaPlatform {
		t.Fatalf("classification = %q/%v, want platform", resolution.Classification, resolution.HasClassification)
	}
}

func TestResolveEventSchema_DoesNotInventSchemaForReferenceOnlyPlatformEvent(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{}
	root := &runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}}
	bundle.FlowTree = flowmodel.Tree[runtimecontracts.FlowContractView]{Root: root, ByID: map[string]*runtimecontracts.FlowContractView{".": root}}
	platform, err := runtimecontracts.ParsePlatformSpecDocument([]byte(`
platform_tables:
  diagnostics_encoding:
    runtime_log_encoding:
      event_name: platform.runtime_log
      produced_by_type: platform
      payload_fields: {message: human-readable}
platform_events:
  catalog:
    platform.runtime_log:
      defined_in: platform_tables.diagnostics_encoding
      schema_authority: platform-spec.yaml#platform_tables.diagnostics_encoding
      producer_evidence: {produced_by_type: platform, produced_by: runtime}
`), "platform-spec.yaml")
	if err != nil {
		t.Fatal(err)
	}
	bundle.Platform = platform

	source := Wrap(bundle)
	if scope, found := source.FlowScopeByID("."); !found || scope.ID != "." {
		t.Fatal("reference-only control requires an admitted dot-root flow")
	}
	resolution := ResolveEventSchema(source, ".", "platform.runtime_log")
	if resolution.HasSchema {
		t.Fatalf("reference-only platform event resolved as payload schema: %#v", resolution)
	}
}

func TestResolveFlowEventProof_TemplateInstanceOutputUsesTemplateCatalog(t *testing.T) {
	field, err := runtimecontracts.ParseTemplateInstanceField("instance_id")
	if err != nil {
		t.Fatal(err)
	}
	root := runtimecontracts.FlowContractView{Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}}
	root.Children = []runtimecontracts.FlowContractView{{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "child"},
		Path:  "child",
		Schema: runtimecontracts.FlowSchemaDocument{
			Instance: field,
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "child.done"}}},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"child.done": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"step": {Type: "string"},
					},
					Required: []string{"step"},
				},
			},
		},
	}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				".":     &root,
				"child": &root.Children[0],
			},
			ByPath: map[string]*runtimecontracts.FlowContractView{
				"child": &root.Children[0],
			},
		},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}

	source := Wrap(bundle)
	proof := ResolveFlowEventProof(source, "child", "child/inst-1/child.done")
	if !proof.HasSchema {
		t.Fatal("expected concrete template instance event to resolve template schema proof")
	}
	if proof.Local != "child.done" {
		t.Fatalf("Local = %q, want child.done", proof.Local)
	}
	if proof.CatalogKey != "child/child.done" {
		t.Fatalf("CatalogKey = %q, want child/child.done", proof.CatalogKey)
	}
	if proof.EventKey() != "child/inst-1/child.done" {
		t.Fatalf("EventKey = %q, want concrete event identity", proof.EventKey())
	}
	if proof.Entry.Payload.Properties["step"].Type != "string" {
		t.Fatalf("Entry payload = %#v, want step string", proof.Entry.Payload)
	}
	if proof.IsAuthored(source) {
		t.Fatal("concrete template-instance event identity must not become an authored declaration")
	}
	declaration := ResolveFlowEventProof(source, "child", "child/child.done")
	if !declaration.IsAuthored(source) {
		t.Fatalf("template declaration proof = %#v, want authored identity", declaration)
	}
}

func TestResolveFlowEventProof_TemplateDescendantPathDoesNotBecomeInstanceLocalEvent(t *testing.T) {
	root := runtimecontracts.FlowContractView{}
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "child"}, Schema: runtimecontracts.FlowSchemaDocument{},
		Path: "child",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"micro.done": {},
		},
	}
	grandchild := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "grandchild"}, Schema: runtimecontracts.FlowSchemaDocument{},
		Path: "child/grandchild",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"micro.done": {},
		},
	}
	child.Children = []runtimecontracts.FlowContractView{grandchild}
	root.Children = []runtimecontracts.FlowContractView{child}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child":      &root.Children[0],
				"grandchild": &root.Children[0].Children[0],
			},
			ByPath: map[string]*runtimecontracts.FlowContractView{
				"child":            &root.Children[0],
				"child/grandchild": &root.Children[0].Children[0],
			},
		},
	}

	proof := ResolveFlowEventProof(Wrap(bundle), "child", "child/grandchild/micro.done")
	if proof.Local == "micro.done" {
		t.Fatalf("Local = %q, want descendant path to remain non-local for child proof", proof.Local)
	}
}
