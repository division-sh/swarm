package contracts

import (
	"os"
	"path/filepath"
	"testing"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestFanOutCompiledPlanRegistryOwnsEverySiteAndReturnsImmutableCopies(t *testing.T) {
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(".", "dispatcher")
	if err != nil {
		t.Fatal(err)
	}
	newSpec := func() *FanOutSpec {
		return &FanOutSpec{ItemsFrom: "payload.items", As: "row_item", Identity: "row_item.id", Emit: EmitSpec{
			Event: "item.requested", Fields: map[string]ExpressionValue{"id": CELExpression("row_item.id")},
		}}
	}
	handler := SystemNodeEventHandler{
		FanOut:     newSpec(),
		Rules:      []HandlerRuleEntry{{Condition: "payload.ready", FanOut: newSpec()}},
		OnComplete: []HandlerRuleEntry{{Condition: "else", FanOut: newSpec()}},
	}
	bundle := fanOutPlanRegistryTestBundle(t, handler)
	if failures := bundle.PrepareFanOutPlans(); len(failures) != 0 {
		t.Fatalf("PrepareFanOutPlans failures: %v", failures)
	}
	plans := bundle.FanOutPlansForHandler(node, "batch.ready")
	if len(plans) != 3 {
		t.Fatalf("compiled plans = %d, want handler/rule/on_complete", len(plans))
	}
	wantKinds := map[FanOutSiteKind]int{FanOutSiteHandler: -1, FanOutSiteRule: 0, FanOutSiteOnComplete: 0}
	for _, plan := range plans {
		if wantIndex, ok := wantKinds[plan.Site.Kind]; !ok || plan.Site.Index != wantIndex {
			t.Fatalf("compiled site = %#v, want closed site-kind/index set %#v", plan.Site, wantKinds)
		}
		if plan.Ref.BundleHash == "" || plan.Ref.SemanticDigest == "" {
			t.Fatalf("compiled plan lacks durable identity: %#v", plan.Ref)
		}
		if _, err := plan.Ref.ElementRef.DeclarationIdentity(); err != nil {
			t.Fatalf("compiled plan lacks canonical declaration identity: %v", err)
		}
	}

	plan := plans[0]
	plan.ItemsPath.Segments[0] = "corrupted"
	plan.Emit.Fields["id"] = CELExpression("corrupted")
	loaded, ok := bundle.FanOutPlanForSite(plan.Site)
	if !ok {
		t.Fatal("compiled plan disappeared")
	}
	if loaded.ItemsPath.Segments[0] != "items" || loaded.Emit.Fields["id"].CEL != "row_item.id" {
		t.Fatalf("caller mutated canonical plan: %#v", loaded)
	}
}

func fanOutPlanRegistryTestBundle(t *testing.T, handler SystemNodeEventHandler) *WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	schemaFile := filepath.Join(root, "schema.yaml")
	platformFile := filepath.Join(root, "platform-spec.yaml")
	if err := os.WriteFile(schemaFile, []byte("name: fan-out-plan-registry-test\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(platformFile, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	return &WorkflowContractBundle{
		SourceArtifact: artifact,
		Paths:          ContractPaths{PlatformSpecFile: platformFile},
		Nodes:          map[string]SystemNodeContract{"dispatcher": {ID: "dispatcher", EventHandlers: map[string]SystemNodeEventHandler{"batch.ready": handler}}},
		Events:         map[string]EventCatalogEntry{"batch.ready": {Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{"items": {Type: "[json]"}}}}},
	}
}

func TestFanOutSourceAfterWritesClassifiesEveryEntityMutationForm(t *testing.T) {
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(".", "dispatcher")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		write      WorkflowDataWrite
		projection bool
		want       bool
	}{
		{name: "explicit direct", write: WorkflowDataWrite{TargetRef: "entity.items", Value: LiteralExpression([]any{"next"})}, want: true},
		{name: "implicit direct", write: WorkflowDataWrite{TargetField: "items", Value: LiteralExpression([]any{"next"})}, want: true},
		{name: "mapped path", write: WorkflowDataWrite{TargetPathRef: "entity.items", SourceField: "replacement"}, want: true},
		{name: "computed value", write: WorkflowDataWrite{TargetRef: "entity.items", Value: CELExpression("payload.replacement")}, want: true},
		{name: "contained append", write: WorkflowDataWrite{Operation: WorkflowDataOperationAppend, TargetRef: "entity.items", Value: LiteralExpression("next")}, want: true},
		{name: "nested source field", write: WorkflowDataWrite{TargetRef: "entity.items.0", Value: LiteralExpression("next")}, want: true},
		{name: "different field", write: WorkflowDataWrite{TargetRef: "entity.other", Value: LiteralExpression([]any{"next"})}, want: false},
		{name: "accumulator projection", projection: true, want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle := &WorkflowContractBundle{
				RootEntities: EntityContractsDocument{
					"subject": {Fields: map[string]EntityFieldDecl{
						"items": {Type: "[text]"},
						"other": {Type: "[text]"},
					}},
				},
			}
			handler := SystemNodeEventHandler{
				FanOut: &FanOutSpec{ItemsFrom: "entity.items", As: "item", Identity: "item", Emit: EmitSpec{Event: "item.requested"}},
			}
			if tc.projection {
				handler.Accumulate = &AccumulateSpec{Into: "collected"}
				decl := bundle.RootEntities["subject"]
				field := decl.Fields["items"]
				field.MaterializeFrom = "dispatcher.collected"
				decl.Fields["items"] = field
				bundle.RootEntities["subject"] = decl
			} else {
				handler.DataAccumulation.Writes = []WorkflowDataWrite{tc.write}
			}
			qualified, err := completeFanOutSemanticsTestIdentity(node, handler)
			if err != nil {
				t.Fatal(err)
			}
			path, err := ValidateFanOutItemsSource(*qualified.FanOut)
			if err != nil {
				t.Fatal(err)
			}
			if got := bundle.fanOutSourceAfterWrites(node, qualified, *qualified.FanOut, path); got != tc.want {
				t.Fatalf("fanOutSourceAfterWrites = %v, want %v", got, tc.want)
			}
		})
	}
}

func completeFanOutSemanticsTestIdentity(node runtimeidentity.ExecutableNode, handler SystemNodeEventHandler) (SystemNodeEventHandler, error) {
	return QualifySystemNodeHandlerRuleRefsForEvent(node, "test.event", handler)
}
