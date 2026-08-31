package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/contractelementidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestResolveHandlerCollectionItemTypeOwnsDirectAndIntermediateSources(t *testing.T) {
	bundle := &WorkflowContractBundle{
		RootTypes: TypeCatalogDocument{Types: map[string]NamedTypeDecl{
			"WorkItem": {Fields: map[string]TypeFieldSpec{
				"id":   {Type: "text"},
				"tags": {Type: "[text]"},
			}},
		}},
		RootEntities: EntityContractsDocument{
			"work_state": {Fields: map[string]EntityFieldDecl{"items": {Type: "[WorkItem]"}}},
		},
		Events: map[string]EventCatalogEntry{
			"work.received": {Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{"items": {Type: "[WorkItem]"}}, Required: []string{"items"}}},
		},
	}
	node := identitytest.RootNode(t, "worker")
	handler := SystemNodeEventHandler{
		Query:  &QuerySpec{Source: "payload.items", StoreAs: "computed.queried"},
		Filter: &FilterSpec{ItemsFrom: "computed.queried", StoreAs: "computed.filtered"},
	}

	tests := []struct {
		name       string
		source     string
		wantOrigin string
	}{
		{name: "payload", source: "payload.items", wantOrigin: "payload.items"},
		{name: "entity", source: "entity.items", wantOrigin: "entity.items"},
		{name: "query intermediate", source: "computed.queried", wantOrigin: "payload.items"},
		{name: "filter chain", source: "computed.filtered", wantOrigin: "payload.items"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resolved, err := bundle.ResolveHandlerCollectionItemType(node, "work.received", handler, tc.source)
			if err != nil {
				t.Fatalf("ResolveHandlerCollectionItemType: %v", err)
			}
			if resolved.Origin != tc.wantOrigin || resolved.ItemType.Name != "WorkItem" {
				t.Fatalf("resolution = %#v, want origin %q WorkItem", resolved, tc.wantOrigin)
			}
		})
	}
}

func TestResolveHandlerCollectionItemTypeRejectsUnresolvedIntermediate(t *testing.T) {
	bundle := &WorkflowContractBundle{}
	_, err := bundle.ResolveHandlerCollectionItemType(identitytest.RootNode(t, "worker"), "work.received", SystemNodeEventHandler{}, "computed.rows")
	if err == nil || !strings.Contains(err.Error(), "supported query/filter intermediate") {
		t.Fatalf("ResolveHandlerCollectionItemType error = %v, want unresolved intermediate rejection", err)
	}
}

func TestResolveHandlerCollectionItemTypeOwnsImplicitOutputsAndEntityTables(t *testing.T) {
	bundle := collectionSemanticsTestBundle()
	node := identitytest.RootNode(t, "worker")
	handler := SystemNodeEventHandler{
		Query:  &QuerySpec{Source: "payload.items", Select: []string{"id", "note"}},
		Filter: &FilterSpec{ItemsFrom: "computed.query", Condition: "item.id != ''"},
	}

	queryOutput, err := bundle.ResolveHandlerCollectionItemType(node, "work.received", handler, "computed.query")
	if err != nil {
		t.Fatalf("resolve implicit query output: %v", err)
	}
	if queryOutput.Source != "computed.query" || queryOutput.Origin != "payload.items" {
		t.Fatalf("query output = %#v", queryOutput)
	}
	note, ok := queryOutput.ItemType.Field("note")
	if !ok || !note.IsOptional {
		t.Fatalf("projected note = %#v, want optional field", note)
	}

	filtered, err := bundle.ResolveHandlerCollectionItemType(node, "work.received", handler, "computed.filter")
	if err != nil {
		t.Fatalf("resolve implicit filter output: %v", err)
	}
	if filtered.Source != "computed.filter" || filtered.Origin != "payload.items" {
		t.Fatalf("filter output = %#v", filtered)
	}

	entityHandler := SystemNodeEventHandler{Query: &QuerySpec{Entities: "items"}}
	entityPlan, err := bundle.ResolveHandlerQueryCollectionPlan(node, "work.received", entityHandler)
	if err != nil {
		t.Fatalf("resolve entity-table query: %v", err)
	}
	if entityPlan.Source.Kind != WorkflowCollectionSourceEntityTable || entityPlan.Source.EntityType != "items" {
		t.Fatalf("entity source = %#v", entityPlan.Source)
	}
	if _, ok := entityPlan.Source.ItemType.Field("status"); !ok {
		t.Fatalf("entity source item type = %#v, want status", entityPlan.Source.ItemType)
	}
}

func TestResolveHandlerQueryCollectionPlanRejectsDualSource(t *testing.T) {
	bundle := collectionSemanticsTestBundle()
	_, err := bundle.ResolveHandlerQueryCollectionPlan(identitytest.RootNode(t, "worker"), "work.received", SystemNodeEventHandler{
		Query: &QuerySpec{Source: "payload.items", Entities: "items"},
	})
	if err == nil || !strings.Contains(err.Error(), "exactly one collection source") {
		t.Fatalf("dual-source error = %v", err)
	}
}

func TestResolveHandlerCollectionPlanEnforcesPhaseOrderedDependencies(t *testing.T) {
	bundle := collectionSemanticsTestBundle()
	node := identitytest.RootNode(t, "worker")
	valid := SystemNodeEventHandler{
		Query:   &QuerySpec{Source: "payload.items"},
		Filter:  &FilterSpec{ItemsFrom: "computed.query", Condition: "item.id != ''"},
		GroupBy: &GroupBySpec{ItemsFrom: "computed.filter", Key: "status"},
		Reduce:  &ReduceSpec{ItemsFrom: "computed.filter", Operation: "sum"},
		Count:   &CountSpec{ItemsFrom: "computed.filter"},
	}
	if _, err := bundle.ResolveHandlerCollectionPlan(node, "work.received", valid); err != nil {
		t.Fatalf("resolve valid phase-ordered collection plan: %v", err)
	}

	tests := []struct {
		name    string
		handler SystemNodeEventHandler
	}{
		{name: "query from same query", handler: SystemNodeEventHandler{Query: &QuerySpec{Source: "computed.query"}}},
		{name: "query from later filter", handler: SystemNodeEventHandler{Query: &QuerySpec{Source: "computed.filter"}, Filter: &FilterSpec{ItemsFrom: "payload.items", Condition: "true"}}},
		{name: "query from later group", handler: SystemNodeEventHandler{Query: &QuerySpec{Source: "computed.group_by"}, GroupBy: &GroupBySpec{ItemsFrom: "payload.items", Key: "status"}}},
		{name: "query from later reduce", handler: SystemNodeEventHandler{Query: &QuerySpec{Source: "computed.reduce"}, Reduce: &ReduceSpec{ItemsFrom: "payload.items", Operation: "sum"}}},
		{name: "query from later count", handler: SystemNodeEventHandler{Query: &QuerySpec{Source: "computed.count"}, Count: &CountSpec{ItemsFrom: "payload.items"}}},
		{name: "filter from same filter", handler: SystemNodeEventHandler{Filter: &FilterSpec{ItemsFrom: "computed.filter", Condition: "true"}}},
		{name: "filter from later group", handler: SystemNodeEventHandler{Filter: &FilterSpec{ItemsFrom: "computed.group_by", Condition: "true"}, GroupBy: &GroupBySpec{ItemsFrom: "payload.items", Key: "status"}}},
		{name: "group from later reduce", handler: SystemNodeEventHandler{GroupBy: &GroupBySpec{ItemsFrom: "computed.reduce", Key: "status"}, Reduce: &ReduceSpec{ItemsFrom: "payload.items", Operation: "sum"}}},
		{name: "reduce from later count", handler: SystemNodeEventHandler{Reduce: &ReduceSpec{ItemsFrom: "computed.count", Operation: "sum"}, Count: &CountSpec{ItemsFrom: "payload.items"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := bundle.ResolveHandlerCollectionPlan(node, "work.received", tc.handler)
			if err == nil || !strings.Contains(err.Error(), "same or a later execution phase") {
				t.Fatalf("error = %v, want future-phase rejection", err)
			}
		})
	}
}

func TestResolveHandlerCollectionPlanRejectsDuplicateAndOverlappingOutputs(t *testing.T) {
	bundle := collectionSemanticsTestBundle()
	node := identitytest.RootNode(t, "worker")
	tests := []struct {
		name    string
		handler SystemNodeEventHandler
	}{
		{
			name: "mismatched collection exact collision",
			handler: SystemNodeEventHandler{
				Query:  &QuerySpec{Source: "payload.items", StoreAs: "computed.rows"},
				Filter: &FilterSpec{ItemsFrom: "payload.other_items", Condition: "true", StoreAs: "computed.rows"},
			},
		},
		{
			name: "list aggregate exact collision",
			handler: SystemNodeEventHandler{
				Query: &QuerySpec{Source: "payload.items", StoreAs: "computed.rows"},
				Count: &CountSpec{ItemsFrom: "payload.items", StoreAs: "computed.rows"},
			},
		},
		{
			name: "parent then child overlap",
			handler: SystemNodeEventHandler{
				Query:  &QuerySpec{Source: "payload.items", StoreAs: "computed.rows"},
				Filter: &FilterSpec{ItemsFrom: "payload.items", Condition: "true", StoreAs: "computed.rows.filtered"},
			},
		},
		{
			name: "child then parent overlap",
			handler: SystemNodeEventHandler{
				Query:  &QuerySpec{Source: "payload.items", StoreAs: "computed.rows.filtered"},
				Filter: &FilterSpec{ItemsFrom: "payload.items", Condition: "true", StoreAs: "computed.rows"},
			},
		},
		{
			name: "unrooted entity alias collision",
			handler: SystemNodeEventHandler{
				Query: &QuerySpec{Source: "payload.items", StoreAs: "entity.rows"},
				Count: &CountSpec{ItemsFrom: "payload.items", StoreAs: "rows"},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := bundle.ResolveHandlerCollectionPlan(node, "work.received", tc.handler)
			if err == nil || !strings.Contains(err.Error(), "duplicate or overlapping ownership") {
				t.Fatalf("error = %v, want output ownership rejection", err)
			}
		})
	}
}

func TestResolveCollectionPlansAdmitGroupAndSelectionFields(t *testing.T) {
	bundle := collectionSemanticsTestBundle()
	node := identitytest.RootNode(t, "worker")

	query, err := bundle.ResolveHandlerQueryCollectionPlan(node, "work.received", SystemNodeEventHandler{
		Query: &QuerySpec{Source: "payload.items", GroupBy: "item.status", Select: []string{"id", "note"}},
	})
	if err != nil {
		t.Fatalf("resolve query plan: %v", err)
	}
	if query.GroupBy == nil || query.GroupBy.Name != "status" || len(query.Select) != 2 || !query.Select[1].IsOptional {
		t.Fatalf("query plan = %#v", query)
	}

	group, err := bundle.ResolveHandlerGroupByCollectionPlan(node, "work.received", SystemNodeEventHandler{
		GroupBy: &GroupBySpec{ItemsFrom: "payload.items", Key: "status"},
	})
	if err != nil {
		t.Fatalf("resolve standalone group plan: %v", err)
	}
	if group.Key.Name != "status" || group.StoreAs != "computed.group_by" {
		t.Fatalf("group plan = %#v", group)
	}
}

func TestResolveCollectionPlansRejectInvalidSelectors(t *testing.T) {
	bundle := collectionSemanticsTestBundle()
	node := identitytest.RootNode(t, "worker")
	tests := []struct {
		name    string
		handler SystemNodeEventHandler
		want    string
	}{
		{name: "unknown query group", handler: SystemNodeEventHandler{Query: &QuerySpec{Source: "payload.items", GroupBy: "missing"}}, want: "undeclared item field missing"},
		{name: "optional query group", handler: SystemNodeEventHandler{Query: &QuerySpec{Source: "payload.items", GroupBy: "note"}}, want: "without a presence decision"},
		{name: "collection query group", handler: SystemNodeEventHandler{Query: &QuerySpec{Source: "payload.items", GroupBy: "tags"}}, want: "must be scalar"},
		{name: "unknown selection", handler: SystemNodeEventHandler{Query: &QuerySpec{Source: "payload.items", Select: []string{"missing"}}}, want: "undeclared item field missing"},
		{name: "unknown standalone group", handler: SystemNodeEventHandler{GroupBy: &GroupBySpec{ItemsFrom: "payload.items", Key: "missing"}}, want: "undeclared item field missing"},
		{name: "optional standalone group", handler: SystemNodeEventHandler{GroupBy: &GroupBySpec{ItemsFrom: "payload.items", Key: "note"}}, want: "without a presence decision"},
		{name: "collection standalone group", handler: SystemNodeEventHandler{GroupBy: &GroupBySpec{ItemsFrom: "payload.items", Key: "tags"}}, want: "must be scalar"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.handler.Query != nil {
				_, err = bundle.ResolveHandlerQueryCollectionPlan(node, "work.received", tc.handler)
			} else {
				_, err = bundle.ResolveHandlerGroupByCollectionPlan(node, "work.received", tc.handler)
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func collectionSemanticsTestBundle() *WorkflowContractBundle {
	return &WorkflowContractBundle{
		RootTypes: TypeCatalogDocument{Types: map[string]NamedTypeDecl{
			"WorkItem": {Fields: map[string]TypeFieldSpec{
				"id":     {Type: "text"},
				"status": {Type: "text"},
				"note":   {Type: "text", IsOptional: true},
				"tags":   {Type: "[text]"},
			}},
			"OtherItem": {Fields: map[string]TypeFieldSpec{"value": {Type: "integer"}}},
		}},
		RootEntities: EntityContractsDocument{
			"items": {Fields: map[string]EntityFieldDecl{"id": {Type: "text"}, "status": {Type: "text"}}},
		},
		Events: map[string]EventCatalogEntry{
			"work.received": {Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{"items": {Type: "[WorkItem]"}, "other_items": {Type: "[OtherItem]"}}, Required: []string{"items", "other_items"}}},
		},
	}
}

func TestFanOutCollectionItemTypePreservesNamedFieldPresence(t *testing.T) {
	item, err := resolveWorkflowCollectionItemType(CatalogTypeReference{
		Type: "[WorkItem]",
		Catalog: TypeCatalogDocument{Types: map[string]NamedTypeDecl{
			"WorkItem": {Fields: map[string]TypeFieldSpec{
				"id":   {Type: "text"},
				"note": {Type: "text", IsOptional: true},
			}},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("resolveFanOutCollectionItemType: %v", err)
	}
	note, ok := item.Field("note")
	if !ok || !note.IsOptional || note.Type.Kind != CatalogTypeText {
		t.Fatalf("note = %#v, want optional text field", note)
	}
}

func TestWorkflowCollectionItemTypeRejectsUntypedArray(t *testing.T) {
	_, err := resolveWorkflowCollectionItemType(CatalogTypeReference{Type: "array"}, nil)
	if err == nil || !strings.Contains(err.Error(), "exact collection item type") {
		t.Fatalf("resolveWorkflowCollectionItemType error = %v, want exact item type rejection", err)
	}
}

func TestFanOutCompiledPlanRegistryOwnsEverySiteAndReturnsImmutableCopies(t *testing.T) {
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(runtimeidentity.RootPackageKey, "", "dispatcher")
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{
		"00000000-0000-4000-8000-000000002271",
		"00000000-0000-4000-8000-000000002272",
		"00000000-0000-4000-8000-000000002273",
	}
	newSpec := func(raw string) *FanOutSpec {
		id, err := contractelementidentity.ParseContractElementID(raw)
		if err != nil {
			t.Fatal(err)
		}
		return &FanOutSpec{ElementID: id, ItemsFrom: "payload.items", As: "row_item", Identity: "row_item.id", Emit: EmitSpec{
			Event: "item.requested", Fields: map[string]ExpressionValue{"id": CELExpression("row_item.id")},
		}}
	}
	handler := SystemNodeEventHandler{
		FanOut:     newSpec(ids[0]),
		Rules:      []HandlerRuleEntry{{Condition: "payload.ready", FanOut: newSpec(ids[1])}},
		OnComplete: []HandlerRuleEntry{{Condition: "else", FanOut: newSpec(ids[2])}},
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
		if plan.Ref.BundleHash == "" || plan.Ref.SemanticDigest == "" || plan.Ref.ElementRef.ElementID == "" {
			t.Fatalf("compiled plan lacks durable identity: %#v", plan.Ref)
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
	packageFile := filepath.Join(root, "package.yaml")
	platformFile := filepath.Join(root, "platform-spec.yaml")
	if err := os.WriteFile(packageFile, []byte("name: fan-out-plan-registry-test\nversion: 1.0.0\nflows: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(platformFile, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return &WorkflowContractBundle{
		Paths:  ContractPaths{ContractsRoot: root, ProjectPackageFile: packageFile, PlatformSpecFile: platformFile},
		Nodes:  map[string]SystemNodeContract{"dispatcher": {ID: "dispatcher", EventHandlers: map[string]SystemNodeEventHandler{"batch.ready": handler}}},
		Events: map[string]EventCatalogEntry{"batch.ready": {Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{"items": {Type: "[json]"}}}}},
	}
}

func TestFanOutSourceAfterWritesClassifiesEveryEntityMutationForm(t *testing.T) {
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(runtimeidentity.RootPackageKey, "", "dispatcher")
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
	elementID, err := contractelementidentity.ParseContractElementID("00000000-0000-4000-8000-000000002274")
	if err != nil {
		return SystemNodeEventHandler{}, err
	}
	handler.FanOut.ElementID = elementID
	return QualifySystemNodeHandlerRuleRefs(node, handler)
}
