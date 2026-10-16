package contracts

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

const detachedCatalogFields = "  label: {type: text, pattern: '^[a-z]+$', length: {min: 1, max: 10}, equal_to: peer}\n  peer: {type: text, length: {min: 1, max: 10}}\n  score: {type: numeric, range: {min: 1.5, max: 9.5}, equal_to: other}\n  other: {type: numeric, range: {min: 1.5, max: 9.5}}\n  note: text?\n"

func loadDetachedCatalogFixture(t *testing.T) *WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	types := "enums:\n  Choice: {values: [alpha, beta], default: alpha}\ntypes:\n  Probe:\n" + bytesToIndented(detachedCatalogFields) + "  Outer:\n    child: Probe\n"
	for name, body := range map[string]string{
		"schema.yaml": "name: detached-catalog\nstages: {done: {final: true}}\n",
		"types.yaml":  types, "state.yaml": "person:\n" + detachedCatalogFields + "  members: map[text]Probe\n",
		"events.yaml":       "audit.record:\n" + detachedCatalogFields + "batch.ready:\n  rows: [Probe]\n  label: text\n  result: Probe\n",
		"child/schema.yaml": "name: child\nstages: {done: {final: true}}\n",
		"child/types.yaml":  types, "child/state.yaml": "person:\n" + detachedCatalogFields,
		"child/events.yaml": "audit.record:\n" + detachedCatalogFields,
	} {
		writeFixtureFile(t, filepath.Join(root, name), body)
	}
	repo := repoRootForContractsTest(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func bytesToIndented(s string) string {
	var out bytes.Buffer
	for _, line := range bytes.Split([]byte(s), []byte("\n")) {
		if len(line) != 0 {
			out.WriteString("  ")
			out.Write(line)
			out.WriteByte('\n')
		}
	}
	return out.String()
}

func detachedBoundField(bound string) string {
	if bound == "length_min" || bound == "length_max" {
		return "label"
	}
	return "score"
}

func mutateDetachedBound(r SchemaRefinements, bound string) {
	switch bound {
	case "length_min":
		*r.Length.Min = 2
	case "length_max":
		*r.Length.Max = 9
	case "range_min":
		*r.Range.Min = 2.5
	case "range_max":
		*r.Range.Max = 8.5
	default:
		panic("unknown detachment bound")
	}
}

type catalogDetachedObservation struct {
	read        func() any
	refinements func() SchemaRefinements
}

func detachedTypeField(t *testing.T, typ ResolvedCatalogType, name string) ResolvedCatalogField {
	t.Helper()
	if typ.Kind == CatalogTypeList {
		typ = *typ.Element
	}
	if typ.Kind == CatalogTypeMap {
		typ = *typ.Value
	}
	field, ok := typ.FieldPath(name)
	if !ok {
		t.Fatalf("missing structural field %s", name)
	}
	return field
}

func detachedResolve(t *testing.T, ref CatalogTypeReference) ResolvedCatalogType {
	t.Helper()
	typ, err := ref.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	return typ
}

func detachedCatalogObservations(t *testing.T, b *WorkflowContractBundle, bound string) map[string]catalogDetachedObservation {
	t.Helper()
	name := detachedBoundField(bound)
	out := map[string]catalogDetachedObservation{}
	addCatalog := func(label string, get func() TypeCatalogDocument) {
		out[label] = catalogDetachedObservation{func() any { return get() }, func() SchemaRefinements { return get().Types["Probe"].Fields[name].Refinements }}
	}
	addEntities := func(label string, get func() EntityContractsDocument) {
		out[label] = catalogDetachedObservation{func() any { return get() }, func() SchemaRefinements { return get()["person"].Fields[name].Refinements }}
	}
	addCatalog("D03/root", b.RootTypeCatalog)
	addCatalog("D03/flow", func() TypeCatalogDocument {
		v, ok := b.FlowTypeCatalogByID("child")
		if !ok {
			t.Fatal("missing child types")
		}
		return v
	})
	addCatalog("D03/resolved", func() TypeCatalogDocument { return b.ResolvedTypeCatalogForFlow("child") })
	addEntities("D05/root", b.RootEntityContracts)
	addEntities("D05/flow", func() EntityContractsDocument {
		v, ok := b.FlowEntityContractsByID("child")
		if !ok {
			t.Fatal("missing child entities")
		}
		return v
	})
	addEntities("D05/resolved", func() EntityContractsDocument { return b.ResolvedEntityContractsForFlow("child") })
	addEntities("D06/root-wrapper", func() EntityContractsDocument {
		name, contract, ok := b.RootPrimaryEntityContract()
		if !ok {
			t.Fatal("missing root primary wrapper")
		}
		return EntityContractsDocument{name: contract}
	})
	addEntities("D06/flow-wrapper", func() EntityContractsDocument {
		name, contract, ok := b.FlowPrimaryEntityContract("child")
		if !ok {
			t.Fatal("missing flow primary wrapper")
		}
		return EntityContractsDocument{name: contract}
	})
	addEntities("D06/test-setup", func() EntityContractsDocument {
		primary, err := b.ResolveTestSetupPrimaryEntity("child", "person")
		if err != nil {
			t.Fatal(err)
		}
		return EntityContractsDocument{primary.EntityType: primary.Contract}
	})
	for _, flow := range []string{".", "child"} {
		primary := func() PrimaryEntityContract {
			v, err := b.ResolveFlowPrimaryEntity(flow)
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
		out["D06/primary/"+flow] = catalogDetachedObservation{func() any { return primary() }, func() SchemaRefinements { return primary().Contract.Fields[name].Refinements }}
		out["D07/structural/"+flow] = catalogDetachedObservation{func() any {
			v, err := primary().StructuralType()
			if err != nil {
				t.Fatal(err)
			}
			return v
		}, func() SchemaRefinements {
			v, err := primary().StructuralType()
			if err != nil {
				t.Fatal(err)
			}
			return detachedTypeField(t, v, name).Refinements
		}}
	}
	for _, ref := range []string{"Probe", "[Probe]", "map[text]Probe", "Outer"} {
		resolved := detachedResolve(t, CatalogTypeReference{Type: ref, Catalog: b.RootTypeCatalog()})
		path := name
		if ref == "Outer" {
			path = "child." + name
		}
		out["D04/clone/"+ref] = catalogDetachedObservation{func() any { return resolved }, func() SchemaRefinements { return detachedTypeField(t, resolved.Clone(), path).Refinements }}
	}
	compiled, ok, err := b.ResolveCompiledFlowEventSchema(".", "audit.record")
	if err != nil || !ok {
		t.Fatalf("compiled event: %t %v", ok, err)
	}
	for label, get := range map[string]func() SchemaRefinements{
		"D01/field": func() SchemaRefinements {
			f, ok := compiled.StructuralField(name)
			if !ok {
				t.Fatal("missing compiled field")
			}
			return f.Refinements
		},
		"D01/type": func() SchemaRefinements {
			v, _ := compiled.StructuralType()
			return detachedTypeField(t, v, name).Refinements
		},
	} {
		out[label] = catalogDetachedObservation{func() any { v, _ := compiled.StructuralType(); return v }, get}
	}
	successor, err := compiled.withRequiredField("added", "uuid", map[string]any{"type": "string", "format": "uuid"})
	if err != nil {
		t.Fatal(err)
	}
	out["D02/successor"] = catalogDetachedObservation{func() any {
		return []any{compiled.value.structuralType, successor.value.structuralType, compiled.AcceptanceSchema(), successor.AcceptanceSchema(), compiled.Source(), successor.Source()}
	}, func() SchemaRefinements { f, _ := successor.StructuralField(name); return f.Refinements }}
	addDetachedPlanObservations(t, out, b, name)
	return out
}

func addDetachedPlanObservations(t *testing.T, out map[string]catalogDetachedObservation, b *WorkflowContractBundle, name string) {
	t.Helper()
	ref := CatalogTypeReference{Type: "[Probe]", Catalog: b.RootTypeCatalog()}
	projection, err := AdmitCollectionProjection(ref)
	if err != nil {
		t.Fatal(err)
	}
	out["D08/item"] = catalogDetachedObservation{func() any { return projection }, func() SchemaRefinements { return detachedTypeField(t, projection.ItemType(), name).Refinements }}
	out["D08/clone"] = catalogDetachedObservation{func() any { return projection }, func() SchemaRefinements { return detachedTypeField(t, projection.Clone().Type, name).Refinements }}
	mapProjection, err := AdmitCollectionProjection(CatalogTypeReference{Type: "map[text]Probe", Catalog: ref.Catalog})
	if err != nil {
		t.Fatal(err)
	}
	out["D08/map-clone"] = catalogDetachedObservation{func() any { return mapProjection }, func() SchemaRefinements {
		if mapProjection.ItemType().Kind != CatalogTypeText {
			t.Fatal("map projection changed its text-key item type")
		}
		return detachedTypeField(t, mapProjection.Clone().Type, name).Refinements
	}}
	input := CompiledConnectionInput{value: &compiledConnectionInputValue{evidence: FlowInputInstanceSourceTypeEvidence{SourceType: ref, ReceiverType: ref}}}
	for label, selectType := range map[string]func(FlowInputInstanceSourceTypeEvidence) CatalogTypeReference{
		"source":   func(e FlowInputInstanceSourceTypeEvidence) CatalogTypeReference { return e.SourceType },
		"receiver": func(e FlowInputInstanceSourceTypeEvidence) CatalogTypeReference { return e.ReceiverType },
	} {
		out["D09/connection/"+label] = catalogDetachedObservation{func() any { return input.SourceEvidence() }, func() SchemaRefinements {
			return selectType(input.SourceEvidence()).Catalog.Types["Probe"].Fields[name].Refinements
		}}
	}
	plan := FanOutCompiledPlan{CollectionType: ref, CollectionProjection: projection, ItemType: projection.ItemType(), ItemAlias: "item", Identity: "payload.id", MaxItems: 10}
	semantics := plan.SemanticEvidence()
	for label, get := range map[string]func() FanOutPlanSemantics{"compiled": plan.SemanticEvidence, "clone": semantics.Clone} {
		out["D09/fanout/"+label+"/catalog"] = catalogDetachedObservation{func() any { return get() }, func() SchemaRefinements { return get().CollectionType.Catalog.Types["Probe"].Fields[name].Refinements }}
		out["D09/fanout/"+label+"/item"] = catalogDetachedObservation{func() any { return get() }, func() SchemaRefinements { return detachedTypeField(t, get().ItemType, name).Refinements }}
		out["D09/fanout/"+label+"/projection"] = catalogDetachedObservation{func() any { return get() }, func() SchemaRefinements {
			return detachedTypeField(t, get().CollectionProjection.Type, name).Refinements
		}}
	}
	join := WorkflowJoinPlan{Node: identitytest.RootNode(t, "collector"), ResultType: CatalogTypeReference{Type: "Probe", Catalog: ref.Catalog}, MembersCollectionProjection: mapProjection}
	out["D09/join/catalog"] = catalogDetachedObservation{func() any { return join }, func() SchemaRefinements {
		return join.Clone().ResultType.Catalog.Types["Probe"].Fields[name].Refinements
	}}
	out["D09/join/projection"] = catalogDetachedObservation{func() any { return join }, func() SchemaRefinements {
		return detachedTypeField(t, join.Clone().MembersCollectionProjection.Type, name).Refinements
	}}
	entry, _, ok := b.ResolveFlowEventCatalogEntry(".", "audit.record")
	if !ok {
		t.Fatal("missing authored event declaration")
	}
	out["D10/event-local-copy"] = catalogDetachedObservation{func() any { return entry }, func() SchemaRefinements { return cloneEventCatalogEntry(entry).Payload.Properties[name].Refinements }}
}

func detachedSnapshot(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestCatalogTypeDetachmentRetainedBoundaries(t *testing.T) {
	for _, bound := range []string{"length_min", "length_max", "range_min", "range_max"} {
		names := detachedCatalogObservations(t, loadDetachedCatalogFixture(t), bound)
		for name := range names {
			t.Run(bound+"/"+name, func(t *testing.T) {
				b := loadDetachedCatalogFixture(t)
				observation := detachedCatalogObservations(t, b, bound)[name]
				before := detachedSnapshot(t, observation.read())
				artifact := append([]byte(nil), b.SourceArtifact.LogicalBlob()...)
				mutateDetachedBound(observation.refinements(), bound)
				after := detachedSnapshot(t, observation.read())
				if !bytes.Equal(before, after) {
					t.Fatalf("retained %s changed: before=%s after=%s", name, before, after)
				}
				if !bytes.Equal(artifact, b.SourceArtifact.LogicalBlob()) {
					t.Fatal("source artifact changed")
				}
			})
		}
	}
}

func TestCatalogTypeDetachmentCompiledInputCapture(t *testing.T) {
	for _, bound := range []string{"length_min", "length_max", "range_min", "range_max"} {
		t.Run(bound, func(t *testing.T) {
			b := loadDetachedCatalogFixture(t)
			entry, _, ok := b.ResolveFlowEventCatalogEntry(".", "audit.record")
			if !ok {
				t.Fatal("missing authored event declaration")
			}
			compiled, err := CompileImportedEventSchema(".", "audit.record", entry, CompiledEventSchemaSource{FlowPath: ".", Layer: "audit", File: "events.yaml"})
			if err != nil {
				t.Fatal(err)
			}
			successor, err := compiled.withRequiredField("added", "uuid", map[string]any{"type": "string", "format": "uuid"})
			if err != nil {
				t.Fatal(err)
			}
			snapshot := func() any {
				return []any{compiled.value.structuralType, successor.value.structuralType, compiled.CanonicalAcceptanceSchema(), successor.CanonicalAcceptanceSchema(), compiled.AcceptanceSchemaDigest(), successor.AcceptanceSchemaDigest(), compiled.Source(), successor.Source()}
			}
			before := detachedSnapshot(t, snapshot())
			mutateDetachedBound(entry.Payload.Properties[detachedBoundField(bound)].Refinements, bound)
			if after := detachedSnapshot(t, snapshot()); !bytes.Equal(before, after) {
				t.Fatalf("input mutation changed captured evidence: before=%s after=%s", before, after)
			}
		})
	}
}

func TestCatalogTypeDetachmentPrimaryAndResolverInputCapture(t *testing.T) {
	for _, bound := range []string{"length_min", "length_max", "range_min", "range_max"} {
		for _, origin := range []string{"primary", "resolver"} {
			t.Run(bound+"/"+origin, func(t *testing.T) {
				b := loadDetachedCatalogFixture(t)
				primary, err := b.ResolveRootPrimaryEntity()
				if err != nil {
					t.Fatal(err)
				}
				catalog := b.RootTypeCatalog()
				typ := detachedResolve(t, CatalogTypeReference{Type: "Probe", Catalog: catalog})
				input := catalog.Types["Probe"].Fields[detachedBoundField(bound)].Refinements
				if origin == "primary" {
					typ, err = primary.StructuralType()
					if err != nil {
						t.Fatal(err)
					}
					input = primary.Contract.Fields[detachedBoundField(bound)].Refinements
				}
				before := detachedSnapshot(t, typ)
				mutateDetachedBound(input, bound)
				if after := detachedSnapshot(t, typ); !bytes.Equal(before, after) {
					t.Fatalf("%s input changed returned structural evidence: before=%s after=%s", origin, before, after)
				}
			})
		}
	}
}

func TestCatalogTypeDetachmentFlowOverrideEnum(t *testing.T) {
	b := loadDetachedCatalogFixture(t)
	before := detachedSnapshot(t, b.ResolvedTypeCatalogForFlow("child"))
	view := b.ResolvedTypeCatalogForFlow("child")
	view.Enums["Choice"].Values[0] = "changed"
	if after := detachedSnapshot(t, b.ResolvedTypeCatalogForFlow("child")); !bytes.Equal(before, after) {
		t.Fatalf("incoming flow enum aliased retained declaration: before=%s after=%s", before, after)
	}
}

func TestCatalogTypeDetachmentCompiledConnection(t *testing.T) {
	for _, bound := range []string{"length_min", "length_max", "range_min", "range_max"} {
		for _, side := range []string{"source", "receiver"} {
			t.Run(bound+"/"+side, func(t *testing.T) {
				root := canonicalrouting.CopyConnectionPolicies(t, false)
				path := filepath.Join(root, "schema.yaml")
				body, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				writeFixtureFile(t, path, string(body)+"stages: {done: {final: true}}\n")
				writeFixtureFile(t, filepath.Join(root, "types.yaml"), "types:\n  Probe:\n"+bytesToIndented(detachedCatalogFields))
				repo := repoRootForContractsTest(t)
				b, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
				if err != nil {
					t.Fatal(err)
				}
				connects := b.CompositionConnects()
				if len(connects) != 2 {
					t.Fatalf("compiled connection count %d", len(connects))
				}
				for _, connect := range connects {
					input, ok, err := b.ConnectionInput(connect)
					if err != nil || !ok {
						t.Fatalf("actual connection admission: %t %v", ok, err)
					}
					before := detachedSnapshot(t, input.SourceEvidence())
					view := input.SourceEvidence().SourceType
					if side == "receiver" {
						view = input.SourceEvidence().ReceiverType
					}
					mutateDetachedBound(view.Catalog.Types["Probe"].Fields[detachedBoundField(bound)].Refinements, bound)
					if after := detachedSnapshot(t, input.SourceEvidence()); !bytes.Equal(before, after) {
						t.Fatalf("compiled edge %s/%s changed: before=%s after=%s", bound, side, before, after)
					}
				}
			})
		}
	}
}

func TestCatalogTypeDetachmentCompiledPlans(t *testing.T) {
	for _, bound := range []string{"length_min", "length_max", "range_min", "range_max"} {
		t.Run(bound+"/fanout", func(t *testing.T) {
			b := loadDetachedCatalogFixture(t)
			node := identitytest.RootNode(t, "collector")
			handler := SystemNodeEventHandler{FanOut: &FanOutSpec{
				ItemsFrom: "payload.rows", As: "row", Identity: "row.label",
				Emit: EmitSpec{Event: "audit.record"},
			}}
			if err := b.CompileFanOutHandlerPlans(node, "batch.ready", handler); err != nil {
				t.Fatal(err)
			}
			plans := b.FanOutPlansForHandler(node, "batch.ready")
			if len(plans) != 1 {
				t.Fatalf("compiled fan-out plans = %d", len(plans))
			}
			plan := plans[0]
			before := detachedSnapshot(t, plan.SemanticEvidence())
			view := plan.SemanticEvidence()
			mutateDetachedBound(detachedTypeField(t, view.ItemType, detachedBoundField(bound)).Refinements, bound)
			mutateDetachedBound(view.CollectionType.Catalog.Types["Probe"].Fields[detachedBoundField(bound)].Refinements, bound)
			fresh, ok := b.FanOutPlanForSite(plan.Site)
			if !ok || !bytes.Equal(before, detachedSnapshot(t, fresh.SemanticEvidence())) || plan.Ref != fresh.Ref {
				t.Fatal("compiled fan-out registry evidence or identity changed")
			}
		})
		t.Run(bound+"/join", func(t *testing.T) {
			b := loadDetachedCatalogFixture(t)
			node := identitytest.RootNode(t, "collector")
			join, err := b.CompileWorkflowJoinPlan(node, "batch.ready", SystemNodeEventHandler{Join: &JoinSpec{
				Stage: "done", Members: JoinMembersSpec{From: "state.members", By: "payload.label"},
				Output: "payload.result", Until: "audit.record", OnCompleteFound: true, OnComplete: HandlerRuleEntry{AdvancesTo: "done"},
			}})
			if err != nil {
				t.Fatal(err)
			}
			before := detachedSnapshot(t, join)
			mutateDetachedBound(join.Clone().ResultType.Catalog.Types["Probe"].Fields[detachedBoundField(bound)].Refinements, bound)
			mutateDetachedBound(detachedTypeField(t, join.Clone().MembersCollectionProjection.Type, detachedBoundField(bound)).Refinements, bound)
			if !bytes.Equal(before, detachedSnapshot(t, join)) {
				t.Fatal("compiled arrival join evidence changed")
			}
		})
	}
}
