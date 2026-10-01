package apiv1

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func dataShowWire(t *testing.T, handler *Handler, params map[string]any) map[string]any {
	t.Helper()
	status, response, body := callReadOnlyProbeRPC(t, handler, "data.show", params, "Bearer "+testToken)
	if status != 200 || response.Error != nil {
		t.Fatalf("data.show %#v: status=%d error=%#v body=%s", params, status, response.Error, body)
	}
	return asMap(t, response.Result)
}

func assertDataShowWire(t *testing.T, got map[string]any, expected any) {
	t.Helper()
	raw, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]any
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("wire=%#v, want %#v", got, want)
	}
}

func assertDataShowEvidence(t *testing.T, handler *Handler, params map[string]any, expected any) {
	t.Helper()
	all := []any{}
	seen := map[string]bool{}
	for {
		wire := dataShowWire(t, handler, params)
		items := asSlice(t, wire["items"])
		raw, err := canonicaljson.Bytes(items)
		if err != nil || wire["item_count"] != float64(len(items)) || wire["encoded_items_bytes"] != float64(len(raw)) || len(items) > 1 {
			t.Fatalf("page accounting=%#v/%v", wire, err)
		}
		all = append(all, items...)
		continuation := asMap(t, wire["continuation"])
		if continuation["state"] == "end" {
			break
		}
		cursor, ok := continuation["cursor"].(string)
		if !ok || cursor == "" || seen[cursor] || len(cursor) > 4096 {
			t.Fatalf("invalid continuation=%#v", continuation)
		}
		seen[cursor] = true
		params["page"] = map[string]any{"limit": 1, "cursor": cursor}
	}
	raw, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	want := []any{}
	if string(raw) != "null" {
		if err := json.Unmarshal(raw, &want); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(all, want) {
		t.Fatalf("evidence=%#v, want %#v", all, want)
	}
}

func dataShowSource(t *testing.T, selected DurableDataStore, bundle string, ref durabledata.DeclarationRef, expected durabledata.ExpectedHead, operation, input string) durabledata.SourceOperationResult {
	t.Helper()
	result, err := selected.ExecuteDataSourceOperation(context.Background(), durabledata.SourceCommand{Operation: operation, SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: bundle, Declaration: ref, ExpectedHead: expected, InputFormat: "jsonl", Input: []byte(input)})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func dataShowReadOnlyHandlers(t *testing.T, fixture dataRunLifecycleFixture) map[string]MethodHandler {
	t.Helper()
	snapshot := func() map[string][]string {
		t.Helper()
		state := map[string][]string{}
		for _, table := range []string{"resource_declarations", "resource_bundle_declarations", "resource_bundle_import_shapes", "resource_heads", "resource_head_history", "resource_versions", "resource_version_provenance", "resource_version_pins", "resource_source_invocations", "resource_prune_invocations", "resource_prune_pin_evidence", "resource_run_creation_operations", "resource_run_creation_child_evaluations", "resource_run_creation_child_reservations"} {
			func() {
				rows, err := fixture.db.QueryContext(context.Background(), "SELECT * FROM "+table)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				columns, err := rows.Columns()
				if err != nil {
					t.Fatal(err)
				}
				for rows.Next() {
					values := make([]any, len(columns))
					pointers := make([]any, len(columns))
					for i := range values {
						pointers[i] = &values[i]
					}
					if err := rows.Scan(pointers...); err != nil {
						t.Fatal(err)
					}
					state[table] = append(state[table], fmt.Sprintf("%#v", values))
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				sort.Strings(state[table])
			}()
		}
		return state
	}
	handlers := OperatorDataHandlers(DataHandlerOptions{Store: fixture.reconstructed})
	show := handlers["data.show"]
	handlers["data.show"] = func(ctx context.Context, req Request) (any, error) {
		before := snapshot()
		result, err := show(ctx, req)
		if after := snapshot(); !reflect.DeepEqual(before, after) {
			t.Fatalf("data.show mutated durable facts: params=%#v", req.Params)
		}
		return result, err
	}
	return handlers
}

func TestDataShowReadFamilyAcrossSelectedStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		ctx := context.Background()
		catalog, ref := dataHTTPProbeCatalog(t)
		for _, keyType := range []string{"boolean", "number"} {
			keyRef, err := durabledata.ParseDeclarationRef(".", keyType+".observed")
			if err != nil {
				t.Fatal(err)
			}
			schema := map[string]any{"type": "object", "additionalProperties": false, "required": []string{"key"}, "properties": map[string]any{"key": map[string]any{"type": keyType}}}
			compiled, defects := durabledata.CompileJSONL(keyRef, schema, "key", nil)
			if len(defects) != 0 {
				t.Fatal(defects)
			}
			catalog.Declarations = append(catalog.Declarations, durabledata.Declaration{Name: keyRef.EventName, Ref: keyRef, BusinessKey: "key", SchemaDigest: compiled.Manifest.SchemaDigest, CanonicalSchema: compiled.CanonicalSchema})
			catalog.ImportShapes = append(catalog.ImportShapes, durabledata.ImportShape{BundleHash: dataProbeBundleHash, Declaration: keyRef, SchemaDigest: compiled.Manifest.SchemaDigest, BusinessKey: "key", Fields: []durabledata.ImportShapeField{{Name: "key", Required: true, Text: false}}})
		}
		emptyRef, _ := durabledata.ParseDeclarationRef(".", "empty.loaded")
		emptyCompiled, defects := durabledata.CompileJSONL(emptyRef, map[string]any{"type": "object", "additionalProperties": false}, "", nil)
		if len(defects) != 0 {
			t.Fatal(defects)
		}
		catalog.Declarations = append(catalog.Declarations, durabledata.Declaration{Name: emptyRef.EventName, Ref: emptyRef, SchemaDigest: emptyCompiled.Manifest.SchemaDigest, CanonicalSchema: emptyCompiled.CanonicalSchema})
		catalog.ImportShapes = append(catalog.ImportShapes, durabledata.ImportShape{BundleHash: dataProbeBundleHash, Declaration: emptyRef, SchemaDigest: emptyCompiled.Manifest.SchemaDigest, Fields: []durabledata.ImportShapeField{}})
		sort.Slice(catalog.Declarations, func(i, j int) bool {
			return durabledata.CompareDeclarationRef(catalog.Declarations[i].Ref, catalog.Declarations[j].Ref) < 0
		})
		sort.Slice(catalog.ImportShapes, func(i, j int) bool {
			return durabledata.CompareDeclarationRef(catalog.ImportShapes[i].Declaration, catalog.ImportShapes[j].Declaration) < 0
		})
		if _, err := fixture.primary.EnsureSourceArtifactWithData(ctx, dataProbeSourceArtifact, catalog); err != nil {
			t.Fatal(err)
		}
		handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: dataShowReadOnlyHandlers(t, fixture)})
		first := dataShowSource(t, fixture.primary, dataProbeBundleHash, ref, durabledata.AbsentHead(), "import", "{\"slug\":\"alpha\"}\n{\"slug\":\"beta\"}\n")
		second := dataShowSource(t, fixture.primary, dataProbeBundleHash, ref, first.Head.After, "import", "{\"slug\":\"gamma\"}\n")
		before, err := fixture.primary.ListDataDeclarationSummaries(ctx, dataProbeBundleHash)
		if err != nil {
			t.Fatal(err)
		}
		t.Run("declarations", func(t *testing.T) {
			wire := dataShowWire(t, handler, map[string]any{"view": "declarations", "bundle_hash": dataProbeBundleHash, "page": map[string]any{}})
			items := asSlice(t, wire["items"])
			if len(items) != len(before) || wire["item_count"] != float64(len(before)) || asMap(t, wire["continuation"])["state"] != "end" {
				t.Fatalf("inventory=%#v", wire)
			}
			for i, item := range before {
				assertDataShowWire(t, asMap(t, items[i]), item)
			}
		})
		t.Run("import_shape", func(t *testing.T) {
			shape, err := fixture.primary.GetDeclarationImportShape(ctx, dataProbeBundleHash, ref)
			if err != nil {
				t.Fatal(err)
			}
			assertDataShowWire(t, dataShowWire(t, handler, map[string]any{"view": "import_shape", "bundle_hash": dataProbeBundleHash, "declaration": dataRunDeclaration(ref), "schema_digest": string(shape.SchemaDigest)}), shape)
			wire := dataShowWire(t, handler, map[string]any{"view": "import_shape", "bundle_hash": dataProbeBundleHash, "declaration": dataRunDeclaration(emptyRef), "schema_digest": emptyCompiled.Manifest.SchemaDigest})
			if fields, ok := wire["fields"].([]any); !ok || len(fields) != 0 {
				t.Fatalf("fieldless shape=%#v", wire)
			}
			versions := dataShowWire(t, handler, map[string]any{"view": "versions", "declaration": dataRunDeclaration(emptyRef), "page": map[string]any{}})
			if len(asSlice(t, versions["items"])) != 0 {
				t.Fatalf("empty version inventory=%#v", versions)
			}
		})
		for _, view := range []string{"versions", "head_history"} {
			t.Run(view, func(t *testing.T) {
				params := dataShowResourceParams(view)
				wire := dataShowWire(t, handler, params)
				items := asSlice(t, wire["items"])
				if len(items) != 1 || asMap(t, wire["continuation"])["state"] != "more" {
					t.Fatalf("first page=%#v", wire)
				}
				if view == "versions" && asMap(t, items[0])["alias"] != "v1" {
					t.Fatalf("first alias=%#v", items)
				}
				if view == "head_history" && asMap(t, items[0])["revision"] != float64(1) {
					t.Fatalf("first revision=%#v", items)
				}
				params["page"] = map[string]any{"limit": 1, "cursor": asMap(t, wire["continuation"])["cursor"]}
				wire = dataShowWire(t, handler, params)
				items = asSlice(t, wire["items"])
				if len(items) != 1 || asMap(t, wire["continuation"])["state"] != "end" {
					t.Fatalf("last page=%#v", wire)
				}
				if view == "versions" && asMap(t, items[0])["alias"] != "v2" {
					t.Fatalf("last alias=%#v", items)
				}
				if view == "head_history" && (asMap(t, items[0])["revision"] != float64(2) || asMap(t, asMap(t, items[0])["operation_ref"])["source_invocation_id"] != second.SourceInvocationID) {
					t.Fatalf("last history=%#v", items)
				}
			})
		}
		t.Run("version", func(t *testing.T) {
			for _, selector := range []map[string]any{{"kind": "head"}, {"kind": "alias", "alias": "v1"}, {"kind": "version", "version_id": first.Candidate.VersionID}} {
				params := dataShowResourceParams("version")
				params["selector"] = selector
				wire := dataShowWire(t, handler, params)
				want := first.Candidate.VersionID
				if selector["kind"] == "head" {
					want = second.Candidate.VersionID
				}
				if wire["version_id"] != string(want) {
					t.Fatalf("selector %#v returned %#v", selector, wire)
				}
			}
		})
		for _, view := range []string{"rows", "row", "export_chunk", "provenance", "pins"} {
			t.Run(view, func(t *testing.T) {
				params := dataShowResourceParams(view)
				params["selector"] = map[string]any{"kind": "version", "version_id": first.Candidate.VersionID}
				wire := dataShowWire(t, handler, params)
				switch view {
				case "row":
					if wire["key"] != "alpha" || wire["ordinal"] != float64(1) || asMap(t, wire["value"])["slug"] != "alpha" {
						t.Fatalf("row=%#v", wire)
					}
				case "rows":
					if asMap(t, asSlice(t, wire["items"])[0])["key"] != "alpha" {
						t.Fatalf("rows=%#v", wire)
					}
					params["page"] = map[string]any{"limit": 1, "cursor": asMap(t, wire["continuation"])["cursor"]}
					last := dataShowWire(t, handler, params)
					if asMap(t, asSlice(t, last["items"])[0])["key"] != "beta" || asMap(t, last["continuation"])["state"] != "end" {
						t.Fatalf("last rows=%#v", last)
					}
				case "export_chunk":
					decoded, err := base64.StdEncoding.DecodeString(wire["chunk_base64"].(string))
					if err != nil || string(decoded) != "{\"slug\":\"alpha\"}\n" || wire["first_ordinal"] != float64(1) || wire["row_count"] != float64(1) {
						t.Fatalf("export=%#v/%v", wire, err)
					}
				case "provenance":
					items := asSlice(t, wire["items"])
					if len(items) != 1 || asMap(t, items[0])["sequence"] != nil || asMap(t, asMap(t, items[0])["producer_ref"])["source_invocation_id"] != first.SourceInvocationID {
						t.Fatalf("provenance=%#v", wire)
					}
				case "pins":
					if len(asSlice(t, wire["items"])) != 0 || asMap(t, wire["continuation"])["state"] != "end" {
						t.Fatalf("unpinned=%#v", wire)
					}
				}
			})
		}
		after, err := fixture.primary.ListDataDeclarationSummaries(ctx, dataProbeBundleHash)
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("reads mutated inventory: %#v/%v", after, err)
		}
		t.Run("typed_row_keys", func(t *testing.T) {
			for _, test := range []struct {
				kind, input string
				key         any
			}{{"boolean", "{\"key\":false}\n{\"key\":true}\n", true}, {"number", "{\"key\":1}\n{\"key\":2}\n", 2}} {
				ref, _ := durabledata.ParseDeclarationRef(".", test.kind+".observed")
				result := dataShowSource(t, fixture.primary, dataProbeBundleHash, ref, durabledata.AbsentHead(), "import", test.input)
				wire := dataShowWire(t, handler, map[string]any{"view": "row", "declaration": dataRunDeclaration(ref), "selector": map[string]any{"kind": "version", "version_id": result.Candidate.VersionID}, "row_selector": map[string]any{"key": test.key}})
				if !reflect.DeepEqual(asMap(t, wire["value"])["key"], map[string]any{"boolean": true, "number": float64(2)}[test.kind]) {
					t.Fatalf("typed key=%#v", wire)
				}
			}
		})
		t.Run("keyless_and_empty", func(t *testing.T) {
			keylessRef, _ := durabledata.ParseDeclarationRef(".", "score.observed")
			result := dataShowSource(t, fixture.primary, dataProbeBundleHash, keylessRef, durabledata.AbsentHead(), "import", "{\"label\":\"same\"}\n{\"label\":\"other\"}\n{\"label\":\"same\"}\n")
			params := map[string]any{"view": "row", "declaration": dataRunDeclaration(keylessRef), "selector": map[string]any{"kind": "head"}, "row_selector": map[string]any{"position": 3}}
			wire := dataShowWire(t, handler, params)
			if wire["ordinal"] != float64(3) || wire["key"] != nil || asMap(t, wire["value"])["label"] != "same" {
				t.Fatalf("ordinal multiplicity=%#v", wire)
			}
			for _, row := range []map[string]any{{"position": 0}, {"position": 99}, {"key": "same"}, {"position": 1, "key": "same"}} {
				params["row_selector"] = row
				_, response, _ := callReadOnlyProbeRPC(t, handler, "data.show", params, "Bearer "+testToken)
				if response.Error == nil {
					t.Fatalf("bad row selector admitted: %#v", row)
				}
			}
			params = map[string]any{"view": "rows", "declaration": dataRunDeclaration(keylessRef), "selector": map[string]any{"kind": "head"}, "page": map[string]any{"limit": 1}}
			assertDataShowEvidence(t, handler, params, []durabledata.RowDTO{
				{Declaration: keylessRef, VersionID: result.Candidate.VersionID, Ordinal: 1, Value: map[string]any{"label": "same"}},
				{Declaration: keylessRef, VersionID: result.Candidate.VersionID, Ordinal: 2, Value: map[string]any{"label": "other"}},
				{Declaration: keylessRef, VersionID: result.Candidate.VersionID, Ordinal: 3, Value: map[string]any{"label": "same"}},
			})
			empty := dataShowSource(t, fixture.primary, dataProbeBundleHash, keylessRef, result.Head.After, "import", "")
			for _, view := range []string{"rows", "export_chunk"} {
				params["view"] = view
				params["page"] = map[string]any{}
				wire := dataShowWire(t, handler, params)
				if asMap(t, wire["continuation"])["state"] != "end" {
					t.Fatalf("empty continuation=%#v", wire)
				}
				if view == "rows" {
					if len(asSlice(t, wire["items"])) != 0 {
						t.Fatal(wire)
					}
				} else if wire["row_count"] != float64(0) || wire["first_ordinal"] != float64(0) || wire["chunk_base64"] != "" || wire["version_id"] != string(empty.Candidate.VersionID) {
					t.Fatalf("empty export=%#v", wire)
				}
			}
		})
		t.Run("pruned", func(t *testing.T) {
			result, err := fixture.primary.PruneDataResource(ctx, durabledata.PruneCommand{PruneInvocationID: uuid.NewString(), Actor: "operator", Declaration: ref, VersionID: first.Candidate.VersionID, ExpectedHead: second.Head.After})
			if err != nil || result.Outcome != "pruned" {
				t.Fatalf("prune=%#v/%v", result, err)
			}
			for _, view := range []string{"version", "provenance", "pins"} {
				params := dataShowResourceParams(view)
				params["selector"] = map[string]any{"kind": "version", "version_id": first.Candidate.VersionID}
				dataShowWire(t, handler, params)
			}
			for _, view := range []string{"row", "rows", "export_chunk"} {
				params := dataShowResourceParams(view)
				params["selector"] = map[string]any{"kind": "version", "version_id": first.Candidate.VersionID}
				status, response, _ := callReadOnlyProbeRPC(t, handler, "data.show", params, "Bearer "+testToken)
				if status != 200 || response.Error == nil || asMap(t, response.Error.Data)["code"] != string(durabledata.CodePayloadPruned) {
					t.Fatalf("pruned %s=%#v", view, response)
				}
			}
		})
		t.Run("auth", func(t *testing.T) {
			status, response, _ := callReadOnlyProbeRPC(t, handler, "data.show", dataShowResourceParams("version"), "Bearer wrong")
			if status == 200 && response.Error == nil {
				t.Fatal("unauthorized read admitted")
			}
		})
		t.Run("malformed_and_bounded", func(t *testing.T) {
			for _, view := range []string{"versions", "rows", "export_chunk"} {
				params := dataShowResourceParams(view)
				params["selector"] = map[string]any{"kind": "head"}
				if view == "versions" {
					delete(params, "selector")
				}
				params["page"] = map[string]any{"byte_limit": 1}
				_, response, _ := callReadOnlyProbeRPC(t, handler, "data.show", params, "Bearer "+testToken)
				if response.Error == nil {
					t.Fatalf("impossible byte budget admitted for %s", view)
				}
				params["unknown"] = true
				_, response, _ = callReadOnlyProbeRPC(t, handler, "data.show", params, "Bearer "+testToken)
				if response.Error == nil || response.Error.Code != codeInvalidParams {
					t.Fatalf("unknown field admitted for %s: %#v", view, response)
				}
			}
		})
	})
}

func TestDataShowOperationReadFamilyAcrossSelectedStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		ctx := context.Background()
		catalog, ref := dataHTTPProbeCatalog(t)
		compiled, defects := durabledata.CompileJSONL(ref, map[string]any{"type": "object", "additionalProperties": false, "required": []string{"slug"}, "properties": map[string]any{"slug": map[string]any{"type": "string"}, "payload": map[string]any{"type": "string"}}}, "slug", nil)
		if len(defects) != 0 {
			t.Fatal(defects)
		}
		for i := range catalog.Declarations {
			if catalog.Declarations[i].Ref == ref {
				catalog.Declarations[i].SchemaDigest, catalog.Declarations[i].CanonicalSchema = compiled.Manifest.SchemaDigest, compiled.CanonicalSchema
			}
		}
		for i := range catalog.ImportShapes {
			if catalog.ImportShapes[i].Declaration == ref {
				catalog.ImportShapes[i].SchemaDigest = compiled.Manifest.SchemaDigest
				catalog.ImportShapes[i].Fields = []durabledata.ImportShapeField{{Name: "payload", Text: true}, {Name: "slug", Required: true, Text: true}}
			}
		}
		if _, err := fixture.primary.EnsureSourceArtifactWithData(ctx, dataProbeSourceArtifact, catalog); err != nil {
			t.Fatal(err)
		}
		handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: dataShowReadOnlyHandlers(t, fixture)})
		first := dataShowSource(t, fixture.primary, dataProbeBundleHash, ref, durabledata.AbsentHead(), "import", "{\"slug\":\"alpha\",\"payload\":\"before\"}\n{\"slug\":\"beta\"}\n")
		second := dataShowSource(t, fixture.primary, dataProbeBundleHash, ref, first.Head.After, "import", "{\"slug\":\"alpha\",\"payload\":\"after\"}\n{\"slug\":\"gamma\"}\n")
		check := dataShowSource(t, fixture.primary, dataProbeBundleHash, ref, second.Head.After, "check", "{\"slug\":\"delta\"}\n")
		rejected := dataShowSource(t, fixture.primary, dataProbeBundleHash, ref, second.Head.After, "check", "{\"unknown\":true}\n")
		conflict := dataShowSource(t, fixture.primary, dataProbeBundleHash, ref, durabledata.AbsentHead(), "import", "{\"slug\":\"stale\"}\n")
		for _, result := range []durabledata.SourceOperationResult{first, second, check, rejected, conflict} {
			record, err := fixture.primary.LoadDataSourceOperation(ctx, result.SourceInvocationID)
			if err != nil {
				t.Fatal(err)
			}
			for _, detail := range []string{"summary", "defects", "delta_added", "delta_removed", "delta_changed"} {
				t.Run("source/"+result.Operation+"/"+result.Outcome+"/"+detail, func(t *testing.T) {
					wire := dataShowWire(t, handler, dataShowOperationParams("source", result.SourceInvocationID, detail))
					if detail == "summary" {
						summary := durabledata.SummarizeSource(record)
						assertDataShowWire(t, wire, durabledata.OperationSummary{Kind: "source", Source: &summary})
						return
					}
					expected := map[string]any{"defects": record.Evidence.Defects, "delta_added": record.Evidence.DeltaAdded, "delta_removed": record.Evidence.DeltaRemoved, "delta_changed": record.Evidence.DeltaChanged}[detail]
					assertDataShowEvidence(t, handler, dataShowOperationParams("source", result.SourceInvocationID, detail), expected)
				})
			}
		}
		for _, test := range []struct {
			version durabledata.VersionID
			head    durabledata.ExpectedHead
			outcome string
		}{
			{first.Candidate.VersionID, second.Head.After, "pruned"},
			{first.Candidate.VersionID, second.Head.After, "already_pruned"},
			{first.Candidate.VersionID, durabledata.AbsentHead(), "head_conflict"},
			{second.Candidate.VersionID, second.Head.After, "refused_current"},
			{check.Candidate.VersionID, second.Head.After, "rejected"},
		} {
			prune, err := fixture.primary.PruneDataResource(ctx, durabledata.PruneCommand{PruneInvocationID: uuid.NewString(), Actor: "operator", Declaration: ref, VersionID: test.version, ExpectedHead: test.head})
			if err != nil || prune.Outcome != test.outcome {
				t.Fatalf("prune %s=%#v/%v", test.outcome, prune, err)
			}
			for _, detail := range []string{"summary", "defects"} {
				t.Run("prune/"+prune.Outcome+"/"+detail, func(t *testing.T) {
					params := dataShowOperationParams("prune", prune.PruneInvocationID, detail)
					if detail == "defects" && prune.Defects == nil {
						status, response, _ := callReadOnlyProbeRPC(t, handler, "data.show", params, "Bearer "+testToken)
						if status != 200 || response.Error == nil || response.Error.Code != codeInvalidParams {
							t.Fatalf("absent defects=%#v", response)
						}
						return
					}
					wire := dataShowWire(t, handler, params)
					if detail == "summary" {
						assertDataShowWire(t, wire, durabledata.OperationSummary{Kind: "prune", Prune: &prune})
					} else {
						assertDataShowEvidence(t, handler, params, prune.Defects.Items)
					}
				})
			}
		}
		// Use the actual run-creation owner to populate receipts and pins, not a DTO mock.
		runCatalog, scanRef, _ := dataRunLifecycleCatalog(t, runStartTestBundleHash, false)
		if err := registerDataRunLifecycleCatalog(ctx, fixture.primary, runCatalog); err != nil {
			t.Fatal(err)
		}
		source := semanticview.Wrap(runStartTestBundle("scan.requested"))
		bus, err := newScopedAPITestEventBus(t, fixture.primary, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatal(err)
		}
		if err := registerDataRunLifecycleCatalog(ctx, fixture.primary, runCatalog); err != nil {
			t.Fatal(err)
		}
		publisher := eventPublishTestHandlerWithStores(t, fixture.primary, fixture.primary, fixture.primary, bus, source)
		for _, accepted := range []bool{true, false} {
			runID := uuid.NewString()
			expected := durabledata.AbsentHead()
			input := "{\"topic\":\"private-payload\"}\n"
			if !accepted {
				input = "{\"unknown\":true}\n"
				summary, err := fixture.primary.ResolveDataVersionSummary(ctx, scanRef, durabledata.VersionSelector{Kind: "head"})
				if err != nil {
					t.Fatal(err)
				}
				expected = durabledata.VersionHead(summary.VersionID)
			}
			data := map[string]any{"imports": []any{dataRunFusedImport(uuid.NewString(), scanRef, expected, []byte(input))}, "pins": []any{}}
			response := rpcCall(t, publisher, dataRunEventPublishBody(runID, uuid.NewString(), data))
			if (response.Error == nil) != accepted {
				t.Fatalf("create accepted=%v: %#v", accepted, response)
			}
			record, err := fixture.primary.LoadDataRunCreationOperation(ctx, runID)
			if err != nil {
				t.Fatal(err)
			}
			for _, detail := range []string{"summary", "request_binding", "child_evaluations", "child_defects", "run_binding"} {
				t.Run(fmt.Sprintf("run_creation/%v/%s", accepted, detail), func(t *testing.T) {
					wire := dataShowWire(t, handler, dataShowOperationParams("run_creation", runID, detail))
					if detail == "summary" {
						assertDataShowWire(t, wire, durabledata.OperationSummary{Kind: "run_creation", RunCreation: &record.Summary})
					} else if detail == "request_binding" {
						assertDataShowWire(t, wire, record.RequestBinding)
						raw, _ := json.Marshal(wire)
						if strings.Contains(string(raw), "private-payload") || strings.Contains(string(raw), "actor") || strings.Contains(string(raw), "content_base64") {
							t.Fatalf("binding exposed payload: %s", raw)
						}
					} else {
						expected := map[string]any{"child_evaluations": record.Evidence.ChildEvaluations, "child_defects": record.Evidence.ChildDefects, "run_binding": record.Evidence.RunBinding}[detail]
						assertDataShowEvidence(t, handler, dataShowOperationParams("run_creation", runID, detail), expected)
					}
				})
			}
			if accepted {
				var pin *durabledata.Pin
				for _, item := range record.Evidence.RunBinding {
					if item.Pin != nil {
						pin = item.Pin
						break
					}
				}
				if pin == nil {
					t.Fatal("accepted receipt has no pin")
				}
				// Move the current head so the separate pinned refusal is reachable.
				newHead := dataShowSource(t, fixture.primary, runStartTestBundleHash, scanRef, durabledata.VersionHead(pin.VersionID), "import", "{\"topic\":\"next\"}\n")
				prune, err := fixture.primary.PruneDataResource(ctx, durabledata.PruneCommand{PruneInvocationID: uuid.NewString(), Actor: "operator", Declaration: scanRef, VersionID: pin.VersionID, ExpectedHead: newHead.Head.After})
				if err != nil || prune.Outcome != "refused_pinned" {
					t.Fatalf("pinned prune=%#v/%v", prune, err)
				}
				t.Run("prune/pins", func(t *testing.T) {
					assertDataShowWire(t, dataShowWire(t, handler, dataShowOperationParams("prune", prune.PruneInvocationID, "summary")), durabledata.OperationSummary{Kind: "prune", Prune: &prune})
					wire := dataShowWire(t, handler, dataShowOperationParams("prune", prune.PruneInvocationID, "pins"))
					items := asSlice(t, wire["items"])
					if len(items) != 1 || asMap(t, items[0])["run_id"] != runID {
						t.Fatalf("pin evidence=%#v", wire)
					}
					pins, err := fixture.primary.LoadDataPruneOperationPins(ctx, prune.PruneInvocationID)
					if err != nil {
						t.Fatal(err)
					}
					assertDataShowEvidence(t, handler, dataShowOperationParams("prune", prune.PruneInvocationID, "pins"), pins)
				})
			}
		}
	})
}
