package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func nativeSharedCoordinatorSource(t *testing.T, source string) string {
	t.Helper()
	fs := token.NewFileSet()
	file, err := parser.ParseFile(fs, "fixture.go", "package fixture\n"+source, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn := file.Decls[0].(*ast.FuncDecl)
	if fn.Name.Name == "newExternalRuntimeTestPipelineCoordinator" {
		fn.Type.Params.List = append(fn.Type.Params.List[:2], fn.Type.Params.List[3:]...)
	}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if ok && name.Name == "newExternalRuntimeTestPipelineCoordinator" {
			call.Args = append(call.Args[:2], call.Args[3:]...)
		}
		return true
	})
	var out bytes.Buffer
	if err := format.Node(&out, fs, fn); err != nil {
		t.Fatal(err)
	}
	value := out.String()
	if fn.Name.Name == "TestDeliveryContinuationCoordinatorRecoversNodeDeliveriesThroughCanonicalSelectedStore" {
		value = strings.ReplaceAll(value, "(context.Context, *sql.DB, nodeDeliveryRecoveryStore)", "(context.Context, nodeDeliveryRecoveryStore)")
		value = strings.Replace(value, "\t\t\t\t_, db, cleanup := testutil.StartPostgres(t)\n\t\t\t\tt.Cleanup(cleanup)\n\t\t\t\tselected := storetest.AdmitPostgresRuntimeStore(t, db)", "\t\t\t\tselected := storetest.StartPostgresRuntimeStore(t)", 1)
		value = strings.Replace(value, "return ctx, db, selected", "return ctx, selected", 1)
		value = strings.Replace(value, "return ctx, storetest.Database(selected), selected", "return ctx, selected", 1)
		value = strings.Replace(value, "ctx, db, selected := backend.setup(t)", "ctx, selected := backend.setup(t)", 1)
	}
	return value
}

func q10SharedCoordinatorCurrentSource(row recipe) (string, bool, error) {
	if row.File != "internal/runtime/final_flow_instance_authoring_runtime_test.go" || row.Function != "TestFinalFlowInstanceAuthoringRuntime_PublishActivatesAndExecutesSelectedTemplateInstance" {
		return "", false, nil
	}
	const original = "\t\tSELECT COUNT(*) FROM entity_state\n\t\tWHERE flow_instance LIKE 'account/%'\n\t\t  AND current_state = 'reviewed'\n\t\t  AND fields @> $1::jsonb\n"
	const current = "\t\tSELECT COUNT(*) FROM entity_state es JOIN flow_instances fi\n\t\t  ON fi.run_id=es.run_id AND fi.instance_path=es.flow_instance AND fi.entity_id=es.entity_id\n\t\tWHERE es.flow_instance LIKE 'account/%'\n\t\t  AND fi.current_state = 'reviewed'\n\t\t  AND es.fields @> $1::jsonb\n"
	if row.Family != "native-shared-coordinator-authority" || strings.Count(row.Successor, original) != 1 {
		return "", true, fmt.Errorf("missing exact original account stage/field readback")
	}
	return strings.Replace(row.Successor, original, current, 1), true, nil
}

func TestNativeSharedCoordinatorRemovesOnlyUnusedAuthorityAcrossEveryCaller(t *testing.T) {
	packet := loadReviewedSuccessors2617(t)
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched, calls := 0, 0
	for _, row := range rows {
		if row.Family != "native-shared-coordinator-authority" {
			continue
		}
		matched++
		current := row
		if source, found, err := q10SharedCoordinatorCurrentSource(row); err != nil {
			t.Fatal(err)
		} else if found {
			row.Successor = source
		}
		row = historicalMechanicalRecipe(t, row)
		want, err := canonicalFunction(nativeSharedCoordinatorSource(t, row.Before))
		got, afterErr := canonicalFunction(row.After)
		body := selectedCausalObservationBody(t, row.File, row.Function)
		actual, sourceErr := canonicalFunction(body)
		currentBody, successorErr := reviewedSharedCoordinatorCurrentBody2617(current, body, packet)
		currentAfter, currentErr := canonicalFunction(currentBody)
		if err != nil || afterErr != nil || sourceErr != nil || successorErr != nil || currentErr != nil || want != got || actual != currentAfter {
			t.Fatalf("%s changed outside unused authority propagation: %v", row.Function, successorErr)
		}
		if current.Successor == "" {
			unlisted := current
			unlisted.Successor = strings.Replace(body, "{", "{ unreviewedSetup();", 1)
			if _, err := r52SharedCoordinatorCurrentBody(unlisted, unlisted.Successor); err == nil {
				t.Fatal("unlisted source plus Successor pair accepted")
			}
		}
		fn := projectionShapeFunction(t, row.After)
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			name, ok := call.Fun.(*ast.Ident)
			if ok && name.Name == "newExternalRuntimeTestPipelineCoordinator" {
				calls++
				if len(call.Args) != 4 {
					t.Fatal("shared coordinator still accepts raw authority")
				}
			}
			return true
		})
		for _, cut := range []string{"opts.DecisionCards = owner", "opts.DeliveryRuntime = bus", "t.Fatal(", "t.Fatalf(", "case <-", "selected,"} {
			mutant := strings.Replace(row.After, cut, "unreviewedCut", 1)
			if mutant == row.After {
				continue
			}
			changed, mutantErr := canonicalFunction(mutant)
			if mutantErr == nil && changed == want {
				t.Fatalf("lost owner/workload assertion accepted: %s", cut)
			}
		}
	}
	if matched != 18 || calls != 20 {
		t.Fatalf("shared coordinator coverage=%d roots/%d calls,want18/20", matched, calls)
	}
}

func TestQ10SharedCoordinatorReadbackRetainsExactIdentityAndLifecycle(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		current, found, err := q10SharedCoordinatorCurrentSource(row)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			continue
		}
		matched++
		guard := recipe{File: row.File, Function: row.Function, Before: current, After: current}
		for _, edit := range [][2]string{
			{"fi.run_id=es.run_id AND ", ""}, {"fi.instance_path=es.flow_instance AND ", ""},
			{"fi.entity_id=es.entity_id", "fi.entity_id=fi.entity_id"},
			{"fi.current_state = 'reviewed'", "es.current_state = 'reviewed'"},
			{"es.fields @> $1::jsonb", "true"}, {"es.flow_instance LIKE 'account/%'", "true"},
			{"`, 1, `", "`, 0, `"}, {"loaded.CurrentState != \"reviewed\"", "false"},
			{"loaded.Fields[\"score\"] != \"91\"", "false"},
		} {
			mutant := strings.Replace(current, edit[0], edit[1], 1)
			if mutant == current {
				t.Fatalf("missing exact account readback control: %q", edit[0])
			}
			if _, changed, err := rewriteFunction(row.File, []byte("package proof\n"+mutant), guard); err == nil || changed {
				t.Fatalf("account readback lost identity, stage, fields or cardinality: %q", edit[0])
			}
		}
		if _, changed, err := rewriteFunction(row.File, []byte("package proof\n"+row.Successor), guard); err == nil || changed {
			t.Fatal("retired data-row stage readback accepted as current")
		}
	}
	if matched != 1 {
		t.Fatalf("Q10 coordinator readbacks=%d, want1", matched)
	}
}
