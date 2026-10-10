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

// Initial construction, inactive-state faults and preview setup are separately
// inspected semantic repairs. This transformation changes only finite callers
// of the already-proven native construction/publication recipe.
func rewritePipelineDeliveryCompiledCaller(name, source string) (string, error) {
	counts := map[string]int{
		"TestPipelineCompiledOrdinaryCarrierExecutionOnBothStores":            1,
		"TestPipelineCompiledTransitionGuardDispositionOnBothStores":          2,
		"TestGuardKillUsesExactStageInEitherDeclarationOrderBothStores":       1,
		"TestPipelineCompiledTransitionStageGuardsOnBothStores":               5,
		"TestTerminalReceiverClaimFailsClosedWithoutEngineMutationBothStores": 1,
		"TestCaseDistinctReadyReceiverClaimExecutesAndSettlesBothStores":      1,
		"TestFallbackClaimSettlesWithoutBusinessTransitionBothStores":         1,
		"TestGuardRefusalClaimSettlesWithoutBusinessTransitionBothStores":     1,
	}
	want, known := counts[name]
	if !known {
		return "", fmt.Errorf("unreviewed native pipeline delivery caller %s", name)
	}
	declaration := "func " + name + "(t *testing.T)"
	if strings.Count(source, declaration) != 1 {
		return "", fmt.Errorf("native pipeline caller declaration changed: %s", name)
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "caller.go", "package probe\n"+source, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return "", fmt.Errorf("native pipeline caller source changed: %s", name)
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok || fn.Name.Name != name {
		return "", fmt.Errorf("native pipeline caller identity changed: %s", name)
	}
	count, invalid := 0, false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		callee, ok := call.Fun.(*ast.Ident)
		if !ok || callee.Name != "newCompiledAdapterFixture" {
			return true
		}
		count++
		if len(call.Args) != 6 {
			invalid = true
			return true
		}
		seed, ok := call.Args[5].(*ast.Ident)
		if !ok || (seed.Name != "true" && seed.Name != "false") {
			invalid = true
			return true
		}
		callee.Name = "newNativeCompiledAdapterFixture"
		call.Args = append(call.Args, ast.NewIdent("open"))
		return true
	})
	if invalid || count != want {
		return "", fmt.Errorf("native pipeline caller constructor closure changed: %s", name)
	}
	fn.Name.Name = "VerifyNative" + strings.TrimPrefix(name, "Test") + "ForTest"
	fn.Type.Params.List = append(fn.Type.Params.List, &ast.Field{Names: []*ast.Ident{ast.NewIdent("open")}, Type: ast.NewIdent("pipelineDeliveryNativeOpenerForTest")})
	var out bytes.Buffer
	if err := format.Node(&out, token.NewFileSet(), fn); err != nil {
		return "", err
	}
	source = out.String()
	source = strings.ReplaceAll(source, "f.pc.deliveryStore.(*pipelineTestDeliveryOwner)", "f.native.Store")
	source = strings.ReplaceAll(source, "owner.commitInitial(f.ctx, evt, route)", "f.native.PublishNode(f.ctx, evt, route)")
	return canonicalFunction(source)
}

func TestPipelineDeliveryCompiledCallerRewriteRejectsUnreviewedShape(t *testing.T) {
	const name = "TestPipelineCompiledOrdinaryCarrierExecutionOnBothStores"
	before := "func " + name + "(t *testing.T) { f := newCompiledAdapterFixture(t, backend, bundle, flow, stage, true); requireBusinessAssertions(t, f) }"
	after, err := rewritePipelineDeliveryCompiledCaller(name, before)
	if err != nil || !strings.Contains(after, "requireBusinessAssertions(t, f)") || !strings.Contains(after, "stage, true, open)") {
		t.Fatalf("finite native caller rewrite lost assertions or setup: %s/%v", after, err)
	}
	for _, input := range []string{
		strings.Replace(before, "newCompiledAdapterFixture", "foreignConstructor", 1),
		strings.Replace(before, "; requireBusinessAssertions", "; newCompiledAdapterFixture(t, backend, bundle, flow, stage, true); requireBusinessAssertions", 1),
		strings.Replace(before, "t *testing.T", "t interface{}", 1),
	} {
		if _, err := rewritePipelineDeliveryCompiledCaller(name, input); err == nil {
			t.Fatal("changed constructor/consumer shape escaped finite propagation")
		}
	}
	if _, err := rewritePipelineDeliveryCompiledCaller("foreign", before); err == nil {
		t.Fatal("unreviewed caller admitted")
	}
}

func TestPipelineDeliveryCompiledCallerRecipesPreserveAssertions(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	mechanical, semantic := 0, 0
	for _, row := range rows {
		switch row.Family {
		case "native-pipeline-delivery-compiled-callers":
			mechanical++
			want, err := rewritePipelineDeliveryCompiledCaller(row.Function, row.Before)
			got, afterErr := canonicalFunction(row.After)
			if err != nil || afterErr != nil || want != got {
				t.Fatalf("native pipeline caller did more than its finite propagation: %s/%v/%v", row.Function, err, afterErr)
			}
		case "native-pipeline-delivery-compiled-semantic":
			semantic++
			if _, err := rewritePipelineDeliveryCompiledCaller(row.Function, row.Before); err == nil {
				t.Fatalf("semantic repair was admitted as mechanical: %s", row.Function)
			}
		default:
			continue
		}
		actual, err := canonicalFunction(selectedCausalObservationBody(t, row.File, "VerifyNative"+strings.TrimPrefix(row.Function, "Test")+"ForTest"))
		want, afterErr := canonicalFunction(row.After)
		if err != nil || afterErr != nil || actual != want {
			t.Fatalf("native pipeline caller differs from its source-pinned repair: %s/%v/%v", row.Function, err, afterErr)
		}
	}
	if mechanical != 8 || semantic != 4 {
		t.Fatalf("compiled delivery caller closure = %d mechanical/%d semantic, want 8/4", mechanical, semantic)
	}
}

func TestNativePipelineExactLoopAndJoinRecipesStaySourcePinned(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-pipeline-delivery-loop-callers" && row.Family != "native-pipeline-delivery-exact-join-semantic" && row.Family != "native-pipeline-delivery-bus-semantic" {
			continue
		}
		count++
		source := []byte("package probe\n" + row.Before)
		after, changed, err := rewriteFunction(row.File, source, row)
		if err != nil || !changed {
			t.Fatalf("reviewed native replacement failed: %s/%v", row.Function, err)
		}
		if _, changed, err := rewriteFunction(row.File, after, row); err != nil || changed {
			t.Fatalf("native replacement was not idempotent: %s/%v", row.Function, err)
		}
		if _, _, err := rewriteFunction(row.File, bytes.Replace(source, []byte("{"), []byte("{ unreviewedEscape();"), 1), row); err == nil {
			t.Fatalf("changed native consumer admitted: %s", row.Function)
		}
		actual, err := canonicalFunction(selectedCausalObservationBody(t, row.File, nativePipelineRecipeReplacementName(t, row)))
		want, afterErr := canonicalFunction(row.After)
		if err != nil || afterErr != nil || actual != want {
			t.Fatalf("native semantic repair drifted: %s/%v/%v", row.Function, err, afterErr)
		}
	}
	if count != 18 {
		t.Fatalf("native exact/loop/join/bus closure recipes=%d, want 18", count)
	}
}

func nativePipelineRecipeReplacementName(t *testing.T, row recipe) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "replacement.go", "package probe\n"+row.After, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		t.Fatalf("invalid native recipe: %s/%v", row.Function, err)
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok {
		t.Fatalf("native recipe is not a function: %s", row.Function)
	}
	return fn.Name.Name
}
