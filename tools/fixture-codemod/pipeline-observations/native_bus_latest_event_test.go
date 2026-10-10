package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

func nativeNestedLatestEventReadsPreserved(source string) bool {
	file, err := parser.ParseFile(token.NewFileSet(), "proof.go", "package proof\n"+source, 0)
	if err != nil {
		return false
	}
	want := map[string]int{"step.begin": 1, "child/micro.start": 1, "child/grandchild/micro.done": 1, "child/micro.relayed": 1}
	valid := true
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "ReadLatestNamedEventIdentityStorage" {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok || pkg.Name != "storetest" || len(call.Args) != 4 {
			valid = false
			return true
		}
		ctx, ctxOK := call.Args[0].(*ast.Ident)
		owner, ownerOK := call.Args[1].(*ast.Ident)
		name, nameOK := call.Args[2].(*ast.BasicLit)
		exclusion, exclusionOK := call.Args[3].(*ast.BasicLit)
		if !ctxOK || ctx.Name != "ctx" || !ownerOK || owner.Name != "pg" || !nameOK || !exclusionOK || exclusion.Value != `""` {
			valid = false
			return true
		}
		value, err := strconv.Unquote(name.Value)
		if err != nil || want[value] != 1 {
			valid = false
			return true
		}
		want[value]--
		return true
	})
	for _, remaining := range want {
		valid = valid && remaining == 0
	}
	return valid
}

func TestNativeBusLatestEventReuseRetainsGlobalScopeAndAllFourReads(t *testing.T) {
	source := selectedCausalObservationBody(t, "internal/runtime/bus/eventbus_publish_test.go", "TestEventBusPublish_NestedThreeLevelConnectChainExecutesEndToEnd")
	if !nativeNestedLatestEventReadsPreserved(source) {
		t.Fatal("nested latest lookup changed original owner, context, name or scope")
	}
	for _, pair := range [][2]string{
		{"ReadLatestNamedEventIdentityStorage(ctx, pg,", "ReadLatestNamedEventIdentityStorage(context.Background(), pg,"},
		{"ReadLatestNamedEventIdentityStorage(ctx, pg,", "ReadLatestNamedEventIdentityStorage(ctx, foreignStore,"},
		{`"step.begin", ""`, `"wrong.event", ""`},
		{`"step.begin", ""`, `"step.begin", "hidden-event"`},
	} {
		mutant := strings.Replace(source, pair[0], pair[1], 1)
		if mutant == source || nativeNestedLatestEventReadsPreserved(mutant) {
			t.Fatalf("weakened latest lookup passed: %v", pair)
		}
	}
	owner := selectedCausalObservationBody(t, "internal/store/internal/runtimepersistence/test_api_event_storage.go", "ReadLatestNamedEventIdentityStorageForTest")
	if !strings.Contains(owner, "`SELECT CAST(event_id AS TEXT) FROM events WHERE event_name=$1 AND CAST(event_id AS TEXT)<>$2 ORDER BY created_at DESC LIMIT 1`") ||
		!strings.Contains(owner, "readServedDeliveryObservation(ctx, selected,") ||
		!strings.Contains(owner, "eventName, excludedEventID).Scan(&eventID)") {
		t.Fatal("existing latest owner changed its exact physical scope/order")
	}
}
