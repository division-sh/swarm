package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var family145NativeBusRoutingCalls = map[string][]string{
	"TestEventBusPublish_AgentOnlyConnectDoesNotAuthorizeUnrelatedNode": {"storetest.StartPostgresRuntimeStore(t)", "eventBusTestRunContextForSource(t, pg, semanticview.Wrap(bundle))", "seedComponentFlowConstruction(t, ctx, pg, source,", "pg.CommitFlowInstanceActivation(constructionCtx, command)", "pc.FinalizeInitialEntryLifecycle(constructionCtx, committed.Lifecycle)", "runtimebustest.SubscribeIdentity(t, eb, agentIdentity, admission.CarrierOnly())", "defer runtimebustest.Unsubscribe(eb, agentID)", "delivered.Complete()", "storetest.CountTemplateNodeDelivery(ctx, pg, evt.ID(), \"account-setup-node-one\")"},
}

func family145NativeBusRoutingProofError(function, before, after string) error {
	if err := family145ConnectorProofError(function, before, after); err != nil {
		return err
	}
	for _, cut := range family145NativeBusRoutingCalls[function] {
		if !strings.Contains(after, cut) {
			return fmt.Errorf("bus routing lost native authority/scope/lifetime: %s", cut)
		}
	}
	return nil
}

func TestFamily145NativeBusRoutingRetainsConstructionPublicationAndNodeExclusion(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, row := range rows {
		cuts, ok := family145NativeBusRoutingCalls[row.Function]
		if !ok {
			continue
		}
		seen++
		if err := family145NativeBusRoutingProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range cuts {
			if family145NativeBusRoutingProofError(row.Function, row.Before, strings.ReplaceAll(row.After, cut, "foreignCut(")) == nil {
				t.Fatalf("lost exact bus construction/recipient/lifetime cut accepted: %s", cut)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.Before)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145NativeBusRoutingProofError(row.Function, row.Before, strings.ReplaceAll(row.After, condition, "false")) == nil {
				t.Fatalf("lost original bus assertion accepted: %s", condition)
			}
		}
		assertFamily145NativeBusNodeQuery(t, row.Before)
	}
	if seen != len(family145NativeBusRoutingCalls) {
		t.Fatalf("finite bus routing inventory incomplete: %d", seen)
	}
}

func assertFamily145NativeBusNodeQuery(t *testing.T, before string) {
	t.Helper()
	path := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", "delivery", "read_projections.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, data, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "CountTemplateNodeDeliveryTx" {
			continue
		}
		original := family145ConnectorSQL(t, "package proof\n"+before)
		native := family145ConnectorSQL(t, "package proof\n"+family145NodeString(fn))
		if len(original) != 1 || len(native) != 1 || original[0] != strings.Replace(native[0], "$2", "'account-setup-node-one'", 1) {
			t.Fatalf("existing node owner changed physical event/type/subscriber scope: original=%v native=%v", original, native)
		}
		return
	}
	t.Fatal("original closed delivery observation owner is missing")
}
