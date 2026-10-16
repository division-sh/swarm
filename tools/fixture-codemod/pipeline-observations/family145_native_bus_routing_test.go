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
	"TestEventBusPublish_UndeclaredDescendantEmissionFailsClosedBeforeChildMutation": {"storetest.StartPostgresRuntimeStore(t)", "exactEventBusWorkflowFixtures(t, semanticview.Wrap(bundle),", "eventBusTestRunContextForSource(t, pg, semanticview.Wrap(bundle))", "workflowStore.PrepareInitialEntryLifecycle(", "CommitFlowInstanceActivation(", "workflowStore.FinalizeInitialEntryLifecycle(", "eb.WaitForQuiescence(ctx)", "storetest.CountBusValidationDeadLetterNode(ctx, pg, routerNode.Key())", "storetest.CountUndeclaredBusChildEvents(ctx, pg)", "storetest.CountUndeclaredBusChildEntities(ctx, pg)", "storetest.ReadBusRoutingDeliveryDiagnostics(ctx, pg)", "storetest.ReadBusRoutingEventDiagnostics(context.Background(), pg)", "if row.HasSubscriber"},
	"TestEventBusPublish_NestedDescendantCompletionFollowsDeclaredAncestorConnects":  {"storetest.StartPostgresRuntimeStore(t)", "exactEventBusWorkflowFixtures(t, semanticview.Wrap(bundle),", "eventBusTestRunContextForSource(t, pg, semanticview.Wrap(bundle))", "workflowStore.PrepareInitialEntryLifecycle(", "CommitFlowInstanceActivation(", "workflowStore.FinalizeInitialEntryLifecycle(", "eb.WaitForQuiescence(ctx)", "storetest.ReadBusRoutingEventNames(context.Background(), pg)", "storetest.ReadBusAncestorDeliveryDiagnostics(context.Background(), pg)"},
	"TestEventBusPublish_AgentOnlyConnectDoesNotAuthorizeUnrelatedNode":              {"storetest.StartPostgresRuntimeStore(t)", "eventBusTestRunContextForSource(t, pg, semanticview.Wrap(bundle))", "seedComponentFlowConstruction(t, ctx, pg, source,", "pg.CommitFlowInstanceActivation(constructionCtx, command)", "pc.FinalizeInitialEntryLifecycle(constructionCtx, committed.Lifecycle)", "runtimebustest.SubscribeIdentity(t, eb, agentIdentity, admission.CarrierOnly())", "defer runtimebustest.Unsubscribe(eb, agentID)", "delivered.Complete()", "storetest.CountTemplateNodeDelivery(ctx, pg, evt.ID(), \"account-setup-node-one\")"},
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

func assertFamily145BusRefusalQueries(t *testing.T, before string) {
	t.Helper()
	original := family145ConnectorSQL(t, "package proof\n"+before)
	for _, owner := range []struct {
		file, function string
		index, variant int
	}{
		{"delivery/read_projections.go", "CountBusValidationDeadLetterNodeTx", 0, 0},
		{"delivery/read_projections.go", "ReadBusRoutingDeliveryDiagnosticsTx", 1, 1},
		{"eventrecord/bus_routing_observation.go", "CountUndeclaredBusChildEventsTx", 2, 0},
		{"entityruntime/template_storage_observation.go", "CountUndeclaredBusChildEntitiesTx", 3, 0},
		{"eventrecord/bus_routing_observation.go", "ReadBusRoutingEventDiagnosticsTx", 4, 1},
	} {
		path := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", owner.file)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, data, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != owner.function {
				continue
			}
			found = true
			queries := family145ConnectorSQL(t, "package proof\n"+family145NodeString(fn))
			if len(original) != 5 || len(queries) <= owner.variant || queries[owner.variant] != original[owner.index] {
				t.Fatalf("refusal observation changed scope/columns/NULL/join/order: %s", owner.function)
			}
		}
		if !found {
			t.Fatalf("refusal original owner is missing: %s", owner.function)
		}
	}
}

func family145BusAssertionCounterexample(source, condition string) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "proof.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return "", err
	}
	changed := false
	ast.Inspect(file, func(node ast.Node) bool {
		conditional, ok := node.(*ast.IfStmt)
		if ok && family145NodeString(conditional.Cond) == condition {
			conditional.Cond = ast.NewIdent("false")
			changed = true
		}
		return true
	})
	if !changed {
		return "", fmt.Errorf("original assertion condition not found in candidate: %s", condition)
	}
	return family145NodeString(file.Decls[0]), nil
}

func assertFamily145BusAncestorQueries(t *testing.T, before string) {
	t.Helper()
	original := family145ConnectorSQL(t, "package proof\n"+before)
	for _, owner := range []struct {
		file, function string
		index, variant int
	}{
		{"eventrecord/bus_routing_observation.go", "ReadBusRoutingEventNamesTx", 0, 0},
		{"delivery/read_projections.go", "ReadBusAncestorDeliveryDiagnosticsTx", 1, 1},
	} {
		path := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", owner.file)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, data, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != owner.function {
				continue
			}
			found = true
			queries := family145ConnectorSQL(t, "package proof\n"+family145NodeString(fn))
			if len(original) != 2 || len(queries) <= owner.variant || queries[owner.variant] != original[owner.index] {
				t.Fatalf("ancestor observation changed exact global columns/NULL/order/join scope: %s", owner.function)
			}
		}
		if !found {
			t.Fatalf("ancestor original owner is missing: %s", owner.function)
		}
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
