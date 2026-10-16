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

var family145ArtifactCallbackCuts = map[string][]string{
	"TestRuleResultEventsFlowThroughDurableCallbackDelivery":       {"storetest.StartPostgresRuntimeStore(t)", "seedRuntimeTestRun(t, pg)", "canonicalrouting.CopyArtifactActionResultDelivery(t, \"template\", false)", "seedRuntimeTestPreparedInstance(t, ctx, pg, pc, instance)", "flowroutefixture.StageAndPublish(ctx, bus,", "bus.Publish(ctx, requestEvent)", "storetest.ReadTemplateSourceNamedEventIdentity(ctx, pg, resultEventType, tc.requestEventID)", "assertArtifactActionResultEventContext(t, ctx, pg, resultEventID, tc.resultKind, \"repo-scaffold/inst-1\", artifactActionResultEntityID)", "assertArtifactActionResultNodeRoute(t, ctx, pg, resultEventID, \"repo-scaffold/inst-1\", artifactActionResultEntityID)", "waitArtifactActionResultHandlerStarted(t, ctx, pg, resultHandlerStarted, resultEventID)", "waitArtifactActionResultSettledCount(t, ctx, pg, 1, resultEventID, artifactActionResultDeliveryTargetRouteJSON(\"repo-scaffold/inst-1\", artifactActionResultEntityID), repoNodeID)", "waitRuntimeNodeDeliveryOutcome(t, ctx, pg, resultEventID, repoNodeID)"},
	"TestRuleResultEventsFlowThroughStaticServiceCallbackDelivery": {"storetest.StartPostgresRuntimeStore(t)", "seedRuntimeTestRun(t, pg)", "canonicalrouting.CopyArtifactActionResultDelivery(t, \"static\", childRequest)", "seedRuntimeTestPreparedInstance(t, ctx, pg, pc, runtimepipeline.WorkflowInstance{", "coordinate.ValidateConstruction(source, templateInstanceDeliveryRunID)", "semanticview.WorkflowStageTopology(source, coordinate.TemplateID)", "topology.InitialStoredStage()", "storetest.ReadPostgresArtifactCallbackSourceRoute(ctx, pg, requestEventID)", "bus.Publish(ctx, requestEvent)", "storetest.ReadTemplateSourceNamedEventIdentity(ctx, pg, resultEventType, requestEventID)", "assertArtifactActionResultEventContext(t, ctx, pg, resultEventID, tc.resultKind, tc.wantFlowPath, parent.EntityID)", "assertArtifactActionResultNodeRoute(t, ctx, pg, resultEventID, tc.wantFlowPath, parent.EntityID)", "waitArtifactActionResultHandlerStarted(t, ctx, pg, resultHandlerStarted, resultEventID)", "waitArtifactActionResultSettledCount(t, ctx, pg, 1, resultEventID, artifactActionResultDeliveryTargetRouteJSON(tc.wantFlowPath, parent.EntityID), repoNodeID)", "waitRuntimeNodeDeliveryOutcome(t, ctx, pg, resultEventID, repoNodeID)"},
	"assertArtifactActionResultEventContext":                       {"storetest.ReadPostgresArtifactCallbackEventContext(ctx, selected, eventID)", "entityID, flowInstance, sourceRouteJSON, payloadJSON := observed.EntityID, observed.FlowInstance, observed.SourceRoute, observed.Payload"},
	"assertArtifactActionResultNodeRoute":                          {"storetest.CountPostgresTemplateNodeTarget(ctx, selected, eventID, wantRoute, artifactActionResultNodeID(t))", "storetest.ReadPostgresArtifactCallbackTargetDiagnostics(ctx, selected, eventID)", "if got == 1", "time.Now().Add(5 * time.Second)", "time.Sleep(25 * time.Millisecond)"},
	"waitArtifactActionResultHandlerStarted":                       {"storetest.ReadPostgresArtifactCallbackDeliveries(ctx, selected, eventID)", "storetest.ReadPostgresArtifactCallbackReceipts(ctx, selected, eventID)", "time.After(5 * time.Second)", "case got := <-started:", "if got == eventID", "case <-deadline:"},
	"waitArtifactActionResultDBCount":                              {"storetest.CountPostgresArtifactCallbackSettledTarget(ctx, selected, eventID, target, nodeID)", "storetest.ReadPostgresArtifactCallbackSettlementDiagnostics(ctx, selected, eventID)", "if got == want", "time.Now().Add(5 * time.Second)", "time.Sleep(25 * time.Millisecond)"},
}

func family145ArtifactCallbackProofError(function, before, after string) error {
	if function == "waitArtifactActionResultDBCount" {
		// Only the failed fixed-count label changes when its SQL/variadic escape
		// is removed. The expected count, temporal cut and diagnostic stay live.
		before = strings.Replace(before, `t.Fatalf("count = %d, want %d for query %s; deliveries=%s", got, want, strings.TrimSpace(query), diagnostics)`, `t.Fatalf("count = %d, want %d for settled callback event %s node %s target %s; deliveries=%s", got, want, eventID, nodeID, target, diagnostics)`, 1)
	}
	if err := family145ConnectorProofError(function, before, after); err != nil {
		return err
	}
	for _, cut := range family145ArtifactCallbackCuts[function] {
		if !strings.Contains(after, cut) {
			return fmt.Errorf("callback lost original construction/observation/temporal cut: %s", cut)
		}
	}
	return nil
}

func assertFamily145ArtifactCallbackSQL(t *testing.T, original map[string]string) {
	t.Helper()
	for _, port := range []struct {
		file, function, before string
		index                  int
	}{
		{"eventrecord/artifact_callback_observation.go", "ReadPostgresArtifactCallbackEventContextTx", "assertArtifactActionResultEventContext", 0},
		{"eventrecord/artifact_callback_observation.go", "ReadPostgresArtifactCallbackSourceRouteTx", "TestRuleResultEventsFlowThroughStaticServiceCallbackDelivery", 1},
		{"delivery/read_projections.go", "CountPostgresTemplateNodeTargetTx", "assertArtifactActionResultNodeRoute", 0},
		{"delivery/read_projections.go", "ReadPostgresArtifactCallbackTargetDiagnosticsTx", "assertArtifactActionResultNodeRoute", 1},
		{"delivery/read_projections.go", "ReadPostgresArtifactCallbackDeliveriesTx", "waitArtifactActionResultHandlerStarted", 0},
		{"pipelinepersistence/artifact_callback_observation.go", "ReadPostgresArtifactCallbackReceiptsTx", "waitArtifactActionResultHandlerStarted", 1},
		{"delivery/read_projections.go", "CountPostgresArtifactCallbackSettledTargetTx", "TestRuleResultEventsFlowThroughDurableCallbackDelivery", 1},
	} {
		before := family145ConnectorSQL(t, "package proof\n"+original[port.before])
		queries := family145ArtifactCallbackOwnerSQL(t, port.file, port.function)
		if len(before) <= port.index || len(queries) != 1 || queries[0] != before[port.index] {
			t.Fatalf("callback changed exact columns/NULL/native JSON/predicate scope: %s: %v vs %v", port.function, queries, before)
		}
	}
	before := family145ConnectorSQL(t, "package proof\n"+original["waitArtifactActionResultDBCount"])
	native := family145ArtifactCallbackOwnerSQL(t, "delivery/read_projections.go", "ReadPostgresArtifactCallbackSettlementDiagnosticsTx")
	if len(before) != 1 || len(native) != 1 {
		t.Fatalf("combined callback diagnostic inventory changed: %v/%v", before, native)
	}
	receiptCut := `(SELECTCOALESCE(jsonb_agg(to_jsonb(r)),'[]'::jsonb)FROMevent_receiptsrWHEREr.event_id=$1)`
	if !strings.Contains(before[0], receiptCut) || strings.Replace(before[0], receiptCut, "$2::jsonb", 1) != native[0] {
		t.Fatalf("combined diagnostic changed beyond its owned receipt substitution: %v/%v", before, native)
	}
}

func family145ArtifactCallbackOwnerSQL(t *testing.T, path, function string) []string {
	t.Helper()
	path = filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, data, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Name.Name == function {
			return family145ConnectorSQL(t, "package proof\n"+family145NodeString(fn))
		}
	}
	t.Fatalf("exact callback owner missing: %s", function)
	return nil
}
