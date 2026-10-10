package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeBusSourceBoundaryRetiresFakeProtocolAndPreservesActualMutations(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-source-boundary" {
			continue
		}
		matched++
		before := row.Before
		for _, pair := range [][2]string{
			{"_ context.Context, eventID string", "ctx context.Context, eventID string"},
			{"\to.claimEvent++\n", "\to.claimEvent++\n\to.claimFact, _ = runtimecorrelation.SourceArtifactFactFromContext(ctx)\n"},
			{"\tpostCommit := make([]runtimepipelinefixture.OwnerAction, 0, 1)\n\trollback := make([]runtimepipelinefixture.OwnerAction, 0, 1)\n\trouteCtx := runtimepipelinefixture.WithSQLTx(foreignCtx, &sql.Tx{})\n\trouteCtx = runtimepipelinefixture.WithPostCommitActions(routeCtx, &postCommit)\n\trouteCtx = runtimepipelinefixture.WithRollbackActions(routeCtx, &rollback)\n", ""},
			{"bus.AddFlowInstanceRouteContextFixture(routeCtx, req)", "bus.AddFlowInstanceRouteContextFixture(foreignCtx, req)"},
			{"store.upsertCalls != 0 || len(postCommit) != 0 || len(rollback) != 0 || bus.HasFlowInstanceRoute(req.Identity)", "store.upsertCalls != 0 || bus.HasFlowInstanceRoute(req.Identity)"},
			{"\"route add mutations = upsert:%d post_commit:%d rollback:%d local:%v, want zero\"", "\"route add mutations = upsert:%d local:%v, want zero\""},
			{"store.upsertCalls, len(postCommit), len(rollback), bus.HasFlowInstanceRoute(req.Identity),", "store.upsertCalls, bus.HasFlowInstanceRoute(req.Identity),"},
			{"TestPostCommitDispatchIgnoresAmbientSQLContextAndPreservesBusOwnedSourceFact", "TestPostCommitDispatchPreservesBusOwnedSourceFactThroughClaimAndSettlement"},
			{"\tpostCommit := make([]runtimepipelinefixture.OwnerAction, 0, 1)\n\trollback := make([]runtimepipelinefixture.OwnerAction, 0, 1)\n", ""},
			{"\tctx = runtimepipelinefixture.WithSQLTx(ctx, &sql.Tx{})\n\tctx = runtimepipelinefixture.WithPostCommitActions(ctx, &postCommit)\n\tctx = runtimepipelinefixture.WithRollbackActions(ctx, &rollback)\n", ""},
			{"\tif len(postCommit) != 0 || len(rollback) != 0 {\n\t\tt.Fatalf(\"ambient transaction actions = commit:%d rollback:%d, want none\", len(postCommit), len(rollback))\n\t}\n", ""},
		} {
			before = strings.Replace(before, pair[0], pair[1], 1)
		}
		after := strings.Replace(row.After, "\tfor _, action := range []string{\"claim\", \"settle\"} {\n\t\tif !owned.Matches(owner.sourceFact(action)) {\n\t\t\tt.Fatalf(\"post-commit %s lost the exact immutable bus source\", action)\n\t\t}\n\t}\n", "", 1)
		if before != after {
			t.Fatal("source conflict, zero mutation, actual routing, exact claim or settlement changed")
		}
		if strings.Contains(row.After, "runtimepipelinefixture") || strings.Contains(row.After, "sql.Tx") {
			t.Fatal("dummy transaction interpretation survives")
		}
	}
	if matched != 3 {
		t.Fatalf("source boundary recipes=%d,want3", matched)
	}
	actual := selectedCausalObservationBody(t, "internal/runtime/bus/source_artifact_mutation_roots_test.go", "TestPostCommitDispatchPreservesBusOwnedSourceFactThroughClaimAndSettlement")
	if !strings.Contains(actual, "\tfor _, action := range []string{\"claim\", \"settle\"} {\n\t\tif !owned.Matches(owner.sourceFact(action)) {\n\t\t\tt.Fatalf(\"post-commit %s lost the exact immutable bus source\", action)\n\t\t}\n\t}") {
		t.Fatal("actual claim and settlement no longer require the exact bus source")
	}
}
