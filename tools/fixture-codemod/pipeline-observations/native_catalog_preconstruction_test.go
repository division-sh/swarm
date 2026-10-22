package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"strings"
	"testing"
)

func catalogPreconstructionRecipe(t *testing.T) recipe {
	t.Helper()
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var found []recipe
	for _, row := range rows {
		if row.Family == "native-catalog-preconstruction" {
			found = append(found, row)
		}
	}
	if len(found) != 1 || found[0].File != "internal/runtime/cataloge2e/runtime_harness_test.go" || found[0].Function != "newRuntimeHarnessWithTerminalProvider" {
		t.Fatal("catalog setup must have one exact shared construction recipe")
	}
	return found[0]
}

func TestNativeCatalogPreconstructionPreservesEveryRuntimeAndReplayStatement(t *testing.T) {
	row := catalogPreconstructionRecipe(t)
	expected, err := nativeCatalogPreconstructionAfter(t, row.Before)
	actual, parseErr := canonicalFunction(row.After)
	if err != nil || parseErr != nil || expected != actual {
		t.Fatalf("catalog construction/source/replay workload changed: %v / %v", err, parseErr)
	}
}

func nativeCatalogPreconstructionAfter(t *testing.T, source string) (string, error) {
	t.Helper()
	fn := projectionShapeFunction(t, source)
	oldFixture := projectionShapeStatement(t, `fixture := runlifecyclefixture.Fixture{
Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: catalogRuntimeRunID,
Source: sourceArtifactFact, Artifact: bundle.SourceArtifact,
}`)
	newFixture := projectionShapeStatement(t, `fixture := storetest.RunFixture{
Origin: storetest.ScenarioSetupOrigin(), RunID: catalogRuntimeRunID,
BundleHash: sourceArtifactFact.BundleHash(), Artifact: bundle.SourceArtifact,
}`)
	oldWrite := projectionShapeStatement(t, `if pg != nil {
runlifecyclefixture.RequirePostgres(t, fixtureCtx, db, fixture)
} else {
runlifecyclefixture.RequireSQLite(t, fixtureCtx, db, fixture)
}`)
	setup, writes, clock := 0, 0, 0
	var body []ast.Stmt
	for _, stmt := range fn.Body.List {
		if formattedNativeReadNode(stmt) == "startedAt := catalogHarnessStartBoundary(t, db, backend)" {
			clock++
			body = append(body, projectionShapeStatement(t, "startedAt := catalogHarnessStartBoundary(t, pg, backend)"))
			continue
		}
		if formattedNativeReadNode(stmt) == formattedNativeReadNode(oldFixture) {
			setup++
			body = append(body,
				projectionShapeStatement(t, "var runSetup storetest.RunFixtureStore = pg"),
				projectionShapeStatement(t, "if pg == nil { runSetup = sqlite }"), newFixture)
			continue
		}
		loop, ok := stmt.(*ast.RangeStmt)
		if ok && formattedNativeReadNode(loop.X) == "append([]string{catalogRuntimeRunID}, additionalRunIDs...)" {
			for i, child := range loop.Body.List {
				if formattedNativeReadNode(child) == formattedNativeReadNode(oldWrite) {
					writes++
					loop.Body.List[i] = projectionShapeStatement(t, "storetest.RequireRun(t, fixtureCtx, runSetup, fixture)")
				}
			}
		}
		body = append(body, stmt)
	}
	if setup != 1 || writes != 1 || clock != 1 {
		return "", fmt.Errorf("unreviewed catalog scenario setup: fixture=%d writer=%d clock=%d", setup, writes, clock)
	}
	fn.Body.List = body
	return canonicalFunction(formattedNativeReadNode(fn))
}

func TestNativeCatalogPreconstructionRejectsLostAuthorityAndOrdering(t *testing.T) {
	row := catalogPreconstructionRecipe(t)
	for _, pair := range [][2]string{
		{"Source: sourceArtifactFact", "Source: foreignFact"},
		{"RunID: catalogRuntimeRunID", "RunID: otherRun"},
		{"Artifact: bundle.SourceArtifact", "Artifact: otherArtifact"},
		{"ScenarioSetupOrigin()", "EventOrigin(t, eventID, eventType)"},
		{"RequirePostgres(t, fixtureCtx, db, fixture)", "RequirePostgres(t, context.Background(), db, fixture)"},
		{"RequireSQLite(t, fixtureCtx, db, fixture)", "RequireSQLite(t, fixtureCtx, otherDB, fixture)"},
	} {
		changed := strings.Replace(row.Before, pair[0], pair[1], 1)
		if changed == row.Before {
			t.Fatalf("negative control did not change source: %v", pair)
		}
		if _, err := nativeCatalogPreconstructionAfter(t, changed); err == nil {
			t.Fatalf("contradictory scenario authority matched: %v", pair)
		}
	}
	expected, err := nativeCatalogPreconstructionAfter(t, row.Before)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{
		{"BundleHash: sourceArtifactFact.BundleHash()", "BundleHash: otherFact.BundleHash()"},
		{"if pg == nil", "if pg != nil"},
		{"runSetup = sqlite", "runSetup = foreignStore"},
		{"storetest.RequireRun(t, fixtureCtx, runSetup, fixture)", "storetest.RequireRun(t, ctx, runSetup, fixture)"},
		{"storetest.RequireRun(t, fixtureCtx, runSetup, fixture)", ""},
		{"rt, err := runtime.NewRuntime(ctx, deps)", "rt, err := runtime.NewRuntime(context.Background(), deps)"},
		{"catalogHarnessStartBoundary(t, pg, backend)", "catalogHarnessStartBoundary(t, foreignStore, backend)"},
	} {
		changed := strings.Replace(row.After, pair[0], pair[1], 1)
		if changed == row.After {
			t.Fatalf("negative control did not change output: %v", pair)
		}
		actual, err := canonicalFunction(changed)
		if err == nil && actual == expected {
			t.Fatalf("lost catalog authority, construction gate or replay statement accepted: %v", pair)
		}
	}
}
