package main

import (
	"crypto/sha256"
	_ "embed"
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

//go:embed family145-transitions.json
var family145TransitionBytes []byte

func TestFamily145AdmissionSnapshotsRetainWorkloadsAndRejectDrift(t *testing.T) {
	var rows []struct {
		File, Function, Before, After string
		Removed                       bool `json:"Removed,omitempty"`
	}
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(rows)
	if err != nil || len(rows) != 91 || fmt.Sprintf("%x", sha256.Sum256(raw)) != "faed9b91cb0240c4e75ebd3f693da97090f0da9792214003877427884776f7ce" {
		t.Fatalf("finite family145 source transition changed: %d/%v", len(rows), err)
	}
	for _, row := range rows {
		finite := recipe{File: row.File, Function: row.Function, Before: row.Before, After: row.After}
		if row.Removed {
			finite.After, finite.Removed = row.Before, true
			if _, err := checkpointRecipes([]byte("package proof\n"+row.Before), []recipe{finite}); err == nil {
				t.Fatalf("retired raw helper survived: %s", row.Function)
			}
			if remaining, err := checkpointRecipes([]byte("package proof\n"), []recipe{finite}); err != nil || len(remaining) != 0 {
				t.Fatalf("exact helper retirement failed: %s/%v", row.Function, err)
			}
			continue
		}
		source := []byte("package proof\n" + row.Before)
		after, changed, err := rewriteFunction(row.File, source, finite)
		if err != nil || !changed {
			t.Fatalf("finite rewrite failed: %s/%s/%v", row.File, row.Function, err)
		}
		if _, changed, err := rewriteFunction(row.File, after, finite); err != nil || changed {
			t.Fatalf("replay changed native successor: %s/%v", row.Function, err)
		}
		hostile := strings.Replace(row.Before, "{", "{ unreviewedAuthority();", 1)
		if _, _, err := rewriteFunction(row.File, []byte("package proof\n"+hostile), finite); err == nil {
			t.Fatalf("changed binding/workload accepted: %s", row.Function)
		}
		if family145SemanticOwner(row.Function) {
			continue
		}
		beforeAssertions, beforeErr := runSetupAssertions(row.Before)
		afterAssertions, afterErr := runSetupAssertions(row.After)
		if beforeErr != nil || afterErr != nil || strings.Join(beforeAssertions, "\n") != strings.Join(afterAssertions, "\n") {
			t.Fatalf("caller lost an assertion: %s/%v/%v", row.Function, beforeErr, afterErr)
		}
	}
}

func family145SemanticOwner(function string) bool {
	switch function {
	case "captureSelectedExecutionSourceRevision", "admitSelectedExecutionSourceArtifact",
		"seedSelectedExecutionStateOnlySourceRun", "seedSelectedExecutionSourceRunWithPrimaryRouteModeAndSource",
		"selectedForkExecutableMutationSnapshotForTest", "selectedContractDynamicRuntimeGlobalSnapshot",
		"assertSelectedContractDeferredWorkRejectionHasNoForkMutation",
		"postgresEventDeliveryTargetRoutes", "scanDeliveryTargetRoutes", "seedEventPublishConstructedFlow",
		"assertRunStartPersistence":
		return true
	}
	return false
}

func TestFamily145PublicationCallersHaveNoRawConstructionOrReadback(t *testing.T) {
	for _, name := range []string{"operator_event_publish_test.go", "operator_run_start_test.go"} {
		data, err := os.ReadFile(filepath.Join("../../..", "internal/apiv1", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := family145PublicationRawEscape(data); err != nil {
			t.Fatalf("%s retained a raw publication dependency: %v", name, err)
		}
	}
	for _, expression := range []string{"storetest.AdmitPostgresRuntimeStore(t, db)", "storetest.DatabaseForTest(selected)", "db.QueryRow(query)", "db.Exec(query)"} {
		source := []byte("package proof\nfunc witness() { " + expression + " }")
		if err := family145PublicationRawEscape(source); err == nil {
			t.Fatalf("publication escape control passed: %s", expression)
		}
	}
}

func family145PublicationRawEscape(source []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "publication.go", source, parser.AllErrors)
	if err != nil {
		return err
	}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		member, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch member.Sel.Name {
		case "AdmitPostgresRuntimeStore", "NewPostgresStoreForTest", "DatabaseForTest", "Database", "QueryRow", "QueryRowContext", "Query", "QueryContext", "Exec", "ExecContext", "Begin", "BeginTx":
			err = fmt.Errorf("raw call %s", member.Sel.Name)
			return false
		}
		return err == nil
	})
	return err
}

func TestFamily145ForkCallerRewritePreservesExactWorkload(t *testing.T) {
	before := `package runforkexecution
func caller(t testing.TB, db *sql.DB, pg *store.PostgresStore) {
	before := readExactClock()
	revision := captureSelectedExecutionSourceRevision(t, db, runID, runforkrevision.FamilyEvents)
	seedSelectedConstructedRootHistory(t, ctx, db, pg, loaded, runID, eventID, eventName, at, mode, routes, input)
	seedSelectedConstructedAgentRootHistory(t, ctx, db, pg, loaded, runID, eventID, at)
	assertExactRevisionAndClock(t, revision, before)
}`
	after, revisions, histories, helpers, err := rewriteFamily145ForkCallerSource("execution_test.go", []byte(before))
	if err != nil || revisions != 1 || histories != 2 || helpers != 0 {
		t.Fatalf("finite rewrite: %d/%d/%d err=%v", revisions, histories, helpers, err)
	}
	want := strings.ReplaceAll(before, "captureSelectedExecutionSourceRevision(t, db,", "captureSelectedExecutionSourceRevision(t, pg,")
	want = strings.ReplaceAll(want, "History(t, ctx, db, pg,", "History(t, ctx, pg,")
	beforeFile, err := parser.ParseFile(token.NewFileSet(), "want.go", want, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	afterFile, err := parser.ParseFile(token.NewFileSet(), "after.go", after, parser.AllErrors)
	if err != nil || family145NodeString(beforeFile.Decls[0]) != family145NodeString(afterFile.Decls[len(afterFile.Decls)-1]) {
		t.Fatalf("rewriter changed more than exact owner arguments: %s/%v", after, err)
	}
}

func TestFamily145ForkCallerRewriteRejectsUnreviewedBindings(t *testing.T) {
	for _, args := range []string{"t, peerDB, runID", "t, openPool(), runID", "t, db, runID"} {
		name := "execution_test.go"
		if args == "t, db, runID" {
			name = "unreviewed_test.go"
		}
		source := "package proof\nfunc caller() { captureSelectedExecutionSourceRevision(" + args + ") }"
		if _, _, _, _, err := rewriteFamily145ForkCallerSource(name, []byte(source)); err == nil {
			t.Fatalf("unreviewed binding accepted: %s/%s", name, args)
		}
	}
	for _, owner := range []string{"peer", "factory()", "selected.pg"} {
		source := "package proof\nfunc caller() { seedSelectedConstructedRootHistory(t, ctx, db, " + owner + ", loaded) }"
		if _, _, _, _, err := rewriteFamily145ForkCallerSource("execution_test.go", []byte(source)); err == nil {
			t.Fatalf("unreviewed history owner accepted: %s", owner)
		}
	}
}

func TestFamily145ForkCallerInventoryConsumesOriginalSelectedOwner(t *testing.T) {
	var revisions, histories int
	for _, source := range family145ForkCallerSources {
		data, err := os.ReadFile(filepath.Join("../../..", "internal/runtime/runforkexecution", source.name))
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), source.name, data, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if fn.Name.Name == "captureSelectedExecutionSourceRevision" {
				want := `func captureSelectedExecutionSourceRevision(t testing.TB, selected any, runID string, families ...runforkrevision.Family) int64 {
	t.Helper()
	return storetest.CaptureSelectedRunForkRevision(t, context.Background(), selected, runID, families...)
}`
				actual, err := canonicalFunction(family145NodeString(fn))
				expected, wantErr := canonicalFunction(want)
				if err != nil || wantErr != nil || actual != expected {
					t.Fatal("revision helper bypasses original selected receipt owner")
				}
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				helper, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				switch helper.Name {
				case "captureSelectedExecutionSourceRevision":
					revisions++
					want := family145ForkRevisionOwner(source.name, fn.Name.Name)
					if len(call.Args) < 3 || want == "" || family145NodeString(call.Args[1]) != want {
						t.Errorf("revision owner changed: %s/%s", source.name, fn.Name.Name)
					}
				case "seedSelectedConstructedRootHistory", "seedSelectedConstructedAgentRootHistory":
					histories++
					if len(call.Args) < 4 || (family145NodeString(call.Args[2]) != "pg" && family145NodeString(call.Args[2]) != "selected") {
						t.Errorf("history owner changed: %s/%s", source.name, fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	if revisions != 30 || histories != 21 {
		t.Fatalf("finite caller inventory changed: revisions=%d history=%d", revisions, histories)
	}
}
