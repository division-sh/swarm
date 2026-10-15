package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func family145OperationSourceCaptureError(before, after string) error {
	cut := strings.Index(before, "\ttx, err := db.BeginTx(ctx, nil)")
	if cut < 0 || !strings.Contains(before[cut:], "runforkrevision.AllFamilies()...") {
		return fmt.Errorf("operation-source predecessor lost its original complete capture")
	}
	want := strings.Replace(before[:cut], "backend string, db *sql.DB, selected any", "selected any", 1)
	want += "\tstoretest.CaptureSelectedRunForkRevision(t, ctx, selected, runID)\n}"
	actual, err := canonicalFunction(after)
	expected, wantErr := canonicalFunction(want)
	if err != nil || wantErr != nil || actual != expected {
		return fmt.Errorf("operation-source capture lost exact context, selected owner, frontier cut or original native setup")
	}
	return nil
}

func family145OperationSourceCalls(source string, native bool) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "operation.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	var calls []string
	var invalid bool
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || family145NodeString(call.Fun) != "seedSelectedOperationSource" {
			return true
		}
		args := call.Args
		if !native {
			if len(args) < 8 {
				invalid = true
				return true
			}
			args = append(append([]ast.Expr{}, args[:2]...), args[4:]...)
		}
		if len(args) < 6 || (family145NodeString(args[2]) != "selected" && family145NodeString(args[2]) != "s") {
			invalid = true
			return true
		}
		var values []string
		for _, arg := range args {
			values = append(values, family145NodeString(arg))
		}
		calls = append(calls, strings.Join(values, ","))
		return true
	})
	if invalid {
		return nil, fmt.Errorf("operation-source caller lost original selected identity or exact argument tuple")
	}
	return calls, nil
}

func TestFamily145OperationSourceCaptureAndNineCallersStayClosed(t *testing.T) {
	var transitions []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &transitions); err != nil {
		t.Fatal(err)
	}
	var owners, count int
	for _, row := range transitions {
		if row.Function == "seedSelectedOperationSource" {
			owners++
			if err := family145OperationSourceCaptureError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			for _, hostile := range []string{
				strings.Replace(row.After, "(t, ctx, selected, runID)", "(t, context.Background(), selected, runID)", 1),
				strings.Replace(row.After, "(t, ctx, selected, runID)", "(t, ctx, rawPool, runID)", 1),
				strings.Replace(row.After, "(t, ctx, selected, runID)", "(t, ctx, selected, foreignRun)", 1),
			} {
				if err := family145OperationSourceCaptureError(row.Before, hostile); err == nil {
					t.Fatal("foreign context, owner or frontier accepted")
				}
			}
			continue
		}
		before, err := family145OperationSourceCalls(row.Before, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(before) == 0 {
			continue
		}
		after, err := family145OperationSourceCalls(row.After, true)
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("operation-source caller changed context, native owner, optional input or cut: %s/%s/%v", row.File, row.Function, err)
		}
		count += len(after)
		if _, err := family145OperationSourceCalls(strings.Replace(row.After, "ctx, selected, loaded", "ctx, rawPool, loaded", 1), true); err == nil && strings.Contains(row.After, "ctx, selected, loaded") {
			t.Fatal("raw caller owner accepted")
		}
	}
	if owners != 1 || count != 9 {
		t.Fatalf("operation-source owner/caller inventory=%d/%d want1/9", owners, count)
	}
	files, err := filepath.Glob("../../../internal/runtime/runforkexecution/*test.go")
	if err != nil {
		t.Fatal(err)
	}
	var live int
	for _, path := range files {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			if fn, ok := declaration.(*ast.FuncDecl); ok {
				calls, err := family145OperationSourceCalls(family145NodeString(fn), true)
				if err != nil {
					t.Fatalf("unmigrated live operation-source entry point: %s/%s/%v", path, fn.Name, err)
				}
				live += len(calls)
			}
		}
	}
	if live != count {
		t.Fatalf("unaccounted live operation-source caller: %d want%d", live, count)
	}
}
