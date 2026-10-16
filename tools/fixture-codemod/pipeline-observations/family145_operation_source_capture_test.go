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
