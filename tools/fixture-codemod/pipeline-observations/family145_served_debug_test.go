package main

import (
	_ "embed"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicendpoint"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

//go:embed family145-served-debug-preimages.json
var family145ServedDebugPreimages []byte

type servedDebugPreimage struct {
	File, Function, SHA256, Before, After string
	DebugCalls, HelperCalls               int
}

var family145ServedDebugRows = func() []servedDebugPreimage {
	var rows []servedDebugPreimage
	if err := json.Unmarshal(family145ServedDebugPreimages, &rows); err != nil {
		panic(err)
	}
	return rows
}()

// Only the exact reviewed current body composes back to the signed predecessor.
// Mutated arguments, assertions, captures or control flow never enter this path.
func family145ServedDebugCallerInverse(function, after string) string {
	after = family145RunPhysicalCallerInverse(function, after)
	after = family145RequestKeyCallerInverse(function, after)
	after = family145ChannelIntentCallerInverse(function, after)
	after = family145ForkNoticeCallerInverse(function, after)
	after = family145ForkCompanionCallerInverse(function, after)
	after = family145PublicJoinCallerInverse(function, after)
	after = family145ReceiverReadbackCallerInverse(function, after)
	after = family145PublicA2EndpointCallerInverse(function, after)
	for _, row := range family145ServedDebugRows {
		if row.Function == function && row.After != "" && after == row.After {
			return row.Before
		}
	}
	return after
}

func family145CheckServedDebugWholeRule(t *testing.T, file, function, after string) bool {
	t.Helper()
	after = family145RunPhysicalPrevious(file, function, after)
	after = family145RequestKeyCallerInverse(function, after)
	after = family145ChannelIntentCallerInverse(function, after)
	after = family145ForkNoticeCallerInverse(function, after)
	after = family145ForkCompanionCallerInverse(function, after)
	after = family145ReceiverReadbackCallerInverse(function, after)
	after = publicendpoint.Previous(file, function, after)
	for _, row := range family145ServedDebugRows {
		if row.File == file && row.Function == function {
			if after != row.After {
				t.Fatalf("served diagnostic propagation escaped complete signed rule: %s/%s", file, function)
			}
			return true
		}
	}
	return false
}

func family145ServedDebugNormalize(t *testing.T, source string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "rule.go", "package proof\n"+source, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		t.Fatalf("invalid finite declaration: %v", err)
	}
	return family145NodeString(file.Decls[0])
}

func family145ServedDebugCurrentDeclaration(t *testing.T, row servedDebugPreimage) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("../../..", row.File))
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), row.File, raw, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && function.Recv == nil && function.Name.Name == row.Function {
			current := sourceBlobPreviousDeclaration(t, row.File, row.Function, family145NodeString(function))
			current = runphysical.Previous(runIdentityPins, row.File, row.Function, current)
			current = runphysical.Previous(runDeliveryLivePins(t), row.File, row.Function, current)
			current = runphysical.Previous(sourceForkPins(t), row.File, row.Function, current)
			current = family145RunScalarCallerInverse(row.Function, current)
			current = family145WorkflowStateCallerInverse(row.Function, current)
			current = family145RunPhysicalPrevious(row.File, row.Function, current)
			current = family145RequestKeyCallerInverse(row.Function, current)
			current = family145ChannelIntentCallerInverse(row.Function, current)
			current = family145ForkNoticeCallerInverse(row.Function, current)
			current = family145ForkCompanionCallerInverse(row.Function, current)
			current = family145ReceiverReadbackCallerInverse(row.Function, current)
			return publicendpoint.Previous(row.File, row.Function, current)
		}
		if group, ok := declaration.(*ast.GenDecl); ok && group.Tok == token.TYPE && len(group.Specs) == 1 && "type/"+group.Specs[0].(*ast.TypeSpec).Name.Name == row.Function {
			return family145NodeString(group)
		}
	}
	return ""
}

func family145ServedDebugHostiles(t *testing.T, row servedDebugPreimage) {
	t.Helper()
	finite := recipe{File: row.File, Function: row.Function, Before: row.Before, After: row.After}
	for _, cut := range []string{"ReceiverStateReader", "p.deps.EventStore", "t.Fatal", "t.Errorf", "reflect.DeepEqual", "releaseOnce.Do", "first.kill", "45 * time.Second", "50 * time.Millisecond", "ReadServedEventRunIDStorage", "return err", "idempotency_key"} {
		if !strings.Contains(row.After, cut) {
			continue
		}
		hostile := strings.Replace(row.After, cut, "foreignDiagnosticCut()", 1)
		if _, _, err := rewriteFunction(row.File, []byte("package proof\n"+hostile), finite); err == nil {
			t.Fatalf("diagnostic rule accepted lost identity/assertion/temporal/owner cut: %s/%s", row.Function, cut)
		}
	}
}
