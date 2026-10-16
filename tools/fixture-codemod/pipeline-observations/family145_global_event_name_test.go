package main

import (
	_ "embed"
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strconv"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelsettlement"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

//go:embed family145-global-event-name-preimages.json
var globalEventNameBytes []byte

func globalNameEarlierCurrentDeclaration(t *testing.T, file, function, current string) string {
	t.Helper()
	current, err := selectedreadrole.EarlierSource("../../..", file, function, current)
	if err != nil {
		t.Fatal(err)
	}
	pins, err := servedsettlement.Decode(servedSettlementBytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		if pin.File == file && pin.Function == function {
			actual := servedSettlementEarlierDeclaration(t, file, function, current)
			if actual != pin.After {
				t.Fatal("global-name comparison lost the exact later settlement stage:", function)
			}
			return pin.Before
		}
	}
	previous, err := channelsettlement.EarlierSource("../../..", file, function, current)
	if err != nil {
		t.Fatal(err)
	}
	return previous
}

func globalNameObservationDelta(t *testing.T, old, current []byte) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "observation.go", current, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	declarations := []ast.Decl{}
	fields, methods := 0, 0
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Name.Name == "CountEventNameStorage" {
			methods++
			want := `func(o channelObservation)CountEventNameStorage(ctx context.Context,eventName string)(int,error){var count int;err:=o.selected.InspectSnapshot(ctx,func(ctx context.Context)error{var err error;count,err=private.CountEventNameStorageForTest(ctx,o.selected,eventName);return err});if err!=nil{return 0,err};return count,nil}`
			parsed, err := parser.ParseFile(token.NewFileSet(), "bridge.go", "package proof\n"+want, parser.AllErrors)
			if err != nil || family145NodeString(fn) != family145NodeString(parsed.Decls[0]) {
				t.Fatal("channel global name method changed its native owner/context/snapshot/error cut:", err)
			}
			continue
		}
		fields += removeGlobalNameObservationField(declaration)
		declarations = append(declarations, declaration)
	}
	file.Decls = declarations
	before, err := parser.ParseFile(token.NewFileSet(), "old.go", old, parser.AllErrors)
	if err != nil || fields != 1 || methods != 1 || family145NodeString(file) != family145NodeString(before) {
		t.Fatal("global count addition changed other observer fields/methods/cleanup or added another role:", fields, methods, err)
	}
}

func removeGlobalNameObservationField(declaration ast.Decl) int {
	group, ok := declaration.(*ast.GenDecl)
	if !ok {
		return 0
	}
	count := 0
	for _, spec := range group.Specs {
		typed, ok := spec.(*ast.TypeSpec)
		if !ok || typed.Name.Name != "ChannelAnchorObservation" {
			continue
		}
		face, ok := typed.Type.(*ast.InterfaceType)
		if !ok {
			return -1
		}
		var fields []*ast.Field
		for _, field := range face.Methods.List {
			if len(field.Names) == 1 && field.Names[0].Name == "CountEventNameStorage" {
				count++
				if family145NodeString(field.Type) != "func(context.Context, string) (int, error)" {
					return -1
				}
				continue
			}
			fields = append(fields, field)
		}
		face.Methods.List = fields
	}
	return count
}

var globalNameSQLShape = regexp.MustCompile(`(?i)^\s*select\s+count\(\*\)\s+from\s+events\s+where\s+event_name\s*=\s*(?:\?|\$1|'(?:[^']|'')*')\s*$`)

func globalNameDirectSQL(file *ast.File) bool {
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || method.Sel.Name != "QueryRow" && method.Sel.Name != "QueryRowContext" {
			return true
		}
		for _, arg := range call.Args {
			literal, ok := arg.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				continue
			}
			query, err := strconv.Unquote(literal.Value)
			if err != nil {
				found = true
				continue
			}
			if globalNameSQLShape.MatchString(query) {
				found = true
			}
		}
		return true
	})
	return found
}
