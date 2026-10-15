package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

const predecessor = "c760d28bd28b84c7b62578cbe7037833d441747f"
const ledger = "tools/fixture-codemod/pipeline-observations/family145-public-join-preimages.json"

var helpers = map[string]string{
	"a2ReadJoinPublicObligation": "Endpoint", "a2RequireJoinPublicGraph": "BundleHash",
	"a2ReadJoinPublicEvent": "Endpoint", "a2WaitJoinPublicRun": "Endpoint",
	"a2RequireJoinPublicClients": "Endpoint", "a2JoinPublicCLI": "Endpoint",
	"a2RequireJoinPublicTrace": "Endpoint",
}

type pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
	Source                                string
}
type edit struct {
	start, end int
	after      string
}

func main() {
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-public-join [-write]")
	}
	var pins []pin
	snapshot, err := exec.Command("git", "ls-tree", "-r", "--name-only", predecessor, "--", "internal/serveapp").Output()
	must(err)
	must(checkoutsource.WalkDir(".", "internal/serveapp", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || !strings.Contains("\n"+string(snapshot), "\n"+path+"\n") {
			return err
		}
		raw, err := exec.Command("git", "show", predecessor+":"+path).Output()
		if err != nil {
			return err
		}
		pins = append(pins, sourcePins(path, raw)...)
		return nil
	}))
	calls, declarations := 0, 0
	for _, p := range pins {
		calls += p.Calls
		if helpers[p.Function] != "" {
			declarations++
		}
	}
	if calls != 31 || declarations != 7 || len(pins) != 10 {
		panic(fmt.Sprintf("public join inventory calls=%d helpers=%d bodies=%d", calls, declarations, len(pins)))
	}
	outputs := prepare(pins)
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	fmt.Printf("calls=%d helpers=%d bodies=%d manifest_sha256=%x\n", calls, declarations, len(pins), sha256.Sum256(raw))
	if write {
		for path, output := range outputs {
			must(os.WriteFile(path, output, 0644))
		}
		must(os.WriteFile(ledger, raw, 0644))
	}
}

func sourcePins(path string, raw []byte) []pin {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, raw, parser.AllErrors)
	must(err)
	var pins []pin
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		source := string(raw[fset.Position(function.Pos()).Offset:fset.Position(function.End()).Offset])
		after, calls, err := transform(fset, function, source)
		must(err)
		if source == after {
			continue
		}
		before := nodeString(function)
		parsed, err := parser.ParseFile(token.NewFileSet(), path, "package proof\n"+after, parser.AllErrors)
		must(err)
		pins = append(pins, pin{path, function.Name.Name, before, nodeString(parsed.Decls[0]), fmt.Sprintf("%x", sha256.Sum256([]byte(before))), calls, after})
	}
	return pins
}

func transform(fset *token.FileSet, function *ast.FuncDecl, source string) (string, int, error) {
	projection := helpers[function.Name.Name]
	start := fset.Position(function.Pos()).Offset
	var edits []edit
	permitted := map[token.Pos]bool{}
	if projection != "" {
		parameter, err := parameterEdit(fset, function, projection)
		if err != nil {
			return "", 0, err
		}
		edits = append(edits, parameter)
	}
	calls := 0
	var invalid error
	ast.Inspect(function.Body, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok && projection != "" && nodeString(selector.X) == "rt" && selector.Sel.Name == projection {
			permitted[selector.X.Pos()] = true
			edits = append(edits, edit{fset.Position(selector.Pos()).Offset - start, fset.Position(selector.End()).Offset - start, argumentName(projection)})
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if !ok || helpers[name.Name] == "" {
			return true
		}
		if len(call.Args) < 2 || nodeString(call.Args[0]) != "t" || nodeString(call.Args[1]) != "rt" || name.Obj != nil && name.Obj.Kind != ast.Fun {
			invalid = fmt.Errorf("foreign join caller %s/%s", function.Name.Name, name.Name)
			return false
		}
		argument := call.Args[1]
		permitted[argument.Pos()] = true
		after := "rt." + helpers[name.Name]
		if projection != "" {
			if projection != helpers[name.Name] {
				invalid = fmt.Errorf("join helper crosses its declared public projection")
				return false
			}
			after = argumentName(projection)
		}
		edits = append(edits, edit{fset.Position(argument.Pos()).Offset - start, fset.Position(argument.End()).Offset - start, after})
		calls++
		return true
	})
	if projection != "" {
		ast.Inspect(function.Body, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok && id.Name == "rt" && !permitted[id.Pos()] {
				invalid = fmt.Errorf("join helper consumes authority beyond its public projection")
			}
			return true
		})
	}
	if invalid != nil {
		return "", 0, invalid
	}
	slices.SortFunc(edits, func(a, b edit) int { return b.start - a.start })
	for _, e := range edits {
		source = source[:e.start] + e.after + source[e.end:]
	}
	return source, calls, nil
}

func parameterEdit(fset *token.FileSet, function *ast.FuncDecl, projection string) (edit, error) {
	if function.Recv != nil || len(function.Type.Params.List) < 2 {
		return edit{}, fmt.Errorf("public join helper lost its parameter inventory")
	}
	field := function.Type.Params.List[1]
	if len(field.Names) != 1 || field.Names[0].Name != "rt" || nodeString(field.Type) != "servedControlProofRuntime" {
		return edit{}, fmt.Errorf("foreign join helper carrier")
	}
	start := fset.Position(function.Pos()).Offset
	return edit{fset.Position(field.Pos()).Offset - start, fset.Position(field.End()).Offset - start, argumentName(projection) + " string"}, nil
}

func argumentName(projection string) string {
	if projection == "BundleHash" {
		return "bundleHash"
	}
	return "endpoint"
}

func prepare(pins []pin) map[string][]byte {
	grouped := map[string][]pin{}
	for _, p := range pins {
		grouped[p.File] = append(grouped[p.File], p)
	}
	outputs := map[string][]byte{}
	for path, rows := range grouped {
		raw, err := os.ReadFile(path)
		must(err)
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, raw, parser.AllErrors)
		must(err)
		var edits []edit
		for _, row := range rows {
			edits = append(edits, currentEdit(fset, file, row))
		}
		slices.SortFunc(edits, func(a, b edit) int { return b.start - a.start })
		for _, e := range edits {
			raw = append(raw[:e.start], append([]byte(e.after), raw[e.end:]...)...)
		}
		outputs[path], err = format.Source(raw)
		must(err)
	}
	return outputs
}

func currentEdit(fset *token.FileSet, file *ast.File, row pin) edit {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != row.Function {
			continue
		}
		actual := nodeString(function)
		comparison, err := servedsettlement.ObservationEarlierSource(".", row.File, row.Function, actual)
		must(err)
		if comparison != row.Before && comparison != row.After {
			panic("join consumer escaped complete rule: " + row.File + "/" + row.Function)
		}
		if actual != comparison {
			return edit{fset.Position(function.Pos()).Offset, fset.Position(function.End()).Offset, actual}
		}
		return edit{fset.Position(function.Pos()).Offset, fset.Position(function.End()).Offset, row.Source}
	}
	panic("public join declaration disappeared")
}

func nodeString(node ast.Node) string {
	var out bytes.Buffer
	must(format.Node(&out, token.NewFileSet(), node))
	return out.String()
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
