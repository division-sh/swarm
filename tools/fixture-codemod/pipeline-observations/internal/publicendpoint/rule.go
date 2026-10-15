package publicendpoint

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"slices"
)

//go:embed preimages.json
var manifest []byte

const Digest = "4f37a50d17478fdbd4cb64bb8df0d73c5722c7e92578878e20e277950cc83d1c"

type Pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
}

func Pins() []Pin {
	if ManifestDigest() != Digest {
		panic("complete public endpoint predecessor/output manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(manifest, &pins); err != nil {
		panic(err)
	}
	return pins
}

func ManifestDigest() string { return fmt.Sprintf("%x", sha256.Sum256(manifest)) }

// Previous accepts only a complete, exact successor of this finite family.
func Previous(file, function, current string) string {
	for _, pin := range Pins() {
		if pin.File == file && pin.Function == function && pin.After == current {
			return pin.Before
		}
	}
	return current
}

var Helpers = map[string]int{
	"requireA2PortfolioEntity": 4, "requireA2PortfolioEvents": 4,
	"requireA2PortfolioEvent": 6, "requireA2PortfolioEmission": 6,
	"requireA2PortfolioSetupRoutes": 6,
}

type edit struct {
	start, end int
	after      string
}

// Transform narrows only public readback: the same endpoint and request bytes,
// with no runtime, SQL, selected-role, mutation or lifetime substitution.
func Transform(fset *token.FileSet, function *ast.FuncDecl, source string) (string, int, error) {
	_, helper := Helpers[function.Name.Name]
	start := fset.Position(function.Pos()).Offset
	var edits []edit
	permitted := map[token.Pos]bool{}
	if helper {
		if !hasRuntimeCarrier(function) {
			return "", 0, fmt.Errorf("public helper has foreign carrier: %s", function.Name.Name)
		}
		field := function.Type.Params.List[1]
		edits = append(edits, edit{fset.Position(field.Pos()).Offset - start, fset.Position(field.End()).Offset - start, "endpoint string"})
	}
	calls := 0
	var invalid error
	ast.Inspect(function.Body, func(node ast.Node) bool {
		if helper {
			if selector, ok := node.(*ast.SelectorExpr); ok && nodeString(selector.X) == "rt" && selector.Sel.Name == "Endpoint" {
				permitted[selector.X.Pos()] = true
				edits = append(edits, edit{fset.Position(selector.Pos()).Offset - start, fset.Position(selector.End()).Offset - start, "endpoint"})
			}
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if !ok {
			return true
		}
		arity, selected := Helpers[name.Name]
		if !selected {
			return true
		}
		if len(call.Args) != arity || call.Ellipsis.IsValid() || nodeString(call.Args[0]) != "t" || nodeString(call.Args[1]) != "rt" || (name.Obj != nil && name.Obj.Kind != ast.Fun) {
			invalid = fmt.Errorf("unclassified public helper caller: %s/%s", function.Name.Name, name.Name)
			return false
		}
		permitted[call.Args[1].Pos()] = true
		replacement := "rt.Endpoint"
		if helper {
			replacement = "endpoint"
		}
		argument := call.Args[1]
		edits = append(edits, edit{fset.Position(argument.Pos()).Offset - start, fset.Position(argument.End()).Offset - start, replacement})
		calls++
		return true
	})
	if helper && invalid == nil {
		invalid = carrierEscape(function.Body, permitted)
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

func hasRuntimeCarrier(function *ast.FuncDecl) bool {
	if function.Recv != nil || len(function.Type.Params.List) < 2 {
		return false
	}
	field := function.Type.Params.List[1]
	return len(field.Names) == 1 && field.Names[0].Name == "rt" && nodeString(field.Type) == "servedControlProofRuntime"
}

func carrierEscape(body *ast.BlockStmt, permitted map[token.Pos]bool) error {
	var err error
	ast.Inspect(body, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && id.Name == "rt" && !permitted[id.Pos()] {
			err = fmt.Errorf("public helper uses runtime authority beyond its endpoint")
		}
		return true
	})
	return err
}

func nodeString(node ast.Node) string {
	var out bytes.Buffer
	if err := format.Node(&out, token.NewFileSet(), node); err != nil {
		panic(err)
	}
	return out.String()
}
