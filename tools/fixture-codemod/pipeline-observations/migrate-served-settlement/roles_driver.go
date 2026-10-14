package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
	"golang.org/x/tools/go/packages"
)

type servedRole struct {
	Fields []string
	Index  int
}

var servedRoleTypes = map[string]string{
	"Endpoint": "string", "Backend": "string", "BundleHash": "string",
	"ReceiverStateReader": "receiverProofStateReader",
	"ReadRunDeliveries":   "servedRunDeliveryReader",
	"Probe":               "*lifecycletest.Probe",
	"Runtime":             "*runtimepkg.Runtime",
}

func migrateRoles(write, withScalar bool) {
	root, err := os.Getwd()
	must(err)
	old := sourcePredecessor
	sourcePredecessor = servedsettlement.ScalarPredecessor
	defer func() { sourcePredecessor = old }()
	originals := signedSources(root)
	pkgs := load(root, originals)
	roles, uses := sourceClosedServedRoles(root, pkgs, originals)
	var typed map[string]string
	if withScalar {
		typed = scalarUses(root, originals)
	}
	outputs, pins := servedRoleCandidates(originals, roles, uses, typed)
	validateCandidate(root, outputs)
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	calls := 0
	for _, pin := range pins {
		calls += pin.Calls
		fmt.Printf("source\t%s\t%s\t%d\n", pin.File, pin.Function, pin.Calls)
	}
	names := make([]string, 0, len(roles))
	for name := range roles {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		fmt.Printf("role\t%s\t%d\t%s\n", name, roles[name].Index, strings.Join(roles[name].Fields, ","))
	}
	fmt.Printf("roles=%d bodies=%d rewritten-calls=%d sha256=%x\n", len(roles), len(pins), calls, sha256.Sum256(raw))
	if !write {
		return
	}
	for path, candidate := range outputs {
		must(os.WriteFile(path, candidate, 0644))
	}
	must(os.WriteFile("tools/fixture-codemod/pipeline-observations/family145-served-setup-role-preimages.json", raw, 0644))
}

func discoverServedRoles(pkgs []*packages.Package, blocked map[string]bool) map[string]servedRole {
	roles := map[string]servedRole{}
	for changed := true; changed; {
		changed = false
		for _, pkg := range pkgs {
			for _, file := range pkg.Syntax {
				for _, decl := range file.Decls {
					fn, ok := decl.(*ast.FuncDecl)
					if !ok || fn.Recv != nil || fn.Body == nil || blocked[fn.Name.Name] {
						continue
					}
					if _, exists := roles[fn.Name.Name]; exists {
						continue
					}
					role, ok := servedFunctionRole(pkg, fn, roles)
					if ok {
						roles[fn.Name.Name], changed = role, true
					}
				}
			}
		}
	}
	return roles
}

func sourceClosedServedRoles(root string, pkgs []*packages.Package, originals map[string][]byte) (map[string]servedRole, map[string]string) {
	blocked := map[string]bool{}
	for {
		roles := discoverServedRoles(pkgs, blocked)
		uses := typedRoleCalls(root, pkgs, roles)
		untyped := untypedServedRoleReferences(originals, roles, uses)
		if len(untyped) == 0 {
			return roles, uses
		}
		for name, path := range untyped {
			fmt.Printf("held-untyped-helper\t%s\t%s\n", name, path)
			blocked[name] = true
		}
	}
}

func untypedServedRoleReferences(originals map[string][]byte, roles map[string]servedRole, uses map[string]string) map[string]string {
	untyped := map[string]string{}
	for path, raw := range originals {
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, raw, parser.AllErrors)
		must(err)
		definitions := map[token.Pos]bool{}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil {
				definitions[fn.Name.Pos()] = true
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			id, ok := n.(*ast.Ident)
			if !ok {
				return true
			}
			if _, target := roles[id.Name]; !target || definitions[id.Pos()] {
				return true
			}
			key := fmt.Sprintf("%s:%d", path, set.Position(id.Pos()).Offset)
			if uses[key] != id.Name {
				untyped[id.Name] = path
			}
			return true
		})
	}
	return untyped
}

func servedFunctionRole(pkg *packages.Package, fn *ast.FuncDecl, known map[string]servedRole) (servedRole, bool) {
	original, index, fieldIndex := servedRoleParameter(pkg, fn)
	if original == nil {
		return servedRole{}, false
	}
	fields, allowed, valid := servedRoleFieldUses(pkg, fn, original, known)
	for id, object := range pkg.TypesInfo.Uses {
		if id.Pos() < fn.Body.Pos() || id.End() > fn.Body.End() {
			continue
		}
		if (object == original && !allowed[id]) || (id.Name == original.Name() && object != original) {
			valid = false
		}
	}
	if !valid || len(fields) == 0 {
		return servedRole{}, false
	}
	// Other raw arguments would create a new, still-raw signature.
	for i, field := range fn.Type.Params.List {
		if i != fieldIndex && strings.Contains(requestkey.Node(field.Type), "sql.") {
			return servedRole{}, false
		}
	}
	var names []string
	for field := range fields {
		names = append(names, field)
	}
	slices.Sort(names)
	return servedRole{Fields: names, Index: index}, true
}

func servedRoleParameter(pkg *packages.Package, fn *ast.FuncDecl) (types.Object, int, int) {
	index, fieldIndex := 0, -1
	var original types.Object
	for i, field := range fn.Type.Params.List {
		if len(field.Names) == 1 {
			object := pkg.TypesInfo.Defs[field.Names[0]]
			if object != nil && isServedRoleType(object.Type()) {
				if original != nil {
					return nil, 0, -1
				}
				original, fieldIndex = object, i
				continue
			}
		}
		if original == nil {
			index += len(field.Names)
		}
	}
	return original, index, fieldIndex
}

func servedRoleFieldUses(pkg *packages.Package, fn *ast.FuncDecl, original types.Object, known map[string]servedRole) (map[string]bool, map[*ast.Ident]bool, bool) {
	fields, allowed := map[string]bool{}, map[*ast.Ident]bool{}
	valid := true
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.SelectorExpr:
			id, ok := node.X.(*ast.Ident)
			if !ok || pkg.TypesInfo.Uses[id] != original {
				return true
			}
			if _, ok := servedRoleTypes[node.Sel.Name]; !ok {
				valid = false
				return true
			}
			fields[node.Sel.Name], allowed[id] = true, true
		case *ast.CallExpr:
			id, ok := node.Fun.(*ast.Ident)
			if !ok {
				return true
			}
			role, ok := known[id.Name]
			if !ok || len(node.Args) <= role.Index {
				return true
			}
			arg, ok := node.Args[role.Index].(*ast.Ident)
			if !ok || pkg.TypesInfo.Uses[arg] != original {
				return true
			}
			allowed[arg] = true
			for _, field := range role.Fields {
				fields[field] = true
			}
		}
		return true
	})
	return fields, allowed, valid
}

func isServedRoleType(t types.Type) bool {
	named, ok := t.(*types.Named)
	return ok && named.Obj().Name() == "servedControlProofRuntime" && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "github.com/division-sh/swarm/internal/serveapp"
}

func typedRoleCalls(root string, pkgs []*packages.Package, roles map[string]servedRole) map[string]string {
	uses := map[string]string{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				ident, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				role, ok := roles[ident.Name]
				if !ok {
					return true
				}
				fn, ok := pkg.TypesInfo.Uses[ident].(*types.Func)
				if !ok || fn.Pkg() == nil || fn.Pkg().Path() != "github.com/division-sh/swarm/internal/serveapp" {
					panic("shadowed served role call")
				}
				if len(call.Args) <= role.Index || !isServedRoleType(pkg.TypesInfo.TypeOf(call.Args[role.Index])) {
					panic("foreign served role argument")
				}
				_, err := servedRoleArgumentValues(call.Args[role.Index], role.Fields)
				if err != nil {
					panic(ident.Name + ": " + err.Error())
				}
				pos := pkg.Fset.Position(call.Pos())
				path, err := filepath.Rel(root, pos.Filename)
				must(err)
				uses[fmt.Sprintf("%s:%d", filepath.ToSlash(path), pos.Offset)] = ident.Name
				return true
			})
		}
	}
	return uses
}

func servedRoleCandidates(originals map[string][]byte, roles map[string]servedRole, uses, scalarTypes map[string]string) (map[string][]byte, []runphysical.Pin) {
	paths := make([]string, 0, len(originals))
	for path := range originals {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	outputs := map[string][]byte{}
	var pins []runphysical.Pin
	for _, path := range paths {
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, originals[path], parser.AllErrors)
		must(err)
		validateServedRoleSourceReferences(path, set, file, roles, uses)
		var rows []runphysical.Pin
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Body == nil {
				continue
			}
			edits, calls := servedRoleDeclarationEdits(path, set, fn, roles, uses, scalarTypes)
			if len(edits) == 0 {
				continue
			}
			start, end := set.Position(fn.Pos()).Offset, set.Position(fn.End()).Offset
			for i := range edits {
				edits[i].Start -= start
				edits[i].End -= start
			}
			after, err := requestkey.Normalize(string(apply(originals[path][start:end], edits)))
			must(err)
			before := requestkey.Node(fn)
			rows = append(rows, runphysical.Pin{File: path, Function: fn.Name.Name, Before: before, After: after, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(before))), Calls: calls})
		}
		if len(rows) == 0 {
			continue
		}
		out, err := destinationCandidate(path, rows)
		must(err)
		out = servedNativeRuntimeImport(out)
		if len(scalarFilePins(path, originals[path], scalarTypes)) > 0 {
			outputs[path] = scalarImports(out)
		} else {
			outputs[path], err = format.Source(out)
			must(err)
		}
		pins = append(pins, rows...)
	}
	return outputs, pins
}

func servedNativeRuntimeImport(raw []byte) []byte {
	const signature = "selectedProofRuntime *runtimepkg.Runtime"
	if !strings.Contains(string(raw), signature) {
		return raw
	}
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	must(err)
	const native = `"github.com/division-sh/swarm/internal/runtime"`
	for _, imported := range file.Imports {
		if imported.Path.Value != native {
			continue
		}
		alias := "runtime"
		if imported.Name != nil {
			alias = imported.Name.Name
		}
		return []byte(strings.ReplaceAll(string(raw), signature, "selectedProofRuntime *"+alias+".Runtime"))
	}
	if strings.Count(string(raw), "import (\n") != 1 {
		panic("native runtime role imports lost their exact group")
	}
	return []byte(strings.Replace(string(raw), "import (\n", "import (\nruntimepkg "+native+"\n", 1))
}

func validateServedRoleSourceReferences(path string, set *token.FileSet, file *ast.File, roles map[string]servedRole, uses map[string]string) {
	definitions := map[token.Pos]bool{}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil {
			definitions[fn.Name.Pos()] = true
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		id, ok := node.(*ast.Ident)
		if !ok {
			return true
		}
		if _, target := roles[id.Name]; !target || definitions[id.Pos()] {
			return true
		}
		key := fmt.Sprintf("%s:%d", path, set.Position(id.Pos()).Offset)
		if uses[key] != id.Name {
			var observed []string
			for source, name := range uses {
				if name == id.Name {
					observed = append(observed, source)
				}
			}
			slices.Sort(observed)
			panic(fmt.Sprintf("untyped/excluded/indirect served role reference: %s/%s; typed=%v", key, id.Name, observed))
		}
		return true
	})
}

func servedRoleDeclarationEdits(path string, set *token.FileSet, fn *ast.FuncDecl, roles map[string]servedRole, uses, scalarTypes map[string]string) ([]servedsettlement.Splice, int) {
	var edits []servedsettlement.Splice
	role, owner := roles[fn.Name.Name]
	param := ""
	if owner {
		index := 0
		for _, field := range fn.Type.Params.List {
			if index == role.Index {
				param = field.Names[0].Name
				var parameters []string
				for _, name := range role.Fields {
					parameters = append(parameters, roleParameter(name)+" "+servedRoleTypes[name])
				}
				edits = append(edits, servedsettlement.Splice{Start: set.Position(field.Pos()).Offset, End: set.Position(field.End()).Offset, After: strings.Join(parameters, ",")})
				break
			}
			index += len(field.Names)
		}
	}
	calls := 0
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if selector, ok := n.(*ast.SelectorExpr); ok && owner && requestkey.Node(selector.X) == param {
			edits = append(edits, servedsettlement.Splice{Start: set.Position(selector.Pos()).Offset, End: set.Position(selector.End()).Offset, After: roleParameter(selector.Sel.Name)})
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		key := fmt.Sprintf("%s:%d", path, set.Position(call.Pos()).Offset)
		if callee := uses[key]; callee != "" {
			calleeRole := roles[callee]
			arg := call.Args[calleeRole.Index]
			values, err := servedRoleArgumentValues(arg, calleeRole.Fields)
			must(err)
			if owner && requestkey.Node(arg) == param {
				for i, field := range calleeRole.Fields {
					values[i] = roleParameter(field)
				}
			}
			edits = append(edits, servedsettlement.Splice{Start: set.Position(arg.Pos()).Offset, End: set.Position(arg.End()).Offset, After: strings.Join(values, ",")})
			calls++
		}
		if result := scalarTypes[key]; result != "" {
			if edit, _, err := servedsettlement.ScalarCall(set, call, result); err == nil {
				edits = append(edits, edit)
				calls++
				return false
			}
		}
		return true
	})
	return edits, calls
}

func roleParameter(field string) string { return "selectedProof" + field }

func servedRoleArgumentValues(argument ast.Expr, fields []string) ([]string, error) {
	values := make([]string, 0, len(fields))
	_, identifier := argument.(*ast.Ident)
	embedded := false
	if selector, ok := argument.(*ast.SelectorExpr); ok && selector.Sel.Name == "servedControlProofRuntime" {
		_, embedded = selector.X.(*ast.Ident)
	}
	if identifier || embedded {
		for _, field := range fields {
			values = append(values, requestkey.Node(argument)+"."+field)
		}
		return values, nil
	}
	literal, ok := argument.(*ast.CompositeLit)
	if !ok || len(literal.Elts) != len(fields) {
		return nil, fmt.Errorf("served role would repeat evaluation or discard fields: %s", requestkey.Node(argument))
	}
	for i, item := range literal.Elts {
		field, ok := item.(*ast.KeyValueExpr)
		if !ok || requestkey.Node(field.Key) != fields[i] {
			return nil, fmt.Errorf("served role would reorder literal evaluation")
		}
		values = append(values, requestkey.Node(field.Value))
	}
	return values, nil
}
