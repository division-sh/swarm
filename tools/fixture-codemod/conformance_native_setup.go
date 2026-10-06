package main

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/token"
	"go/types"
)

func ignoredConformanceDatabase(info *types.Info, fn *ast.FuncDecl) (*ast.Field, bool) {
	if fn.Body == nil || fn.Type.Params == nil || len(fn.Type.Params.List) < 4 {
		return nil, false
	}
	wantUses := 0
	switch fn.Name.Name {
	case "newFanInBarrierRuntimeForSource":
	case "newFanInBarrierRuntime":
		wantUses = 1
	default:
		return nil, false
	}
	field := fn.Type.Params.List[2]
	if len(field.Names) != 1 || field.Names[0].Name != "db" || !sqlDatabasePointer(info.TypeOf(field.Type)) {
		return nil, false
	}
	object := info.Defs[field.Names[0]]
	uses, forwarded := 0, 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && info.Uses[id] == object {
			uses++
		}
		if call, ok := node.(*ast.CallExpr); ok && ignoredConformanceDatabaseCall(info, call) {
			callee, _ := call.Fun.(*ast.Ident)
			if id, ok := call.Args[2].(*ast.Ident); ok && info.Uses[id] == object && callee.Name == "newFanInBarrierRuntimeForSource" {
				forwarded++
			}
		}
		return true
	})
	if uses != wantUses || forwarded != wantUses {
		return nil, false
	}
	return field, true
}

func sqlDatabasePointer(value types.Type) bool {
	if value == nil {
		return false
	}
	pointer, ok := types.Unalias(value).(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := types.Unalias(pointer.Elem()).(*types.Named)
	return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "database/sql" && named.Obj().Name() == "DB"
}

func ignoredConformanceDatabaseCall(info *types.Info, call *ast.CallExpr) bool {
	id, ok := call.Fun.(*ast.Ident)
	if !ok || len(call.Args) < 4 {
		return false
	}
	fn, ok := info.Uses[id].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != conformancePackage {
		return false
	}
	switch fn.Name() {
	case "newFanInBarrierRuntime", "newFanInBarrierRuntimeForSource":
	default:
		return false
	}
	sig := fn.Type().(*types.Signature)
	if sig.Params().Len() < 4 || !sqlDatabasePointer(sig.Params().At(2).Type()) {
		return false
	}
	return pureFixtureReference(call.Args[2])
}

// Narrow the whole finite family only when the terminal owner and every use
// are proven safe. A forwarding wrapper alone does not prove an unused DB.
func ignoredConformanceDatabaseFamily(info *types.Info, files []*ast.File) map[*types.Func]bool {
	functions := map[*types.Func]bool{}
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || (fn.Name.Name != "newFanInBarrierRuntimeForSource" && fn.Name.Name != "newFanInBarrierRuntime") {
				continue
			}
			object, ok := info.Defs[fn.Name].(*types.Func)
			if !ok || object.Pkg() == nil || object.Pkg().Path() != conformancePackage || fn.Recv != nil {
				return nil
			}
			if _, ok := ignoredConformanceDatabase(info, fn); !ok {
				return nil
			}
			functions[object] = true
		}
	}
	if len(functions) != 2 {
		return nil
	}
	valid := true
	for _, file := range files {
		directUses := map[*ast.Ident]bool{}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if !ok {
				return true
			}
			object, _ := info.Uses[id].(*types.Func)
			if functions[object] {
				directUses[id] = true
				valid = valid && ignoredConformanceDatabaseCall(info, call)
			}
			return true
		})
		ast.Inspect(file, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok {
				object, _ := info.Uses[id].(*types.Func)
				if functions[object] && !directUses[id] {
					valid = false
				}
			}
			return true
		})
	}
	if !valid {
		return nil
	}
	return functions
}

func rewriteIgnoredConformanceDatabase(fset *token.FileSet, info *types.Info, fn *ast.FuncDecl, family map[*types.Func]bool) (string, bool) {
	object, _ := info.Defs[fn.Name].(*types.Func)
	if !family[object] {
		return "", false
	}
	if _, ok := ignoredConformanceDatabase(info, fn); !ok {
		return "", false
	}
	fn.Type.Params.List = append(fn.Type.Params.List[:2], fn.Type.Params.List[3:]...)
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok && ignoredConformanceDatabaseCall(info, call) {
			call.Args = append(call.Args[:2], call.Args[3:]...)
		}
		return true
	})
	var out bytes.Buffer
	if err := format.Node(&out, fset, fn); err != nil {
		panic(err)
	}
	return out.String(), true
}
