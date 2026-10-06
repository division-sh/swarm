package main

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/printer"
	"go/token"
	"go/types"
	"strings"
)

func exprString(node ast.Node) string {
	var out bytes.Buffer
	if err := format.Node(&out, token.NewFileSet(), node); err != nil {
		panic(err)
	}
	return out.String()
}

func notifyExecutionPoolSlot(name string) (int, int, bool) {
	switch name {
	case "assertNotifyAllChildrenCompletedTurns", "countNotifyAllChildrenLifecycleTransitions":
		return 3, 7, true
	case "assertNotifyAllChildrenRunPersisted", "loadNotifyAllChildrenFailure", "assertNotifyAllChildrenFlowInstanceCount":
		return 3, 5, true
	case "waitNotifyAllChildrenFanOutCursor":
		return 2, 5, true
	}
	return 0, 0, false
}

func notifyExecutionReadFunction(name string) string {
	switch name {
	case "assertNotifyAllChildrenCompletedTurns":
		return "ReadNotifyCompletedTurns"
	case "countNotifyAllChildrenLifecycleTransitions":
		return "ReadNotifyLifecycleTransitionCount"
	case "assertNotifyAllChildrenRunPersisted":
		return "ReadNotifyRunPresence"
	case "loadNotifyAllChildrenFailure":
		return "ReadNotifyLatestEventFailure"
	case "assertNotifyAllChildrenFlowInstanceCount":
		return "ReadNotifyFlowInstanceCount"
	case "waitNotifyAllChildrenFanOutCursor":
		return "ReadNotifyFanOutCursor"
	}
	return ""
}

func rewriteNotifyExecutionOwner(fset *token.FileSet, info *types.Info, fn *ast.FuncDecl, source []byte) (string, bool) {
	index, arity, ok := notifyExecutionPoolSlot(fn.Name.Name)
	if !ok || fn.Type.Params.NumFields() != arity {
		return "", false
	}
	parameter := fn.Type.Params.List[index]
	if len(parameter.Names) != 1 || parameter.Names[0].Name != "db" || info.TypeOf(parameter.Type) == nil || info.TypeOf(parameter.Type).String() != "*database/sql.DB" {
		return "", false
	}
	unused, native := true, false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && id.Name == "db" {
			unused = false
		}
		if call, ok := node.(*ast.CallExpr); ok {
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
				if object, ok := info.Uses[selector.Sel].(*types.Func); ok && object.Pkg() != nil && object.Pkg().Path() == storetestPackage && object.Name() == notifyExecutionReadFunction(fn.Name.Name) {
					native = true
				}
			}
		}
		return true
	})
	if !unused || !native {
		return "", false
	}
	lo, hi := fset.Position(parameter.Pos()).Offset, fset.Position(parameter.End()).Offset
	for hi < len(source) && (source[hi] == ' ' || source[hi] == '\t' || source[hi] == '\n') {
		hi++
	}
	if hi < len(source) && source[hi] == ',' {
		hi++
	}
	start, end := fset.Position(fn.Pos()).Offset, fset.Position(fn.End()).Offset
	out := append(append(append([]byte{}, source[start:lo]...), source[hi:end]...), '\n')
	formatted, err := format.Source(append([]byte("package conformance\n"), out...))
	if err != nil {
		panic(err)
	}
	return strings.TrimPrefix(string(formatted), "package conformance\n\n"), true
}

func matchNotifyExecutionCaller(info *types.Info, call *ast.CallExpr) (int, bool) {
	id, ok := call.Fun.(*ast.Ident)
	if !ok {
		return 0, false
	}
	object, ok := info.Uses[id].(*types.Func)
	if !ok || object.Pkg() == nil || object.Pkg().Path() != conformancePackage {
		return 0, false
	}
	index, arity, ok := notifyExecutionPoolSlot(object.Name())
	if !ok || len(call.Args) != arity {
		return 0, false
	}
	signature, ok := object.Type().(*types.Signature)
	if !ok || signature.Params().Len() != arity || signature.Params().At(index).Type().String() != "*database/sql.DB" {
		return 0, false
	}
	if info.TypeOf(call.Args[index]) == nil || info.TypeOf(call.Args[index]).String() != "*database/sql.DB" {
		return 0, false
	}
	switch call.Args[index].(type) {
	case *ast.Ident, *ast.SelectorExpr:
		return index, true
	}
	return 0, false
}

// A pool binding becomes a discard only if every use was an audited pure pool
// argument removed by this exact propagation. Other reads/writes keep it live.
func rewriteNotifyExecutionPoolBinding(fset *token.FileSet, info *types.Info, fn *ast.FuncDecl, comments []*ast.CommentGroup) (string, bool) {
	uses, removed := notifyExecutionPoolUses(info, fn)
	changed := removeNotifyExecutionPoolArguments(info, fn)
	discardNotifyExecutionPoolBindings(info, fn, uses, removed)
	if !changed {
		return "", false
	}
	var out bytes.Buffer
	var retained []*ast.CommentGroup
	for _, comment := range comments {
		if comment.Pos() >= fn.Pos() && comment.End() <= fn.End() {
			retained = append(retained, comment)
		}
	}
	if err := format.Node(&out, fset, &printer.CommentedNode{Node: fn, Comments: retained}); err != nil {
		panic(err)
	}
	return out.String(), true
}

func notifyExecutionPoolUses(info *types.Info, fn *ast.FuncDecl) (map[types.Object]int, map[types.Object]int) {
	uses, removed := map[types.Object]int{}, map[types.Object]int{}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && info.Uses[id] != nil {
			uses[info.Uses[id]]++
		}
		if call, ok := node.(*ast.CallExpr); ok {
			if index, ok := matchNotifyExecutionCaller(info, call); ok {
				if id, ok := call.Args[index].(*ast.Ident); ok {
					removed[info.Uses[id]]++
				}
			}
		}
		return true
	})
	return uses, removed
}

func removeNotifyExecutionPoolArguments(info *types.Info, fn *ast.FuncDecl) bool {
	changed := false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if index, ok := matchNotifyExecutionCaller(info, call); ok {
				call.Args = append(call.Args[:index], call.Args[index+1:]...)
				changed = true
			}
		}
		return true
	})
	return changed
}

func discardNotifyExecutionPoolBindings(info *types.Info, fn *ast.FuncDecl, uses, removed map[types.Object]int) {
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if !ok || assignment.Tok != token.DEFINE || len(assignment.Lhs) < 2 {
			return true
		}
		for _, expression := range assignment.Lhs {
			id, ok := expression.(*ast.Ident)
			if !ok {
				continue
			}
			object := info.Defs[id]
			if object != nil && object.Type().String() == "*database/sql.DB" && removed[object] > 0 && uses[object] == removed[object] {
				id.Name = "_"
			}
		}
		return true
	})
}
