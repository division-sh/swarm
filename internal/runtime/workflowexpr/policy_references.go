package workflowexpr

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
)

// PolicyReferenceError is a declaration failure, not a CEL admission failure.
type PolicyReferenceError struct {
	Paths []string
	Keys  []string
}

func (e *PolicyReferenceError) Error() string {
	return fmt.Sprintf("undeclared policy read %s; declared root keys: %s", strings.Join(e.Paths, ", "), strings.Join(e.Keys, ", "))
}

// ValidatePolicyReferences uses the same checked expression and presence dialect
// as execution. Policy remains literal data, not an inferred structural schema.
func ValidatePolicyReferences(expression string, policy map[string]any, opts ValueExpressionOptions) error {
	opts.DeclaredPolicy = policy
	return ValidateValueExpressionWithOptions(expression, opts)
}

func validateCheckedPolicyReferences(compiled *cel.Ast, policy map[string]any) error {
	missing := map[string]struct{}{}
	var visit func(celast.Expr, workflowPresenceFacts, bool)
	visit = func(expr celast.Expr, facts workflowPresenceFacts, shadowed bool) {
		if expr == nil {
			return
		}
		if !shadowed {
			if path, ok := policyStaticPath(expr); ok && len(path) != 0 && !policyPresenceRead(expr) {
				name := policyPathName(path)
				if _, proven := facts[name]; !proven && !policyPathExists(policy, path) {
					missing[name] = struct{}{}
				}
			}
		}
		switch expr.Kind() {
		case celast.SelectKind:
			visit(expr.AsSelect().Operand(), facts, shadowed)
		case celast.CallKind:
			call := expr.AsCall()
			args := call.Args()
			switch call.FunctionName() {
			case "_&&_", "_||_":
				if len(args) == 2 {
					visit(args[0], facts, shadowed)
					visit(args[1], mergeWorkflowPresenceFacts(facts, policyFactsWhen(args[0], call.FunctionName() == "_&&_")), shadowed)
					return
				}
			case "_?_:_":
				if len(args) == 3 {
					visit(args[0], facts, shadowed)
					visit(args[1], mergeWorkflowPresenceFacts(facts, policyFactsWhen(args[0], true)), shadowed)
					visit(args[2], mergeWorkflowPresenceFacts(facts, policyFactsWhen(args[0], false)), shadowed)
					return
				}
			}
			if call.IsMemberFunction() {
				visit(call.Target(), facts, shadowed)
			}
			for _, arg := range args {
				visit(arg, facts, shadowed)
			}
		case celast.ComprehensionKind:
			c := expr.AsComprehension()
			visit(c.IterRange(), facts, shadowed)
			visit(c.AccuInit(), facts, shadowed)
			bodyShadow := shadowed || c.IterVar() == "policy" || c.IterVar2() == "policy" || c.AccuVar() == "policy"
			bodyFacts := facts
			if bodyShadow {
				bodyFacts = nil
			}
			visit(c.LoopCondition(), bodyFacts, bodyShadow)
			visit(c.LoopStep(), bodyFacts, bodyShadow)
			visit(c.Result(), facts, shadowed || c.AccuVar() == "policy")
		case celast.ListKind:
			for _, item := range expr.AsList().Elements() {
				visit(item, facts, shadowed)
			}
		case celast.MapKind:
			for _, item := range expr.AsMap().Entries() {
				visit(item.AsMapEntry().Key(), facts, shadowed)
				visit(item.AsMapEntry().Value(), facts, shadowed)
			}
		case celast.StructKind:
			for _, item := range expr.AsStruct().Fields() {
				visit(item.AsStructField().Value(), facts, shadowed)
			}
		}
	}
	visit(compiled.NativeRep().Expr(), nil, false)
	if len(missing) == 0 {
		return nil
	}
	failure := &PolicyReferenceError{}
	for path := range missing {
		failure.Paths = append(failure.Paths, path)
	}
	for key := range policy {
		failure.Keys = append(failure.Keys, strconv.Quote(key))
	}
	sort.Strings(failure.Paths)
	sort.Strings(failure.Keys)
	return failure
}

func policyPresenceRead(expr celast.Expr) bool {
	if expr.Kind() == celast.SelectKind {
		return expr.AsSelect().IsTestOnly()
	}
	if expr.Kind() == celast.CallKind {
		switch expr.AsCall().FunctionName() {
		case "_?._", "_[?_]":
			return true
		}
	}
	return false
}

// Paths preserve arbitrary string keys and integer indexes without dot splitting.
func policyStaticPath(expr celast.Expr) ([]any, bool) {
	switch expr.Kind() {
	case celast.IdentKind:
		return nil, expr.AsIdent() == "policy"
	case celast.SelectKind:
		selection := expr.AsSelect()
		parent, ok := policyStaticPath(selection.Operand())
		return append(parent, selection.FieldName()), ok
	case celast.CallKind:
		call := expr.AsCall()
		args := call.Args()
		switch call.FunctionName() {
		case "_[_]", "_[?_]", "_?._":
			if len(args) == 2 && args[1].Kind() == celast.LiteralKind {
				parent, ok := policyStaticPath(args[0])
				switch key := args[1].AsLiteral().Value().(type) {
				case string, int64:
					return append(parent, key), ok
				}
			}
		}
	}
	return nil, false
}

func policyPathName(path []any) string {
	name := "policy"
	for _, key := range path {
		switch value := key.(type) {
		case string:
			name += "[" + strconv.Quote(value) + "]"
		case int64:
			name += "[" + strconv.FormatInt(value, 10) + "]"
		}
	}
	return name
}

func policyPathExists(policy map[string]any, path []any) bool {
	var current any = policy
	for _, key := range path {
		switch container := current.(type) {
		case map[string]any:
			field, ok := key.(string)
			if !ok {
				return false
			}
			current, ok = container[field]
			if !ok {
				return false
			}
		case []any:
			index, ok := key.(int64)
			if !ok || index < 0 || index >= int64(len(container)) {
				return false
			}
			current = container[index]
		default:
			return false
		}
	}
	return true
}

func policyFactsWhen(expr celast.Expr, truth bool) workflowPresenceFacts {
	if expr.Kind() == celast.SelectKind && expr.AsSelect().IsTestOnly() && truth {
		if path, ok := policyStaticPath(expr); ok {
			return workflowPresenceFacts{policyPathName(path): {}}
		}
	}
	if expr.Kind() != celast.CallKind {
		return nil
	}
	call := expr.AsCall()
	args := call.Args()
	switch call.FunctionName() {
	case "!_":
		if len(args) == 1 {
			return policyFactsWhen(args[0], !truth)
		}
	case "_&&_", "_||_":
		if len(args) == 2 {
			and := call.FunctionName() == "_&&_"
			if truth == and {
				return mergeWorkflowPresenceFacts(policyFactsWhen(args[0], truth), policyFactsWhen(args[1], truth))
			}
			return intersectWorkflowPresenceFacts(policyFactsWhen(args[0], truth), mergeWorkflowPresenceFacts(policyFactsWhen(args[0], !truth), policyFactsWhen(args[1], truth)))
		}
	case "_?_:_":
		if len(args) == 3 {
			return intersectWorkflowPresenceFacts(mergeWorkflowPresenceFacts(policyFactsWhen(args[0], true), policyFactsWhen(args[1], truth)), mergeWorkflowPresenceFacts(policyFactsWhen(args[0], false), policyFactsWhen(args[2], truth)))
		}
	}
	return nil
}
