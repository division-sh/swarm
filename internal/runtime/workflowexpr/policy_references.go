package workflowexpr

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
	celtypes "github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// PolicyReferenceError is a declaration failure, not a CEL admission failure.
type PolicyReferenceError struct {
	Paths       []string
	Keys        []string
	Suggestions []string
}

func (e *PolicyReferenceError) Error() string {
	message := fmt.Sprintf("undeclared policy read %s; declared root keys: %s", strings.Join(e.Paths, ", "), strings.Join(e.Keys, ", "))
	for _, suggestion := range e.Suggestions {
		message += "; did you mean " + suggestion + "?"
	}
	return message
}

// ValidatePolicyReferences uses the same checked expression and presence dialect
// as execution. Policy remains literal data, not an inferred structural schema.
func ValidatePolicyReferences(expression string, policy map[string]any, opts ValueExpressionOptions) error {
	if policy == nil {
		policy = map[string]any{}
	}
	opts.DeclaredPolicy = policy
	return ValidateValueExpressionWithOptions(expression, opts)
}

// IsStaticPolicyReference shares checked CEL path identity with declaration
// checking. Query operands remain static selectors, not a second expression language.
func IsStaticPolicyReference(expression string) (bool, error) {
	compiled, err := checkedValueExpression(expression, ValueExpressionOptions{})
	if err != nil {
		return false, err
	}
	if compiled.OutputType().TypeName() == "optional_type" {
		return false, nil
	}
	_, optional, ok := policyStaticPath(compiled.NativeRep().Expr())
	return ok && !optional, nil
}

func validateCheckedPolicyReferences(compiled *cel.Ast, policy map[string]any) error {
	native := compiled.NativeRep()
	missing := map[string]string{}
	var visit func(celast.Expr, bool)
	visit = func(expr celast.Expr, shadowed bool) {
		if expr == nil {
			return
		}
		if !shadowed {
			collectPolicyRead(expr, native, policy, missing)
		}
		switch expr.Kind() {
		case celast.SelectKind:
			visit(expr.AsSelect().Operand(), shadowed)
		case celast.CallKind:
			visitPolicyCall(expr.AsCall(), policy, shadowed, visit)
		case celast.ComprehensionKind:
			c := expr.AsComprehension()
			visit(c.IterRange(), shadowed)
			visit(c.AccuInit(), shadowed)
			bodyShadow := shadowed || c.IterVar() == "policy" || c.IterVar2() == "policy" || c.AccuVar() == "policy"
			visit(c.LoopCondition(), bodyShadow)
			visit(c.LoopStep(), bodyShadow)
			visit(c.Result(), shadowed || c.AccuVar() == "policy")
		case celast.ListKind:
			for _, item := range expr.AsList().Elements() {
				visit(item, shadowed)
			}
		case celast.MapKind:
			for _, item := range expr.AsMap().Entries() {
				visit(item.AsMapEntry().Key(), shadowed)
				visit(item.AsMapEntry().Value(), shadowed)
			}
		case celast.StructKind:
			for _, item := range expr.AsStruct().Fields() {
				visit(item.AsStructField().Value(), shadowed)
			}
		}
	}
	visit(native.Expr(), false)
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
	for _, path := range failure.Paths {
		if suggestion := missing[path]; suggestion != "" {
			failure.Suggestions = append(failure.Suggestions, suggestion)
		}
	}
	return failure
}

func collectPolicyRead(expr celast.Expr, native *celast.AST, policy map[string]any, missing map[string]string) {
	path, optional, ok := policyStaticPath(expr)
	if !ok || len(path) == 0 {
		return
	}
	_, present, valid := policyPathValue(policy, path)
	if present || (valid && (optional || policyPresenceRead(expr) || native.GetType(expr.ID()).TypeName() == "optional_type")) {
		return
	}
	missing[policyPathName(path)] = nearestPolicyPath(policy, path)
}

func visitPolicyCall(call celast.CallExpr, policy map[string]any, shadowed bool, visit func(celast.Expr, bool)) {
	args := call.Args()
	var truth, known bool
	if !shadowed && len(args) > 0 {
		truth, known = policyPresenceTruth(args[0], policy)
	}
	switch call.FunctionName() {
	case "_&&_", "_||_":
		if len(args) == 2 {
			visit(args[0], shadowed)
			if !known || truth == (call.FunctionName() == "_&&_") {
				visit(args[1], shadowed)
			}
			return
		}
	case "_?_:_":
		if len(args) == 3 {
			visit(args[0], shadowed)
			if !known || truth {
				visit(args[1], shadowed)
			}
			if !known || !truth {
				visit(args[2], shadowed)
			}
			return
		}
	}
	if call.IsMemberFunction() {
		visit(call.Target(), shadowed)
	}
	for _, arg := range args {
		visit(arg, shadowed)
	}
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

// Keep CEL literal kinds intact, including unsigned and non-index selectors.
func policyStaticPath(expr celast.Expr) ([]ref.Val, bool, bool) {
	switch expr.Kind() {
	case celast.IdentKind:
		return nil, false, expr.AsIdent() == "policy"
	case celast.SelectKind:
		selection := expr.AsSelect()
		parent, optional, ok := policyStaticPath(selection.Operand())
		return append(parent, celtypes.String(selection.FieldName())), optional, ok
	case celast.CallKind:
		call := expr.AsCall()
		args := call.Args()
		switch call.FunctionName() {
		case "_[_]", "_[?_]", "_?._":
			if len(args) == 2 && args[1].Kind() == celast.LiteralKind {
				parent, optional, ok := policyStaticPath(args[0])
				return append(parent, args[1].AsLiteral()), optional || call.FunctionName() != "_[_]", ok
			}
		}
	}
	return nil, false, false
}

func policyPathName(path []ref.Val) string {
	name := "policy"
	for _, key := range path {
		switch value := key.(type) {
		case celtypes.String:
			name += "[" + strconv.Quote(string(value)) + "]"
		case celtypes.Uint:
			name += "[" + strconv.FormatUint(uint64(value), 10) + "u]"
		case celtypes.Double:
			literal := strconv.FormatFloat(float64(value), 'g', -1, 64)
			if !strings.ContainsAny(literal, ".eE") {
				literal += ".0"
			}
			name += "[" + literal + "]"
		case celtypes.Null:
			name += "[null]"
		case celtypes.Bytes:
			name += "[b" + strconv.Quote(string(value)) + "]"
		default:
			name += fmt.Sprintf("[%v]", value)
		}
	}
	return name
}

func policyPathValue(policy map[string]any, path []ref.Val) (value any, present, valid bool) {
	var current any = policy
	for _, key := range path {
		switch container := current.(type) {
		case map[string]any:
			field, ok := key.(celtypes.String)
			if !ok {
				return nil, false, true
			}
			current, ok = container[string(field)]
			if !ok {
				return nil, false, true
			}
		case []any:
			index, err := celtypes.IndexOrError(key)
			if err != nil || celtypes.Int(index).Equal(key) != celtypes.True {
				return nil, false, false
			}
			if index < 0 || index >= len(container) {
				return nil, false, true
			}
			current = container[index]
		default:
			return nil, false, true
		}
	}
	return current, true, true
}

// Resolve only presence decisions over the immutable direct policy root. This
// is not evaluation or dataflow analysis of arbitrary CEL objects/aliases.
func policyPresenceTruth(expr celast.Expr, policy map[string]any) (truth, known bool) {
	if expr.Kind() == celast.SelectKind && expr.AsSelect().IsTestOnly() {
		if path, _, ok := policyStaticPath(expr); ok {
			_, present, valid := policyPathValue(policy, path)
			return present, valid
		}
	}
	if expr.Kind() != celast.CallKind {
		return false, false
	}
	call := expr.AsCall()
	args := call.Args()
	switch call.FunctionName() {
	case "!_":
		if len(args) == 1 {
			truth, known := policyPresenceTruth(args[0], policy)
			return !truth, known
		}
	case "_&&_", "_||_":
		if len(args) == 2 {
			left, leftKnown := policyPresenceTruth(args[0], policy)
			right, rightKnown := policyPresenceTruth(args[1], policy)
			absorbing := call.FunctionName() == "_||_"
			if (leftKnown && left == absorbing) || (rightKnown && right == absorbing) {
				return absorbing, true
			}
			return !absorbing, leftKnown && rightKnown
		}
	case "_?_:_":
		if len(args) != 3 {
			return false, false
		}
		test, known := policyPresenceTruth(args[0], policy)
		if !known {
			return false, false
		}
		if test {
			return policyPresenceTruth(args[1], policy)
		}
		return policyPresenceTruth(args[2], policy)
	}
	return false, false
}

func nearestPolicyPath(policy map[string]any, path []ref.Val) string {
	for i, key := range path {
		parent, present, valid := policyPathValue(policy, path[:i])
		fields, object := parent.(map[string]any)
		name, text := key.(celtypes.String)
		if !present || !valid || !object || !text {
			return ""
		}
		if _, exists := fields[string(name)]; exists {
			continue
		}
		best, bestDistance := "", -1
		for candidate := range fields {
			distance := policyKeyDistance(string(name), candidate)
			if bestDistance < 0 || distance < bestDistance || (distance == bestDistance && candidate < best) {
				best, bestDistance = candidate, distance
			}
		}
		if bestDistance >= 0 {
			return policyPathName(append(path[:i:i], celtypes.String(best)))
		}
		return ""
	}
	return ""
}

// Suggestions never normalize, replace or authorize an authored key.
func policyKeyDistance(left, right string) int {
	a, b := []rune(left), []rune(right)
	previous := make([]int, len(b)+1)
	for i := range previous {
		previous[i] = i
	}
	for i, x := range a {
		row := make([]int, len(b)+1)
		row[0] = i + 1
		for j, y := range b {
			cost := 0
			if x != y {
				cost = 1
			}
			row[j+1] = min(row[j]+1, previous[j+1]+1, previous[j]+cost)
		}
		previous = row
	}
	return previous[len(b)]
}
