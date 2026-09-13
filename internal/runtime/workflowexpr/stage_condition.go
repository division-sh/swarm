package workflowexpr

import (
	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
)

// ConditionCanHoldAtStage narrows an existing topology scope using only literal
// _entity.current_state comparisons and boolean composition in CEL's stock AST. Unknown
// business predicates retain both outcomes; event names confer no stage facts.
// The ordinary expression checker remains responsible for admission/type errors.
func ConditionCanHoldAtStage(expression, stage string, truth bool) bool {
	env, err := cel.NewEnv(cel.OptionalTypes())
	if err != nil {
		return true
	}
	parsed, issues := env.Parse(RewriteLoopRoot(expression))
	if issues != nil && issues.Err() != nil || parsed == nil || parsed.NativeRep() == nil {
		return true
	}
	mask := stageConditionOutcomes(parsed.NativeRep().Expr(), stage)
	if truth {
		return mask&stageConditionTrue != 0
	}
	return mask&stageConditionFalse != 0
}

const (
	stageConditionFalse   uint8 = 1
	stageConditionTrue    uint8 = 2
	stageConditionUnknown       = stageConditionFalse | stageConditionTrue
)

func stageConditionOutcomes(expr celast.Expr, stage string) uint8 {
	if expr.Kind() == celast.LiteralKind {
		if value, ok := expr.AsLiteral().Value().(bool); ok {
			return stageConditionBool(value)
		}
	}
	if expr.Kind() != celast.CallKind {
		return stageConditionUnknown
	}
	call := expr.AsCall()
	args := call.Args()
	if call.FunctionName() == "!_" && len(args) == 1 {
		mask := stageConditionOutcomes(args[0], stage)
		return (mask&stageConditionTrue)>>1 | (mask&stageConditionFalse)<<1
	}
	if len(args) != 2 {
		return stageConditionUnknown
	}
	switch call.FunctionName() {
	case "_==_", "_!=_":
		for index := range args {
			path, ok := workflowSelectionPath(args[index])
			other := args[1-index]
			if !ok || path != "_entity.current_state" || other.Kind() != celast.LiteralKind {
				continue
			}
			value, ok := other.AsLiteral().Value().(string)
			if ok {
				return stageConditionBool((value == stage) == (call.FunctionName() == "_==_"))
			}
		}
	case "_&&_", "_||_":
		left, right := stageConditionOutcomes(args[0], stage), stageConditionOutcomes(args[1], stage)
		var outcomes uint8
		for _, a := range []bool{false, true} {
			for _, b := range []bool{false, true} {
				if left&stageConditionBool(a) != 0 && right&stageConditionBool(b) != 0 {
					if call.FunctionName() == "_&&_" {
						outcomes |= stageConditionBool(a && b)
					} else {
						outcomes |= stageConditionBool(a || b)
					}
				}
			}
		}
		return outcomes
	}
	return stageConditionUnknown
}

func stageConditionBool(value bool) uint8 {
	if value {
		return stageConditionTrue
	}
	return stageConditionFalse
}
