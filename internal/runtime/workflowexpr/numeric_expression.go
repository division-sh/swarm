package workflowexpr

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
	celenv "github.com/google/cel-go/common/env"
	"github.com/google/cel-go/common/functions"
	"github.com/google/cel-go/common/overloads"
	celtypes "github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

const workflowNumericTypeName = "swarm.numeric"

// The checker must not confuse the closed int-or-double family with either
// double alone or an unrestricted dynamic value. Only checked dispatch is erased.
func workflowNumericType() *cel.Type { return cel.OpaqueType(workflowNumericTypeName) }

func workflowNumericCheckError(err error) error {
	if strings.Contains(err.Error(), workflowNumericTypeName) {
		return fmt.Errorf("%w; numeric-family arithmetic requires an explicit int(...) or double(...) conversion", err)
	}
	return err
}

type numericDispatch struct {
	function string
	id       string
	args     []*cel.Type
	result   *cel.Type
}

func workflowNumericDispatches() []numericDispatch {
	number := workflowNumericType()
	out := []numericDispatch{
		{"int", "swarm_numeric_to_int", []*cel.Type{number}, cel.IntType},
		{"double", "swarm_numeric_to_double", []*cel.Type{number}, cel.DoubleType},
		{"string", "swarm_numeric_to_string", []*cel.Type{number}, cel.StringType},
	}
	for _, op := range []struct {
		name string
		ids  []string
	}{
		{"_<_", []string{overloads.LessInt64, overloads.LessInt64Double, overloads.LessDoubleInt64, overloads.LessDouble}},
		{"_<=_", []string{overloads.LessEqualsInt64, overloads.LessEqualsInt64Double, overloads.LessEqualsDoubleInt64, overloads.LessEqualsDouble}},
		{"_>_", []string{overloads.GreaterInt64, overloads.GreaterInt64Double, overloads.GreaterDoubleInt64, overloads.GreaterDouble}},
		{"_>=_", []string{overloads.GreaterEqualsInt64, overloads.GreaterEqualsInt64Double, overloads.GreaterEqualsDoubleInt64, overloads.GreaterEqualsDouble}},
	} {
		for index, pair := range [][]*cel.Type{{number, number}, {number, cel.IntType}, {number, cel.DoubleType}, {cel.IntType, number}, {cel.DoubleType, number}} {
			out = append(out, numericDispatch{op.name, fmt.Sprintf("swarm_numeric_%s_%d", op.ids[0], index), pair, cel.BoolType})
		}
	}
	return out
}

func workflowNumericEnvOptions() []cel.EnvOption {
	var options []cel.EnvOption
	for _, dispatch := range workflowNumericDispatches() {
		options = append(options, cel.Function(dispatch.function, cel.Overload(dispatch.id, dispatch.args, dispatch.result)))
	}
	return options
}

func newWorkflowExpressionEnv(options ...cel.EnvOption) (*cel.Env, error) {
	// Stock equality uses one generic parameter, which cannot express the closed
	// numeric union. Keep its execution unchanged; validate the two checked operand
	// types below before planning. Other operators retain stock declarations.
	subset := celenv.NewLibrarySubset().AddExcludedFunctions([]*celenv.Function{
		celenv.NewFunction("_==_"), celenv.NewFunction("_!=_"),
	}...)
	base := []cel.EnvOption{cel.StdLib(cel.StdLibSubset(subset))}
	for _, op := range []struct{ function, id string }{{"_==_", overloads.Equals}, {"_!=_", overloads.NotEquals}} {
		base = append(base, cel.Function(op.function, cel.Overload(op.id,
			[]*cel.Type{cel.TypeParamType("L"), cel.TypeParamType("R")}, cel.BoolType)))
	}
	return cel.NewCustomEnv(append(base, options...)...)
}

func validateWorkflowEquality(left, right *cel.Type) error {
	if left.TypeName() == workflowNumericTypeName || right.TypeName() == workflowNumericTypeName {
		for _, operand := range []*cel.Type{left, right} {
			switch operand.TypeName() {
			case workflowNumericTypeName, "int", "double", "null_type", "dyn":
			default:
				return fmt.Errorf("numeric equality requires numeric operands, got %s and %s", left, right)
			}
		}
		return nil
	}
	// Delegate all non-numeric equality admission to CEL rather than creating
	// another interpretation of nullable, structural, or collection equality.
	env, err := cel.NewEnv(cel.Variable("lhs", workflowEqualityType(left)), cel.Variable("rhs", workflowEqualityType(right)))
	if err != nil {
		return err
	}
	_, issues := env.Compile("lhs == rhs")
	if issues != nil {
		return issues.Err()
	}
	return nil
}

func workflowEqualityType(t *cel.Type) *cel.Type {
	// Collections retain their previous homogeneous comparison contract, while
	// their runtime elements keep the admitted int/double kinds.
	switch t.TypeName() {
	case workflowNumericTypeName:
		return cel.DoubleType
	case "list":
		return cel.ListType(workflowEqualityType(t.Parameters()[0]))
	case "map":
		return cel.MapType(workflowEqualityType(t.Parameters()[0]), workflowEqualityType(t.Parameters()[1]))
	case "optional_type":
		return cel.OptionalType(workflowEqualityType(t.Parameters()[0]))
	default:
		return t
	}
}

func workflowTypeContains(t *cel.Type, name string) bool {
	if t == nil {
		return false
	}
	if t.TypeName() == name {
		return true
	}
	for _, param := range t.Parameters() {
		if workflowTypeContains(param, name) {
			return true
		}
	}
	return false
}

// CEL can unify heterogeneous conditionals/collections to dyn. Do not let that
// erase a declared numeric family and thereby authorize arbitrary string methods
// or bypass a typed sink. Opaque/untyped roots retain their separate contracts.
func validateWorkflowNumericEvidence(compiled *cel.Ast) error {
	native := compiled.NativeRep()
	var failure error
	celast.PostOrderVisit(native.Expr(), celast.NewExprVisitor(func(expr celast.Expr) {
		if failure != nil {
			return
		}
		if expr.Kind() == celast.CallKind {
			call := expr.AsCall()
			if (call.FunctionName() == "_==_" || call.FunctionName() == "_!=_") && len(call.Args()) == 2 {
				failure = validateWorkflowEquality(native.GetType(call.Args()[0].ID()), native.GetType(call.Args()[1].ID()))
			}
		}
		if failure != nil || !workflowTypeContains(native.GetType(expr.ID()), "dyn") {
			return
		}
		var inputs []celast.Expr
		switch expr.Kind() {
		case celast.CallKind:
			call := expr.AsCall()
			inputs = append(inputs, call.Args()...)
			if call.IsMemberFunction() {
				inputs = append(inputs, call.Target())
			}
		case celast.ListKind:
			inputs = expr.AsList().Elements()
		case celast.MapKind:
			for _, entry := range expr.AsMap().Entries() {
				inputs = append(inputs, entry.AsMapEntry().Key(), entry.AsMapEntry().Value())
			}
		}
		for _, input := range inputs {
			if workflowTypeContains(native.GetType(input.ID()), workflowNumericTypeName) {
				failure = fmt.Errorf("numeric expression loses its closed type; use an explicit int(...) or double(...) conversion before combining incompatible kinds")
				return
			}
		}
	}))
	return failure
}

func workflowProgram(env *cel.Env, compiled *cel.Ast) (cel.Program, error) {
	if err := validateWorkflowNumericEvidence(compiled); err != nil {
		return nil, err
	}
	// Bind only the closed checked overload IDs, after authored checking. Delegating
	// an erased reference to the whole stock function would also admit its string
	// conversions. Guard the family first; values and stock operators are unchanged.
	used := map[string]bool{}
	for _, reference := range compiled.NativeRep().ReferenceMap() {
		for _, id := range reference.OverloadIDs {
			used[id] = true
		}
	}
	functionsByName := env.Functions()
	var options []cel.EnvOption
	for _, dispatch := range workflowNumericDispatches() {
		if !used[dispatch.id] {
			continue
		}
		bindings, err := functionsByName[dispatch.function].Bindings()
		if err != nil {
			return nil, err
		}
		var stock *functions.Overload
		for _, binding := range bindings {
			if binding.Operator == dispatch.function {
				stock = binding
				break
			}
		}
		if stock == nil {
			return nil, fmt.Errorf("missing stock numeric dispatch for %s", dispatch.function)
		}
		guarded := func(args ...ref.Val) ref.Val {
			for _, arg := range args {
				switch arg.(type) {
				case celtypes.Int, celtypes.Double:
				default:
					return celtypes.NewErr("numeric operation requires an admitted integer or double, got %s", arg.Type())
				}
			}
			if stock.Function != nil {
				return stock.Function(args...)
			}
			if len(args) == 1 && stock.Unary != nil {
				return stock.Unary(args[0])
			}
			if len(args) == 2 && stock.Binary != nil {
				return stock.Binary(args[0], args[1])
			}
			return celtypes.NewErr("missing stock numeric operation binding")
		}
		options = append(options, cel.Function(dispatch.id,
			cel.Overload(dispatch.id, dispatch.args, dispatch.result), cel.SingletonFunctionBinding(guarded)))
	}
	if len(options) == 0 {
		return env.PlanProgram(compiled.NativeRep())
	}
	planEnv, err := env.Extend(options...)
	if err != nil {
		return nil, err
	}
	return planEnv.PlanProgram(compiled.NativeRep())
}
