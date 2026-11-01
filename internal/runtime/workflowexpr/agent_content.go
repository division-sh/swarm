package workflowexpr

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/google/cel-go/cel"
)

// The derived actor descriptor carries only the existing immutable CEL program,
// avoiding a dependency from the core actor/frame models back to contracts.
func PrepareAgentContentExpression(expression string, payload *contracts.ResolvedCatalogType) (cel.Program, error) {
	_, program, err := prepareCheckedValueExpression(expression, ValueExpressionOptions{AllowEventAt: true, PayloadType: payload})
	return program, err
}

func EvalAgentContentExpression(program cel.Program, ctx ValueContext) (any, error) {
	prepared := PreparedValueExpression{program: program}
	result, err := prepared.Eval(ctx)
	if err != nil {
		return nil, err
	}
	if !result.Present() {
		return nil, fmt.Errorf("agent content projection is absent")
	}
	return result.Value(), nil
}
