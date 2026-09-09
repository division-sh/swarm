package engine

import "github.com/division-sh/swarm/internal/runtime/workflowexpr"

type Evaluator interface {
	EvalBool(expression string, ctx BaseContext, options workflowexpr.ValueExpressionOptions) (bool, error)
	EvalValue(expression string, ctx BaseContext) (any, error)
}

type NoopEvaluator struct{}

func (NoopEvaluator) EvalBool(string, BaseContext, workflowexpr.ValueExpressionOptions) (bool, error) {
	return false, ErrNotImplemented
}

func (NoopEvaluator) EvalValue(string, BaseContext) (any, error) {
	return nil, ErrNotImplemented
}
