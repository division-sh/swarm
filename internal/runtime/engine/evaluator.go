package engine

type Evaluator interface {
	EvalBool(expression string, ctx BaseContext) (bool, error)
	EvalValue(expression string, ctx BaseContext) (any, error)
}

type NoopEvaluator struct{}

func (NoopEvaluator) EvalBool(string, BaseContext) (bool, error) {
	return false, ErrNotImplemented
}

func (NoopEvaluator) EvalValue(string, BaseContext) (any, error) {
	return nil, ErrNotImplemented
}
