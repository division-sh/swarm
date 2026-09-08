package engine

import runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"

type Evaluator interface {
	EvalBool(expression string, ctx BaseContext, payloadType *runtimecontracts.ResolvedCatalogType) (bool, error)
	EvalValue(expression string, ctx BaseContext) (any, error)
}

type NoopEvaluator struct{}

func (NoopEvaluator) EvalBool(string, BaseContext, *runtimecontracts.ResolvedCatalogType) (bool, error) {
	return false, ErrNotImplemented
}

func (NoopEvaluator) EvalValue(string, BaseContext) (any, error) {
	return nil, ErrNotImplemented
}
