package engine

import (
	"fmt"
	"strings"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

// Evaluation stays with the expression owner; mutation semantics live in entityruntime.
func evaluateContainedMutation(frame *executionFrame, current BaseContext, write rc.WorkflowDataWrite) (entityruntime.Mutation, error) {
	op := entityruntime.Mutation{Operation: string(write.Operation), Target: write.Target(),
		HasKey: !write.Key.IsZero(), HasIndex: !write.Index.IsZero()}
	var err error
	if op.HasKey {
		op.Key, err = evalRequiredDataOperationExpression(current, frame.state, write.Key, "key", frameExpressionOptions(frame))
		if err != nil {
			return op, err
		}
	}
	if op.HasIndex {
		op.Index, err = evalRequiredDataOperationExpression(current, frame.state, write.Index, "index", frameExpressionOptions(frame))
		if err != nil {
			return op, err
		}
	}
	if write.Operation != rc.WorkflowDataOperationDelete {
		op.Value, err = evalRequiredDataOperationExpression(current, frame.state, write.Value, "value", frameExpressionOptions(frame))
	}
	return op, err
}

func evalRequiredDataOperationExpression(base BaseContext, state ExecutionState, expr rc.ExpressionValue, label string, opts workflowexpr.ValueExpressionOptions) (any, error) {
	value, ok, err := evalExpressionValue(base, state, expr, opts)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%s expression did not resolve", strings.TrimSpace(label))
	}
	return value, nil
}
