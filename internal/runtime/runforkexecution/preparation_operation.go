package runforkexecution

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func detachedSelectedForkOperation(operation *runfork.ForkOperationRequest) (*runfork.ForkOperationRequest, error) {
	if operation == nil {
		return nil, nil
	}
	canonical, _, err := operation.Canonical()
	if err != nil {
		return nil, err
	}
	if canonical.ResolvedPoint != nil {
		point := *canonical.ResolvedPoint
		canonical.ResolvedPoint = &point
	}
	return &canonical, nil
}

func bindPreparedSelectedForkOperation(operation *runfork.ForkOperationRequest, plan runfork.RunForkPlan) (*runfork.ForkOperationRequest, error) {
	operation, err := detachedSelectedForkOperation(operation)
	if err != nil || operation == nil {
		return operation, err
	}
	point := plan.ForkPoint
	if err := point.Validate(); err != nil {
		return nil, err
	}
	if operation.SourceRunID != plan.SourceRunID ||
		(operation.ForkEventID != "" && operation.ForkEventID != point.EventID) ||
		(operation.ResolvedPoint != nil && *operation.ResolvedPoint != point) {
		return nil, fmt.Errorf("fork operation differs from prepared source revision")
	}
	operation.ResolvedPoint = &point
	return operation, nil
}
