package engine

import (
	"context"
	"reflect"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/identity"
)

type DeclarativeNode struct {
	nodeID   identity.NodeID
	executor *Executor
}

func NewDeclarativeNode(nodeID string, executor *Executor) *DeclarativeNode {
	if executor == nil {
		return nil
	}
	return &DeclarativeNode{nodeID: identity.NormalizeNodeID(nodeID), executor: executor}
}

func (n *DeclarativeNode) NodeID() string {
	if n == nil {
		return ""
	}
	return n.nodeID.String()
}

func (n *DeclarativeNode) handle(ctx context.Context, req ExecutionRequest) (ExecutionResult, error) {
	nodeID := identity.NormalizeNodeID(firstNonEmpty(n.nodeID.String(), req.NodeID.String()))
	if nodeID.IsZero() {
		return ExecutionResult{}, ErrMissingNodeID
	}
	req.NodeID = nodeID
	if isZeroHandler(req.Handler) {
		handler, ok := n.executor.deps.Source.NodeEventHandler(nodeID.String(), string(req.Event.Type))
		if !ok {
			return ExecutionResult{}, ErrMissingNodeHandler
		}
		req.Handler = handler
	}
	return n.executor.Execute(ctx, req)
}

func (n *DeclarativeNode) Handle(ctx context.Context, req ExecutionRequest) (ExecutionResult, error) {
	if n == nil || n.executor == nil {
		return ExecutionResult{}, ErrNotImplemented
	}
	return n.handle(ctx, req)
}

func isZeroHandler(handler runtimecontracts.SystemNodeEventHandler) bool {
	return reflect.DeepEqual(handler, runtimecontracts.SystemNodeEventHandler{})
}
