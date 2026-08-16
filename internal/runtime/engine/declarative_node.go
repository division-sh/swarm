package engine

import (
	"context"
	"reflect"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type DeclarativeNode struct {
	node     identity.ExecutableNode
	executor *Executor
}

func NewDeclarativeNode(node identity.ExecutableNode, executor *Executor) *DeclarativeNode {
	if executor == nil || !node.Valid() {
		return nil
	}
	return &DeclarativeNode{node: node, executor: executor}
}

func (n *DeclarativeNode) NodeID() string {
	if n == nil {
		return ""
	}
	return n.node.NodeID()
}

func (n *DeclarativeNode) handle(ctx context.Context, req ExecutionRequest) (ExecutionResult, error) {
	if !n.node.Valid() || !req.Node.Valid() || !n.node.Equal(req.Node) {
		return ExecutionResult{}, ErrMissingNodeID
	}
	if isZeroHandler(req.Handler) {
		resolved := resolvedExecutionHandler(n.executor.deps.Source, req.Node, string(req.Event.Type()))
		if !resolved.matched {
			return ExecutionResult{}, ErrMissingNodeHandler
		}
		req.Handler = resolved.handler
		if strings.TrimSpace(req.HandlerEventKey) == "" {
			req.HandlerEventKey = resolved.handlerEventKey
		}
	}
	if strings.TrimSpace(req.HandlerEventKey) == "" {
		resolved := resolvedExecutionHandler(n.executor.deps.Source, req.Node, string(req.Event.Type()))
		if resolved.matched {
			req.HandlerEventKey = resolved.handlerEventKey
		}
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

type executionHandlerResolution struct {
	handler         runtimecontracts.SystemNodeEventHandler
	handlerEventKey string
	matched         bool
}

func resolvedExecutionHandler(source semanticview.Source, node identity.ExecutableNode, eventType string) executionHandlerResolution {
	if source == nil {
		return executionHandlerResolution{}
	}
	resolved := semanticview.ResolveExecutableNodeSubscriptionHandler(source, node, eventType)
	if resolved.Matched {
		return executionHandlerResolution{
			handler:         resolved.Handler,
			handlerEventKey: resolved.HandlerEventKey,
			matched:         true,
		}
	}
	return executionHandlerResolution{}
}
