package engine

import (
	"context"
	"reflect"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
		resolved := resolvedExecutionHandler(n.executor.deps.Source, nodeID.String(), string(req.Event.Type()))
		if !resolved.matched {
			return ExecutionResult{}, ErrMissingNodeHandler
		}
		req.Handler = resolved.handler
		if strings.TrimSpace(req.HandlerEventKey) == "" {
			req.HandlerEventKey = resolved.handlerEventKey
		}
	}
	if strings.TrimSpace(req.HandlerEventKey) == "" {
		resolved := resolvedExecutionHandler(n.executor.deps.Source, nodeID.String(), string(req.Event.Type()))
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

func resolvedExecutionHandler(source semanticview.Source, nodeID, eventType string) executionHandlerResolution {
	if source == nil {
		return executionHandlerResolution{}
	}
	if bundle, ok := semanticview.Bundle(source); ok {
		resolved := bundle.ResolveNodeEventHandler(nodeID, eventType)
		if resolved.Matched {
			handler, ok := source.NodeEventHandler(nodeID, eventType)
			if !ok {
				handler = resolved.Handler
			}
			return executionHandlerResolution{
				handler:         handler,
				handlerEventKey: strings.TrimSpace(resolved.AuthoredEventType),
				matched:         true,
			}
		}
	}
	handler, ok := source.NodeEventHandler(nodeID, eventType)
	if !ok {
		return executionHandlerResolution{}
	}
	return executionHandlerResolution{
		handler:         handler,
		handlerEventKey: matchedHandlerEventKeyFromHandlers(source.NodeEventHandlers(nodeID), eventType),
		matched:         true,
	}
}

func matchedHandlerEventKeyFromHandlers(handlers map[string]runtimecontracts.SystemNodeEventHandler, eventType string) string {
	eventType = strings.TrimSpace(eventType)
	for key := range handlers {
		if strings.TrimSpace(key) == eventType {
			return strings.TrimSpace(key)
		}
	}
	for key := range handlers {
		key = strings.TrimSpace(key)
		if key != "" && eventidentity.MatchPattern(key, eventType) {
			return key
		}
	}
	return eventType
}
