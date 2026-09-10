package pipeline

import "github.com/division-sh/swarm/internal/runtime/semanticview"

type WorkflowModule interface {
	SemanticSource() semanticview.Source
	WorkflowNodes() []WorkflowNode
	GuardRegistry() GuardRegistry
	ActionRegistry() ActionRegistry
}
