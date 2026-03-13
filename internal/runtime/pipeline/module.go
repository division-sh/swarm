package pipeline

import "empireai/internal/runtime/semanticview"

type WorkflowModule interface {
	SemanticSource() semanticview.Source
	WorkflowDefinition() *WorkflowDefinition
	WorkflowNodes() []WorkflowNode
	GuardRegistry() GuardRegistry
	ActionRegistry() ActionRegistry
}

var defaultWorkflowModuleFactory func() WorkflowModule

func SetDefaultWorkflowModuleFactory(factory func() WorkflowModule) {
	defaultWorkflowModuleFactory = factory
}

func defaultWorkflowModule() WorkflowModule {
	module := defaultWorkflowModuleOrNil()
	if module == nil {
		panic("pipeline: workflow module is required; configure SetDefaultWorkflowModuleFactory")
	}
	return module
}

func defaultWorkflowModuleOrNil() WorkflowModule {
	if defaultWorkflowModuleFactory == nil {
		return nil
	}
	return defaultWorkflowModuleFactory()
}

func DefaultWorkflowModuleOrNil() WorkflowModule {
	return defaultWorkflowModuleOrNil()
}

func DefaultWorkflowSemanticSourceOrNil() semanticview.Source {
	module := defaultWorkflowModuleOrNil()
	if module == nil {
		return nil
	}
	return module.SemanticSource()
}
