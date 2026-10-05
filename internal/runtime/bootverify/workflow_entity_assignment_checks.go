package bootverify

import (
	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func (checker *checkerContext) entityAssignmentReaders(node identity.ExecutableNode, event string, handler c.SystemNodeEventHandler) []expressionReference {
	// Reader sites and execution outcomes must use the same authored rule identity.
	// The scoped-node census supplies raw handlers, unlike the executable source.
	if qualified, err := c.QualifySystemNodeHandlerRuleRefsForEvent(node, event, handler); err == nil {
		handler = qualified
	}
	readers := handlerExecutableReaderExpressionsForSource(checker.source, node, event, handler)
	analysis := checker.entityAssignmentAnalysis(node.FlowPath())
	if analysis == nil {
		return readers
	}
	return pipeline.AnnotateWorkflowEntityAssignmentReaders(node, event, handler, analysis, readers)
}

func (checker *checkerContext) entityAssignmentAnalysis(flowID string) *engine.EntityAssignmentAnalysis {
	return pipeline.FlowConstructorDiagnosticAssignment(checker.constructorContracts(flowID))
}

func (checker *checkerContext) constructorContracts(flowID string) []pipeline.FlowConstructor {
	if checker.flowConstructors == nil {
		checker.flowConstructors = map[string][]pipeline.FlowConstructor{}
	}
	constructors, cached := checker.flowConstructors[flowID]
	if !cached {
		constructors, _ = pipeline.CompileFlowConstructors(checker.source, flowID)
		checker.flowConstructors[flowID] = constructors
	}
	return constructors
}
