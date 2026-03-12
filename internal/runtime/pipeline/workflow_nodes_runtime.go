package pipeline

import (
	"context"
	"database/sql"
	"strings"

	"empireai/internal/events"
)

type workflowNodeExecutor = WorkflowNodeExecutor

func (pc *FactoryPipelineCoordinator) BackgroundNodes(bus systemNodeBus, db *sql.DB) []BackgroundNode {
	if pc == nil || bus == nil {
		return nil
	}
	out := make([]BackgroundNode, 0, 1)
	for _, node := range pc.WorkflowNodes() {
		if strings.TrimSpace(node.ExecutionType) != "workflow_node" {
			continue
		}
		if executor := pc.backgroundWorkflowExecutor(strings.TrimSpace(node.ID)); executor != nil {
			if bg := newBackgroundWorkflowNode(executor, bus, db); bg != nil {
				out = append(out, bg)
			}
		}
	}
	return out
}

func (pc *FactoryPipelineCoordinator) backgroundWorkflowExecutor(nodeID string) WorkflowNodeExecutor {
	if pc == nil {
		return nil
	}
	nodeID = strings.TrimSpace(nodeID)
	for _, executor := range pc.workflowNodeExecutors() {
		if strings.TrimSpace(executor.NodeID()) != nodeID {
			continue
		}
		provider, ok := executor.(BackgroundWorkflowExecutorProvider)
		if !ok {
			return nil
		}
		return provider.BackgroundWorkflowExecutor()
	}
	return nil
}

func (pc *FactoryPipelineCoordinator) workflowNodeExecutors() []workflowNodeExecutor {
	if pc == nil {
		return nil
	}
	source := pc.SemanticSource()
	if source == nil {
		return nil
	}
	nodes := pc.WorkflowNodes()
	out := make([]workflowNodeExecutor, 0, len(nodes))
	for _, node := range nodes {
		nodeID := strings.TrimSpace(node.ID)
		if nodeID == "" {
			continue
		}
		contract, ok := source.NodeEntries()[nodeID]
		if !ok {
			continue
		}
		executor := NewNode(contract, newCoordinatorHandlerExecutionEngine(pc, nodeID), nil)
		if executor == nil {
			continue
		}
		out = append(out, executor)
	}
	return out
}

func (pc *FactoryPipelineCoordinator) scoringTransitionExecutor() workflowNodeExecutor {
	if pc == nil || pc.scoringState == nil {
		return nil
	}
	return newScoringTransitionExecutor(pc)
}

func (pc *FactoryPipelineCoordinator) workflowNodeInterceptPolicy(eventType string, evt events.Event) (bool, bool) {
	for _, executor := range pc.workflowNodeExecutors() {
		if consume, handled := executor.InterceptPolicy(eventType, evt); handled {
			return consume, true
		}
	}
	return false, false
}

func (pc *FactoryPipelineCoordinator) dispatchWorkflowNodeEvent(ctx context.Context, evt events.Event) bool {
	eventType := strings.TrimSpace(string(evt.Type))
	if eventType == "" {
		return false
	}
	for _, executor := range pc.workflowNodeExecutors() {
		if handled := executor.Handle(ctx, evt); handled {
			return true
		}
	}
	return false
}
