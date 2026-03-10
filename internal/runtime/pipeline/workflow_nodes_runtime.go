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
	nodeIDs := make(map[string]struct{})
	for _, node := range pc.WorkflowNodes() {
		nodeIDs[strings.TrimSpace(node.ID)] = struct{}{}
	}
	_, hasScan := nodeIDs["scan-orchestrator"]
	_, hasDiscovery := nodeIDs["discovery-aggregator"]
	_, hasValidation := nodeIDs["validation-orchestrator"]
	_, hasLifecycle := nodeIDs["lifecycle-orchestrator"]
	_, hasScoring := nodeIDs[ScoringNodeID]

	out := make([]workflowNodeExecutor, 0, 5)
	if hasScan && pc.scanCoordinator != nil {
		out = append(out, &ScanOrchestrator{coordinator: pc.scanCoordinator})
	}
	if hasDiscovery && pc.scanCoordinator != nil {
		out = append(out, &DiscoveryAggregator{coordinator: pc})
	}
	if hasValidation && pc.validationGate != nil {
		out = append(out, &ValidationOrchestrator{coordinator: pc})
	}
	if hasLifecycle && pc.validationGate != nil {
		out = append(out, &LifecycleOrchestrator{coordinator: pc})
	}
	if hasScoring {
		if scoringExecutor := pc.scoringTransitionExecutor(); scoringExecutor != nil {
			out = append(out, scoringExecutor)
		}
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
