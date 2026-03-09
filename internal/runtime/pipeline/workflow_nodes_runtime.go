package pipeline

import (
	"context"
	"strings"

	"empireai/internal/events"
)

type workflowNodeExecutor = WorkflowNodeExecutor

func (pc *FactoryPipelineCoordinator) workflowNodeExecutors() []workflowNodeExecutor {
	if pc == nil {
		return nil
	}
	nodeIDs := make(map[string]struct{})
	for _, node := range pc.WorkflowNodes() {
		nodeIDs[strings.TrimSpace(node.ID)] = struct{}{}
	}
	_, legacyCoordinator := nodeIDs["pipeline-coordinator"]
	_, hasScan := nodeIDs["scan-orchestrator"]
	_, hasDiscovery := nodeIDs["discovery-aggregator"]
	_, hasValidation := nodeIDs["validation-orchestrator"]
	_, hasLifecycle := nodeIDs["lifecycle-orchestrator"]
	_, hasScoring := nodeIDs[ScoringNodeID]
	// Scoring is still a dedicated runtime node. The split-executor model owns
	// the other four system nodes directly; scoring remains the one explicit
	// architectural exception until Phase 3 unifies it.
	useSplitExecutors := !legacyCoordinator && (hasScan || hasDiscovery || hasValidation || hasLifecycle)

	out := make([]workflowNodeExecutor, 0, 5)
	if useSplitExecutors && hasScan && pc.scanCoordinator != nil {
		out = append(out, &ScanOrchestrator{coordinator: pc.scanCoordinator})
	}
	if useSplitExecutors && hasDiscovery && pc.scanCoordinator != nil {
		out = append(out, &DiscoveryAggregator{coordinator: pc})
	}
	if useSplitExecutors && hasValidation && pc.validationGate != nil {
		out = append(out, &ValidationOrchestrator{coordinator: pc})
	}
	if useSplitExecutors && hasLifecycle && pc.validationGate != nil {
		out = append(out, &LifecycleOrchestrator{coordinator: pc})
	}
	if hasScoring && pc.scoringState != nil {
		out = append(out, pc.scoringState)
	}
	if useSplitExecutors && len(out) > 0 {
		return out
	}
	if pc.scanCoordinator != nil {
		out = append(out, pc.scanCoordinator)
	}
	if pc.scoringState != nil {
		out = append(out, pc.scoringState)
	}
	if pc.validationGate != nil {
		out = append(out, pc.validationGate)
	}
	return out
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
