package pipeline

import "sync"

type ScanCoordinator struct {
	scans        map[string]*scanAccumulator
	pendingDedup map[string]pendingCandidate
	runtime      scanWorkflowRuntime
	payloadFactory *PipelinePayloadFactory
	mu           *sync.Mutex
}

func NewScanCoordinator() *ScanCoordinator {
	return &ScanCoordinator{
		scans:        make(map[string]*scanAccumulator),
		pendingDedup: make(map[string]pendingCandidate),
	}
}

type ScoringState struct {
	accumulators map[string]*scoringAccumulator
	runtime      scoringWorkflowRuntime
	payloadFactory *PipelinePayloadFactory
	mu           *sync.Mutex
}

func NewScoringState() *ScoringState {
	return &ScoringState{accumulators: make(map[string]*scoringAccumulator)}
}

type ValidationGate struct {
	states map[string]*validationPipelineState
	runtime validationWorkflowRuntime
	payloadFactory *PipelinePayloadFactory
	mu     *sync.Mutex
}

func NewValidationGate() *ValidationGate {
	return &ValidationGate{states: make(map[string]*validationPipelineState)}
}
