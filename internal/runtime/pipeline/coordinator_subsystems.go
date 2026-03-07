package pipeline

type ScanCoordinator struct {
	scans        map[string]*scanAccumulator
	pendingDedup map[string]pendingCandidate
}

func NewScanCoordinator() *ScanCoordinator {
	return &ScanCoordinator{
		scans:        make(map[string]*scanAccumulator),
		pendingDedup: make(map[string]pendingCandidate),
	}
}

type ScoringState struct {
	accumulators map[string]*scoringAccumulator
}

func NewScoringState() *ScoringState {
	return &ScoringState{accumulators: make(map[string]*scoringAccumulator)}
}

type ValidationGate struct {
	states map[string]*validationPipelineState
}

func NewValidationGate() *ValidationGate {
	return &ValidationGate{states: make(map[string]*validationPipelineState)}
}
