package pipeline

func expectedAgents(mode string) int {
	mode = normalizeScanMode(mode)
	if mode == "" {
		mode = bundleDefaultScanMode(scanOrchestratorContractSource())
	}
	if source := scanOrchestratorContractSource(); source != nil {
		expectedScanners := scanDispatchKeysForMode(source, mode)
		if len(expectedScanners) > 0 {
			return scanDispatchExpectedAgents(mode, expectedScanners)
		}
	}
	return 1
}
