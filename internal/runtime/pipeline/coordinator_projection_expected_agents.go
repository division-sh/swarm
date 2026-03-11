package pipeline

func expectedAgents(mode string) int {
	mode = normalizeScanMode(mode)
	if mode == "" {
		mode = bundleDefaultScanMode(scanOrchestratorContractBundle())
	}
	if bundle := scanOrchestratorContractBundle(); bundle != nil {
		expectedScanners := scanDispatchKeysForMode(bundle, mode)
		if len(expectedScanners) > 0 {
			return scanDispatchExpectedAgents(mode, expectedScanners)
		}
	}
	return 1
}
