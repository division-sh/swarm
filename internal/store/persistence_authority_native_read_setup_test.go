package store_test

import "testing"

func verifyNativeAPIReadSetupDoesNotReceiveRawAuthority(t *testing.T, findings []authorityFinding) {
	for _, finding := range findings {
		if nativeAPIReadSetupAuthority(finding) {
			t.Errorf("native API read/control fixture regained raw authority: %s", finding.registryLine())
		}
	}
}

func nativeAPIReadSetupAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	switch finding.File {
	case "internal/apiv1/selected_store_read_supported_surface_test.go":
		return finding.Enclosing == "TestSelectedStoreRunReadHandlersExecuteAcrossBackends"
	case "internal/apiv1/operator_fan_out_test.go":
		return finding.Enclosing == "TestFanOutReadAPISelectedStores"
	case "internal/apiv1/operator_entity_test.go":
		return finding.Enclosing == "TestOperatorEntityHandlersServeContractEntityTypesFromPostgres"
	case "internal/apiv1/operator_run_control_test.go":
		return finding.Enclosing == "TestOperatorRunControlHandlersTypedResourceErrors" || finding.Enclosing == "TestOperatorRunStopDoesNotReplayCommittedTransitionAfterReconciliationFailure"
	case "internal/apiv1/operator_run_start_test.go":
		return finding.Enclosing == "TestOperatorRunStartHandlersLeaveSplitControlMethodsUnavailable"
	}
	return false
}
