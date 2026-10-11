package store_test

import "testing"

func verifyNativeProjectionRoundTripFixturesDoNotReceiveRawAuthority(t *testing.T, findings []authorityFinding) {
	for _, finding := range findings {
		if nativeProjectionRoundTripAuthority(finding) {
			t.Errorf("native projection fixture regained raw authority: %s", finding.registryLine())
		}
	}
}

func nativeProjectionRoundTripAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	switch finding.File {
	case "internal/runtime/pipeline/workflow_projection_native_fixture_test.go", "internal/runtime/pipeline/workflow_projection_native_external_test.go", "internal/runtime/pipeline/workflow_initial_preparation_native_external_test.go":
		return true
	case "internal/runtime/pipeline/workflow_instance_activation_test.go":
		return finding.Enclosing == "TestWorkflowInitialLifecyclePreparationRejectsUnownedEmissions" || finding.Enclosing == "VerifyWorkflowInitialLifecyclePreparationRejectsUnownedEmissionsForTest"
	case "internal/runtime/pipeline/workflow_instance_store_projection_test.go":
		switch finding.Enclosing {
		case "TestWorkflowInstanceStoreProjection_RoundTripPreservesCanonicalState", "VerifyWorkflowInstanceStoreProjection_RoundTripPreservesCanonicalStateForTest",
			"TestWorkflowInstanceStoreProjection_StaticRowsPersistCanonicalFlowPathOnRoundTrip", "VerifyWorkflowInstanceStoreProjection_StaticRowsPersistCanonicalFlowPathOnRoundTripForTest":
			return true
		}
	}
	return false
}

func TestNativeProjectionRoundTripGuardRejectsRawParametersAndCallbackEvidence(t *testing.T) {
	for _, source := range []string{
		`package fixture;import "database/sql";func VerifyWorkflowInstanceStoreProjection_RoundTripPreservesCanonicalStateForTest(db *sql.DB){_ = db}`,
		`package fixture;import("context";"database/sql");func VerifyWorkflowInstanceStoreProjection_StaticRowsPersistCanonicalFlowPathOnRoundTripForTest(read func(context.Context,*sql.Tx)error){_ = read}`,
	} {
		rejected := false
		for _, finding := range debtAuthorityFindingsFromSource(t, "internal/runtime/pipeline/workflow_instance_store_projection_test.go", source) {
			rejected = rejected || nativeProjectionRoundTripAuthority(finding)
		}
		if !rejected {
			t.Fatal("projection verifier recovered raw authority")
		}
	}
}

func TestNativeInitialPreparationGuardRejectsRawParametersAndCallbacks(t *testing.T) {
	for _, source := range []string{
		`package fixture;import "database/sql";func VerifyWorkflowInitialLifecyclePreparationRejectsUnownedEmissionsForTest(db *sql.DB){_ = db}`,
		`package fixture;import("context";"database/sql");func VerifyWorkflowInitialLifecyclePreparationRejectsUnownedEmissionsForTest(write func(context.Context,*sql.Tx)error){_ = write}`,
	} {
		rejected := false
		for _, finding := range debtAuthorityFindingsFromSource(t, "internal/runtime/pipeline/workflow_instance_activation_test.go", source) {
			rejected = rejected || nativeProjectionRoundTripAuthority(finding)
		}
		if !rejected {
			t.Fatal("initial lifecycle preparation regained raw authority")
		}
	}
}
