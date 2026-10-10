package store_test

import "testing"

func verifyNativeBookkeepingFixturesDoNotReceiveRawAuthority(t *testing.T, findings []authorityFinding) {
	for _, finding := range findings {
		if nativeBookkeepingAuthority(finding) {
			t.Errorf("native bookkeeping fixture regained raw authority: %s", finding.registryLine())
		}
	}
}
func nativeBookkeepingAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	switch finding.File {
	case "internal/runtime/pipeline/workflow_bookkeeping_native_fixture_test.go", "internal/runtime/pipeline/workflow_bookkeeping_native_external_test.go":
		return true
	case "internal/runtime/pipeline/workflow_instance_store_mutate_test.go":
		return finding.Enclosing == "TestWorkflowEngineCompleteCarrierPreservesBookkeepingOnBothStores" || finding.Enclosing == "VerifyWorkflowEngineCompleteCarrierPreservesBookkeepingForTest"
	}
	return false
}
func TestNativeBookkeepingGuardRejectsRawParametersAndCallbacks(t *testing.T) {
	for _, source := range []string{
		`package fixture;import "database/sql";func VerifyWorkflowEngineCompleteCarrierPreservesBookkeepingForTest(db *sql.DB){_ = db}`,
		`package fixture;import("context";"database/sql");func VerifyWorkflowEngineCompleteCarrierPreservesBookkeepingForTest(write func(context.Context,*sql.Tx)error){_ = write}`,
	} {
		rejected := false
		for _, finding := range debtAuthorityFindingsFromSource(t, "internal/runtime/pipeline/workflow_instance_store_mutate_test.go", source) {
			rejected = rejected || nativeBookkeepingAuthority(finding)
		}
		if !rejected {
			t.Fatal("bookkeeping fixture regained raw authority")
		}
	}
}
