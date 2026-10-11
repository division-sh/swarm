package store_test

import "testing"

func verifyNativeLookupMissFixturesDoNotReceiveRawAuthority(t *testing.T, findings []authorityFinding) {
	for _, finding := range findings {
		if nativeLookupMissAuthority(finding) {
			t.Errorf("native lookup-miss fixture regained raw authority: %s", finding.registryLine())
		}
	}
}
func nativeLookupMissAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	switch finding.File {
	case "internal/runtime/pipeline/workflow_lookup_native_fixture_test.go", "internal/runtime/pipeline/workflow_lookup_native_external_test.go":
		return true
	case "internal/runtime/pipeline/workflow_instance_store_mutate_test.go":
		return finding.Enclosing == "TestWorkflowInstanceLookupMissIsTypedAndExactOnBothStores" || finding.Enclosing == "VerifyWorkflowInstanceLookupMissIsTypedAndExactForTest" || finding.Enclosing == "workflowInstanceRowCount"
	}
	return false
}
func TestNativeLookupMissGuardRejectsRawParametersAndCallbacks(t *testing.T) {
	for _, source := range []string{
		`package fixture;import "database/sql";func workflowInstanceRowCount(db *sql.DB){_ = db}`,
		`package fixture;import("context";"database/sql");func VerifyWorkflowInstanceLookupMissIsTypedAndExactForTest(count func(context.Context,*sql.Tx)error){_ = count}`,
	} {
		rejected := false
		for _, finding := range debtAuthorityFindingsFromSource(t, "internal/runtime/pipeline/workflow_instance_store_mutate_test.go", source) {
			rejected = rejected || nativeLookupMissAuthority(finding)
		}
		if !rejected {
			t.Fatal("lookup miss regained raw authority")
		}
	}
}
