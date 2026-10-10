package store_test

import "testing"

func verifyNativeProjectionHeaderFixturesDoNotReceiveRawAuthority(t *testing.T, findings []authorityFinding) {
	for _, finding := range findings {
		if nativeProjectionHeaderAuthority(finding) {
			t.Errorf("completed projection fixture file regained raw authority: %s", finding.registryLine())
		}
	}
}
func nativeProjectionHeaderAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	switch finding.File {
	case "internal/runtime/pipeline/workflow_instance_store_projection_test.go", "internal/runtime/pipeline/workflow_projection_header_native_fixture_test.go", "internal/runtime/pipeline/workflow_projection_header_native_external_test.go":
		return true
	}
	return false
}
func TestNativeProjectionHeaderGuardRejectsRawParametersAndCallbacks(t *testing.T) {
	for _, source := range []string{
		`package fixture;import "database/sql";func VerifyWorkflowInstanceFixtureListUsesConstructedHeadersForTest(db *sql.DB){_ = db}`,
		`package fixture;import("context";"database/sql");func sharedProjectionProbe(fault func(context.Context,*sql.Tx)error){_ = fault}`,
	} {
		rejected := false
		for _, finding := range debtAuthorityFindingsFromSource(t, "internal/runtime/pipeline/workflow_instance_store_projection_test.go", source) {
			rejected = rejected || nativeProjectionHeaderAuthority(finding)
		}
		if !rejected {
			t.Fatal("completed projection file recovered raw authority")
		}
	}
}
