package store_test

import "testing"

func verifyNativeProjectionShapeFixturesDoNotReceiveRawAuthority(t *testing.T, findings []authorityFinding) {
	for _, finding := range findings {
		if nativeProjectionShapeAuthority(finding) {
			t.Errorf("native projection shape fixture regained raw authority: %s", finding.registryLine())
		}
	}
}
func nativeProjectionShapeAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	switch finding.File {
	case "internal/runtime/pipeline/workflow_projection_shape_native_fixture_test.go", "internal/runtime/pipeline/workflow_projection_shape_native_external_test.go":
		return true
	case "internal/runtime/pipeline/workflow_instance_store_projection_test.go":
		return finding.Enclosing == "TestWorkflowInstanceStoreProjection_RejectsMalformedPersistedShapes" || finding.Enclosing == "VerifyWorkflowInstanceStoreProjection_RejectsMalformedPersistedShapesForTest"
	}
	return false
}
func TestNativeProjectionShapeGuardRejectsRawParametersAndCallbacks(t *testing.T) {
	for _, source := range []string{
		`package fixture;import "database/sql";func VerifyWorkflowInstanceStoreProjection_RejectsMalformedPersistedShapesForTest(db *sql.DB){_ = db}`,
		`package fixture;import("context";"database/sql");func VerifyWorkflowInstanceStoreProjection_RejectsMalformedPersistedShapesForTest(fault func(context.Context,*sql.Tx)error){_ = fault}`,
	} {
		rejected := false
		for _, finding := range debtAuthorityFindingsFromSource(t, "internal/runtime/pipeline/workflow_instance_store_projection_test.go", source) {
			rejected = rejected || nativeProjectionShapeAuthority(finding)
		}
		if !rejected {
			t.Fatal("projection shape verifier recovered raw authority")
		}
	}
}
