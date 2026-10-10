package store_test

import "testing"

func verifyNativeProjectionStorageFixturesDoNotReceiveRawAuthority(t *testing.T, findings []authorityFinding) {
	for _, finding := range findings {
		if nativeProjectionStorageAuthority(finding) {
			t.Errorf("native projection storage fixture regained raw authority: %s", finding.registryLine())
		}
	}
}

func nativeProjectionStorageAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	switch finding.File {
	case "internal/runtime/pipeline/workflow_projection_storage_native_fixture_test.go", "internal/runtime/pipeline/workflow_projection_storage_native_external_test.go":
		return true
	case "internal/runtime/pipeline/workflow_instance_store_projection_test.go":
		switch finding.Enclosing {
		case "TestWorkflowInstanceStoreProjection_DoesNotExposeControlStatusAsEntityField", "VerifyWorkflowInstanceStoreProjection_DoesNotExposeControlStatusAsEntityFieldForTest",
			"TestWorkflowInstanceStoreCreateRejectsDuplicateWithoutMutatingProjection", "VerifyWorkflowInstanceStoreCreateRejectsDuplicateWithoutMutatingProjectionForTest":
			return true
		}
	}
	return false
}

func TestNativeProjectionStorageGuardRejectsRawParametersAndCallbacks(t *testing.T) {
	for _, source := range []string{
		`package fixture;import "database/sql";func VerifyWorkflowInstanceStoreProjection_DoesNotExposeControlStatusAsEntityFieldForTest(db *sql.DB){_ = db}`,
		`package fixture;import("context";"database/sql");func VerifyWorkflowInstanceStoreCreateRejectsDuplicateWithoutMutatingProjectionForTest(read func(context.Context,*sql.Tx)error){_ = read}`,
	} {
		rejected := false
		for _, finding := range debtAuthorityFindingsFromSource(t, "internal/runtime/pipeline/workflow_instance_store_projection_test.go", source) {
			rejected = rejected || nativeProjectionStorageAuthority(finding)
		}
		if !rejected {
			t.Fatal("projection storage verifier recovered raw authority")
		}
	}
}
