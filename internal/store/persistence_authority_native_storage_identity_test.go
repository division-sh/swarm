package store_test

import "testing"

func verifyNativeStorageIdentityFixturesDoNotReceiveRawAuthority(t *testing.T, findings []authorityFinding) {
	for _, finding := range findings {
		if nativeStorageIdentityAuthority(finding) {
			t.Errorf("native storage identity fixture regained raw authority: %s", finding.registryLine())
		}
	}
}

func nativeStorageIdentityAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	switch finding.File {
	case "internal/runtime/pipeline/workflow_storage_identity_native_external_test.go", "internal/runtime/pipeline/workflow_missing_run_native_external_test.go", "internal/runtime/pipeline/workflow_instance_store_run_scope_test.go":
		return true
	case "internal/runtime/pipeline/workflow_instance_store_mutate_test.go", "internal/runtime/pipeline/workflow_instance_store_sqlite_test.go":
		switch finding.Enclosing {
		case "TestWorkflowInstanceStore_RunScopedCurrentStateRowsDoNotBleed", "VerifyWorkflowInstanceStore_RunScopedCurrentStateRowsDoNotBleedForTest",
			"TestWorkflowInstanceStoreAddressesRowsOnlyByExactRouteOnBothStores", "VerifyWorkflowInstanceStoreAddressesRowsOnlyByExactRouteOnBothStoresForTest",
			"TestSQLiteWorkflowInstanceStore_PreservesParentRouteControlMetadata", "VerifySQLiteWorkflowInstanceStore_PreservesParentRouteControlMetadataForTest":
			return true
		}
	}
	return false
}

func TestNativeStorageIdentityGuardRejectsRawParametersAndCallbackEvidence(t *testing.T) {
	for _, source := range []string{
		`package fixture;import "database/sql";func VerifyWorkflowInstanceStore_RunScopedCurrentStateRowsDoNotBleedForTest(db *sql.DB){_ = db}`,
		`package fixture;import("context";"database/sql");func VerifyWorkflowInstanceStoreAddressesRowsOnlyByExactRouteOnBothStoresForTest(read func(context.Context,*sql.Tx)error){_ = read}`,
		`package fixture;import "database/sql";func VerifyWorkflowInstanceStore_RequiresRunContextForTest(db *sql.DB){_ = db}`,
		`package fixture;import "database/sql";func unlistedRunScopeSibling(db *sql.DB){_ = db}`,
	} {
		rejected := false
		for _, finding := range debtAuthorityFindingsFromSource(t, "internal/runtime/pipeline/workflow_instance_store_run_scope_test.go", source) {
			rejected = rejected || nativeStorageIdentityAuthority(finding)
		}
		if !rejected {
			t.Fatal("storage identity verifier recovered raw authority")
		}
	}
}
