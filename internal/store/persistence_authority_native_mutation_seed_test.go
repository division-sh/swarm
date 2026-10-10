package store_test

import "testing"

func verifyNativeMutationSeedFixturesDoNotReceiveRawAuthority(t *testing.T, findings []authorityFinding) {
	for _, finding := range findings {
		if nativeMutationSeedAuthority(finding) {
			t.Errorf("native mutation seed regained raw authority: %s", finding.registryLine())
		}
	}
}
func nativeMutationSeedAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	switch finding.File {
	case "internal/runtime/pipeline/workflow_mutation_native_fixture_test.go", "internal/runtime/pipeline/workflow_mutation_native_external_test.go", "internal/runtime/pipeline/workflow_scheduler_timer_native_fixture_test.go", "internal/runtime/pipeline/workflow_scheduler_timer_native_external_test.go":
		return true
	case "internal/runtime/pipeline/workflow_instance_store_mutate_test.go":
		return true
	}
	return false
}
func TestNativeMutationSeedGuardRejectsRawParametersAndCallbacks(t *testing.T) {
	for _, source := range []string{
		`package fixture;import "database/sql";func seedWorkflowInstanceForMutationTest(db *sql.DB){_ = db}`,
		`package fixture;import("context";"database/sql");func VerifyUpdateEntityState_RejectsCompetingStaleCallbackSnapshotForTest(writer func(context.Context,*sql.Tx)error){_ = writer}`,
		`package fixture;import "database/sql";func VerifyWorkflowInstanceStoreMutate_IgnoresSchedulerOwnedTimerRowsForTest(db *sql.DB){_ = db}`,
		`package fixture;import "database/sql";func newUnlistedMutationSibling(db *sql.DB){_ = db}`,
	} {
		rejected := false
		for _, finding := range debtAuthorityFindingsFromSource(t, "internal/runtime/pipeline/workflow_instance_store_mutate_test.go", source) {
			rejected = rejected || nativeMutationSeedAuthority(finding)
		}
		if !rejected {
			t.Fatal("mutation seed/caller recovered raw authority")
		}
	}
}
