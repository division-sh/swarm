package testtiming

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func TestVerifyRunRequiredBackendEvidenceFailsClosed(t *testing.T) {
	proofs, err := testplanning.LoadParityProofs(filepath.Join("..", "apiv1", "testdata", "public_surface_backend_matrix.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(filepath.Join("..", "..", ".github", "test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	policy, err := testplanning.LoadPolicy(file)
	if err != nil {
		t.Fatal(err)
	}
	var required []testplanning.RequiredTest
	for _, proof := range proofs {
		if strings.HasPrefix(proof.ID, "verify-run-") && len(proof.Children) != 0 {
			required = append(required, testplanning.RequiredTest{
				TestRoot: testplanning.TestRoot{Package: policy.Module + "/" + filepath.ToSlash(filepath.Dir(proof.Path)), Name: proof.Name},
				Children: proof.Children,
			})
		}
	}
	required = append(required, testplanning.RequiredTest{
		TestRoot: testplanning.TestRoot{Package: policy.Module + "/internal/cliapp", Name: "TestVerifyRunPublicAdmissionBothStores"},
		Children: policy.Units["catalog-required-verify"].RequiredChildren["TestVerifyRunPublicAdmissionBothStores"],
	})
	// Enumerate the expected family so deleting catalog rows cannot make this
	// counterexample vacuous. The public matrix is separately declared in policy.
	want := map[string]bool{}
	for _, name := range []string{"TestVerifyRunMutationDriftBothStores", "TestVerifyRunHistoryAdmissionBothStores", "TestVerifyRunPhysicalJSONAdmissionBothStores", "TestVerifyRunSharedForkOrderBothStores", "TestVerifyRunOverlappingForkOrderBothStores", "TestVerifyRunCrossRunIsolationBothStores", "TestVerifyRunEntityMembershipBothStores", "TestVerifyRunMissingAndEmptyExistingRunBothStores", "TestVerifyRunSnapshotIsolationBothStores", "TestVerifyRunCanonicalMembershipBothStores", "TestVerifyRunCanonicalHeaderDomainsBothStores", "TestVerifyRunConstructedPairAdmissionBothStores", "TestVerifyRunSharedHeaderReadScopeBothStores", "TestVerifyRunStateOnlyDoesNotAcquireConstructionBothStores", "TestSelectedForkHistoricalFieldlessHeaderBothStores", "TestVerifyRunNativeConstructionBothStores", "TestMutationDomainsReachBothForkConsumersBothStores", "TestForkActivationInventoriesConstructedHeadersBothStores", "TestOrdinaryReplayInitializedReceiversBothStores", "TestActivityAdmissionConsumesConstructedHeaderBothStores", "TestConstructedHeaderGateSummaryBothStores", "TestConstructedHeaderGateFreezeBothStores", "TestFanOutBarrierConsumesConstructedHeaderBothStores", "TestSelectedContractActivitySourceProjectionBothStores", "TestSelectedContractActivitySourceProjectionPreservesNumericPayloadBothStores", "TestRunTerminalizationAtomicallyFencesGateActivationsAndCardsOnBothStores", "TestVerifyRunJobflowRegistryIntegrationBothStores", "TestVerifyRunPublicAdmissionBothStores"} {
		want[name] = true
	}
	for _, proof := range required {
		if !want[proof.Name] {
			t.Fatalf("unexpected required proof %s", proof.Name)
		}
		delete(want, proof.Name)
		t.Run(proof.Name, func(t *testing.T) {
			if len(proof.Children) == 0 {
				t.Fatal("backend obligations are empty")
			}
			unit := testplanning.ProofUnit{RequiredTests: []testplanning.RequiredTest{proof}, TestBearingPackages: []string{proof.Package}}
			report := Report{Tests: []TestTiming{{Package: proof.Package, Test: proof.Name, Result: "pass"}}}
			postgres := -1
			for _, child := range proof.Children {
				report.Tests = append(report.Tests, TestTiming{Package: proof.Package, Test: proof.Name + "/" + child, Result: "pass"})
				if strings.HasPrefix(child, "postgres/") || child == "postgres" {
					postgres = len(report.Tests) - 1
				}
			}
			if postgres < 0 {
				t.Fatal("PostgreSQL requirement missing")
			}
			if problems := requiredExecutionProblems(unit, report); len(problems) != 0 {
				t.Fatalf("complete synthetic validator control rejected: %v", problems)
			}
			for _, result := range []string{"missing", "skip", "fail"} {
				t.Run(result, func(t *testing.T) {
					changed := report
					changed.Tests = append([]TestTiming(nil), report.Tests...)
					if result == "missing" {
						changed.Tests = append(changed.Tests[:postgres], changed.Tests[postgres+1:]...)
					} else {
						changed.Tests[postgres].Result = result
					}
					if problems := requiredExecutionProblems(unit, changed); !strings.Contains(strings.Join(problems, "; "), "postgres") {
						t.Fatalf("%s PostgreSQL backend qualified behind passing parent: %v", result, problems)
					}
				})
			}
		})
	}
	if len(want) != 0 {
		t.Fatalf("missing required proof catalog declarations: %v", want)
	}
}
