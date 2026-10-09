package testplanning

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

type verifyRunProof struct {
	pkg      string
	name     string
	children []string
}

// These are execution obligations, not runtime evidence or invented backend cells.
var verifyRunProofs = []verifyRunProof{
	{pkg: "internal/cliapp", name: "TestVerifyRunPublicAdmissionBothStores"},
	{pkg: "internal/cliapp", name: "TestVerifyRunPublicFlagAndAbsentStoreAdmission"},
	{pkg: "internal/cliapp", name: "TestVerifyRunTypedFailuresRetainCoordinatesAndCancellation"},
	{pkg: "internal/cliapp", name: "TestVerifyRunPublicOutputParity"},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestVerifyRunMutationDriftBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestVerifyRunHistoryAdmissionBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestVerifyRunPhysicalJSONAdmissionBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestVerifyRunSharedForkOrderBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestVerifyRunOverlappingForkOrderBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestVerifyRunCrossRunIsolationBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestVerifyRunEntityMembershipBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestVerifyRunMissingAndEmptyExistingRunBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestVerifyRunSnapshotIsolationBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestVerifyRunSnapshotFailuresRetainCause"},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestVerifyRunCanonicalMembershipBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestVerifyRunCanonicalHeaderDomainsBothStores", children: []string{"sqlite/lifecycle_state", "sqlite/bookkeeping", "sqlite/gate", "sqlite/accumulator", "sqlite/authored_field", "postgres/lifecycle_state", "postgres/bookkeeping", "postgres/gate", "postgres/accumulator", "postgres/authored_field"}},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestVerifyRunConstructedPairAdmissionBothStores", children: []string{"sqlite/missing_fields", "sqlite/fieldless_fields", "sqlite/foreign_run", "sqlite/wrong_entity", "sqlite/wrong_path", "sqlite/wrong_type", "sqlite/conflicting_fields", "sqlite/invalid_header_json", "sqlite/invalid_fields_json", "sqlite/invalid_revision", "sqlite/empty_declared_type", "postgres/missing_fields", "postgres/fieldless_fields", "postgres/foreign_run", "postgres/wrong_entity", "postgres/wrong_path", "postgres/wrong_type", "postgres/conflicting_fields", "postgres/invalid_header_json", "postgres/invalid_fields_json", "postgres/invalid_revision", "postgres/empty_declared_type"}},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestVerifyRunSharedHeaderReadScopeBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestVerifyRunStateOnlyDoesNotAcquireConstructionBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestVerifyRunSharedHeaderReadPreservesMutationLocks"},
	{pkg: "internal/store/internal/backend/runforkpersistence", name: "TestSelectedForkHistoricalFieldlessHeaderBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/store/internal/runtimepersistence", name: "TestVerifyRunNativeConstructionBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/store/internal/runtimepersistence", name: "TestMutationDomainsReachBothForkConsumersBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/store/internal/runtimepersistence", name: "TestForkActivationInventoriesConstructedHeadersBothStores", children: []string{"sqlite/fieldless", "sqlite/fields", "postgres/fieldless", "postgres/fields"}},
	{pkg: "internal/store/internal/runtimepersistence", name: "TestOrdinaryReplayInitializedReceiversBothStores", children: []string{"sqlite/wrong_path", "sqlite/wrong_run", "sqlite/foreign_template", "sqlite/wrong_stage", "sqlite/malformed_bookkeeping", "sqlite/state_only_receiver", "sqlite/missing_declared_fields", "postgres/wrong_path", "postgres/wrong_run", "postgres/foreign_template", "postgres/wrong_stage", "postgres/malformed_bookkeeping", "postgres/state_only_receiver", "postgres/missing_declared_fields"}},
	{pkg: "internal/store/internal/runtimepersistence", name: "TestActivityAdmissionConsumesConstructedHeaderBothStores", children: []string{"postgres/fieldless/current", "postgres/fieldless/malformed_bucket", "postgres/fieldless/missing_header", "postgres/fieldless/stale", "postgres/fieldless/wrong_entity", "postgres/fieldless/wrong_header_flow", "postgres/fieldless/wrong_path", "postgres/fields/current", "postgres/fields/malformed_bucket", "postgres/fields/missing_header", "postgres/fields/stale", "postgres/fields/wrong_entity", "postgres/fields/wrong_header_flow", "postgres/fields/wrong_path", "sqlite/fieldless/current", "sqlite/fieldless/malformed_bucket", "sqlite/fieldless/missing_header", "sqlite/fieldless/stale", "sqlite/fieldless/wrong_entity", "sqlite/fieldless/wrong_header_flow", "sqlite/fieldless/wrong_path", "sqlite/fields/current", "sqlite/fields/malformed_bucket", "sqlite/fields/missing_header", "sqlite/fields/stale", "sqlite/fields/wrong_entity", "sqlite/fields/wrong_header_flow", "sqlite/fields/wrong_path"}},
	{pkg: "internal/store/internal/runtimepersistence", name: "TestConstructedHeaderGateSummaryBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/store/internal/runtimepersistence", name: "TestConstructedHeaderGateFreezeBothStores", children: []string{"sqlite/fields=false", "sqlite/fields=true", "postgres/fields=false", "postgres/fields=true"}},
	{pkg: "internal/store/internal/runtimepersistence", name: "TestFanOutBarrierConsumesConstructedHeaderBothStores", children: []string{"postgres/fieldless/current", "postgres/fieldless/malformed_bucket", "postgres/fieldless/missing_header", "postgres/fieldless/stale", "postgres/fieldless/wrong_header_flow", "postgres/fieldless/wrong_path", "postgres/fieldless/wrong_scope", "postgres/fields/current", "postgres/fields/malformed_bucket", "postgres/fields/missing_fields", "postgres/fields/missing_header", "postgres/fields/stale", "postgres/fields/wrong_header_flow", "postgres/fields/wrong_path", "postgres/fields/wrong_scope", "sqlite/fieldless/current", "sqlite/fieldless/malformed_bucket", "sqlite/fieldless/missing_header", "sqlite/fieldless/stale", "sqlite/fieldless/wrong_header_flow", "sqlite/fieldless/wrong_path", "sqlite/fieldless/wrong_scope", "sqlite/fields/current", "sqlite/fields/malformed_bucket", "sqlite/fields/missing_fields", "sqlite/fields/missing_header", "sqlite/fields/stale", "sqlite/fields/wrong_header_flow", "sqlite/fields/wrong_path", "sqlite/fields/wrong_scope"}},
	{pkg: "internal/store/internal/runtimepersistence", name: "TestSelectedContractActivitySourceProjectionBothStores", children: []string{"sqlite/root_source", "sqlite/root_source_independent_target", "sqlite/static_source", "sqlite/static_source_independent_target", "sqlite/root_payload_disagrees", "sqlite/static_payload_disagrees", "postgres/root_source", "postgres/root_source_independent_target", "postgres/static_source", "postgres/static_source_independent_target", "postgres/root_payload_disagrees", "postgres/static_payload_disagrees"}},
	{pkg: "internal/store/internal/runtimepersistence", name: "TestSelectedContractActivitySourceProjectionPreservesNumericPayloadBothStores", children: []string{"sqlite/root", "sqlite/static", "postgres/root", "postgres/static"}},
	{pkg: "internal/store/internal/runtimepersistence", name: "TestRunTerminalizationAtomicallyFencesGateActivationsAndCardsOnBothStores", children: []string{"sqlite", "postgres"}},
	{pkg: "internal/serveapp", name: "TestVerifyRunJobflowRegistryIntegrationBothStores", children: []string{"sqlite/clean/text", "sqlite/clean/json", "sqlite/drift/text", "sqlite/drift/json", "postgres/clean/text", "postgres/clean/json", "postgres/drift/text", "postgres/drift/json"}},
	{pkg: "internal/runtime/mutationlog", name: "TestVerifyRunJSONPreservesAtomicNumericEvidence"},
	{pkg: "internal/runtime/mutationlog", name: "TestVerifyRunProjectionDomainComparison"},
	{pkg: "internal/runtime/mutationlog", name: "TestVerifyRunProjectionEntityMembership"},
}

func verifyRunPublicChildren() []string {
	var children []string
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mutations := range []bool{false, true} {
			for _, missing := range []bool{false, true} {
				for _, mode := range []string{"json", "text", "quiet"} {
					for _, uppercase := range []bool{false, true} {
						children = append(children, fmt.Sprintf("%s/mutations_%t/missing_%t/%s/uppercase_%t", backend, mutations, missing, mode, uppercase))
					}
				}
			}
		}
	}
	return children
}

func loadVerifyRunPlanInputs(t *testing.T) (Policy, RootInventory, []ParityProof, []string) {
	t.Helper()
	policy := loadPersistenceDebtPolicy(t)
	root := filepath.Join("..", "..")
	inventory, err := DiscoverRootInventory(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	proofs, err := LoadParityProofs(filepath.Join(root, "internal/apiv1/testdata/public_surface_backend_matrix.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var packages []string
	for pkg := range inventory.Packages {
		packages = append(packages, pkg)
	}
	return policy, inventory, proofs, packages
}

func TestVerifyRunManagedSelectionRequiresEverySurfaceAndStore(t *testing.T) {
	policy, inventory, proofs, packages := loadVerifyRunPlanInputs(t)
	model := WeightModel{Version: WeightModelVersion, SourceRunID: "verify-run-partition"}
	for _, venue := range []string{VenueLocal, VenueCI} {
		for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
			t.Run(venue+"/"+tier, func(t *testing.T) {
				plan, err := BuildPlan(policy, model, packages, tier, "verify --run mandatory selection", "test-head", BuildOptions{Venue: venue})
				if err != nil {
					t.Fatal(err)
				}
				if err := BindExecution(&plan, inventory, proofs, policy); err != nil {
					t.Fatal(err)
				}
				for _, proof := range verifyRunProofs {
					key := TestRoot{Package: policy.Module + "/" + proof.pkg, Name: proof.name}
					var owners []string
					for _, unit := range plan.Units {
						for _, selected := range unit.SelectedRoots {
							if selected == key {
								owners = append(owners, unit.ID)
							}
						}
						for _, required := range unit.RequiredTests {
							if required.TestRoot != key {
								continue
							}
							want := slices.Clone(proof.children)
							if proof.name == "TestVerifyRunPublicAdmissionBothStores" {
								want = verifyRunPublicChildren()
							}
							slices.Sort(want)
							if !slices.Equal(required.Children, want) {
								t.Errorf("%s.%s children=%v, want %v", key.Package, key.Name, required.Children, want)
							}
						}
					}
					if len(owners) != 1 {
						t.Fatalf("%s.%s owners=%v, want exactly one", key.Package, key.Name, owners)
					}
					if tier == ProfileCore {
						switch proof.pkg {
						case "internal/cliapp":
							if owners[0] != "catalog-required-verify" {
								t.Fatalf("CLI owner=%s", owners[0])
							}
						case "internal/serveapp":
							if owners[0] != "verify-run-registry" {
								t.Fatalf("registry owner=%s", owners[0])
							}
						case "internal/store/internal/runtimepersistence":
							if owners[0] != "verify-run-native" {
								t.Fatalf("native owner=%s", owners[0])
							}
						}
					} else if owners[0] == "verify-run-native" || owners[0] == "verify-run-registry" {
						t.Fatalf("%s duplicates existing lifecycle/full partition", owners[0])
					}
					t.Logf("%s.%s owner=%s; required children=%d", key.Package, key.Name, owners[0], len(proof.children))
				}
			})
		}
	}
}

func TestVerifyRunManagedSelectionRejectsMissingSourceRoot(t *testing.T) {
	policy, inventory, proofs, packages := loadVerifyRunPlanInputs(t)
	for _, proof := range verifyRunProofs {
		t.Run(proof.name, func(t *testing.T) {
			copyInventory := inventory
			copyInventory.Packages = make(map[string]PackageRoots, len(inventory.Packages))
			for key, value := range inventory.Packages {
				copyInventory.Packages[key] = value
			}
			pkg := policy.Module + "/" + proof.pkg
			entry := copyInventory.Packages[pkg]
			entry.Roots = slices.DeleteFunc(slices.Clone(entry.Roots), func(name string) bool { return name == proof.name })
			copyInventory.Packages[pkg] = entry
			plan, err := BuildPlan(policy, WeightModel{Version: WeightModelVersion, SourceRunID: "missing-run-proof"}, packages, ProfileCore, "missing-root counterexample", "test-head", BuildOptions{Venue: VenueCI})
			if err != nil {
				t.Fatal(err)
			}
			err = BindExecution(&plan, copyInventory, proofs, policy)
			if err == nil {
				t.Fatalf("missing required root %s accepted", proof.name)
			}
			emptyRegistry := proof.name == "TestVerifyRunJobflowRegistryIntegrationBothStores" &&
				strings.Contains(err.Error(), "unit verify-run-registry selects no active test roots")
			if !strings.Contains(err.Error(), proof.name) && !emptyRegistry {
				t.Fatalf("missing required root %s misattributed: %v", proof.name, err)
			}
		})
	}
}
