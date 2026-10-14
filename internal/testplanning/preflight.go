package testplanning

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// Membership changes require reviewed catch/cost evidence. This is local-only;
// it must not enter Profiles, TierRank, CI-Units or hosted coverage deferrals.
var preflightCandidates = []TestRoot{
	preflightRoot("internal/cliapp", "TestVerifyBootStartupPredicateCensus"),
	preflightRoot("tools/fixture-codemod/pipeline-observations", "TestNativeRunSetupWholeFamilySnapshotsAndHostility"),
	preflightRoot("internal/runtime/conformance", "TestFinalFlowInstanceAuthoringFixture_RouteAuthorityBypassInventoryStaysClassified"),
	preflightRoot("internal/runtime/manager", "TestDynamicFlowRuntimeReadinessProductionConsumersStatic"),
	preflightRoot("internal/testplanning", "TestCurrentProofPlansBindActiveRequiredRoots"),
	preflightRoot("scripts/rewrite-stages-2566", "TestRewrite2566NoFinalRootCensusIsExplicitlyClassified"),
	preflightRoot("internal/store", "TestPersistenceAuthorityFindingRegistry"),
	preflightRoot("internal/store", "TestPersistenceEffectiveMethodSetsDoNotExposeRawAuthority"),
	preflightRoot("internal/store", "TestPersistenceAuthorityDebtRunFixturePolicyDestinationMatchesSource"),
	preflightRoot("tools/fixture-codemod/pipeline-observations", "TestServedDeliveryQuiescenceMechanicalCallerRecipesPreserveAllOtherWork"),
	preflightRoot("internal/store/internal/backend/authoractivity", "TestOrderingScopeConstructorInventoryIsClosed"),
	preflightRoot("internal/testutil", "TestPostgresTestEnvironmentHasNoCompetingReaderOrProjector"),
	preflightRoot("internal/testtiming", "TestMacOSPossessionSelectsSQLiteMatrixWithoutPostgres"),
	preflightRoot("internal/runtime/effects", "TestDirectPrimitiveOwnershipManifestIsTotal"),
	preflightRoot("internal/runtime/deliverycontinuation", "TestExecutableDeliveryContinuationHasClosedProductionConsumers"),
	preflightRoot("internal/runtime/runforkreadiness", "Test2376AdmissionSealsCompiledExecutionIdentity"),
	preflightRoot("internal/runtime/runforkreadiness", "TestAdmissionPreservesLoaderAdmittedEffectiveSourceContext"),
	preflightRoot("internal/runtime/runforkreadiness", "TestAdmissionRejectsIncompleteOrForeignBindings"),
	preflightRoot("internal/runtime/runforkreadiness", "TestAdmissionRequiresPositiveSealForEmptyProjection"),
	preflightRoot("internal/runtime/runforkreadiness", "TestAdmissionRetainsExactRunlessPromptWithoutPersistingIt"),
	preflightRoot("internal/runtime/runforkreadiness", "TestAdmissionSealsCompleteProjectionAndResolvedAgentAuthority"),
	preflightRoot("internal/runtime/runforkreadiness", "TestAdmissionSealsResolvedModelRevision"),
	preflightRoot("internal/runtime/runforkreadiness", "TestSelectedAgentConstructionUsesExactFixedHeader"),
	preflightRoot("internal/runtime/runforkreadiness", "TestSelectedConstructedActorProjectionCompleteCensus"),
	preflightRoot("internal/runtime/runforkreadiness", "TestSelectedContractAdmissionBindsHistoricalStateKindsAndPresence"),
	preflightRoot("internal/runtime/runforkreadiness", "TestSelectedContractHeaderMissingHistoricalEvidenceFailsClosed"),
	preflightRoot("internal/runtime/runforkreadiness", "TestSelectedContractHeaderRejectsEveryReceiverConfigCopy"),
	preflightRoot("internal/runtime/runforkreadiness", "TestSelectedContractRejectsContradictoryRecordedHeader"),
	preflightRoot("internal/runtime/runforkreadiness", "TestSelectedReadinessRequiresCompleteConstruction"),
	preflightRoot("internal/runtime/runforkexecution", "TestSelectedForkReceiverOwnershipBoundary"),
	preflightRoot("internal/runtime/core/worklifetime", "TestProductionAsyncSitesAreClassified"),
}

var preflightSafetyRoots = []TestRoot{
	preflightRoot("internal/testplanning", "TestPreflightSelectionRefusesMissingDuplicateAndForeignRoots"),
	preflightRoot("internal/testplanning", "TestPreflightPlanIntegrityAndSourceRefusals"),
	preflightRoot("internal/testplanning", "TestPreflightStructuralSelectionUsesCanonicalOwner"),
	preflightRoot("internal/testtiming", "TestPreflightReportRefusesIncompleteAndForeignProof"),
	preflightRoot("internal/testtiming", "TestFailureLedgerRefusesAmbiguousAndUnreviewedEvidence"),
	preflightRoot("internal/testtiming", "TestFailureLedgerMatchingNeverAdoptsForeignAttribution"),
	preflightRoot("internal/testtiming", "TestFailurePopulationRetainsUnknownMixedAndEarlierAttempts"),
	preflightRoot("internal/testtiming", "TestFailureLedgerFrozen2353SourceIndex"),
	preflightRoot("cmd/swarm-test", "TestPreflightCommandJoinsCancellationAndRefusesIncompleteExecution"),
	preflightRoot("cmd/swarm-test", "TestPreflightCurrentSourceUsesAllFiniteOwners"),
	preflightRoot("cmd/swarm-test-timing", "TestFailureObservationRetainsNativeBoundaries"),
}

// This compiler guard's two obligations are local selection requirements; its
// native root owner has no declared children. No hosted selection is changed.
var preflightRequiredChildren = map[TestRoot][]string{
	preflightRoot("internal/runtime/runforkexecution", "TestSelectedForkReceiverOwnershipBoundary"): {"hostile", "production"},
}

type PreflightPackage struct {
	Package  string         `json:"package"`
	Run      string         `json:"run"`
	Required []RequiredTest `json:"required"`
}

type PreflightPlan struct {
	Version      int                  `json:"version"`
	HeadSHA      string               `json:"head_sha"`
	BaseSHA      string               `json:"base_sha"`
	BuildContext BuildContext         `json:"build_context"`
	Packages     []PreflightPackage   `json:"packages"`
	Digest       string               `json:"digest"`
	Comparison   *PreflightComparison `json:"comparison,omitempty"`
}

type PreflightComparison struct {
	OriginMasterSHA string `json:"origin_master_sha"`
	DiffSHA256      string `json:"diff_sha256"`
	DebtRequired    bool   `json:"debt_required"`
}

var preflightDebtRoots = []TestRoot{
	preflightRoot("internal/store", "TestPersistenceAuthorityDebtRatchet"),
	preflightRoot("internal/store", "TestNativeFixtureFamiliesDoNotReceiveRawAuthority"),
}

func preflightRoot(pkg, name string) TestRoot {
	return TestRoot{Package: "github.com/division-sh/swarm/" + pkg, Name: name}
}

func BuildPreflightPlan(repo, base string, inventory RootInventory, native RunPlan, comparison PreflightComparison) (PreflightPlan, error) {
	if err := native.Validate(); err != nil {
		return PreflightPlan{}, err
	}
	if native.Profile != ProfileFull || native.Venue != VenueLocal || native.BuildContext != inventory.BuildContext {
		return PreflightPlan{}, fmt.Errorf("preflight requires the bound native local full inventory, not full execution")
	}
	structural, err := StaticAuthorityGuardRoots(repo)
	if err != nil {
		return PreflightPlan{}, err
	}
	roots := append(slices.Clone(preflightCandidates), structural...)
	roots = append(roots, preflightSafetyRoots...)
	if comparison.DebtRequired {
		roots = append(roots, preflightDebtRoots...)
	}
	owners := make(map[TestRoot][]RequiredTest)
	for _, unit := range native.Units {
		for _, root := range unit.RequiredTests {
			owners[root.TestRoot] = append(owners[root.TestRoot], root)
		}
	}
	var required []RequiredTest
	for _, root := range roots {
		if len(owners[root]) != 1 {
			return PreflightPlan{}, fmt.Errorf("preflight root %s/%s has %d native owners, want one", root.Package, root.Name, len(owners[root]))
		}
		proof := owners[root][0]
		proof.Children = append(slices.Clone(proof.Children), preflightRequiredChildren[root]...)
		sort.Strings(proof.Children)
		proof.Children = slices.Compact(proof.Children)
		required = append(required, proof)
	}
	plan, err := bindPreflightSelection(native.HeadSHA, base, inventory, required)
	if err != nil {
		return plan, err
	}
	plan.Comparison = &comparison
	plan.Digest = plan.digest()
	return plan, plan.Validate()
}

func bindPreflightSelection(head, base string, inventory RootInventory, required []RequiredTest) (PreflightPlan, error) {
	plan := PreflightPlan{Version: 1, HeadSHA: head, BaseSHA: base, BuildContext: inventory.BuildContext}
	seen := map[TestRoot]bool{}
	grouped := map[string][]RequiredTest{}
	for _, root := range required {
		pkg, ok := inventory.Packages[root.Package]
		if !ok || !pkg.HasTestFiles || !slices.Contains(pkg.Roots, root.Name) || seen[root.TestRoot] || strings.Contains(root.Name, "/") {
			return plan, fmt.Errorf("missing, foreign or duplicate preflight root %s/%s", root.Package, root.Name)
		}
		seen[root.TestRoot] = true
		root.Children = slices.Clone(root.Children)
		grouped[root.Package] = append(grouped[root.Package], root)
	}
	for pkg, roots := range grouped {
		sort.Slice(roots, func(i, j int) bool { return roots[i].Name < roots[j].Name })
		plan.Packages = append(plan.Packages, PreflightPackage{Package: pkg, Run: preflightSelector(roots), Required: roots})
	}
	sort.Slice(plan.Packages, func(i, j int) bool { return plan.Packages[i].Package < plan.Packages[j].Package })
	plan.Digest = plan.digest()
	return plan, plan.Validate()
}

func preflightSelector(roots []RequiredTest) string {
	var names []string
	for _, root := range roots {
		names = append(names, regexp.QuoteMeta(root.Name))
	}
	return "^(" + strings.Join(names, "|") + ")$"
}

func (p PreflightPlan) Validate() error {
	head, headErr := hex.DecodeString(p.HeadSHA)
	base, baseErr := hex.DecodeString(p.BaseSHA)
	if p.Version != 1 || headErr != nil || len(head) != 20 || baseErr != nil || len(base) != 20 || len(p.Packages) == 0 || p.Digest != p.digest() {
		return fmt.Errorf("invalid or changed local preflight identity")
	}
	if err := p.BuildContext.Validate(); err != nil {
		return err
	}
	if p.Comparison != nil {
		origin, originErr := hex.DecodeString(p.Comparison.OriginMasterSHA)
		diff, diffErr := hex.DecodeString(p.Comparison.DiffSHA256)
		if originErr != nil || len(origin) != 20 || diffErr != nil || len(diff) != 32 {
			return fmt.Errorf("invalid frozen preflight comparison")
		}
	}
	seen := map[TestRoot]bool{}
	for _, pkg := range p.Packages {
		if pkg.Package == "" || len(pkg.Required) == 0 || pkg.Run != preflightSelector(pkg.Required) {
			return fmt.Errorf("invalid finite preflight package %q", pkg.Package)
		}
		for _, root := range pkg.Required {
			if root.Package != pkg.Package || root.Name == "" || seen[root.TestRoot] || strings.Contains(root.Name, "/") {
				return fmt.Errorf("invalid preflight root %s/%s", root.Package, root.Name)
			}
			seen[root.TestRoot] = true
		}
	}
	if p.Comparison != nil {
		for _, root := range preflightDebtRoots {
			if seen[root] != p.Comparison.DebtRequired {
				return fmt.Errorf("preflight debt eligibility and canonical root %s disagree", root.Name)
			}
		}
	}
	return nil
}

func (p PreflightPlan) digest() string {
	p.Digest = ""
	raw, _ := json.Marshal(p)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
