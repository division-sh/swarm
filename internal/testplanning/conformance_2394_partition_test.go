package testplanning

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var conformance2394Units = []string{
	"conformance-2", "conformance-2394-core", "conformance-2394-pressure", "conformance-2394-reporter",
}

const conformance2394Soak = "TestIssue2394TwentyTwoIntentFifteenMinuteSoakBothStores"

func conformance2394Fixture(t *testing.T) (Policy, []string, string) {
	t.Helper()
	root := filepath.Join("..", "..")
	f, err := os.Open(filepath.Join(root, ".github", "test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	policy, err := LoadPolicy(f)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "internal/runtime/conformance")
	paths, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, path := range paths {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Recv == nil && (strings.HasPrefix(fn.Name.Name, "Test") || strings.HasPrefix(fn.Name.Name, "Example") || strings.HasPrefix(fn.Name.Name, "Fuzz")) {
				names = append(names, fn.Name.Name)
			}
		}
	}
	sort.Strings(names)
	return policy, names, dir
}

func validateConformance2394Partition(policy Policy, names []string) ([][]string, error) {
	pkg := []string{"github.com/division-sh/swarm/internal/runtime/conformance"}
	patterns := make([]*regexp.Regexp, len(conformance2394Units))
	groups := make([][]string, len(patterns))
	for i, id := range conformance2394Units {
		u := policy.Units[id]
		if !reflect.DeepEqual(u.Packages, pkg) || u.CountMode != "count-1" || u.EnvironmentID != "ci-postgres-gateway-empty-v1" || u.BudgetClass != "broad" || u.GoTimeout != "" || u.Skip != "" || strings.Contains(u.Run, "/") {
			return nil, fmt.Errorf("%s changed package/count/environment/budget or filtered a root: %+v", id, u)
		}
		var err error
		patterns[i], err = regexp.Compile(u.Run)
		if err != nil {
			return nil, err
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		id := "conformance-soak-" + backend
		want := UnitPolicy{Packages: pkg, Run: "^" + conformance2394Soak + "$/^" + backend + "$", GoTimeout: "22m", CountMode: "count-1", EnvironmentID: "ci-postgres-gateway-empty-v1", BudgetClass: "soak"}
		if !reflect.DeepEqual(policy.Units[id], want) {
			return nil, fmt.Errorf("%s changed mandatory original soak: %+v", id, policy.Units[id])
		}
	}
	ids := append(append([]string{}, conformance2394Units...), "conformance-soak-sqlite", "conformance-soak-postgres")
	for _, profile := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
		for _, id := range ids {
			count := 0
			for _, member := range policy.Profiles[profile].Units {
				if member == id {
					count++
				}
			}
			want := 1
			if profile == ProfileCore && id != "conformance-2394-pressure" || profile != ProfileFull && (strings.HasPrefix(id, "conformance-soak-") || id == "conformance-2394-reporter") {
				want = 0
			}
			if count != want {
				return nil, fmt.Errorf("%s has %d scheduled %s units, want %d", profile, count, id, want)
			}
		}
	}
	old := regexp.MustCompile(`^(Test($|[^FHV].*)|Example.*|Fuzz.*)$`)
	for _, name := range names {
		owners := 0
		for i, pattern := range patterns {
			if pattern.MatchString(name) {
				owners++
				groups[i] = append(groups[i], name)
			}
		}
		want := 0
		if old.MatchString(name) && name != conformance2394Soak {
			want = 1
		}
		if owners != want {
			return nil, fmt.Errorf("%s has %d owners, want %d from original conformance-2", name, owners, want)
		}
	}
	for i, group := range groups {
		if len(group) == 0 {
			return nil, fmt.Errorf("%s is empty", conformance2394Units[i])
		}
	}
	return groups, nil
}

func TestConformanceVolumeFanOutProofPartition(t *testing.T) {
	policy, names, _ := conformance2394Fixture(t)
	const unitID = "conformance-heavy-fanout"
	const selection = `^TestVolumeFanOut(ExactJobflow1362ImportRouteAndSettleBothStores|ServingCardinalityMixedOutputPartitionEquivalenceBothStores)$`
	unit := policy.Units[unitID]
	want := UnitPolicy{
		Packages: []string{"github.com/division-sh/swarm/internal/runtime/conformance"},
		Run:      selection, CountMode: "count-1", EnvironmentID: "ci-postgres-gateway-empty-v1", BudgetClass: "full",
	}
	if !reflect.DeepEqual(unit, want) {
		t.Fatalf("heavy fan-out proof envelope changed: %+v", unit)
	}
	selected := regexp.MustCompile(selection)
	general := regexp.MustCompile(policy.Units["conformance-1"].Run)
	complement := regexp.MustCompile(policy.Units["conformance-2"].Run)
	var selectedNames []string
	for _, name := range names {
		if !strings.HasPrefix(name, "TestVolume") {
			continue
		}
		if !selected.MatchString(name) || general.MatchString(name) || complement.MatchString(name) {
			t.Fatalf("volume proof %s is missing or overlaps another conformance unit", name)
		}
		selectedNames = append(selectedNames, name)
	}
	if len(selectedNames) != 2 {
		t.Fatalf("heavy fan-out partition has %d roots, want two: %v", len(selectedNames), selectedNames)
	}
	for _, profile := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
		count := 0
		for _, id := range policy.Profiles[profile].Units {
			if id == unitID {
				count++
			}
		}
		want := 0
		if profile == ProfileFull {
			want = 1
		}
		if count != want {
			t.Fatalf("%s schedules %s %d times, want %d", profile, unitID, count, want)
		}
	}
}

func TestConformance2394PartitionPreservesCompleteRoots(t *testing.T) {
	policy, names, dir := conformance2394Fixture(t)
	groups, err := validateConformance2394Partition(policy, names)
	if err != nil {
		t.Fatal(err)
	}
	// #2307 adds these eight general roots to the reviewed 113-root census.
	// The generated-results guard, #2323 proof-census regression, and two
	// #2300 decode-site roots add four more; #2323 adds two YAML checkout
	// boundary proofs; #2304 adds two event-authority guards. No prior root,
	// backend, or command envelope moves partitions.
	actionRetirementRoots := []string{
		"TestActionRetirementCorpusLedgerIsComplete",
		"TestActionRetirementCorpusHasNoLiveAuthoredActions",
		"TestActionRetirementCorpusScannerPreservesHomonymsAndRejectsAliases",
		"TestActionRetirementCorpusScannerRejectsTruncatedFragments",
		"TestRetiredActionHistoricalFixtureFailsClosed",
		"TestNoRetiredHandlerActionInterpreters",
		"TestNoRetiredHandlerActionInterpretersRejectsHostileRestoration",
		"TestCanonicalFormsRegistryPinsHandlerActionRetirement",
	}
	// #2281 adds eighteen general roots alongside master's W4/W5 guard root.
	deploymentFeedRoots := []string{
		"TestDeploymentResourceRunStartEmptyVersionDoesNotInventReceiverBothStores",
		"TestDeploymentResourceRunStartPinDocumentRowsBothStores",
		"TestDeploymentSourceChangedPinPublicForkAndLostResponse",
		"TestDeploymentSourceDynamicReceiverSelectedForkRefusesWithoutMutation",
		"TestDeploymentSourceEmptyVersionForkIsQuiescentAndReplayable",
		"TestDeploymentSourceFixedTPendingReceiverAndChangedPinBothStores",
		"TestDeploymentSourceFixedTPublishedBeforePipelineChangedPinBothStores",
		"TestDeploymentSourceKeylessEqualRowsPreserveMultiplicityBothStores",
		"TestDeploymentSourceQuiescedBeforeActivationCrashBothStores",
		"TestDeploymentSourceSelectedControlOnlyRepeatedStopBothStores",
		"TestDeploymentSourceSelectedForkBeforeFirstRow",
		"TestDeploymentSourceSelectedForkConcurrentFirstMaterialization",
		"TestDeploymentSourceSelectedLastRowPendingBlocksQuiescenceBothStores",
		"TestDeploymentSourceSelectedPredecessorClaimCannotSettleAfterRecoveryBothStores",
		"TestDeploymentSourceTwoPinnedFeedsSettleIndependentlyBothStores",
		"TestSelectedDeploymentExternalEffectRecoveryBothStores",
		"TestSelectedExternalEffectFixtureControlBothStores",
		"TestServedParityHarnessRunStartDeploymentFeedLifecycle",
	}
	eventAuthorityRoots := []string{
		"TestCanonicalFormsEventAuthorityRetirement",
		"TestCanonicalFormsEventAuthorityGuardDetectsRestoredPrivateConstructors",
	}
	fileRowRoots := []string{
		"TestDataTextFile2456KeylessOperatorRouteRestartReplayBothStores",
		"TestDataTextFile2456Keyed37DynamicReceiversBothStores",
		"TestDataTextFile2456MultiFieldAndLimitsBothStores",
		"TestDataTextFile2456RejectedMultiPinServedReplayBothStores",
		"TestDataTextFile2456ConcurrentFirstAttemptBothStores",
		"TestDataTextFile2456HostileGrammarNoMutationBothStores",
		"TestStandaloneTextFileImportSupport2456BothStores",
		"TestStandaloneTextFileEmptyKeyedRequiredDirectory2456BothStores",
	}
	fileRowLongRoots := []string{
		"TestFileRow2456Keyed37DeltaOldPinBothStores",
		"TestFileRow2456FreshProcessBindingAndExactWireBothStores",
		"TestFileRow2456DefaultBundleAndHeadPinReplayBothStores",
	}
	// A2 replaces the stream/window and singleton lifecycle roots with ordinary
	// keyed stream/lifecycle proofs, and adds repeated fan-out isolation. The
	// event-ID carry assertion now lives in the keyed stream journey; event-ID
	// pin dedup is retired. All surviving roots still have exactly one owner.
	a2Roots := []string{
		"TestA2RepeatedFanOutTriggerIsolationOnBothStores",
		"TestKeyedPortfolioStreamRoutesAndRetainsIndependentPeriodsOnBothStores",
		"TestKeyedStageLifecyclePreservesRouteAndEntityAcrossRestartOnBothBackends",
	}
	// #2486 adds the accepted-base fixture-relocation ratchet; no root is removed.
	// #2241 adds the backend/delay isolation proof for the authorized reporter ceiling.
	// #2376 adds one manifest-reader ledger ratchet without moving existing roots.
	// #2438 adds the names-only output ledger mutation proof; all prior roots remain.
	// #2496 adds the event variant of the existing public permanent-result journey.
	// Its explicit-empty reporter regression also proves exact no-write settlement.
	want := []int{166, 14, 5, 1}
	for i, group := range groups {
		if len(group) != want[i] {
			t.Fatalf("%s census=%d, want reviewed %d; account new roots explicitly", conformance2394Units[i], len(group), want[i])
		}
		for _, name := range group {
			t.Logf("%s\t%s", conformance2394Units[i], name)
		}
	}
	const preparedFaultProof = "TestSemanticProofPreparedFaultMatchesRawBothStores"
	const emptyReporterProof = "TestEmptyReporterSettlesWithoutConstructedHeaderMutationBothStores"
	if i := sort.SearchStrings(groups[0], emptyReporterProof); i == len(groups[0]) || groups[0][i] != emptyReporterProof {
		t.Fatalf("#2496 empty reporter proof missing from %s", conformance2394Units[0])
	}
	const manifestReaderProof = "Test2376ManifestReaderLedgerIsAdditive"
	if i := sort.SearchStrings(groups[0], manifestReaderProof); i == len(groups[0]) || groups[0][i] != manifestReaderProof {
		t.Fatalf("#2376 manifest reader proof missing from %s", conformance2394Units[0])
	}
	const relocationProof = "TestCatalogFixtureDecodeRelocationPreservesAcceptedBaseBudget"
	if i := sort.SearchStrings(groups[0], relocationProof); i == len(groups[0]) || groups[0][i] != relocationProof {
		t.Fatalf("#2486 relocation proof missing from %s", conformance2394Units[0])
	}
	const nodeAdmissionProof = "TestNodeAdmissionUsesSourceValueWithoutRawDecoderFallback"
	const schemaAdmissionProof = "TestSchemaAdmissionOwnershipHasNoRetiredInterpreter"
	const reporterCeilingProof = "TestNumericReporterIssuanceMergeCeilingIsolation"
	if i := sort.SearchStrings(groups[0], reporterCeilingProof); i == len(groups[0]) || groups[0][i] != reporterCeilingProof {
		t.Fatalf("general conformance partition omitted reporter ceiling isolation proof %s", reporterCeilingProof)
	}
	if i := sort.SearchStrings(groups[0], schemaAdmissionProof); i == len(groups[0]) || groups[0][i] != schemaAdmissionProof {
		t.Fatalf("schema admission ownership proof missing from %s", conformance2394Units[0])
	}
	if i := sort.SearchStrings(groups[0], nodeAdmissionProof); i == len(groups[0]) || groups[0][i] != nodeAdmissionProof {
		t.Fatalf("#2489 source admission proof is missing from conformance-2: %v", groups[0])
	}
	const checkoutProof = "TestProducerRoutingProofCensusRejectsForeignOnlyEntrypoint"
	if i := sort.SearchStrings(groups[0], checkoutProof); i == len(groups[0]) || groups[0][i] != checkoutProof {
		t.Fatalf("#2323 checkout proof is missing from conformance-2: %v", groups[0])
	}
	const yamlCheckoutProof = "TestProducerRoutingYAMLCensusExcludesNestedCheckout"
	if i := sort.SearchStrings(groups[0], yamlCheckoutProof); i == len(groups[0]) || groups[0][i] != yamlCheckoutProof {
		t.Fatalf("#2323 YAML checkout proof is missing from conformance-2: %v", groups[0])
	}
	if i := sort.SearchStrings(groups[0], preparedFaultProof); i == len(groups[0]) || groups[0][i] != preparedFaultProof {
		t.Fatalf("general conformance partition omitted %s", preparedFaultProof)
	}
	for _, name := range actionRetirementRoots {
		if i := sort.SearchStrings(groups[0], name); i == len(groups[0]) || groups[0][i] != name {
			t.Fatalf("general conformance partition omitted reviewed #2307 root %s", name)
		}
	}
	for _, name := range a2Roots {
		if i := sort.SearchStrings(groups[0], name); i == len(groups[0]) || groups[0][i] != name {
			t.Fatalf("general conformance partition omitted A2 replacement root %s", name)
		}
	}
	for _, name := range []string{
		"TestFanInStreamConformance_RoutesToSingletonAndKernelEnforcesWindowedDedup",
		"TestFanInStreamConformance_EventIDDedupUsesEventIdentity",
		"TestCreateEventIDCarryProjectionReachesHandler",
		"TestSingletonStageLifecyclePreservesRouteAndEntityAcrossRestartOnBothBackends",
	} {
		if i := sort.SearchStrings(names, name); i < len(names) && names[i] == name {
			t.Fatalf("retired A2 proof root survived its replacement: %s", name)
		}
	}
	for _, name := range deploymentFeedRoots {
		if i := sort.SearchStrings(groups[0], name); i == len(groups[0]) || groups[0][i] != name {
			t.Fatalf("general conformance partition omitted reviewed #2281 root %s", name)
		}
	}
	const eventBranchProof = "TestEventSourceChangedPinPublicForkAndLostResponse"
	if i := sort.SearchStrings(groups[0], eventBranchProof); i == len(groups[0]) || groups[0][i] != eventBranchProof {
		t.Fatalf("general conformance partition omitted typed event-branch proof %s", eventBranchProof)
	}
	for _, name := range eventAuthorityRoots {
		if i := sort.SearchStrings(groups[0], name); i == len(groups[0]) || groups[0][i] != name {
			t.Fatalf("general conformance partition omitted reviewed #2304 root %s", name)
		}
	}
	for _, name := range fileRowRoots {
		if i := sort.SearchStrings(groups[0], name); i == len(groups[0]) || groups[0][i] != name {
			t.Fatalf("general conformance partition omitted reviewed #2456 root %s", name)
		}
	}
	fileRowLong := regexp.MustCompile(policy.Units["conformance-1"].Run)
	for _, name := range fileRowLongRoots {
		if i := sort.SearchStrings(names, name); i == len(names) || names[i] != name {
			t.Fatalf("#2456 long file-row root %s is missing", name)
		}
		i := sort.SearchStrings(groups[0], name)
		if !fileRowLong.MatchString(name) || i < len(groups[0]) && groups[0][i] == name {
			t.Fatalf("#2456 long file-row root %s must belong only to conformance-1", name)
		}
	}
	const generatedResultsProof = "TestActionRetirementCorpusExcludesGeneratedTestResults"
	if i := sort.SearchStrings(groups[0], generatedResultsProof); i == len(groups[0]) || groups[0][i] != generatedResultsProof {
		t.Fatalf("general conformance partition omitted generated-results guard %s", generatedResultsProof)
	}
	for _, name := range []string{"TestCanonicalFormsRegistryRatchetsOffOwnerDecodeBypasses", "TestCanonicalFormsRegistryRejectsUnregisteredDecodeBypasses", "TestCanonicalFormsDecodeSiteCensusExcludesNestedCheckout", "TestDecodeBypassFamilyCorrectionRequiresUnchangedAgentWriteSite"} {
		if i := sort.SearchStrings(groups[0], name); i == len(groups[0]) || groups[0][i] != name {
			t.Fatalf("general conformance partition omitted #2300 census root %s", name)
		}
	}
	const reporterProof = "TestIssue2394ReporterFiveHundredDelayedCommitsBothStores"
	if i := sort.SearchStrings(groups[3], reporterProof); i == len(groups[3]) || groups[3][i] != reporterProof {
		t.Fatalf("reporter conformance partition omitted %s", reporterProof)
	}
	t.Log("complete disjoint census:187 =164 general +14 core +5 pressure +1 reporter +3 long file-row roots in conformance-1")
	for _, profile := range []string{ProfileFull} {
		var units []ProofUnit
		for _, id := range policy.Profiles[profile].Units {
			u := policy.Units[id]
			for _, pkg := range u.Packages {
				if pkg == policy.Module+"/internal/runtime/conformance" {
					units = append(units, ProofUnit{ID: id, Packages: u.Packages, Run: u.Run, Skip: u.Skip, GoTimeout: u.GoTimeout, CountMode: u.CountMode, BudgetClass: u.BudgetClass})
				}
			}
		}
		if err := ValidateConformanceProofPartition(dir, units); err != nil {
			t.Fatalf("%s: %v", profile, err)
		}
	}
}

func TestConformance2394PartitionRejectsScopeDrift(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*UnitPolicy)
	}{
		{"omitted", func(u *UnitPolicy) { u.Run = "^$" }},
		{"overlap", func(u *UnitPolicy) { u.Run = "^TestIssue2394.*$" }},
		{"foreign_root", func(u *UnitPolicy) { u.Run = "^Test.*$" }},
		{"backend_skip", func(u *UnitPolicy) { u.Skip = "postgres" }},
		{"backend_selection", func(u *UnitPolicy) { u.Run += "/sqlite" }},
		{"count", func(u *UnitPolicy) { u.CountMode = "cache-default" }},
		{"budget", func(u *UnitPolicy) { u.BudgetClass = "full" }},
		{"go_timeout", func(u *UnitPolicy) { u.GoTimeout = "5m" }},
		{"environment", func(u *UnitPolicy) { u.EnvironmentID = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy, names, _ := conformance2394Fixture(t)
			u := policy.Units["conformance-2394-core"]
			tc.edit(&u)
			policy.Units["conformance-2394-core"] = u
			if _, err := validateConformance2394Partition(policy, names); err == nil {
				t.Fatal("changed root scope/envelope accepted")
			}
		})
	}
	t.Run("unclassified_new_root", func(t *testing.T) {
		policy, names, _ := conformance2394Fixture(t)
		names = append(names, "TestIssue2394UnclassifiedNewProof")
		if _, err := validateConformance2394Partition(policy, names); err == nil {
			t.Fatal("new unselected ordinary root accepted")
		}
	})
	t.Run("optional_profile", func(t *testing.T) {
		policy, names, _ := conformance2394Fixture(t)
		p := policy.Profiles[ProfileCore]
		var keep []string
		for _, id := range p.Units {
			if id != "conformance-2394-pressure" {
				keep = append(keep, id)
			}
		}
		p.Units = keep
		policy.Profiles[ProfileCore] = p
		if _, err := validateConformance2394Partition(policy, names); err == nil {
			t.Fatal("optional ordinary unit accepted")
		}
	})
	t.Run("shortened_soak", func(t *testing.T) {
		policy, names, _ := conformance2394Fixture(t)
		u := policy.Units["conformance-soak-postgres"]
		u.GoTimeout = "3m"
		policy.Units["conformance-soak-postgres"] = u
		if _, err := validateConformance2394Partition(policy, names); err == nil {
			t.Fatal("changed original soak accepted")
		}
	})
}
