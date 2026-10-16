package store_test

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Ruling6200510991: exactly the accepted B/E inputs and the pinned release input.
// Old collector constants, source proofs and baseline maps remain immutable.
const debtCatchUpNativeEntryLane = "74fd91d46e7c13e358858dfd03ae2b283769c67a"
const debtCatchUpNativeEntryInput = "4d175b5b225c17f1ddd11d303cefa6b17a769028"
const debtCatchUpNativeEntryMaster = "53788da1332cbecaf805b13722316257d7ac6829"
const debtCatchUpNativeEntryCollectorTo = "f6d712543a35e26917682ae29347e1eb5c293082f0ac6bd500d63a1418577a31"

var debtCatchUpNativeEntryNativePins = map[string]string{
	"internal/store/internal/backend/sqlite/transaction.go":                    "8c24f9daea15f4ab4959360f74c449348d93b2b57374695eb190ac2a63295c65",
	"internal/store/internal/backend/sqlite/test_agent_snapshot_corruption.go": "9c47a40cc1093c0fa7f6e19de539b93264a2cc8bd9ed8865e8d0bc3668236ccc",
	"internal/store/internal/backend/postgres/transaction.go":                  "604eab38ee4d7c4ad899a4b82662320ea3a7070be41a92da0277309de5d02197",
	"internal/runtime/startupownership/agent_recovery_freshness.go":            "638e0a9274a32be119904d643e19aa9bb9e093b55604f632149ae2f405690f6c",
	"internal/runtime/startupownership/agent_recovery_claim_handoff.go":        "8b57ca14a1b71bc2be4c44718faefa1d3247ea9332ab0ab6d3fbe163600e9b44",
	"internal/runtime/startupownership/agent_recovery_seed_check.go":           "15c24972306542afc7ef9dc354ccb4e0407fd800ef559022d787be37821f2b21",
	"internal/store/internal/startupownership/agent_recovery_claim.go":         "cf2b5007458b2e15023f2d2588a9725e5b817d5b93f4a08e28fee86be0a5ba51",
	"internal/store/internal/startupownership/process_operations.go":           "4c59d5e33a1a122525bc29debd9a8dbaf7c04eb03cba548cab62e16591098821",
	"internal/store/internal/runtimepersistence/composition.go":                "1ff439f97646d8df6a4e9d333a4f3b581902278d7e8233fe92f33bb72c01ea36",
	"internal/store/internal/backend/delivery/lifecycle.go":                    "592e5497fdb0d2f151ca0cf94c7fd7e9001d1f04c3b8b13e1dc98e6681451470",
	"internal/runtime/conformance/delivery_lifecycle_conformance_test.go":      "3407e380b9946c0563079df9815615fe025bda389873e3427557065b44bda2b3",
}

var debtCatchUpNativeEntryDeclarationPins = map[string]string{
	"func:debtValidateCollectorIdentity":             "4028f7f8bc9217844356a2a850df308b6ef5363afea96d9278e5b2b622e2bb20",
	"func:debtCatchUpCollectorSource":                "95faa08316a3402d0b219dac7d0fa2726d98cace8b782f633371f91cc0eaf235",
	"func:debtSourceUnrefreshedBaseline":             "0b5c6bcba43eb761a24e49e9af81a26525a43ceac083a6bf56c13c561209af83",
	"func:debtCatchUpNativeEntryCollectorTransition": "d09f075a37cd0439c6cf07e071d68acbdde28e2d63526c54c362c455bb3ef9de",
	"func:debtCatchUpNativeEntryPredecessors":        "9cfbb1d7119b9e49729b2471a480f23225f14ea5a71ca6639e58217ad61746a2",
	"func:debtCatchUpNativeEntryOwnerSource":         "0ac3f18d9971f3d35e3b0fee57d689d93ef37cfb13810d2eb98dbf72cec410b0",
	"func:debtCatchUpNativeEntryUnrefreshedBaseline": "a10d6720e1e4033292f7a0c9631532984b903a3c1e2046194315124d886c9fd9",
}

func TestPersistenceAuthorityDebtNativeEntryCatchUpSourceIsExact(t *testing.T) {
	root := debtCatchUpHistoricalArchive(t, debtRepair2698Before)
	if actual, err := debtCollectorDigest(root); err != nil || actual != debtCatchUpNativeEntryCollectorTo {
		t.Fatalf("native-entry collector=%s expected=%s: %v", actual, debtCatchUpNativeEntryCollectorTo, err)
	}
	for _, path := range debtCatchUpCollectorFiles {
		prior, err := debtGit(root, "show", debtCatchUpNativeEntryLane+":"+path)
		if err != nil {
			t.Fatal(err)
		}
		current, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		before, after := debtCatchUpDeclarations(t, prior), debtCatchUpDeclarations(t, current)
		for key, body := range after {
			if pin, composed := debtCatchUpNativeEntryDeclarationPins[key]; composed {
				if debtBaselineChecksum([]byte(body)) != pin {
					t.Errorf("native-entry composed declaration changed: %s/%s", path, key)
				}
			} else if before[key] != body {
				t.Errorf("unlisted collector declaration change: %s/%s", path, key)
			}
		}
		for key := range before {
			if _, exists := after[key]; !exists {
				t.Errorf("lost prior collector declaration: %s/%s", path, key)
			}
		}
	}
	if err := debtCatchUpCollectorSource(root); err != nil {
		t.Fatal(err)
	}
	proveNativeEntryOwnerComposition(t, root)
	proveNativeEntryConformanceAssertions(t, root)
}

func proveNativeEntryOwnerComposition(t *testing.T, root string) {
	t.Helper()
	for path := range debtCatchUpNativeEntryNativePins {
		if path == "internal/store/internal/runtimepersistence/composition.go" || path == "internal/store/internal/backend/delivery/lifecycle.go" ||
			path == "internal/runtime/conformance/delivery_lifecycle_conformance_test.go" {
			continue
		}
		incoming, err := debtGit(root, "show", debtCatchUpNativeEntryInput+":"+path)
		if err != nil {
			t.Fatal(err)
		}
		current, err := os.ReadFile(filepath.Join(root, path))
		if err != nil || !bytes.Equal(incoming, current) {
			t.Errorf("native owner differs from exact accepted packet: %s: %v", path, err)
		}
	}
	proveNativeEntryCompositionBindings(t, root)
	const lifecycle = "internal/store/internal/backend/delivery/lifecycle.go"
	incoming, err := debtGit(root, "show", debtCatchUpNativeEntryInput+":"+lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(filepath.Join(root, lifecycle))
	if err != nil {
		t.Fatal(err)
	}
	old, actual := debtCatchUpDeclarations(t, incoming), debtCatchUpDeclarations(t, current)
	for _, key := range []string{"func:claimDeliveryWithLease*DeliveryPostgresOwner", "func:claimDeliveryWithLease*DeliverySQLiteOwner"} {
		if old[key] == "" || actual[key] != old[key] {
			t.Errorf("native claim entry differs from reviewed E owner: %s", key)
		}
	}
	const guard = "internal/store/internal/runtimepersistence/operator_snapshot_boundary_guard_test.go"
	approved, err := debtGit(root, "show", debtCatchUpNativeEntryLane+":"+guard)
	if err != nil {
		t.Fatal(err)
	}
	actualGuard, err := os.ReadFile(filepath.Join(root, guard))
	if err != nil || !bytes.Equal(approved, actualGuard) {
		t.Fatal("accepted exact topology role/consumer guard changed:", err)
	}
}

func proveNativeEntryCompositionBindings(t *testing.T, root string) {
	t.Helper()
	const path = "internal/store/internal/runtimepersistence/composition.go"
	before, err := debtGit(root, "show", debtCatchUpNativeEntryLane+":"+path)
	if err != nil {
		t.Fatal(err)
	}
	want := string(before)
	for _, backend := range []string{"Postgres", "SQLite"} {
		anchor := "\tif err := storestartupownership.Bind" + backend + "AgentRecoveryReader(startupOwner, store); err != nil {\n\t\treturn nil, err\n\t}\n"
		addition := "\tif err := storestartupownership.Bind" + backend + "AgentRecoveryClaims(startupOwner, deliveryOwner); err != nil {\n\t\treturn nil, err\n\t}\n"
		if strings.Count(want, anchor) != 1 {
			t.Fatal("source composition lost its unique original assembly anchor:", backend)
		}
		want = strings.Replace(want, anchor, anchor+addition, 1)
	}
	actual, err := os.ReadFile(filepath.Join(root, path))
	if err != nil || string(actual) != want {
		t.Fatal("assembly changed beyond the two exact accepted once-bound claim owners:", err)
	}
}

func proveNativeEntryConformanceAssertions(t *testing.T, root string) {
	t.Helper()
	const path = "internal/runtime/conformance/delivery_lifecycle_conformance_test.go"
	before, err := debtGit(root, "show", debtCatchUpNativeEntryLane+":"+path)
	if err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Fatal(err)
	}
	old, actual := debtCatchUpDeclarations(t, before), debtCatchUpDeclarations(t, current)
	beforeLiterals := nativeEntryProofLiterals(t, old["func:TestExecutableDeliveryLifecycleParity"])
	currentLiterals := nativeEntryProofLiterals(t, actual["func:TestExecutableDeliveryLifecycleParity"])
	for literal, count := range beforeLiterals {
		if currentLiterals[literal] < count {
			t.Error("original conformance proof literal lost:", literal)
		}
	}
}

func nativeEntryProofLiterals(t *testing.T, source string) map[string]int {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "proof.go", "package proof\n"+source, 0)
	if err != nil {
		t.Fatal(err)
	}
	literals := map[string]int{}
	ast.Inspect(file, func(node ast.Node) bool {
		if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			literals[literal.Value]++
		}
		return true
	})
	return literals
}

func TestPersistenceAuthorityDebtNativeEntryCatchUpForwardAndForeignProvenance(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	head := nativeEntryHeadBaseline(t, root)
	proof := debtDeliveryRetirementProof{Root: root, Actual: head.Sites}
	for _, input := range [][2]string{{debtCatchUpNativeEntryLane, debtCatchUpFreshnessCollectorTo}, {debtCatchUpNativeEntryInput, debtProcessLeaseFreshnessCollectorTo}, {debtCatchUpNativeEntryMaster, debtProcessLeaseCollectorTo}} {
		prior, _, err := debtCatchUpSignedBaseline(root, input[0], input[1])
		if err != nil {
			t.Fatal(err)
		}
		if err := debtValidateCollectorIdentity(prior.Collector, head.Collector, prior, head, "", proof); err != nil {
			t.Fatalf("exact native-entry forward input refused: %s: %v", input[0], err)
		}
		proveNativeEntryForeignProvenance(t, prior, head, proof)
	}
}

func nativeEntryHeadBaseline(t *testing.T, root string) authorityDebtBaseline {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, debtBaselinePath))
	if err != nil {
		t.Fatal(err)
	}
	head, err := parseAuthorityDebtBaseline(raw)
	if err != nil {
		t.Fatal(err)
	}
	head.Collector = debtCatchUpNativeEntryCollectorTo
	return head
}

func proveNativeEntryForeignProvenance(t *testing.T, prior, head authorityDebtBaseline, proof debtDeliveryRetirementProof) {
	t.Helper()
	for _, mutation := range []string{"foreign", "bootstrap", "reverse", "changed-predecessor", "new-site", "increase"} {
		before, after, actual := prior, head, maps.Clone(head.Sites)
		before.Sites, after.Sites = maps.Clone(prior.Sites), maps.Clone(head.Sites)
		switch mutation {
		case "foreign":
			before.Collector = strings.Repeat("0", 64)
		case "bootstrap":
			after.BootstrapSource = strings.Repeat("0", 40)
		case "reverse":
			before, after = after, before
		case "changed-predecessor":
			for key := range before.Sites {
				delete(before.Sites, key)
				break
			}
		case "new-site", "increase":
			for _, row := range actual {
				if mutation == "new-site" {
					row.Declaration = "foreignNativeEntryClaim"
				} else {
					row.Multiplicity++
				}
				actual[row.identity()], after.Sites[row.identity()] = row, row
				break
			}
		}
		changedProof := proof
		changedProof.Actual = actual
		if debtValidateCollectorIdentity(before.Collector, after.Collector, before, after, "", changedProof) == nil {
			t.Error("foreign native-entry provenance admitted:", mutation)
		}
	}
}

func TestPersistenceAuthorityDebtNativeEntryCatchUpReconcilesActualOrigins(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	cache := debtAnalysisCacheForCheckout(t, root)
	findings := debtLoadPersistenceAuthorityFindings(t, root)
	findings = append(findings, debtSelectedBoundaryFindings(t, root)...)
	actual := authorityDebtSites(findings)
	head := nativeEntryHeadBaseline(t, root)
	for _, input := range [][2]string{{debtCatchUpNativeEntryLane, debtCatchUpFreshnessCollectorTo}, {debtCatchUpNativeEntryInput, debtProcessLeaseFreshnessCollectorTo}} {
		original := cache.baseSites(t, materializeDebtBase(t, root, input[0]), input[0])
		prior, _, err := debtCatchUpSignedBaseline(root, input[0], input[1])
		if err != nil {
			t.Fatal(err)
		}
		failures := authorityDebtRatchet(actual, head.Sites, prior.Sites, original, debtDeliveryRetirementProof{Root: root, Actual: actual})
		if len(failures) != 0 {
			t.Fatalf("actual native-entry source origin %s: %v", input[0], failures)
		}
		t.Logf("actual source %s=%d; common=%d; no unmatched additions or resurrection", input[0], authorityDebtCount(original), authorityDebtCount(actual))
	}
}

func TestPersistenceAuthorityDebtNativeEntryCatchUpRefusesChangedCurrentSource(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	copy := debtCatchUpSourceCopyFrom(t, root)
	var paths []string
	for path := range debtCatchUpNativeEntryNativePins {
		paths = append(paths, path)
	}
	const custody = "internal/store/internal/startupownership"
	entries, err := os.ReadDir(filepath.Join(root, custody))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".go" {
			paths = append(paths, custody+"/"+entry.Name())
		}
	}
	for _, path := range paths {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
	if err := debtCatchUpCollectorSource(copy); err != nil {
		t.Fatal("exact native-entry source copy refused:", err)
	}
	paths = append(paths, debtCatchUpCollectorFiles...)
	for _, path := range paths {
		raw, err := os.ReadFile(filepath.Join(copy, path))
		if err != nil {
			t.Fatal(err)
		}
		mutation := "\nvar unreviewedNativeEntrySource int\n"
		if strings.HasPrefix(path, custody+"/") {
			mutation = "\nfunc (*StartupPostgresOwner) unreviewedNativeEntrySource() {}\n"
		}
		debtWriteModuleSource(t, copy, path, string(raw)+mutation)
		if err := debtCatchUpCollectorSource(copy); err == nil {
			t.Error("changed native-entry collector/owner admitted:", path)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
	debtWriteModuleSource(t, copy, custody+"/unreviewed_native_entry.go", "package startupownership\nfunc (*StartupPostgresOwner) unreviewedNativeEntry() {}\n")
	if err := debtCatchUpCollectorSource(copy); err == nil {
		t.Fatal("new-file native custody method escaped the complete owner pin")
	}
}
