package store_test

import (
	"bytes"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// #2542/6211981991: one forward source-policy continuation, no new debt rights.
const debtRepair2698Before = "e857a1e482a2e08ab63f2e8169b4472e608cfdc2"
const debtRepair2698After = "f5c57acafdae2cd0750f493c15ca667ca3d84e33"
const debtRepair2698SourceSHA = "d5deace20725d4bafb1a8a294634618daf9312a5b91eff0d7a14e8c1ce6d39e6"
const debtRepair2698SourcePath = "tools/fixture-codemod/pipeline-observations/internal/selectedreadrole/testdata/repair_2698_sources.json"
const debtRepair2698FixtureNamePacketPath = "tools/fixture-codemod/pipeline-observations/internal/selectedreadrole/testdata/repair_2698_fixture_name_successor.json"
const debtRepair2698FixtureNameSource = "internal/store/internal/runtimepersistence/test_event_support.go"
const debtRepair2698FixtureNameAfter = "6a6c51b3924ff248e5a6b4c382f4631d3ff734c8"
const debtRepair2698FixtureNameSHA = "910a0a6e02aba0d8f3245454c0505ab237c6ee0da11f18a0f78b8a305a0b6976"
const debtRepair2698CollectorTo = "502836943c1bdeacad123044ecebba5bacb85970942ab67297a4fb4719b202e2"
const debtRepair2698ProofCostBefore = "02886cacfda701c53551d2a06cca05955eaa1b5b"
const debtRepair2698ProofCostCollectorTo = "7898e5f737ab7f1ab9d69439eb484f57c95c3c251737a06a4c21e64dbcc24580"
const debtRepair2698CleanupPacketPath = "tools/fixture-codemod/pipeline-observations/internal/selectedreadrole/testdata/repair_2698_cleanup_successor.json"
const debtRepair2698CleanupSource = "internal/serveapp/main_runtime_test.go"
const debtRepair2698CleanupAfter = "6d6c3eafaea02d3e0fef5e3183870174436adbc1"
const debtRepair2698CleanupSHA = "976911841d61b4088ff55ce29b0e499b5629b1e3b764b6c91169538493debd05"
const debtR04HistoricalCollectorTo = "b37781f671f13fbf2995ddcb94b747b8fdd89cdf1c28ef5f06920ad5091a3e8b"
const debtR04ExecutionPath = "internal/runtime/runforkexecution/execution_test.go"
const debtR04ExecutionBeforeSHA = "a83a5fcfa8e784937c21f1a1ad079714368aacff35c39c32b5d81877bc87c3da"
const debtR04ExecutionAfterSHA = "42e79a42a17acb12ec3362a28457c08dc4d66509e0e14f6d9bb84ac68f52076a"
const debtR04JoinedCollectorTo = "252bf4c09f8ede5dfdafbb110e0172c3404cc3932bff8d34527480dcc7790853"

type debtR04CatchUpSource struct {
	File, BeforeSHA256, AfterSHA256 string
	Changed, Added                  []string
}

// #2700/6246811067 and 6246812192: exact historical comparisons, not
// semantic-equivalence claims or permission to normalize live census input.
var debtR04CatchUpSources = []debtR04CatchUpSource{
	{
		File:         "internal/runtime/pipeline/workflow_gate_recovery_external_test.go",
		BeforeSHA256: "c310bc5755d986c8599397e68e565eacbd04842daf51c693783e0b61d344f129",
		AfterSHA256:  "5629c958a271e430c18a16935bc5911e25acc5bbabe2e005a1fe2a6310cace7c",
		Changed: []string{
			"func:TestApprovedActivityHoldsThenDispatchesExactFrozenInputOnBothStores",
			"func:TestProposedEffectCompletedRouteReplaysBeforeBundleFenceAndPreservesReplyContextOnBothStores",
			"func:proposedEffectProofBundle",
		},
		Added: []string{
			"func:TestAuthoredHTTPApprovalKeepsFrozenInputAndTerminalOutcomeBothStores",
			"func:proposedEffectProofBundleForMethod",
			"func:testApprovedActivityFrozenInputBothStores",
		},
	},
	{
		File:         "internal/runtime/runforkexecution/activity_fork_execution_test.go",
		BeforeSHA256: "51dfa6c9a4f45a7e26ff976c03ad012de52e5f264348385755c9f90e459cffce",
		AfterSHA256:  "012628116c3737b8095e1a14a8411714428b145c2f8872e8cab1e14a3c49cb65",
		Changed: []string{
			"func:(activityForkFixture).seedSource",
			"func:runSelectedContractActivityForkCases",
			"func:seedSelectedContractActivityAttempt",
			"func:selectedContractActivityDeclaredSource",
			"func:selectedContractActivitySourceWithMode",
			"type:selectedContractActivityForkCase",
			"type:selectedContractActivityRequestPayload",
		},
	},
	{
		File:         "internal/serveapp/semantic_numeric_activity_test.go",
		BeforeSHA256: "62e880a705e69375f38af07557cb016d7d7339b3f78bca9159571063e26773a0",
		AfterSHA256:  "866b9b4d007152e9576bd0fbc4b3d7d3241b5d60a27876b303b0b1d072ee0f02",
		Changed:      []string{"func:proveActivitySemanticResultExecutionParity"},
	},
}

func TestPersistenceAuthorityDebtRepair2698DeclarationCacheUsesActualBytes(t *testing.T) {
	root := t.TempDir()
	const path = "current.go"
	const original = "package proof\nfunc Original(){ first() }\n"
	debtWriteModuleSource(t, root, path, original)
	file := filepath.Join(root, path)
	first, err := debtServedDebugDeclarations(file)
	if err != nil || first["Original"] == "" {
		t.Fatal("exact source failed:", err)
	}
	first["Original"] = "foreign caller mutation"
	again, err := debtServedDebugDeclarations(file)
	if err != nil || again["Original"] == first["Original"] {
		t.Fatal("caller mutated a warmed declaration:", err)
	}
	changed := strings.Replace(original, "first", "other", 1)
	debtWriteModuleSource(t, root, path, changed)
	actual, err := debtServedDebugDeclarations(file)
	if err != nil || actual["Original"] == again["Original"] {
		t.Fatal("same-sized changed source reused the prior body:", err)
	}
	for _, hostile := range []string{original + "func Original(){}", "package proof\nfunc {"} {
		debtWriteModuleSource(t, root, path, hostile)
		if _, err := debtServedDebugDeclarations(file); err == nil {
			t.Fatal("duplicate/malformed actual source admitted by warm cache")
		}
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if _, err := debtServedDebugDeclarations(file); err == nil {
		t.Fatal("missing actual source admitted by warm cache")
	}
}

func TestPersistenceAuthorityDebtRepair2698ExactSourceAndForwardPolicy(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	if actual, err := debtCollectorDigest(root); err != nil || actual != debtRepair2698ProofCostCollectorTo && actual != debtR04CollectorTo && actual != debtR04HistoricalCollectorTo && actual != debtR04JoinedCollectorTo {
		t.Fatalf("native repair collector=%s expected=%s: %v", actual, debtRepair2698ProofCostCollectorTo, err)
	}
	if err := debtCatchUpCollectorSource(root); err != nil {
		t.Fatal(err)
	}
	trusted, _, err := debtCatchUpSignedBaseline(root, debtRepair2698Before, debtCatchUpNativeEntryCollectorTo)
	if err != nil {
		t.Fatal(err)
	}
	head, _, err := debtCatchUpSignedBaseline(root, debtRepair2698ProofCostBefore, debtRepair2698CollectorTo)
	if err != nil {
		t.Fatal(err)
	}
	head.Collector = debtRepair2698ProofCostCollectorTo
	proof := debtDeliveryRetirementProof{Root: root, Actual: maps.Clone(head.Sites)}
	if !debtRepair2698ProofCostCollectorTransition(trusted.Collector, head.Collector, trusted, head, proof) {
		t.Fatal("exact forward source continuation refused")
	}
	assertDebtCatchUpForeignProvenance(t, trusted, head, proof)
	proveR04CollectorContinuation(t, root)
}

func proveR04CollectorContinuation(t *testing.T, root string) {
	t.Helper()
	actual, err := debtCollectorDigest(root)
	if err != nil || actual != debtR04JoinedCollectorTo {
		t.Fatalf("R04 continuation must use its executing collector: %s: %v", actual, err)
	}
	before, _, err := debtCatchUpSignedBaseline(root, debtR04After, debtRepair2698ProofCostCollectorTo)
	if err != nil {
		t.Fatal(err)
	}
	after := before
	after.Collector = actual
	proof := debtDeliveryRetirementProof{Root: root, Actual: maps.Clone(after.Sites)}
	if !debtR04JoinedCollectorTransition(before.Collector, after.Collector, before, after, proof) {
		t.Fatal("complete authenticated R04 continuation refused")
	}
	if debtR04JoinedCollectorTransition(after.Collector, before.Collector, after, before, proof) ||
		debtRepair2698ProofCostCollectorTransition(before.Collector, after.Collector, before, after, proof) {
		t.Fatal("R04 reverse edge or retargeted predecessor accepted")
	}
	staged := before
	staged.Collector = debtR04CollectorTo
	if !debtR04JoinedCollectorTransition(staged.Collector, after.Collector, staged, after, proof) ||
		debtR04JoinedCollectorTransition(after.Collector, staged.Collector, after, staged, proof) ||
		debtR04CollectorTransition(before.Collector, after.Collector, before, after, proof) {
		t.Fatal("historical continuation retargeted its staged edge or accepted reverse provenance")
	}
	historical := before
	historical.Collector = debtR04HistoricalCollectorTo
	if !debtR04JoinedCollectorTransition(historical.Collector, after.Collector, historical, after, proof) ||
		debtR04JoinedCollectorTransition(after.Collector, historical.Collector, after, historical, proof) ||
		debtR04HistoricalCollectorTransition(before.Collector, after.Collector, before, after, proof) {
		t.Fatal("joined continuation retargeted its historical edge or accepted reverse provenance")
	}
	assertDebtCatchUpForeignProvenance(t, before, after, proof)
	proveR04SourceContinuation(t, root)
	proveR04HistoricalSourceContinuation(t, root)
	proveR04JoinedSourceContinuation(t, root)
}

func proveR04HistoricalSourceContinuation(t *testing.T, root string) {
	t.Helper()
	before, after, err := debtR04ExecutionImages(root)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := debtR04HistoricalSource(root, debtR04ExecutionPath, after)
	if err != nil || !bytes.Equal(previous, before) {
		t.Fatal("execution source did not reconstruct the exact reviewed predecessor:", err)
	}
	once, err := debtR04HistoricalSource(root, debtR04ExecutionPath, previous)
	if err != nil || !bytes.Equal(once, previous) {
		t.Fatal("execution source inverse applied twice:", err)
	}
	for _, pair := range [][2][]byte{{after, before}, {append(bytes.Clone(before), '\n'), after}, {before, append(bytes.Clone(after), '\n')}} {
		if _, err := debtR04ExecutionInverse(pair[0], pair[1]); err == nil {
			t.Fatal("execution source accepted reversed or unrelated complete images")
		}
	}
	foreign, err := debtR04HistoricalSource(root, debtR04ExecutionPath+".foreign", after)
	if err != nil || !bytes.Equal(foreign, after) {
		t.Fatal("execution source acquired an inverse at a foreign path:", err)
	}
	packet, err := debtRepair2698ReadCleanupPacket(root)
	if err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(filepath.Join(root, debtRepair2698CleanupSource))
	if err != nil {
		t.Fatal(err)
	}
	previous, err = debtRepair2698CleanupHistoricalSource(root, debtRepair2698CleanupSource, current)
	if err != nil || debtBaselineChecksum(previous) != packet.Files[0].BeforeSHA256 {
		t.Fatal("cleanup source did not reconstruct its recorded predecessor:", err)
	}
	once, err = debtRepair2698CleanupHistoricalSource(root, debtRepair2698CleanupSource, previous)
	if err != nil || !bytes.Equal(once, previous) {
		t.Fatal("cleanup source inverse applied twice:", err)
	}
}

func proveR04JoinedSourceContinuation(t *testing.T, root string) {
	t.Helper()
	declarations := 0
	for _, row := range debtR04CatchUpSources {
		before, after, err := debtR04CatchUpImages(root, row)
		if err != nil {
			t.Fatal(err)
		}
		declarations += len(row.Changed) + len(row.Added)
		previous, err := debtR04CatchUpHistoricalSource(root, row.File, after)
		if err != nil || !bytes.Equal(previous, before) {
			t.Fatal("joined source did not reconstruct its signed predecessor:", row.File, err)
		}
		once, err := debtR04CatchUpHistoricalSource(root, row.File, previous)
		if err != nil || !bytes.Equal(once, previous) {
			t.Fatal("joined source inverse consumed its predecessor twice:", row.File, err)
		}
		foreign, err := debtR04CatchUpHistoricalSource(root, row.File+".foreign", after)
		if err != nil || !bytes.Equal(foreign, after) {
			t.Fatal("foreign joined source acquired an inverse:", err)
		}
		proveR04JoinedInverseMutants(t, row, before, after)
	}
	if declarations != 14 {
		t.Fatalf("joined source declaration inventory = %d, want 14", declarations)
	}
	packet, err := debtCatchUpReadPacket(root)
	if err != nil || len(packet.Files) != 229 {
		t.Fatal("joined source lost its complete 229-member historical frame:", err)
	}
	if err := debtRepair2698SourcePolicy(root, packet); err != nil {
		t.Fatal("joined source lost its complete 41-member historical frame:", err)
	}
	if err := debtCatchUpCollectorSource(root); err != nil {
		t.Fatal("joined source failed complete source comparison:", err)
	}
}

func proveR04JoinedInverseMutants(t *testing.T, row debtR04CatchUpSource, before, after []byte) {
	t.Helper()
	for _, pair := range [][2][]byte{{after, before}, {append(bytes.Clone(before), '\n'), after}, {before, append(bytes.Clone(after), '\n')}} {
		if _, err := debtR04CatchUpInverse(row, pair[0], pair[1]); err == nil {
			t.Fatal("joined source accepted reversed or changed complete images:", row.File)
		}
	}
	foreign, missing, duplicate, supplied := row, row, row, row
	foreign.File += ".foreign"
	missing.Changed = append([]string(nil), row.Changed[1:]...)
	duplicate.Changed = append(append([]string(nil), row.Changed...), row.Changed[0])
	supplied.AfterSHA256 = debtBaselineChecksum(before)
	for _, mutant := range []debtR04CatchUpSource{foreign, missing, duplicate, supplied} {
		if _, err := debtR04CatchUpInverse(mutant, before, after); err == nil {
			t.Fatal("joined inverse accepted foreign/missing/duplicate/supplied inventory:", row.File)
		}
	}
}

func proveR04SourceContinuation(t *testing.T, root string) {
	t.Helper()
	for _, mutation := range []string{"foreign-path", "missing-path", "duplicate-path", "stale-source", "reverse-source"} {
		pins := append([]debtRepair2698SourceFile(nil), debtR04SourcePins[:]...)
		switch mutation {
		case "foreign-path":
			pins[0].File += ".foreign"
		case "missing-path":
			pins = pins[:2]
		case "duplicate-path":
			pins[0] = pins[1]
		case "stale-source":
			pins[0].AfterSHA256 = pins[0].BeforeSHA256
		case "reverse-source":
			pins[0].BeforeSHA256, pins[0].AfterSHA256 = pins[0].AfterSHA256, pins[0].BeforeSHA256
		}
		if _, _, err := debtR04SourceImagesFor(root, pins); err == nil {
			t.Error("R04 source certificate admitted", mutation)
		}
	}
	raw, err := os.ReadFile(filepath.Join(root, debtR04CompositionPath))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := debtR04CompositionPredecessor(root, raw)
	if err != nil || debtBaselineChecksum(previous) != debtCatchUpNativeEntryNativePins[debtR04CompositionPath] {
		t.Fatal("R04 assembly did not reconstruct the exact predecessor:", err)
	}
	once, err := debtR04CompositionPredecessor(root, previous)
	if err != nil || !bytes.Equal(once, previous) {
		t.Fatal("R04 assembly inverse consumed its predecessor twice:", err)
	}
	if _, err := debtR04CompositionPredecessor(root, append(bytes.Clone(raw), '\n')); err == nil {
		t.Fatal("R04 inverse accepted supplied bytes instead of the actual current image")
	}
}

func TestPersistenceAuthorityDebtRepair2698ProofCostForwardEdgeKeepsOriginalFingerprintAndRefusesReverse(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	archive := debtCatchUpHistoricalArchive(t, debtRepair2698ProofCostBefore)
	if actual, err := debtCollectorDigest(archive); err != nil || actual != debtRepair2698CollectorTo {
		t.Fatalf("first repair source was retargeted: %s: %v", actual, err)
	}
	trusted, _, err := debtCatchUpSignedBaseline(root, debtRepair2698ProofCostBefore, debtRepair2698CollectorTo)
	if err != nil {
		t.Fatal(err)
	}
	head := trusted
	head.Collector = debtRepair2698ProofCostCollectorTo
	proof := debtDeliveryRetirementProof{Root: root, Actual: maps.Clone(head.Sites)}
	if !debtRepair2698ProofCostCollectorTransition(trusted.Collector, head.Collector, trusted, head, proof) {
		t.Fatal("exact shrink-only proof-cost successor refused")
	}
	if debtRepair2698ProofCostCollectorTransition(head.Collector, trusted.Collector, head, trusted, proof) ||
		debtRepair2698CollectorTransition(trusted.Collector, head.Collector, trusted, head, proof) {
		t.Fatal("reverse edge or retargeting the original transition was accepted")
	}
	assertDebtCatchUpForeignProvenance(t, trusted, head, proof)
}

func TestPersistenceAuthorityDebtRepair2698ReconcilesLiveCensus(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	findings := debtLoadPersistenceAuthorityFindings(t, root)
	findings = append(findings, debtSelectedBoundaryFindings(t, root)...)
	actual := authorityDebtSites(findings)
	raw, err := os.ReadFile(filepath.Join(root, debtBaselinePath))
	if err != nil {
		t.Fatal(err)
	}
	head, err := parseAuthorityDebtBaseline(raw)
	if err != nil {
		t.Fatal(err)
	}
	original, _, err := debtCatchUpSignedBaseline(root, debtRepair2698Before, debtCatchUpNativeEntryCollectorTo)
	if err != nil {
		t.Fatal(err)
	}
	proof := debtDeliveryRetirementProof{Root: root, Actual: actual}
	if failures := authorityDebtRatchet(actual, head.Sites, original.Sites, original.Sites, proof); len(failures) != 0 {
		t.Fatal("native repair added/resurrected debt against its exact predecessor:", failures)
	}
	if err := debtCatchUpNativeEntryPredecessors(root, actual, head.Sites); err != nil {
		t.Fatal(err)
	}
}

func TestPersistenceAuthorityDebtRepair2698CopiedWarmSourceRefusesEscapes(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	copy := debtCatchUpSourceCopyFrom(t, root)
	copyRepair2698CustodySources(t, root, copy)
	packet, err := debtRepair2698ReadPacket(copy)
	if err != nil {
		t.Fatal(err)
	}
	if err := debtCatchUpCollectorSource(copy); err != nil {
		t.Fatal("exact copied source refused:", err)
	}
	assertRepair2698CleanupActualMutants(t, copy)
	assertR04ExecutionActualMutants(t, copy)
	assertR04JoinedActualMutants(t, copy)
	for _, file := range packet.Files {
		actual, err := os.ReadFile(filepath.Join(copy, file.File))
		if err != nil {
			t.Fatal(err)
		}
		previous, err := debtRepair2698HistoricalSource(copy, file.File, actual)
		if err != nil || debtBaselineChecksum(previous) != file.BeforeSHA256 {
			t.Fatal("exact current source did not retain its predecessor:", file.File, err)
		}
		once, err := debtRepair2698HistoricalSource(copy, file.File, previous)
		if err != nil || !bytes.Equal(once, previous) {
			t.Fatal("inverse consumed its predecessor twice:", err)
		}
		foreign, err := debtRepair2698HistoricalSource(copy, file.File+".foreign", actual)
		if err != nil || !bytes.Equal(foreign, actual) {
			t.Fatal("foreign path acquired an inverse:", err)
		}
	}
	file := packet.Files[0]
	actual, err := os.ReadFile(filepath.Join(copy, file.File))
	if err != nil {
		t.Fatal(err)
	}
	mutant := append(bytes.Clone(actual), []byte("\nfunc unreviewedRawCaller(db interface{ Exec(string, ...any) }) { db.Exec(\"DELETE FROM events\") }\n")...)
	debtWriteModuleSource(t, copy, file.File, string(mutant))
	if err := debtCatchUpCollectorSource(copy); err == nil {
		t.Fatal("extra raw caller disappeared through the warmed historical view")
	}
	previous, err := debtRepair2698HistoricalSource(copy, file.File, mutant)
	if err != nil || !bytes.Equal(previous, mutant) {
		t.Fatal("actual added caller was replaced by historical source:", err)
	}
	debtWriteModuleSource(t, copy, file.File, string(actual))
	if err := os.Remove(filepath.Join(copy, file.File)); err != nil {
		t.Fatal(err)
	}
	if err := debtCatchUpCollectorSource(copy); err == nil {
		t.Fatal("missing copied current file admitted after warming")
	}
	debtWriteModuleSource(t, copy, file.File, string(actual))
	manifest, err := os.ReadFile(filepath.Join(copy, debtRepair2698SourcePath))
	if err != nil {
		t.Fatal(err)
	}
	for _, replacement := range [][]byte{
		append(bytes.Clone(manifest), '\n'),
		bytes.Replace(manifest, []byte(file.File), []byte(file.File+".foreign"), 1),
		bytes.Replace(manifest, []byte(debtRepair2698Before), []byte(debtRepair2698After), 1),
		bytes.Replace(manifest, []byte(packet.Files[1].File), []byte(file.File), 1),
	} {
		debtWriteModuleSource(t, copy, debtRepair2698SourcePath, string(replacement))
		if err := debtCatchUpCollectorSource(copy); err == nil {
			t.Fatal("warm cache admitted changed/foreign/reverse/duplicate certificate")
		}
	}
	if err := os.Remove(filepath.Join(copy, debtRepair2698SourcePath)); err != nil {
		t.Fatal(err)
	}
	if err := debtCatchUpCollectorSource(copy); err == nil {
		t.Fatal("missing certificate admitted")
	}
}

func assertRepair2698CleanupActualMutants(t *testing.T, root string) {
	t.Helper()
	packet, err := debtRepair2698ReadCleanupPacket(root)
	if err != nil {
		t.Fatal(err)
	}
	file := packet.Files[0]
	current, err := os.ReadFile(filepath.Join(root, file.File))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := debtRepair2698CleanupHistoricalSource(root, file.File, current)
	if err != nil || debtBaselineChecksum(previous) != file.BeforeSHA256 {
		t.Fatal("exact cleanup predecessor refused:", err)
	}
	for _, changed := range [][]byte{
		previous,
		append(bytes.Clone(current), '\n'),
		bytes.Replace(current, []byte("} else if code != 0 {"), []byte("} else if code == 0 {"), 1),
		append(bytes.Clone(current), []byte("\nfunc extraRawCaller(db interface{ Exec(string, ...any) }) { db.Exec(\"DELETE FROM events\") }\n")...),
	} {
		debtWriteModuleSource(t, root, file.File, string(changed))
		if err := debtCatchUpCollectorSource(root); err == nil {
			t.Fatal("warm cleanup continuation accepted stale/changed/raw-caller source")
		}
		got, err := debtRepair2698HistoricalSource(root, file.File, changed)
		if err == nil && !bytes.Equal(got, changed) {
			t.Fatal("unreviewed cleanup source acquired an inverse")
		}
	}
	debtWriteModuleSource(t, root, file.File, string(current))
	manifest, err := os.ReadFile(filepath.Join(root, debtRepair2698CleanupPacketPath))
	if err != nil {
		t.Fatal(err)
	}
	duplicate := packet
	duplicate.Files = append(append([]debtRepair2698SourceFile(nil), packet.Files...), file)
	duplicateBytes, err := json.Marshal(duplicate)
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range [][]byte{
		append(bytes.Clone(manifest), '\n'),
		bytes.Replace(manifest, []byte(file.File), []byte(file.File+".foreign"), 1),
		bytes.Replace(manifest, []byte(packet.AfterCommit), []byte(packet.BeforeCommit), 1),
		duplicateBytes,
	} {
		debtWriteModuleSource(t, root, debtRepair2698CleanupPacketPath, string(changed))
		if err := debtCatchUpCollectorSource(root); err == nil {
			t.Fatal("warm cleanup continuation accepted changed/foreign/reverse/duplicate packet")
		}
	}
	if err := os.Remove(filepath.Join(root, debtRepair2698CleanupPacketPath)); err != nil {
		t.Fatal(err)
	}
	if err := debtCatchUpCollectorSource(root); err == nil {
		t.Fatal("missing cleanup continuation admitted")
	}
	debtWriteModuleSource(t, root, debtRepair2698CleanupPacketPath, string(manifest))
	if err := os.Remove(filepath.Join(root, file.File)); err != nil {
		t.Fatal(err)
	}
	if _, err := debtRepair2698CleanupHistoricalSource(root, file.File, current); err == nil {
		t.Fatal("pristine gitdir concealed missing copied cleanup source")
	}
	debtWriteModuleSource(t, root, file.File, string(current))
}

func assertR04ExecutionActualMutants(t *testing.T, root string) {
	t.Helper()
	before, current, err := debtR04ExecutionImages(root)
	if err != nil {
		t.Fatal(err)
	}
	positive, err := debtR04ExecutionFunction(current, "TestSelectedContractForkAuthoredHTTPToolPersistsCapabilityAndRejectsHostileAdmission")
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range [][]byte{
		before,
		append(bytes.Clone(current), '\n'),
		append(bytes.Clone(current), positive...),
		append(bytes.Clone(current), []byte("\nfunc extraRawCaller(db interface{ Exec(string, ...any) }) { db.Exec(\"DELETE FROM events\") }\n")...),
	} {
		debtWriteModuleSource(t, root, debtR04ExecutionPath, string(changed))
		if err := debtCatchUpCollectorSource(root); err == nil {
			t.Fatal("warm execution continuation accepted stale/duplicate/unrelated/raw-caller source")
		}
		got, err := debtR04HistoricalSource(root, debtR04ExecutionPath, changed)
		if err == nil && !bytes.Equal(got, changed) {
			t.Fatal("unreviewed execution source acquired an inverse")
		}
		if _, err := debtR04HistoricalSource(root, debtR04ExecutionPath, current); err == nil {
			t.Fatal("supplied approved bytes concealed changed live execution source")
		}
	}
	if err := os.Remove(filepath.Join(root, debtR04ExecutionPath)); err != nil {
		t.Fatal(err)
	}
	if _, err := debtR04HistoricalSource(root, debtR04ExecutionPath, current); err == nil {
		t.Fatal("pristine gitdir concealed missing copied execution source")
	}
	debtWriteModuleSource(t, root, debtR04ExecutionPath, string(current))
	if err := debtCatchUpCollectorSource(root); err != nil {
		t.Fatal("exact execution source failed after hostile restoration:", err)
	}
}

func assertR04JoinedActualMutants(t *testing.T, root string) {
	t.Helper()
	for _, row := range debtR04CatchUpSources {
		before, current, err := debtR04CatchUpImages(root, row)
		if err != nil {
			t.Fatal(err)
		}
		declarations, err := debtR04CatchUpDeclarations(current)
		if err != nil {
			t.Fatal(err)
		}
		for _, changed := range [][]byte{
			before,
			append(bytes.Clone(current), '\n'),
			append(bytes.Clone(current), declarations[row.Changed[0]]...),
			append(bytes.Clone(current), []byte("\nfunc extraRawCaller(db interface{ Exec(string, ...any) }) { db.Exec(\"DELETE FROM events\") }\n")...),
		} {
			debtWriteModuleSource(t, root, row.File, string(changed))
			if err := debtCatchUpCollectorSource(root); err == nil {
				t.Fatal("warm joined continuation accepted changed/raw/duplicate/stale source:", row.File)
			}
			if _, err := debtR04CatchUpHistoricalSource(root, row.File, current); err == nil {
				t.Fatal("supplied approved bytes concealed changed live joined source:", row.File)
			}
		}
		if err := os.Remove(filepath.Join(root, row.File)); err != nil {
			t.Fatal(err)
		}
		if _, err := debtR04CatchUpHistoricalSource(root, row.File, current); err == nil {
			t.Fatal("pristine gitdir concealed missing copied joined source:", row.File)
		}
		debtWriteModuleSource(t, root, row.File, string(current))
	}
	if err := debtCatchUpCollectorSource(root); err != nil {
		t.Fatal("exact joined source failed after hostile restoration:", err)
	}
}

func copyRepair2698CustodySources(t *testing.T, root, destination string) {
	t.Helper()
	const owner = "internal/store/internal/startupownership"
	entries, err := os.ReadDir(filepath.Join(root, owner))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" {
			continue
		}
		path := owner + "/" + entry.Name()
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, destination, path, string(raw))
	}
	copyR04CompositionSource(t, root, destination)
}

func copyR04CompositionSource(t *testing.T, root, destination string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, debtR04CompositionPath))
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, destination, debtR04CompositionPath, string(raw))
	gitdir, err := debtGit(root, "rev-parse", "--absolute-git-dir")
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, destination, ".git", "gitdir: "+strings.TrimSpace(string(gitdir))+"\n")
}

func TestPersistenceAuthorityDebtRepair2698FixtureNameCopiedWarmBoundary(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	copy := debtCatchUpSourceCopyFrom(t, root)
	copyRepair2698CustodySources(t, root, copy)
	packet, err := debtRepair2698ReadFixtureNamePacket(copy)
	if err != nil {
		t.Fatal(err)
	}
	if err := debtCatchUpCollectorSource(copy); err != nil {
		t.Fatal("exact copied endpoint refused:", err)
	}
	for _, file := range []debtRepair2698SourceFile{packet.Files[0], packet.Guard} {
		t.Run(filepath.Base(file.File), func(t *testing.T) {
			assertRepair2698FixtureNameActualMutants(t, copy, packet, file)
		})
	}
	manifest, err := os.ReadFile(filepath.Join(copy, debtRepair2698FixtureNamePacketPath))
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range [][]byte{
		append(bytes.Clone(manifest), '\n'),
		bytes.Replace(manifest, []byte(packet.Files[0].File), []byte(packet.Guard.File), 1),
		bytes.Replace(manifest, []byte(packet.AfterCommit), []byte(packet.BeforeCommit), 1),
	} {
		debtWriteModuleSource(t, copy, debtRepair2698FixtureNamePacketPath, string(changed))
		if err := debtCatchUpCollectorSource(copy); err == nil {
			t.Fatal("warm cache accepted changed/duplicate/reversed rename certificate")
		}
	}
	if err := os.Remove(filepath.Join(copy, debtRepair2698FixtureNamePacketPath)); err != nil {
		t.Fatal(err)
	}
	if err := debtCatchUpCollectorSource(copy); err == nil {
		t.Fatal("missing rename certificate accepted")
	}
}

func assertRepair2698FixtureNameActualMutants(t *testing.T, root string, packet debtRepair2698FixtureNamePacket, file debtRepair2698SourceFile) {
	t.Helper()
	current, err := os.ReadFile(filepath.Join(root, file.File))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := debtRepair2698SignedSource(root, packet.BeforeCommit, file.File, file.BeforeSHA256)
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range [][]byte{
		previous,
		append(bytes.Clone(current), '\n'),
		bytes.Replace(current, []byte("validateUnretainedCleanupHistoryInputsForTest"), []byte("foreignValidator"), 1),
		append(bytes.Clone(current), []byte("\nfunc extraRawCaller(db interface{ Exec(string, ...any) }) { db.Exec(\"DELETE FROM events\") }\n")...),
	} {
		debtWriteModuleSource(t, root, file.File, string(changed))
		if err := debtCatchUpCollectorSource(root); err == nil {
			t.Fatal("warm source accepted stale/body/extra-call mutation")
		}
	}
	if err := os.Remove(filepath.Join(root, file.File)); err != nil {
		t.Fatal(err)
	}
	if err := debtCatchUpCollectorSource(root); err == nil {
		t.Fatal("warm cache accepted missing actual source")
	}
	debtWriteModuleSource(t, root, file.File, string(current))
}
