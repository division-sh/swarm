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
const debtRepair2698CleanupBefore = "d785d2e881965c4e7374fc71234c0b6084834cfa"
const debtRepair2698CleanupCollectorTo = "30ae07018481378e7a90f6ad34438574b57c49705e9affd2af9b2d7a7aec21eb"
const debtRepair2698Round2Before = "ff690517b59af2456c8c9a30c42ca216fb5ae9c1"
const debtRepair2698Round2CollectorTo = "f5da43ac97f3155bef4d51ac115b6500ffdcd4e8d50cbd3e9f09a20f08aba296"
const debtRepair2698SchedulePath = "internal/serveapp/semantic_numeric_schedule_test.go"
const debtRepair2698ScheduleBefore = "ff1ea5580af1ecf153490f5e71e9f2ea66da3616"
const debtRepair2698ScheduleBeforeSHA = "746c7c357dad92054381626d95617e18d186d61e0a0142c23b2893ba86b08e5f"
const debtRepair2698ScheduleAfterSHA = "8de80abaf8df0ecae4fb4fcb7c7a1d3bdbcfd5b80e157084410562af09552119"
const debtRepair2698ScheduleCollectorTo = "d7f0674dc6481d92aef2af07f8a5eb3a349dddf5ec5c167f872e36a0cb7a3ec1"

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
	if actual, err := debtCollectorDigest(root); err != nil || actual != debtActiveCollectorTo {
		t.Fatalf("native repair collector=%s expected=%s: %v", actual, debtActiveCollectorTo, err)
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
	head.Collector = debtActiveCollectorTo
	proof := debtDeliveryRetirementProof{Root: root, Actual: maps.Clone(head.Sites)}
	if !debtRepair2698ScheduleCollectorTransition(trusted.Collector, head.Collector, trusted, head, proof) {
		t.Fatal("exact forward source continuation refused")
	}
	assertDebtCatchUpForeignProvenance(t, trusted, head, proof)
	t.Run("round2-forward-only", func(t *testing.T) {
		original, _, err := debtCatchUpSignedBaseline(root, debtRepair2698Round2Before, debtRepair2698CleanupCollectorTo)
		if err != nil {
			t.Fatal(err)
		}
		archive := debtCatchUpHistoricalArchive(t, debtRepair2698Round2Before)
		if actual, err := debtCollectorDigest(archive); err != nil || actual != debtRepair2698CleanupCollectorTo {
			t.Fatalf("round-2 retargeted its original collector: %s: %v", actual, err)
		}
		current := original
		current.Collector = debtRepair2698Round2CollectorTo
		proof := debtDeliveryRetirementProof{Root: root, Actual: maps.Clone(current.Sites)}
		if !debtRepair2698Round2CollectorTransition(original.Collector, current.Collector, original, current, proof) ||
			debtRepair2698Round2CollectorTransition(current.Collector, original.Collector, current, original, proof) ||
			debtRepair2698CleanupCollectorTransition(original.Collector, current.Collector, original, current, proof) {
			t.Fatal("round-2 edge refused forward or accepted reverse/retargeted provenance")
		}
		assertDebtCatchUpForeignProvenance(t, original, current, proof)
	})
	t.Run("schedule-forward-only", func(t *testing.T) {
		original, _, err := debtCatchUpSignedBaseline(root, debtRepair2698ScheduleBefore, debtRepair2698Round2CollectorTo)
		if err != nil {
			t.Fatal(err)
		}
		current := original
		current.Collector = debtRepair2698ScheduleCollectorTo
		proof := debtDeliveryRetirementProof{Root: root, Actual: maps.Clone(current.Sites)}
		if !debtRepair2698ScheduleCollectorTransition(original.Collector, current.Collector, original, current, proof) ||
			debtRepair2698ScheduleCollectorTransition(current.Collector, original.Collector, current, original, proof) ||
			debtRepair2698Round2CollectorTransition(original.Collector, current.Collector, original, current, proof) {
			t.Fatal("schedule edge refused forward or accepted reverse/retargeted provenance")
		}
		assertDebtCatchUpForeignProvenance(t, original, current, proof)
	})
	t.Run("cleanup-forward-only", func(t *testing.T) {
		original, _, err := debtCatchUpSignedBaseline(root, debtRepair2698CleanupBefore, debtRepair2698ProofCostCollectorTo)
		if err != nil {
			t.Fatal(err)
		}
		archive := debtCatchUpHistoricalArchive(t, debtRepair2698CleanupBefore)
		if actual, err := debtCollectorDigest(archive); err != nil || actual != debtRepair2698ProofCostCollectorTo {
			t.Fatalf("cleanup retargeted the old fingerprint: %s: %v", actual, err)
		}
		current := original
		current.Collector = debtRepair2698CleanupCollectorTo
		proof := debtDeliveryRetirementProof{Root: root, Actual: maps.Clone(current.Sites)}
		if !debtRepair2698CleanupCollectorTransition(original.Collector, current.Collector, original, current, proof) ||
			debtRepair2698CleanupCollectorTransition(current.Collector, original.Collector, current, original, proof) ||
			debtRepair2698ProofCostCollectorTransition(original.Collector, current.Collector, original, current, proof) {
			t.Fatal("cleanup edge refused forward or accepted reverse/retargeted provenance")
		}
		assertDebtCatchUpForeignProvenance(t, original, current, proof)
	})
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

func assertRepair2698ScheduleActualMutants(t *testing.T, root string) {
	t.Helper()
	path := filepath.Join(root, debtRepair2698SchedulePath)
	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.WriteFile(path, current, 0600); err != nil {
			t.Fatal(err)
		}
	}()
	before, err := debtRepair2698HistoricalSource(root, debtRepair2698SchedulePath, current)
	if err != nil || debtBaselineChecksum(before) != debtRepair2698ScheduleBeforeSHA {
		t.Fatal("exact schedule reader source did not restore its predecessor:", err)
	}
	for _, hostile := range [][]byte{before, append(bytes.Clone(current), '\n'), append(bytes.Clone(current), []byte("\nfunc extraScheduleRawCaller(db interface{ Exec(string, ...any) }) { db.Exec(\"DELETE FROM events\") }\n")...)} {
		got, err := debtRepair2698HistoricalSource(root, debtRepair2698SchedulePath, hostile)
		if err == nil && !bytes.Equal(got, hostile) {
			t.Fatal("stale/changed/extra-call schedule source acquired preservation credit")
		}
		if err := os.WriteFile(path, hostile, 0600); err != nil {
			t.Fatal(err)
		}
		if debtCatchUpCollectorSource(root) == nil {
			t.Fatal("warm copied source accepted a stale, changed or added raw caller endpoint")
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := debtRepair2698HistoricalSource(root, debtRepair2698SchedulePath, current); err == nil {
		t.Fatal("pristine gitdir hid a missing copied schedule reader fixture")
	}
}

func TestPersistenceAuthorityDebtRepair2698CopiedWarmSourceRefusesEscapes(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	copy := debtCatchUpSourceCopyFrom(t, root)
	copyRepair2698CustodySources(t, root, copy)
	assertRepair2698CleanupActualMutants(t, copy)
	assertRepair2698ScheduleActualMutants(t, copy)
	packet, err := debtRepair2698ReadPacket(copy)
	if err != nil {
		t.Fatal(err)
	}
	if err := debtCatchUpCollectorSource(copy); err != nil {
		t.Fatal("exact copied source refused:", err)
	}
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
