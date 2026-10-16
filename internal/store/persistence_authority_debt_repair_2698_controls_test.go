package store_test

import (
	"bytes"
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
	if actual, err := debtCollectorDigest(root); err != nil || actual != debtRepair2698ProofCostCollectorTo {
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
