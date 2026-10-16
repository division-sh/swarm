package store_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// Non-self-hashed exact inputs; neither historical collector identity changes.
const debtCatchUpFreshnessLane = "7224af50400a3b06c90ecaa9987370ae06beaf76"
const debtCatchUpFreshnessInput = "a2621ebde98d9a43d9783cbfd078b63ccc3ab14a"
const debtCatchUpFreshnessCollectorTo = "d3a09746e6d563a3f3f2665bbad37eb26f6c7d79d71e0fd88b73e496d737614f"

var debtCatchUpFreshnessNativePins = map[string]string{
	"internal/store/internal/backend/sqlite/transaction.go":                    "3fab408af2e12f7fa8a8dc252834535349359ab9e496d3df06a1a06802126204",
	"internal/store/internal/backend/sqlite/test_agent_snapshot_corruption.go": "9c47a40cc1093c0fa7f6e19de539b93264a2cc8bd9ed8865e8d0bc3668236ccc",
}

var debtCatchUpFreshnessDeclarationPins = map[string]string{
	"func:debtValidateCollectorIdentity":           "7d025230690f5c49738e52f18640928c57fd9059066a07e7acf488639284c117",
	"func:debtCatchUpCollectorSource":              "0e272ec196e9371bbda99c9baeadf4746df8ae1d081619c62b1476e032f87a9a",
	"func:debtCatchUpFreshnessCollectorTransition": "4d20cb4889630c6def4298bea6be19ec9d7c56979cc0f48f7f368f9591eb0c6e",
	"func:debtCatchUpFreshnessPredecessors":        "033d6c7ece99244c41d5a8e8b899432873dcea730a23536050046ebc835e4a64",
	"func:debtCatchUpFreshnessOwnerSource":         "e852e431c161135f7612b6f6ce76f47ad93d5d5515663eabe2edf953e938ce0b",
}

func TestPersistenceAuthorityDebtFreshnessCatchUpSourceIsExact(t *testing.T) {
	root := debtCatchUpHistoricalArchive(t, debtCatchUpNativeEntryLane)
	if actual, err := debtCollectorDigest(root); err != nil || actual != debtCatchUpFreshnessCollectorTo {
		t.Errorf("common descendant collector=%s expected=%s error=%v", actual, debtCatchUpFreshnessCollectorTo, err)
	}
	for _, path := range debtCatchUpCollectorFiles {
		versions := make([]map[string]string, 4)
		for index, ref := range []string{"3a6de3d3b224fb74003c6da3377a8b85d1fd3b9e", debtCatchUpFreshnessLane, debtCatchUpFreshnessInput, ""} {
			var raw []byte
			var err error
			if ref == "" {
				raw, err = os.ReadFile(filepath.Join(root, path))
			} else {
				raw, err = debtGit(root, "show", ref+":"+path)
			}
			if err != nil {
				t.Fatal(err)
			}
			versions[index] = debtCatchUpDeclarations(t, raw)
		}
		assertFreshnessCatchUpDeclarations(t, path, versions)
	}
	if err := debtCatchUpFreshnessOwnerSource(root); err != nil {
		t.Fatal(err)
	}
	proveFreshnessCatchUpNativeComposition(t, root)
}

func assertFreshnessCatchUpDeclarations(t *testing.T, path string, versions []map[string]string) {
	t.Helper()
	keys := map[string]bool{}
	for _, version := range versions {
		for key := range version {
			keys[key] = true
		}
	}
	for key := range keys {
		base, lane, incoming, current := versions[0][key], versions[1][key], versions[2][key], versions[3][key]
		if expected, composed := debtCatchUpFreshnessDeclarationPins[key]; composed {
			if actual := debtBaselineChecksum([]byte(current)); actual != expected {
				t.Errorf("composed declaration %s/%s digest=%s expected=%s", path, key, actual, expected)
			}
			continue
		}
		want := lane
		if incoming != base {
			if lane != base && lane != incoming {
				t.Errorf("unlisted double-changed declaration %s/%s", path, key)
				continue
			}
			want = incoming
		}
		if current != want {
			t.Errorf("reviewed declaration lost or overwritten: %s/%s", path, key)
		}
	}
}

func proveFreshnessCatchUpNativeComposition(t *testing.T, root string) {
	t.Helper()
	const transaction = "internal/store/internal/backend/sqlite/transaction.go"
	approved, err := debtGit(root, "show", debtCatchUpFreshnessInput+":"+transaction)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(filepath.Join(root, transaction))
	if err != nil || !bytes.Equal(actual, approved) {
		t.Fatal("lower native cleanup/observation owner differs from reviewed E input:", err)
	}
	const fault = "internal/store/internal/backend/sqlite/test_agent_snapshot_corruption.go"
	before, err := debtGit(root, "show", debtCatchUpFreshnessLane+":"+fault)
	if err != nil {
		t.Fatal(err)
	}
	anchor := []byte("\toperation := mutationOperation{label: \"agent snapshot corruption fixture\"")
	if bytes.Count(before, anchor) != 1 {
		t.Fatal("original outer corruption owner lost its unique admission anchor")
	}
	pair := []byte("\twriteObservation := b.agentRecoveryWrites.ObserveWriteStart()\n\tdefer writeObservation.End()\n")
	want := bytes.Replace(before, anchor, append(pair, anchor...), 1)
	actual, err = os.ReadFile(filepath.Join(root, fault))
	if err != nil || !bytes.Equal(actual, want) {
		t.Fatal("outer corruption owner changed beyond the exact approved observer pair:", err)
	}
}

func TestPersistenceAuthorityDebtFreshnessCatchUpForwardAndForeignProvenance(t *testing.T) {
	root := debtCatchUpHistoricalArchive(t, debtCatchUpNativeEntryLane)
	raw, err := os.ReadFile(filepath.Join(root, debtBaselinePath))
	if err != nil {
		t.Fatal(err)
	}
	head, err := parseAuthorityDebtBaseline(raw)
	if err != nil {
		t.Fatal(err)
	}
	head.Collector = debtCatchUpFreshnessCollectorTo
	// Retained-ledger controls only; actual per-origin census proof is separate.
	proof := debtDeliveryRetirementProof{Root: root, Actual: head.Sites}
	for _, input := range [][2]string{{debtCatchUpFreshnessLane, debtCatchUpCollectorTo}, {debtCatchUpFreshnessInput, debtProcessLeaseFreshnessCollectorTo}, {debtCatchUpMaster, debtProcessLeaseCollectorTo}, {debtCatchUpLane, debtServedObservationCollectorTo}} {
		trusted, _, err := debtCatchUpSignedBaseline(root, input[0], input[1])
		if err != nil {
			t.Fatal(err)
		}
		if err := debtValidateCollectorIdentity(input[1], head.Collector, trusted, head, "", proof); err != nil {
			t.Fatal("exact common descendant refused:", err)
		}
		assertDebtCatchUpForeignProvenance(t, trusted, head, proof)
	}
}

func TestPersistenceAuthorityDebtFreshnessCatchUpRefusesChangedCurrentSource(t *testing.T) {
	root := debtCatchUpHistoricalArchive(t, debtCatchUpNativeEntryLane)
	copy := debtCatchUpSourceCopyFrom(t, root)
	var paths []string
	for path := range debtCatchUpFreshnessNativePins {
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
		t.Fatal("exact joined-source copy refused:", err)
	}
	paths = append(append([]string{}, debtCatchUpCollectorFiles...),
		"internal/store/internal/backend/sqlite/transaction.go",
		"internal/store/internal/backend/sqlite/test_agent_snapshot_corruption.go")
	for _, path := range paths {
		raw, err := os.ReadFile(filepath.Join(copy, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw)+"\nvar unreviewedFreshnessSource int\n")
		if err := debtCatchUpCollectorSource(copy); err == nil {
			t.Fatal("changed current collector/native source admitted:", path)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
	debtWriteModuleSource(t, copy, custody+"/unreviewed_custody.go", "package startupownership\nfunc (*StartupPostgresOwner) unreviewedCustodyMethod() {}\n")
	if err := debtCatchUpCollectorSource(copy); err == nil {
		t.Fatal("new-file native custody method escaped the all-method source pin")
	}
}
