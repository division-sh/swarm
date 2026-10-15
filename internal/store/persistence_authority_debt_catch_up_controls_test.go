package store_test

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// These immutable inputs and the finished destination belong outside the
// self-hashed validator. Historical constants remain in their original owners.
const debtCatchUpBase = "26701e7b187579220dffe72e01debd2e89cd2675"
const debtCatchUpMaster = "19dc188f6d57985a3360a619ea57b890b3c148dd"
const debtCatchUpLane = "dec6f1a009400d230b0bb92c83c0997ae35b2938"
const debtCatchUpHistoryPacketSHA = "ddadfba0b9aff02fa963daebb07d95f9bda9b71ea84d2ea3c45cec466275aa5e"
const debtCatchUpCollectorTo = "06cd3bb8d6a0bceeb68f62266dd77a2816cd92f36305d79e14347f074c25b010"
const debtCatchUpForkSuccessorSHA = "bc2a02af3b9d426d97611481f691db79094983a350d5c40ed79741d96dd7d0c4"
const debtCatchUpForkSourceSHA = "c817aff36c29a349de2e64fbe2e8312cc93316ddc2fcf37f31a3838efd081992"

var debtCatchUpCollectorFiles = []string{
	"internal/store/persistence_authority_debt_census_test.go",
	"internal/store/persistence_authority_debt_test.go",
	"internal/store/persistence_authority_debt_cache_test.go",
	"internal/store/persistence_authority_debt_sidecar_test.go",
	"internal/store/persistence_authority_debt_process_lease_test.go",
	"internal/checkoutsource/fixture_role.go",
}

func debtCatchUpHistoricalArchive(t *testing.T, ref string) string {
	t.Helper()
	root := persistenceAuthorityRepoRoot(t)
	archive := materializeDebtBase(t, root, ref)
	gitdir, err := debtGit(root, "rev-parse", "--absolute-git-dir")
	if err != nil {
		t.Fatal(err)
	}
	// Same immutable object database, only for historical source receipts.
	debtWriteModuleSource(t, archive, ".git", "gitdir: "+strings.TrimSpace(string(gitdir))+"\n")
	return archive
}

func TestPersistenceAuthorityDebtCatchUpCollectorFingerprintIsExact(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	actual, err := debtCollectorDigest(root)
	if err != nil || actual != debtCatchUpCollectorTo {
		t.Fatalf("finished joined source: actual=%s expected=%s error=%v", actual, debtCatchUpCollectorTo, err)
	}
	if err := debtCatchUpCollectorSource(root); err != nil {
		t.Fatal(err)
	}
	for _, input := range [][2]string{{debtCatchUpLane, debtServedObservationCollectorTo}, {debtCatchUpMaster, debtProcessLeaseCollectorTo}} {
		copy := t.TempDir()
		for _, path := range debtCatchUpCollectorFiles {
			raw, err := debtGit(root, "show", input[0]+":"+path)
			if err != nil && path == "internal/store/persistence_authority_debt_process_lease_test.go" && input[0] == debtCatchUpLane {
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			debtWriteModuleSource(t, copy, path, string(raw))
		}
		if actual, err := debtCollectorDigest(copy); err != nil || actual != input[1] {
			t.Fatalf("immutable collector predecessor %s changed: %s: %v", input[0], actual, err)
		}
	}
}

func TestPersistenceAuthorityDebtCatchUpForkRenameRetainsAlreadyAcceptedRemovals(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	lane, _, err := debtCatchUpSignedBaseline(root, debtCatchUpLane, debtServedObservationCollectorTo)
	if err != nil {
		t.Fatal(err)
	}
	old, current := debtControlSet(), debtControlSet()
	for _, historical := range debtForkProofRows() {
		retained, found := lane.Sites[historical.identity()]
		if !found {
			t.Fatal("accepted lane lost a fork-rename donor")
		}
		old[retained.identity()] = retained
		renamed := debtForkProofRenamed(retained)
		current[renamed.identity()] = renamed
	}
	if authorityDebtCount(old) != 17 || authorityDebtCount(current) != 17 || !debtCatchUpForkProofDonors(old, old) {
		t.Fatal("common descendant resurrected four previously removed query/scan occurrences")
	}
	for _, ceiling := range []map[string]authorityDebtSite{old, current} {
		if failures := authorityDebtRatchet(current, ceiling, old, old); len(failures) != 0 {
			t.Fatal("exact17 ->17 rename refused:", failures)
		}
	}
	for _, retained := range old {
		debtForkProofHostility(t, retained, old, current)
	}
	_, normalized, source, failures := debtForkProofComparisons(current, old, old, old)
	if len(failures) != 0 || authorityDebtCount(normalized) != 17 || authorityDebtCount(source) != 17 {
		t.Fatal("existing rename earned new deletion credit")
	}
}

func debtCatchUpDeclarations(t *testing.T, raw []byte) map[string]string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "collector.go", raw, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	declarations := map[string]string{}
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok {
			key := "func:" + fn.Name.Name
			if fn.Recv != nil {
				key += catchUpDeclarationText(t, fn.Recv.List[0].Type)
			}
			declarations[key] = catchUpDeclarationText(t, fn)
			continue
		}
		group := declaration.(*ast.GenDecl)
		for _, spec := range group.Specs {
			switch spec := spec.(type) {
			case *ast.TypeSpec:
				declarations["type:"+spec.Name.Name] = catchUpDeclarationText(t, spec)
			case *ast.ValueSpec:
				for _, name := range spec.Names {
					declarations[group.Tok.String()+":"+name.Name] = catchUpDeclarationText(t, spec)
				}
			}
		}
	}
	return declarations
}

func catchUpDeclarationText(t *testing.T, node any) string {
	t.Helper()
	var canonical bytes.Buffer
	if err := format.Node(&canonical, token.NewFileSet(), node); err != nil {
		t.Fatal(err)
	}
	return canonical.String()
}

func TestPersistenceAuthorityDebtCatchUpCollectorPreservesBothPolicyOwners(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	for _, path := range debtCatchUpCollectorFiles {
		versions := make([]map[string]string, 4)
		for index, ref := range []string{debtCatchUpBase, debtCatchUpMaster, debtCatchUpLane, ""} {
			var raw []byte
			var err error
			if ref == "" {
				raw, err = os.ReadFile(filepath.Join(root, path))
			} else {
				raw, err = debtGit(root, "show", ref+":"+path)
			}
			if err != nil && path == "internal/store/persistence_authority_debt_process_lease_test.go" && (index == 0 || index == 2) {
				versions[index] = map[string]string{}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			versions[index] = debtCatchUpDeclarations(t, raw)
		}
		assertDebtCatchUpPolicyComposition(t, path, versions)
	}
}

func assertDebtCatchUpPolicyComposition(t *testing.T, path string, versions []map[string]string) {
	t.Helper()
	keys := map[string]bool{}
	for _, declarations := range versions {
		for key := range declarations {
			keys[key] = true
		}
	}
	for key := range keys {
		base, master, lane, current := versions[0][key], versions[1][key], versions[2][key], versions[3][key]
		if debtCatchUpComposedPolicyDeclaration(path, key) {
			continue // Independently pinned by the finished source fingerprint.
		}
		want := lane
		if master != base {
			want = master
			if lane != base && lane != master {
				t.Errorf("unlisted double-changed policy: %s/%s", path, key)
				continue
			}
		}
		if current != want {
			t.Errorf("policy owner declaration was lost or overwritten: %s/%s", path, key)
		}
	}
}

func debtCatchUpComposedPolicyDeclaration(path, key string) bool {
	if path == "internal/store/persistence_authority_debt_test.go" {
		switch key {
		case "func:debtForkProofComparisons", "func:debtForkProofSourcePins", "func:debtValidateCollectorIdentity", "func:authorityDebtRatchet", "func:TestPersistenceAuthorityDebtRatchet":
			return true
		default:
			return false
		}
	}
	if path != "internal/store/persistence_authority_debt_sidecar_test.go" {
		return false
	}
	switch key {
	case "func:debtSourceUnrefreshedBaseline", "type:debtCatchUpSourceFile", "type:debtCatchUpSourcePacket", "func:debtCatchUpReadPacket", "func:debtCatchUpLaneSource", "func:debtCatchUpLaneDeclarations", "func:debtCatchUpLaneDeclaration",
		"func:debtCatchUpForkProofDonors", "func:debtCatchUpSignedBaseline", "func:debtCatchUpCollectorSource", "func:debtCatchUpCollectorTransition", "func:debtCatchUpBothPredecessors", "func:debtCatchUpUnrefreshedBaseline":
		return true
	default:
		return false
	}
}

func TestPersistenceAuthorityDebtCatchUpForwardAndForeignProvenance(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, debtBaselinePath))
	if err != nil {
		t.Fatal(err)
	}
	head, err := debtBaselineWithCollector(root, raw, debtCatchUpCollectorTo, false)
	if err != nil {
		t.Fatal(err)
	}
	// These are retained-ledger controls, not substitute census evidence.
	proof := debtDeliveryRetirementProof{Root: root, Actual: head.Sites}
	for _, input := range [][2]string{{debtCatchUpLane, debtServedObservationCollectorTo}, {debtCatchUpMaster, debtProcessLeaseCollectorTo}} {
		trusted, _, err := debtCatchUpSignedBaseline(root, input[0], input[1])
		if err != nil {
			t.Fatal(err)
		}
		if err := debtValidateCollectorIdentity(input[1], head.Collector, trusted, head, "", proof); err != nil {
			t.Fatal("exact joined route refused:", input[0], err)
		}
		assertDebtCatchUpForeignProvenance(t, trusted, head, proof)
	}
	if err := debtValidateCollectorIdentity(head.Collector, head.Collector, head, head, "", proof); err != nil {
		t.Fatal("unchanged landed descendant refused:", err)
	}
}

func assertDebtCatchUpForeignProvenance(t *testing.T, trusted, head authorityDebtBaseline, proof debtDeliveryRetirementProof) {
	t.Helper()
	for _, mutation := range []string{"foreign-base", "matching-foreign-base", "foreign-destination", "matching-foreign-destination", "reverse", "bootstrap", "trusted-bootstrap", "missing-proof", "duplicate-proof", "foreign-source", "donor", "increase", "deletion-pays-for-new-site"} {
		t.Run(trusted.Collector+"/"+mutation, func(t *testing.T) {
			base, destination := trusted.Collector, head.Collector
			before, after := trusted, head
			proofs := []debtDeliveryRetirementProof{proof}
			switch mutation {
			case "foreign-base":
				base = strings.Repeat("a", 64)
			case "matching-foreign-base":
				base, before.Collector = strings.Repeat("a", 64), strings.Repeat("a", 64)
			case "foreign-destination":
				destination = strings.Repeat("b", 64)
			case "matching-foreign-destination":
				destination, after.Collector = strings.Repeat("b", 64), strings.Repeat("b", 64)
			case "reverse":
				base, destination, before, after = destination, base, after, before
			case "bootstrap":
				after.BootstrapSource = strings.Repeat("c", 40)
			case "trusted-bootstrap":
				before.BootstrapSource = strings.Repeat("c", 40)
			case "missing-proof":
				proofs = nil
			case "duplicate-proof":
				proofs = append(proofs, proof)
			case "foreign-source":
				proofs[0].Root = t.TempDir()
			case "donor":
				before.Sites = maps.Clone(before.Sites)
				delete(before.Sites, debtSortedKeys(before.Sites)[0])
			case "increase":
				after.Sites = maps.Clone(after.Sites)
				key := debtSortedKeys(after.Sites)[0]
				row := after.Sites[key]
				row.Multiplicity++
				after.Sites[key] = row
				proofs[0].Actual = after.Sites
			case "deletion-pays-for-new-site":
				after.Sites = maps.Clone(after.Sites)
				delete(after.Sites, debtSortedKeys(after.Sites)[0])
				escape := debtControlSite("call:Exec", 1)
				after.Sites[escape.identity()] = escape
				proofs[0].Actual = after.Sites
			}
			if err := debtValidateCollectorIdentity(base, destination, before, after, destination, proofs...); err == nil {
				t.Fatal("joined-source permission accepted foreign provenance or unrelated payment:", mutation)
			}
		})
	}
}

func TestPersistenceAuthorityDebtCatchUpSourceReadsRemainFresh(t *testing.T) {
	copy := debtCatchUpSourceCopy(t)
	const manifest = "tools/fixture-codemod/pipeline-observations/internal/selectedreadrole/testdata/catch_up_2542.json"
	if err := debtCatchUpCollectorSource(copy); err != nil {
		t.Fatal("exact finite source copy refused:", err)
	}
	for _, path := range []string{debtCatchUpCollectorFiles[0], "internal/serveapp/test_command_runtime_test.go", manifest} {
		raw, err := os.ReadFile(filepath.Join(copy, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw)+"\nvar unreviewedCatchUpSource int\n")
		if err := debtCatchUpCollectorSource(copy); err == nil {
			t.Fatal("warm source control accepted mutation:", path)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
		if err := os.Remove(filepath.Join(copy, path)); err != nil {
			t.Fatal(err)
		}
		if err := debtCatchUpCollectorSource(copy); err == nil {
			t.Fatal("warm source control accepted missing input:", path)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
}

func debtCatchUpSourceCopy(t *testing.T) string {
	t.Helper()
	root, copy := persistenceAuthorityRepoRoot(t), t.TempDir()
	packet, err := debtCatchUpReadPacket(root)
	if err != nil {
		t.Fatal(err)
	}
	paths := append([]string(nil), debtCatchUpCollectorFiles...)
	const manifest = "tools/fixture-codemod/pipeline-observations/internal/selectedreadrole/testdata/catch_up_2542.json"
	paths = append(paths, manifest)
	for _, file := range packet.Files {
		if file.ComposedSHA256 != "absent" {
			paths = append(paths, file.File)
		}
	}
	paths = append(paths, debtCatchUpDeclaredProofInputs(t, root)...)
	for _, path := range paths {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
	gitdir, err := debtGit(root, "rev-parse", "--absolute-git-dir")
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, ".git", "gitdir: "+strings.TrimSpace(string(gitdir))+"\n")
	return copy
}

func debtCatchUpDeclaredProofInputs(t *testing.T, root string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "internal/store/persistence_authority_debt*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string]bool{}
	for _, path := range files {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil || !filepath.IsLocal(value) || !(strings.HasPrefix(value, "internal/") || strings.HasPrefix(value, "tools/") || strings.HasPrefix(value, ".github/")) || strings.ContainsAny(value, "\n\r\t") {
				return true
			}
			info, err := os.Stat(filepath.Join(root, value))
			if err == nil && info.Mode().IsRegular() {
				paths[value] = true
			} else if err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			return true
		})
	}
	var result []string
	for _, path := range debtCatchUpManifestProofFiles(t, root, paths) {
		paths[path] = true
	}
	for path := range paths {
		result = append(result, path)
	}
	return result
}

func debtCatchUpManifestProofFiles(t *testing.T, root string, paths map[string]bool) []string {
	t.Helper()
	type source struct{ File string }
	var files []string
	for path := range paths {
		if filepath.Ext(path) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil || !json.Valid(raw) {
			t.Fatal("invalid declared JSON proof input:", path, err)
		}
		var rows []source
		if bytes.HasPrefix(bytes.TrimSpace(raw), []byte("[")) {
			if err := json.Unmarshal(raw, &rows); err != nil {
				t.Fatal(err)
			}
		} else {
			var packet struct {
				File        string
				Files, Rows []source
			}
			if err := json.Unmarshal(raw, &packet); err != nil {
				t.Fatal(err)
			}
			rows = append(packet.Files, packet.Rows...)
			rows = append(rows, source{File: packet.File})
		}
		for _, row := range rows {
			if strings.HasPrefix(row.File, "internal/") && filepath.IsLocal(row.File) {
				if info, err := os.Stat(filepath.Join(root, row.File)); err == nil && info.Mode().IsRegular() {
					files = append(files, row.File)
				}
			}
		}
	}
	return files
}

func TestPersistenceAuthorityDebtCatchUpPendingMetadataRequiresExactDownwardProvenance(t *testing.T) {
	t.Setenv(debtRefreshEnv, "downward")
	t.Setenv("CI", "")
	t.Setenv("GITHUB_ACTIONS", "")
	root, copy := persistenceAuthorityRepoRoot(t), debtCatchUpSourceCopy(t)
	raw, err := os.ReadFile(filepath.Join(root, debtBaselinePath))
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := parseAuthorityDebtBaseline(raw)
	if err != nil {
		t.Fatal(err)
	}
	_, metadata, err := debtCatchUpSignedBaseline(root, debtCatchUpLane, debtServedObservationCollectorTo)
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata))
	got, err := debtBaselineWithCollector(copy, raw, debtCatchUpCollectorTo, false)
	if err != nil || got.Collector != debtCatchUpCollectorTo || !debtSitesEqual(got.Sites, baseline.Sites) {
		t.Fatal("exact pending metadata changed sites or lost its joined source:", err)
	}
	for _, mutation := range []string{"readonly", "historical", "ci", "actions", "checksum", "predecessor", "foreign-collector", "bootstrap", "increase"} {
		t.Run(mutation, func(t *testing.T) {
			candidate, sidecar := raw, metadata
			collector, historical := debtCatchUpCollectorTo, false
			switch mutation {
			case "readonly":
				t.Setenv(debtRefreshEnv, "")
			case "historical":
				historical = true
			case "ci":
				t.Setenv("CI", "true")
			case "actions":
				t.Setenv("GITHUB_ACTIONS", "true")
			case "checksum":
				sidecar = bytes.Replace(sidecar, []byte(`"baseline_sha256": "`), []byte(`"baseline_sha256": "changed`), 1)
			case "predecessor":
				sidecar = bytes.Replace(sidecar, []byte(debtCacheCollectorFrom), []byte(strings.Repeat("d", 64)), 1)
			case "foreign-collector":
				collector = strings.Repeat("e", 64)
			case "bootstrap":
				changed := baseline
				changed.BootstrapSource = strings.Repeat("f", 40)
				candidate = marshalAuthorityDebtBaseline(changed)
			case "increase":
				changed := baseline
				changed.Sites = maps.Clone(baseline.Sites)
				key := debtSortedKeys(changed.Sites)[0]
				row := changed.Sites[key]
				row.Multiplicity = 1000000
				changed.Sites[key] = row
				candidate = marshalAuthorityDebtBaseline(changed)
			}
			debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(sidecar))
			if _, err := debtBaselineWithCollector(copy, candidate, collector, historical); err == nil {
				t.Fatal("pending collector metadata admitted foreign or non-downward input:", mutation)
			}
		})
	}
}
