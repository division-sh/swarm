package store_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Excluded from the collector hash to avoid fingerprint self-reference.
const debtDeliveryRetirementCollectorTo = "1a7ef390064d157ebac41dcc40699c14739b033e5e9e4796a1909578a2bf870e"

func TestPersistenceAuthorityDebtDeliveryRetirementIsFiniteSourceVerifiedAndOneWay(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	total := 0
	for _, pair := range debtDeliveryRetirementPairs() {
		total += pair[0].Multiplicity
		t.Run(pair[0].Declaration, func(t *testing.T) {
			old, current := debtControlSet(pair[0]), debtControlSet(pair[1])
			for _, test := range []struct {
				name                       string
				actual, head, base, source map[string]authorityDebtSite
				proof, accept              bool
			}{
				{"historical-old", old, old, old, old, false, true},
				{"historical-intermediate", current, current, old, old, false, true},
				{"unrefreshed-head", debtControlSet(), current, old, old, true, true},
				{"refreshed-head", debtControlSet(), debtControlSet(), old, old, true, true},
				{"landed-intermediate-donor", debtControlSet(), debtControlSet(), current, current, true, true},
				{"landed-all-absent", debtControlSet(), debtControlSet(), debtControlSet(), debtControlSet(), true, true},
				{"missing-native-proof", debtControlSet(), current, old, old, false, false},
				{"reintroduced-old", old, current, old, old, true, false},
				{"reintroduced-intermediate", current, current, old, old, true, false},
				{"head-old-resurrection", debtControlSet(), old, old, old, true, false},
				{"missing-base-donor", debtControlSet(), current, debtControlSet(), old, true, false},
				{"missing-source-donor", debtControlSet(), current, old, debtControlSet(), true, false},
				{"missing-all-donors", debtControlSet(), current, debtControlSet(), debtControlSet(), true, false},
			} {
				t.Run(test.name, func(t *testing.T) {
					var proofs []debtDeliveryRetirementProof
					if test.proof {
						proofs = append(proofs, debtDeliveryRetirementProof{Root: root, Actual: test.actual})
					}
					if failures := authorityDebtRatchet(test.actual, test.head, test.base, test.source, proofs...); (len(failures) == 0) != test.accept {
						t.Fatalf("accept=%v failures=%v", test.accept, failures)
					}
				})
			}
			deliveryRetirementSlotControls(t, root, pair)
			actual, head := debtControlSet(), current
			before := authorityDebtCount(head)
			normalized, _, _, failures := debtDeliveryRetirementComparisons(actual, head, old, old, debtDeliveryRetirementProof{Root: root, Actual: actual})
			if len(failures) != 0 || authorityDebtCount(actual) != 0 || authorityDebtCount(head) != before || !debtSitesEqual(normalized, head) {
				t.Fatal("retirement invented actual sites or mutated its baseline")
			}
			added := debtControlSite("call:Exec", 1)
			actual, head = debtControlSet(added), debtControlSet(pair[1], added)
			if failures := authorityDebtRatchet(actual, head, old, old, debtDeliveryRetirementProof{Root: root, Actual: actual}); len(failures) == 0 {
				t.Fatal("retiring an escape paid for unrelated authority")
			}
		})
	}
	if total != 13 {
		t.Fatal("retirement occurrence inventory changed:", total)
	}
	for _, pair := range debtServedDebugSignatureTransitions() {
		if deliveryRetirementContains(pair) {
			continue
		}
		actual := debtControlSet()
		head, base, source := debtControlSet(pair[1]), debtControlSet(pair[0]), debtControlSet(pair[0])
		gotHead, gotBase, gotSource, failures := debtDeliveryRetirementComparisons(actual, head, base, source, debtDeliveryRetirementProof{Root: root, Actual: actual})
		if len(failures) != 0 || !debtSitesEqual(head, gotHead) || !debtSitesEqual(base, gotBase) || !debtSitesEqual(source, gotSource) {
			t.Fatal("seven-pair retirement changed an unrelated identity:", pair[0].Declaration, pair[0].Operation)
		}
		if failures := authorityDebtRatchet(actual, head, base, source); len(failures) == 0 {
			t.Fatal("unrelated deletion accepted without its separate source authorization:", pair[0].Declaration, pair[0].Operation)
		}
	}
	deliveryRetirementProofControls(t, root)
}

func TestPersistenceAuthorityDebtDeliveryRetirementKeepsLaterServedAuthorizationSeparate(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	var later [2]authorityDebtSite
	for _, pair := range debtServedObservationRetirementPairs() {
		if pair[0].Declaration == "TestAudit2277StoppedRunReadinessRestart" && strings.HasSuffix(pair[0].Operation, ".waitServedEventPublishEventID") {
			later = pair
		}
	}
	if later[0].Declaration == "" || deliveryRetirementContains(later) {
		t.Fatal("the later exact authorization was lost or absorbed into the seven-pair owner")
	}
	actual, head, base := debtControlSet(), debtControlSet(later[1]), debtControlSet(later[0])
	if failures := authorityDebtRatchet(actual, head, base, base, debtDeliveryRetirementProof{Root: root, Actual: actual}); len(failures) != 0 {
		t.Fatal("the separate reviewed served-observation retirement was refused:", failures)
	}
	for _, tc := range []struct {
		name string
		site authorityDebtSite
	}{
		{"foreign", requestKeyChangedDebtSite(later[1], "declaration")},
		{"new", debtControlSite("call:unreviewedRawCaller", 1)},
		{"increased", requestKeyChangedDebtSite(later[1], "multiplicity")},
		{"reintroduced", later[0]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := debtControlSet(tc.site)
			if failures := authorityDebtRatchet(actual, debtControlSet(later[1], tc.site), base, base, debtDeliveryRetirementProof{Root: root, Actual: actual}); len(failures) == 0 {
				t.Fatal("a later authorization accepted foreign/new/increased/reintroduced debt")
			}
		})
	}
}

func deliveryRetirementProofControls(t *testing.T, root string) {
	t.Helper()
	pair, actual := debtDeliveryRetirementPairs()[0], debtControlSet()
	proof := debtDeliveryRetirementProof{Root: root, Actual: actual}
	for _, invalid := range [][]debtDeliveryRetirementProof{
		{proof, proof},
		{{Root: "", Actual: actual}},
		{{Root: root, Actual: debtControlSet(pair[1])}},
	} {
		if failures := authorityDebtRatchet(actual, debtControlSet(pair[1]), debtControlSet(pair[0]), debtControlSet(pair[0]), invalid...); len(failures) == 0 {
			t.Fatal("unbound source/census evidence admitted")
		}
	}
	proof.Root = filepath.Join(t.TempDir(), "landed-source-no-longer-needs-donor")
	if failures := authorityDebtRatchet(actual, actual, actual, actual, proof); len(failures) != 0 {
		t.Fatal("landed deletion still requires a historical donor:", failures)
	}
}

func deliveryRetirementSlotControls(t *testing.T, root string, pair [2]authorityDebtSite) {
	t.Helper()
	for slot := range 4 {
		for _, field := range []string{"coexistence", "multiplicity", "kind", "file", "declaration", "operation", "resolved", "family", "owner"} {
			t.Run(fmt.Sprintf("slot-%d/%s", slot, field), func(t *testing.T) {
				deliveryRetirementMutation(t, root, pair, slot, field)
			})
		}
	}
}

func deliveryRetirementContains(pair [2]authorityDebtSite) bool {
	for _, approved := range debtDeliveryRetirementPairs() {
		if pair == approved {
			return true
		}
	}
	return false
}

func deliveryRetirementMutation(t *testing.T, root string, pair [2]authorityDebtSite, slot int, field string) {
	t.Helper()
	states := [4]map[string]authorityDebtSite{debtControlSet(), debtControlSet(pair[1]), debtControlSet(pair[0]), debtControlSet(pair[0])}
	states[slot] = maps.Clone(states[slot])
	site := pair[1]
	if slot >= 2 {
		site = pair[0]
	}
	delete(states[slot], site.identity())
	if field == "coexistence" {
		states[slot][pair[0].identity()], states[slot][pair[1].identity()] = pair[0], pair[1]
	} else {
		site = requestKeyChangedDebtSite(site, field)
		if field == "multiplicity" {
			site.Multiplicity = pair[0].Multiplicity + 1
		}
		states[slot][site.identity()] = site
	}
	if failures := authorityDebtRatchet(states[0], states[1], states[2], states[3], debtDeliveryRetirementProof{Root: root, Actual: states[0]}); len(failures) == 0 {
		t.Fatalf("retirement accepted changed slot=%d field=%s", slot, field)
	}
}

func TestPersistenceAuthorityDebtDeliveryRetirementRefusesChangedNativeSource(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	copy := debtCatchUpSourceCopyFrom(t, root)
	copyRepair2698CustodySources(t, root, copy)
	const file = "internal/serveapp/main_runtime_test.go"
	const manifest = "tools/fixture-codemod/pipeline-observations/family145-delivery-status-preimages.json"
	bodies, err := debtDeliveryStatusBodies(root)
	if err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(filepath.Join(copy, file))
	if err != nil {
		t.Fatal(err)
	}
	pair := debtDeliveryRetirementPairs()[0]
	actual := debtControlSet()
	check := func() []string {
		return authorityDebtRatchet(actual, debtControlSet(pair[1]), debtControlSet(pair[0]), debtControlSet(pair[0]), debtDeliveryRetirementProof{Root: copy, Actual: actual})
	}
	if failures := check(); len(failures) != 0 {
		t.Fatal(failures)
	}
	for _, body := range bodies {
		declarations, err := debtServedDebugDeclarations(filepath.Join(copy, file))
		if err != nil {
			t.Fatal(err)
		}
		current := declarations[body.Function]
		mutant := deliveryRetirementMutatedBody(t, current)
		if current == "" || mutant == current {
			t.Fatal("hostile retirement source cut is not exact:", body.Function)
		}
		debtSourceBlobCopyDeclaration(t, copy, file, body.Function, mutant)
		changed, err := debtServedDebugDeclarations(filepath.Join(copy, file))
		if err != nil || changed[body.Function] == current || !strings.Contains(changed[body.Function], "foreignRetirementSource()") {
			t.Fatal("hostile retirement mutation did not reach the actual declaration:", body.Function, err)
		}
		if failures := check(); len(failures) == 0 {
			t.Fatal("changed complete native source admitted:", body.Function)
		}
		debtWriteModuleSource(t, copy, file, string(original))
	}
	debtWriteModuleSource(t, copy, file, string(original))
	debtWriteModuleSource(t, copy, manifest, "[]\n")
	if failures := check(); len(failures) == 0 {
		t.Fatal("missing/changed exact source manifest admitted")
	}
	if err := os.Remove(filepath.Join(copy, file)); err != nil {
		t.Fatal(err)
	}
	if failures := check(); len(failures) == 0 {
		t.Fatal("missing native source admitted")
	}
}

func deliveryRetirementMutatedBody(t *testing.T, current string) string {
	t.Helper()
	const prefix = "package proof\n"
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "retirement.go", prefix+current, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		t.Fatal("retirement mutant requires a unique complete declaration:", err)
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok || fn.Body == nil {
		t.Fatal("retirement mutant requires a function body")
	}
	start := set.Position(fn.Body.Lbrace).Offset - len(prefix) + 1
	return current[:start] + "\nforeignRetirementSource();\n" + current[start:]
}

func TestPersistenceAuthorityDebtDeliveryRetirementFingerprint(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	if collector, err := debtCollectorDigest(root); err != nil || collector != debtActiveCollectorTo {
		t.Fatalf("delivery retirement collector=%s expected=%s error=%v", collector, debtActiveCollectorTo, err)
	}
}

func TestPersistenceAuthorityDebtDeliveryRetirementCollectorPreservesExactChain(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	old, current := debtServedDebugTransitionStates()
	for _, pair := range debtRequestKeySignatureTransitions() {
		old[pair[0].identity()], current[pair[1].identity()] = pair[0], pair[1]
	}
	_, uncertainty := debtPublicA2EndpointTransition()
	old[uncertainty.identity()], current[uncertainty.identity()] = uncertainty, uncertainty
	actual := maps.Clone(current)
	for _, pair := range debtDeliveryRetirementPairs() {
		delete(actual, pair[1].identity())
	}
	proof := debtDeliveryRetirementProof{Root: root, Actual: actual}
	for _, origin := range []string{debtReceiverReadbackCollectorTo, debtRequestKeyCollectorTo, debtRunScalarCollectorTo, debtDeliveryStatusCollectorTo} {
		for _, refreshed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/refreshed-%v", origin, refreshed), func(t *testing.T) {
				trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: origin, Sites: old}
				head := trusted
				head.Collector, head.Sites = debtDeliveryRetirementCollectorTo, current
				if refreshed {
					head.Sites = actual
				}
				if err := debtValidateCollectorIdentity(origin, head.Collector, trusted, head, "", proof); err != nil {
					t.Fatal("exact collector ancestry refused:", err)
				}
				for _, mutation := range []string{"bootstrap", "missing-donor", "addition", "multiplicity", "foreign-base", "foreign-destination", "reverse", "missing-proof"} {
					t.Run(mutation, func(t *testing.T) {
						trusted, head, proof := trusted, head, proof
						base, destination := origin, head.Collector
						head.Sites, trusted.Sites = maps.Clone(head.Sites), maps.Clone(trusted.Sites)
						pair := debtDeliveryRetirementPairs()[0]
						switch mutation {
						case "bootstrap":
							head.BootstrapSource = strings.Repeat("b", 40)
						case "missing-donor":
							delete(trusted.Sites, pair[0].identity())
							head.Sites[pair[1].identity()] = pair[1] // Claiming a transfer still requires its donor.
						case "addition":
							added := debtControlSite("call:Exec", 1)
							head.Sites[added.identity()] = added
						case "multiplicity":
							changed := pair[1]
							changed.Multiplicity++
							head.Sites[changed.identity()] = changed
						case "foreign-base":
							base, trusted.Collector = strings.Repeat("b", 64), strings.Repeat("b", 64)
						case "foreign-destination":
							destination, head.Collector = strings.Repeat("c", 64), strings.Repeat("c", 64)
						case "reverse":
							base, destination = destination, base
							trusted.Collector, head.Collector = base, destination
						case "missing-proof":
							proof.Root = ""
						}
						if err := debtValidateCollectorIdentity(base, destination, trusted, head, "", proof); err == nil {
							t.Fatal("invalid retirement collector chain admitted:", mutation)
						}
					})
				}
			})
		}
	}
}

func TestPersistenceAuthorityDebtDeliveryRetirementMetadataIsLocalAndSourceBound(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	copy := debtCatchUpSourceCopyFrom(t, root)
	copyRepair2698CustodySources(t, root, copy)
	_, current := debtServedDebugTransitionStates()
	baseline := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtCacheCollectorFrom, Sites: current}
	data := marshalAuthorityDebtBaseline(baseline)
	metadata, err := debtCollectorMetadataBytes(debtCollectorMetadata{Version: 1, Collector: debtRunScalarCollectorTo, BaselineSHA256: debtBaselineChecksum(data), Predecessor: debtCacheCollectorFrom})
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata))
	t.Setenv(debtRefreshEnv, "downward")
	t.Setenv("CI", "")
	t.Setenv("GITHUB_ACTIONS", "")
	got, err := debtBaselineWithCollector(copy, data, debtDeliveryRetirementCollectorTo, false)
	if err != nil || got.Collector != debtDeliveryRetirementCollectorTo || !debtSitesEqual(got.Sites, baseline.Sites) {
		t.Fatal("local metadata rotation changed retained occurrences:", err)
	}
	for _, mode := range []string{"read-only", "CI", "GITHUB_ACTIONS", "historical", "malformed", "foreign-collector", "checksum", "source"} {
		t.Run(mode, func(t *testing.T) {
			input := deliveryRetirementMetadataMutation(t, copy, mode, data, metadata)
			if _, err := debtBaselineWithCollector(copy, input, debtDeliveryRetirementCollectorTo, mode == "historical"); err == nil {
				t.Fatal("unqualified local metadata transition admitted:", mode)
			}
		})
	}
	if stored, err := os.ReadFile(filepath.Join(copy, debtCollectorSidecarPath)); err != nil || string(stored) != string(metadata) {
		t.Fatal("validation rewrote the sidecar:", err)
	}
}

func deliveryRetirementMetadataMutation(t *testing.T, copy, mode string, data, metadata []byte) []byte {
	t.Helper()
	switch mode {
	case "read-only":
		t.Setenv(debtRefreshEnv, "")
	case "CI", "GITHUB_ACTIONS":
		t.Setenv(mode, "true")
	case "malformed", "foreign-collector":
		changed := string(metadata) + " "
		if mode == "foreign-collector" {
			changed = strings.Replace(string(metadata), debtRunScalarCollectorTo, debtDeliveryStatusCollectorTo, 1)
		}
		debtWriteModuleSource(t, copy, debtCollectorSidecarPath, changed)
		t.Cleanup(func() { debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata)) })
	case "checksum":
		return append(append([]byte{}, data...), '\n')
	case "source":
		path := "tools/fixture-codemod/pipeline-observations/family145-delivery-status-preimages.json"
		raw, err := os.ReadFile(filepath.Join(copy, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw)+" ")
		t.Cleanup(func() { debtWriteModuleSource(t, copy, path, string(raw)) })
	}
	return data
}
