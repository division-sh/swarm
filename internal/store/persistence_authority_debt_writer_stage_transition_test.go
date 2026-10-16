package store_test

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const debtWriterStageCollectorTo = "93fcda6e89ca7cba0b866019360aa1ad2dce28af8190f667e736d01d57e67806"

func debtWriterStageTransitionStates() (map[string]authorityDebtSite, map[string]authorityDebtSite) {
	old, current := debtControlSet(), debtControlSet()
	for _, pair := range debtWriterStageSignatureTransitions() {
		old[pair[0].identity()] = pair[0]
		current[pair[1].identity()] = pair[1]
	}
	return old, current
}

func TestPersistenceAuthorityDebtWriterStageTransfersPreserveSevenExactOccurrences(t *testing.T) {
	old, current := debtWriterStageTransitionStates()
	if authorityDebtCount(old) != 7 || authorityDebtCount(current) != 7 {
		t.Fatal("seven transfers must retain seven raw debt units")
	}
	for _, test := range []struct {
		name                       string
		actual, head, base, source map[string]authorityDebtSite
		accept                     bool
	}{
		{"unchanged", old, old, old, old, true},
		{"forward-unrefreshed", current, old, old, old, true},
		{"forward-refreshed", current, current, old, old, true},
		{"landed-inert", current, current, current, current, true},
		{"reverse", old, old, current, current, false},
		{"resurrection", old, old, old, current, false},
		{"missing-source", current, current, old, debtControlSet(), false},
		{"missing-head", current, debtControlSet(), old, old, false},
		{"genuine-later-deletion", debtControlSet(), debtControlSet(), current, current, true},
		{"unrelated-addition-after-deletion", debtControlSet(debtControlSite("call:Exec", 1)), debtControlSet(debtControlSite("call:Exec", 1)), current, current, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if failures := authorityDebtRatchet(test.actual, test.head, test.base, test.source); (len(failures) == 0) != test.accept {
				t.Fatalf("accepted=%v failures=%v", test.accept, failures)
			}
		})
	}
	for _, pair := range debtWriterStageSignatureTransitions() {
		t.Run(pair[0].Declaration, func(t *testing.T) {
			debtRunFixtureSignatureHostileControls(t, pair, old, current)
			changed := pair[1]
			changed.Resolved = strings.Replace(changed.Resolved, "|arg=*database/sql.DB|", "|arg=*database/sql.Tx|", 1)
			actual := maps.Clone(current)
			delete(actual, pair[1].identity())
			actual[changed.identity()] = changed
			if len(authorityDebtRatchet(actual, actual, old, old)) == 0 {
				t.Fatal("changed raw DB type admitted")
			}
		})
	}
	trusted, actual := maps.Clone(old), maps.Clone(current)
	prior, escape := debtControlSite("call:QueryRow", 3), debtControlSite("call:Exec", 1)
	trusted[prior.identity()], actual[escape.identity()] = prior, escape
	if authorityDebtCount(actual) >= authorityDebtCount(trusted) || len(authorityDebtRatchet(actual, actual, trusted, trusted)) == 0 {
		t.Fatal("seven transfers paid for an unrelated escape")
	}
	for _, pair := range debtWriterStageSignatureTransitions() {
		if old[pair[0].identity()] != pair[0] || old[pair[1].identity()].Multiplicity != 0 {
			t.Fatal("transition mutated original maps")
		}
	}
}

func TestPersistenceAuthorityDebtWriterStageModelMatchesReviewedManifestAndBodies(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, ".github/audit-artifacts/issue-2542-writer-stage-signature-transition-request.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct {
		File, Enclosing, Operation, Kind, Family, Owner, Before, After string
		Multiplicity                                                   int
	}
	if err := json.Unmarshal(raw, &rows); err != nil || len(rows) != 7 {
		t.Fatal("reviewed manifest is not seven exact pairs:", err)
	}
	for i, pair := range debtWriterStageSignatureTransitions() {
		row := rows[i]
		want := authorityDebtSite{row.Kind, row.File, row.Enclosing, row.Operation, row.Before, row.Family, row.Owner, row.Multiplicity}
		current := want
		current.Resolved = row.After
		if pair != [2]authorityDebtSite{want, current} {
			t.Fatal("permission model exceeds or alters independently approved pair")
		}
	}
	digest, err := debtCollectorDigest(root)
	if err != nil || digest != debtActiveCollectorTo {
		t.Fatalf("reviewed exact current successor=%s pinned=%s error=%v", digest, debtActiveCollectorTo, err)
	}
}

func TestPersistenceAuthorityDebtWriterStageSourcePinsRejectBodyAndManifestTampering(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	sourcePath := "internal/serveapp/main_runtime_test.go"
	manifestPath := ".github/audit-artifacts/issue-2542-writer-stage-signature-transition-request.json"
	raw, err := os.ReadFile(filepath.Join(root, "internal/store/testdata/persistence_authority_debt_writer_stage_source_pins.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(raw, &rows); err != nil || len(rows) != 11 {
		t.Fatal("immutable eleven-body source evidence changed:", err)
	}
	var before strings.Builder
	before.WriteString("package serveapp\n")
	for _, row := range rows {
		before.WriteString(row.Before + "\n")
	}
	manifest, err := os.ReadFile(filepath.Join(root, manifestPath))
	if err != nil {
		t.Fatal(err)
	}
	copy := debtServedDebugSourceCopy(t)
	source, err := os.ReadFile(filepath.Join(copy, sourcePath))
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, manifestPath, string(manifest))
	if err := debtWriterStageSourcePins(copy, true); err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, sourcePath, before.String())
	if err := debtWriterStageSourcePins(copy, false); err != nil {
		t.Fatal("pinned predecessor evidence rejected:", err)
	}
	debtWriteModuleSource(t, copy, sourcePath, string(source))
	for _, pin := range debtWriterStageBodyPins() {
		needle := "func " + pin[0] + "("
		start := strings.Index(string(source), needle)
		if start < 0 {
			t.Fatal("missing source-pinned declaration")
		}
		end := start + strings.Index(string(source[start:]), "{\n") + 2
		changed := string(source[:end]) + "unreviewedRawArgumentChange();\n" + string(source[end:])
		debtWriteModuleSource(t, copy, sourcePath, changed)
		if err := debtWriterStageSourcePins(copy, true); err == nil {
			t.Fatalf("changed caller/callee authority/value/capture passed: %s", pin[0])
		}
	}
	debtWriteModuleSource(t, copy, sourcePath, string(source))
	debtWriteModuleSource(t, copy, manifestPath, string(manifest)+" ")
	if err := debtWriterStageSourcePins(copy, true); err == nil {
		t.Fatal("changed approved manifest admitted")
	}
}

func TestPersistenceAuthorityDebtWriterStageCollectorTransferAndPriorCompositionAreExact(t *testing.T) {
	old, current := debtWriterStageTransitionStates()
	legacy := debtControlSite("call:QueryRow", 3)
	old[legacy.identity()], current[legacy.identity()] = legacy, legacy
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtStageCompletionCollectorTo, Sites: old}
	landed := trusted
	landed.Collector, landed.Sites = debtWriterStageCollectorTo, current
	for _, test := range []struct {
		name   string
		sites  map[string]authorityDebtSite
		accept bool
	}{
		{"unrefreshed", old, true},
		{"forward", current, true},
		{"genuine-other-removal", func() map[string]authorityDebtSite { _, sites := debtWriterStageTransitionStates(); return sites }(), true},
		{"seven-units-lost", debtControlSet(legacy), false},
		{"unrelated-new-despite-lower-total", debtControlSet(debtControlSite("call:Exec", 1)), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			head := landed
			head.Sites = test.sites
			if err := debtValidateCollectorIdentity(trusted.Collector, head.Collector, trusted, head, ""); (err == nil) != test.accept {
				t.Fatalf("accepted=%v error=%v", test.accept, err)
			}
		})
	}
	if err := debtValidateCollectorIdentity(landed.Collector, landed.Collector, landed, landed, ""); err != nil {
		t.Fatal("landed no-op refused:", err)
	}
	if err := debtValidateCollectorIdentity(landed.Collector, trusted.Collector, landed, trusted, ""); err == nil {
		t.Fatal("reverse collector transfer admitted")
	}
	for _, field := range []string{"base", "destination", "metadata"} {
		base, target, head := trusted.Collector, landed.Collector, landed
		switch field {
		case "base":
			base = strings.Repeat("b", 64)
		case "destination":
			target = strings.Repeat("c", 64)
		case "metadata":
			head.Collector = trusted.Collector
		}
		if debtValidateCollectorIdentity(base, target, trusted, head, "") == nil {
			t.Fatalf("foreign or unrotated collector admitted: %s", field)
		}
	}
	debtWriterStageCheckPriorComposition(t)
}

func debtWriterStageCheckPriorComposition(t *testing.T) {
	t.Helper()
	old, current := debtWriterStageTransitionStates()
	before, after := debtStageCompletionTransition()
	old[before.identity()], current[after.identity()] = before, after
	for _, pair := range debtRunFixtureSignatureTransitions() {
		old[pair[0].identity()], current[pair[1].identity()] = pair[0], pair[1]
	}
	quiescenceOld, quiescenceNew := debtQuiescenceUncertaintyTransition()
	old[quiescenceOld.identity()], current[quiescenceNew.identity()] = quiescenceOld, quiescenceNew
	if failures := authorityDebtRatchet(current, current, old, old); len(failures) != 0 {
		t.Fatal("reviewed exact transition composition rejected:", failures)
	}
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtFixtureRoleCollectorTo, Sites: old}
	head := trusted
	head.Collector, head.Sites = debtWriterStageCollectorTo, current
	if err := debtValidateCollectorIdentity(trusted.Collector, head.Collector, trusted, head, ""); err != nil {
		t.Fatal("reviewed predecessor collector composition rejected:", err)
	}
	delete(old, before.identity())
	if debtValidateCollectorIdentity(trusted.Collector, head.Collector, trusted, head, "") == nil {
		t.Fatal("missing preceding exact source admitted")
	}
}

func TestPersistenceAuthorityDebtWriterStageUnrefreshedMetadataIsScopedAndFailClosed(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	copy := debtCatchUpSourceCopyFrom(t, root)
	copyRepair2698CustodySources(t, root, copy)
	for _, path := range []string{"internal/serveapp/main_runtime_test.go", ".github/audit-artifacts/issue-2542-writer-stage-signature-transition-request.json", "tools/fixture-codemod/pipeline-observations/family145-served-debug-preimages.json", "tools/fixture-codemod/pipeline-observations/family145-request-key-preimages.json", "tools/fixture-codemod/pipeline-observations/family145-run-scalar-preimages.json", "tools/fixture-codemod/pipeline-observations/family145-delivery-status-preimages.json", "tools/fixture-codemod/pipeline-observations/family145-event-name-preimages.json"} {
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(data))
	}
	old, _ := debtWriterStageTransitionStates()
	baseline := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtCacheCollectorFrom, Sites: old}
	data := marshalAuthorityDebtBaseline(baseline)
	metadata, err := debtCollectorMetadataBytes(debtCollectorMetadata{Version: 1, Collector: debtStageCompletionCollectorTo, BaselineSHA256: debtBaselineChecksum(data), Predecessor: debtCacheCollectorFrom})
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata))
	t.Setenv(debtRefreshEnv, "downward")
	t.Setenv("CI", "")
	t.Setenv("GITHUB_ACTIONS", "")
	got, err := debtBaselineWithCollector(copy, data, debtWriterStageCollectorTo, false)
	if err != nil || got.Collector != debtWriterStageCollectorTo || !debtSitesEqual(got.Sites, old) {
		t.Fatal("bounded metadata rotation changed debt or refused:", err)
	}
	t.Setenv(debtRefreshEnv, "")
	if _, err := debtBaselineWithCollector(copy, data, debtWriterStageCollectorTo, false); err == nil {
		t.Fatal("normal read-only run accepted unrefreshed metadata")
	}
	t.Setenv(debtRefreshEnv, "downward")
	t.Setenv("CI", "true")
	if _, err := debtBaselineWithCollector(copy, data, debtWriterStageCollectorTo, false); err == nil {
		t.Fatal("CI accepted local-only metadata rotation")
	}
	t.Setenv("CI", "")
	if _, err := debtBaselineWithCollector(copy, data, debtWriterStageCollectorTo, true); err == nil {
		t.Fatal("historical source accepted forward metadata")
	}
	debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata)+" ")
	if _, err := debtBaselineWithCollector(copy, data, debtWriterStageCollectorTo, false); err == nil {
		t.Fatal("noncanonical metadata admitted")
	}
	debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata))
	for _, pair := range debtWriterStageSignatureTransitions() {
		missing := baseline
		missing.Sites = maps.Clone(old)
		delete(missing.Sites, pair[0].identity())
		changed := marshalAuthorityDebtBaseline(missing)
		matching, err := debtCollectorMetadataBytes(debtCollectorMetadata{Version: 1, Collector: debtStageCompletionCollectorTo, BaselineSHA256: debtBaselineChecksum(changed), Predecessor: debtCacheCollectorFrom})
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(matching))
		if _, err := debtBaselineWithCollector(copy, changed, debtWriterStageCollectorTo, false); err == nil {
			t.Fatal("missing donor admitted by metadata rotation")
		}
	}
}
