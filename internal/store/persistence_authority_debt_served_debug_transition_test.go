package store_test

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const debtServedDebugCollectorTo = "95de50b780ae728dd0cbc9517df19970a2cdc7c7e72e0a13aa6c06e3344ce22c"

func debtServedDebugTransitionStates() (map[string]authorityDebtSite, map[string]authorityDebtSite) {
	old, current := debtControlSet(), debtControlSet()
	for _, pair := range debtServedDebugSignatureTransitions() {
		old[pair[0].identity()], current[pair[1].identity()] = pair[0], pair[1]
	}
	return old, current
}

func TestPersistenceAuthorityDebtServedDebugTransfersConserveAll71Occurrences(t *testing.T) {
	old, current := debtServedDebugTransitionStates()
	if len(old) != 49 || len(current) != 49 || authorityDebtCount(old) != 71 || authorityDebtCount(current) != 71 {
		t.Fatal("exact49 pairs must conserve71 counted raw occurrences")
	}
	for _, state := range []struct {
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
		{"missing-head", current, debtControlSet(), old, old, false},
		{"missing-source", current, current, old, debtControlSet(), false},
		{"later-genuine-deletion", debtControlSet(), debtControlSet(), current, current, true},
	} {
		t.Run(state.name, func(t *testing.T) {
			if failures := authorityDebtRatchet(state.actual, state.head, state.base, state.source); (len(failures) == 0) != state.accept {
				t.Fatalf("accepted=%v failures=%v", state.accept, failures)
			}
		})
	}
	for _, pair := range debtServedDebugSignatureTransitions() {
		t.Run(pair[0].Declaration+"/"+pair[0].Operation, func(t *testing.T) {
			debtRunFixtureSignatureHostileControls(t, pair, old, current)
			changed := pair[1]
			changed.Resolved = strings.Replace(changed.Resolved, "|arg=*database/sql.DB|", "|arg=*database/sql.Tx|", 1)
			actual := maps.Clone(current)
			delete(actual, pair[1].identity())
			actual[changed.identity()] = changed
			if len(authorityDebtRatchet(actual, actual, old, old)) == 0 {
				t.Fatal("changed raw carrier type admitted")
			}
		})
	}
	trusted, actual := maps.Clone(old), maps.Clone(current)
	donor, escape := debtControlSite("call:QueryRow", 3), debtControlSite("call:Exec", 1)
	trusted[donor.identity()], actual[escape.identity()] = donor, escape
	if authorityDebtCount(actual) >= authorityDebtCount(trusted) || len(authorityDebtRatchet(actual, actual, trusted, trusted)) == 0 {
		t.Fatal("deleting unrelated debt paid for a new escape")
	}
}

func TestPersistenceAuthorityDebtServedDebugModelMatchesExactReviewedManifest(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, ".github/audit-artifacts/issue-2542-served-debug-signature-transition-request.json"))
	if err != nil || debtBaselineChecksum(raw) != "bcf9e019cc0ecbd0eccd28c97689074e509b94372ece4d8f93c6c77f790be018" {
		t.Fatalf("reviewed request changed: %v", err)
	}
	var rows []struct {
		File, Enclosing, Operation, Kind, Family, Owner, Before, After string
		Multiplicity                                                   int
	}
	if err := json.Unmarshal(raw, &rows); err != nil || len(rows) != 49 {
		t.Fatal("invalid exact49-pair request:", err)
	}
	for index, pair := range debtServedDebugSignatureTransitions() {
		row := rows[index]
		old := authorityDebtSite{row.Kind, row.File, row.Enclosing, row.Operation, row.Before, row.Family, row.Owner, row.Multiplicity}
		current := old
		current.Resolved = row.After
		if pair != [2]authorityDebtSite{old, current} {
			t.Fatal("policy exceeded its independently approved request")
		}
	}
	if err := debtServedDebugSourcePins(root); err != nil {
		t.Fatal(err)
	}
	digest, err := debtCollectorDigest(root)
	if err != nil || digest != debtActiveCollectorTo {
		t.Fatalf("actual current source=%s pinned=%s err=%v", digest, debtActiveCollectorTo, err)
	}
}

func debtServedDebugSourceCopy(t *testing.T) string {
	t.Helper()
	root := persistenceAuthorityRepoRoot(t)
	copy := debtCatchUpSourceCopyFrom(t, root)
	copyRepair2698CustodySources(t, root, copy)
	return copy
}

func TestPersistenceAuthorityDebtServedDebugSourcePinsRejectProvenanceAndNearMatches(t *testing.T) {
	copy := debtServedDebugSourceCopy(t)
	if err := debtServedDebugSourcePins(copy); err != nil {
		t.Fatal(err)
	}
	bodies, err := debtServedDebugBodies(copy)
	if err != nil {
		t.Fatal(err)
	}
	needed := debtServedDebugRequiredSources()
	for _, body := range bodies {
		if !needed[body.File+"/"+body.Function] && !needed["callee/"+body.Function] && !needed["capture/"+body.Function] {
			continue
		}
		t.Run(body.Function, func(t *testing.T) {
			raw, current := debtServedDebugCurrentSource(t, copy, body)
			changed := strings.Replace(current, "{\n", "{\nforeignReaderProvenance();\n", 1)
			if changed == current {
				t.Fatal("hostile declaration mutation is absent")
			}
			debtSourceBlobCopyDeclaration(t, copy, body.File, body.Function, changed)
			if err := debtServedDebugSourcePins(copy); err == nil {
				t.Fatal("changed complete reader provenance/argument/body admitted")
			}
			debtWriteModuleSource(t, copy, body.File, string(raw))
			for _, role := range []string{"rt.ReceiverStateReader", "p.deps.EventStore", "reader"} {
				if !strings.Contains(current, role) {
					continue
				}
				changed := strings.Replace(current, role, "foreignSameTypedReader", 1)
				debtSourceBlobCopyDeclaration(t, copy, body.File, body.Function, changed)
				if err := debtServedDebugSourcePins(copy); err == nil {
					t.Fatal("changed same-typed inspection provenance admitted")
				}
				debtWriteModuleSource(t, copy, body.File, string(raw))
			}
		})
	}
	for _, path := range []string{".github/audit-artifacts/issue-2542-served-debug-signature-transition-request.json", "tools/fixture-codemod/pipeline-observations/family145-served-debug-preimages.json"} {
		raw, err := os.ReadFile(filepath.Join(copy, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(raw)+" ")
		if err := debtServedDebugSourcePins(copy); err == nil {
			t.Fatal("changed immutable source/request evidence admitted")
		}
		debtWriteModuleSource(t, copy, path, string(raw))
	}
}

func debtServedDebugCurrentSource(t *testing.T, root string, body debtServedDebugBody) ([]byte, string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, body.File))
	if err != nil {
		t.Fatal(err)
	}
	decls, err := debtServedDebugDeclarations(filepath.Join(root, body.File))
	if err != nil {
		t.Fatal(err)
	}
	current, found := decls[body.Function]
	if !found || current == "" {
		t.Fatal("hostile source requires its unique parsed current declaration")
	}
	return raw, current
}

func TestPersistenceAuthorityDebtServedDebugFourWriterStageCompositionsRemainExact(t *testing.T) {
	copy := debtServedDebugSourceCopy(t)
	if err := debtWriterStageSourcePins(copy, true); err != nil {
		t.Fatal("four exact successors failed original eleven pins:", err)
	}
	bodies, err := debtServedDebugBodies(copy)
	if err != nil {
		t.Fatal(err)
	}
	composed := 0
	for _, body := range bodies {
		previous, err := debtServedDebugWriterStagePrevious(copy, body.Function, body.After)
		if err != nil {
			t.Fatal(err)
		}
		if previous == body.After {
			continue
		}
		composed++
		if previous != body.Before {
			t.Fatal("composition did not preserve the exact reviewed intermediate")
		}
		changed := strings.Replace(body.After, "{\n", "{\nforeignSameTypedReader();\n", 1)
		if _, err := debtServedDebugWriterStagePrevious(copy, body.Function, changed); err == nil {
			t.Fatal("writer-stage successor near-match admitted")
		}
	}
	if composed != 4 {
		t.Fatalf("composed%d bodies, want exactly four", composed)
	}
}

func TestPersistenceAuthorityDebtServedDebugCollectorComposesOnlyExactPredecessors(t *testing.T) {
	old, current := debtServedDebugTransitionStates()
	trusted := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtWriterStageCollectorTo, Sites: old}
	head := trusted
	head.Collector, head.Sites = debtServedDebugCollectorTo, current
	if err := debtValidateCollectorIdentity(trusted.Collector, head.Collector, trusted, head, ""); err != nil {
		t.Fatal(err)
	}
	for _, sites := range []map[string]authorityDebtSite{old, current} {
		head.Sites = sites
		if err := debtValidateCollectorIdentity(trusted.Collector, head.Collector, trusted, head, ""); err != nil {
			t.Fatal("exact unrefreshed/refreshed transfer refused:", err)
		}
	}
	for _, pair := range debtServedDebugSignatureTransitions() {
		missing := head
		missing.Sites = maps.Clone(current)
		delete(missing.Sites, pair[1].identity())
		if debtValidateCollectorIdentity(trusted.Collector, head.Collector, trusted, missing, "") == nil {
			t.Fatal("collector change erased retained raw occurrences")
		}
	}
	if debtValidateCollectorIdentity(head.Collector, trusted.Collector, head, trusted, "") == nil {
		t.Fatal("reverse collector transfer admitted")
	}
	for _, pair := range debtWriterStageSignatureTransitions() {
		old[pair[0].identity()], current[pair[1].identity()] = pair[0], pair[1]
	}
	before, after := debtStageCompletionTransition()
	old[before.identity()], current[after.identity()] = before, after
	for _, pair := range debtRunFixtureSignatureTransitions() {
		old[pair[0].identity()], current[pair[1].identity()] = pair[0], pair[1]
	}
	qBefore, qAfter := debtQuiescenceUncertaintyTransition()
	old[qBefore.identity()], current[qAfter.identity()] = qBefore, qAfter
	trusted.Collector, trusted.Sites = debtFixtureRoleCollectorTo, old
	head.Sites = current
	if err := debtValidateCollectorIdentity(trusted.Collector, head.Collector, trusted, head, ""); err != nil {
		t.Fatal("approved prior exact chain refused:", err)
	}
	delete(old, before.identity())
	if debtValidateCollectorIdentity(trusted.Collector, head.Collector, trusted, head, "") == nil {
		t.Fatal("missing earlier chain donor admitted")
	}
}

func TestPersistenceAuthorityDebtServedDebugUnrefreshedMetadataRemainsLocalAndReadOnly(t *testing.T) {
	copy := debtServedDebugSourceCopy(t)
	old, _ := debtServedDebugTransitionStates()
	baseline := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtCacheCollectorFrom, Sites: old}
	data := marshalAuthorityDebtBaseline(baseline)
	metadata, err := debtCollectorMetadataBytes(debtCollectorMetadata{Version: 1, Collector: debtWriterStageCollectorTo, BaselineSHA256: debtBaselineChecksum(data), Predecessor: debtCacheCollectorFrom})
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata))
	t.Setenv(debtRefreshEnv, "downward")
	t.Setenv("CI", "")
	t.Setenv("GITHUB_ACTIONS", "")
	got, err := debtBaselineWithCollector(copy, data, debtServedDebugCollectorTo, false)
	if err != nil || got.Collector != debtServedDebugCollectorTo || !debtSitesEqual(got.Sites, old) {
		t.Fatal("local rotation changed raw debt or refused exact source:", err)
	}
	for _, mode := range []string{"read-only", "CI", "GITHUB_ACTIONS", "historical", "malformed-metadata"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "read-only" {
				t.Setenv(debtRefreshEnv, "")
			}
			if mode == "CI" || mode == "GITHUB_ACTIONS" {
				t.Setenv(mode, "true")
			}
			if mode == "malformed-metadata" {
				debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata)+" ")
				defer debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(metadata))
			}
			if _, err := debtBaselineWithCollector(copy, data, debtServedDebugCollectorTo, mode == "historical"); err == nil {
				t.Fatal("unrefreshed metadata escaped its exact local transition")
			}
		})
	}
	for _, pair := range debtServedDebugSignatureTransitions() {
		missing := baseline
		missing.Sites = maps.Clone(old)
		delete(missing.Sites, pair[0].identity())
		changed := marshalAuthorityDebtBaseline(missing)
		matching, err := debtCollectorMetadataBytes(debtCollectorMetadata{Version: 1, Collector: debtWriterStageCollectorTo, BaselineSHA256: debtBaselineChecksum(changed), Predecessor: debtCacheCollectorFrom})
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, debtCollectorSidecarPath, string(matching))
		if _, err := debtBaselineWithCollector(copy, changed, debtServedDebugCollectorTo, false); err == nil {
			t.Fatal("missing exact donor admitted during metadata rotation")
		}
	}
}
