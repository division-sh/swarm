package store_test

import (
	"archive/tar"
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

const debtBaselinePath = "internal/store/testdata/persistence_authority_debt_baseline.tsv"
const debtRefreshEnv = "SWARM_REFRESH_PERSISTENCE_AUTHORITY_DEBT"
const debtG01CollectorFrom = "494fd3b6300c4163241395ef9e3aa59ce58eb32f45e9f5d8bc5a5078401303d5"
const debtQuiescenceCollectorFrom = "65fe2e66c01eb0e19ce2b44d53f545cc76dd823f5f8b58562e139a362cc9c267"
const debtRunFixtureCollectorFrom = "4ab44a86253d3b09474c20463ca1a4e5c6296f4a4694e4d80d904832757ae047"

// The reviewed, unlanded bootstrap cannot move when its guard PR is rebased.
const debtBootstrapSource = "52b954ec26c85a47605cefb8d7030f2a842f8c24"
const debtFixtureRoleCollectorFrom = "dd36814075a0641f1085eb8fe8d74ae1cd61d8fba3b6047a8514b175ddbfa53e"

type authorityDebtSite struct {
	Kind, File, Declaration, Operation, Resolved, Family, Replacement string
	Multiplicity                                                      int
}

func (site authorityDebtSite) identity() string {
	return strings.Join([]string{site.Kind, site.File, site.Declaration, site.Operation, site.Resolved, site.Family, site.Replacement}, "\t")
}

type authorityDebtBaseline struct {
	BootstrapSource, Collector string
	Sites                      map[string]authorityDebtSite
}

func authorityDebtOperation(member string) string {
	if cut := strings.LastIndex(member, "#"); cut >= 0 {
		if _, err := strconv.Atoi(member[cut+1:]); err == nil {
			return member[:cut]
		}
	}
	return member
}

func authorityDebtRoute(finding authorityFinding) (string, string) {
	path, operation := finding.File, finding.Member
	if finding.Kind == "unresolved-excluded-source" {
		return "excluded-source-uncertainty", "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase"
	}
	if finding.Kind == "selected-boundary" {
		return "selected-construction", "selected construction/work-lifetime owner; do not recover a concrete store"
	}
	if finding.Kind == "forbidden-test-consumption" || finding.Kind == "inactive-test-consumption" {
		return "fixture-confinement", "named private evidence/fault operation; no generic fixture import"
	}
	if strings.Contains(operation, "Database") || strings.HasPrefix(finding.Kind, "context-") || strings.Contains(finding.Kind, "construction") || strings.Contains(finding.Kind, "export") {
		return "authority-carrier", "original selected construction/work-lifetime owner; no getter or transaction-context recovery"
	}
	if strings.Contains(path, "pipeline") {
		return "workflow", "selected workflow/construction owner; domain observation port for readback"
	}
	if strings.Contains(path, "runfork") || strings.Contains(path, "run_fork") {
		return "fork", "selected fork/source/event/lifecycle operation and bounded fork observation"
	}
	if strings.Contains(path, "mailbox") {
		return "mailbox", "selected mailbox/completion owner and named mailbox fault/readback"
	}
	if strings.Contains(path, "event") || strings.Contains(operation, "Event") {
		return "event", "selected canonical event operation and domain event observation port"
	}
	if strings.Contains(path, "run") || strings.Contains(operation, "Run") {
		return "lifecycle", "selected run lifecycle/candidate/source owner and bounded lifecycle observation"
	}
	if strings.Contains(path, "channel") {
		return "channel", "selected channel/delivery owner and named channel fault/readback"
	}
	if strings.Contains(operation, "Query") || strings.Contains(operation, "Scan") || strings.Contains(operation, "Next") {
		return "observation", "domain observation port on the exact selected owner; missing exact port: escalate #2542"
	}
	return "unowned-authority", "exact domain owner not identified here; escalate #2542, never invent a SQL escape"
}

func authorityDebtSites(findings []authorityFinding) map[string]authorityDebtSite {
	sites := map[string]authorityDebtSite{}
	for _, finding := range findings {
		if finding.Kind != "unresolved-excluded-source" && finding.Kind != "selected-boundary" && finding.Kind != "selected-store-construction" && finding.Kind != "forbidden-test-consumption" && finding.Kind != "inactive-test-consumption" && (!finding.RawSQL || debtRawSQLDispositionAllowed(finding, "fixture-2151")) {
			continue
		}
		family, replacement := authorityDebtRoute(finding)
		site := authorityDebtSite{Kind: finding.Kind, File: finding.File, Declaration: finding.Enclosing, Operation: authorityDebtOperation(finding.Member), Resolved: finding.Resolved, Family: family, Replacement: replacement, Multiplicity: 1}
		key := site.identity()
		if prior, ok := sites[key]; ok {
			site.Multiplicity = prior.Multiplicity + 1
		}
		sites[key] = site
	}
	return sites
}

func authorityDebtCount(sites map[string]authorityDebtSite) int {
	total := 0
	for _, site := range sites {
		total += site.Multiplicity
	}
	return total
}

func authorityDebtSubset(actual, allowed map[string]authorityDebtSite, label string) []string {
	var failures []string
	for key, site := range actual {
		limit := allowed[key].Multiplicity
		if site.Multiplicity > limit {
			failures = append(failures, fmt.Sprintf("%s new/increased site: %s declaration=%s operation=%s kind=%s family=%s old=%d new=%d owner=%s", label, site.File, site.Declaration, site.Operation, site.Kind, site.Family, limit, site.Multiplicity, site.Replacement))
		}
	}
	sort.Strings(failures)
	return failures
}

type debtDeliveryRetirementProof struct {
	Root   string
	Actual map[string]authorityDebtSite
}

// Ruling6147242761 authorizes only these seven existing historical pairs.
func debtDeliveryRetirementPairs() [7][2]authorityDebtSite {
	declarations := map[string]int{
		"TestRunServeRuntimeJoinFailureReachesAPIAndCLI":                           2,
		"TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation": 3,
		"publishServedLiveAgentHoldEvent":                                          1,
		"runServedAgentRestartLifecycleProof":                                      2,
		"runServedLiveAgentEventReplayLifecycleProof":                              2,
		"runServedRunControlLifecycleProof":                                        2,
		"startServedSessionCleanupProof":                                           1,
	}
	var result [7][2]authorityDebtSite
	index := 0
	for _, pair := range debtServedDebugSignatureTransitions() {
		old := pair[0]
		if n := declarations[old.Declaration]; n != 0 && old.File == "internal/serveapp/main_runtime_test.go" &&
			old.Operation == "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishDeliveryStatusCountForRun" && old.Multiplicity == n && pair[1].Multiplicity == n {
			if index == len(result) {
				panic("duplicate delivery retirement identity")
			}
			result[index] = pair
			index++
			delete(declarations, old.Declaration)
		}
	}
	if index != len(result) || len(declarations) != 0 {
		panic("exact seven delivery retirement pairs changed")
	}
	return result
}

func debtDeliveryRetirementComparisons(actual, head, base, source map[string]authorityDebtSite, proofs ...debtDeliveryRetirementProof) (map[string]authorityDebtSite, map[string]authorityDebtSite, map[string]authorityDebtSite, []string) {
	if len(proofs) == 0 {
		return head, base, source, nil // Historical pre-retirement comparisons are unchanged.
	}
	if len(proofs) != 1 || proofs[0].Root == "" || !debtSitesEqual(actual, proofs[0].Actual) {
		return head, base, source, []string{"delivery retirement lacks its exact source/census proof"}
	}
	pairs := debtDeliveryRetirementPairs()
	if debtDeliveryRetirementAllAbsent(actual, head, base, source) {
		return head, base, source, nil // Landed deletion needs no fictitious donor forever.
	}
	if err := debtDeliveryStatusSourcePins(proofs[0].Root); err != nil {
		return head, base, source, []string{err.Error()}
	}
	for _, pair := range pairs {
		if err := debtDeliveryRetirementState(pair, actual, head, base, source); err != nil {
			return head, base, source, []string{err.Error()}
		}
		old, current := pair[0], pair[1]
		if base[old.identity()] == old {
			// Comparison copies only. The census, baseline and actual deletion stay intact.
			base = debtForwardQuiescenceUncertainty(base, old, current)
			source = debtForwardQuiescenceUncertainty(source, old, current)
		}
	}
	return head, base, source, nil
}

func debtDeliveryRetirementAllAbsent(states ...map[string]authorityDebtSite) bool {
	for _, pair := range debtDeliveryRetirementPairs() {
		if !debtDeliveryRetirementPairAbsent(pair, states...) {
			return false
		}
	}
	return true
}

func debtDeliveryRetirementPairAbsent(pair [2]authorityDebtSite, states ...map[string]authorityDebtSite) bool {
	for _, state := range states {
		if state[pair[0].identity()].Multiplicity != 0 || state[pair[1].identity()].Multiplicity != 0 {
			return false
		}
	}
	return true
}

func debtDeliveryRetirementState(pair [2]authorityDebtSite, actual, head, base, source map[string]authorityDebtSite) error {
	old, current := pair[0], pair[1]
	if actual[old.identity()].Multiplicity != 0 || actual[current.identity()].Multiplicity != 0 ||
		head[old.identity()].Multiplicity != 0 || head[current.identity()].Multiplicity != 0 && head[current.identity()] != current {
		return fmt.Errorf("delivery retirement reintroduced raw authority: %s", old.Declaration)
	}
	if debtDeliveryRetirementPairAbsent(pair, actual, head, base, source) {
		return nil
	}
	if base[old.identity()] == old && source[old.identity()] == old &&
		base[current.identity()].Multiplicity == 0 && source[current.identity()].Multiplicity == 0 {
		return nil
	}
	if base[current.identity()] == current && source[current.identity()] == current &&
		base[old.identity()].Multiplicity == 0 && source[old.identity()].Multiplicity == 0 {
		return nil
	}
	return fmt.Errorf("delivery retirement lacks exact historical donor: %s", old.Declaration)
}

func debtDeliveryRetirementCollectorTransition(base, current string, trusted, head authorityDebtBaseline, proofs ...debtDeliveryRetirementProof) bool {
	if current != debtDeliveryRetirementCollectorTo || head.Collector != current || trusted.Collector != base ||
		trusted.BootstrapSource != head.BootstrapSource || len(proofs) != 1 {
		return false
	}
	if len(authorityDebtRatchet(proofs[0].Actual, head.Sites, trusted.Sites, trusted.Sites, proofs...)) != 0 {
		return false
	}
	if base == debtDeliveryStatusCollectorTo || base == debtRunScalarCollectorTo {
		return true
	}
	// Validate the old fingerprint chain against its exact pre-retirement receipt.
	// This is a metadata comparison copy, never an invented actual census site.
	intermediate := head
	intermediate.Collector, intermediate.Sites = debtDeliveryStatusCollectorTo, maps.Clone(head.Sites)
	for _, pair := range debtDeliveryRetirementPairs() {
		if intermediate.Sites[pair[1].identity()].Multiplicity == 0 && trusted.Sites[pair[0].identity()] == pair[0] {
			intermediate.Sites[pair[1].identity()] = pair[1]
		}
	}
	return debtDeliveryStatusCollectorTransition(base, intermediate.Collector, trusted, intermediate)
}

func authorityDebtRatchet(actual, head, base, baseActual map[string]authorityDebtSite, retirement ...debtDeliveryRetirementProof) []string {
	var retirementFailures []string
	head, base, baseActual, retirementFailures = debtDeliveryRetirementComparisons(actual, head, base, baseActual, retirement...)
	if len(retirementFailures) != 0 {
		return retirementFailures
	}
	head, base, baseActual, retirementFailures = debtPublicRollbackRetirementComparisons(actual, head, base, baseActual, retirement...)
	if len(retirementFailures) != 0 {
		return retirementFailures
	}
	head, base, baseActual, failures := debtStageCompletionComparisons(actual, head, base, baseActual)
	if len(failures) != 0 {
		return failures
	}
	head, base, baseActual, failures = debtRunFixtureSignatureComparisons(actual, head, base, baseActual)
	if len(failures) != 0 {
		return failures
	}
	head, base, baseActual, failures = debtWriterStageSignatureComparisons(actual, head, base, baseActual)
	if len(failures) != 0 {
		return failures
	}
	head, base, baseActual, failures = debtServedDebugSignatureComparisons(actual, head, base, baseActual)
	if len(failures) != 0 {
		return failures
	}
	head, base, baseActual, failures = debtPublicA2EndpointComparisons(actual, head, base, baseActual)
	if len(failures) != 0 {
		return failures
	}
	head, base, baseActual, failures = debtRequestKeySignatureComparisons(actual, head, base, baseActual)
	if len(failures) != 0 {
		return failures
	}
	old, current := debtQuiescenceUncertaintyTransition()
	for _, state := range []map[string]authorityDebtSite{actual, head, base, baseActual} {
		if state[old.identity()].Multiplicity > 0 && state[current.identity()].Multiplicity > 0 {
			return []string{"both reviewed uncertainty identities survive; owner=exact served delivery quiescence reader"}
		}
	}
	// This exact forward substitution is inert once the trusted source lands it.
	if actual[current.identity()] == current && actual[old.identity()].Multiplicity == 0 &&
		base[old.identity()] == old && base[current.identity()].Multiplicity == 0 &&
		baseActual[old.identity()] == old && baseActual[current.identity()].Multiplicity == 0 {
		head = debtForwardQuiescenceUncertainty(head, old, current)
		base = debtForwardQuiescenceUncertainty(base, old, current)
		baseActual = debtForwardQuiescenceUncertainty(baseActual, old, current)
	}
	failures = authorityDebtSubset(actual, head, "actual -> head baseline")
	failures = append(failures, authorityDebtSubset(head, base, "head -> trusted baseline")...)
	failures = append(failures, authorityDebtSubset(actual, baseActual, "actual -> trusted source (resurrection)")...)
	return failures
}

// Ruling6158639937 permits retirement only of the two already-recorded raw
// rollback caller identities. These are comparison copies, never new debt rows.
func debtPublicRollbackRetirementComparisons(actual, head, base, source map[string]authorityDebtSite, proofs ...debtDeliveryRetirementProof) (map[string]authorityDebtSite, map[string]authorityDebtSite, map[string]authorityDebtSite, []string) {
	if !debtPublicRollbackHasRetirement(actual, head, base, source) {
		return head, base, source, nil
	}
	if len(proofs) != 1 || proofs[0].Root == "" || !debtSitesEqual(actual, proofs[0].Actual) {
		return head, base, source, []string{"rollback retirement lacks its exact current source/census proof"}
	}
	if err := debtPublicRollbackNativePins(proofs[0].Root); err != nil {
		return head, base, source, []string{err.Error()}
	}
	if err := debtRequestKeySourcePins(proofs[0].Root); err != nil {
		return head, base, source, []string{err.Error()}
	}
	for _, pair := range debtRequestKeySignatureTransitions() {
		if err := debtDeliveryRetirementState(pair, actual, head, base, source); err != nil {
			return head, base, source, []string{err.Error()}
		}
		if base[pair[0].identity()] == pair[0] {
			base = debtForwardQuiescenceUncertainty(base, pair[0], pair[1])
			source = debtForwardQuiescenceUncertainty(source, pair[0], pair[1])
		}
	}
	return head, base, source, nil
}

func debtPublicRollbackHasRetirement(actual, head, base, source map[string]authorityDebtSite) bool {
	for _, pair := range debtRequestKeySignatureTransitions() {
		if debtDeliveryRetirementPairAbsent(pair, actual) && (base[pair[0].identity()].Multiplicity != 0 || source[pair[0].identity()].Multiplicity != 0) {
			return true
		}
	}
	return false
}

// Ruling 6112778624 authorizes one declaration relocation, not a debt removal.
func debtStageCompletionTransition() (authorityDebtSite, authorityDebtSite) {
	old := authorityDebtSite{
		Kind: "selected-store-construction", File: "internal/apiv1/operator_stage_completion_journey_test.go",
		Declaration: "stageCompletionRehydrate",
		Operation:   "construction:*github.com/division-sh/swarm/internal/store.SQLiteRuntimeStore",
		Resolved:    "*github.com/division-sh/swarm/internal/store.SQLiteRuntimeStore", Family: "authority-carrier",
		Replacement: "original selected construction/work-lifetime owner; no getter or transaction-context recovery", Multiplicity: 1,
	}
	current := old
	current.Declaration = "TestExactStageCompletionRealWriterAndRehydratedSourceBothStores"
	return old, current
}

// Ruling6144577451 retains both live raw calls, each 1 -> 1; no removal credit.
func debtRequestKeySignatureTransitions() [2][2]authorityDebtSite {
	const before = "func=func(t *testing.T, db *database/sql.DB, backend github.com/division-sh/swarm/internal/servedparity.Backend, idempotencyKey string, eventID string, runID string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|arg=string|arg=string|results=()"
	const after = "func=func(t *testing.T, db *database/sql.DB, backend github.com/division-sh/swarm/internal/servedparity.Backend, eventID string, runID string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|arg=string|results=()"
	var pairs [2][2]authorityDebtSite
	for i, declaration := range [2]string{"TestRootConnectedTemplatePublicationRollbackIsAtomicAcrossSelectedStores", "TestRootConnectedTemplatePublicationRollsBackAfterDeliveryAndCompletionAcrossSelectedStores"} {
		old := authorityDebtSite{Kind: "raw-operation", File: "internal/serveapp/test_command_runtime_test.go", Declaration: declaration,
			Operation: "call:github.com/division-sh/swarm/internal/serveapp.requirePublicInputRollbackNoResidue", Resolved: before,
			Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}
		current := old
		current.Resolved = after
		pairs[i] = [2]authorityDebtSite{old, current}
	}
	return pairs
}

func debtRequestKeySignatureComparisons(actual, head, base, source map[string]authorityDebtSite) (map[string]authorityDebtSite, map[string]authorityDebtSite, map[string]authorityDebtSite, []string) {
	for _, pair := range debtRequestKeySignatureTransitions() {
		old, current := pair[0], pair[1]
		for _, state := range []map[string]authorityDebtSite{actual, head, base, source} {
			if state[old.identity()].Multiplicity > 0 && state[current.identity()].Multiplicity > 0 {
				return head, base, source, []string{"both reviewed request-key raw-call identities survive: " + old.Declaration}
			}
		}
		if actual[current.identity()] == current && actual[old.identity()].Multiplicity == 0 &&
			base[old.identity()] == old && base[current.identity()].Multiplicity == 0 &&
			source[old.identity()] == old && source[current.identity()].Multiplicity == 0 {
			head = debtForwardQuiescenceUncertainty(head, old, current)
			base = debtForwardQuiescenceUncertainty(base, old, current)
			source = debtForwardQuiescenceUncertainty(source, old, current)
		}
	}
	return head, base, source, nil
}

type debtRequestKeyBody struct {
	File, Function, Before, After, SHA256 string
}

func debtRequestKeyBodies(root string) ([]debtRequestKeyBody, error) {
	raw, err := os.ReadFile(filepath.Join(root, "tools/fixture-codemod/pipeline-observations/family145-request-key-preimages.json"))
	if err != nil || debtBaselineChecksum(raw) != "cb7b37f1c09039dff3e2c4b92e9288e25ab6001ee1bbe65225396e91773a5e1c" {
		return nil, fmt.Errorf("exact request-key complete source evidence changed: %v", err)
	}
	var bodies []debtRequestKeyBody
	if err := json.Unmarshal(raw, &bodies); err != nil || len(bodies) != 28 {
		return nil, fmt.Errorf("invalid request-key complete source inventory: %v", err)
	}
	return bodies, nil
}

func debtRequestKeySourcePrevious(root, file, function, current string) (string, error) {
	var err error
	current, err = debtPublicRollbackSourcePrevious(root, file, function, current)
	if err != nil {
		return "", err
	}
	current, err = debtRunScalarSourcePrevious(root, file, function, current)
	if err != nil {
		return "", err
	}
	if !debtRequestKeySuccessorCandidate(current) {
		return current, nil // Earlier immutable fixtures do not require future evidence.
	}
	bodies, err := debtRequestKeyBodies(root)
	if err != nil {
		return "", err
	}
	for _, body := range bodies {
		if body.File == file && body.Function == function && current == body.After && debtBaselineChecksum([]byte(body.Before)) == body.SHA256 {
			return body.Before, nil
		}
	}
	return current, nil
}

func debtRunScalarBodies(root string) ([]debtRequestKeyBody, error) {
	raw, err := os.ReadFile(filepath.Join(root, "tools/fixture-codemod/pipeline-observations/family145-run-scalar-preimages.json"))
	if err != nil || debtBaselineChecksum(raw) != "441c152d124c3f218fe1577a3962b8b7bc153919bec0121ad89a48a904dfddca" {
		return nil, fmt.Errorf("exact run scalar source evidence changed: %v", err)
	}
	var bodies []debtRequestKeyBody
	if err := json.Unmarshal(raw, &bodies); err != nil || len(bodies) != 6 {
		return nil, fmt.Errorf("invalid run scalar complete source inventory: %v", err)
	}
	return bodies, nil
}

func debtRunScalarSourcePrevious(root, file, function, current string) (string, error) {
	var err error
	current, err = debtDeliveryStatusSourcePrevious(root, file, function, current)
	if err != nil {
		return "", err
	}
	if !strings.Contains(current, "servedEventPublishScalarCount(t, reader,") {
		return current, nil // Immutable earlier bodies do not require future manifests.
	}
	bodies, err := debtRunScalarBodies(root)
	if err != nil {
		return "", err
	}
	for _, body := range bodies {
		if body.File == file && body.Function == function && current == body.After && debtBaselineChecksum([]byte(body.Before)) == body.SHA256 {
			return body.Before, nil
		}
	}
	return current, nil
}

func debtRunScalarSourcePins(root string) error {
	bodies, err := debtRunScalarBodies(root)
	if err != nil {
		return err
	}
	for _, body := range bodies {
		declarations, err := debtServedDebugDeclarations(filepath.Join(root, body.File))
		if err != nil {
			return err
		}
		previous, err := debtDeliveryStatusSourcePrevious(root, body.File, body.Function, declarations[body.Function])
		if err != nil || previous != body.After || debtBaselineChecksum([]byte(body.Before)) != body.SHA256 {
			return fmt.Errorf("run scalar source owner/assertion/cut changed: %s/%s: %v", body.File, body.Function, err)
		}
	}
	return nil
}

func debtDeliveryStatusBodies(root string) ([]debtRequestKeyBody, error) {
	raw, err := os.ReadFile(filepath.Join(root, "tools/fixture-codemod/pipeline-observations/family145-delivery-status-preimages.json"))
	if err != nil || debtBaselineChecksum(raw) != "b4284bb05b933c6ac8bb151de9355e3f5bd28a6ff363b2f926d9da5a8b0042d4" {
		return nil, fmt.Errorf("exact delivery status source evidence changed: %v", err)
	}
	var bodies []debtRequestKeyBody
	if err := json.Unmarshal(raw, &bodies); err != nil || len(bodies) != 14 {
		return nil, fmt.Errorf("invalid delivery status complete source inventory: %v", err)
	}
	return bodies, nil
}

func debtDeliveryStatusSourcePrevious(root, file, function, current string) (string, error) {
	var err error
	current, err = debtSourceForkSourcePrevious(root, file, function, current)
	if err != nil {
		return "", err
	}
	current, err = debtEventNameSourcePrevious(root, file, function, current)
	if err != nil {
		return "", err
	}
	if !debtDeliveryStatusSuccessorCandidate(current) {
		return current, nil
	}
	bodies, err := debtDeliveryStatusBodies(root)
	if err != nil {
		return "", err
	}
	for _, body := range bodies {
		if body.File == file && body.Function == function && current == body.After && debtBaselineChecksum([]byte(body.Before)) == body.SHA256 {
			return body.Before, nil
		}
	}
	return current, nil
}

func debtEventNameBodies(root string) ([]debtRequestKeyBody, error) {
	raw, err := os.ReadFile(filepath.Join(root, "tools/fixture-codemod/pipeline-observations/family145-event-name-preimages.json"))
	if err != nil || debtBaselineChecksum(raw) != "3ec05c09f5a792662c6623c10efac08ae2c897fd8308a4d9e62aa941d1f3b0b1" {
		return nil, fmt.Errorf("exact event name source evidence changed: %v", err)
	}
	var bodies []debtRequestKeyBody
	if err := json.Unmarshal(raw, &bodies); err != nil || len(bodies) != 6 {
		return nil, fmt.Errorf("invalid event name complete source inventory: %v", err)
	}
	return bodies, nil
}

func debtEventNameSourcePrevious(root, file, function, current string) (string, error) {
	if file != "internal/serveapp/main_runtime_test.go" || !debtEventNameSuccessorCandidate(current) {
		return current, nil
	}
	bodies, err := debtEventNameBodies(root)
	if err != nil {
		return "", err
	}
	for _, body := range bodies {
		if body.File == file && body.Function == function && current == body.After && debtBaselineChecksum([]byte(body.Before)) == body.SHA256 {
			return body.Before, nil
		}
	}
	return current, nil
}

func debtEventNameSuccessorCandidate(current string) bool {
	for _, marker := range []string{
		"servedEventNameCount(t, reader,", "servedEventNameCount(t, rt.ReceiverStateReader,",
		"requireServedEventNameCount(t, rt.ReceiverStateReader,", "servedEventNameCountForRun(t, rt.ReceiverStateReader,",
		"storetest.CountEventNameStorage(ctx, reader,", "storetest.CountRunNamedEventStorage(ctx, reader,",
	} {
		if strings.Contains(current, marker) {
			return true
		}
	}
	return false
}

func debtEventNameSourcePins(root string) error {
	bodies, err := debtEventNameBodies(root)
	if err != nil {
		return err
	}
	const file = "internal/serveapp/main_runtime_test.go"
	declarations, err := debtServedDebugDeclarations(filepath.Join(root, file))
	if err != nil {
		return err
	}
	for _, body := range bodies {
		if body.File != file || declarations[body.Function] != body.After || debtBaselineChecksum([]byte(body.Before)) != body.SHA256 {
			return fmt.Errorf("event name owner/assertion/polling cut changed: %s/%s", body.File, body.Function)
		}
	}
	return nil
}

func debtEventNameCollectorTransition(base, current string, trusted, head authorityDebtBaseline, proofs ...debtDeliveryRetirementProof) bool {
	if current != debtEventNameCollectorTo || head.Collector != current || trusted.Collector != base || len(proofs) != 1 ||
		trusted.BootstrapSource != head.BootstrapSource || debtEventNameSourcePins(proofs[0].Root) != nil {
		return false
	}
	if base == debtDeliveryRetirementCollectorTo {
		return len(authorityDebtRatchet(proofs[0].Actual, head.Sites, trusted.Sites, trusted.Sites, proofs...)) == 0
	}
	intermediate := head
	intermediate.Collector = debtDeliveryRetirementCollectorTo
	return debtDeliveryRetirementCollectorTransition(base, intermediate.Collector, trusted, intermediate, proofs...)
}

func debtDeliveryStatusSuccessorCandidate(current string) bool {
	for _, marker := range []string{
		"servedEventPublishDeliveryStatusCount(t, reader,", "servedEventPublishDeliveryStatusCount(t, rt.ReceiverStateReader,",
		"waitServedEventPublishDeliveryStatusCount(t, reader,", "waitServedEventPublishDeliveryStatusCountForRun(t, reader,",
		"waitServedEventPublishDeliveryStatusCountForRun(t, rt.ReceiverStateReader,", "requireNoServedDeliveryStatusDuring(t, rt.ReceiverStateReader,",
	} {
		if strings.Contains(current, marker) {
			return true
		}
	}
	return false
}

func debtDeliveryStatusSourcePins(root string) error {
	bodies, err := debtDeliveryStatusBodies(root)
	if err != nil {
		return err
	}
	files := map[string]map[string]string{}
	for _, body := range bodies {
		if files[body.File] == nil {
			files[body.File], err = debtServedDebugDeclarations(filepath.Join(root, body.File))
			if err != nil {
				return err
			}
		}
		previous, err := debtSourceForkSourcePrevious(root, body.File, body.Function, files[body.File][body.Function])
		if err != nil {
			return err
		}
		previous, err = debtEventNameSourcePrevious(root, body.File, body.Function, previous)
		if err != nil || previous != body.After || debtBaselineChecksum([]byte(body.Before)) != body.SHA256 {
			return fmt.Errorf("delivery status owner/assertion/polling cut changed: %s/%s: %v", body.File, body.Function, err)
		}
	}
	return nil
}

func debtDeliveryStatusCollectorTransition(base, current string, trusted, head authorityDebtBaseline) bool {
	if current != debtDeliveryStatusCollectorTo || head.Collector != current || trusted.Collector != base {
		return false
	}
	if base != debtRunScalarCollectorTo {
		intermediate := head
		intermediate.Collector = debtRunScalarCollectorTo
		if !debtRunScalarCollectorTransition(base, debtRunScalarCollectorTo, trusted, intermediate) {
			return false
		}
	}
	return trusted.BootstrapSource == head.BootstrapSource && len(authorityDebtRatchet(head.Sites, head.Sites, trusted.Sites, trusted.Sites)) == 0
}

func debtRunScalarCollectorTransition(base, current string, trusted, head authorityDebtBaseline) bool {
	if current != debtRunScalarCollectorTo || head.Collector != current || trusted.Collector != base {
		return false
	}
	if base != debtRequestKeyCollectorTo {
		intermediate := head
		intermediate.Collector = debtRequestKeyCollectorTo
		if !debtRequestKeyCollectorTransition(base, debtRequestKeyCollectorTo, trusted, intermediate) {
			return false
		}
	}
	return trusted.BootstrapSource == head.BootstrapSource && len(authorityDebtRatchet(head.Sites, head.Sites, trusted.Sites, trusted.Sites)) == 0
}

func debtRequestKeySuccessorCandidate(current string) bool {
	for _, marker := range []string{
		"servedEventPublishAPIIdempotencyCount(t, reader,", "servedEventPublishAPIIdempotencyCount(t, rt.ReceiverStateReader,",
		"servedEventPublishEventCountByIdempotencyKey(t, reader,", "requireServedControlAPIIdempotencyRows(t, rt.ReceiverStateReader,",
		"requireServedReplayNoDeliveryHistoryNoMutation(t, endpoint, reader,", "servedEventPublishSourcedEventCount(t, reader,",
		"storetest.ReadAPIIdempotencyCardinality(", "storetest.ReadEventIdempotencyCardinality(", "storetest.CountSourceEventPublications(",
	} {
		if strings.Contains(current, marker) {
			return true
		}
	}
	return false
}

func debtRequestKeySourcePins(root string) error {
	bodies, err := debtRequestKeyBodies(root)
	if err != nil {
		return err
	}
	declarations, err := debtServedDebugDeclarations(filepath.Join(root, "internal/serveapp/test_command_runtime_test.go"))
	if err != nil {
		return err
	}
	wanted := map[string]bool{"requirePublicInputRollbackNoResidue": true}
	for _, pair := range debtRequestKeySignatureTransitions() {
		wanted[pair[0].Declaration] = true
	}
	for _, body := range bodies {
		if body.File != "internal/serveapp/test_command_runtime_test.go" || !wanted[body.Function] {
			continue
		}
		current, err := debtPublicRollbackSourcePrevious(root, body.File, body.Function, declarations[body.Function])
		if err != nil || current != body.After || debtBaselineChecksum([]byte(body.Before)) != body.SHA256 {
			return fmt.Errorf("reviewed request-key raw-call capture/cut/body changed: %s", body.Function)
		}
		delete(wanted, body.Function)
	}
	if len(wanted) != 0 {
		return fmt.Errorf("request-key source proof lacks exact helper/callers: %v", wanted)
	}
	return nil
}

func debtRequestKeySourceTransition(root string, actual, source map[string]authorityDebtSite) error {
	for _, pair := range debtRequestKeySignatureTransitions() {
		if actual[pair[1].identity()] == pair[1] && source[pair[0].identity()] == pair[0] {
			return debtRequestKeySourcePins(root)
		}
	}
	return nil // Landed no-op or genuine later deletion needs no signature transfer.
}

func debtRequestKeyCollectorTransition(base, current string, trusted, head authorityDebtBaseline) bool {
	if current != debtRequestKeyCollectorTo || head.Collector != current || trusted.Collector != base {
		return false
	}
	if base != debtReceiverReadbackCollectorTo {
		intermediate := trusted
		intermediate.Collector = debtReceiverReadbackCollectorTo
		if debtValidateCollectorIdentity(base, intermediate.Collector, trusted, intermediate, "") != nil {
			return false
		}
	}
	for _, pair := range debtRequestKeySignatureTransitions() {
		if trusted.Sites[pair[0].identity()] != pair[0] || trusted.Sites[pair[1].identity()].Multiplicity != 0 ||
			(head.Sites[pair[0].identity()] != pair[0] && head.Sites[pair[1].identity()] != pair[1]) {
			return false
		}
	}
	return len(authorityDebtRatchet(head.Sites, head.Sites, trusted.Sites, trusted.Sites)) == 0
}

func debtStageCompletionComparisons(actual, head, base, source map[string]authorityDebtSite) (map[string]authorityDebtSite, map[string]authorityDebtSite, map[string]authorityDebtSite, []string) {
	old, current := debtStageCompletionTransition()
	for _, state := range []map[string]authorityDebtSite{actual, head, base, source} {
		if state[old.identity()].Multiplicity > 0 && state[current.identity()].Multiplicity > 0 {
			return head, base, source, []string{"both reviewed stage-completion construction identities survive"}
		}
	}
	if actual[current.identity()] == current && actual[old.identity()].Multiplicity == 0 &&
		base[old.identity()] == old && base[current.identity()].Multiplicity == 0 &&
		source[old.identity()] == old && source[current.identity()].Multiplicity == 0 {
		head = debtForwardQuiescenceUncertainty(head, old, current)
		base = debtForwardQuiescenceUncertainty(base, old, current)
		source = debtForwardQuiescenceUncertainty(source, old, current)
	}
	return head, base, source, nil
}

func debtStageCompletionSourcePins(root string, forward bool) error {
	// Whole-file pins include both declaration bodies, imports and the factory's
	// independent construction and reverse-order cleanup. No product code moves.
	journey := "1dbe3fb4fae40329c6e3dff70e7e8f0ab10eda60dfa205f1c1e244bd1582cae7"
	if forward {
		journey = "c4d8f0768a8b8e0988b97196b2c7dc0df4db593bcaac32783c553204ef298ee8"
	}
	for _, pin := range [][2]string{
		{"internal/apiv1/operator_stage_completion_journey_test.go", journey},
		{"internal/store/storetest/runtime_reopen.go", "5dd3752bfbb96b53916e428f056be160751456fd2cf7414d8cbaad00b9808a89"},
	} {
		data, err := os.ReadFile(filepath.Join(root, pin[0]))
		if err != nil {
			return err
		}
		if debtBaselineChecksum(data) != pin[1] {
			return fmt.Errorf("reviewed stage-completion relocation source changed: %s", pin[0])
		}
	}
	return nil
}

func debtStageCompletionSourceTransition(root, baseRoot string, actual, source map[string]authorityDebtSite) error {
	old, current := debtStageCompletionTransition()
	if actual[current.identity()] != current || source[old.identity()] != old {
		return nil // Landed no-op and genuine subsequent deletion need no relocation.
	}
	if err := debtStageCompletionSourcePins(root, true); err != nil {
		return err
	}
	return debtStageCompletionSourcePins(baseRoot, false)
}

func debtStageCompletionCollectorTransition(base, current string, trusted, head authorityDebtBaseline) bool {
	old, forward := debtStageCompletionTransition()
	if base != debtFixtureRoleCollectorTo || trusted.Collector != base ||
		current != debtStageCompletionCollectorTo || head.Collector != current {
		return false
	}
	if trusted.Sites[old.identity()] != old || trusted.Sites[forward.identity()].Multiplicity != 0 ||
		(head.Sites[old.identity()] != old && head.Sites[forward.identity()] != forward) {
		return false
	}
	return len(authorityDebtRatchet(head.Sites, head.Sites, trusted.Sites, trusted.Sites)) == 0
}

// Reviewed at 2b699b9af7c05fb8f319ae190f9c2f802bd7bd10 ->
// 762f3084c5f6f50539c9f56a3dc15a1b603822f3 (ruling 6091976606).
// These nine unchanged calls reuse the registrar's existing selected owner.
// Its raw input, admission and registration remain debt; only the result changed.
func debtRunFixtureSignatureTransitions() [9][2]authorityDebtSite {
	const old = "func=func(t *testing.T, ctx context.Context, db *database/sql.DB, sourceRoot string) string|recv=<nil>|arg=*testing.T|arg=context.Context|arg=*database/sql.DB|arg=string|results=(string)"
	const current = "func=func(t *testing.T, ctx context.Context, db *database/sql.DB, sourceRoot string) (string, github.com/division-sh/swarm/internal/store/storetest.RunFixtureStore)|recv=<nil>|arg=*testing.T|arg=context.Context|arg=*database/sql.DB|arg=string|results=(string,github.com/division-sh/swarm/internal/store/storetest.RunFixtureStore)"
	return [9][2]authorityDebtSite{
		{debtRunFixtureSignatureSite("TestRunForkRuntimeOwnerHarness_ActivateSelectedBindingConsumesRuntimeAdmission", old), debtRunFixtureSignatureSite("TestRunForkRuntimeOwnerHarness_ActivateSelectedBindingConsumesRuntimeAdmission", current)},
		{debtRunFixtureSignatureSite("TestRunForkRuntimeOwnerHarness_ActivateSelectedBindingRejectsDeliveryReplayWithoutPersistedRouteRecovery", old), debtRunFixtureSignatureSite("TestRunForkRuntimeOwnerHarness_ActivateSelectedBindingRejectsDeliveryReplayWithoutPersistedRouteRecovery", current)},
		{debtRunFixtureSignatureSite("TestRunForkRuntimeOwnerHarness_DryRunBundleAddsContractFrontierAdmissionJSON", old), debtRunFixtureSignatureSite("TestRunForkRuntimeOwnerHarness_DryRunBundleAddsContractFrontierAdmissionJSON", current)},
		{debtRunFixtureSignatureSite("TestRunForkRuntimeOwnerHarness_MaterializeOnlyUsesCanonicalStoreOwnerJSON", old), debtRunFixtureSignatureSite("TestRunForkRuntimeOwnerHarness_MaterializeOnlyUsesCanonicalStoreOwnerJSON", current)},
		{debtRunFixtureSignatureSite("TestRunForkRuntimeOwnerHarness_PersistedBundleDoesNotRequireAmbientSource", old), debtRunFixtureSignatureSite("TestRunForkRuntimeOwnerHarness_PersistedBundleDoesNotRequireAmbientSource", current)},
		{debtRunFixtureSignatureSite("TestRunForkRuntimeOwnerHarness_SelectedContractsExecuteReportsSourceAdvancedBranchJSON", old), debtRunFixtureSignatureSite("TestRunForkRuntimeOwnerHarness_SelectedContractsExecuteReportsSourceAdvancedBranchJSON", current)},
		{debtRunFixtureSignatureSite("TestRunForkRuntimeOwnerHarness_SelectedContractsExecuteThroughCanonicalOwnerJSON", old), debtRunFixtureSignatureSite("TestRunForkRuntimeOwnerHarness_SelectedContractsExecuteThroughCanonicalOwnerJSON", current)},
		{debtRunFixtureSignatureSite("TestRunForkRuntimeOwnerHarness_SelectedContractsExecutesExplicitHostRefusal", old), debtRunFixtureSignatureSite("TestRunForkRuntimeOwnerHarness_SelectedContractsExecutesExplicitHostRefusal", current)},
		{debtRunFixtureSignatureSite("seedRunForkCLIActivationSourceWithoutRevision", old), debtRunFixtureSignatureSite("seedRunForkCLIActivationSourceWithoutRevision", current)},
	}
}

func debtRunFixtureSignatureSite(declaration, resolved string) authorityDebtSite {
	return authorityDebtSite{
		Kind: "raw-operation", File: "internal/serveapp/run_fork_runtime_test.go", Declaration: declaration,
		Operation: "call:github.com/division-sh/swarm/internal/serveapp.registerRunForkCLIContractCatalog",
		Resolved:  resolved, Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1,
	}
}

func debtRunFixtureSignatureComparisons(actual, head, base, source map[string]authorityDebtSite) (map[string]authorityDebtSite, map[string]authorityDebtSite, map[string]authorityDebtSite, []string) {
	for _, pair := range debtRunFixtureSignatureTransitions() {
		old, current := pair[0], pair[1]
		for _, state := range []map[string]authorityDebtSite{actual, head, base, source} {
			if state[old.identity()].Multiplicity > 0 && state[current.identity()].Multiplicity > 0 {
				return head, base, source, []string{"both reviewed run-fixture signature identities survive: " + old.Declaration}
			}
		}
		if actual[current.identity()] == current && actual[old.identity()].Multiplicity == 0 &&
			base[old.identity()] == old && base[current.identity()].Multiplicity == 0 &&
			source[old.identity()] == old && source[current.identity()].Multiplicity == 0 {
			head = debtForwardQuiescenceUncertainty(head, old, current)
			base = debtForwardQuiescenceUncertainty(base, old, current)
			source = debtForwardQuiescenceUncertainty(source, old, current)
		}
	}
	return head, base, source, nil
}

// Ruling6138883675: exactly49 pairs retain71 raw occurrences, with zero credit.
func debtServedDebugSignatureTransitions() [49][2]authorityDebtSite {
	return [49][2]authorityDebtSite{
		{
			{Kind: "raw-operation", File: "internal/serveapp/issue2277_audit_probe_test.go", Declaration: "TestAudit2277StoppedRunReadinessRestart", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishEventID", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventName string) string|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|results=(string)", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/issue2277_audit_probe_test.go", Declaration: "TestAudit2277StoppedRunReadinessRestart", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishEventID", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventName string) string|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|results=(string)", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/mailbox_entity_filter_test.go", Declaration: "startServedMailboxEntityFilterProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycleForNode", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, nodeID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "mailbox", Replacement: "selected mailbox/completion owner and named mailbox fault/readback", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/mailbox_entity_filter_test.go", Declaration: "startServedMailboxEntityFilterProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycleForNode", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, nodeID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "mailbox", Replacement: "selected mailbox/completion owner and named mailbox fault/readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeJoinFailureReachesAPIAndCLI", Operation: "call:github.com/division-sh/swarm/internal/serveapp.servedJoinTarget", Resolved: "func=func(t *testing.T, db *database/sql.DB, runID string) string|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|results=(string)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeJoinFailureReachesAPIAndCLI", Operation: "call:github.com/division-sh/swarm/internal/serveapp.servedJoinTarget", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, runID string) string|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|results=(string)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeJoinFailureReachesAPIAndCLI", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedDeliveryOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeJoinFailureReachesAPIAndCLI", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedDeliveryOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeJoinFailureReachesAPIAndCLI", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishDeliveryStatusCountForRun", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, subscriberType string, subscriberID string, status string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeJoinFailureReachesAPIAndCLI", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishDeliveryStatusCountForRun", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, subscriberType string, subscriberID string, status string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeJoinFailureReachesAPIAndCLI", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishReceiptOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeJoinFailureReachesAPIAndCLI", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishReceiptOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation", Operation: "call:github.com/division-sh/swarm/internal/serveapp.servedJoinTarget", Resolved: "func=func(t *testing.T, db *database/sql.DB, runID string) string|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|results=(string)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation", Operation: "call:github.com/division-sh/swarm/internal/serveapp.servedJoinTarget", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, runID string) string|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|results=(string)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishDeliveryStatusCountForRun", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, subscriberType string, subscriberID string, status string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 3},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishDeliveryStatusCountForRun", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, subscriberType string, subscriberID string, status string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 3},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishReceiptOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishReceiptOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedJoinSourceTimer", Resolved: "func=func(t *testing.T, db *database/sql.DB, runID string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedJoinSourceTimer", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, runID string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeNukeQuiescesSessionWriterBeforeCleanupPostgres", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycle", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeNukeQuiescesSessionWriterBeforeCleanupPostgres", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycle", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "createServedControlWaitingRun", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycleForNode", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, nodeID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "createServedControlWaitingRun", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycleForNode", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, nodeID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "publishServedLiveAgentHoldEvent", Operation: "call:github.com/division-sh/swarm/internal/serveapp.requireServedEventPublishCommittedReplayScope", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, wantScope string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "publishServedLiveAgentHoldEvent", Operation: "call:github.com/division-sh/swarm/internal/serveapp.requireServedEventPublishCommittedReplayScope", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, wantScope string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "publishServedLiveAgentHoldEvent", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedDeliveryOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "publishServedLiveAgentHoldEvent", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedDeliveryOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "publishServedLiveAgentHoldEvent", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishDeliveryStatusCountForRun", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, subscriberType string, subscriberID string, status string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "publishServedLiveAgentHoldEvent", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishDeliveryStatusCountForRun", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, subscriberType string, subscriberID string, status string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "requireServedStoppedPendingDelivery", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedDeliveryOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "requireServedStoppedPendingDelivery", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedDeliveryOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "requireServedStoppedPendingDelivery", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishReceiptOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "requireServedStoppedPendingDelivery", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishReceiptOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedAgentRestartLifecycleProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishDeliveryStatusCountForRun", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, subscriberType string, subscriberID string, status string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedAgentRestartLifecycleProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishDeliveryStatusCountForRun", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, subscriberType string, subscriberID string, status string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedCreateCarryProjectionPostgresProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedCreateCarryProjectionProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, backend string, bundleHash string)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=string|arg=string|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedCreateCarryProjectionPostgresProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedCreateCarryProjectionProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, bundleHash string)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedCreateCarryProjectionProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishEventID", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventName string) string|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|results=(string)", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedCreateCarryProjectionProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishEventID", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventName string) string|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|results=(string)", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedCreateCarryProjectionSQLiteProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedCreateCarryProjectionProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, backend string, bundleHash string)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=string|arg=string|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedCreateCarryProjectionSQLiteProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedCreateCarryProjectionProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, bundleHash string)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedDynamicAutoEmitProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.requireServedEventPublishCommittedReplayScope", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, wantScope string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedDynamicAutoEmitProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.requireServedEventPublishCommittedReplayScope", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, wantScope string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedDynamicAutoEmitProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedDeliveryOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedDynamicAutoEmitProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedDeliveryOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedDynamicAutoEmitProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishEventID", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventName string) string|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|results=(string)", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedDynamicAutoEmitProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishEventID", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventName string) string|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|results=(string)", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedEventPublishActiveLoadProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.requireServedEventPublishCommittedReplayScope", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, wantScope string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedEventPublishActiveLoadProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.requireServedEventPublishCommittedReplayScope", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, wantScope string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedEventPublishActiveLoadProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycleForNode", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, nodeID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedEventPublishActiveLoadProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycleForNode", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, nodeID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedEventPublishFollowUpProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycleForNode", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, nodeID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedEventPublishFollowUpProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycleForNode", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, nodeID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedEventPublishTargetRouteProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishEventID", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventName string) string|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|results=(string)", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedEventPublishTargetRouteProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishEventID", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventName string) string|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|results=(string)", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedLiveAgentEventReplayLifecycleProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishDeliveryStatusCountForRun", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, subscriberType string, subscriberID string, status string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedLiveAgentEventReplayLifecycleProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishDeliveryStatusCountForRun", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, subscriberType string, subscriberID string, status string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedRunControlLifecycleProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.requireServedStoppedPendingDelivery", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, eventID string, subscriberID string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedRunControlLifecycleProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.requireServedStoppedPendingDelivery", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, eventID string, subscriberID string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedRunControlLifecycleProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishDeliveryStatusCountForRun", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, subscriberType string, subscriberID string, status string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedRunControlLifecycleProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishDeliveryStatusCountForRun", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, subscriberType string, subscriberID string, status string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedRunForkBackendProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycleForNode", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, nodeID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedRunForkBackendProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycleForNode", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, nodeID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedRuntimeIngressControlLifecycleProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishReceiptOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedRuntimeIngressControlLifecycleProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishReceiptOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "startServedSessionCleanupProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycle", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "startServedSessionCleanupProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycle", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "startServedSessionCleanupProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishDeliveryStatusCountForRun", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, subscriberType string, subscriberID string, status string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "startServedSessionCleanupProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishDeliveryStatusCountForRun", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, subscriberType string, subscriberID string, status string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "waitForServedEventPublishNodeDeliveryLifecycle", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycleForNode", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, nodeID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "waitForServedEventPublishNodeDeliveryLifecycle", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycleForNode", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, nodeID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/receiver_failure_settlement_test.go", Declaration: "TestReceiverCompositionFailureSettlementBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishReceiptOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/receiver_failure_settlement_test.go", Declaration: "TestReceiverCompositionFailureSettlementBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishReceiptOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/reset_candidate_fence_test.go", Declaration: "TestResetCandidateSetFencesConsumersWhileSecondPreparesBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycle", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/reset_candidate_fence_test.go", Declaration: "TestResetCandidateSetFencesConsumersWhileSecondPreparesBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycle", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/reset_convergence_receipt_test.go", Declaration: "proveServedResetFinalReceiptFailure", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycle", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/reset_convergence_receipt_test.go", Declaration: "proveServedResetFinalReceiptFailure", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycle", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/reset_served_parity_test.go", Declaration: "TestServedResetCLIDryRunApplyAndReplayBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycle", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/reset_served_parity_test.go", Declaration: "TestServedResetCLIDryRunApplyAndReplayBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycle", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/reset_served_parity_test.go", Declaration: "TestServedResetRetainClearAndHistoricalReplayBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycle", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/serveapp/reset_served_parity_test.go", Declaration: "TestServedResetRetainClearAndHistoricalReplayBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycle", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/selected_fork_pending_input_mixed_test.go", Declaration: "TestSelectedForkPendingInputMixedCompletionBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedDeliveryOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/selected_fork_pending_input_mixed_test.go", Declaration: "TestSelectedForkPendingInputMixedCompletionBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedDeliveryOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/selected_fork_pending_input_mixed_test.go", Declaration: "TestSelectedForkPendingInputMixedCompletionBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishReceiptOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/selected_fork_pending_input_mixed_test.go", Declaration: "TestSelectedForkPendingInputMixedCompletionBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitServedEventPublishReceiptOutcomeCount", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, eventID string, subscriberType string, subscriberID string, outcome string, want int)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/selected_reset_admission_test.go", Declaration: "TestSelectedResetPreparationAdmissionOrderBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycle", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, eventID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/selected_reset_admission_test.go", Declaration: "TestSelectedResetPreparationAdmissionOrderBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.waitForServedEventPublishNodeDeliveryLifecycle", Resolved: "func=func(t *testing.T, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, runID string, eventID string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/semantic_numeric_activity_test.go", Declaration: "proveActivitySemanticResultExecutionParity", Operation: "call:github.com/division-sh/swarm/internal/serveapp.semanticNumericOutput", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, run string)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=string|results=()", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 3},
			{Kind: "raw-operation", File: "internal/serveapp/semantic_numeric_activity_test.go", Declaration: "proveActivitySemanticResultExecutionParity", Operation: "call:github.com/division-sh/swarm/internal/serveapp.semanticNumericOutput", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, run string)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|results=()", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 3},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/semantic_numeric_gate_test.go", Declaration: "TestWorkflowGateSemanticNumericOutcomeExecutionParity", Operation: "call:github.com/division-sh/swarm/internal/serveapp.semanticNumericOutput", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, run string)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=string|results=()", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/serveapp/semantic_numeric_gate_test.go", Declaration: "TestWorkflowGateSemanticNumericOutcomeExecutionParity", Operation: "call:github.com/division-sh/swarm/internal/serveapp.semanticNumericOutput", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, run string)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|results=()", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/semantic_numeric_ingress_test.go", Declaration: "TestServedSemanticNumericEventSubscription", Operation: "call:github.com/division-sh/swarm/internal/serveapp.semanticNumericOutput", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, run string)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=string|results=()", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/semantic_numeric_ingress_test.go", Declaration: "TestServedSemanticNumericEventSubscription", Operation: "call:github.com/division-sh/swarm/internal/serveapp.semanticNumericOutput", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, run string)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|results=()", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/semantic_numeric_ingress_test.go", Declaration: "TestServedSemanticNumericIdempotencyReplay", Operation: "call:github.com/division-sh/swarm/internal/serveapp.semanticNumericOutput", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, run string)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=string|results=()", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 3},
			{Kind: "raw-operation", File: "internal/serveapp/semantic_numeric_ingress_test.go", Declaration: "TestServedSemanticNumericIdempotencyReplay", Operation: "call:github.com/division-sh/swarm/internal/serveapp.semanticNumericOutput", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, run string)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|results=()", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 3},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/semantic_numeric_schedule_test.go", Declaration: "TestGenericScheduleSemanticPayloadExecutionParity", Operation: "call:github.com/division-sh/swarm/internal/serveapp.semanticNumericOutput", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, run string)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=string|results=()", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/serveapp/semantic_numeric_schedule_test.go", Declaration: "TestGenericScheduleSemanticPayloadExecutionParity", Operation: "call:github.com/division-sh/swarm/internal/serveapp.semanticNumericOutput", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, run string)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|results=()", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 2},
		},
	}
}

func debtWriterStageSignatureTransitions() [7][2]authorityDebtSite {
	return [7][2]authorityDebtSite{
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeEventPublishExistingRunActiveLoadServedPathDefaultSQLite", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedEventPublishActiveLoadProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, backend string, bundleHash string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe, agentStarted <-chan struct{}, release chan struct{}, releaseOnce *sync.Once)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|arg=chan struct{}|arg=chan struct{}|arg=*sync.Once|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeEventPublishExistingRunActiveLoadServedPathDefaultSQLite", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedEventPublishActiveLoadProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, bundleHash string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe, agentStarted <-chan struct{}, release chan struct{}, releaseOnce *sync.Once)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|arg=chan struct{}|arg=chan struct{}|arg=*sync.Once|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeEventPublishExistingRunActiveLoadServedPathPostgres", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedEventPublishActiveLoadProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, backend string, bundleHash string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe, agentStarted <-chan struct{}, release chan struct{}, releaseOnce *sync.Once)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|arg=chan struct{}|arg=chan struct{}|arg=*sync.Once|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeEventPublishExistingRunActiveLoadServedPathPostgres", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedEventPublishActiveLoadProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, bundleHash string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe, agentStarted <-chan struct{}, release chan struct{}, releaseOnce *sync.Once)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|arg=chan struct{}|arg=chan struct{}|arg=*sync.Once|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeEventPublishRunIDFollowUpServedPathDefaultSQLite", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedEventPublishFollowUpProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, backend string, bundleHash string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe) github.com/division-sh/swarm/internal/serveapp.servedEventPublishFollowUpProof|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=(github.com/division-sh/swarm/internal/serveapp.servedEventPublishFollowUpProof)", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeEventPublishRunIDFollowUpServedPathDefaultSQLite", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedEventPublishFollowUpProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, bundleHash string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe) github.com/division-sh/swarm/internal/serveapp.servedEventPublishFollowUpProof|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=(github.com/division-sh/swarm/internal/serveapp.servedEventPublishFollowUpProof)", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeEventPublishTargetRouteServedPathDefaultSQLite", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedEventPublishTargetRouteProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, backend string, bundleHash string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeEventPublishTargetRouteServedPathDefaultSQLite", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedEventPublishTargetRouteProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, bundleHash string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeEventPublishTargetRouteServedPathPostgres", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedEventPublishTargetRouteProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, backend string, bundleHash string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeEventPublishTargetRouteServedPathPostgres", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedEventPublishTargetRouteProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, bundleHash string, probe *github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=*github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest.Probe|results=()", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedDynamicAutoEmitPostgresProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedDynamicAutoEmitProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, backend string, bundleHash string, blocked <-chan github.com/division-sh/swarm/internal/serveapp.servedEventPublishPreHandlerProof, release chan struct{}, releaseOnce *sync.Once)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=string|arg=string|arg=chan github.com/division-sh/swarm/internal/serveapp.servedEventPublishPreHandlerProof|arg=chan struct{}|arg=*sync.Once|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedDynamicAutoEmitPostgresProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedDynamicAutoEmitProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, bundleHash string, blocked <-chan github.com/division-sh/swarm/internal/serveapp.servedEventPublishPreHandlerProof, release chan struct{}, releaseOnce *sync.Once)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=chan github.com/division-sh/swarm/internal/serveapp.servedEventPublishPreHandlerProof|arg=chan struct{}|arg=*sync.Once|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedDynamicAutoEmitSQLiteProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedDynamicAutoEmitProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, backend string, bundleHash string, blocked <-chan github.com/division-sh/swarm/internal/serveapp.servedEventPublishPreHandlerProof, release chan struct{}, releaseOnce *sync.Once)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=string|arg=string|arg=chan github.com/division-sh/swarm/internal/serveapp.servedEventPublishPreHandlerProof|arg=chan struct{}|arg=*sync.Once|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "runServedDynamicAutoEmitSQLiteProof", Operation: "call:github.com/division-sh/swarm/internal/serveapp.runServedDynamicAutoEmitProof", Resolved: "func=func(t *testing.T, endpoint string, db *database/sql.DB, reader github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader, backend string, bundleHash string, blocked <-chan github.com/division-sh/swarm/internal/serveapp.servedEventPublishPreHandlerProof, release chan struct{}, releaseOnce *sync.Once)|recv=<nil>|arg=*testing.T|arg=string|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/serveapp.receiverProofStateReader|arg=string|arg=string|arg=chan github.com/division-sh/swarm/internal/serveapp.servedEventPublishPreHandlerProof|arg=chan struct{}|arg=*sync.Once|results=()", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
	}
}
func debtWriterStageBodyPins() [11][3]string {
	return [11][3]string{
		{"TestRunServeRuntimeEventPublishExistingRunActiveLoadServedPathDefaultSQLite", "5604a1d1f377ce1202d307db5475773dd145ee338ba703fae631d4f3c1cf28f0", "98b54c10edff7a9e9ceb65744ccc1e6c543e99621df081d4946e611747dd2c39"},
		{"TestRunServeRuntimeEventPublishExistingRunActiveLoadServedPathPostgres", "4409c1774f051b8d3432d95ef0fd38739979964ca42cefc18d471f14a3313831", "66aa175283aedd353b94dcc97ae33e200f337d4fa459fbcba318c679426c9752"},
		{"TestRunServeRuntimeEventPublishRunIDFollowUpServedPathDefaultSQLite", "e2d1859db9c48a640494a2be4b4a11e16c25cb43b3be178bbf4abe399de3bd11", "491cb870b45fa09a6b12f05e7484c16a41f95fd9380e600d1a1c7ed55a822acc"},
		{"TestRunServeRuntimeEventPublishTargetRouteServedPathDefaultSQLite", "cbc13981f8306bb965552a7128346170c36955823914ae9093e669b853ce3f42", "bce26befd6d96a7acdb9c0e18fbc8320d6f2ea6a079f7d28eefc87710e38a8dd"},
		{"TestRunServeRuntimeEventPublishTargetRouteServedPathPostgres", "ebec118524715e23fecad4bd1e1971617ba9d69ffed3d3f4f622e7ef19b6cbdf", "4d3a7999fece6d4ba9b7a7e6aedfc137c9cd3dbc8f38bca77c02e12a7216062e"},
		{"runServedDynamicAutoEmitPostgresProof", "ffcbd68129db076f0b7ec0d5543817b7341c64f300b8818805aa8adb93855508", "e3c0a1e48278c55d96ea3b919394a0b59a76be4d22da975e49b032dc49bba0d2"},
		{"runServedDynamicAutoEmitProof", "34cbbc186ba2c165a50d1708f3df3d8ac5ebc7150c70093fd06854fc118e0392", "4bbea4c2c3b0372226cad6bcf2e993d2600e53948c5c1ca7322dc9b0a0e42715"},
		{"runServedDynamicAutoEmitSQLiteProof", "3c015d969ea13cb3ce469fd6bc9bb42633b0e14d5e63e65b6c90298215ee9e5d", "a6d926bb953258ae4447d9b276deb766f76dc3c90678f62d5347678b73883113"},
		{"runServedEventPublishActiveLoadProof", "c9ca3c17c821ed9f88883d04ce32518e16289ad960da5b02921fb6d3b645d8a4", "bb6bb683c6f2c2e7d2e5c297898dadf6b53e3b8460f9d311e8687a1185e8ec83"},
		{"runServedEventPublishFollowUpProof", "b88f7b1d10b68072e7bbc5be5bb0f4c4325d0f29e16f6fdf6bab6e3dda7216de", "952cbc2540fa32fc175424b7a45231649cea900fa797dc1cd83913349ad99e9d"},
		{"runServedEventPublishTargetRouteProof", "9436dc577635ba058f4ed22f8133f42f8007daaaff8dc95d2ec6bd5b3e979bd5", "34c5c96987fe1253959151ae4d9a64356ca1bbc176430720da251a879d8e71de"},
	}
}

// Ruling6136688648: exactly seven retained raw calls, each counted once.
func debtWriterStageSignatureComparisons(actual, head, base, source map[string]authorityDebtSite) (map[string]authorityDebtSite, map[string]authorityDebtSite, map[string]authorityDebtSite, []string) {
	for _, pair := range debtWriterStageSignatureTransitions() {
		old, current := pair[0], pair[1]
		for _, state := range []map[string]authorityDebtSite{actual, head, base, source} {
			if state[old.identity()].Multiplicity > 0 && state[current.identity()].Multiplicity > 0 {
				return head, base, source, []string{"both reviewed writer-stage signature identities survive: " + old.Declaration}
			}
		}
		if actual[current.identity()] == current && actual[old.identity()].Multiplicity == 0 &&
			base[old.identity()] == old && base[current.identity()].Multiplicity == 0 &&
			source[old.identity()] == old && source[current.identity()].Multiplicity == 0 {
			head = debtForwardQuiescenceUncertainty(head, old, current)
			base = debtForwardQuiescenceUncertainty(base, old, current)
			source = debtForwardQuiescenceUncertainty(source, old, current)
		}
	}
	return head, base, source, nil
}

func debtWriterStageSourcePins(root string, forward bool) error {
	path := filepath.Join(root, "internal/serveapp/main_runtime_test.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
	if err != nil {
		return err
	}
	bodies := map[string]string{}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Recv != nil {
			continue
		}
		if _, duplicate := bodies[fn.Name.Name]; duplicate {
			return fmt.Errorf("duplicate writer-stage declaration: %s", fn.Name.Name)
		}
		var body bytes.Buffer
		if err := format.Node(&body, token.NewFileSet(), fn); err != nil {
			return err
		}
		source := body.String()
		if forward {
			source, err = debtRequestKeySourcePrevious(root, "internal/serveapp/main_runtime_test.go", fn.Name.Name, source)
			if err != nil {
				return err
			}
			source, err = debtServedDebugWriterStagePrevious(root, fn.Name.Name, source)
			if err != nil {
				return err
			}
		}
		bodies[fn.Name.Name] = debtBaselineChecksum([]byte(source))
	}
	column := 1
	if forward {
		column = 2
		manifest, err := os.ReadFile(filepath.Join(root, ".github/audit-artifacts/issue-2542-writer-stage-signature-transition-request.json"))
		if err != nil || debtBaselineChecksum(manifest) != "261b557e0f689f3da462576fa0a16b4e5716c8501a869175a35391652371d0ce" {
			return fmt.Errorf("reviewed seven-pair manifest changed: %v", err)
		}
	}
	for _, pin := range debtWriterStageBodyPins() {
		if bodies[pin[0]] != pin[column] {
			return fmt.Errorf("reviewed writer-stage caller/callee body changed: %s", pin[0])
		}
	}
	return nil
}

func debtWriterStageSourceTransition(root, baseRoot string, actual, source map[string]authorityDebtSite) error {
	for _, pair := range debtWriterStageSignatureTransitions() {
		if actual[pair[1].identity()] == pair[1] && source[pair[0].identity()] == pair[0] {
			if err := debtWriterStageSourcePins(root, true); err != nil {
				return err
			}
			return debtWriterStageSourcePins(baseRoot, false)
		}
	}
	return nil // Landed no-op and genuine later deletion do not remap sources.
}

func debtWriterStageCollectorTransition(base, current string, trusted, head authorityDebtBaseline) bool {
	if current != debtWriterStageCollectorTo || head.Collector != current || trusted.Collector != base {
		return false
	}
	if base != debtStageCompletionCollectorTo {
		intermediate := trusted
		intermediate.Collector = debtStageCompletionCollectorTo
		if debtValidateCollectorIdentity(base, intermediate.Collector, trusted, intermediate, "") != nil {
			return false
		}
	}
	for _, pair := range debtWriterStageSignatureTransitions() {
		if trusted.Sites[pair[0].identity()] != pair[0] || trusted.Sites[pair[1].identity()].Multiplicity != 0 ||
			(head.Sites[pair[0].identity()] != pair[0] && head.Sites[pair[1].identity()] != pair[1]) {
			return false
		}
	}
	return len(authorityDebtRatchet(head.Sites, head.Sites, trusted.Sites, trusted.Sites)) == 0
}

func debtServedDebugSignatureComparisons(actual, head, base, source map[string]authorityDebtSite) (map[string]authorityDebtSite, map[string]authorityDebtSite, map[string]authorityDebtSite, []string) {
	for _, pair := range debtServedDebugSignatureTransitions() {
		old, current := pair[0], pair[1]
		for _, state := range []map[string]authorityDebtSite{actual, head, base, source} {
			if state[old.identity()].Multiplicity > 0 && state[current.identity()].Multiplicity > 0 {
				return head, base, source, []string{"both reviewed served-debug signature identities survive: " + old.Declaration}
			}
		}
		if actual[current.identity()] == current && actual[old.identity()].Multiplicity == 0 &&
			base[old.identity()] == old && base[current.identity()].Multiplicity == 0 &&
			source[old.identity()] == old && source[current.identity()].Multiplicity == 0 {
			head = debtForwardQuiescenceUncertainty(head, old, current)
			base = debtForwardQuiescenceUncertainty(base, old, current)
			source = debtForwardQuiescenceUncertainty(source, old, current)
		}
	}
	return head, base, source, nil
}

type debtServedDebugBody struct {
	File, Function, Before, After, SHA256 string
	DebugCalls, HelperCalls               int
}

func debtServedDebugBodies(root string) ([]debtServedDebugBody, error) {
	path := "tools/fixture-codemod/pipeline-observations/family145-served-debug-preimages.json"
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil || debtBaselineChecksum(raw) != "5bc30573335e4c9efc8915f7db12eb345938e927b347bf368007c1b899a8bd4a" {
		return nil, fmt.Errorf("exact served-debug85 source evidence changed: %v", err)
	}
	var bodies []debtServedDebugBody
	if err := json.Unmarshal(raw, &bodies); err != nil || len(bodies) != 85 {
		return nil, fmt.Errorf("invalid served-debug source inventory: %v", err)
	}
	return bodies, nil
}

// Compose ONLY the four approved complete successors through their immutable
// intermediate. The original eleven writer-stage hashes are never rewritten.
func debtServedDebugWriterStagePrevious(root, function, current string) (string, error) {
	var reviewed string
	for _, pin := range debtWriterStageBodyPins() {
		if pin[0] == function {
			reviewed = pin[2]
		}
	}
	if reviewed == "" || debtBaselineChecksum([]byte(current)) == reviewed {
		return current, nil
	}
	switch function {
	case "runServedDynamicAutoEmitProof", "runServedEventPublishActiveLoadProof", "runServedEventPublishFollowUpProof", "runServedEventPublishTargetRouteProof":
	default:
		return "", fmt.Errorf("writer-stage source changed outside four approved compositions: %s", function)
	}
	bodies, err := debtServedDebugBodies(root)
	if err != nil {
		return "", err
	}
	for _, body := range bodies {
		if body.File == "internal/serveapp/main_runtime_test.go" && body.Function == function && body.After == current && debtBaselineChecksum([]byte(body.Before)) == reviewed {
			return body.Before, nil
		}
	}
	return "", fmt.Errorf("served-debug writer-stage successor is not exact: %s", function)
}

func debtServedDebugDeclarations(path string) (map[string]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	bodies := map[string]string{}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Recv != nil {
			continue
		}
		if _, duplicate := bodies[fn.Name.Name]; duplicate {
			return nil, fmt.Errorf("duplicate served-debug declaration: %s", fn.Name.Name)
		}
		var out bytes.Buffer
		if err := format.Node(&out, token.NewFileSet(), fn); err != nil {
			return nil, err
		}
		bodies[fn.Name.Name] = out.String()
	}
	return bodies, nil
}

func debtServedDebugRequiredSources() map[string]bool {
	needed := map[string]bool{}
	for _, pair := range debtServedDebugSignatureTransitions() {
		needed[pair[0].File+"/"+pair[0].Declaration] = true
		callee := pair[0].Operation[strings.LastIndex(pair[0].Operation, ".")+1:]
		// Each helper definition is independently selected from the reviewed
		// ledger; callers cannot choose a different owner with the same type.
		needed["callee/"+callee] = true
	}
	for _, root := range []string{"TestRunServeRuntimeJoinFailureReachesAPIAndCLI", "TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation", "startServedSessionCleanupProof", "runServedCreateCarryProjectionSQLiteProof", "runServedCreateCarryProjectionPostgresProof", "TestServedSemanticNumericIdempotencyReplay", "runServedStandingServiceLifecycleBackendProof", "runServedGeneratedInputFixtureBackendProof", "runServedPublicMockApprovalBackendProof"} {
		needed["capture/"+root] = true
	}
	return needed
}

func debtServedDebugSourcePins(root string) error {
	manifest, err := os.ReadFile(filepath.Join(root, ".github/audit-artifacts/issue-2542-served-debug-signature-transition-request.json"))
	if err != nil || debtBaselineChecksum(manifest) != "bcf9e019cc0ecbd0eccd28c97689074e509b94372ece4d8f93c6c77f790be018" {
		return fmt.Errorf("reviewed49-pair/71-occurrence manifest changed: %v", err)
	}
	bodies, err := debtServedDebugBodies(root)
	if err != nil {
		return err
	}
	needed := debtServedDebugRequiredSources()
	loaded := map[string]map[string]string{}
	for _, body := range bodies {
		keys := []string{body.File + "/" + body.Function, "callee/" + body.Function, "capture/" + body.Function}
		if !needed[keys[0]] && !needed[keys[1]] && !needed[keys[2]] {
			continue
		}
		if loaded[body.File] == nil {
			loaded[body.File], err = debtServedDebugDeclarations(filepath.Join(root, body.File))
			if err != nil {
				return err
			}
		}
		previous, err := debtSourceForkSourcePrevious(root, body.File, body.Function, loaded[body.File][body.Function])
		if err != nil {
			return err
		}
		previous, err = debtRunIdentitySourcePrevious(root, body.File, body.Function, previous)
		if err != nil {
			return err
		}
		previous, err = debtWorkflowStateSourcePrevious(root, body.File, body.Function, previous)
		if err != nil {
			return err
		}
		previous, err = debtRequestKeySourcePrevious(root, body.File, body.Function, previous)
		if err != nil {
			return err
		}
		previous, err = debtReceiverReadbackSourcePrevious(root, body.File, body.Function, previous)
		if err != nil {
			return err
		}
		if body.After == "" || previous != body.After {
			return fmt.Errorf("reviewed served-debug reader provenance/body changed: %s/%s", body.File, body.Function)
		}
		for _, key := range keys {
			delete(needed, key)
		}
	}
	if len(needed) != 0 {
		return fmt.Errorf("served-debug source proof lacks required callers/owners/captures: %v", needed)
	}
	return nil
}

const debtWorkflowStateFile = "internal/serveapp/selected_fork_pending_input_mixed_test.go"
const debtWorkflowStateFunction = "TestSelectedForkPendingInputMixedCompletionBothStores"
const debtWorkflowStateManifest = "tools/fixture-codemod/pipeline-observations/family145-workflow-state-preimages.json"

func debtWorkflowStateBody(root string) (debtRequestKeyBody, error) {
	var selected debtRequestKeyBody
	raw, err := os.ReadFile(filepath.Join(root, debtWorkflowStateManifest))
	if err != nil || debtBaselineChecksum(raw) != "25d031ef995292f317b42fcd901405ca22945818948369ef40a0b4735540cb1f" {
		return selected, fmt.Errorf("exact workflow-state source manifest changed: %v", err)
	}
	var bodies []debtRequestKeyBody
	if err := json.Unmarshal(raw, &bodies); err != nil || len(bodies) != 12 {
		return selected, fmt.Errorf("workflow-state complete declaration inventory changed: %v", err)
	}
	count := 0
	for _, body := range bodies {
		if body.File == debtWorkflowStateFile && body.Function == debtWorkflowStateFunction {
			selected, count = body, count+1
		}
	}
	if count != 1 || selected.SHA256 != "0d53178f013abec650c211c09b504834e05118d95ac818ec13330af35529de72" ||
		debtBaselineChecksum([]byte(selected.Before)) != selected.SHA256 ||
		debtBaselineChecksum([]byte(selected.After)) != "fa0ee686114e34250674dadf96618abc3f3ded16800da56048b77d2b08559748" {
		return selected, fmt.Errorf("workflow-state mixed-completion predecessor/current source is not exact")
	}
	return selected, nil
}

func debtWorkflowStateNativePins(root string) error {
	for _, pin := range [][2]string{
		{"internal/store/internal/backend/pipelinepersistence/workflow_state_entity_observation.go", "5b0614e523e1ea8b709dd83de935cf26712e3219449211e625cab607112ef22c"},
		{"internal/store/internal/backend/entityruntime/run_state_cardinality.go", "0e28ecd04a85bc32e5c0288502aed08173fc1e22bd98e29497a79ec6bff438fc"},
		{"internal/store/internal/runtimepersistence/test_workflow_state_observation.go", "4f3a45e563a3edf79abcd6ab26871cd5955da67e8f0e8e1088762dfe71bad336"},
	} {
		raw, err := os.ReadFile(filepath.Join(root, pin[0]))
		if err != nil || debtBaselineChecksum(raw) != pin[1] {
			return fmt.Errorf("workflow-state native owner changed: %s: %v", pin[0], err)
		}
	}
	return nil
}

// This one complete inverse composes source evidence, never census occurrences.
func debtWorkflowStateSourcePrevious(root, file, function, current string) (string, error) {
	if file != debtWorkflowStateFile || function != debtWorkflowStateFunction ||
		debtBaselineChecksum([]byte(current)) != "fa0ee686114e34250674dadf96618abc3f3ded16800da56048b77d2b08559748" {
		return current, nil
	}
	body, err := debtWorkflowStateBody(root)
	if err != nil {
		return "", err
	}
	if err := debtWorkflowStateNativePins(root); err != nil {
		return "", err
	}
	return body.Before, nil
}

func debtWorkflowStateSourcePins(root string) error {
	body, err := debtWorkflowStateBody(root)
	if err != nil {
		return err
	}
	declarations, err := debtServedDebugDeclarations(filepath.Join(root, debtWorkflowStateFile))
	if err != nil || declarations[debtWorkflowStateFunction] != body.After {
		return fmt.Errorf("workflow-state mixed-completion current source changed: %v", err)
	}
	return debtWorkflowStateNativePins(root)
}

func debtWorkflowStateCollectorTransition(base, current string, trusted, head authorityDebtBaseline, proofs ...debtDeliveryRetirementProof) bool {
	if current != debtWorkflowStateCollectorTo || head.Collector != current || trusted.Collector != base || len(proofs) != 1 ||
		trusted.BootstrapSource != head.BootstrapSource || debtWorkflowStateSourcePins(proofs[0].Root) != nil {
		return false
	}
	if base == debtEventNameCollectorTo {
		return len(authorityDebtRatchet(proofs[0].Actual, head.Sites, trusted.Sites, trusted.Sites, proofs...)) == 0
	}
	intermediate := head
	intermediate.Collector = debtEventNameCollectorTo
	return debtEventNameCollectorTransition(base, intermediate.Collector, trusted, intermediate, proofs...)
}

func debtServedDebugSourceTransition(root string, actual, source map[string]authorityDebtSite) error {
	for _, pair := range debtServedDebugSignatureTransitions() {
		if actual[pair[1].identity()] == pair[1] && source[pair[0].identity()] == pair[0] {
			return debtServedDebugSourcePins(root)
		}
	}
	return nil // Landed no-op or genuine deletion does not reconstruct authority.
}

func debtServedDebugCollectorTransition(base, current string, trusted, head authorityDebtBaseline) bool {
	if current != debtServedDebugCollectorTo || head.Collector != current || trusted.Collector != base {
		return false
	}
	if base != debtWriterStageCollectorTo {
		intermediate := trusted
		intermediate.Collector = debtWriterStageCollectorTo
		if debtValidateCollectorIdentity(base, intermediate.Collector, trusted, intermediate, "") != nil {
			return false
		}
	}
	for _, pair := range debtServedDebugSignatureTransitions() {
		if trusted.Sites[pair[0].identity()] != pair[0] || trusted.Sites[pair[1].identity()].Multiplicity != 0 ||
			(head.Sites[pair[0].identity()] != pair[0] && head.Sites[pair[1].identity()] != pair[1]) {
			return false
		}
	}
	return len(authorityDebtRatchet(head.Sites, head.Sites, trusted.Sites, trusted.Sites)) == 0
}

func debtQuiescenceUncertaintyTransition() (authorityDebtSite, authorityDebtSite) {
	old := authorityDebtSite{
		Kind: "unresolved-excluded-source", File: "internal/serveapp/a2_nested_portfolio_isolation_test.go",
		Declaration: "TestA2NestedPortfolioSamePeriodAndMemberIsolationBothStores",
		Operation:   "declaration:facfe2d5f45cc0bfd4df3a8a37eddc3609a5f880fd001b91a277966bf6d9a802",
		Resolved:    "unresolved|source-context:4b4e4348dc36ff1ca795a129d908f43ec58f41675314b93feca26224afffc5b4",
		Family:      "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase",
		Multiplicity: 1,
	}
	current := old
	current.Operation = "declaration:02a974ebc762c71be9999ece645b4d35076f0518811a05b09851433a9a67ff06"
	return old, current
}

// Ruling6139798019 appends one counted uncertainty receipt to the prior pair.
func debtPublicA2EndpointTransition() (authorityDebtSite, authorityDebtSite) {
	_, old := debtQuiescenceUncertaintyTransition()
	current := old
	current.Operation = "declaration:07c61f6897d875c893313af0f6c3e68fb338a9e08fd58c7f6c85ee7af0331f48"
	return old, current
}

func debtPublicA2EndpointComparisons(actual, head, base, source map[string]authorityDebtSite) (map[string]authorityDebtSite, map[string]authorityDebtSite, map[string]authorityDebtSite, []string) {
	ancestor, _ := debtQuiescenceUncertaintyTransition()
	old, current := debtPublicA2EndpointTransition()
	for _, state := range []map[string]authorityDebtSite{actual, head, base, source} {
		present := 0
		for _, site := range []authorityDebtSite{ancestor, old, current} {
			if state[site.identity()].Multiplicity > 1 {
				return head, base, source, []string{"reviewed A2 uncertainty multiplicity increased"}
			}
			if state[site.identity()].Multiplicity > 0 {
				present++
			}
		}
		if present > 1 {
			return head, base, source, []string{"reviewed ancestor/intermediate/current A2 uncertainty identities coexist"}
		}
	}
	if actual[current.identity()] != current {
		return head, base, source, nil
	}
	for _, state := range []map[string]authorityDebtSite{head, base, source} {
		if state[ancestor.identity()] != ancestor && state[old.identity()] != old && state[current.identity()] != current {
			return head, base, source, []string{"reviewed A2 uncertainty lacks exact comparison donor"}
		}
	}
	forward := func(state map[string]authorityDebtSite) map[string]authorityDebtSite {
		state = debtForwardQuiescenceUncertainty(state, ancestor, old)
		return debtForwardQuiescenceUncertainty(state, old, current)
	}
	return forward(head), forward(base), forward(source), nil
}

func debtPublicA2EndpointSourcePins(root string) error {
	const manifest = "tools/fixture-codemod/pipeline-observations/internal/publicendpoint/preimages.json"
	raw, err := os.ReadFile(filepath.Join(root, manifest))
	if err != nil || debtBaselineChecksum(raw) != "4f37a50d17478fdbd4cb64bb8df0d73c5722c7e92578878e20e277950cc83d1c" {
		return fmt.Errorf("reviewed A2 complete source manifest changed: %v", err)
	}
	var pins []struct{ File, Function, Before, After, SHA256 string }
	if err := json.Unmarshal(raw, &pins); err != nil || len(pins) != 16 {
		return fmt.Errorf("invalid A2 complete source inventory: %v", err)
	}
	old, current := debtPublicA2EndpointTransition()
	for _, pin := range pins {
		if pin.File != current.File || pin.Function != current.Declaration {
			continue
		}
		if "declaration:"+debtBaselineChecksum([]byte(pin.Before)) != old.Operation ||
			"declaration:"+debtBaselineChecksum([]byte(pin.After)) != current.Operation || pin.SHA256 != strings.TrimPrefix(old.Operation, "declaration:") {
			return fmt.Errorf("A2 source manifest lost exact before/after provenance")
		}
		bodies, err := debtServedDebugDeclarations(filepath.Join(root, pin.File))
		if err != nil || bodies[pin.Function] != pin.After {
			return fmt.Errorf("current A2 uncertainty body differs from reviewed successor: %v", err)
		}
		return nil
	}
	return fmt.Errorf("reviewed A2 uncertainty source missing from manifest")
}

func debtPublicA2EndpointCollectorTransition(base, current string, trusted, head authorityDebtBaseline) bool {
	if current != debtPublicA2EndpointCollectorTo || head.Collector != current || trusted.Collector != base {
		return false
	}
	if base != debtServedDebugCollectorTo {
		intermediate := trusted
		intermediate.Collector = debtServedDebugCollectorTo
		if debtValidateCollectorIdentity(base, intermediate.Collector, trusted, intermediate, "") != nil {
			return false
		}
	}
	ancestor, _ := debtQuiescenceUncertaintyTransition()
	old, successor := debtPublicA2EndpointTransition()
	if trusted.Sites[ancestor.identity()] != ancestor && trusted.Sites[old.identity()] != old {
		return false
	}
	if head.Sites[ancestor.identity()] != ancestor && head.Sites[old.identity()] != old && head.Sites[successor.identity()] != successor {
		return false
	}
	return len(authorityDebtRatchet(head.Sites, head.Sites, trusted.Sites, trusted.Sites)) == 0
}

// Ruling6140617142 permits one full-body successor, not another identity pair.
func debtReceiverReadbackSourcePrevious(root, file, function, current string) (string, error) {
	if file != "internal/serveapp/receiver_failure_settlement_test.go" || function != "TestReceiverCompositionFailureSettlementBothStores" {
		return current, nil
	}
	const beforeHash = "2f7cfdc7e2c181c70f1eea10c631613f3dcb6b63d9d3ba3b8b9e79e58461d27c"
	if debtBaselineChecksum([]byte(current)) == beforeHash {
		return current, nil
	}
	raw, err := os.ReadFile(filepath.Join(root, "tools/fixture-codemod/pipeline-observations/family145-receiver-public-readback-preimages.json"))
	if err != nil || debtBaselineChecksum(raw) != "a75db13f04646436e22522c83c9a04d929d12e384d9b48bfa585fe5bdf499709" {
		return "", fmt.Errorf("receiver readback complete source manifest changed: %v", err)
	}
	var pins []struct{ File, Function, Before, After, SHA256 string }
	if err := json.Unmarshal(raw, &pins); err != nil || len(pins) != 15 {
		return "", fmt.Errorf("receiver readback exact inventory changed: %v", err)
	}
	for _, pin := range pins {
		if pin.File == file && pin.Function == function && pin.After == current &&
			pin.SHA256 == beforeHash && debtBaselineChecksum([]byte(pin.Before)) == beforeHash &&
			debtBaselineChecksum([]byte(current)) == "aa4f1230b22b93fa4e5b8674607a35cab21f709d33d329076371d75371c5a775" {
			return pin.Before, nil
		}
	}
	return "", fmt.Errorf("receiver readback source escaped the one reviewed complete successor")
}

func debtReceiverReadbackCollectorTransition(base, current string, trusted, head authorityDebtBaseline) bool {
	if current != debtReceiverReadbackCollectorTo || head.Collector != current || trusted.Collector != base {
		return false
	}
	if base != debtPublicA2EndpointCollectorTo {
		intermediate := trusted
		intermediate.Collector = debtPublicA2EndpointCollectorTo
		if debtValidateCollectorIdentity(base, intermediate.Collector, trusted, intermediate, "") != nil {
			return false
		}
	}
	return len(authorityDebtRatchet(head.Sites, head.Sites, trusted.Sites, trusted.Sites)) == 0
}

func debtForwardQuiescenceUncertainty(sites map[string]authorityDebtSite, old, current authorityDebtSite) map[string]authorityDebtSite {
	if sites[old.identity()] != old || sites[current.identity()].Multiplicity != 0 {
		return sites
	}
	forward := make(map[string]authorityDebtSite, len(sites))
	for key, site := range sites {
		forward[key] = site
	}
	delete(forward, old.identity())
	forward[current.identity()] = current
	return forward
}

func parseAuthorityDebtBaseline(data []byte) (authorityDebtBaseline, error) {
	baseline := authorityDebtBaseline{Sites: map[string]authorityDebtSite{}}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), 4<<20)
	var prior string
	schema, header := false, false
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "# persistence-authority-debt-v1":
			if schema {
				return baseline, fmt.Errorf("duplicate debt schema")
			}
			schema = true
		case strings.HasPrefix(line, "# bootstrap-source="):
			if baseline.BootstrapSource != "" {
				return baseline, fmt.Errorf("duplicate bootstrap source")
			}
			baseline.BootstrapSource = strings.TrimPrefix(line, "# bootstrap-source=")
		case strings.HasPrefix(line, "# collector="):
			if baseline.Collector != "" {
				return baseline, fmt.Errorf("duplicate collector")
			}
			baseline.Collector = strings.TrimPrefix(line, "# collector=")
		case line == "# kind\tfile\tdeclaration\toperation\tresolved\tfamily\treplacement-owner\tmultiplicity":
			if header {
				return baseline, fmt.Errorf("duplicate debt header")
			}
			header = true
		case line == "":
			return baseline, fmt.Errorf("blank debt baseline row")
		case strings.HasPrefix(line, "#"):
			return baseline, fmt.Errorf("unknown debt baseline metadata %q", line)
		default:
			parts := strings.Split(line, "\t")
			if len(parts) != 8 {
				return baseline, fmt.Errorf("corrupt debt baseline row: %q", line)
			}
			count, err := strconv.Atoi(parts[7])
			if err != nil || count <= 0 {
				return baseline, fmt.Errorf("invalid debt multiplicity %q", parts[7])
			}
			for _, part := range parts[:7] {
				if part == "" || strings.ContainsAny(part, "\r\n") {
					return baseline, fmt.Errorf("missing debt identity field")
				}
			}
			if !filepath.IsLocal(parts[1]) || filepath.ToSlash(parts[1]) != parts[1] {
				return baseline, fmt.Errorf("invalid debt consumer path")
			}
			site := authorityDebtSite{parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6], count}
			key := site.identity()
			if prior != "" && key <= prior {
				return baseline, fmt.Errorf("duplicate or unsorted debt site %q", key)
			}
			prior = key
			baseline.Sites[key] = site
		}
	}
	if err := scanner.Err(); err != nil {
		return baseline, err
	}
	if !schema || !header || !authorityDebtHex(baseline.BootstrapSource, 40) || !authorityDebtHex(baseline.Collector, 64) {
		return baseline, fmt.Errorf("missing/corrupt debt schema, source or collector identity")
	}
	return baseline, nil
}

func authorityDebtHex(value string, size int) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(value) == size && len(decoded) == size/2 && strings.ToLower(value) == value
}

func marshalAuthorityDebtBaseline(baseline authorityDebtBaseline) []byte {
	var out strings.Builder
	fmt.Fprintf(&out, "# persistence-authority-debt-v1\n# bootstrap-source=%s\n# collector=%s\n# kind\tfile\tdeclaration\toperation\tresolved\tfamily\treplacement-owner\tmultiplicity\n", baseline.BootstrapSource, baseline.Collector)
	keys := make([]string, 0, len(baseline.Sites))
	for key := range baseline.Sites {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(&out, "%s\t%d\n", key, baseline.Sites[key].Multiplicity)
	}
	return []byte(out.String())
}

func debtCollectorDigest(root string) (string, error) {
	// Include the whole census implementation and the ratchet/scope policy.
	// Hostile tests and generated debt rows do not define the permission model.
	hash := sha256.New()
	for _, path := range []string{"internal/store/persistence_authority_debt_census_test.go", "internal/store/persistence_authority_debt_test.go", "internal/store/persistence_authority_debt_cache_test.go", "internal/store/persistence_authority_debt_sidecar_test.go"} {
		if path == "internal/store/persistence_authority_debt_cache_test.go" || path == "internal/store/persistence_authority_debt_sidecar_test.go" {
			if _, err := os.Stat(filepath.Join(root, path)); os.IsNotExist(err) {
				continue // Only immutable predecessor archives predate these owners.
			}
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, path), nil, parser.AllErrors)
		if err != nil {
			return "", err
		}
		var canonical bytes.Buffer
		if err := format.Node(&canonical, token.NewFileSet(), file); err != nil {
			return "", err
		}
		fmt.Fprintf(hash, "%s\x00%s\x00", path, canonical.Bytes())
	}
	legacy := hex.EncodeToString(hash.Sum(nil))
	const rolePath = "internal/checkoutsource/fixture_role.go"
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, rolePath), nil, parser.AllErrors)
	if os.IsNotExist(err) && (legacy == debtFixtureRoleCollectorFrom || legacy == debtRunFixtureCollectorFrom || legacy == debtQuiescenceCollectorFrom || legacy == debtG01CollectorFrom || legacy == debtCacheCollectorFrom) {
		return legacy, nil // Exact immutable policies predating the shared role owner.
	}
	if err != nil {
		return "", err
	}
	var canonical bytes.Buffer
	if err := format.Node(&canonical, token.NewFileSet(), file); err != nil {
		return "", err
	}
	fmt.Fprintf(hash, "%s\x00%s\x00", rolePath, canonical.Bytes())
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func debtGit(root string, args ...string) ([]byte, error) {
	command := exec.Command("git", append([]string{"-C", root}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("git %v: %w: %s", args, err, output)
	}
	return output, nil
}

func debtTrustedBase(root string) (string, error) {
	target := "origin/master"
	candidate := ""
	if eventPath := os.Getenv("GITHUB_EVENT_PATH"); eventPath != "" {
		data, err := os.ReadFile(eventPath)
		if err != nil {
			return "", err
		}
		var event struct {
			Before      string `json:"before"`
			After       string `json:"after"`
			PullRequest *struct {
				Base struct {
					SHA string `json:"sha"`
				} `json:"base"`
				Head struct {
					SHA string `json:"sha"`
				} `json:"head"`
			} `json:"pull_request"`
		}
		if err := json.Unmarshal(data, &event); err != nil {
			return "", err
		}
		if event.PullRequest != nil {
			target = event.PullRequest.Base.SHA
			candidate = event.PullRequest.Head.SHA
			if !authorityDebtHex(target, 40) || !authorityDebtHex(candidate, 40) {
				return "", fmt.Errorf("missing authenticated PR base/head identity")
			}
		} else if event.Before != "" || event.After != "" {
			if !authorityDebtHex(event.Before, 40) || !authorityDebtHex(event.After, 40) {
				return "", fmt.Errorf("missing authenticated push predecessor/head identity")
			}
			head, err := debtGit(root, "rev-parse", "HEAD")
			if err != nil {
				return "", err
			}
			if strings.TrimSpace(string(head)) != event.After {
				return "", fmt.Errorf("push event does not name the tested head")
			}
			target = event.Before
		}
	}
	// Hosted proof checkouts are shallow. Retrieve history, not a caller-chosen
	// baseline file; qualification remains bound to the actual merge base.
	shallow, err := debtGit(root, "rev-parse", "--is-shallow-repository")
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(string(shallow)) == "true" {
		if _, err := debtGit(root, "fetch", "--no-tags", "--unshallow", "origin"); err != nil {
			return "", err
		}
	}
	if _, err := debtGit(root, "cat-file", "-e", target+"^{commit}"); err != nil {
		if !authorityDebtHex(target, 40) {
			return "", err
		}
		if _, err := debtGit(root, "fetch", "--no-tags", "origin", target); err != nil {
			return "", err
		}
	}
	comparison := "HEAD"
	if candidate != "" {
		entries, err := debtGit(root, "ls-tree", target, "--", debtBaselinePath)
		if err != nil {
			return "", err
		}
		if len(bytes.TrimSpace(entries)) == 0 {
			history, err := debtGit(root, "log", "-1", "--format=%H", target, "--", debtBaselinePath)
			if err != nil {
				return "", err
			}
			if len(bytes.TrimSpace(history)) != 0 {
				return "", fmt.Errorf("landed debt baseline disappeared from authenticated lineage; rebootstrap refused")
			}
			// Initial bootstrap is tied to the candidate's extraction base, not
			// GitHub's synthetic merge. Once landed, the tested integration tree
			// consumes the already-landed baseline without a rebootstrap.
			if _, err := debtGit(root, "cat-file", "-e", candidate+"^{commit}"); err != nil {
				if _, err := debtGit(root, "fetch", "--no-tags", "origin", candidate); err != nil {
					return "", err
				}
			}
			comparison = candidate
		}
	}
	base, err := debtGit(root, "merge-base", comparison, target)
	if err != nil {
		return "", err
	}
	sha := strings.TrimSpace(string(base))
	if head, err := debtGit(root, "rev-parse", "HEAD"); err != nil {
		return "", err
	} else if sha == strings.TrimSpace(string(head)) {
		// A local checkout at master still needs an independent predecessor.
		parent, err := debtGit(root, "rev-parse", "HEAD^")
		if err != nil {
			return "", err
		}
		sha = strings.TrimSpace(string(parent))
	}
	if !authorityDebtHex(sha, 40) {
		return "", fmt.Errorf("invalid trusted integration base")
	}
	return sha, nil
}

func materializeDebtBase(t *testing.T, root, sha string) string {
	t.Helper()
	directory := t.TempDir()
	command := exec.Command("git", "-C", root, "archive", "--format=tar", sha)
	pipe, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	reader := tar.NewReader(pipe)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			command.Process.Kill()
			command.Wait()
			t.Fatal(err)
		}
		if !filepath.IsLocal(header.Name) {
			command.Process.Kill()
			command.Wait()
			t.Fatalf("nonlocal trusted archive member %s", header.Name)
		}
		path := filepath.Join(directory, header.Name)
		switch header.Typeflag {
		case tar.TypeXGlobalHeader, tar.TypeXHeader:
			// git archive records its source commit in a PAX metadata header.
			continue
		case tar.TypeDir:
			err = os.MkdirAll(path, 0755)
		case tar.TypeReg:
			err = os.MkdirAll(filepath.Dir(path), 0755)
			if err != nil {
				break
			}
			var file *os.File
			file, err = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(header.Mode)&0777)
			if err != nil {
				break
			}
			_, err = io.Copy(file, reader)
			err = errors.Join(err, file.Close())
		case tar.TypeSymlink:
			err = os.MkdirAll(filepath.Dir(path), 0755)
			if err == nil {
				err = os.Symlink(header.Linkname, path)
			}
		default:
			err = fmt.Errorf("unsupported trusted archive entry %s", header.Name)
		}
		if err != nil {
			command.Process.Kill()
			command.Wait()
			t.Fatal(err)
		}
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("archive trusted base: %v: %s", err, stderr.String())
	}
	return directory
}

func debtCheckBootstrapAncestry(root, origin, integrationBase string) error {
	if !authorityDebtHex(origin, 40) {
		return fmt.Errorf("invalid bootstrap origin")
	}
	base, err := debtGit(root, "merge-base", origin, integrationBase)
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(base)) != origin {
		return fmt.Errorf("bootstrap origin is not an ancestor of the trusted integration base")
	}
	return nil
}

func TestPersistenceAuthorityDebtRatchet(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	base, err := debtTrustedBase(root)
	if err != nil {
		t.Fatal(err)
	}
	collector, err := debtCollectorDigest(root)
	if err != nil {
		t.Fatal(err)
	}
	baseRoot := materializeDebtBase(t, root, base)
	cache := debtAnalysisCacheForCheckout(t, root)
	baseActual := cache.baseSites(t, baseRoot, base)
	headFindings := debtLoadPersistenceAuthorityFindings(t, root)
	headFindings = append(headFindings, debtSelectedBoundaryFindings(t, root)...)
	actual := authorityDebtSites(headFindings)
	baseBytes, baseErr := os.ReadFile(filepath.Join(baseRoot, debtBaselinePath))
	headBytes, headErr := os.ReadFile(filepath.Join(root, debtBaselinePath))
	refresh := os.Getenv(debtRefreshEnv)
	if refresh != "" && refresh != "downward" {
		t.Fatalf("%s supports only downward", debtRefreshEnv)
	}
	if refresh != "" && (os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "") {
		t.Fatal("normal CI is read-only; debt refresh refused")
	}
	bootstrap := os.IsNotExist(baseErr)
	if bootstrap {
		history, err := debtGit(root, "log", "-1", "--format=%H", base, "--", debtBaselinePath)
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(string(history)) != "" {
			t.Fatal("landed debt baseline disappeared from trusted lineage; rebootstrap refused")
		}
	}
	var trusted, head authorityDebtBaseline
	if bootstrap {
		if err := debtCheckBootstrapAncestry(root, debtBootstrapSource, base); err != nil {
			t.Fatal(err)
		}
		originActual := baseActual
		if base != debtBootstrapSource {
			originRoot := materializeDebtBase(t, root, debtBootstrapSource)
			originFindings := debtLoadPersistenceAuthorityFindings(t, originRoot)
			originFindings = append(originFindings, debtSelectedBoundaryFindings(t, originRoot)...)
			originActual = authorityDebtSites(originFindings)
		}
		trusted = authorityDebtBaseline{BootstrapSource: debtBootstrapSource, Collector: collector, Sites: originActual}
		if headErr != nil {
			if !os.IsNotExist(headErr) || refresh != "downward" {
				t.Fatalf("initial debt baseline missing: %v; explicit downward bootstrap required", headErr)
			}
			head = trusted
		} else {
			head, err = parseAuthorityDebtBaseline(headBytes)
			if err != nil {
				t.Fatal(err)
			}
		}
	} else {
		if baseErr != nil {
			t.Fatal(baseErr)
		}
		baseCollector, err := debtCollectorDigest(baseRoot)
		if err != nil {
			t.Fatal(err)
		}
		trusted, err = debtBaselineWithCollector(baseRoot, baseBytes, baseCollector, true)
		if err != nil {
			t.Fatal(err)
		}
		if headErr != nil {
			t.Fatalf("landed debt baseline cannot be removed or bootstrapped again: %v", headErr)
		}
		head, err = debtBaselineWithCollector(root, headBytes, collector, false)
		if err != nil {
			t.Fatal(err)
		}
		if err := debtValidateCollectorIdentity(baseCollector, collector, trusted, head, collector, debtDeliveryRetirementProof{Root: root, Actual: actual}); err != nil {
			t.Fatal(err)
		}
	}
	if head.BootstrapSource != trusted.BootstrapSource || head.Collector != collector {
		t.Fatal("debt baseline was reseeded or changed collector identity")
	}
	if bootstrap && !debtSitesEqual(head.Sites, trusted.Sites) {
		t.Fatal("bootstrap baseline must equal actual extraction-base debt exactly")
	}
	if err := debtStageCompletionSourceTransition(root, baseRoot, actual, baseActual); err != nil {
		t.Fatal(err)
	}
	if err := debtWriterStageSourceTransition(root, baseRoot, actual, baseActual); err != nil {
		t.Fatal(err)
	}
	if err := debtServedDebugSourceTransition(root, actual, baseActual); err != nil {
		t.Fatal(err)
	}
	_, a2Successor := debtPublicA2EndpointTransition()
	if actual[a2Successor.identity()] == a2Successor {
		if err := debtPublicA2EndpointSourcePins(root); err != nil {
			t.Fatal(err)
		}
	}
	if err := debtRequestKeySourceTransition(root, actual, baseActual); err != nil {
		t.Fatal(err)
	}
	failures := authorityDebtRatchet(actual, head.Sites, trusted.Sites, baseActual, debtDeliveryRetirementProof{Root: root, Actual: actual})
	if len(failures) > 0 {
		t.Fatalf("authority debt ratchet failed: base=%s old=%d head=%d actual=%d\n%s", base, authorityDebtCount(trusted.Sites), authorityDebtCount(head.Sites), authorityDebtCount(actual), strings.Join(failures, "\n"))
	}
	if refresh == "downward" {
		removed := authorityDebtCount(head.Sites) - authorityDebtCount(actual)
		normalized, normalizedBase, normalizedSource, _ := debtStageCompletionComparisons(actual, head.Sites, trusted.Sites, baseActual)
		normalized, _, _, _ = debtRunFixtureSignatureComparisons(actual, normalized, normalizedBase, normalizedSource)
		normalized, _, _, _ = debtWriterStageSignatureComparisons(actual, normalized, normalizedBase, normalizedSource)
		normalized, _, _, _ = debtServedDebugSignatureComparisons(actual, normalized, normalizedBase, normalizedSource)
		normalized, _, _, _ = debtPublicA2EndpointComparisons(actual, normalized, normalizedBase, normalizedSource)
		normalized, _, _, _ = debtRequestKeySignatureComparisons(actual, normalized, normalizedBase, normalizedSource)
		for _, key := range debtSortedKeys(normalized) {
			before := normalized[key]
			after := actual[key].Multiplicity
			if after < before.Multiplicity {
				t.Logf("debt removed=%d %s", before.Multiplicity-after, key)
			}
		}
		head.Sites = actual
		// Preserve the historical TSV header; active collector identity is sidecar-owned.
		head.Collector = debtCacheCollectorFrom
		data := marshalAuthorityDebtBaseline(head)
		if err := os.WriteFile(filepath.Join(root, debtBaselinePath), data, 0644); err != nil {
			t.Fatal(err)
		}
		metadata, err := debtCollectorMetadataBytes(debtCollectorMetadata{Version: 1, Collector: collector, BaselineSHA256: debtBaselineChecksum(data), Predecessor: debtCacheCollectorFrom})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, debtCollectorSidecarPath), metadata, 0644); err != nil {
			t.Fatal(err)
		}
		t.Logf("downward-only refresh removed=%d added=0", removed)
	}
	rawSites := 0
	for _, finding := range headFindings {
		if finding.Kind == "raw-operation" {
			rawSites++
		}
	}
	unresolved := 0
	confirmedRaw := 0
	for _, site := range actual {
		if site.Kind == "unresolved-excluded-source" {
			unresolved += site.Multiplicity
		} else if site.Kind == "raw-operation" {
			confirmedRaw += site.Multiplicity
		}
	}
	t.Logf("debt source=%s collector=%s total-findings=%d raw-operation-sites=%d debt=%d inherited-baseline=%d confirmed-raw-operation-debt=%d unresolved-excluded-occurrences=%d", base, collector, len(headFindings), rawSites, authorityDebtCount(actual), authorityDebtCount(head.Sites), confirmedRaw, unresolved)
}

func debtSortedKeys(sites map[string]authorityDebtSite) []string {
	keys := make([]string, 0, len(sites))
	for key := range sites {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
func debtValidateCollectorIdentity(base, current string, trusted, head authorityDebtBaseline, reviewedTransition string, retirement ...debtDeliveryRetirementProof) error {
	if head.BootstrapSource != trusted.BootstrapSource {
		return fmt.Errorf("debt baseline bootstrap source changed")
	}
	if base == current && trusted.Collector == current && head.Collector == current {
		return nil
	}
	if debtPublicRollbackCollectorTransition(base, current, trusted, head, retirement...) || debtRunIdentityCollectorTransition(base, current, trusted, head, retirement...) || debtReviewedCollectorTransition(base, current, trusted, head) || debtDeliveryRetirementCollectorTransition(base, current, trusted, head, retirement...) || debtEventNameCollectorTransition(base, current, trusted, head, retirement...) || debtWorkflowStateCollectorTransition(base, current, trusted, head, retirement...) {
		return nil
	}
	if current == debtFixtureRoleCollectorTo && head.Collector == current {
		if base == debtFixtureRoleCollectorFrom {
			if trusted.Collector == base && debtSitesEqual(head.Sites, trusted.Sites) {
				return nil
			}
		} else {
			// Compose only the already-approved chain into the exact old policy;
			// the final extraction changes metadata, never its intermediate rows.
			intermediate := head
			intermediate.Collector = debtFixtureRoleCollectorFrom
			if debtValidateCollectorIdentity(base, debtFixtureRoleCollectorFrom, trusted, intermediate, "") == nil {
				return nil
			}
		}
	}
	if base == debtG01CollectorFrom && trusted.Collector == debtG01CollectorFrom &&
		current == debtG01CollectorTo && head.Collector == debtG01CollectorTo &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil
	}
	if base == debtCacheCollectorFrom && trusted.Collector == debtCacheCollectorFrom &&
		current == reviewedTransition && authorityDebtHex(reviewedTransition, 64) &&
		head.Collector == current && debtSitesEqual(head.Sites, trusted.Sites) {
		return nil
	}
	if current == debtRunFixtureCollectorTo && head.Collector == debtRunFixtureCollectorTo &&
		debtRunFixtureSignatureCollectorTransition(trusted.Sites, head.Sites) &&
		debtRunFixtureCollectorPredecessor(base, trusted, head) {
		return nil
	}
	old, forward := debtQuiescenceUncertaintyTransition()
	if base == debtQuiescenceCollectorFrom && trusted.Collector == debtQuiescenceCollectorFrom &&
		current == debtQuiescenceCollectorTo && head.Collector == debtQuiescenceCollectorTo &&
		trusted.Sites[old.identity()] == old && trusted.Sites[forward.identity()].Multiplicity == 0 &&
		(head.Sites[old.identity()] == old || head.Sites[forward.identity()] == forward) &&
		len(authorityDebtRatchet(head.Sites, head.Sites, trusted.Sites, trusted.Sites)) == 0 {
		return nil
	}
	return fmt.Errorf("authority census/role policy changed outside the exact reviewed transition; silent scan narrowing is forbidden")
}

func debtReviewedCollectorTransition(base, current string, trusted, head authorityDebtBaseline) bool {
	return debtStageCompletionCollectorTransition(base, current, trusted, head) ||
		debtWriterStageCollectorTransition(base, current, trusted, head) ||
		debtServedDebugCollectorTransition(base, current, trusted, head) ||
		debtPublicA2EndpointCollectorTransition(base, current, trusted, head) ||
		debtReceiverReadbackCollectorTransition(base, current, trusted, head) ||
		debtRequestKeyCollectorTransition(base, current, trusted, head) ||
		debtRunScalarCollectorTransition(base, current, trusted, head) ||
		debtDeliveryStatusCollectorTransition(base, current, trusted, head)
}

func debtRunFixtureCollectorPredecessor(base string, trusted, head authorityDebtBaseline) bool {
	if base == debtRunFixtureCollectorFrom && trusted.Collector == base {
		return true
	}
	// The accumulated branch may land both reviewed transitions together. Prove
	// the exact preceding quiescence step, rather than admitting a foreign origin.
	if base != debtQuiescenceCollectorFrom || trusted.Collector != base {
		return false
	}
	intermediate := head
	intermediate.Collector = debtRunFixtureCollectorFrom
	return debtValidateCollectorIdentity(base, debtRunFixtureCollectorFrom, trusted, intermediate, "") == nil
}

func debtRunFixtureSignatureCollectorTransition(trusted, head map[string]authorityDebtSite) bool {
	for _, pair := range debtRunFixtureSignatureTransitions() {
		old, current := pair[0], pair[1]
		if trusted[old.identity()] != old || trusted[current.identity()].Multiplicity != 0 ||
			(head[old.identity()] != old && head[current.identity()] != current) {
			return false
		}
	}
	return len(authorityDebtRatchet(head, head, trusted, trusted)) == 0
}

func debtSitesEqual(left, right map[string]authorityDebtSite) bool {
	if len(left) != len(right) {
		return false
	}
	for key, site := range left {
		if right[key] != site {
			return false
		}
	}
	return true
}

func debtSelectedBoundaryFindings(t *testing.T, root string) []authorityFinding {
	t.Helper()
	var findings []authorityFinding
	err := checkoutsource.WalkDir(root, root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case "testdata", "vendor", "node_modules", ".swarm":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		findings = append(findings, debtSelectedBoundarySource(relative, file)...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

func debtSelectedBoundarySource(path string, file *ast.File) []authorityFinding {
	insideStore := strings.HasPrefix(path, "internal/store/")
	insideServe := strings.HasPrefix(path, "internal/serveapp/")
	insideSelected := strings.HasPrefix(path, "internal/store/selected/")
	insideInfrastructure := strings.HasPrefix(path, "internal/testpostgres/") || filepath.ToSlash(filepath.Dir(path)) == "internal/testutil"
	selectedConsumer := insideSelected || insideServe || strings.HasPrefix(path, "internal/cliapp/")
	constructionConsumer := insideSelected || filepath.ToSlash(filepath.Dir(path)) == "internal/store" || strings.HasPrefix(path, "internal/store/construction/") || strings.HasPrefix(path, "internal/store/internal/")
	imports := map[string]string{}
	var findings []authorityFinding
	add := func(declaration, operation, reason string) {
		findings = append(findings, authorityFinding{Kind: "selected-boundary", File: path, Enclosing: declaration, Member: operation, Resolved: reason, RawSQL: true})
	}
	for _, imp := range file.Imports {
		name, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			panic(err)
		}
		alias := filepath.Base(name)
		if imp.Name != nil {
			alias = imp.Name.Name
		}
		imports[alias] = name
		switch name {
		case "github.com/division-sh/swarm/internal/store/selected":
			if !selectedConsumer {
				add(file.Name.Name, "import:"+name, "selected owner outside process construction/inspection roles")
			}
		case "github.com/division-sh/swarm/internal/store/construction":
			if !constructionConsumer {
				add(file.Name.Name, "import:"+name, "direct construction outside selected construction roles")
			}
		case "reflect":
			if !strings.HasSuffix(path, "_test.go") && (insideServe || insideSelected) {
				add(file.Name.Name, "import:reflect", "reflection in selected-store production projection")
			}
		}
	}
	retired := map[string]bool{"storeBundle": true, "selectedConcreteRuntimeStore": true, "selectedRuntimeStoreFacade": true, "selectedStoreBundleRoleLedger": true, "validateSelectedStoreBundleRoles": true, "APIOptionalCapabilityBuilder": true, "configuredRunFork": true, "BundleWriter": true}
	for _, decl := range file.Decls {
		name := file.Name.Name
		if fn, ok := decl.(*ast.FuncDecl); ok {
			name = fn.Name.Name
		}
		ast.Inspect(decl, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok && (insideServe || insideSelected) && retired[id.Name] {
				add(name, "retired:"+id.Name, "retired selected-store interpreter")
			}
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			qualifier, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			if imports[qualifier.Name] == "github.com/division-sh/swarm/internal/store" && (selector.Sel.Name == "PostgresStore" || selector.Sel.Name == "SQLiteRuntimeStore") && (insideServe || (!strings.HasSuffix(path, "_test.go") && !insideStore && !insideInfrastructure)) {
				add(name, "concrete:"+selector.Sel.Name, "concrete selected-store recovery/projection")
			}
			return true
		})
	}
	return findings
}
