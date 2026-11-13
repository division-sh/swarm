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
const debtDeploymentRegistrationCollectorFrom = "4a683b60cf5147aece5eabc4e8f044555f64a15623f9e7106ddb4d9726961f9a"
const debtS1CollectorFrom = "2b1b5a1147e3733dbc57049a2515c05f0b0103e595a666c524232a7ff9012e5a"

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

func authorityDebtRatchet(actual, head, base, baseActual map[string]authorityDebtSite) []string {
	head, base, baseActual, failures := debtRunFixtureSignatureComparisons(actual, head, base, baseActual)
	if len(failures) != 0 {
		return failures
	}
	head, base, baseActual = debtW4LiveFixtureComparisons(actual, head, base, baseActual)
	head, base, baseActual, failures = debtForkProofComparisons(actual, head, base, baseActual)
	if len(failures) != 0 {
		return failures
	}
	head, base, baseActual, failures = debtProcessLeaseRelocationComparisons(actual, head, base, baseActual)
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

// #2764/6360810282 permits exactly these two build-excluded platform stubs.
// They return error/nil and execute no persistence; their full source identities
// remain debt, not a new benign classification or an extraction exclusion.
func debtWithDeploymentRegistrationStubs(sites map[string]authorityDebtSite) map[string]authorityDebtSite {
	out := make(map[string]authorityDebtSite, len(sites)+2)
	for key, site := range sites {
		out[key] = site
	}
	for _, stub := range [2]struct{ name, digest string }{
		{"OpenChannelRegistration", "38dad8ee484412399b136f2a768a54d0cbb866e6bdd788083313df3c413dbdd3"},
		{"deploymentChannelHandlers", "7890217cded1dd01ed764ec3a9fab9d2daad2c882b0920a7d3ead721e5710d50"},
	} {
		site := authorityDebtSite{
			Kind: "unresolved-excluded-source", File: "internal/serveapp/channel_deployment_unsupported.go",
			Declaration: stub.name, Operation: "declaration:" + stub.digest,
			Resolved: "unresolved|source-context:14375e4f0f053ea0b9c5c0715496c22cb494c816197b9efdb94415151ce4d24d",
			Family:   "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase",
			Multiplicity: 1,
		}
		out[site.identity()] = site
	}
	return out
}

// LEAD #2667/6396880696 permits only these two additional counted sites.
// D24's one-symbol classification and every other site's limit are unchanged.
func debtWithS1RetainedSourceConstructors(sites map[string]authorityDebtSite) map[string]authorityDebtSite {
	out := make(map[string]authorityDebtSite, len(sites)+2)
	for key, site := range sites {
		out[key] = site
	}
	for _, result := range [2]string{
		"*github.com/division-sh/swarm/internal/store.PostgresStore",
		"*github.com/division-sh/swarm/internal/store.SQLiteRuntimeStore",
	} {
		site := authorityDebtSite{
			Kind: "selected-store-construction", File: "internal/store/internal/backend/runforkpersistence/selected_rooted_source_test.go",
			Declaration: "newSelectedRootedSourceFixture", Operation: "construction:" + result, Resolved: result,
			Family: "authority-carrier", Replacement: "original selected construction/work-lifetime owner; no getter or transaction-context recovery",
			Multiplicity: 1,
		}
		out[site.identity()] = site
	}
	return out
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

func debtW1RunObservationSignatureTransitions() [10][2]authorityDebtSite {
	const file = "internal/serveapp/standing_ingress_supported_surface_test.go"
	const observation = "domain observation port on the exact selected owner; missing exact port: escalate #2542"
	const unowned = "exact domain owner not identified here; escalate #2542, never invent a SQL escape"
	var pairs [10][2]authorityDebtSite
	for index, names := range [][3]string{
		{"assertServedStandingState", "assertServedFlowRunState", "|arg=*string|arg=*int64|arg=*string|arg=*string"},
		{"loadServedStandingOwnerByFlow", "loadServedLandingRunByFlow", "|arg=*string|arg=*string|arg=*int64"},
	} {
		for offset, site := range []authorityDebtSite{
			{Kind: "raw-operation", File: file, Operation: "call:database/sql.(*database/sql.DB).QueryRowContext", Resolved: "func=func(ctx context.Context, query string, args ...any) *database/sql.Row|recv=*database/sql.DB|arg=context.Context|arg=string|arg=[]any|results=(*database/sql.Row)", Family: "observation", Replacement: observation, Multiplicity: 1},
			{Kind: "raw-operation", File: file, Operation: "call:database/sql.(*database/sql.Row).Scan", Resolved: "func=func(dest ...any) error|recv=*database/sql.Row" + names[2] + "|results=(error)", Family: "observation", Replacement: observation, Multiplicity: 1},
			{Kind: "raw-type", File: file, Operation: "param:db", Resolved: "*database/sql.DB", Family: "unowned-authority", Replacement: unowned, Multiplicity: 1},
		} {
			site.Declaration = names[0]
			current := site
			current.Declaration = names[1]
			if offset == 1 {
				current.Resolved = "func=func(dest ...any) error|recv=*database/sql.Row|arg=*string|results=(error)"
			}
			if index == 0 && offset == 0 {
				current.Resolved = "func=func(ctx context.Context, query string, args ...any) *database/sql.Row|recv=*database/sql.DB|arg=context.Context|arg=string|arg=string|results=(*database/sql.Row)"
			}
			pairs[index*3+offset] = [2]authorityDebtSite{site, current}
		}
	}
	old := authorityDebtSite{Kind: "raw-operation", File: file, Declaration: "TestStandingRestartMixedHealthyAndTerminalProcessParity", Operation: "call:github.com/division-sh/swarm/internal/serveapp.assertServedStandingState", Resolved: "func=func(t *testing.T, db *database/sql.DB, backend string, serviceID string, runID string, generation int64, effectiveState string, runStatus string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=int64|arg=string|arg=string|results=()", Family: "unowned-authority", Replacement: unowned, Multiplicity: 3}
	current := old
	current.Operation = "call:github.com/division-sh/swarm/internal/serveapp.assertServedFlowRunState"
	current.Resolved = "func=func(t *testing.T, db *database/sql.DB, backend string, runID string, runStatus string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|results=()"
	current.Family, current.Replacement = "lifecycle", "selected run lifecycle/candidate/source owner and bounded lifecycle observation"
	pairs[6] = [2]authorityDebtSite{old, current}
	old.Operation = "call:github.com/division-sh/swarm/internal/serveapp.loadServedStandingOwnerByFlow"
	old.Resolved = "func=func(t testing.TB, db *database/sql.DB, backend string, flowID string) (serviceID string, runID string, generation int64)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|results=(string,string,int64)"
	old.Multiplicity = 2
	current = old
	current.Operation = "call:github.com/division-sh/swarm/internal/serveapp.loadServedLandingRunByFlow"
	current.Resolved = "func=func(t testing.TB, db *database/sql.DB, backend string, flowID string) string|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|results=(string)"
	current.Family, current.Replacement = "lifecycle", "selected run lifecycle/candidate/source owner and bounded lifecycle observation"
	pairs[7] = [2]authorityDebtSite{old, current}
	old.File, current.File = "internal/serveapp/channel_delivery_inbound_supported_surface_test.go", "internal/serveapp/channel_delivery_inbound_supported_surface_test.go"
	old.Declaration, current.Declaration = "waitChannelCardMessageID", "waitChannelCardMessageID"
	old.Family = "channel"
	old.Replacement = "selected channel/delivery owner and named channel fault/readback"
	old.Multiplicity, current.Multiplicity = 1, 1
	pairs[8] = [2]authorityDebtSite{old, current}
	old = authorityDebtSite{Kind: "raw-operation", File: "internal/serveapp/standing_ingress_dormancy_supported_surface_test.go", Declaration: "TestDormantIngressProvisionRestartSignedInputBothStores", Operation: "call:database/sql.(*database/sql.Row).Scan", Resolved: "func=func(dest ...any) error|recv=*database/sql.Row|arg=*string|arg=*int64|arg=*string|results=(error)", Family: "observation", Replacement: observation, Multiplicity: 1}
	current = old
	current.Resolved = "func=func(dest ...any) error|recv=*database/sql.Row|arg=*string|arg=*string|results=(error)"
	pairs[9] = [2]authorityDebtSite{old, current}
	return pairs
}

// #2970/6731069804: the same seven oracle calls retain all existing debt;
// only the exact helper signature and three positive-event arguments changed.
func debtW2DeliveryEvidenceSignatureTransitions() [6][2]authorityDebtSite {
	const before = "func=func(t *testing.T, rt github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime, runID string) map[string]github.com/division-sh/swarm/internal/serveapp.servedForkDeliveryEvidence|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|arg=string|results=(map[string]github.com/division-sh/swarm/internal/serveapp.servedForkDeliveryEvidence)"
	const after = "func=func(t *testing.T, rt github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime, runID string, executedEvent ...string) map[string]github.com/division-sh/swarm/internal/serveapp.servedForkDeliveryEvidence|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|arg=string|results=(map[string]github.com/division-sh/swarm/internal/serveapp.servedForkDeliveryEvidence)"
	const positive = "func=func(t *testing.T, rt github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime, runID string, executedEvent ...string) map[string]github.com/division-sh/swarm/internal/serveapp.servedForkDeliveryEvidence|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|arg=string|arg=string|results=(map[string]github.com/division-sh/swarm/internal/serveapp.servedForkDeliveryEvidence)"
	var pairs [6][2]authorityDebtSite
	for index, call := range [6]struct {
		file, declaration, resolved string
		count                       int
	}{
		{"fork_delivery_route_evidence_test.go", "TestServedForkConnectedDeliveryRouteEvidenceOnBothStores", after, 2},
		{"fork_loop_generation_notice_test.go", "requireForkLoopStateEffect", after, 1},
		{"fork_receiver_acquisition_effect_only_test.go", "requireSupplementalForkAcquisitionSettlement", positive, 1},
		{"fork_receiver_ownership_test.go", "requireForkReceiverBusinessMutation", positive, 1},
		{"fork_receiver_ownership_test.go", "requireForkReceiverDelivery", positive, 1},
		{"fork_recipient_authority_test.go", "requireServedForkConsumerAuthority", after, 1},
	} {
		old := authorityDebtSite{
			Kind: "raw-operation", File: "internal/serveapp/" + call.file, Declaration: call.declaration,
			Operation: "call:github.com/division-sh/swarm/internal/serveapp.readServedForkDeliveryEvidence", Resolved: before,
			Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: call.count,
		}
		current := old
		current.Resolved = call.resolved
		pairs[index] = [2]authorityDebtSite{old, current}
	}
	return pairs
}

// #3015/6837884761: nine existing count-one calls keep the original SQL owner.
// Only their explicit fixture preparation-mode signature and arguments changed.
func debtChannelCrashPreparationSignatureTransitions() [9][2]authorityDebtSite {
	const before = "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, token string, withSummary bool) (*github.com/division-sh/swarm/internal/serveapp.channelOnboardingE2EHarness, *database/sql.DB, string)|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|arg=bool|results=(*github.com/division-sh/swarm/internal/serveapp.channelOnboardingE2EHarness,*database/sql.DB,string)"
	const after = "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, token string, withSummary bool, prepareOnly ...bool) (*github.com/division-sh/swarm/internal/serveapp.channelOnboardingE2EHarness, *database/sql.DB, string)|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|arg=bool|results=(*github.com/division-sh/swarm/internal/serveapp.channelOnboardingE2EHarness,*database/sql.DB,string)"
	const prepared = "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, token string, withSummary bool, prepareOnly ...bool) (*github.com/division-sh/swarm/internal/serveapp.channelOnboardingE2EHarness, *database/sql.DB, string)|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|arg=bool|arg=bool|results=(*github.com/division-sh/swarm/internal/serveapp.channelOnboardingE2EHarness,*database/sql.DB,string)"
	const ttlBefore = "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, token string, withSummary bool, draftTTL time.Duration) (*github.com/division-sh/swarm/internal/serveapp.channelOnboardingE2EHarness, *database/sql.DB, string)|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|arg=bool|arg=time.Duration|results=(*github.com/division-sh/swarm/internal/serveapp.channelOnboardingE2EHarness,*database/sql.DB,string)"
	const ttlAfter = "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, token string, withSummary bool, draftTTL time.Duration, prepareOnly ...bool) (*github.com/division-sh/swarm/internal/serveapp.channelOnboardingE2EHarness, *database/sql.DB, string)|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|arg=bool|arg=time.Duration|results=(*github.com/division-sh/swarm/internal/serveapp.channelOnboardingE2EHarness,*database/sql.DB,string)"
	const ttlForwarded = "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, token string, withSummary bool, draftTTL time.Duration, prepareOnly ...bool) (*github.com/division-sh/swarm/internal/serveapp.channelOnboardingE2EHarness, *database/sql.DB, string)|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|arg=bool|arg=time.Duration|arg=[]bool|results=(*github.com/division-sh/swarm/internal/serveapp.channelOnboardingE2EHarness,*database/sql.DB,string)"
	var pairs [9][2]authorityDebtSite
	for index, call := range [9]struct {
		file, declaration, operation, before, after string
	}{
		{"channel_ack_process_supported_surface_test.go", "TestChannelActionAcknowledgmentAbruptProcessDeathPublicJourney", "startChannelAnchorJourney", before, prepared},
		{"channel_delivery_anchor_supported_surface_test.go", "startChannelAnchorJourney", "startChannelAnchorJourneyWithDraftTTL", ttlBefore, ttlForwarded},
		{"channel_delivery_draft_temporal_supported_surface_test.go", "TestChannelDraftTerminalRestartPublicJourney", "startChannelAnchorJourneyWithDraftTTL", ttlBefore, ttlAfter},
		{"channel_delivery_process_supported_surface_test.go", "TestChannelDeliveryAbruptProcessDeathPublicJourney", "startChannelAnchorJourney", before, prepared},
		{"channel_delivery_scan_isolation_supported_surface_test.go", "TestChannelDeliveryCompiledRenderFailureDoesNotStarveReceiptUpdates", "startChannelAnchorJourney", before, after},
		{"channel_delivery_settlement_diagnostic_test.go", "TestChannelDeliverySettlementDiagnosticBothStores", "startChannelAnchorJourney", before, after},
		{"channel_delivery_temporal_supported_surface_test.go", "TestChannelDeliveryHumanTemporalReceiptPublicJourney", "startChannelAnchorJourney", before, after},
		{"channel_delivery_uncertain_copy_supported_surface_test.go", "TestChannelDeliveryUncertainCopyAuthorityPublicJourney", "startChannelAnchorJourney", before, after},
		{"channel_source_lifecycle_supported_surface_test.go", "TestChannelSourceLifecyclePublicJourney", "startChannelAnchorJourney", before, after},
	} {
		old := authorityDebtSite{
			Kind: "raw-operation", File: "internal/serveapp/" + call.file, Declaration: call.declaration,
			Operation: "call:github.com/division-sh/swarm/internal/serveapp." + call.operation, Resolved: call.before,
			Family: "channel", Replacement: "selected channel/delivery owner and named channel fault/readback", Multiplicity: 1,
		}
		current := old
		current.Resolved = call.after
		pairs[index] = [2]authorityDebtSite{old, current}
	}
	return pairs
}

// #2338/6840391584: ten counted constructor signatures; exact facts argument only.
func debtR130ConstructorSignatureTransitions() [10][2]authorityDebtSite {
	return [10][2]authorityDebtSite{
		{
			{Kind: "raw-operation", File: "internal/apiv1/sqlite_observability_supported_surface_test.go", Declaration: "newPostgresObservabilitySurfaceFixture", Operation: "call:github.com/division-sh/swarm/internal/store/storetest.NewPostgresStoreForTest", Resolved: "func=func(db *database/sql.DB) *github.com/division-sh/swarm/internal/store.PostgresStore|recv=invalid type|arg=*database/sql.DB|results=(*github.com/division-sh/swarm/internal/store.PostgresStore)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/sqlite_observability_supported_surface_test.go", Declaration: "newPostgresObservabilitySurfaceFixture", Operation: "call:github.com/division-sh/swarm/internal/store/storetest.NewPostgresStoreForTest", Resolved: "func=func(db *database/sql.DB, facts github.com/division-sh/swarm/internal/channelprovider.FactsLookup) *github.com/division-sh/swarm/internal/store.PostgresStore|recv=invalid type|arg=*database/sql.DB|arg=func(provider string) (github.com/division-sh/swarm/internal/channelprovider.Binding, bool)|results=(*github.com/division-sh/swarm/internal/store.PostgresStore)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/cliapp/verify_composition_test.go", Declaration: "newVerifyCompositionBootStore", Operation: "call:github.com/division-sh/swarm/internal/store/construction.OpenPostgres", Resolved: "func=func(dsn string) (*github.com/division-sh/swarm/internal/store/internal/runtimepersistence.PostgresStore, *database/sql.DB, error)|recv=invalid type|arg=string|results=(*github.com/division-sh/swarm/internal/store/internal/runtimepersistence.PostgresStore,*database/sql.DB,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/cliapp/verify_composition_test.go", Declaration: "newVerifyCompositionBootStore", Operation: "call:github.com/division-sh/swarm/internal/store/construction.OpenPostgres", Resolved: "func=func(dsn string, providerFacts github.com/division-sh/swarm/internal/channelprovider.FactsLookup) (*github.com/division-sh/swarm/internal/store/internal/runtimepersistence.PostgresStore, *database/sql.DB, error)|recv=invalid type|arg=string|arg=func(provider string) (github.com/division-sh/swarm/internal/channelprovider.Binding, bool)|results=(*github.com/division-sh/swarm/internal/store/internal/runtimepersistence.PostgresStore,*database/sql.DB,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeUnavailableBundleStartupRecoveryFailsPersistedMissingBeforeCleanup", Operation: "call:github.com/division-sh/swarm/internal/store/storetest.NewPostgresStoreForTest", Resolved: "func=func(db *database/sql.DB) *github.com/division-sh/swarm/internal/store.PostgresStore|recv=invalid type|arg=*database/sql.DB|results=(*github.com/division-sh/swarm/internal/store.PostgresStore)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestRunServeRuntimeUnavailableBundleStartupRecoveryFailsPersistedMissingBeforeCleanup", Operation: "call:github.com/division-sh/swarm/internal/store/storetest.NewPostgresStoreForTest", Resolved: "func=func(db *database/sql.DB, facts github.com/division-sh/swarm/internal/channelprovider.FactsLookup) *github.com/division-sh/swarm/internal/store.PostgresStore|recv=invalid type|arg=*database/sql.DB|arg=func(provider string) (github.com/division-sh/swarm/internal/channelprovider.Binding, bool)|results=(*github.com/division-sh/swarm/internal/store.PostgresStore)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/schema_boot_teaching_error_test.go", Declaration: "TestServeBootLegacySchemaRendersTeachingError", Operation: "call:github.com/division-sh/swarm/internal/store/construction.OpenSQLiteRuntime", Resolved: "func=func(path string) (*github.com/division-sh/swarm/internal/store/internal/runtimepersistence.SQLiteRuntimeStore, *database/sql.DB, error)|recv=invalid type|arg=string|results=(*github.com/division-sh/swarm/internal/store/internal/runtimepersistence.SQLiteRuntimeStore,*database/sql.DB,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/schema_boot_teaching_error_test.go", Declaration: "TestServeBootLegacySchemaRendersTeachingError", Operation: "call:github.com/division-sh/swarm/internal/store/construction.OpenSQLiteRuntime", Resolved: "func=func(path string, providerFacts github.com/division-sh/swarm/internal/channelprovider.FactsLookup) (*github.com/division-sh/swarm/internal/store/internal/runtimepersistence.SQLiteRuntimeStore, *database/sql.DB, error)|recv=invalid type|arg=string|arg=func(provider string) (github.com/division-sh/swarm/internal/channelprovider.Binding, bool)|results=(*github.com/division-sh/swarm/internal/store/internal/runtimepersistence.SQLiteRuntimeStore,*database/sql.DB,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/schema_boot_teaching_error_test.go", Declaration: "TestServeBootLegacySchemaRendersTeachingError", Operation: "call:github.com/division-sh/swarm/internal/store/storetest.NewPostgresStoreForTest", Resolved: "func=func(db *database/sql.DB) *github.com/division-sh/swarm/internal/store.PostgresStore|recv=invalid type|arg=*database/sql.DB|results=(*github.com/division-sh/swarm/internal/store.PostgresStore)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/schema_boot_teaching_error_test.go", Declaration: "TestServeBootLegacySchemaRendersTeachingError", Operation: "call:github.com/division-sh/swarm/internal/store/storetest.NewPostgresStoreForTest", Resolved: "func=func(db *database/sql.DB, facts github.com/division-sh/swarm/internal/channelprovider.FactsLookup) *github.com/division-sh/swarm/internal/store.PostgresStore|recv=invalid type|arg=*database/sql.DB|arg=func(provider string) (github.com/division-sh/swarm/internal/channelprovider.Binding, bool)|results=(*github.com/division-sh/swarm/internal/store.PostgresStore)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/startup_integrity_test.go", Declaration: "TestRunServeSourceArtifactIntegrityRejectsBeforeReadinessBothStores", Operation: "call:github.com/division-sh/swarm/internal/store/storetest.NewPostgresStoreForTest", Resolved: "func=func(db *database/sql.DB) *github.com/division-sh/swarm/internal/store.PostgresStore|recv=invalid type|arg=*database/sql.DB|results=(*github.com/division-sh/swarm/internal/store.PostgresStore)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/startup_integrity_test.go", Declaration: "TestRunServeSourceArtifactIntegrityRejectsBeforeReadinessBothStores", Operation: "call:github.com/division-sh/swarm/internal/store/storetest.NewPostgresStoreForTest", Resolved: "func=func(db *database/sql.DB, facts github.com/division-sh/swarm/internal/channelprovider.FactsLookup) *github.com/division-sh/swarm/internal/store.PostgresStore|recv=invalid type|arg=*database/sql.DB|arg=func(provider string) (github.com/division-sh/swarm/internal/channelprovider.Binding, bool)|results=(*github.com/division-sh/swarm/internal/store.PostgresStore)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/store/storetest/sqlite.go", Declaration: "AdmitPostgresRuntimeStore", Operation: "call:github.com/division-sh/swarm/internal/store/storetest.NewPostgresStoreForTest", Resolved: "func=func(db *database/sql.DB) *github.com/division-sh/swarm/internal/store.PostgresStore|recv=<nil>|arg=*database/sql.DB|results=(*github.com/division-sh/swarm/internal/store.PostgresStore)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/store/storetest/sqlite.go", Declaration: "AdmitPostgresRuntimeStore", Operation: "call:github.com/division-sh/swarm/internal/store/storetest.NewPostgresStoreForTest", Resolved: "func=func(db *database/sql.DB, facts github.com/division-sh/swarm/internal/channelprovider.FactsLookup) *github.com/division-sh/swarm/internal/store.PostgresStore|recv=<nil>|arg=*database/sql.DB|arg=func(provider string) (github.com/division-sh/swarm/internal/channelprovider.Binding, bool)|results=(*github.com/division-sh/swarm/internal/store.PostgresStore)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/store/storetest/sqlite.go", Declaration: "AdmitSQLiteRuntimeStore", Operation: "call:github.com/division-sh/swarm/internal/store/storetest.NewSQLiteRuntimeStoreForTest", Resolved: "func=func(db *database/sql.DB) *github.com/division-sh/swarm/internal/store.SQLiteRuntimeStore|recv=<nil>|arg=*database/sql.DB|results=(*github.com/division-sh/swarm/internal/store.SQLiteRuntimeStore)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/store/storetest/sqlite.go", Declaration: "AdmitSQLiteRuntimeStore", Operation: "call:github.com/division-sh/swarm/internal/store/storetest.NewSQLiteRuntimeStoreForTest", Resolved: "func=func(db *database/sql.DB, facts github.com/division-sh/swarm/internal/channelprovider.FactsLookup) *github.com/division-sh/swarm/internal/store.SQLiteRuntimeStore|recv=<nil>|arg=*database/sql.DB|arg=func(provider string) (github.com/division-sh/swarm/internal/channelprovider.Binding, bool)|results=(*github.com/division-sh/swarm/internal/store.SQLiteRuntimeStore)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/store/storetest/sqlite.go", Declaration: "NewPostgresStoreForTest", Operation: "call:github.com/division-sh/swarm/internal/store/internal/runtimepersistence.NewPostgresStoreForTest", Resolved: "func=func(db *database/sql.DB) *github.com/division-sh/swarm/internal/store/internal/runtimepersistence.PostgresStore|recv=invalid type|arg=*database/sql.DB|results=(*github.com/division-sh/swarm/internal/store/internal/runtimepersistence.PostgresStore)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/store/storetest/sqlite.go", Declaration: "NewPostgresStoreForTest", Operation: "call:github.com/division-sh/swarm/internal/store/internal/runtimepersistence.NewPostgresStoreForTest", Resolved: "func=func(db *database/sql.DB, providerFacts github.com/division-sh/swarm/internal/channelprovider.FactsLookup) *github.com/division-sh/swarm/internal/store/internal/runtimepersistence.PostgresStore|recv=invalid type|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/channelprovider.FactsLookup|results=(*github.com/division-sh/swarm/internal/store/internal/runtimepersistence.PostgresStore)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/store/storetest/sqlite.go", Declaration: "NewSQLiteRuntimeStoreForTest", Operation: "call:github.com/division-sh/swarm/internal/store/internal/runtimepersistence.NewSQLiteRuntimeStoreForTest", Resolved: "func=func(db *database/sql.DB) *github.com/division-sh/swarm/internal/store/internal/runtimepersistence.SQLiteRuntimeStore|recv=invalid type|arg=*database/sql.DB|results=(*github.com/division-sh/swarm/internal/store/internal/runtimepersistence.SQLiteRuntimeStore)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/store/storetest/sqlite.go", Declaration: "NewSQLiteRuntimeStoreForTest", Operation: "call:github.com/division-sh/swarm/internal/store/internal/runtimepersistence.NewSQLiteRuntimeStoreForTest", Resolved: "func=func(db *database/sql.DB, providerFacts github.com/division-sh/swarm/internal/channelprovider.FactsLookup) *github.com/division-sh/swarm/internal/store/internal/runtimepersistence.SQLiteRuntimeStore|recv=invalid type|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/channelprovider.FactsLookup|results=(*github.com/division-sh/swarm/internal/store/internal/runtimepersistence.SQLiteRuntimeStore)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
	}
}

func debtRunFixtureSignatureComparisons(actual, head, base, source map[string]authorityDebtSite) (map[string]authorityDebtSite, map[string]authorityDebtSite, map[string]authorityDebtSite, []string) {
	runFixtures, selectedFixtures := debtRunFixtureSignatureTransitions(), debtSelectedFixtureSignatureTransitions()
	w1Fixtures := debtW1RunObservationSignatureTransitions()
	rollbackFixtures := debtLiveRollbackSignatureTransitions()
	pairs := runFixtures[:]
	forwardOnly := len(pairs)
	pairs = append(pairs, debtDeploymentHandlerSignatureTransition(), debtSelectedBindingRootArgumentTransition())
	pairs = append(append(pairs, selectedFixtures[:]...), w1Fixtures[:]...)
	pairs = append(pairs, rollbackFixtures[:]...)
	portableFixtures := debtPortableRuntimeSignatureTransitions()
	pairs = append(pairs, portableFixtures[:]...)
	globalFixtures := debtPortableGlobalSignatureTransitions()
	pairs = append(pairs, globalFixtures[:]...)
	w2Evidence := debtW2DeliveryEvidenceSignatureTransitions()
	pairs = append(pairs, w2Evidence[:]...)
	channelPreparation := debtChannelCrashPreparationSignatureTransitions()
	pairs = append(pairs, channelPreparation[:]...)
	r130Constructors := debtR130ConstructorSignatureTransitions()
	pairs = append(pairs, r130Constructors[:]...)
	for index, pair := range pairs {
		old, current := pair[0], pair[1]
		for _, state := range []map[string]authorityDebtSite{actual, head, base, source} {
			if state[old.identity()].Multiplicity > 0 && state[current.identity()].Multiplicity > 0 {
				return head, base, source, []string{"both reviewed run-fixture signature identities survive: " + old.Declaration}
			}
		}
		sourcePredecessor := source[old.identity()] == old && source[current.identity()].Multiplicity == 0
		sourceLanded := index >= forwardOnly && source[current.identity()] == current && source[old.identity()].Multiplicity == 0
		if actual[current.identity()] == current && actual[old.identity()].Multiplicity == 0 &&
			base[old.identity()] == old && base[current.identity()].Multiplicity == 0 &&
			(sourcePredecessor || sourceLanded) {
			head = debtForwardQuiescenceUncertainty(head, old, current)
			base = debtForwardQuiescenceUncertainty(base, old, current)
			source = debtForwardQuiescenceUncertainty(source, old, current)
		}
	}
	// The selected-owner calls may already be landed while their allowance
	// retains the old signature. Only the exact reviewed source proves this arm.
	for _, pair := range selectedFixtures {
		old, current := pair[0], pair[1]
		if actual[current.identity()] == current && actual[old.identity()].Multiplicity == 0 &&
			source[current.identity()] == current && source[old.identity()].Multiplicity == 0 &&
			base[old.identity()] == old && base[current.identity()].Multiplicity == 0 {
			head = debtForwardQuiescenceUncertainty(head, old, current)
			base = debtForwardQuiescenceUncertainty(base, old, current)
		}
	}
	// #2413/6565922169: this exact counted pair also admits its landed source.
	old, current := debtCatalogRuntimeRowsSignatureTransition()
	for _, state := range []map[string]authorityDebtSite{actual, head, base, source} {
		if state[old.identity()].Multiplicity > 0 && state[current.identity()].Multiplicity > 0 {
			return head, base, source, []string{"both reviewed catalog runtime-row signatures survive: " + old.Declaration}
		}
	}
	sourcePredecessor := source[old.identity()] == old && source[current.identity()].Multiplicity == 0
	sourceLanded := source[current.identity()] == current && source[old.identity()].Multiplicity == 0
	if actual[current.identity()] == current && actual[old.identity()].Multiplicity == 0 &&
		base[old.identity()] == old && base[current.identity()].Multiplicity == 0 && (sourcePredecessor || sourceLanded) {
		head = debtForwardQuiescenceUncertainty(head, old, current)
		base = debtForwardQuiescenceUncertainty(base, old, current)
		source = debtForwardQuiescenceUncertainty(source, old, current)
	}
	return head, base, source, nil
}

// #2409/6671745779: enumerated counted tuples, never prefix normalization.
func debtPortableGlobalSignatureTransitions() [308][2]authorityDebtSite {
	return [308][2]authorityDebtSite{
		{
			{Kind: "raw-operation", File: "cmd/swarm-test/completion_preflight.go", Declaration: "completionPlanPreflight", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(github.com/division-sh/swarm/internal/testpostgres.Connection).Open", Resolved: "func=func() (*database/sql.DB, error)|recv=github.com/division-sh/swarm/internal/testpostgres.Connection|results=(*database/sql.DB,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "cmd/swarm-test/completion_preflight.go", Declaration: "completionPlanPreflight", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(github.com/division-sh/swarm/tests/support/testpostgres.Connection).Open", Resolved: "func=func() (*database/sql.DB, error)|recv=github.com/division-sh/swarm/tests/support/testpostgres.Connection|results=(*database/sql.DB,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "cmd/swarm-test/completion_preflight.go", Declaration: "completionPlanPreflight", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.ConnectionFromEnvironmentIfSet", Resolved: "func=func() (github.com/division-sh/swarm/internal/testpostgres.Connection, bool, error)|recv=invalid type|results=(github.com/division-sh/swarm/internal/testpostgres.Connection,bool,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "cmd/swarm-test/completion_preflight.go", Declaration: "completionPlanPreflight", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.ConnectionFromEnvironmentIfSet", Resolved: "func=func() (github.com/division-sh/swarm/tests/support/testpostgres.Connection, bool, error)|recv=invalid type|results=(github.com/division-sh/swarm/tests/support/testpostgres.Connection,bool,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "cmd/swarm-test/completion_preflight.go", Declaration: "completionPlanPreflight", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.ValidateServerCapacity", Resolved: "func=func(ctx context.Context, db interface{QueryRowContext(context.Context, string, ...any) *database/sql.Row}) error|recv=invalid type|arg=context.Context|arg=*database/sql.DB|results=(error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "cmd/swarm-test/completion_preflight.go", Declaration: "completionPlanPreflight", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.ValidateServerCapacity", Resolved: "func=func(ctx context.Context, db interface{QueryRowContext(context.Context, string, ...any) *database/sql.Row}) error|recv=invalid type|arg=context.Context|arg=*database/sql.DB|results=(error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "cmd/swarm-test/completion_preflight.go", Declaration: "completionPlanPreflight", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/internal/testpostgres.Connection", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "local-raw-type", File: "cmd/swarm-test/completion_preflight.go", Declaration: "completionPlanPreflight", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/tests/support/testpostgres.Connection", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runCreator", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(*github.com/division-sh/swarm/internal/testpostgres.ServiceRegistry).RunCreator", Resolved: "func=func(ctx context.Context, leaseID string, creatorFD uintptr) error|recv=*github.com/division-sh/swarm/internal/testpostgres.ServiceRegistry|arg=context.Context|arg=string|arg=uintptr|results=(error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runCreator", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(*github.com/division-sh/swarm/tests/support/testpostgres.ServiceRegistry).RunCreator", Resolved: "func=func(ctx context.Context, leaseID string, creatorFD uintptr) error|recv=*github.com/division-sh/swarm/tests/support/testpostgres.ServiceRegistry|arg=context.Context|arg=string|arg=uintptr|results=(error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runCreator", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.NewServiceRegistry", Resolved: "func=func(stateRoot string, dockerBin string) *github.com/division-sh/swarm/internal/testpostgres.ServiceRegistry|recv=invalid type|arg=string|arg=string|results=(*github.com/division-sh/swarm/internal/testpostgres.ServiceRegistry)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runCreator", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.NewServiceRegistry", Resolved: "func=func(stateRoot string, dockerBin string) *github.com/division-sh/swarm/tests/support/testpostgres.ServiceRegistry|recv=invalid type|arg=string|arg=string|results=(*github.com/division-sh/swarm/tests/support/testpostgres.ServiceRegistry)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "cmd/swarm-test/main.go", Declaration: "runCreator", Operation: "local:registry", Resolved: "*github.com/division-sh/swarm/internal/testpostgres.ServiceRegistry", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "local-raw-type", File: "cmd/swarm-test/main.go", Declaration: "runCreator", Operation: "local:registry", Resolved: "*github.com/division-sh/swarm/tests/support/testpostgres.ServiceRegistry", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(*github.com/division-sh/swarm/internal/testpostgres.Service).Close", Resolved: "func=func(ctx context.Context) error|recv=*github.com/division-sh/swarm/internal/testpostgres.Service|arg=context.Context|results=(error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(*github.com/division-sh/swarm/tests/support/testpostgres.Service).Close", Resolved: "func=func(ctx context.Context) error|recv=*github.com/division-sh/swarm/tests/support/testpostgres.Service|arg=context.Context|results=(error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(*github.com/division-sh/swarm/internal/testpostgres.Service).InheritLeaseTo", Resolved: "func=func(cmd *os/exec.Cmd) error|recv=*github.com/division-sh/swarm/internal/testpostgres.Service|arg=*os/exec.Cmd|results=(error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(*github.com/division-sh/swarm/tests/support/testpostgres.Service).InheritLeaseTo", Resolved: "func=func(cmd *os/exec.Cmd) error|recv=*github.com/division-sh/swarm/tests/support/testpostgres.Service|arg=*os/exec.Cmd|results=(error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(*github.com/division-sh/swarm/internal/testpostgres.Service).MarkChildRunning", Resolved: "func=func() error|recv=*github.com/division-sh/swarm/internal/testpostgres.Service|results=(error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(*github.com/division-sh/swarm/tests/support/testpostgres.Service).MarkChildRunning", Resolved: "func=func() error|recv=*github.com/division-sh/swarm/tests/support/testpostgres.Service|results=(error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(*github.com/division-sh/swarm/internal/testpostgres.ServiceRegistry).Provision", Resolved: "func=func(ctx context.Context, executable string) (_ *github.com/division-sh/swarm/internal/testpostgres.Service, resultErr error)|recv=*github.com/division-sh/swarm/internal/testpostgres.ServiceRegistry|arg=context.Context|arg=string|results=(*github.com/division-sh/swarm/internal/testpostgres.Service,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(*github.com/division-sh/swarm/tests/support/testpostgres.ServiceRegistry).Provision", Resolved: "func=func(ctx context.Context, executable string) (_ *github.com/division-sh/swarm/tests/support/testpostgres.Service, resultErr error)|recv=*github.com/division-sh/swarm/tests/support/testpostgres.ServiceRegistry|arg=context.Context|arg=string|results=(*github.com/division-sh/swarm/tests/support/testpostgres.Service,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.ChildEnvironment", Resolved: "func=func(env []string, connection github.com/division-sh/swarm/internal/testpostgres.Connection) ([]string, error)|recv=invalid type|arg=[]string|arg=github.com/division-sh/swarm/internal/testpostgres.Connection|results=([]string,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.ChildEnvironment", Resolved: "func=func(env []string, connection github.com/division-sh/swarm/tests/support/testpostgres.Connection) ([]string, error)|recv=invalid type|arg=[]string|arg=github.com/division-sh/swarm/tests/support/testpostgres.Connection|results=([]string,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.ConnectionFromEnvironmentIfSet", Resolved: "func=func() (github.com/division-sh/swarm/internal/testpostgres.Connection, bool, error)|recv=invalid type|results=(github.com/division-sh/swarm/internal/testpostgres.Connection,bool,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.ConnectionFromEnvironmentIfSet", Resolved: "func=func() (github.com/division-sh/swarm/tests/support/testpostgres.Connection, bool, error)|recv=invalid type|results=(github.com/division-sh/swarm/tests/support/testpostgres.Connection,bool,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.DefaultServiceRegistry", Resolved: "func=func() (*github.com/division-sh/swarm/internal/testpostgres.ServiceRegistry, error)|recv=invalid type|results=(*github.com/division-sh/swarm/internal/testpostgres.ServiceRegistry,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.DefaultServiceRegistry", Resolved: "func=func() (*github.com/division-sh/swarm/tests/support/testpostgres.ServiceRegistry, error)|recv=invalid type|results=(*github.com/division-sh/swarm/tests/support/testpostgres.ServiceRegistry,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/internal/testpostgres.Connection", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 2},
			{Kind: "local-raw-type", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/tests/support/testpostgres.Connection", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 2},
		},
		{
			{Kind: "local-raw-type", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "local:registry", Resolved: "*github.com/division-sh/swarm/internal/testpostgres.ServiceRegistry", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "local-raw-type", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "local:registry", Resolved: "*github.com/division-sh/swarm/tests/support/testpostgres.ServiceRegistry", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "local:service", Resolved: "*github.com/division-sh/swarm/internal/testpostgres.Service", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 2},
			{Kind: "local-raw-type", File: "cmd/swarm-test/main.go", Declaration: "runTestArgs", Operation: "local:service", Resolved: "*github.com/division-sh/swarm/tests/support/testpostgres.Service", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/agent_snapshot_atomicity_test.go", Declaration: "agentSnapshotBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/agent_snapshot_atomicity_test.go", Declaration: "agentSnapshotBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/conversation_fork_commit_omission_probe_test.go", Declaration: "newConversationForkCommitFixture", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/conversation_fork_commit_omission_probe_test.go", Declaration: "newConversationForkCommitFixture", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_administrative_layering_test.go", Declaration: "TestAdministrativeLeaseDoesNotBorrowSurroundingTransaction", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_administrative_layering_test.go", Declaration: "TestAdministrativeLeaseDoesNotBorrowSurroundingTransaction", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_administrative_layering_test.go", Declaration: "TestAdministrativeOperationCancellationReleasesLeaseAndCapacity", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_administrative_layering_test.go", Declaration: "TestAdministrativeOperationCancellationReleasesLeaseAndCapacity", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_administrative_layering_test.go", Declaration: "testRuntimeNukeLayeredPostgresCapacity", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_administrative_layering_test.go", Declaration: "testRuntimeNukeLayeredPostgresCapacity", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_agent_control_test.go", Declaration: "TestOperatorAgentSendDirectivePersistsDirectiveEventOnceOnReplay", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_agent_control_test.go", Declaration: "TestOperatorAgentSendDirectivePersistsDirectiveEventOnceOnReplay", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_agent_control_test.go", Declaration: "TestOperatorAgentSendDirectiveUsesCanonicalRuntimeSourceArtifact", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_agent_control_test.go", Declaration: "TestOperatorAgentSendDirectiveUsesCanonicalRuntimeSourceArtifact", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_data_run_lifecycle_test.go", Declaration: "forEachDataRunLifecycleStore", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_data_run_lifecycle_test.go", Declaration: "forEachDataRunLifecycleStore", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_data_test.go", Declaration: "TestDurableDataHTTPPublicSurfaceAcrossSelectedStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_data_test.go", Declaration: "TestDurableDataHTTPPublicSurfaceAcrossSelectedStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestEventPublishCanonicalClassAndProvenanceReadbackParity", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestEventPublishCanonicalClassAndProvenanceReadbackParity", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishExistingRunTargetRouteRejectsInvalidTargetBeforePersistence", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishExistingRunTargetRouteRejectsInvalidTargetBeforePersistence", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishExplicitRunFollowUpRequiresRecipientBeforePersistence", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishExplicitRunFollowUpRequiresRecipientBeforePersistence", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishHandlersPersistEventReportDeliveriesAndReplayIdempotency", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishHandlersPersistEventReportDeliveriesAndReplayIdempotency", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishOperatorReferenceValidatesSameRunProvenance", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishOperatorReferenceValidatesSameRunProvenance", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishPostgresUsesPublisherScopeWithPlainRequestContext", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishPostgresUsesPublisherScopeWithPlainRequestContext", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishPreCommitFailureFailsClosedWithDeclaredError", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishPreCommitFailureFailsClosedWithDeclaredError", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishPrivateTargetCannotAuthorizePublication", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishPrivateTargetCannotAuthorizePublication", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishRenamedConnectedCreateEntityRejectsCallerIdentityBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishRenamedConnectedCreateEntityRejectsCallerIdentityBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishReturnsDurableAckBeforePostCommitDispatchCompletes", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishReturnsDurableAckBeforePostCommitDispatchCompletes", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishReturnsStoredCompletionWithoutPostCommitReadback", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishReturnsStoredCompletionWithoutPostCommitReadback", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishRootEventNameWinsOverFlowLeafAliases", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishRootEventNameWinsOverFlowLeafAliases", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishRootEventTemplateInputNameCollisionPayloadEntityIDDoesNotSelectTarget", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_publish_test.go", Declaration: "TestOperatorEventPublishRootEventTemplateInputNameCollisionPayloadEntityIDDoesNotSelectTarget", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_replay_test.go", Declaration: "TestOpaqueMissingEventIDReturnsNotFoundAcrossOperatorConsumersParity", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_replay_test.go", Declaration: "TestOpaqueMissingEventIDReturnsNotFoundAcrossOperatorConsumersParity", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_replay_test.go", Declaration: "TestOperatorEventReplayDispatchesCompleteCanonicalSnapshotParity", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_replay_test.go", Declaration: "TestOperatorEventReplayDispatchesCompleteCanonicalSnapshotParity", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_replay_test.go", Declaration: "TestOperatorReplayPreservesFailedEligibilityAndEveryExactRouteSiblingParity", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_event_replay_test.go", Declaration: "TestOperatorReplayPreservesFailedEligibilityAndEveryExactRouteSiblingParity", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_run_completion_system_node_test.go", Declaration: "TestOperatorRunCompletionSystemNodeFlowConvergesSupportedSurfaces", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_run_completion_system_node_test.go", Declaration: "TestOperatorRunCompletionSystemNodeFlowConvergesSupportedSurfaces", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_run_start_test.go", Declaration: "TestOperatorRunStartHandlersAcceptCanonicalBundleHashForCreateNewWork", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_run_start_test.go", Declaration: "TestOperatorRunStartHandlersAcceptCanonicalBundleHashForCreateNewWork", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_run_start_test.go", Declaration: "TestOperatorRunStartHandlersPersistRootEventAndReplayIdempotency", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_run_start_test.go", Declaration: "TestOperatorRunStartHandlersPersistRootEventAndReplayIdempotency", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_run_start_test.go", Declaration: "TestOperatorRunStartHandlersUseActiveEphemeralBundleScopeForCreateNewWork", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_run_start_test.go", Declaration: "TestOperatorRunStartHandlersUseActiveEphemeralBundleScopeForCreateNewWork", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_runtime_context_test.go", Declaration: "newOperatorRuntimeContextFixture", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_runtime_context_test.go", Declaration: "newOperatorRuntimeContextFixture", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_stage_completion_journey_test.go", Declaration: "TestExactStageCompletionRealWriterAndRehydratedSourceBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_stage_completion_journey_test.go", Declaration: "TestExactStageCompletionRealWriterAndRehydratedSourceBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_stage_completion_read_test.go", Declaration: "TestExactStageCompletionPublicReadbackBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_stage_completion_read_test.go", Declaration: "TestExactStageCompletionPublicReadbackBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_test_setup_test.go", Declaration: "TestOperatorTestSetupHandlersPersistEntitiesAndReplayIdempotency", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_test_setup_test.go", Declaration: "TestOperatorTestSetupHandlersPersistEntitiesAndReplayIdempotency", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/operator_test_setup_test.go", Declaration: "TestOperatorTestSetupRejectsContractInvalidEntities", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/operator_test_setup_test.go", Declaration: "TestOperatorTestSetupRejectsContractInvalidEntities", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/runtime_nuke_durable_replay_test.go", Declaration: "TestRuntimeNukeDurableReplaySurvivesAPICompletionLossAndExpiration", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/runtime_nuke_durable_replay_test.go", Declaration: "TestRuntimeNukeDurableReplaySurvivesAPICompletionLossAndExpiration", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/selected_store_read_supported_surface_test.go", Declaration: "TestPostgresAgentConversationOwnerBacksSupportedAPISurface", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/selected_store_read_supported_surface_test.go", Declaration: "TestPostgresAgentConversationOwnerBacksSupportedAPISurface", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/apiv1/sqlite_observability_supported_surface_test.go", Declaration: "newPostgresObservabilitySurfaceFixture", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/apiv1/sqlite_observability_supported_surface_test.go", Declaration: "newPostgresObservabilitySurfaceFixture", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/cliapp/data_show_selected_test.go", Declaration: "TestDataShowCLIReadFamilyAcrossSelectedStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/cliapp/data_show_selected_test.go", Declaration: "TestDataShowCLIReadFamilyAcrossSelectedStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/cliapp/main_test.go", Declaration: "setPostgresEnvFromDSN", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(github.com/division-sh/swarm/internal/testpostgres.Connection).Parameters", Resolved: "func=func() github.com/division-sh/swarm/internal/testpostgres.Parameters|recv=github.com/division-sh/swarm/internal/testpostgres.Connection|results=(github.com/division-sh/swarm/internal/testpostgres.Parameters)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/cliapp/main_test.go", Declaration: "setPostgresEnvFromDSN", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(github.com/division-sh/swarm/tests/support/testpostgres.Connection).Parameters", Resolved: "func=func() github.com/division-sh/swarm/tests/support/testpostgres.Parameters|recv=github.com/division-sh/swarm/tests/support/testpostgres.Connection|results=(github.com/division-sh/swarm/tests/support/testpostgres.Parameters)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/cliapp/main_test.go", Declaration: "setPostgresEnvFromDSN", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.ParseConnection", Resolved: "func=func(raw string) (github.com/division-sh/swarm/internal/testpostgres.Connection, error)|recv=invalid type|arg=string|results=(github.com/division-sh/swarm/internal/testpostgres.Connection,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/cliapp/main_test.go", Declaration: "setPostgresEnvFromDSN", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.ParseConnection", Resolved: "func=func(raw string) (github.com/division-sh/swarm/tests/support/testpostgres.Connection, error)|recv=invalid type|arg=string|results=(github.com/division-sh/swarm/tests/support/testpostgres.Connection,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "internal/cliapp/main_test.go", Declaration: "setPostgresEnvFromDSN", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/internal/testpostgres.Connection", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "local-raw-type", File: "internal/cliapp/main_test.go", Declaration: "setPostgresEnvFromDSN", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/tests/support/testpostgres.Connection", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/cliapp/runtime_log_filter_supported_test.go", Declaration: "TestRuntimeLogFilterSupportedConsumersBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/cliapp/runtime_log_filter_supported_test.go", Declaration: "TestRuntimeLogFilterSupportedConsumersBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/cliapp/store_authority_test.go", Declaration: "TestStoreStatusDoesNotBootstrapEmptyPostgres", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(*github.com/division-sh/swarm/internal/testpostgres.Manager).Acquire", Resolved: "func=func(ctx context.Context, withTemplate bool) (*github.com/division-sh/swarm/internal/testpostgres.Sandbox, error)|recv=*github.com/division-sh/swarm/internal/testpostgres.Manager|arg=context.Context|arg=bool|results=(*github.com/division-sh/swarm/internal/testpostgres.Sandbox,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/cliapp/store_authority_test.go", Declaration: "TestStoreStatusDoesNotBootstrapEmptyPostgres", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(*github.com/division-sh/swarm/tests/support/testpostgres.Manager).Acquire", Resolved: "func=func(ctx context.Context, withTemplate bool) (*github.com/division-sh/swarm/tests/support/testpostgres.Sandbox, error)|recv=*github.com/division-sh/swarm/tests/support/testpostgres.Manager|arg=context.Context|arg=bool|results=(*github.com/division-sh/swarm/tests/support/testpostgres.Sandbox,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/cliapp/store_authority_test.go", Declaration: "TestStoreStatusDoesNotBootstrapEmptyPostgres", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(*github.com/division-sh/swarm/internal/testpostgres.Sandbox).Release", Resolved: "func=func(ctx context.Context) error|recv=*github.com/division-sh/swarm/internal/testpostgres.Sandbox|arg=context.Context|results=(error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/cliapp/store_authority_test.go", Declaration: "TestStoreStatusDoesNotBootstrapEmptyPostgres", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(*github.com/division-sh/swarm/tests/support/testpostgres.Sandbox).Release", Resolved: "func=func(ctx context.Context) error|recv=*github.com/division-sh/swarm/tests/support/testpostgres.Sandbox|arg=context.Context|results=(error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/cliapp/store_authority_test.go", Declaration: "TestStoreStatusDoesNotBootstrapEmptyPostgres", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(github.com/division-sh/swarm/internal/testpostgres.Connection).String", Resolved: "func=func() (string, error)|recv=github.com/division-sh/swarm/internal/testpostgres.Connection|results=(string,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/cliapp/store_authority_test.go", Declaration: "TestStoreStatusDoesNotBootstrapEmptyPostgres", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(github.com/division-sh/swarm/tests/support/testpostgres.Connection).String", Resolved: "func=func() (string, error)|recv=github.com/division-sh/swarm/tests/support/testpostgres.Connection|results=(string,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/cliapp/store_authority_test.go", Declaration: "TestStoreStatusDoesNotBootstrapEmptyPostgres", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.ManagerFromEnvironment", Resolved: "func=func(ctx context.Context) (*github.com/division-sh/swarm/internal/testpostgres.Manager, error)|recv=invalid type|arg=context.Context|results=(*github.com/division-sh/swarm/internal/testpostgres.Manager,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/cliapp/store_authority_test.go", Declaration: "TestStoreStatusDoesNotBootstrapEmptyPostgres", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.ManagerFromEnvironment", Resolved: "func=func(ctx context.Context) (*github.com/division-sh/swarm/tests/support/testpostgres.Manager, error)|recv=invalid type|arg=context.Context|results=(*github.com/division-sh/swarm/tests/support/testpostgres.Manager,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "internal/cliapp/store_authority_test.go", Declaration: "TestStoreStatusDoesNotBootstrapEmptyPostgres", Operation: "local:manager", Resolved: "*github.com/division-sh/swarm/internal/testpostgres.Manager", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "local-raw-type", File: "internal/cliapp/store_authority_test.go", Declaration: "TestStoreStatusDoesNotBootstrapEmptyPostgres", Operation: "local:manager", Resolved: "*github.com/division-sh/swarm/tests/support/testpostgres.Manager", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "internal/cliapp/store_authority_test.go", Declaration: "TestStoreStatusDoesNotBootstrapEmptyPostgres", Operation: "local:sandbox", Resolved: "*github.com/division-sh/swarm/internal/testpostgres.Sandbox", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "local-raw-type", File: "internal/cliapp/store_authority_test.go", Declaration: "TestStoreStatusDoesNotBootstrapEmptyPostgres", Operation: "local:sandbox", Resolved: "*github.com/division-sh/swarm/tests/support/testpostgres.Sandbox", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/cliapp/verify_composition_test.go", Declaration: "newVerifyCompositionBootStore", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(github.com/division-sh/swarm/internal/testpostgres.Connection).Parameters", Resolved: "func=func() github.com/division-sh/swarm/internal/testpostgres.Parameters|recv=github.com/division-sh/swarm/internal/testpostgres.Connection|results=(github.com/division-sh/swarm/internal/testpostgres.Parameters)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/cliapp/verify_composition_test.go", Declaration: "newVerifyCompositionBootStore", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(github.com/division-sh/swarm/tests/support/testpostgres.Connection).Parameters", Resolved: "func=func() github.com/division-sh/swarm/tests/support/testpostgres.Parameters|recv=github.com/division-sh/swarm/tests/support/testpostgres.Connection|results=(github.com/division-sh/swarm/tests/support/testpostgres.Parameters)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/cliapp/verify_composition_test.go", Declaration: "newVerifyCompositionBootStore", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.ParseConnection", Resolved: "func=func(raw string) (github.com/division-sh/swarm/internal/testpostgres.Connection, error)|recv=invalid type|arg=string|results=(github.com/division-sh/swarm/internal/testpostgres.Connection,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/cliapp/verify_composition_test.go", Declaration: "newVerifyCompositionBootStore", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.ParseConnection", Resolved: "func=func(raw string) (github.com/division-sh/swarm/tests/support/testpostgres.Connection, error)|recv=invalid type|arg=string|results=(github.com/division-sh/swarm/tests/support/testpostgres.Connection,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/cliapp/verify_composition_test.go", Declaration: "newVerifyCompositionBootStore", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/cliapp/verify_composition_test.go", Declaration: "newVerifyCompositionBootStore", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "internal/cliapp/verify_composition_test.go", Declaration: "newVerifyCompositionBootStore", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/internal/testpostgres.Connection", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "local-raw-type", File: "internal/cliapp/verify_composition_test.go", Declaration: "newVerifyCompositionBootStore", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/tests/support/testpostgres.Connection", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/cliapp/verify_deployment_test.go", Declaration: "TestVerifyFreshPostgresSchemaAccountsAbsenceWithoutClaimingPossession", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(github.com/division-sh/swarm/internal/testpostgres.Connection).Parameters", Resolved: "func=func() github.com/division-sh/swarm/internal/testpostgres.Parameters|recv=github.com/division-sh/swarm/internal/testpostgres.Connection|results=(github.com/division-sh/swarm/internal/testpostgres.Parameters)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/cliapp/verify_deployment_test.go", Declaration: "TestVerifyFreshPostgresSchemaAccountsAbsenceWithoutClaimingPossession", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(github.com/division-sh/swarm/tests/support/testpostgres.Connection).Parameters", Resolved: "func=func() github.com/division-sh/swarm/tests/support/testpostgres.Parameters|recv=github.com/division-sh/swarm/tests/support/testpostgres.Connection|results=(github.com/division-sh/swarm/tests/support/testpostgres.Parameters)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/cliapp/verify_deployment_test.go", Declaration: "TestVerifyFreshPostgresSchemaAccountsAbsenceWithoutClaimingPossession", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.ParseConnection", Resolved: "func=func(raw string) (github.com/division-sh/swarm/internal/testpostgres.Connection, error)|recv=invalid type|arg=string|results=(github.com/division-sh/swarm/internal/testpostgres.Connection,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/cliapp/verify_deployment_test.go", Declaration: "TestVerifyFreshPostgresSchemaAccountsAbsenceWithoutClaimingPossession", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.ParseConnection", Resolved: "func=func(raw string) (github.com/division-sh/swarm/tests/support/testpostgres.Connection, error)|recv=invalid type|arg=string|results=(github.com/division-sh/swarm/tests/support/testpostgres.Connection,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/cliapp/verify_deployment_test.go", Declaration: "TestVerifyFreshPostgresSchemaAccountsAbsenceWithoutClaimingPossession", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/cliapp/verify_deployment_test.go", Declaration: "TestVerifyFreshPostgresSchemaAccountsAbsenceWithoutClaimingPossession", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "internal/cliapp/verify_deployment_test.go", Declaration: "TestVerifyFreshPostgresSchemaAccountsAbsenceWithoutClaimingPossession", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/internal/testpostgres.Connection", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "local-raw-type", File: "internal/cliapp/verify_deployment_test.go", Declaration: "TestVerifyFreshPostgresSchemaAccountsAbsenceWithoutClaimingPossession", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/tests/support/testpostgres.Connection", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/releasee2e/golden_agent_workload_test.go", Declaration: "goldenPostgresStore", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.ValidateServerCapacity", Resolved: "func=func(ctx context.Context, db interface{QueryRowContext(context.Context, string, ...any) *database/sql.Row}) error|recv=invalid type|arg=context.Context|arg=*database/sql.DB|results=(error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/releasee2e/golden_agent_workload_test.go", Declaration: "goldenPostgresStore", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.ValidateServerCapacity", Resolved: "func=func(ctx context.Context, db interface{QueryRowContext(context.Context, string, ...any) *database/sql.Row}) error|recv=invalid type|arg=context.Context|arg=*database/sql.DB|results=(error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/releasee2e/postgres_capacity_admission_test.go", Declaration: "TestGoldenPostgresStoreCapacityAdmission", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(*github.com/division-sh/swarm/internal/testpostgres.Manager).Acquire", Resolved: "func=func(ctx context.Context, withTemplate bool) (*github.com/division-sh/swarm/internal/testpostgres.Sandbox, error)|recv=*github.com/division-sh/swarm/internal/testpostgres.Manager|arg=context.Context|arg=bool|results=(*github.com/division-sh/swarm/internal/testpostgres.Sandbox,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/releasee2e/postgres_capacity_admission_test.go", Declaration: "TestGoldenPostgresStoreCapacityAdmission", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(*github.com/division-sh/swarm/tests/support/testpostgres.Manager).Acquire", Resolved: "func=func(ctx context.Context, withTemplate bool) (*github.com/division-sh/swarm/tests/support/testpostgres.Sandbox, error)|recv=*github.com/division-sh/swarm/tests/support/testpostgres.Manager|arg=context.Context|arg=bool|results=(*github.com/division-sh/swarm/tests/support/testpostgres.Sandbox,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/releasee2e/postgres_capacity_admission_test.go", Declaration: "TestGoldenPostgresStoreCapacityAdmission", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(*github.com/division-sh/swarm/internal/testpostgres.Sandbox).Release", Resolved: "func=func(ctx context.Context) error|recv=*github.com/division-sh/swarm/internal/testpostgres.Sandbox|arg=context.Context|results=(error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/releasee2e/postgres_capacity_admission_test.go", Declaration: "TestGoldenPostgresStoreCapacityAdmission", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(*github.com/division-sh/swarm/tests/support/testpostgres.Sandbox).Release", Resolved: "func=func(ctx context.Context) error|recv=*github.com/division-sh/swarm/tests/support/testpostgres.Sandbox|arg=context.Context|results=(error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/releasee2e/postgres_capacity_admission_test.go", Declaration: "TestGoldenPostgresStoreCapacityAdmission", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.NewManager", Resolved: "func=func(ctx context.Context, admin github.com/division-sh/swarm/internal/testpostgres.Connection) (*github.com/division-sh/swarm/internal/testpostgres.Manager, error)|recv=invalid type|arg=context.Context|arg=github.com/division-sh/swarm/internal/testpostgres.Connection|results=(*github.com/division-sh/swarm/internal/testpostgres.Manager,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/releasee2e/postgres_capacity_admission_test.go", Declaration: "TestGoldenPostgresStoreCapacityAdmission", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.NewManager", Resolved: "func=func(ctx context.Context, admin github.com/division-sh/swarm/tests/support/testpostgres.Connection) (*github.com/division-sh/swarm/tests/support/testpostgres.Manager, error)|recv=invalid type|arg=context.Context|arg=github.com/division-sh/swarm/tests/support/testpostgres.Connection|results=(*github.com/division-sh/swarm/tests/support/testpostgres.Manager,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/releasee2e/postgres_capacity_admission_test.go", Declaration: "TestGoldenPostgresStoreCapacityAdmission", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.ParseConnection", Resolved: "func=func(raw string) (github.com/division-sh/swarm/internal/testpostgres.Connection, error)|recv=invalid type|arg=string|results=(github.com/division-sh/swarm/internal/testpostgres.Connection,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/releasee2e/postgres_capacity_admission_test.go", Declaration: "TestGoldenPostgresStoreCapacityAdmission", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.ParseConnection", Resolved: "func=func(raw string) (github.com/division-sh/swarm/tests/support/testpostgres.Connection, error)|recv=invalid type|arg=string|results=(github.com/division-sh/swarm/tests/support/testpostgres.Connection,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "internal/releasee2e/postgres_capacity_admission_test.go", Declaration: "TestGoldenPostgresStoreCapacityAdmission", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/internal/testpostgres.Connection", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "local-raw-type", File: "internal/releasee2e/postgres_capacity_admission_test.go", Declaration: "TestGoldenPostgresStoreCapacityAdmission", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/tests/support/testpostgres.Connection", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "internal/releasee2e/postgres_capacity_admission_test.go", Declaration: "TestGoldenPostgresStoreCapacityAdmission", Operation: "local:manager", Resolved: "*github.com/division-sh/swarm/internal/testpostgres.Manager", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "local-raw-type", File: "internal/releasee2e/postgres_capacity_admission_test.go", Declaration: "TestGoldenPostgresStoreCapacityAdmission", Operation: "local:manager", Resolved: "*github.com/division-sh/swarm/tests/support/testpostgres.Manager", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "internal/releasee2e/postgres_capacity_admission_test.go", Declaration: "TestGoldenPostgresStoreCapacityAdmission", Operation: "local:sandbox", Resolved: "*github.com/division-sh/swarm/internal/testpostgres.Sandbox", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "local-raw-type", File: "internal/releasee2e/postgres_capacity_admission_test.go", Declaration: "TestGoldenPostgresStoreCapacityAdmission", Operation: "local:sandbox", Resolved: "*github.com/division-sh/swarm/tests/support/testpostgres.Sandbox", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/artifact_action_result_delivery_test.go", Declaration: "TestRuleResultEventsFlowThroughDurableCallbackDelivery", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/artifact_action_result_delivery_test.go", Declaration: "TestRuleResultEventsFlowThroughDurableCallbackDelivery", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/artifact_action_result_delivery_test.go", Declaration: "TestRuleResultEventsFlowThroughStaticServiceCallbackDelivery", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/artifact_action_result_delivery_test.go", Declaration: "TestRuleResultEventsFlowThroughStaticServiceCallbackDelivery", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/budget_recovery_parity_test.go", Declaration: "TestCompletionBudgetRecoveryProjectionParity", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/budget_recovery_parity_test.go", Declaration: "TestCompletionBudgetRecoveryProjectionParity", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/delivery_completion_test.go", Declaration: "TestEventBusPublishAndWaitIgnoresAmbientSQLTransactionAuthority", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/delivery_completion_test.go", Declaration: "TestEventBusPublishAndWaitIgnoresAmbientSQLTransactionAuthority", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/eventbus_publish_test.go", Declaration: "TestEventBusPublish_AgentOnlyConnectDoesNotAuthorizeUnrelatedNode", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/eventbus_publish_test.go", Declaration: "TestEventBusPublish_AgentOnlyConnectDoesNotAuthorizeUnrelatedNode", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/eventbus_publish_test.go", Declaration: "TestEventBusPublish_NestedDescendantCompletionFollowsDeclaredAncestorConnects", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/eventbus_publish_test.go", Declaration: "TestEventBusPublish_NestedDescendantCompletionFollowsDeclaredAncestorConnects", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/eventbus_publish_test.go", Declaration: "TestEventBusPublish_UndeclaredDescendantEmissionFailsClosedBeforeChildMutation", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/eventbus_publish_test.go", Declaration: "TestEventBusPublish_UndeclaredDescendantEmissionFailsClosedBeforeChildMutation", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineDispatcherAmbientSQLContextDoesNotBypassDurableManifest", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineDispatcherAmbientSQLContextDoesNotBypassDurableManifest", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineDispatcherConsumesImmutableCommittedIntentWithoutAmbientQueue", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineDispatcherConsumesImmutableCommittedIntentWithoutAmbientQueue", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineDispatcherDoesNotConsumeAmbientSQLTransactionAuthority", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineDispatcherDoesNotConsumeAmbientSQLTransactionAuthority", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxAndDispatcher_DeliverInternalSubscribersOutsidePersistedManifest", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxAndDispatcher_DeliverInternalSubscribersOutsidePersistedManifest", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxAndDispatcher_DoesNotFabricateRoutesForPendingInternalDeliveries", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxAndDispatcher_DoesNotFabricateRoutesForPendingInternalDeliveries", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxAndDispatcher_UseCanonicalDirectRecipientManifest", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxAndDispatcher_UseCanonicalDirectRecipientManifest", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxConflictingSameIDBatchRollsBackOrderedOutcomes", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 2},
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxConflictingSameIDBatchRollsBackOrderedOutcomes", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 2},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxDistinctIntentBatchDispatchesEachOnce", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxDistinctIntentBatchDispatchesEachOnce", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxExactDuplicateDispatchIsOperationNoOp", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxExactDuplicateDispatchIsOperationNoOp", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxPersistsEventsAndDeliveriesInTransaction", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxPersistsEventsAndDeliveriesInTransaction", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxPreexistingExactDuplicateBatchDispatchesZero", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxPreexistingExactDuplicateBatchDispatchesZero", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxPreservesAppendOutcomeForEveryIntentInBatch", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxPreservesAppendOutcomeForEveryIntentInBatch", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxPublicationClaimSpansCommitToDispatchAndRollsBack", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxPublicationClaimSpansCommitToDispatchAndRollsBack", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxSubscribedIntentConsumesCanonicalMaterializedRoutePlan", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutboxSubscribedIntentConsumesCanonicalMaterializedRoutePlan", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutbox_TargetFailureDeadLetterErrorFailsClosed", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEngineOutbox_TargetFailureDeadLetterErrorFailsClosed", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEnginePublicationRejectsFilteredDirectIntentBeforePersistence", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/bus/outbox_test.go", Declaration: "TestEnginePublicationRejectsFilteredDirectIntentBeforePersistence", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/cataloge2e/assertions_test.go", Declaration: "TestCatalogCausalEntityIDs_FollowsSourceEventIDChain", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/cataloge2e/assertions_test.go", Declaration: "TestCatalogCausalEntityIDs_FollowsSourceEventIDChain", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/cataloge2e/runtime_harness_test.go", Declaration: "newRuntimeHarnessWithTerminalProvider", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/cataloge2e/runtime_harness_test.go", Declaration: "newRuntimeHarnessWithTerminalProvider", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/channel_runtime_supported_surface_test.go", Declaration: "TestConfiguredChannelRuntimeDispatchesImportedAgentDurablyAcrossSelectedStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/channel_runtime_supported_surface_test.go", Declaration: "TestConfiguredChannelRuntimeDispatchesImportedAgentDurablyAcrossSelectedStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/delivery_lifecycle_conformance_test.go", Declaration: "deliveryLifecycleConformanceBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/delivery_lifecycle_conformance_test.go", Declaration: "deliveryLifecycleConformanceBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/empty_handler_construction_test.go", Declaration: "TestEmptyReporterSettlesWithoutConstructedHeaderMutationBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/empty_handler_construction_test.go", Declaration: "TestEmptyReporterSettlesWithoutConstructedHeaderMutationBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_in_barrier_runtime_conformance_test.go", Declaration: "TestFanInBarrierCanonicalRuntimeCompletesAfterRestartOnBothBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_in_barrier_runtime_conformance_test.go", Declaration: "TestFanInBarrierCanonicalRuntimeCompletesAfterRestartOnBothBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_in_barrier_runtime_conformance_test.go", Declaration: "TestRootToSingletonFirstDeliveryMaterializesReceiverEntityOnBothBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_in_barrier_runtime_conformance_test.go", Declaration: "TestRootToSingletonFirstDeliveryMaterializesReceiverEntityOnBothBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_in_barrier_runtime_conformance_test.go", Declaration: "testFanInSingletonRoutePersistsExactSelectedOwnerOnBothBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_in_barrier_runtime_conformance_test.go", Declaration: "testFanInSingletonRoutePersistsExactSelectedOwnerOnBothBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_in_stream_conformance_test.go", Declaration: "TestKeyedPortfolioStreamRoutesAndRetainsIndependentPeriodsOnBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_in_stream_conformance_test.go", Declaration: "TestKeyedPortfolioStreamRoutesAndRetainsIndependentPeriodsOnBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_b17_mixed_dependency_test.go", Declaration: "b17MixedNestedRuntime", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_b17_mixed_dependency_test.go", Declaration: "b17MixedNestedRuntime", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_expired_evaluation_conformance_test.go", Declaration: "TestFanOutProductionCallerM35ExpiredEvaluationOnBothBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_expired_evaluation_conformance_test.go", Declaration: "TestFanOutProductionCallerM35ExpiredEvaluationOnBothBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_nested_serving_helpers_test.go", Declaration: "newNestedServingRuntime", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_nested_serving_helpers_test.go", Declaration: "newNestedServingRuntime", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_resource_journey_test.go", Declaration: "startDeploymentProgressProbe", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(github.com/division-sh/swarm/internal/testpostgres.Connection).Open", Resolved: "func=func() (*database/sql.DB, error)|recv=github.com/division-sh/swarm/internal/testpostgres.Connection|results=(*database/sql.DB,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_resource_journey_test.go", Declaration: "startDeploymentProgressProbe", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(github.com/division-sh/swarm/tests/support/testpostgres.Connection).Open", Resolved: "func=func() (*database/sql.DB, error)|recv=github.com/division-sh/swarm/tests/support/testpostgres.Connection|results=(*database/sql.DB,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_resource_journey_test.go", Declaration: "startDeploymentProgressProbe", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.ParseConnection", Resolved: "func=func(raw string) (github.com/division-sh/swarm/internal/testpostgres.Connection, error)|recv=invalid type|arg=string|results=(github.com/division-sh/swarm/internal/testpostgres.Connection,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_resource_journey_test.go", Declaration: "startDeploymentProgressProbe", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.ParseConnection", Resolved: "func=func(raw string) (github.com/division-sh/swarm/tests/support/testpostgres.Connection, error)|recv=invalid type|arg=string|results=(github.com/division-sh/swarm/tests/support/testpostgres.Connection,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "internal/runtime/conformance/fan_out_resource_journey_test.go", Declaration: "startDeploymentProgressProbe", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/internal/testpostgres.Connection", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "local-raw-type", File: "internal/runtime/conformance/fan_out_resource_journey_test.go", Declaration: "startDeploymentProgressProbe", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/tests/support/testpostgres.Connection", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_resource_selected_fork_test.go", Declaration: "selectedDeploymentResourceFixtureConfigured", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_resource_selected_fork_test.go", Declaration: "selectedDeploymentResourceFixtureConfigured", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_resource_source_proof_test.go", Declaration: "newDeploymentResourceFixtureWithSource", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_resource_source_proof_test.go", Declaration: "newDeploymentResourceFixtureWithSource", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_semantic_prepared_fault_test.go", Declaration: "TestSemanticProofPreparedFaultMatchesRawBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_semantic_prepared_fault_test.go", Declaration: "TestSemanticProofPreparedFaultMatchesRawBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_semantic_proof_helpers_test.go", Declaration: "newSemanticProofFixtureWithSourcePreparation", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_semantic_proof_helpers_test.go", Declaration: "newSemanticProofFixtureWithSourcePreparation", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_semantic_sql_fault_test.go", Declaration: "newSemanticProofSQLFaultStore", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_semantic_sql_fault_test.go", Declaration: "newSemanticProofSQLFaultStore", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_serving_matrix_helpers_test.go", Declaration: "newServingMatrixFixture", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_serving_matrix_helpers_test.go", Declaration: "newServingMatrixFixture", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_shared_serving_conformance_test.go", Declaration: "TestFanOutSharedServingAcrossRealBundleGrantsOnBothBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_shared_serving_conformance_test.go", Declaration: "TestFanOutSharedServingAcrossRealBundleGrantsOnBothBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_supplemental_helpers_test.go", Declaration: "newSupplementalServingFixture", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/fan_out_supplemental_helpers_test.go", Declaration: "newSupplementalServingFixture", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", Declaration: "TestDynamicFlowSourceRevisionConvergesExactAgentSetAndFencesPredecessorsOnBothBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", Declaration: "TestDynamicFlowSourceRevisionConvergesExactAgentSetAndFencesPredecessorsOnBothBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", Declaration: "TestDynamicFlowTerminalizationAndRouteReplacementRollbackTogetherOnBothBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", Declaration: "TestDynamicFlowTerminalizationAndRouteReplacementRollbackTogetherOnBothBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", Declaration: "TestFanOutDeliveryBarrierCompletesThroughRealEventBusAndPublicReadbackOnBothBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", Declaration: "TestFanOutDeliveryBarrierCompletesThroughRealEventBusAndPublicReadbackOnBothBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", Declaration: "TestHandleEmitTool_TemplateAgentEmissionReachesSameInstanceNodeAndTerminalizesEntity", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", Declaration: "TestHandleEmitTool_TemplateAgentEmissionReachesSameInstanceNodeAndTerminalizesEntity", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", Declaration: "TestNotifyAllChildrenRuntimeConformance_MixedValidAndStaleRoutesPersistAndReplayOnBothBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", Declaration: "TestNotifyAllChildrenRuntimeConformance_MixedValidAndStaleRoutesPersistAndReplayOnBothBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", Declaration: "TestNumericFanOutInternalDeliverySettlementCompletesOnBothBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", Declaration: "TestNumericFanOutInternalDeliverySettlementCompletesOnBothBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", Declaration: "TestNumericFanOutTemplateSelectOrCreateMaterializesOnBothBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", Declaration: "TestNumericFanOutTemplateSelectOrCreateMaterializesOnBothBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", Declaration: "runNumericFanOutReporterShape", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go", Declaration: "runNumericFanOutReporterShape", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/persisted_surfaces_test.go", Declaration: "TestCLISessionFailureDoesNotRotateFromStderrProse", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/persisted_surfaces_test.go", Declaration: "TestCLISessionFailureDoesNotRotateFromStderrProse", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/persisted_surfaces_test.go", Declaration: "TestCanonicalMutationSurface_FailsOnMalformedCanonicalMutationField", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/persisted_surfaces_test.go", Declaration: "TestCanonicalMutationSurface_FailsOnMalformedCanonicalMutationField", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/persisted_surfaces_test.go", Declaration: "TestResetOrphanedSessionAftermathSurface_RoundTripsThroughObservabilityReader", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/persisted_surfaces_test.go", Declaration: "TestResetOrphanedSessionAftermathSurface_RoundTripsThroughObservabilityReader", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/persisted_surfaces_test.go", Declaration: "TestReusedLiveSessionKeepsDeliveryFrontierBoundToCanonicalSession", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/persisted_surfaces_test.go", Declaration: "TestReusedLiveSessionKeepsDeliveryFrontierBoundToCanonicalSession", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/persisted_surfaces_test.go", Declaration: "TestStartupPipelineReplayAftermathSurface_RoundTripsThroughObservabilityReader", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/persisted_surfaces_test.go", Declaration: "TestStartupPipelineReplayAftermathSurface_RoundTripsThroughObservabilityReader", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/persisted_surfaces_test.go", Declaration: "TestStartupRecoveryFailurePlatformEventSurface_PreservesRecoveryFailedWithoutPlatformReset", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/persisted_surfaces_test.go", Declaration: "TestStartupRecoveryFailurePlatformEventSurface_PreservesRecoveryFailedWithoutPlatformReset", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/reply_resolution_conformance_test.go", Declaration: "TestReplyResolutionConformance_DurableExplicitCorrelationFailsClosedOnBothBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/reply_resolution_conformance_test.go", Declaration: "TestReplyResolutionConformance_DurableExplicitCorrelationFailsClosedOnBothBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/reply_resolution_conformance_test.go", Declaration: "TestReplyResolutionConformance_DurableRestartRoutesOverlappingRequestsOnBothBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/reply_resolution_conformance_test.go", Declaration: "TestReplyResolutionConformance_DurableRestartRoutesOverlappingRequestsOnBothBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/reply_resolution_conformance_test.go", Declaration: "TestReplyResolutionConformance_TypedHumanTaskPreservesReplyAuthorityAcrossRestart", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/reply_resolution_conformance_test.go", Declaration: "TestReplyResolutionConformance_TypedHumanTaskPreservesReplyAuthorityAcrossRestart", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/conformance/workflow_stage_lifecycle_identity_conformance_test.go", Declaration: "TestKeyedStageLifecyclePreservesRouteAndEntityAcrossRestartOnBothBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/conformance/workflow_stage_lifecycle_identity_conformance_test.go", Declaration: "TestKeyedStageLifecyclePreservesRouteAndEntityAcrossRestartOnBothBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/deliverylifecycle/claim_commit_runtime_test.go", Declaration: "TestClaimHeartbeatRealStoreAcknowledgedCleanupParity", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/deliverylifecycle/claim_commit_runtime_test.go", Declaration: "TestClaimHeartbeatRealStoreAcknowledgedCleanupParity", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/eventbus_receiver_authority_e2e_test.go", Declaration: "TestManagedEffectAuthorityFollowsActingAgentAcrossNodeChain", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/eventbus_receiver_authority_e2e_test.go", Declaration: "TestManagedEffectAuthorityFollowsActingAgentAcrossNodeChain", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "event", Replacement: "selected canonical event operation and domain event observation port", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/final_flow_instance_authoring_runtime_test.go", Declaration: "TestFinalFlowInstanceAuthoringRuntime_PublishActivatesAndExecutesSelectedTemplateInstance", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/final_flow_instance_authoring_runtime_test.go", Declaration: "TestFinalFlowInstanceAuthoringRuntime_PublishActivatesAndExecutesSelectedTemplateInstance", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/inbound_commit_ack_response_test.go", Declaration: "inboundAcknowledgedSelectedFixture", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/inbound_commit_ack_response_test.go", Declaration: "inboundAcknowledgedSelectedFixture", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/inbound_postgres_test.go", Declaration: "TestInboundGateway_GitHubPausedRuntimePersistsAndReleasesSubscribedDispatch", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/inbound_postgres_test.go", Declaration: "TestInboundGateway_GitHubPausedRuntimePersistsAndReleasesSubscribedDispatch", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/inbound_postgres_test.go", Declaration: "TestInboundGateway_ShopifyPostgresPersistsConfiguredManifestDelivery", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/inbound_postgres_test.go", Declaration: "TestInboundGateway_ShopifyPostgresPersistsConfiguredManifestDelivery", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/inbound_postgres_test.go", Declaration: "TestInboundGateway_SlackPausedRuntimePersistsAndReleasesSubscribedDispatch", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/inbound_postgres_test.go", Declaration: "TestInboundGateway_SlackPausedRuntimePersistsAndReleasesSubscribedDispatch", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/inbound_postgres_test.go", Declaration: "TestInboundGateway_StripePausedRuntimePersistsAndReleasesSubscribedDispatch", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/inbound_postgres_test.go", Declaration: "TestInboundGateway_StripePausedRuntimePersistsAndReleasesSubscribedDispatch", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/inbound_postgres_test.go", Declaration: "TestInboundGateway_TelegramPostgresPersistsConfiguredManifestDelivery", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/inbound_postgres_test.go", Declaration: "TestInboundGateway_TelegramPostgresPersistsConfiguredManifestDelivery", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/inbound_postgres_test.go", Declaration: "TestInboundGateway_TwilioPostgresPersistsConfiguredManifestDelivery", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/inbound_postgres_test.go", Declaration: "TestInboundGateway_TwilioPostgresPersistsConfiguredManifestDelivery", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/inbound_postgres_test.go", Declaration: "TestInboundGateway_TypeformAndIntercomPostgresPersistsConfiguredManifestDelivery", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/inbound_postgres_test.go", Declaration: "TestInboundGateway_TypeformAndIntercomPostgresPersistsConfiguredManifestDelivery", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/manager/flow_activation_test.go", Declaration: "TestActivateFlowInstanceDoesNotConsumeAmbientTransactionAuthority", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/manager/flow_activation_test.go", Declaration: "TestActivateFlowInstanceDoesNotConsumeAmbientTransactionAuthority", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/manager/flow_activation_test.go", Declaration: "TestFlowActivationRejectsForeignSemanticSourceBeforeAnyMutation", Operation: "call:github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/manager/flow_activation_test.go", Declaration: "TestFlowActivationRejectsForeignSemanticSourceBeforeAnyMutation", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.WithSQLTx", Resolved: "func=func(ctx context.Context, tx *database/sql.Tx) context.Context|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|results=(context.Context)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/manager/terminal_retirement_store_test.go", Declaration: "TestTerminalPanicFinalizationBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/manager/terminal_retirement_store_test.go", Declaration: "TestTerminalPanicFinalizationBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/microsoft_graph_connector_client_credentials_supported_surface_test.go", Declaration: "TestMicrosoftGraphClientCredentialsConnectorPackRoundTripThroughActivityJournal", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/microsoft_graph_connector_client_credentials_supported_surface_test.go", Declaration: "TestMicrosoftGraphClientCredentialsConnectorPackRoundTripThroughActivityJournal", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/node_delivery_startup_recovery_test.go", Declaration: "TestCommittedPipelineHandoffCleanupFailureWakesExactDeliveryOnceParity", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/node_delivery_startup_recovery_test.go", Declaration: "TestCommittedPipelineHandoffCleanupFailureWakesExactDeliveryOnceParity", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/node_delivery_startup_recovery_test.go", Declaration: "TestRuntimeStartHydratesPersistedAgentsBeforeRecoveringNodeDeliveriesParity", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/node_delivery_startup_recovery_test.go", Declaration: "TestRuntimeStartHydratesPersistedAgentsBeforeRecoveringNodeDeliveriesParity", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/node_delivery_startup_recovery_test.go", Declaration: "TestRuntimeStartRecoveryDisabledRejectsExecutableDeliveryInventoryParity", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/node_delivery_startup_recovery_test.go", Declaration: "TestRuntimeStartRecoveryDisabledRejectsExecutableDeliveryInventoryParity", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/notion_connector_managed_credential_supported_surface_test.go", Declaration: "TestNotionManagedCredentialConnectorPackRoundTripThroughActivityJournal", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/notion_connector_managed_credential_supported_surface_test.go", Declaration: "TestNotionManagedCredentialConnectorPackRoundTripThroughActivityJournal", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/operator_channel_inbound_supported_surface_test.go", Declaration: "TestInboundGatewaySignedTelegramOperatorChannelClaimSelectedStoreParity", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/operator_channel_inbound_supported_surface_test.go", Declaration: "TestInboundGatewaySignedTelegramOperatorChannelClaimSelectedStoreParity", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/pipeline/workflow_diagnostic_selected_store_external_test.go", Declaration: "newWorkflowDiagnosticNativeFixture", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/pipeline/workflow_diagnostic_selected_store_external_test.go", Declaration: "newWorkflowDiagnosticNativeFixture", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/pipeline/workflow_gate_recovery_external_test.go", Declaration: "openPostgresGateRecoveryStore", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/pipeline/workflow_gate_recovery_external_test.go", Declaration: "openPostgresGateRecoveryStore", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkadmission/revision_source_route_test.go", Declaration: "TestRevisionProjectedSourceRouteDrivesFrontierAndHistoryAcrossReceiverContext", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkadmission/revision_source_route_test.go", Declaration: "TestRevisionProjectedSourceRouteDrivesFrontierAndHistoryAcrossReceiverContext", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkadmission/revision_source_route_test.go", Declaration: "TestRunForkPointRevisionedSourceRouteDrivesSelectedHistoryMatrixPostgres", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkadmission/revision_source_route_test.go", Declaration: "TestRunForkPointRevisionedSourceRouteDrivesSelectedHistoryMatrixPostgres", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/constructed_agent_input_execution_test.go", Declaration: "proveSelectedConstructedAgentInput", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/constructed_agent_input_execution_test.go", Declaration: "proveSelectedConstructedAgentInput", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/control_lifetime_parity_test.go", Declaration: "testSelectedForkControlLifetime", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/control_lifetime_parity_test.go", Declaration: "testSelectedForkControlLifetime", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestActivateSelectedContractRunForkExecutesReplayReadyContractSwapThroughSelectedRecipients", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestActivateSelectedContractRunForkExecutesReplayReadyContractSwapThroughSelectedRecipients", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkAdmitsExactSourceModeBeforeMaterialization", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkAdmitsExactSourceModeBeforeMaterialization", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkAdmitsSameSourceActiveDeliveryForkPointEmission", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkAdmitsSameSourceActiveDeliveryForkPointEmission", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkBranchesWhenNonReplaySourceFactsAdvancedAfterForkPoint", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkBranchesWhenNonReplaySourceFactsAdvancedAfterForkPoint", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkClaudeOAuthPersistsStartupAndTurnCapabilityAuthority", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkClaudeOAuthPersistsStartupAndTurnCapabilityAuthority", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkConstructsAgentRecipientWithoutHandlerMaterializer", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkConstructsAgentRecipientWithoutHandlerMaterializer", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkDispatchesSourceEventsInPersistedChronology", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkDispatchesSourceEventsInPersistedChronology", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkLoadsDBBackedSourceAndStampsPersistedIdentity", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkLoadsDBBackedSourceAndStampsPersistedIdentity", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkMaterializesAndExecutesForkLocalAgentRuntime", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkMaterializesAndExecutesForkLocalAgentRuntime", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkProviderFailurePreservesEvidenceThroughCleanup", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkProviderFailurePreservesEvidenceThroughCleanup", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkRejectsDeferredWorkBeforeMutation", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkRejectsDeferredWorkBeforeMutation", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkRejectsSameEventReplayScopeWriteSkew", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkRejectsSameEventReplayScopeWriteSkew", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkRejectsUnresolvedFrontierBeforeMaterialization", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkRejectsUnresolvedFrontierBeforeMaterialization", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkTreatsDiagnosticPlatformOutcomeAsLineage", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkTreatsDiagnosticPlatformOutcomeAsLineage", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkTreatsPostTSourceConversationHistoryAsBranchDivergence", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkTreatsPostTSourceConversationHistoryAsBranchDivergence", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkTreatsSourceConversationHistoryAsLineage", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkTreatsSourceConversationHistoryAsLineage", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkTreatsSourceReplayScopeMarkerAsLineage", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkTreatsSourceReplayScopeMarkerAsLineage", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkWritesForkLocalExecutionAndLineage", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkWritesForkLocalExecutionAndLineage", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestSelectedContractForkAuthoredHTTPToolPersistsCapabilityAndRejectsHostileAdmission", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestSelectedContractForkAuthoredHTTPToolPersistsCapabilityAndRejectsHostileAdmission", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestSelectedContractForkProviderTurnsUseCanonicalExecutionFrames", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestSelectedContractForkProviderTurnsUseCanonicalExecutionFrames", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestSelectedContractForkRejectsSyntheticCarryDynamicCreationBeforeMutation", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestSelectedContractForkRejectsSyntheticCarryDynamicCreationBeforeMutation", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestSelectedForkPreparedPreflightUsesExactProviderPromptAndExecutesEligibleMCPToolCall", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestSelectedForkPreparedPreflightUsesExactProviderPromptAndExecutesEligibleMCPToolCall", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/module_fork_w5_test.go", Declaration: "TestW5ToolsPinnedModuleRunsThroughSelectedForkBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/module_fork_w5_test.go", Declaration: "TestW5ToolsPinnedModuleRunsThroughSelectedForkBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/operation_lifetime_parity_test.go", Declaration: "TestSelectedContractOperationReplacementBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/operation_lifetime_parity_test.go", Declaration: "TestSelectedContractOperationReplacementBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/pack_platform_retained_proof_test.go", Declaration: "TestPackPlatformRetainedSourceExecutesSelectedForkBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/pack_platform_retained_proof_test.go", Declaration: "TestPackPlatformRetainedSourceExecutesSelectedForkBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/selected_input_execution_test.go", Declaration: "TestSelectedInputExecutionEvidenceBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/selected_input_execution_test.go", Declaration: "TestSelectedInputExecutionEvidenceBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/source_cleanup_test.go", Declaration: "TestSelectedSourceCleanupBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/source_cleanup_test.go", Declaration: "TestSelectedSourceCleanupBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/workspace_preparation_test.go", Declaration: "proveSelectedWorkspacePreparation", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/workspace_preparation_test.go", Declaration: "proveSelectedWorkspacePreparation", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/slack_connector_managed_credential_supported_surface_test.go", Declaration: "TestSlackManagedCredentialConnectorPackRoundTripThroughActivityJournal", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/slack_connector_managed_credential_supported_surface_test.go", Declaration: "TestSlackManagedCredentialConnectorPackRoundTripThroughActivityJournal", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/telegram_connector_supported_surface_test.go", Declaration: "TestTelegramConnectorBoundedIntegrationRoundTripThroughInboundGateway", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/telegram_connector_supported_surface_test.go", Declaration: "TestTelegramConnectorBoundedIntegrationRoundTripThroughInboundGateway", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/template_flow_pilot_runtime_test.go", Declaration: "TestTemplateFlowPilotRuntime_ParentConnectCreatesTemplateInstanceAndPersistedDeliveryRoute", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/template_flow_pilot_runtime_test.go", Declaration: "TestTemplateFlowPilotRuntime_ParentConnectCreatesTemplateInstanceAndPersistedDeliveryRoute", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/template_instance_delivery_test.go", Declaration: "TestTemplateInstanceAcknowledgedPublishDispatchesRoutedSystemNodeWithoutInternalCarrierAndEmpireStyleSideEffect", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/template_instance_delivery_test.go", Declaration: "TestTemplateInstanceAcknowledgedPublishDispatchesRoutedSystemNodeWithoutInternalCarrierAndEmpireStyleSideEffect", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/template_instance_delivery_test.go", Declaration: "TestTemplateInstanceActivationConfigSubscriberPersistsRenderedRouteAndDeliveryRows", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/template_instance_delivery_test.go", Declaration: "TestTemplateInstanceActivationConfigSubscriberPersistsRenderedRouteAndDeliveryRows", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/template_instance_delivery_test.go", Declaration: "TestTemplateInstanceAutoEmitDispatchesLocalHandlerAndEmpireStyleSideEffect", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/template_instance_delivery_test.go", Declaration: "TestTemplateInstanceAutoEmitDispatchesLocalHandlerAndEmpireStyleSideEffect", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/template_instance_delivery_test.go", Declaration: "TestTemplateInstanceConnectLifecyclePublishRollbackDoesNotLeakInstanceOrRoute", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/template_instance_delivery_test.go", Declaration: "TestTemplateInstanceConnectLifecyclePublishRollbackDoesNotLeakInstanceOrRoute", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/template_instance_delivery_test.go", Declaration: "TestTemplateInstanceNoTargetSystemNodeDeliveryPersistsAuthorityBeforeHandlerExecution", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/template_instance_delivery_test.go", Declaration: "TestTemplateInstanceNoTargetSystemNodeDeliveryPersistsAuthorityBeforeHandlerExecution", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/template_instance_delivery_test.go", Declaration: "TestTemplateInstanceNoTargetSystemNodeDeliveryPersistsReceiptAndReplayScopeSeparately", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/template_instance_delivery_test.go", Declaration: "TestTemplateInstanceNoTargetSystemNodeDeliveryPersistsReceiptAndReplayScopeSeparately", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/template_instance_delivery_test.go", Declaration: "TestTemplateInstanceRootOutboxEventDispatchesRoutedSystemNodeAndEmpireStyleSideEffect", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/template_instance_delivery_test.go", Declaration: "TestTemplateInstanceRootOutboxEventDispatchesRoutedSystemNodeAndEmpireStyleSideEffect", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/tools/executor_entity_test.go", Declaration: "TestEntityTools_GetEntityReturnsForkLocalMaterializedRevision", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/tools/executor_entity_test.go", Declaration: "TestEntityTools_GetEntityReturnsForkLocalMaterializedRevision", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/tools/executor_entity_test.go", Declaration: "TestEntityTools_SaveEntityFieldCrossFlowDenialPreservesSelectedForkRuntimeLineage", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/tools/executor_entity_test.go", Declaration: "TestEntityTools_SaveEntityFieldCrossFlowDenialPreservesSelectedForkRuntimeLineage", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/tools/executor_entity_test.go", Declaration: "newEntityToolTestHarnessWithBundleAndLegacyAccess", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/tools/executor_entity_test.go", Declaration: "newEntityToolTestHarnessWithBundleAndLegacyAccess", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/tools/resource_read_selected_store_test.go", Declaration: "TestResourceReadSelectedStore", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/tools/resource_read_selected_store_test.go", Declaration: "TestResourceReadSelectedStore", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/workflow_timer_startup_recovery_test.go", Declaration: "TestGenericScheduleLifecyclePublishesOneShotAndRecurringThroughWorkflowRuntimeOnBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/workflow_timer_startup_recovery_test.go", Declaration: "TestGenericScheduleLifecyclePublishesOneShotAndRecurringThroughWorkflowRuntimeOnBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/workflow_timer_startup_recovery_test.go", Declaration: "TestRuntimeStartFailsClosedWhenManagerHydrationWouldWithholdWorkflowTimersOnBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/workflow_timer_startup_recovery_test.go", Declaration: "TestRuntimeStartFailsClosedWhenManagerHydrationWouldWithholdWorkflowTimersOnBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/workflow_timer_startup_recovery_test.go", Declaration: "TestRuntimeStartRestoresWorkflowTimersWithoutGenericScheduleStoreOnBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/workflow_timer_startup_recovery_test.go", Declaration: "TestRuntimeStartRestoresWorkflowTimersWithoutGenericScheduleStoreOnBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/workflow_timer_startup_recovery_test.go", Declaration: "TestRuntimeStartWithholdsDueSchedulesAndTimersUntilDynamicTopologyCompletesOnBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/workflow_timer_startup_recovery_test.go", Declaration: "TestRuntimeStartWithholdsDueSchedulesAndTimersUntilDynamicTopologyCompletesOnBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/agent_memory_supported_surface_test.go", Declaration: "runStandingTelegramMemorySupportedSurface", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/agent_memory_supported_surface_test.go", Declaration: "runStandingTelegramMemorySupportedSurface", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/agent_mock_supported_surface_test.go", Declaration: "runMockAgentSupportedSurface", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/agent_mock_supported_surface_test.go", Declaration: "runMockAgentSupportedSurface", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/budget_stage_recovery_test.go", Declaration: "TestBudgetRecoveryLoadsExactRetainedStagesAndStatelessPostureBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/budget_stage_recovery_test.go", Declaration: "TestBudgetRecoveryLoadsExactRetainedStagesAndStatelessPostureBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/channel_delivery_inbound_supported_surface_test.go", Declaration: "runChannelDeliveryInboundDispositionE2E", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "channel", Replacement: "selected channel/delivery owner and named channel fault/readback", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/channel_delivery_inbound_supported_surface_test.go", Declaration: "runChannelDeliveryInboundDispositionE2E", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "channel", Replacement: "selected channel/delivery owner and named channel fault/readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/channel_onboarding_identity_e2e_test.go", Declaration: "newChannelOnboardingE2EHarness", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "channel", Replacement: "selected channel/delivery owner and named channel fault/readback", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/channel_onboarding_identity_e2e_test.go", Declaration: "newChannelOnboardingE2EHarness", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "channel", Replacement: "selected channel/delivery owner and named channel fault/readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/channel_onboarding_supported_surface_test.go", Declaration: "runChannelConnectTelegramFirstUserJourney", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "channel", Replacement: "selected channel/delivery owner and named channel fault/readback", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/channel_onboarding_supported_surface_test.go", Declaration: "runChannelConnectTelegramFirstUserJourney", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "channel", Replacement: "selected channel/delivery owner and named channel fault/readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/channel_onboarding_supported_surface_test.go", Declaration: "writeChannelOnboardingPostgresRuntimeConfig", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(github.com/division-sh/swarm/internal/testpostgres.Connection).Parameters", Resolved: "func=func() github.com/division-sh/swarm/internal/testpostgres.Parameters|recv=github.com/division-sh/swarm/internal/testpostgres.Connection|results=(github.com/division-sh/swarm/internal/testpostgres.Parameters)", Family: "channel", Replacement: "selected channel/delivery owner and named channel fault/readback", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/channel_onboarding_supported_surface_test.go", Declaration: "writeChannelOnboardingPostgresRuntimeConfig", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(github.com/division-sh/swarm/tests/support/testpostgres.Connection).Parameters", Resolved: "func=func() github.com/division-sh/swarm/tests/support/testpostgres.Parameters|recv=github.com/division-sh/swarm/tests/support/testpostgres.Connection|results=(github.com/division-sh/swarm/tests/support/testpostgres.Parameters)", Family: "channel", Replacement: "selected channel/delivery owner and named channel fault/readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/channel_onboarding_supported_surface_test.go", Declaration: "writeChannelOnboardingPostgresRuntimeConfig", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.ParseConnection", Resolved: "func=func(raw string) (github.com/division-sh/swarm/internal/testpostgres.Connection, error)|recv=invalid type|arg=string|results=(github.com/division-sh/swarm/internal/testpostgres.Connection,error)", Family: "channel", Replacement: "selected channel/delivery owner and named channel fault/readback", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/channel_onboarding_supported_surface_test.go", Declaration: "writeChannelOnboardingPostgresRuntimeConfig", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.ParseConnection", Resolved: "func=func(raw string) (github.com/division-sh/swarm/tests/support/testpostgres.Connection, error)|recv=invalid type|arg=string|results=(github.com/division-sh/swarm/tests/support/testpostgres.Connection,error)", Family: "channel", Replacement: "selected channel/delivery owner and named channel fault/readback", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "internal/serveapp/channel_onboarding_supported_surface_test.go", Declaration: "writeChannelOnboardingPostgresRuntimeConfig", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/internal/testpostgres.Connection", Family: "channel", Replacement: "selected channel/delivery owner and named channel fault/readback", Multiplicity: 1},
			{Kind: "local-raw-type", File: "internal/serveapp/channel_onboarding_supported_surface_test.go", Declaration: "writeChannelOnboardingPostgresRuntimeConfig", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/tests/support/testpostgres.Connection", Family: "channel", Replacement: "selected channel/delivery owner and named channel fault/readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/claude_attempt_identity_test.go", Declaration: "newClaudeAttemptProofBackend", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/claude_attempt_identity_test.go", Declaration: "newClaudeAttemptProofBackend", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/dynamic_topology_startup_context_test.go", Declaration: "testDynamicTopologyStartupPreflightTwoContexts", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/dynamic_topology_startup_context_test.go", Declaration: "testDynamicTopologyStartupPreflightTwoContexts", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/inbound_admission_runtime_test.go", Declaration: "TestInboundAdmissionSupportedSurfaceStartupFailuresSQLiteAndPostgres", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/inbound_admission_runtime_test.go", Declaration: "TestInboundAdmissionSupportedSurfaceStartupFailuresSQLiteAndPostgres", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/inbound_admission_runtime_test.go", Declaration: "runInboundAdmissionSupportedSurfacePolicyMatrix", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/inbound_admission_runtime_test.go", Declaration: "runInboundAdmissionSupportedSurfacePolicyMatrix", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/issue2277_audit_probe_test.go", Declaration: "TestAudit2277StoppedRunReadinessRestart", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/issue2277_audit_probe_test.go", Declaration: "TestAudit2277StoppedRunReadinessRestart", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/lifecycle_diagnostic_recovery_test.go", Declaration: "TestLifecycleDiagnosticServedCrashRecoveryAndResetBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/lifecycle_diagnostic_recovery_test.go", Declaration: "TestLifecycleDiagnosticServedCrashRecoveryAndResetBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/lifecycle_release_process_test.go", Declaration: "startLifecycleReleaseProcess", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/lifecycle_release_process_test.go", Declaration: "startLifecycleReleaseProcess", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/lifecycle_transition_restart_test.go", Declaration: "lifecycleRestartHarness", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/lifecycle_transition_restart_test.go", Declaration: "lifecycleRestartHarness", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/mailbox_completion_fixture_test.go", Declaration: "newRetainedMailboxCompletionRuntimeConfigured", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "mailbox", Replacement: "selected mailbox/completion owner and named mailbox fault/readback", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/mailbox_completion_fixture_test.go", Declaration: "newRetainedMailboxCompletionRuntimeConfigured", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "mailbox", Replacement: "selected mailbox/completion owner and named mailbox fault/readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/mailbox_completion_process_test.go", Declaration: "mailboxCompletionProcessHarnessWithSelectionCut", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "mailbox", Replacement: "selected mailbox/completion owner and named mailbox fault/readback", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/mailbox_completion_process_test.go", Declaration: "mailboxCompletionProcessHarnessWithSelectionCut", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "mailbox", Replacement: "selected mailbox/completion owner and named mailbox fault/readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/mailbox_completion_test.go", Declaration: "gateCompletionHarness", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "mailbox", Replacement: "selected mailbox/completion owner and named mailbox fault/readback", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/mailbox_completion_test.go", Declaration: "gateCompletionHarness", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "mailbox", Replacement: "selected mailbox/completion owner and named mailbox fault/readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/mailbox_cursor_restart_test.go", Declaration: "TestServedMailboxCursorRetainedRestartParity", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "mailbox", Replacement: "selected mailbox/completion owner and named mailbox fault/readback", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/mailbox_cursor_restart_test.go", Declaration: "TestServedMailboxCursorRetainedRestartParity", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "mailbox", Replacement: "selected mailbox/completion owner and named mailbox fault/readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestInitializeStateStoresPostgresDoesNotCreateGeneratedEntityTables", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "TestInitializeStateStoresPostgresDoesNotCreateGeneratedEntityTables", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "installServeRuntimePostgresTestStoresForDatabase", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "installServeRuntimePostgresTestStoresForDatabase", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "installServeRuntimePostgresTestStoresForDatabase", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "installServeRuntimePostgresTestStoresForDatabase", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/paused_startup_context_test.go", Declaration: "TestPausedMixedRunsStartupBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/paused_startup_context_test.go", Declaration: "TestPausedMixedRunsStartupBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/postgres_graceful_contract_test.go", Declaration: "TestServePostgresGracefulContract", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/postgres_graceful_contract_test.go", Declaration: "TestServePostgresGracefulContract", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/receiver_restart_test.go", Declaration: "TestReceiverCompositionRestartBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/receiver_restart_test.go", Declaration: "TestReceiverCompositionRestartBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/reset_process_crash_test.go", Declaration: "TestResetProcessDeathRecoversBeforeSourceAdmissionBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/reset_process_crash_test.go", Declaration: "TestResetProcessDeathRecoversBeforeSourceAdmissionBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/reset_projection_real_docker_test.go", Declaration: "TestResetStartupProjectionDisposalRealDocker", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/reset_projection_real_docker_test.go", Declaration: "TestResetStartupProjectionDisposalRealDocker", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/run_fork_runtime_test.go", Declaration: "setPostgresEnvFromDSN", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.(github.com/division-sh/swarm/internal/testpostgres.Connection).Parameters", Resolved: "func=func() github.com/division-sh/swarm/internal/testpostgres.Parameters|recv=github.com/division-sh/swarm/internal/testpostgres.Connection|results=(github.com/division-sh/swarm/internal/testpostgres.Parameters)", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/run_fork_runtime_test.go", Declaration: "setPostgresEnvFromDSN", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.(github.com/division-sh/swarm/tests/support/testpostgres.Connection).Parameters", Resolved: "func=func() github.com/division-sh/swarm/tests/support/testpostgres.Parameters|recv=github.com/division-sh/swarm/tests/support/testpostgres.Connection|results=(github.com/division-sh/swarm/tests/support/testpostgres.Parameters)", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/run_fork_runtime_test.go", Declaration: "setPostgresEnvFromDSN", Operation: "call:github.com/division-sh/swarm/internal/testpostgres.ParseConnection", Resolved: "func=func(raw string) (github.com/division-sh/swarm/internal/testpostgres.Connection, error)|recv=invalid type|arg=string|results=(github.com/division-sh/swarm/internal/testpostgres.Connection,error)", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/run_fork_runtime_test.go", Declaration: "setPostgresEnvFromDSN", Operation: "call:github.com/division-sh/swarm/tests/support/testpostgres.ParseConnection", Resolved: "func=func(raw string) (github.com/division-sh/swarm/tests/support/testpostgres.Connection, error)|recv=invalid type|arg=string|results=(github.com/division-sh/swarm/tests/support/testpostgres.Connection,error)", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "internal/serveapp/run_fork_runtime_test.go", Declaration: "setPostgresEnvFromDSN", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/internal/testpostgres.Connection", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "local-raw-type", File: "internal/serveapp/run_fork_runtime_test.go", Declaration: "setPostgresEnvFromDSN", Operation: "local:connection", Resolved: "github.com/division-sh/swarm/tests/support/testpostgres.Connection", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/run_state_runtime_test.go", Declaration: "TestRunState_KeepsSupportedRunRunningUntilManagerWorkSettles", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/run_state_runtime_test.go", Declaration: "TestRunState_KeepsSupportedRunRunningUntilManagerWorkSettles", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/run_state_runtime_test.go", Declaration: "TestRunState_PreservesRunningTruthWhileManagerWorkIsActive", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/run_state_runtime_test.go", Declaration: "TestRunState_PreservesRunningTruthWhileManagerWorkIsActive", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/schema_boot_teaching_error_test.go", Declaration: "TestServeBootLegacySchemaRendersTeachingError", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/schema_boot_teaching_error_test.go", Declaration: "TestServeBootLegacySchemaRendersTeachingError", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartEmptyPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/serve_selected_lifecycle_test.go", Declaration: "TestTerminalStoreReleaseJoinsDelayedAccessBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/serve_selected_lifecycle_test.go", Declaration: "TestTerminalStoreReleaseJoinsDelayedAccessBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/standing_ingress_dormancy_supported_surface_test.go", Declaration: "TestDormantIngressProvisionRestartSignedInputBothStores", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/standing_ingress_dormancy_supported_surface_test.go", Declaration: "TestDormantIngressProvisionRestartSignedInputBothStores", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/standing_ingress_frozen_admission_test.go", Declaration: "runServeIngressFrozenCredentialJourney", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/standing_ingress_frozen_admission_test.go", Declaration: "runServeIngressFrozenCredentialJourney", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/standing_ingress_supported_surface_test.go", Declaration: "TestStandingIngressSupportedSurfacePostgresRestartPreservesAuthorityAndReplies", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/standing_ingress_supported_surface_test.go", Declaration: "TestStandingIngressSupportedSurfacePostgresRestartPreservesAuthorityAndReplies", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/standing_ingress_supported_surface_test.go", Declaration: "TestStandingRestartMixedHealthyAndTerminalProcessParity", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/standing_ingress_supported_surface_test.go", Declaration: "TestStandingRestartMixedHealthyAndTerminalProcessParity", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/standing_runtime_context_owner_test.go", Declaration: "openStandingRuntimeContextStore", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/standing_runtime_context_owner_test.go", Declaration: "openStandingRuntimeContextStore", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/serveapp/store_backend_selection_test.go", Declaration: "TestSelectedOwnerAPICapabilityMatrixIsExplicitAcrossBackends", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/store_backend_selection_test.go", Declaration: "TestSelectedOwnerAPICapabilityMatrixIsExplicitAcrossBackends", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/store/durable_data_selected_store_test.go", Declaration: "forEachDurableDataStore", Operation: "call:github.com/division-sh/swarm/internal/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/store/durable_data_selected_store_test.go", Declaration: "forEachDurableDataStore", Operation: "call:github.com/division-sh/swarm/tests/support/testutil.StartPostgres", Resolved: "func=func(t *testing.T) (dsn string, db *database/sql.DB, cleanup func())|recv=invalid type|arg=*testing.T|results=(string,*database/sql.DB,func())", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		},
		{
			{Kind: "unresolved-excluded-source", File: "internal/testpostgres/creator_process_other.go", Declaration: "creatorProcessCommand", Operation: "declaration:a2d24bcf6f843fbc626f0f87163d84796f27114ad05db05e252c4e19ddd3fb36", Resolved: "unresolved|source-context:36a16b52bf14440144b554f73347ccdcc4f6d4774a510be5480dbb44b7244330", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
			{Kind: "unresolved-excluded-source", File: "tests/support/testpostgres/creator_process_other.go", Declaration: "creatorProcessCommand", Operation: "declaration:a2d24bcf6f843fbc626f0f87163d84796f27114ad05db05e252c4e19ddd3fb36", Resolved: "unresolved|source-context:36a16b52bf14440144b554f73347ccdcc4f6d4774a510be5480dbb44b7244330", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
		},
		{
			{Kind: "unresolved-excluded-source", File: "internal/testpostgres/creator_process_other.go", Declaration: "validateCreatorProcessSupport", Operation: "declaration:3c2db684f7bc38bf5ea45998042a774e87933410669a7cbbc37198a44eb41aef", Resolved: "unresolved|source-context:36a16b52bf14440144b554f73347ccdcc4f6d4774a510be5480dbb44b7244330", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
			{Kind: "unresolved-excluded-source", File: "tests/support/testpostgres/creator_process_other.go", Declaration: "validateCreatorProcessSupport", Operation: "declaration:3c2db684f7bc38bf5ea45998042a774e87933410669a7cbbc37198a44eb41aef", Resolved: "unresolved|source-context:36a16b52bf14440144b554f73347ccdcc4f6d4774a510be5480dbb44b7244330", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
		},
		{
			{Kind: "unresolved-excluded-source", File: "internal/testpostgres/creator_process_windows.go", Declaration: "creatorProcessCommand", Operation: "declaration:ed457bf06a62cf26d90418f52d872030f55ad28a70661c745b8584e8e0b4448a", Resolved: "unresolved|source-context:63ac198c31cd891ea59bf7f5b017f4312608e4c1ca0ebe6f2ace0bab7274cd89", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
			{Kind: "unresolved-excluded-source", File: "tests/support/testpostgres/creator_process_windows.go", Declaration: "creatorProcessCommand", Operation: "declaration:ed457bf06a62cf26d90418f52d872030f55ad28a70661c745b8584e8e0b4448a", Resolved: "unresolved|source-context:63ac198c31cd891ea59bf7f5b017f4312608e4c1ca0ebe6f2ace0bab7274cd89", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
		},
		{
			{Kind: "unresolved-excluded-source", File: "internal/testpostgres/creator_process_windows.go", Declaration: "validateCreatorProcessSupport", Operation: "declaration:965bc8aa46e602e6c0493c6e73f958bf73f1fb4c41cc01409adcc01899a98062", Resolved: "unresolved|source-context:63ac198c31cd891ea59bf7f5b017f4312608e4c1ca0ebe6f2ace0bab7274cd89", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
			{Kind: "unresolved-excluded-source", File: "tests/support/testpostgres/creator_process_windows.go", Declaration: "validateCreatorProcessSupport", Operation: "declaration:965bc8aa46e602e6c0493c6e73f958bf73f1fb4c41cc01409adcc01899a98062", Resolved: "unresolved|source-context:63ac198c31cd891ea59bf7f5b017f4312608e4c1ca0ebe6f2ace0bab7274cd89", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
		},
		{
			{Kind: "unresolved-excluded-source", File: "internal/testpostgres/file_lock_other.go", Declaration: "Close", Operation: "declaration:194a6579a48bcb7923c83f0caa512f5475b19a2162ed2c57a48e996d6407e7ed", Resolved: "unresolved|source-context:e0f4236112db90ff0c662be674e005caf1fa09b71d6630cf2f63230a0071ef1e", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
			{Kind: "unresolved-excluded-source", File: "tests/support/testpostgres/file_lock_other.go", Declaration: "Close", Operation: "declaration:194a6579a48bcb7923c83f0caa512f5475b19a2162ed2c57a48e996d6407e7ed", Resolved: "unresolved|source-context:e0f4236112db90ff0c662be674e005caf1fa09b71d6630cf2f63230a0071ef1e", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
		},
		{
			{Kind: "unresolved-excluded-source", File: "internal/testpostgres/file_lock_other.go", Declaration: "acquireExistingFileLock", Operation: "declaration:6f0c4bdd5b14f319ce78dcf5781eedb851d138d1cc3e4a771fde93a9daba03b9", Resolved: "unresolved|source-context:e0f4236112db90ff0c662be674e005caf1fa09b71d6630cf2f63230a0071ef1e", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
			{Kind: "unresolved-excluded-source", File: "tests/support/testpostgres/file_lock_other.go", Declaration: "acquireExistingFileLock", Operation: "declaration:6f0c4bdd5b14f319ce78dcf5781eedb851d138d1cc3e4a771fde93a9daba03b9", Resolved: "unresolved|source-context:e0f4236112db90ff0c662be674e005caf1fa09b71d6630cf2f63230a0071ef1e", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
		},
		{
			{Kind: "unresolved-excluded-source", File: "internal/testpostgres/file_lock_other.go", Declaration: "acquireFileLock", Operation: "declaration:1d318518e4a673c30ddcb34478e2fe5f38d9aeee409254bcedca31d9533d2a43", Resolved: "unresolved|source-context:e0f4236112db90ff0c662be674e005caf1fa09b71d6630cf2f63230a0071ef1e", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
			{Kind: "unresolved-excluded-source", File: "tests/support/testpostgres/file_lock_other.go", Declaration: "acquireFileLock", Operation: "declaration:1d318518e4a673c30ddcb34478e2fe5f38d9aeee409254bcedca31d9533d2a43", Resolved: "unresolved|source-context:e0f4236112db90ff0c662be674e005caf1fa09b71d6630cf2f63230a0071ef1e", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
		},
		{
			{Kind: "unresolved-excluded-source", File: "internal/testpostgres/file_lock_windows.go", Declaration: "Close", Operation: "declaration:a83e1e3319f66c21fcf66de1ad494a51f626b8f62ee6595995b28ddbc058017f", Resolved: "unresolved|source-context:5a1dd18f5a8f09fb09f40a4b9fbf2a009be9c970b758eab179fb19e602044425", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
			{Kind: "unresolved-excluded-source", File: "tests/support/testpostgres/file_lock_windows.go", Declaration: "Close", Operation: "declaration:a83e1e3319f66c21fcf66de1ad494a51f626b8f62ee6595995b28ddbc058017f", Resolved: "unresolved|source-context:5a1dd18f5a8f09fb09f40a4b9fbf2a009be9c970b758eab179fb19e602044425", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
		},
		{
			{Kind: "unresolved-excluded-source", File: "internal/testpostgres/file_lock_windows.go", Declaration: "acquireExistingFileLock", Operation: "declaration:57007a73670fe474e060c3fd63c217b57fbca5287a9da6cdb8f6f225cd9ed814", Resolved: "unresolved|source-context:5a1dd18f5a8f09fb09f40a4b9fbf2a009be9c970b758eab179fb19e602044425", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
			{Kind: "unresolved-excluded-source", File: "tests/support/testpostgres/file_lock_windows.go", Declaration: "acquireExistingFileLock", Operation: "declaration:57007a73670fe474e060c3fd63c217b57fbca5287a9da6cdb8f6f225cd9ed814", Resolved: "unresolved|source-context:5a1dd18f5a8f09fb09f40a4b9fbf2a009be9c970b758eab179fb19e602044425", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
		},
		{
			{Kind: "unresolved-excluded-source", File: "internal/testpostgres/file_lock_windows.go", Declaration: "acquireFileLock", Operation: "declaration:1ba23574c8868530e5629b777ac1338226ec163f5fff53b031accfc5f29a8bb6", Resolved: "unresolved|source-context:5a1dd18f5a8f09fb09f40a4b9fbf2a009be9c970b758eab179fb19e602044425", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
			{Kind: "unresolved-excluded-source", File: "tests/support/testpostgres/file_lock_windows.go", Declaration: "acquireFileLock", Operation: "declaration:1ba23574c8868530e5629b777ac1338226ec163f5fff53b031accfc5f29a8bb6", Resolved: "unresolved|source-context:5a1dd18f5a8f09fb09f40a4b9fbf2a009be9c970b758eab179fb19e602044425", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
		},
		{
			{Kind: "unresolved-excluded-source", File: "internal/testpostgres/file_lock_windows.go", Declaration: "openFileLock", Operation: "declaration:70ff652ba1eaa4dfb41b7f2d85e1c37f2dd4d81b57c8a37ff47434d018333396", Resolved: "unresolved|source-context:5a1dd18f5a8f09fb09f40a4b9fbf2a009be9c970b758eab179fb19e602044425", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
			{Kind: "unresolved-excluded-source", File: "tests/support/testpostgres/file_lock_windows.go", Declaration: "openFileLock", Operation: "declaration:70ff652ba1eaa4dfb41b7f2d85e1c37f2dd4d81b57c8a37ff47434d018333396", Resolved: "unresolved|source-context:5a1dd18f5a8f09fb09f40a4b9fbf2a009be9c970b758eab179fb19e602044425", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
		},
		{
			{Kind: "unresolved-excluded-source", File: "internal/testpostgres/lease_inheritance_other.go", Declaration: "inheritFileLock", Operation: "declaration:2122d30930ba12dee1c5acd558276d99ab3c512a1cca635f4307e1885b64cc56", Resolved: "unresolved|source-context:36a16b52bf14440144b554f73347ccdcc4f6d4774a510be5480dbb44b7244330", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
			{Kind: "unresolved-excluded-source", File: "tests/support/testpostgres/lease_inheritance_other.go", Declaration: "inheritFileLock", Operation: "declaration:2122d30930ba12dee1c5acd558276d99ab3c512a1cca635f4307e1885b64cc56", Resolved: "unresolved|source-context:36a16b52bf14440144b554f73347ccdcc4f6d4774a510be5480dbb44b7244330", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
		},
		{
			{Kind: "unresolved-excluded-source", File: "internal/testpostgres/lease_inheritance_windows.go", Declaration: "inheritFileLock", Operation: "declaration:ea649540d126e7d2537d1a1ff2f17b4941874439521071e45168c8aa56c104f4", Resolved: "unresolved|source-context:353a1dbf289eb263316e51ae20ee1e011c2addc43f2957e25506128e38ce7135", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
			{Kind: "unresolved-excluded-source", File: "tests/support/testpostgres/lease_inheritance_windows.go", Declaration: "inheritFileLock", Operation: "declaration:ea649540d126e7d2537d1a1ff2f17b4941874439521071e45168c8aa56c104f4", Resolved: "unresolved|source-context:353a1dbf289eb263316e51ae20ee1e011c2addc43f2957e25506128e38ce7135", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
		},
		{
			{Kind: "unresolved-excluded-source", File: "internal/testpostgres/lease_inheritance_windows_test.go", Declaration: "TestWindowsLeaseInheritanceAddsExactHandle", Operation: "declaration:7d63e598cb8eb8fcd80b9f547bda47a7da79a9001d4d210d344f6ef39522732e", Resolved: "unresolved|source-context:3a3b493a5f57bcff71c2366678339c8a01e90557fdc766293936e3ef8a6bff95", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
			{Kind: "unresolved-excluded-source", File: "tests/support/testpostgres/lease_inheritance_windows_test.go", Declaration: "TestWindowsLeaseInheritanceAddsExactHandle", Operation: "declaration:7d63e598cb8eb8fcd80b9f547bda47a7da79a9001d4d210d344f6ef39522732e", Resolved: "unresolved|source-context:3a3b493a5f57bcff71c2366678339c8a01e90557fdc766293936e3ef8a6bff95", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
		},
		{
			{Kind: "unresolved-excluded-source", File: "internal/testpostgres/state_authority_other.go", Declaration: "validateStateAccess", Operation: "declaration:498df7d4b05b485eb0abe46b2ac9e8a96bf2af8dc4be5d058da40fed98838380", Resolved: "unresolved|source-context:4a9cb317f60b23046b41a6899991fc917495d11ff4163d4c97733055126aced6", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
			{Kind: "unresolved-excluded-source", File: "tests/support/testpostgres/state_authority_other.go", Declaration: "validateStateAccess", Operation: "declaration:498df7d4b05b485eb0abe46b2ac9e8a96bf2af8dc4be5d058da40fed98838380", Resolved: "unresolved|source-context:4a9cb317f60b23046b41a6899991fc917495d11ff4163d4c97733055126aced6", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
		},
		{
			{Kind: "unresolved-excluded-source", File: "internal/testpostgres/state_authority_windows.go", Declaration: "validateStateAccess", Operation: "declaration:74b1601c2bf3e0b492565a1ad221a7442ee1c8f3d8321ea84f8538f477250471", Resolved: "unresolved|source-context:5ab3127bd0ceec47eefd4c067e1acd663749ee3467ebd191fc61d28f385e489d", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
			{Kind: "unresolved-excluded-source", File: "tests/support/testpostgres/state_authority_windows.go", Declaration: "validateStateAccess", Operation: "declaration:74b1601c2bf3e0b492565a1ad221a7442ee1c8f3d8321ea84f8538f477250471", Resolved: "unresolved|source-context:5ab3127bd0ceec47eefd4c067e1acd663749ee3467ebd191fc61d28f385e489d", Family: "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptPostgresOrigin", Operation: "call:github.com/division-sh/swarm/internal/testutil/runlifecyclefixture.corruptOrigin", Resolved: "func=func(ctx context.Context, db *database/sql.DB, dialect github.com/division-sh/swarm/internal/testutil/runlifecyclefixture.Dialect, runID string, origin github.com/division-sh/swarm/internal/runtime/runlifecycle.RunOrigin) error|recv=<nil>|arg=context.Context|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/testutil/runlifecyclefixture.Dialect|arg=string|arg=github.com/division-sh/swarm/internal/runtime/runlifecycle.RunOrigin|results=(error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptPostgresOrigin", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runlifecyclefixture.corruptOrigin", Resolved: "func=func(ctx context.Context, db *database/sql.DB, dialect github.com/division-sh/swarm/tests/support/testutil/runlifecyclefixture.Dialect, runID string, origin github.com/division-sh/swarm/internal/runtime/runlifecycle.RunOrigin) error|recv=<nil>|arg=context.Context|arg=*database/sql.DB|arg=github.com/division-sh/swarm/tests/support/testutil/runlifecyclefixture.Dialect|arg=string|arg=github.com/division-sh/swarm/internal/runtime/runlifecycle.RunOrigin|results=(error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-type", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptPostgresOrigin", Operation: "param:db", Resolved: "*database/sql.DB", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-type", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptPostgresOrigin", Operation: "param:db", Resolved: "*database/sql.DB", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptPostgresSource", Operation: "call:database/sql.(*database/sql.DB).ExecContext", Resolved: "func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.DB|arg=context.Context|arg=string|arg=string|arg=string|results=(database/sql.Result,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptPostgresSource", Operation: "call:database/sql.(*database/sql.DB).ExecContext", Resolved: "func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.DB|arg=context.Context|arg=string|arg=string|arg=string|results=(database/sql.Result,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-type", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptPostgresSource", Operation: "param:db", Resolved: "*database/sql.DB", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-type", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptPostgresSource", Operation: "param:db", Resolved: "*database/sql.DB", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptPostgresState", Operation: "call:database/sql.(*database/sql.DB).ExecContext", Resolved: "func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.DB|arg=context.Context|arg=string|arg=string|arg=string|arg=any|results=(database/sql.Result,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptPostgresState", Operation: "call:database/sql.(*database/sql.DB).ExecContext", Resolved: "func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.DB|arg=context.Context|arg=string|arg=string|arg=string|arg=any|results=(database/sql.Result,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-type", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptPostgresState", Operation: "param:db", Resolved: "*database/sql.DB", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-type", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptPostgresState", Operation: "param:db", Resolved: "*database/sql.DB", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptSQLiteOrigin", Operation: "call:github.com/division-sh/swarm/internal/testutil/runlifecyclefixture.corruptOrigin", Resolved: "func=func(ctx context.Context, db *database/sql.DB, dialect github.com/division-sh/swarm/internal/testutil/runlifecyclefixture.Dialect, runID string, origin github.com/division-sh/swarm/internal/runtime/runlifecycle.RunOrigin) error|recv=<nil>|arg=context.Context|arg=*database/sql.DB|arg=github.com/division-sh/swarm/internal/testutil/runlifecyclefixture.Dialect|arg=string|arg=github.com/division-sh/swarm/internal/runtime/runlifecycle.RunOrigin|results=(error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptSQLiteOrigin", Operation: "call:github.com/division-sh/swarm/tests/support/testutil/runlifecyclefixture.corruptOrigin", Resolved: "func=func(ctx context.Context, db *database/sql.DB, dialect github.com/division-sh/swarm/tests/support/testutil/runlifecyclefixture.Dialect, runID string, origin github.com/division-sh/swarm/internal/runtime/runlifecycle.RunOrigin) error|recv=<nil>|arg=context.Context|arg=*database/sql.DB|arg=github.com/division-sh/swarm/tests/support/testutil/runlifecyclefixture.Dialect|arg=string|arg=github.com/division-sh/swarm/internal/runtime/runlifecycle.RunOrigin|results=(error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-type", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptSQLiteOrigin", Operation: "param:db", Resolved: "*database/sql.DB", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-type", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptSQLiteOrigin", Operation: "param:db", Resolved: "*database/sql.DB", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptSQLiteSource", Operation: "call:database/sql.(*database/sql.DB).ExecContext", Resolved: "func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.DB|arg=context.Context|arg=string|arg=string|arg=string|results=(database/sql.Result,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptSQLiteSource", Operation: "call:database/sql.(*database/sql.DB).ExecContext", Resolved: "func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.DB|arg=context.Context|arg=string|arg=string|arg=string|results=(database/sql.Result,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-type", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptSQLiteSource", Operation: "param:db", Resolved: "*database/sql.DB", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-type", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptSQLiteSource", Operation: "param:db", Resolved: "*database/sql.DB", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptSQLiteState", Operation: "call:database/sql.(*database/sql.DB).ExecContext", Resolved: "func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.DB|arg=context.Context|arg=string|arg=string|arg=any|arg=string|results=(database/sql.Result,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptSQLiteState", Operation: "call:database/sql.(*database/sql.DB).ExecContext", Resolved: "func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.DB|arg=context.Context|arg=string|arg=string|arg=any|arg=string|results=(database/sql.Result,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-type", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptSQLiteState", Operation: "param:db", Resolved: "*database/sql.DB", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-type", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "CorruptSQLiteState", Operation: "param:db", Resolved: "*database/sql.DB", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "ForcePostgresCompletionCandidateRevision", Operation: "call:database/sql.(*database/sql.Tx).ExecContext", Resolved: "func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.Tx|arg=context.Context|arg=string|arg=string|results=(database/sql.Result,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "ForcePostgresCompletionCandidateRevision", Operation: "call:database/sql.(*database/sql.Tx).ExecContext", Resolved: "func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.Tx|arg=context.Context|arg=string|arg=string|results=(database/sql.Result,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-type", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "ForcePostgresCompletionCandidateRevision", Operation: "param:tx", Resolved: "*database/sql.Tx", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-type", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "ForcePostgresCompletionCandidateRevision", Operation: "param:tx", Resolved: "*database/sql.Tx", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "ForceSQLiteCompletionCandidateRevision", Operation: "call:database/sql.(*database/sql.Tx).ExecContext", Resolved: "func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.Tx|arg=context.Context|arg=string|arg=string|results=(database/sql.Result,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "ForceSQLiteCompletionCandidateRevision", Operation: "call:database/sql.(*database/sql.Tx).ExecContext", Resolved: "func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.Tx|arg=context.Context|arg=string|arg=string|results=(database/sql.Result,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-type", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "ForceSQLiteCompletionCandidateRevision", Operation: "param:tx", Resolved: "*database/sql.Tx", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-type", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "ForceSQLiteCompletionCandidateRevision", Operation: "param:tx", Resolved: "*database/sql.Tx", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "corruptOrigin", Operation: "call:database/sql.(*database/sql.DB).ExecContext", Resolved: "func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.DB|arg=context.Context|arg=string|arg=[]any|results=(database/sql.Result,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "corruptOrigin", Operation: "call:database/sql.(*database/sql.DB).ExecContext", Resolved: "func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.DB|arg=context.Context|arg=string|arg=[]any|results=(database/sql.Result,error)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-type", File: "internal/testutil/runlifecyclefixture/fixture.go", Declaration: "corruptOrigin", Operation: "param:db", Resolved: "*database/sql.DB", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-type", File: "tests/support/testutil/runlifecyclefixture/fixture.go", Declaration: "corruptOrigin", Operation: "param:db", Resolved: "*database/sql.DB", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "internal/testutil/runtimepipelinefixture/context.go", Declaration: "SQLConn", Operation: "local:conn", Resolved: "*database/sql.Conn", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
			{Kind: "local-raw-type", File: "tests/support/testutil/runtimepipelinefixture/context.go", Declaration: "SQLConn", Operation: "local:conn", Resolved: "*database/sql.Conn", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-type", File: "internal/testutil/runtimepipelinefixture/context.go", Declaration: "SQLConn", Operation: "result:", Resolved: "*database/sql.Conn", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
			{Kind: "raw-type", File: "tests/support/testutil/runtimepipelinefixture/context.go", Declaration: "SQLConn", Operation: "result:", Resolved: "*database/sql.Conn", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
		},
		{
			{Kind: "local-raw-type", File: "internal/testutil/runtimepipelinefixture/context.go", Declaration: "SQLTx", Operation: "local:tx", Resolved: "*database/sql.Tx", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
			{Kind: "local-raw-type", File: "tests/support/testutil/runtimepipelinefixture/context.go", Declaration: "SQLTx", Operation: "local:tx", Resolved: "*database/sql.Tx", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-type", File: "internal/testutil/runtimepipelinefixture/context.go", Declaration: "SQLTx", Operation: "result:", Resolved: "*database/sql.Tx", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
			{Kind: "raw-type", File: "tests/support/testutil/runtimepipelinefixture/context.go", Declaration: "SQLTx", Operation: "result:", Resolved: "*database/sql.Tx", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/testutil/runtimepipelinefixture/context.go", Declaration: "WithSQLConn", Operation: "call:context.WithValue", Resolved: "func=func(parent context.Context, key any, val any) context.Context|recv=invalid type|arg=context.Context|arg=github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.connKey|arg=*database/sql.Conn|results=(context.Context)", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
			{Kind: "raw-operation", File: "tests/support/testutil/runtimepipelinefixture/context.go", Declaration: "WithSQLConn", Operation: "call:context.WithValue", Resolved: "func=func(parent context.Context, key any, val any) context.Context|recv=invalid type|arg=context.Context|arg=github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.connKey|arg=*database/sql.Conn|results=(context.Context)", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
		},
		{
			{Kind: "context-withvalue", File: "internal/testutil/runtimepipelinefixture/context.go", Declaration: "WithSQLConn", Operation: "context:WithValue", Resolved: "func(parent context.Context, key any, val any) context.Context|arg=context.Context|arg=github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.connKey|arg=*database/sql.Conn", Family: "authority-carrier", Replacement: "original selected construction/work-lifetime owner; no getter or transaction-context recovery", Multiplicity: 1},
			{Kind: "context-withvalue", File: "tests/support/testutil/runtimepipelinefixture/context.go", Declaration: "WithSQLConn", Operation: "context:WithValue", Resolved: "func(parent context.Context, key any, val any) context.Context|arg=context.Context|arg=github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.connKey|arg=*database/sql.Conn", Family: "authority-carrier", Replacement: "original selected construction/work-lifetime owner; no getter or transaction-context recovery", Multiplicity: 1},
		},
		{
			{Kind: "raw-type", File: "internal/testutil/runtimepipelinefixture/context.go", Declaration: "WithSQLConn", Operation: "param:conn", Resolved: "*database/sql.Conn", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
			{Kind: "raw-type", File: "tests/support/testutil/runtimepipelinefixture/context.go", Declaration: "WithSQLConn", Operation: "param:conn", Resolved: "*database/sql.Conn", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/testutil/runtimepipelinefixture/context.go", Declaration: "WithSQLTx", Operation: "call:context.WithValue", Resolved: "func=func(parent context.Context, key any, val any) context.Context|recv=invalid type|arg=context.Context|arg=github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.txKey|arg=*database/sql.Tx|results=(context.Context)", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
			{Kind: "raw-operation", File: "tests/support/testutil/runtimepipelinefixture/context.go", Declaration: "WithSQLTx", Operation: "call:context.WithValue", Resolved: "func=func(parent context.Context, key any, val any) context.Context|recv=invalid type|arg=context.Context|arg=github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.txKey|arg=*database/sql.Tx|results=(context.Context)", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
		},
		{
			{Kind: "context-withvalue", File: "internal/testutil/runtimepipelinefixture/context.go", Declaration: "WithSQLTx", Operation: "context:WithValue", Resolved: "func(parent context.Context, key any, val any) context.Context|arg=context.Context|arg=github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.txKey|arg=*database/sql.Tx", Family: "authority-carrier", Replacement: "original selected construction/work-lifetime owner; no getter or transaction-context recovery", Multiplicity: 1},
			{Kind: "context-withvalue", File: "tests/support/testutil/runtimepipelinefixture/context.go", Declaration: "WithSQLTx", Operation: "context:WithValue", Resolved: "func(parent context.Context, key any, val any) context.Context|arg=context.Context|arg=github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.txKey|arg=*database/sql.Tx", Family: "authority-carrier", Replacement: "original selected construction/work-lifetime owner; no getter or transaction-context recovery", Multiplicity: 1},
		},
		{
			{Kind: "raw-type", File: "internal/testutil/runtimepipelinefixture/context.go", Declaration: "WithSQLTx", Operation: "param:tx", Resolved: "*database/sql.Tx", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
			{Kind: "raw-type", File: "tests/support/testutil/runtimepipelinefixture/context.go", Declaration: "WithSQLTx", Operation: "param:tx", Resolved: "*database/sql.Tx", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/testutil/runtimepipelinefixture/context.go", Declaration: "WithoutSQLTx", Operation: "call:*ast.ParenExpr", Resolved: "func=*database/sql.Tx|recv=<nil>|arg=untyped nil", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
			{Kind: "raw-operation", File: "tests/support/testutil/runtimepipelinefixture/context.go", Declaration: "WithoutSQLTx", Operation: "call:*ast.ParenExpr", Resolved: "func=*database/sql.Tx|recv=<nil>|arg=untyped nil", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/testutil/runtimepipelinefixture/context.go", Declaration: "WithoutSQLTx", Operation: "call:context.WithValue", Resolved: "func=func(parent context.Context, key any, val any) context.Context|recv=invalid type|arg=context.Context|arg=github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.txKey|arg=*database/sql.Tx|results=(context.Context)", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
			{Kind: "raw-operation", File: "tests/support/testutil/runtimepipelinefixture/context.go", Declaration: "WithoutSQLTx", Operation: "call:context.WithValue", Resolved: "func=func(parent context.Context, key any, val any) context.Context|recv=invalid type|arg=context.Context|arg=github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.txKey|arg=*database/sql.Tx|results=(context.Context)", Family: "workflow", Replacement: "selected workflow/construction owner; domain observation port for readback", Multiplicity: 1},
		},
		{
			{Kind: "context-withvalue", File: "internal/testutil/runtimepipelinefixture/context.go", Declaration: "WithoutSQLTx", Operation: "context:WithValue", Resolved: "func(parent context.Context, key any, val any) context.Context|arg=context.Context|arg=github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture.txKey|arg=*database/sql.Tx", Family: "authority-carrier", Replacement: "original selected construction/work-lifetime owner; no getter or transaction-context recovery", Multiplicity: 1},
			{Kind: "context-withvalue", File: "tests/support/testutil/runtimepipelinefixture/context.go", Declaration: "WithoutSQLTx", Operation: "context:WithValue", Resolved: "func(parent context.Context, key any, val any) context.Context|arg=context.Context|arg=github.com/division-sh/swarm/tests/support/testutil/runtimepipelinefixture.txKey|arg=*database/sql.Tx", Family: "authority-carrier", Replacement: "original selected construction/work-lifetime owner; no getter or transaction-context recovery", Multiplicity: 1},
		},
	}
}

// #2409/6652034819: five exact counted signatures retain their existing owners.
func debtPortableRuntimeSignatureTransitions() [5][2]authorityDebtSite {
	var pairs [5][2]authorityDebtSite
	old := authorityDebtSite{
		Kind: "raw-operation", File: "internal/serveapp/fork_receiver_acquisition_effect_only_test.go", Declaration: "TestSelectedForkSupplementalReceiverAcquisitionWithoutPostRevisionEmissionBothStores",
		Operation: "call:github.com/division-sh/swarm/internal/serveapp.requireSupplementalForkAcquisitionSettlement",
		Resolved:  "func=func(t *testing.T, rt github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime, runID string, eventID string, policy github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting.ForkReceiverPolicy) github.com/division-sh/swarm/internal/events.DeliveryRoute|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|arg=string|arg=string|arg=github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting.ForkReceiverPolicy|results=(github.com/division-sh/swarm/internal/events.DeliveryRoute)",
		Family:    "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 2,
	}
	current := old
	current.Resolved = "func=func(t *testing.T, rt github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime, runID string, eventID string, policy github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting.ForkReceiverPolicy) github.com/division-sh/swarm/internal/events.DeliveryRoute|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|arg=string|arg=string|arg=github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting.ForkReceiverPolicy|results=(github.com/division-sh/swarm/internal/events.DeliveryRoute)"
	pairs[0] = [2]authorityDebtSite{old, current}
	old = authorityDebtSite{
		Kind: "callback-type", File: "internal/serveapp/fork_receiver_ownership_test.go", Declaration: "forkReceiverJourneyOptions",
		Operation: "field:afterSource",
		Resolved:  "func(*testing.T, github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime, *github.com/division-sh/swarm/internal/serveapp.selectedStoreOwner, string, string, string, []github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting.ForkReceiver)",
		Family:    "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1,
	}
	current = old
	current.Resolved = "func(*testing.T, github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime, *github.com/division-sh/swarm/internal/serveapp.selectedStoreOwner, string, string, string, []github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting.ForkReceiver)"
	pairs[1] = [2]authorityDebtSite{old, current}
	old = authorityDebtSite{
		Kind: "raw-operation", File: "internal/serveapp/fork_receiver_ownership_test.go", Declaration: "runForkReceiverOwnershipJourney",
		Operation: "call:github.com/division-sh/swarm/internal/serveapp.afterSource",
		Resolved:  "func=func(*testing.T, github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime, *github.com/division-sh/swarm/internal/serveapp.selectedStoreOwner, string, string, string, []github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting.ForkReceiver)|recv=github.com/division-sh/swarm/internal/serveapp.forkReceiverJourneyOptions|arg=*testing.T|arg=github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|arg=*github.com/division-sh/swarm/internal/serveapp.selectedStoreOwner|arg=string|arg=string|arg=string|arg=[]github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting.ForkReceiver|results=()",
		Family:    "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1,
	}
	current = old
	current.Resolved = "func=func(*testing.T, github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime, *github.com/division-sh/swarm/internal/serveapp.selectedStoreOwner, string, string, string, []github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting.ForkReceiver)|recv=github.com/division-sh/swarm/internal/serveapp.forkReceiverJourneyOptions|arg=*testing.T|arg=github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|arg=*github.com/division-sh/swarm/internal/serveapp.selectedStoreOwner|arg=string|arg=string|arg=string|arg=[]github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting.ForkReceiver|results=()"
	pairs[2] = [2]authorityDebtSite{old, current}
	old = authorityDebtSite{
		Kind: "raw-operation", File: "internal/serveapp/fork_receiver_ownership_test.go", Declaration: "runForkReceiverOwnershipJourney",
		Operation: "call:github.com/division-sh/swarm/internal/serveapp.requireForkReceiverExecution",
		Resolved:  "func=func(t *testing.T, rt github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime, runID string, sourceEvent string, path string, label string, kind string, entityID string, policy github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting.ForkReceiverPolicy) github.com/division-sh/swarm/internal/events.DeliveryRoute|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting.ForkReceiverPolicy|results=(github.com/division-sh/swarm/internal/events.DeliveryRoute)",
		Family:    "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1,
	}
	current = old
	current.Resolved = "func=func(t *testing.T, rt github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime, runID string, sourceEvent string, path string, label string, kind string, entityID string, policy github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting.ForkReceiverPolicy) github.com/division-sh/swarm/internal/events.DeliveryRoute|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting.ForkReceiverPolicy|results=(github.com/division-sh/swarm/internal/events.DeliveryRoute)"
	pairs[3] = [2]authorityDebtSite{old, current}
	old = authorityDebtSite{
		Kind: "raw-operation", File: "internal/serveapp/fork_receiver_ownership_test.go", Declaration: "runForkReceiverOwnershipJourney",
		Operation: "call:github.com/division-sh/swarm/internal/serveapp.requireForkReceiverMutationOwners",
		Resolved:  "func=func(t *testing.T, rt github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime, runID string, eventID string, prefix string, receivers []github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting.ForkReceiver)|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|arg=string|arg=string|arg=string|arg=[]github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting.ForkReceiver|results=()",
		Family:    "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 2,
	}
	current = old
	current.Resolved = "func=func(t *testing.T, rt github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime, runID string, eventID string, prefix string, receivers []github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting.ForkReceiver)|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|arg=string|arg=string|arg=string|arg=[]github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting.ForkReceiver|results=()"
	pairs[4] = [2]authorityDebtSite{old, current}
	return pairs
}

// #2881/6567159073: five existing fixture calls retain their native owner.
func debtLiveRollbackSignatureTransitions() [5][2]authorityDebtSite {
	const prefix = "github.com/division-sh/swarm/internal/"
	const backend = prefix + "servedparity.Backend"
	const runtime = "*" + prefix + "runtime.Runtime"
	const snapshot = "map[string]" + prefix + "store/storetest.SelectedForkStorageTableSnapshot"
	const construction = prefix + "store/storetest.ReceiverConstructionStorage"
	const startOld = "func=func(t *testing.T, backend " + backend + ") (string, *database/sql.DB, string)|recv=<nil>|arg=*testing.T|arg=" + backend + "|results=(string,*database/sql.DB,string)"
	const startCurrent = "func=func(t *testing.T, backend " + backend + ") (string, *database/sql.DB, string, " + runtime + ")|recv=<nil>|arg=*testing.T|arg=" + backend + "|results=(string,*database/sql.DB,string," + runtime + ")"
	const checkOld = "func=func(t *testing.T, db *database/sql.DB, backend " + backend + ", idempotencyKey string, eventID string, runID string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=" + backend + "|arg=string|arg=string|arg=string|results=()"
	const checkCurrent = "func=func(t *testing.T, db *database/sql.DB, backend " + backend + ", idempotencyKey string, eventID string, runID string, rt " + runtime + ", before " + snapshot + ", construction " + construction + ")|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=" + backend + "|arg=string|arg=string|arg=string|arg=" + runtime + "|arg=" + snapshot + "|arg=" + construction + "|results=()"
	var pairs [5][2]authorityDebtSite
	for index, declaration := range [3]string{
		"TestRootConnectedTemplatePublicationAcknowledgmentLossBothStores",
		"TestRootConnectedTemplatePublicationRollbackIsAtomicAcrossSelectedStores",
		"TestRootConnectedTemplatePublicationRollsBackAfterDeliveryAndCompletionAcrossSelectedStores",
	} {
		file := "internal/serveapp/test_command_runtime_test.go"
		if index == 0 {
			file = "internal/serveapp/root_publication_ack_loss_test.go"
		}
		old := authorityDebtSite{Kind: "raw-operation", File: file, Declaration: declaration,
			Operation: "call:" + prefix + "serveapp.startPublicInputRollbackRuntime", Resolved: startOld,
			Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}
		current := old
		current.Resolved = startCurrent
		pairs[index] = [2]authorityDebtSite{old, current}
		if index == 0 {
			continue
		}
		old.Operation, old.Resolved = "call:"+prefix+"serveapp.requirePublicInputRollbackNoResidue", checkOld
		current = old
		current.Resolved = checkCurrent
		pairs[index+2] = [2]authorityDebtSite{old, current}
	}
	return pairs
}

// #2764/6498334917 preserves this counted, build-excluded nil-only stub.
func debtDeploymentHandlerSignatureTransition() [2]authorityDebtSite {
	old := authorityDebtSite{
		Kind: "unresolved-excluded-source", File: "internal/serveapp/channel_deployment_unsupported.go",
		Declaration: "deploymentChannelHandlers",
		Operation:   "declaration:7890217cded1dd01ed764ec3a9fab9d2daad2c882b0920a7d3ead721e5710d50",
		Resolved:    "unresolved|source-context:14375e4f0f053ea0b9c5c0715496c22cb494c816197b9efdb94415151ce4d24d",
		Family:      "excluded-source-uncertainty", Replacement: "existing canonical owner or narrow source-evidenced classification; never a file exemption or baseline increase",
		Multiplicity: 1,
	}
	current := old
	current.Operation = "declaration:e62729fa9a6dac500f6809768d77fb1725871a7871addd5fa304b5e73726cb39"
	return [2]authorityDebtSite{old, current}
}

// #2791/6396939990 preserves the same counted INSERT with exact rooted arguments.
func debtSelectedBindingRootArgumentTransition() [2]authorityDebtSite {
	old := authorityDebtSite{
		Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "buildSelectedForkProofContainer",
		Operation: "call:database/sql.(*database/sql.DB).ExecContext",
		Resolved:  "func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.DB|arg=context.Context|arg=string|arg=string|arg=string|arg=string|arg=int64|arg=string|arg=time.Time|results=(database/sql.Result,error)",
		Family:    "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1,
	}
	current := old
	current.Resolved = "func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.DB|arg=context.Context|arg=string|arg=string|arg=string|arg=string|arg=int64|arg=string|arg=time.Time|arg=string|arg=string|results=(database/sql.Result,error)"
	return [2]authorityDebtSite{old, current}
}

func debtCatalogRuntimeRowsSignatureTransition() (authorityDebtSite, authorityDebtSite) {
	old := authorityDebtSite{
		Kind: "raw-operation", File: "internal/runtime/cataloge2e/tier12_runtime_fork_e2e_test.go",
		Declaration: "TestTier12RuntimeFork_SelectedContractForkExecutionFixture",
		Operation:   "call:github.com/division-sh/swarm/internal/runtime/cataloge2e.assertSelectedContractForkRuntimeRows",
		Resolved:    "func=func(t testing.TB, db *database/sql.DB, forkRunID string, forkEventID string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|results=()",
		Family:      "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1,
	}
	current := old
	current.Resolved = "func=func(t testing.TB, db *database/sql.DB, instances github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowInstancePersistenceReader, forkRunID string, forkEventID string)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=*github.com/division-sh/swarm/internal/store.PostgresStore|arg=string|arg=string|results=()"
	return old, current
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

const debtForkProofFile = "internal/serveapp/issue2277_audit_probe_test.go"
const debtForkProofOldName = "TestAudit2277ForkCompletionLoss"
const debtForkProofNewName = "TestAudit2277ForkDurableResultBypassesGenericCompletionBothStores"
const debtForkProofCollectorFrom = "3958381013996586ea697cd868abcacec29e9316706276f603f32523f74b5cb6"
const debtForkProofSuccessorSHA = "77b03c87a0baf626a492fa23112e14cf07a840f3c8c5bba1c395ddec87838198"

// B6129214825 authorizes only these nine identities and 21 existing occurrences.
func debtForkProofRows() [9]authorityDebtSite {
	return [9]authorityDebtSite{
		{Kind: "local-raw-type", File: debtForkProofFile, Declaration: debtForkProofOldName, Operation: "local:rt", Resolved: "github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 1},
		{Kind: "raw-operation", File: debtForkProofFile, Declaration: debtForkProofOldName, Operation: "call:database/sql.(*database/sql.DB).Exec", Resolved: "func=func(query string, args ...any) (database/sql.Result, error)|recv=*database/sql.DB|arg=string|results=(database/sql.Result,error)", Family: "unowned-authority", Replacement: "exact domain owner not identified here; escalate #2542, never invent a SQL escape", Multiplicity: 5},
		{Kind: "raw-operation", File: debtForkProofFile, Declaration: debtForkProofOldName, Operation: "call:database/sql.(*database/sql.DB).QueryRow", Resolved: "func=func(query string, args ...any) *database/sql.Row|recv=*database/sql.DB|arg=string|arg=string|results=(*database/sql.Row)", Family: "observation", Replacement: "domain observation port on the exact selected owner; missing exact port: escalate #2542", Multiplicity: 4},
		{Kind: "raw-operation", File: debtForkProofFile, Declaration: debtForkProofOldName, Operation: "call:database/sql.(*database/sql.DB).QueryRow", Resolved: "func=func(query string, args ...any) *database/sql.Row|recv=*database/sql.DB|arg=string|results=(*database/sql.Row)", Family: "observation", Replacement: "domain observation port on the exact selected owner; missing exact port: escalate #2542", Multiplicity: 2},
		{Kind: "raw-operation", File: debtForkProofFile, Declaration: debtForkProofOldName, Operation: "call:database/sql.(*database/sql.Row).Scan", Resolved: "func=func(dest ...any) error|recv=*database/sql.Row|arg=*int|results=(error)", Family: "observation", Replacement: "domain observation port on the exact selected owner; missing exact port: escalate #2542", Multiplicity: 4},
		{Kind: "raw-operation", File: debtForkProofFile, Declaration: debtForkProofOldName, Operation: "call:database/sql.(*database/sql.Row).Scan", Resolved: "func=func(dest ...any) error|recv=*database/sql.Row|arg=*string|arg=*string|results=(error)", Family: "observation", Replacement: "domain observation port on the exact selected owner; missing exact port: escalate #2542", Multiplicity: 1},
		{Kind: "raw-operation", File: debtForkProofFile, Declaration: debtForkProofOldName, Operation: "call:database/sql.(*database/sql.Row).Scan", Resolved: "func=func(dest ...any) error|recv=*database/sql.Row|arg=*string|results=(error)", Family: "observation", Replacement: "domain observation port on the exact selected owner; missing exact port: escalate #2542", Multiplicity: 1},
		{Kind: "raw-operation", File: debtForkProofFile, Declaration: debtForkProofOldName, Operation: "call:github.com/division-sh/swarm/internal/serveapp.repeatedStaticRunSnapshot", Resolved: "func=func(t *testing.T, db *database/sql.DB, runID string) map[string][][]string|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|results=(map[string][][]string)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 2},
		{Kind: "raw-operation", File: debtForkProofFile, Declaration: debtForkProofOldName, Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedControlProofRuntimeWithFixture", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, fixture func(*testing.T) string) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=func(t *testing.T) string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
	}
}

func debtForkProofRenamed(site authorityDebtSite) authorityDebtSite {
	site.Declaration = debtForkProofNewName
	return site
}

func debtForkProofTransferNeeded(actual, base, source map[string]authorityDebtSite) bool {
	for _, old := range debtForkProofRows() {
		if actual[debtForkProofRenamed(old).identity()].Multiplicity > 0 &&
			(base[old.identity()].Multiplicity > 0 || source[old.identity()].Multiplicity > 0) {
			return true
		}
	}
	return false
}

func debtForkProofComparisons(actual, head, base, source map[string]authorityDebtSite) (map[string]authorityDebtSite, map[string]authorityDebtSite, map[string]authorityDebtSite, []string) {
	for _, state := range []map[string]authorityDebtSite{actual, head, base, source} {
		oldPresent, newPresent := false, false
		for _, old := range debtForkProofRows() {
			oldPresent = oldPresent || state[old.identity()].Multiplicity > 0
			newPresent = newPresent || state[debtForkProofRenamed(old).identity()].Multiplicity > 0
		}
		if oldPresent && newPresent {
			return head, base, source, []string{"historical and renamed fork proof debt coexist"}
		}
	}
	if !debtForkProofTransferNeeded(actual, base, source) {
		return head, base, source, nil
	}
	for _, old := range debtForkProofRows() {
		current := debtForkProofRenamed(old)
		if actual[current.identity()] != current || base[old.identity()] != old || source[old.identity()] != old ||
			(head[old.identity()] != old && head[current.identity()] != current) {
			return head, base, source, []string{"fork proof rename lost or changed its exact debt donor"}
		}
	}
	for _, old := range debtForkProofRows() {
		current := debtForkProofRenamed(old)
		head = debtForwardQuiescenceUncertainty(head, old, current)
		base = debtForwardQuiescenceUncertainty(base, old, current)
		source = debtForwardQuiescenceUncertainty(source, old, current)
	}
	return head, base, source, nil
}

func debtForkProofCollectorTransition(trusted, head map[string]authorityDebtSite) bool {
	for _, old := range debtForkProofRows() {
		current := debtForkProofRenamed(old)
		if trusted[old.identity()] != old || trusted[current.identity()].Multiplicity != 0 ||
			(head[old.identity()] != old && head[current.identity()] != current) {
			return false
		}
	}
	return len(authorityDebtRatchet(head, head, trusted, trusted)) == 0
}

func debtForkProofSourcePins(root string) error {
	data, err := os.ReadFile(filepath.Join(root, "tools/fixture-codemod/pipeline-observations/recipes.json"))
	if err != nil {
		return err
	}
	var rows []struct {
		Family, File, Function, Before, After, Successor string
		Removed                                          bool
		Mechanical                                       json.RawMessage
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return err
	}
	matches := 0
	for _, row := range rows {
		if row.File != debtForkProofFile || row.Function != debtForkProofOldName {
			continue
		}
		matches++
		if row.Family != "native-served-delivery-quiescence-caller" || row.Removed || len(row.Mechanical) != 0 ||
			debtBaselineChecksum([]byte(row.Before)) != "c205dbc515d172b290e459393d2d0f17a37f52534bbecd78530783322636c0f7" ||
			debtBaselineChecksum([]byte(row.After)) != "8a77bfcf4a078afc07b19c2bf238c9d3b50fd6591de92b24224bda662bca1fe3" ||
			debtBaselineChecksum([]byte(row.Successor)) != debtForkProofSuccessorSHA {
			return fmt.Errorf("fork proof transfer changed its historical recipe or exact Successor")
		}
	}
	if matches != 1 {
		return fmt.Errorf("fork proof transfer requires one exact historical recipe")
	}
	data, err = os.ReadFile(filepath.Join(root, debtForkProofFile))
	if err != nil {
		return err
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, debtForkProofFile, data, parser.AllErrors)
	if err != nil {
		return err
	}
	matches = 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Name.Name == debtForkProofOldName {
			return fmt.Errorf("historical fork proof declaration survives its forward transfer")
		}
		if fn.Name.Name != debtForkProofNewName {
			continue
		}
		matches++
		body := data[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset]
		if debtBaselineChecksum(body) != debtForkProofSuccessorSHA {
			return fmt.Errorf("fork proof source differs from its exact conserved Successor")
		}
	}
	if matches != 1 {
		return fmt.Errorf("fork proof transfer requires one live renamed declaration")
	}
	return nil
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
	for _, path := range []string{"internal/store/persistence_authority_debt_census_test.go", "internal/store/persistence_authority_debt_test.go", "internal/store/persistence_authority_debt_cache_test.go", "internal/store/persistence_authority_debt_sidecar_test.go", "internal/store/persistence_authority_debt_process_lease_test.go"} {
		if path == "internal/store/persistence_authority_debt_cache_test.go" || path == "internal/store/persistence_authority_debt_sidecar_test.go" || path == "internal/store/persistence_authority_debt_process_lease_test.go" {
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
	prior := hex.EncodeToString(hash.Sum(nil))
	const nativePath = "internal/store/persistence_authority_debt_native_fixture_test.go"
	file, err = parser.ParseFile(token.NewFileSet(), filepath.Join(root, nativePath), nil, parser.AllErrors)
	if os.IsNotExist(err) && (prior == debtFixtureRoleCollectorTo || prior == debtForkProofCollectorTo || prior == debtProcessLeaseCollectorTo) {
		return prior, nil // Exact immutable predecessors predate this classification.
	}
	if err != nil {
		return "", err
	}
	canonical.Reset()
	if err := format.Node(&canonical, token.NewFileSet(), file); err != nil {
		return "", err
	}
	fmt.Fprintf(hash, "%s\x00%s\x00", nativePath, canonical.Bytes())
	prior = hex.EncodeToString(hash.Sum(nil))
	const excludedPath = "internal/store/persistence_authority_debt_excluded_fixture_test.go"
	file, err = parser.ParseFile(token.NewFileSet(), filepath.Join(root, excludedPath), nil, parser.AllErrors)
	if os.IsNotExist(err) && prior == debtNativeFixtureCollectorTo {
		return prior, nil // Exact immutable predecessor of #642/6202173993.
	}
	if err != nil {
		return "", err
	}
	canonical.Reset()
	if err := format.Node(&canonical, token.NewFileSet(), file); err != nil {
		return "", err
	}
	fmt.Fprintf(hash, "%s\x00%s\x00", excludedPath, canonical.Bytes())
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
		if err := debtValidateCollectorIdentity(baseCollector, collector, trusted, head, collector); err != nil {
			t.Fatal(err)
		}
	}
	if head.BootstrapSource != trusted.BootstrapSource || head.Collector != collector {
		t.Fatal("debt baseline was reseeded or changed collector identity")
	}
	if bootstrap && !debtSitesEqual(head.Sites, trusted.Sites) {
		t.Fatal("bootstrap baseline must equal actual extraction-base debt exactly")
	}
	if debtForkProofTransferNeeded(actual, trusted.Sites, baseActual) {
		if err := debtForkProofSourcePins(root); err != nil {
			t.Fatal(err)
		}
	}
	if debtSelectedFixtureTransferNeeded(actual, trusted.Sites, baseActual) {
		if err := debtSelectedFixtureSourcePins(root, baseRoot); err != nil {
			t.Fatal(err)
		}
	}
	if err := debtValidateProcessLeaseRelocationSource(root, baseRoot, actual, baseActual); err != nil {
		t.Fatal(err)
	}
	comparisonBaseline, comparisonSource := trusted.Sites, baseActual
	if trusted.Collector == debtDeploymentRegistrationCollectorFrom && collector == debtDeploymentRegistrationCollectorTo {
		for key, site := range debtWithDeploymentRegistrationStubs(nil) {
			if actual[key] != site || baseActual[key].Multiplicity != 0 {
				t.Fatal("reviewed deployment stub addition does not match exact source identities")
			}
		}
		comparisonBaseline = debtWithDeploymentRegistrationStubs(comparisonBaseline)
		comparisonSource = debtWithDeploymentRegistrationStubs(comparisonSource)
	}
	if trusted.Collector == debtS1CollectorFrom && collector == debtS1CollectorTo {
		for key, site := range debtWithS1RetainedSourceConstructors(nil) {
			if actual[key] != site || baseActual[key].Multiplicity != 0 {
				t.Fatal("reviewed S1 constructor additions do not match the exact two counted sites")
			}
		}
		comparisonBaseline = debtWithS1RetainedSourceConstructors(comparisonBaseline)
		comparisonSource = debtWithS1RetainedSourceConstructors(comparisonSource)
	}
	// Retain extractor output before any comparison-copy correspondence.
	receiptRoot := filepath.Join(root, "test-results")
	if err := os.MkdirAll(receiptRoot, 0700); err != nil {
		t.Fatal(err)
	}
	receipt, err := os.MkdirTemp(receiptRoot, "authority-full-tuples-")
	if err != nil {
		t.Fatal(err)
	}
	for name, sites := range map[string]map[string]authorityDebtSite{"actual.tsv": actual, "trusted-source.tsv": baseActual} {
		data := marshalAuthorityDebtBaseline(authorityDebtBaseline{BootstrapSource: base, Collector: collector, Sites: sites})
		if err := os.WriteFile(filepath.Join(receipt, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("unforwarded full tuple receipts: test-results/%s", filepath.Base(receipt))
	if trusted.Collector == debtPortableRuntimeRoleCollectorTo && collector == debtPortableGlobalRoleCollectorTo {
		for _, pair := range debtPortableGlobalSignatureTransitions() {
			old, current := pair[0], pair[1]
			if actual[current.identity()] != current || actual[old.identity()].Multiplicity != 0 {
				t.Errorf("portable global actual full tuple mismatch: expected=%+v actual=%+v", current, actual[current.identity()])
			}
			predecessor := baseActual[old.identity()] == old && baseActual[current.identity()].Multiplicity == 0
			landed := baseActual[current.identity()] == current && baseActual[old.identity()].Multiplicity == 0
			if !predecessor && !landed {
				t.Errorf("portable global trusted full tuple mismatch: old=%+v current=%+v", baseActual[old.identity()], baseActual[current.identity()])
			}
		}
		if t.Failed() {
			t.FailNow()
		}
	}
	failures := authorityDebtRatchet(actual, head.Sites, comparisonBaseline, comparisonSource)
	if len(failures) > 0 {
		t.Fatalf("authority debt ratchet failed: base=%s old=%d head=%d actual=%d\n%s", base, authorityDebtCount(trusted.Sites), authorityDebtCount(head.Sites), authorityDebtCount(actual), strings.Join(failures, "\n"))
	}
	if refresh == "downward" {
		removed := authorityDebtCount(head.Sites) - authorityDebtCount(actual)
		normalized, _, _, _ := debtRunFixtureSignatureComparisons(actual, head.Sites, trusted.Sites, baseActual)
		normalized, _, _, _ = debtForkProofComparisons(actual, normalized, trusted.Sites, baseActual)
		normalized, _, _, _ = debtProcessLeaseRelocationComparisons(actual, normalized, trusted.Sites, baseActual)
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
func debtValidateCollectorIdentity(base, current string, trusted, head authorityDebtBaseline, reviewedTransition string) error {
	if head.BootstrapSource != trusted.BootstrapSource {
		return fmt.Errorf("debt baseline bootstrap source changed")
	}
	if base == current && trusted.Collector == current && head.Collector == current {
		return nil
	}
	if base == debtChannelCrashPreparationCollectorTo && trusted.Collector == base &&
		current == debtR130ProviderBindingCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2338/6840391584: exact facts-only signatures and unsupported refusals; baseline unchanged.
	}
	if base == debtW2DeliveryEvidenceCollectorTo && trusted.Collector == base &&
		current == debtChannelCrashPreparationCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #3015/6837884761: nine exact count-one signature successors; baseline sites unchanged.
	}
	if base == debtPortableAgentFixtureCollectorTo && trusted.Collector == base &&
		current == debtW2DeliveryEvidenceCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2970/6731069804: six exact oracle signatures; baseline sites stay unchanged.
	}
	if base == debtPortableGlobalRoleCollectorTo && trusted.Collector == base &&
		current == debtPortableAgentFixtureCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2409/6674839308: exact portable agentfixture role; baseline sites are unchanged.
	}
	if base == debtPortableRuntimeRoleCollectorTo && trusted.Collector == base &&
		current == debtPortableGlobalRoleCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2409/6671395779: exact portable global homes; baseline sites are unchanged.
	}
	if base == debtR83FixtureCollectorTo && trusted.Collector == base &&
		current == debtPortableRuntimeRoleCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2409/6651803711: exact relocated fixture roles; baseline sites are unchanged.
	}
	if base == debtS2OriginalContextCollectorTo && trusted.Collector == base &&
		current == debtR83FixtureCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2925: authorized S2/R83 composition; baseline sites are unchanged.
	}
	if base == debtCatalogRuntimeRowsLandedCollectorTo && trusted.Collector == base &&
		current == debtTestProcessCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2903/6584235489: exact Darwin classification; baseline sites are unchanged.
	}
	if base == debtTestProcessCollectorTo && trusted.Collector == base &&
		current == debtR77PrivateBridgeCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2881: exact collector continuation; no baseline or permission change.
	}
	if base == debtW4ComparisonCollectorTo && trusted.Collector == base &&
		current == debtCatalogRuntimeRowsLandedCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2413/6565922169: one exact landed signature, unchanged baseline sites.
	}
	if base == debtRetiredSourceFreezeCollectorTo && trusted.Collector == base &&
		current == debtR52LiveFixtureCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2845/6561801929: one exact mock-to-live fixture call; unchanged counted baseline.
	}
	if base == debtDeploymentHandlerLandedCollectorTo && trusted.Collector == base &&
		current == debtRetiredSourceFreezeCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #1803/6547180022: one exact retired argument, unchanged accounting sites and baseline.
	}
	if base == debtS3AccountCollectorTo && trusted.Collector == base &&
		current == debtDeploymentHandlerLandedCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // Exact landed deployment-handler correspondence; counted sites and baseline are unchanged.
	}
	if base == debtR77PrivateBridgeCollectorTo && trusted.Collector == base &&
		current == debtS2OriginalContextCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2794/6508200313: same selected uses and native original; unchanged debt sites.
	}
	if base == debtS1CollectorTo && trusted.Collector == base &&
		current == debtProviderSourceCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2727/6501014388: exact landed INSERT correspondence, unchanged counted sites.
	}
	if base == debtProviderSourceCollectorTo && trusted.Collector == base &&
		current == debtS3AccountCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2764/6498334917: exact stub correspondence after landed INSERT comparison, unchanged sites.
	}
	if base == debtW1RebasedComparisonCollectorTo && trusted.Collector == base &&
		current == debtSourceLoaderCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2727/6489643507: source loading and lazy formatting; exact same findings.
	}
	if base == debtCompletedSelectedFixturePinCollectorTo && trusted.Collector == base &&
		current == debtW3LandedFixtureCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2812/6453850248: exact W4 composition; unchanged sites and bootstrap.
	}
	if base == debtCompletedSelectedFixturePinCollectorTo && trusted.Collector == base &&
		current == debtW1ComparisonCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2728/6456523378: finite comparison repair, unchanged baseline.
	}
	if base == debtW3LandedFixtureCollectorTo && trusted.Collector == base &&
		current == debtW1RebasedComparisonCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // Compose the landed W4 matcher and reviewed W1 pairs; unchanged baseline.
	}
	if base == debtRetiredSourceFreezeCollectorTo && trusted.Collector == base &&
		current == debtW4ComparisonCollectorTo && head.Collector == current && reviewedTransition == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2807/6548989493: current-master continuation of the unlanded D edge; baseline sites are unchanged.
	}
	if base == debtSelectedFixtureComposedCollectorTo && trusted.Collector == base &&
		current == debtSelectedFixtureLandedCollectorTo && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #2727/6388897450: exact landed-inert repair; both source endpoints remain authenticated.
	}
	if (base == debtSelectedFixtureComposedCollectorTo || base == debtSelectedFixtureLandedCollectorTo) && trusted.Collector == base &&
		current == debtCompletedSelectedFixturePinCollectorTo && reviewedTransition == current && head.Collector == current &&
		len(authorityDebtRatchet(head.Sites, head.Sites, trusted.Sites, trusted.Sites)) == 0 {
		return nil // #2728/6392837709: delete the completed migration pin; no permission change.
	}
	if base == debtDeploymentRegistrationCollectorTo && trusted.Collector == base &&
		current == debtSelectedFixtureComposedCollectorTo && reviewedTransition == current && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // Exact five source-pinned continuations; source is checked before this transition.
	}
	if current == debtQ1ReopenCollectorTo && head.Collector == current && debtSitesEqual(head.Sites, trusted.Sites) {
		prior := head
		prior.Collector = debtExcludedFixtureCollectorTo
		if debtValidateCollectorIdentity(base, prior.Collector, trusted, prior, "") == nil {
			return nil // #642/6207897765: two exact reopen calls, no baseline edit.
		}
	}
	if current == debtExcludedFixtureCollectorTo && head.Collector == current && debtSitesEqual(head.Sites, trusted.Sites) {
		prior := head
		prior.Collector = debtNativeFixtureCollectorTo
		if debtValidateCollectorIdentity(base, prior.Collector, trusted, prior, "") == nil {
			return nil // Exact three-row classification; baseline sites are immutable.
		}
	}
	if base == debtProcessLeaseCollectorTo && trusted.Collector == base &&
		current == debtNativeFixtureCollectorTo && head.Collector == current && debtSitesEqual(head.Sites, trusted.Sites) {
		return nil // #642/6190983544 + 6191476293: exact fixture/custody policy, no baseline edit.
	}
	if base == debtDeploymentRegistrationCollectorFrom && trusted.Collector == base &&
		current == debtDeploymentRegistrationCollectorTo && head.Collector == current &&
		authorityDebtCount(head.Sites) == authorityDebtCount(trusted.Sites)+2 &&
		debtSitesEqual(head.Sites, debtWithDeploymentRegistrationStubs(trusted.Sites)) {
		return nil
	}
	if base == debtForkProofCollectorFrom && current == debtForkProofCollectorTo &&
		trusted.Collector == base && head.Collector == current &&
		debtForkProofCollectorTransition(trusted.Sites, head.Sites) {
		return nil
	}
	if base == debtForkProofCollectorTo && trusted.Collector == base && current == debtProcessLeaseCollectorTo && head.Collector == current &&
		len(authorityDebtRatchet(head.Sites, head.Sites, trusted.Sites, trusted.Sites)) == 0 {
		return nil
	}
	if base == debtProcessLeaseCollectorFrom && trusted.Collector == base && current == debtProcessLeaseCollectorTo && head.Collector == current &&
		debtSitesEqual(head.Sites, trusted.Sites) {
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
	if base == debtS1CollectorFrom && trusted.Collector == base &&
		current == debtS1CollectorTo && head.Collector == current &&
		authorityDebtCount(head.Sites) == authorityDebtCount(trusted.Sites)+2 &&
		debtSitesEqual(head.Sites, debtWithS1RetainedSourceConstructors(trusted.Sites)) {
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
	insideInfrastructure := strings.HasPrefix(path, "internal/testpostgres/") || filepath.ToSlash(filepath.Dir(path)) == "internal/testutil" ||
		strings.HasPrefix(path, "tests/support/testpostgres/") || filepath.ToSlash(filepath.Dir(path)) == "tests/support/testutil"
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
			if !selectedConsumer && !debtReviewedNativeSelectedImport(path, alias, file) {
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

// Exact original-owner threading correspondence, authorized in 6252008831.
const debtSelectedFixtureOldSHA256 = "b95339d282c82e0800e9e4771b0163b3b30dca0225e8ad27bb1bd65a34192164"
const debtSelectedFixtureCurrentSHA256 = "12c5a5cb5815a5fd2629178bce159bb5fdcdeadafc1f386d538c6d164781a7fe"

func debtSelectedFixtureSignatureTransitions() [5][2]authorityDebtSite {
	return [5][2]authorityDebtSite{
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestActivateSelectedContractRunForkFailsBeforePublishForPostTReplayScopeMarker", Operation: "call:github.com/division-sh/swarm/internal/runtime/runforkexecution.seedSelectedExecutionPostForkSourceEvent", Resolved: "func=func(t *testing.T, db *database/sql.DB, sourceRunID string, sourceEventID string, entityID string, at time.Time)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=time.Time|results=()", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestActivateSelectedContractRunForkFailsBeforePublishForPostTReplayScopeMarker", Operation: "call:github.com/division-sh/swarm/internal/runtime/runforkexecution.seedSelectedExecutionPostForkSourceEvent", Resolved: "func=func(t *testing.T, ctx context.Context, db *database/sql.DB, sourceRunID string, sourceEventID string, entityID string, at time.Time)|recv=<nil>|arg=*testing.T|arg=context.Context|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=time.Time|results=()", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestActivateSelectedContractRunForkRejectsDeferredWorkBeforeExecutableMutation", Operation: "call:github.com/division-sh/swarm/internal/runtime/runforkexecution.seedSelectedExecutionStateOnlySourceRun", Resolved: "func=func(t *testing.T, db *database/sql.DB, sourceRunID string, sourceEventID string, eventName string, at time.Time, sourceFacts ...github.com/division-sh/swarm/internal/runtime/correlation.SourceArtifactFact)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=time.Time|arg=github.com/division-sh/swarm/internal/runtime/correlation.SourceArtifactFact|results=()", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestActivateSelectedContractRunForkRejectsDeferredWorkBeforeExecutableMutation", Operation: "call:github.com/division-sh/swarm/internal/runtime/runforkexecution.seedSelectedExecutionStateOnlySourceRun", Resolved: "func=func(t *testing.T, selected *github.com/division-sh/swarm/internal/store.PostgresStore, db *database/sql.DB, sourceRunID string, sourceEventID string, eventName string, at time.Time, sourceFacts ...github.com/division-sh/swarm/internal/runtime/correlation.SourceArtifactFact)|recv=<nil>|arg=*testing.T|arg=*github.com/division-sh/swarm/internal/store.PostgresStore|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=time.Time|arg=github.com/division-sh/swarm/internal/runtime/correlation.SourceArtifactFact|results=()", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkRejectsDeferredWorkBeforeMutation", Operation: "call:github.com/division-sh/swarm/internal/runtime/runforkexecution.seedSelectedExecutionSourceRunWithPrimaryRoute", Resolved: "func=func(t *testing.T, db *database/sql.DB, sourceRunID string, entityID string, sourceEventID string, eventName string, at time.Time, entityType string, primaryRoute github.com/division-sh/swarm/internal/events.DeliveryRoute, extraRoutes []github.com/division-sh/swarm/internal/events.DeliveryRoute, sourceFacts ...github.com/division-sh/swarm/internal/runtime/correlation.SourceArtifactFact) github.com/division-sh/swarm/internal/events.Event|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=time.Time|arg=string|arg=github.com/division-sh/swarm/internal/events.DeliveryRoute|arg=untyped nil|results=(github.com/division-sh/swarm/internal/events.Event)", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkRejectsDeferredWorkBeforeMutation", Operation: "call:github.com/division-sh/swarm/internal/runtime/runforkexecution.seedSelectedExecutionSourceRunWithPrimaryRoute", Resolved: "func=func(t *testing.T, selected *github.com/division-sh/swarm/internal/store.PostgresStore, db *database/sql.DB, sourceRunID string, entityID string, sourceEventID string, eventName string, at time.Time, entityType string, primaryRoute github.com/division-sh/swarm/internal/events.DeliveryRoute, extraRoutes []github.com/division-sh/swarm/internal/events.DeliveryRoute, sourceFacts ...github.com/division-sh/swarm/internal/runtime/correlation.SourceArtifactFact) github.com/division-sh/swarm/internal/events.Event|recv=<nil>|arg=*testing.T|arg=*github.com/division-sh/swarm/internal/store.PostgresStore|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=time.Time|arg=string|arg=github.com/division-sh/swarm/internal/events.DeliveryRoute|arg=untyped nil|results=(github.com/division-sh/swarm/internal/events.Event)", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkTreatsDiagnosticPlatformOutcomeAsLineage", Operation: "call:github.com/division-sh/swarm/internal/runtime/runforkexecution.seedSelectedExecutionDiagnosticPlatformDeadLetter", Resolved: "func=func(t *testing.T, db *database/sql.DB, sourceRunID string, diagnosticEventID string, at time.Time)|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=time.Time|results=()", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "TestExecuteSelectedContractRunForkTreatsDiagnosticPlatformOutcomeAsLineage", Operation: "call:github.com/division-sh/swarm/internal/runtime/runforkexecution.seedSelectedExecutionDiagnosticPlatformDeadLetter", Resolved: "func=func(t *testing.T, ctx context.Context, db *database/sql.DB, sourceRunID string, diagnosticEventID string, at time.Time)|recv=<nil>|arg=*testing.T|arg=context.Context|arg=*database/sql.DB|arg=string|arg=string|arg=time.Time|results=()", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
		{
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "seedSelectedExecutionSourceRunWithPrimaryRoute", Operation: "call:github.com/division-sh/swarm/internal/runtime/runforkexecution.seedSelectedExecutionSourceRunWithPrimaryRouteModeAndSource", Resolved: "func=func(t *testing.T, db *database/sql.DB, sourceRunID string, entityID string, sourceEventID string, eventName string, at time.Time, entityType string, mode github.com/division-sh/swarm/internal/runtime/executionmode.Mode, primaryRoute github.com/division-sh/swarm/internal/events.DeliveryRoute, extraRoutes []github.com/division-sh/swarm/internal/events.DeliveryRoute, routingSource github.com/division-sh/swarm/internal/events.RoutingSource, envelope github.com/division-sh/swarm/internal/events.EventEnvelope, input github.com/division-sh/swarm/internal/runtime/runforkexecution.selectedExecutionInputFixture, sourceFacts ...github.com/division-sh/swarm/internal/runtime/correlation.SourceArtifactFact) github.com/division-sh/swarm/internal/events.Event|recv=<nil>|arg=*testing.T|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=time.Time|arg=string|arg=github.com/division-sh/swarm/internal/runtime/executionmode.Mode|arg=github.com/division-sh/swarm/internal/events.DeliveryRoute|arg=[]github.com/division-sh/swarm/internal/events.DeliveryRoute|arg=github.com/division-sh/swarm/internal/events.RoutingSource|arg=github.com/division-sh/swarm/internal/events.EventEnvelope|arg=github.com/division-sh/swarm/internal/runtime/runforkexecution.selectedExecutionInputFixture|arg=[]github.com/division-sh/swarm/internal/runtime/correlation.SourceArtifactFact|results=(github.com/division-sh/swarm/internal/events.Event)", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/runtime/runforkexecution/execution_test.go", Declaration: "seedSelectedExecutionSourceRunWithPrimaryRoute", Operation: "call:github.com/division-sh/swarm/internal/runtime/runforkexecution.seedSelectedExecutionSourceRunWithPrimaryRouteModeAndSource", Resolved: "func=func(t *testing.T, selected *github.com/division-sh/swarm/internal/store.PostgresStore, db *database/sql.DB, sourceRunID string, entityID string, sourceEventID string, eventName string, at time.Time, entityType string, mode github.com/division-sh/swarm/internal/runtime/executionmode.Mode, primaryRoute github.com/division-sh/swarm/internal/events.DeliveryRoute, extraRoutes []github.com/division-sh/swarm/internal/events.DeliveryRoute, routingSource github.com/division-sh/swarm/internal/events.RoutingSource, envelope github.com/division-sh/swarm/internal/events.EventEnvelope, input github.com/division-sh/swarm/internal/runtime/runforkexecution.selectedExecutionInputFixture, sourceFacts ...github.com/division-sh/swarm/internal/runtime/correlation.SourceArtifactFact) github.com/division-sh/swarm/internal/events.Event|recv=<nil>|arg=*testing.T|arg=*github.com/division-sh/swarm/internal/store.PostgresStore|arg=*database/sql.DB|arg=string|arg=string|arg=string|arg=string|arg=time.Time|arg=string|arg=github.com/division-sh/swarm/internal/runtime/executionmode.Mode|arg=github.com/division-sh/swarm/internal/events.DeliveryRoute|arg=[]github.com/division-sh/swarm/internal/events.DeliveryRoute|arg=github.com/division-sh/swarm/internal/events.RoutingSource|arg=github.com/division-sh/swarm/internal/events.EventEnvelope|arg=github.com/division-sh/swarm/internal/runtime/runforkexecution.selectedExecutionInputFixture|arg=[]github.com/division-sh/swarm/internal/runtime/correlation.SourceArtifactFact|results=(github.com/division-sh/swarm/internal/events.Event)", Family: "fork", Replacement: "selected fork/source/event/lifecycle operation and bounded fork observation", Multiplicity: 1},
		},
	}
}

func debtSelectedFixtureTransferNeeded(actual, base, source map[string]authorityDebtSite) bool {
	for _, pair := range debtSelectedFixtureSignatureTransitions() {
		old, current := pair[0], pair[1]
		if actual[current.identity()] == current && actual[old.identity()].Multiplicity == 0 &&
			base[old.identity()] == old && source[old.identity()] == old {
			return true
		}
	}
	return false
}

func debtSelectedFixtureSourcePins(root, baseRoot string) error {
	const path = "internal/runtime/runforkexecution/execution_test.go"
	pins := []struct{ source, checksum, checkout string }{
		{debtSelectedFixtureOldSource, debtSelectedFixtureOldSHA256, baseRoot},
		{debtSelectedFixtureCurrentSource, debtSelectedFixtureCurrentSHA256, root},
	}
	before, err := os.ReadFile(filepath.Join(baseRoot, path))
	if err != nil {
		return err
	}
	if debtBaselineChecksum(before) == debtSelectedFixtureComposedOldSHA256 {
		pins[0].source, pins[0].checksum = debtSelectedFixtureComposedOldSource, debtSelectedFixtureComposedOldSHA256
		pins[1].source, pins[1].checksum = debtSelectedFixtureComposedCurrentSource, debtSelectedFixtureComposedCurrentSHA256
	} else if debtBaselineChecksum(before) == debtSelectedFixtureComposedCurrentSHA256 {
		pins[0].source, pins[0].checksum = debtSelectedFixtureComposedCurrentSource, debtSelectedFixtureComposedCurrentSHA256
		pins[1].source, pins[1].checksum = debtSelectedFixtureComposedCurrentSource, debtSelectedFixtureComposedCurrentSHA256
	}
	for _, pin := range pins {
		archived, err := debtGit(root, "show", pin.source+":"+path)
		if err != nil {
			return err
		}
		current, err := os.ReadFile(filepath.Join(pin.checkout, path))
		if err != nil {
			return err
		}
		if debtBaselineChecksum(archived) != pin.checksum || !bytes.Equal(current, archived) {
			return fmt.Errorf("selected fixture signature continuation differs from its exact source %s", pin.source)
		}
	}
	return nil
}

// #2810/6436165120: exact Live-fixture correspondences, not a matcher family.
func debtW4LiveFixtureComparisons(actual, head, base, source map[string]authorityDebtSite) (map[string]authorityDebtSite, map[string]authorityDebtSite, map[string]authorityDebtSite) {
	pairs := debtW4LiveFixtureTransfers()
	composition := debtW4LiveFixtureComposition()
	mockReturn := composition[1]
	mockReturn.Declaration = "startServedTestSetupEntitiesProofRuntimeFromSource"
	compositionAllowed := actual[mockReturn.identity()] == mockReturn && source[mockReturn.identity()] == mockReturn
	sourceCompositionAbsent, sourceCompositionCurrent := true, true
	for _, site := range composition {
		compositionAllowed = compositionAllowed && actual[site.identity()] == site &&
			base[site.identity()].Multiplicity == 0 &&
			(head[site.identity()].Multiplicity == 0 || head[site.identity()] == site)
		sourceCompositionAbsent = sourceCompositionAbsent && source[site.identity()].Multiplicity == 0
		sourceCompositionCurrent = sourceCompositionCurrent && source[site.identity()] == site
	}
	compositionAllowed = compositionAllowed && (sourceCompositionAbsent || sourceCompositionCurrent)
	for _, pair := range pairs {
		old, current := pair[0], pair[1]
		beforeTransfer := source[old.identity()] == old && source[current.identity()].Multiplicity == 0
		landedTransfer := source[current.identity()] == current && source[old.identity()].Multiplicity == 0
		if actual[current.identity()] == current && actual[old.identity()].Multiplicity == 0 &&
			base[old.identity()] == old && base[current.identity()].Multiplicity == 0 &&
			(beforeTransfer || landedTransfer) {
			head = debtForwardQuiescenceUncertainty(head, old, current)
			base = debtForwardQuiescenceUncertainty(base, old, current)
			source = debtForwardQuiescenceUncertainty(source, old, current)
		}
	}
	if compositionAllowed {
		for _, site := range composition {
			// These two authorized compositions consume no predecessor or deletion credit.
			for _, state := range []*map[string]authorityDebtSite{&head, &base, &source} {
				copy := make(map[string]authorityDebtSite, len(*state)+1)
				for key, entry := range *state {
					copy[key] = entry
				}
				copy[site.identity()] = site
				*state = copy
			}
		}
	}
	return head, base, source
}

func debtW4LiveFixtureTransfers() [21][2]authorityDebtSite {
	return [21][2]authorityDebtSite{
		{{Kind: "raw-operation", File: "internal/serveapp/fork_absent_source_test.go", Declaration: "TestServedForkRootSourceIngressBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/fork_absent_source_test.go", Declaration: "TestServedForkRootSourceIngressBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/fork_delivery_route_evidence_test.go", Declaration: "TestServedForkConnectedDeliveryRouteEvidenceOnBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/fork_delivery_route_evidence_test.go", Declaration: "TestServedForkConnectedDeliveryRouteEvidenceOnBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/fork_historical_context_test.go", Declaration: "TestRunForkHistoricalIdentityPublicExecutionBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/fork_historical_context_test.go", Declaration: "TestRunForkHistoricalIdentityPublicExecutionBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/fork_loop_accumulator_test.go", Declaration: "TestServedForkAccumulatorRetainedGenerationBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/fork_loop_accumulator_test.go", Declaration: "TestServedForkAccumulatorRetainedGenerationBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/fork_loop_generation_notice_test.go", Declaration: "TestServedForkLoopGenerationStateEffectBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/fork_loop_generation_notice_test.go", Declaration: "TestServedForkLoopGenerationStateEffectBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/fork_receiver_acquisition_effect_only_test.go", Declaration: "TestSelectedForkSupplementalReceiverAcquisitionWithoutPostRevisionEmissionBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/fork_receiver_acquisition_effect_only_test.go", Declaration: "TestSelectedForkSupplementalReceiverAcquisitionWithoutPostRevisionEmissionBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/fork_receiver_ownership_test.go", Declaration: "TestSelectedForkIndependentReceiverBusinessMutationBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/fork_receiver_ownership_test.go", Declaration: "TestSelectedForkIndependentReceiverBusinessMutationBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/fork_receiver_ownership_test.go", Declaration: "TestSelectedForkReceiverAcquisitionCapabilityRefusalBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/fork_receiver_ownership_test.go", Declaration: "TestSelectedForkReceiverAcquisitionCapabilityRefusalBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/fork_receiver_ownership_test.go", Declaration: "TestServedForkSourceCompletionWithoutForkBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/fork_receiver_ownership_test.go", Declaration: "TestServedForkSourceCompletionWithoutForkBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/fork_receiver_ownership_test.go", Declaration: "runForkReceiverOwnershipJourney", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/fork_receiver_ownership_test.go", Declaration: "runForkReceiverOwnershipJourney", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/fork_recipient_authority_test.go", Declaration: "TestServedForkConnectedRecipientAuthorityOnBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/fork_recipient_authority_test.go", Declaration: "TestServedForkConnectedRecipientAuthorityOnBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/lifecycle_transition_fork_test.go", Declaration: "runLifecycleRootForkPolicy", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/lifecycle_transition_fork_test.go", Declaration: "runLifecycleRootForkPolicy", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/lifecycle_transition_served_test.go", Declaration: "TestServedCompiledTransitionSelectedCarrierEvidenceOnBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/lifecycle_transition_served_test.go", Declaration: "TestServedCompiledTransitionSelectedCarrierEvidenceOnBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/lifecycle_transition_static_fork_test.go", Declaration: "TestServedCompiledTransitionStaticForkEvidenceOnBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/lifecycle_transition_static_fork_test.go", Declaration: "TestServedCompiledTransitionStaticForkEvidenceOnBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "startServedTestSetupEntitiesProofRuntimeFromSource", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeConfigured", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, realWorkspace bool, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|arg=bool|arg=[]github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "startServedTestSetupEntitiesProofRuntimeFromSource", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeConfigured", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, realWorkspace bool, posture github.com/division-sh/swarm/internal/runtime/executionposture.Posture, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|arg=bool|arg=github.com/division-sh/swarm/internal/runtime/executionposture.Posture|arg=[]github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/paused_publication_test.go", Declaration: "TestPausedAcceptedTimerAndDecisionPublicBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/paused_publication_test.go", Declaration: "TestPausedAcceptedTimerAndDecisionPublicBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/receiver_fork_test.go", Declaration: "TestReceiverCompositionForkBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|arg=func(ctx context.Context, _ string, evt github.com/division-sh/swarm/internal/events.Event) error|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/receiver_fork_test.go", Declaration: "TestReceiverCompositionForkBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeConfigured", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, realWorkspace bool, posture github.com/division-sh/swarm/internal/runtime/executionposture.Posture, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|arg=bool|arg=github.com/division-sh/swarm/internal/runtime/executionposture.Posture|arg=func(ctx context.Context, _ string, evt github.com/division-sh/swarm/internal/events.Event) error|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/selected_fork_mailbox_controls_test.go", Declaration: "TestSelectedForkMailboxControlRefusalsBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "mailbox", Replacement: "selected mailbox/completion owner and named mailbox fault/readback", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/selected_fork_mailbox_controls_test.go", Declaration: "TestSelectedForkMailboxControlRefusalsBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "mailbox", Replacement: "selected mailbox/completion owner and named mailbox fault/readback", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/selected_fork_pending_input_test.go", Declaration: "TestExplicitTargetRestrictsConnectedSiblingsBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/selected_fork_pending_input_test.go", Declaration: "TestExplicitTargetRestrictsConnectedSiblingsBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{{Kind: "raw-operation", File: "internal/serveapp/selected_fork_pending_input_test.go", Declaration: "TestSelectedForkPendingInputBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}, {Kind: "raw-operation", File: "internal/serveapp/selected_fork_pending_input_test.go", Declaration: "TestSelectedForkPendingInputBothStores", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1}},
		{
			{Kind: "raw-operation", File: "internal/serveapp/semantic_numeric_ingress_test.go", Declaration: "TestServedSemanticNumericEventSubscription", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
			{Kind: "raw-operation", File: "internal/serveapp/semantic_numeric_ingress_test.go", Declaration: "TestServedSemanticNumericEventSubscription", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedLiveFlowProofRuntimeFromSource", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		},
	}
}

func debtW4LiveFixtureComposition() [2]authorityDebtSite {
	return [2]authorityDebtSite{
		{Kind: "raw-operation", File: "internal/serveapp/main_runtime_test.go", Declaration: "startServedLiveFlowProofRuntimeFromSource", Operation: "call:github.com/division-sh/swarm/internal/serveapp.startServedTestSetupEntitiesProofRuntimeConfigured", Resolved: "func=func(t *testing.T, backend github.com/division-sh/swarm/internal/servedparity.Backend, sourceRoot string, realWorkspace bool, posture github.com/division-sh/swarm/internal/runtime/executionposture.Posture, hooks ...github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook) github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime|recv=<nil>|arg=*testing.T|arg=github.com/division-sh/swarm/internal/servedparity.Backend|arg=string|arg=bool|arg=github.com/division-sh/swarm/internal/runtime/executionposture.Posture|arg=[]github.com/division-sh/swarm/internal/runtime/pipeline.WorkflowNodeHandlerStartHook|results=(github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime)", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
		{Kind: "raw-type", File: "internal/serveapp/main_runtime_test.go", Declaration: "startServedLiveFlowProofRuntimeFromSource", Operation: "result:", Resolved: "github.com/division-sh/swarm/internal/serveapp.servedControlProofRuntime", Family: "lifecycle", Replacement: "selected run lifecycle/candidate/source owner and bounded lifecycle observation", Multiplicity: 1},
	}
}
