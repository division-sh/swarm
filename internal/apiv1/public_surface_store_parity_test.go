package apiv1

import (
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
	"gopkg.in/yaml.v3"
)

type publicSurfaceStoreParity struct {
	Issue                 int                                     `yaml:"issue"`
	ClosureLevel          string                                  `yaml:"closure_level"`
	ClaimsParentClosure   bool                                    `yaml:"claims_parent_closure"`
	ProofPolicyPath       string                                  `yaml:"proof_policy_path"`
	SelectedPurposes      []publicSurfaceSelectedPurposeClaim     `yaml:"selected_purposes"`
	DifferentConstruction publicSurfaceDifferentConstructionRoles `yaml:"different_construction"`
	Claims                []publicSurfaceStoreParityClaim         `yaml:"claims"`
}

type publicSurfaceSelectedPurposeClaim struct {
	ID             string                  `yaml:"id"`
	Constructor    string                  `yaml:"constructor"`
	Consumers      []string                `yaml:"consumers"`
	SemanticOwner  string                  `yaml:"semantic_owner"`
	ClaimIDs       []string                `yaml:"claim_ids"`
	ProofProfile   string                  `yaml:"proof_profile"`
	ProofRefs      []publicSurfaceProofRef `yaml:"proof_refs"`
	CloseProofRefs []publicSurfaceProofRef `yaml:"close_proof_refs"`
}

type publicSurfaceDifferentConstructionRoles struct {
	RuntimeDeps             []string `yaml:"runtime_deps"`
	ManagerPersistenceRoles []string `yaml:"manager_persistence_roles"`
}

type publicSurfaceStoreParityClaim struct {
	ID                       string                  `yaml:"id"`
	SemanticOwners           []string                `yaml:"semantic_owners"`
	BackendDispositions      map[string]string       `yaml:"backend_dispositions"`
	SplitIssue               int                     `yaml:"split_issue,omitempty"`
	SpecRefs                 []string                `yaml:"spec_refs"`
	ProofProfile             string                  `yaml:"proof_profile"`
	ProofRefs                []publicSurfaceProofRef `yaml:"proof_refs"`
	TeachingProofRefs        []publicSurfaceProofRef `yaml:"teaching_proof_refs,omitempty"`
	RiskDimensions           []string                `yaml:"risk_dimensions"`
	RiskTrackers             []publicSurfaceProofRef `yaml:"risk_trackers,omitempty"`
	RequiredPorts            []string                `yaml:"required_ports,omitempty"`
	RuntimeDeps              []string                `yaml:"runtime_deps,omitempty"`
	EventBusDurableRoles     []string                `yaml:"event_bus_durable_roles,omitempty"`
	ManagerSelectedRoles     []string                `yaml:"manager_selected_roles,omitempty"`
	WorkflowPersistenceRoles []string                `yaml:"workflow_persistence_roles,omitempty"`
	OptionalProducts         []string                `yaml:"optional_products,omitempty"`
	OptionalSubroles         []string                `yaml:"optional_subroles,omitempty"`
	PlatformTables           []string                `yaml:"platform_tables,omitempty"`
	PublicMethods            []string                `yaml:"public_methods,omitempty"`
}

type publicSurfaceStoreParitySourceSets struct {
	constructors             map[string]struct{}
	consumers                map[string]struct{}
	requiredPorts            map[string]struct{}
	optionalProducts         map[string]struct{}
	runtimeDeps              map[string]struct{}
	eventBusDurableRoles     map[string]struct{}
	managerPersistenceRoles  map[string]struct{}
	workflowPersistenceRoles map[string]struct{}
	optionalSubroles         map[string]struct{}
	platformTables           map[string]struct{}
}

type publicSurfaceStoreParityValidationContext struct {
	sources publicSurfaceStoreParitySourceSets
	policy  testplanning.Policy
}

const selectedPackageImportPath = "github.com/division-sh/swarm/internal/store/selected"

func TestPublicSurfaceStoreParityRejectsCoverageDrift(t *testing.T) {
	root := repoRoot(t)
	ctx := newPublicSurfaceValidationContext(t, root)
	tests := []struct {
		name   string
		mutate func(*publicSurfaceBackendMatrix)
		want   string
	}{
		{
			name: "selected purpose constructor",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				matrix.StoreParity.SelectedPurposes = matrix.StoreParity.SelectedPurposes[1:]
			},
			want: "selected production constructor census missing OpenRuntime",
		},
		{
			name: "selected purpose consumer",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				storeParityPurposeByID(t, matrix, "authority_inspection").Consumers = []string{"internal/cliapp/missing.go"}
			},
			want: "selected production consumer census missing authority_inspection@internal/cliapp/store_authority.go",
		},
		{
			name: "required port",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				storeParityClaimByID(t, matrix, "store_infrastructure").RequiredPorts = []string{"schema", "workspace"}
			},
			want: "required port census missing pinger",
		},
		{
			name: "selected runtime dependency",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "event_delivery")
				claim.RuntimeDeps = storeParityStringsExcept(claim.RuntimeDeps, "EventStore")
			},
			want: "RuntimeDeps census missing EventStore",
		},
		{
			name: "nested event bus role",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "event_delivery")
				claim.EventBusDurableRoles = storeParityStringsExcept(claim.EventBusDurableRoles, "PreparedEvents")
			},
			want: "EventBusDurable role census missing PreparedEvents",
		},
		{
			name: "nested manager role",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "agent_lifecycle")
				claim.ManagerSelectedRoles = storeParityStringsExcept(claim.ManagerSelectedRoles, "LifecycleCensus")
			},
			want: "ManagerPersistenceRoles census missing LifecycleCensus",
		},
		{
			name: "nested workflow role",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "workflow_runtime")
				claim.WorkflowPersistenceRoles = storeParityStringsExcept(claim.WorkflowPersistenceRoles, "WorkflowEngineMutationOwner")
			},
			want: "WorkflowPersistence role census missing WorkflowEngineMutationOwner",
		},
		{
			name: "optional product",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "bundle_catalog")
				claim.OptionalProducts = storeParityStringsExcept(claim.OptionalProducts, "bundleRegister")
			},
			want: "optional product census missing bundleRegister",
		},
		{
			name: "optional product subrole",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "bundle_delete")
				claim.OptionalSubroles = storeParityStringsExcept(claim.OptionalSubroles, "BundleDelete.locks")
			},
			want: "optional product subrole census missing BundleDelete.locks",
		},
		{
			name: "platform table",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "run_fork_revision")
				claim.PlatformTables = storeParityStringsExcept(claim.PlatformTables, "run_fork_revisions")
			},
			want: "platform table census missing run_fork_revisions",
		},
		{
			name: "duplicate platform table",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "run_fork_revision")
				claim.PlatformTables = append(claim.PlatformTables, "run_fork_revisions")
			},
			want: "platform table census contains duplicate run_fork_revisions",
		},
		{
			name: "public method",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "operator_channels")
				claim.PublicMethods = storeParityStringsExcept(claim.PublicMethods, "channel.list")
			},
			want: "public method census missing channel.list",
		},
		{
			name: "backend disposition",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				delete(storeParityClaimByID(t, matrix, "event_delivery").BackendDispositions, "default_sqlite")
			},
			want: "store parity claim event_delivery missing default_sqlite backend disposition",
		},
		{
			name: "public method disposition link",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "non_store_transport")
				claim.BackendDispositions["default_sqlite"] = "supported_with_dual_backend_proof"
			},
			want: "store parity claim non_store_transport method agent.frame different-concept ledger classification conflicts with claim dispositions",
		},
		{
			name: "stale split",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				storeParityClaimByID(t, matrix, "selected_contract_fork").SplitIssue = 999999
			},
			want: "store parity claim selected_contract_fork split issue #999999 is not active",
		},
		{
			name: "teaching proof",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				storeParityClaimByID(t, matrix, "bundle_delete").TeachingProofRefs = nil
			},
			want: "store parity claim bundle_delete unsupported/split disposition missing teaching_proof_refs",
		},
		{
			name: "executable selector",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				storeParityClaimByID(t, matrix, "event_delivery").ProofRefs[0].Name = "TestMissingStoreParityProof"
			},
			want: "store parity claim event_delivery go_test proof_ref TestMissingStoreParityProof does not resolve",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			matrix := loadPublicSurfaceBackendMatrix(t, root)
			tc.mutate(&matrix)
			problems := validatePublicSurfaceBackendMatrix(root, matrix, ctx)
			if !publicSurfaceProblemsContain(problems, tc.want) {
				t.Fatalf("validation problems missing %q:\n- %s", tc.want, strings.Join(problems, "\n- "))
			}
		})
	}
}

func TestPublicSurfaceStoreParityProofSchedulingRejectsOmittedSelector(t *testing.T) {
	policy := testplanning.Policy{
		Module:          "github.com/division-sh/swarm",
		SpecialPackages: []string{"github.com/division-sh/swarm/internal/apiv1"},
		Profiles: map[string]testplanning.ProfilePolicy{
			"pr-common": {Units: []string{"apiv1-selected"}},
		},
		Units: map[string]testplanning.UnitPolicy{
			"apiv1-selected": {
				Packages: []string{"github.com/division-sh/swarm/internal/apiv1"},
				Run:      "^TestSomeOtherProof$",
			},
		},
	}
	if publicSurfaceStoreParityProofScheduled(policy, "pr-common", "TestParityProof", "internal/apiv1/parity_test.go") {
		t.Fatal("selector omitted from its named profile was accepted")
	}
}

func storeParityClaimByID(t *testing.T, matrix *publicSurfaceBackendMatrix, id string) *publicSurfaceStoreParityClaim {
	t.Helper()
	for index := range matrix.StoreParity.Claims {
		if matrix.StoreParity.Claims[index].ID == id {
			return &matrix.StoreParity.Claims[index]
		}
	}
	t.Fatalf("store parity claim %s not found", id)
	return nil
}

func storeParityPurposeByID(t *testing.T, matrix *publicSurfaceBackendMatrix, id string) *publicSurfaceSelectedPurposeClaim {
	t.Helper()
	for index := range matrix.StoreParity.SelectedPurposes {
		if matrix.StoreParity.SelectedPurposes[index].ID == id {
			return &matrix.StoreParity.SelectedPurposes[index]
		}
	}
	t.Fatalf("store parity purpose %s not found", id)
	return nil
}

func storeParityStringsExcept(values []string, excluded string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != excluded {
			out = append(out, value)
		}
	}
	return out
}

func validatePublicSurfaceStoreParity(root string, layer publicSurfaceStoreParity, mutating []publicSurfaceMutatingAPIParityEntry, reads []publicSurfaceOperatorReadAPIParityEntry, ctx publicSurfaceValidationContext, activeTrackers map[string]struct{}) []string {
	var problems []string
	if layer.Issue != 2275 {
		problems = append(problems, fmt.Sprintf("store_parity issue = %d, want 2275", layer.Issue))
	}
	if layer.ClosureLevel != "store_prevention_first_slice_eliminated" {
		problems = append(problems, fmt.Sprintf("store_parity closure_level = %q, want store_prevention_first_slice_eliminated", layer.ClosureLevel))
	}
	if layer.ClaimsParentClosure {
		problems = append(problems, "store_parity claims_parent_closure = true, want false")
	}
	if layer.ProofPolicyPath != ".github/test-proof-plan.yaml" {
		problems = append(problems, fmt.Sprintf("store_parity proof_policy_path = %q, want .github/test-proof-plan.yaml", layer.ProofPolicyPath))
	}

	sources := ctx.storeParity.sources
	policy := ctx.storeParity.policy

	claimsByID := map[string]publicSurfaceStoreParityClaim{}
	for _, claim := range layer.Claims {
		id := strings.TrimSpace(claim.ID)
		if id == "" {
			problems = append(problems, "store parity claim missing id")
			continue
		}
		if _, ok := claimsByID[id]; ok {
			problems = append(problems, fmt.Sprintf("store parity claim %s appears more than once", id))
		}
		claimsByID[id] = claim
		problems = append(problems, validatePublicSurfaceStoreParityClaim(root, claim, ctx, policy, activeTrackers)...)
	}
	if len(claimsByID) == 0 {
		problems = append(problems, "store parity claims are required")
	}

	problems = append(problems, validatePublicSurfaceSelectedPurposes(layer.SelectedPurposes, claimsByID, sources, ctx, policy)...)
	problems = append(problems, validatePublicSurfaceStoreParityValues("required port", collectStoreParityClaimValues(layer.Claims, func(c publicSurfaceStoreParityClaim) []string { return c.RequiredPorts }), sources.requiredPorts)...)
	problems = append(problems, validatePublicSurfaceStoreParityValues("optional product", collectStoreParityClaimValues(layer.Claims, func(c publicSurfaceStoreParityClaim) []string { return c.OptionalProducts }), sources.optionalProducts)...)
	problems = append(problems, validatePublicSurfaceStoreParityPartition("RuntimeDeps", collectStoreParityClaimValues(layer.Claims, func(c publicSurfaceStoreParityClaim) []string { return c.RuntimeDeps }), layer.DifferentConstruction.RuntimeDeps, sources.runtimeDeps)...)
	problems = append(problems, validatePublicSurfaceStoreParityValues("EventBusDurable role", collectStoreParityClaimValues(layer.Claims, func(c publicSurfaceStoreParityClaim) []string { return c.EventBusDurableRoles }), sources.eventBusDurableRoles)...)
	problems = append(problems, validatePublicSurfaceStoreParityPartition("ManagerPersistenceRoles", collectStoreParityClaimValues(layer.Claims, func(c publicSurfaceStoreParityClaim) []string { return c.ManagerSelectedRoles }), layer.DifferentConstruction.ManagerPersistenceRoles, sources.managerPersistenceRoles)...)
	problems = append(problems, validatePublicSurfaceStoreParityValues("WorkflowPersistence role", collectStoreParityClaimValues(layer.Claims, func(c publicSurfaceStoreParityClaim) []string { return c.WorkflowPersistenceRoles }), sources.workflowPersistenceRoles)...)
	problems = append(problems, validatePublicSurfaceStoreParityValues("optional product subrole", collectStoreParityClaimValues(layer.Claims, func(c publicSurfaceStoreParityClaim) []string { return c.OptionalSubroles }), sources.optionalSubroles)...)
	problems = append(problems, validatePublicSurfaceStoreParityValues("platform table", collectStoreParityClaimValues(layer.Claims, func(c publicSurfaceStoreParityClaim) []string { return c.PlatformTables }), sources.platformTables)...)
	problems = append(problems, validatePublicSurfaceStoreParityValues("public method", collectStoreParityClaimValues(layer.Claims, func(c publicSurfaceStoreParityClaim) []string { return c.PublicMethods }), ctx.apiMethods)...)
	problems = append(problems, validatePublicSurfaceStoreParityMethodLinks(layer.Claims, mutating, reads)...)
	sort.Strings(problems)
	return problems
}

func newPublicSurfaceStoreParityValidationContext(root string) (publicSurfaceStoreParityValidationContext, error) {
	sources, err := loadPublicSurfaceStoreParitySourceSets(root)
	if err != nil {
		return publicSurfaceStoreParityValidationContext{}, err
	}
	policy, err := loadPublicSurfaceStoreParityProofPolicy(root, ".github/test-proof-plan.yaml")
	if err != nil {
		return publicSurfaceStoreParityValidationContext{}, err
	}
	return publicSurfaceStoreParityValidationContext{sources: sources, policy: policy}, nil
}

func validatePublicSurfaceStoreParityMethodLinks(claims []publicSurfaceStoreParityClaim, mutating []publicSurfaceMutatingAPIParityEntry, reads []publicSurfaceOperatorReadAPIParityEntry) []string {
	type methodDisposition struct {
		classification string
		specRef        string
	}
	methods := map[string]methodDisposition{}
	for _, entry := range mutating {
		methods[entry.Method] = methodDisposition{classification: entry.Classification, specRef: entry.SpecRef}
	}
	for _, entry := range reads {
		methods[entry.Method] = methodDisposition{classification: entry.Classification, specRef: entry.SpecRef}
	}
	var problems []string
	for _, claim := range claims {
		for _, method := range claim.PublicMethods {
			entry, ok := methods[method]
			if !ok {
				problems = append(problems, fmt.Sprintf("store parity claim %s method %s is not connected to the public backend ledger", claim.ID, method))
				continue
			}
			sqlite := claim.BackendDispositions["default_sqlite"]
			postgres := claim.BackendDispositions["explicit_postgres"]
			switch entry.classification {
			case "dual_backend_served_proof", "covered_transitively", "dual_backend_api_proof":
				if sqlite != "supported_with_dual_backend_proof" || postgres != "supported_with_dual_backend_proof" {
					problems = append(problems, fmt.Sprintf("store parity claim %s method %s dual-backend ledger classification conflicts with claim dispositions", claim.ID, method))
				}
			case "postgres_only_with_spec_ref":
				if (sqlite != "unsupported_with_exact_spec_and_teaching_proof" && sqlite != "split_with_active_issue_and_fail_closed_proof") || postgres != "supported_with_dual_backend_proof" {
					problems = append(problems, fmt.Sprintf("store parity claim %s method %s postgres-only ledger classification conflicts with claim dispositions", claim.ID, method))
				}
				if !publicSurfaceHasValue(claim.SpecRefs, entry.specRef) {
					problems = append(problems, fmt.Sprintf("store parity claim %s method %s missing exact ledger spec_ref %s", claim.ID, method, entry.specRef))
				}
			case "different_semantic_concept_with_proof":
				if sqlite != "not_applicable_different_semantic_concept_with_proof" || postgres != "not_applicable_different_semantic_concept_with_proof" {
					problems = append(problems, fmt.Sprintf("store parity claim %s method %s different-concept ledger classification conflicts with claim dispositions", claim.ID, method))
				}
			}
		}
	}
	return problems
}

func validatePublicSurfaceStoreParityClaim(root string, claim publicSurfaceStoreParityClaim, ctx publicSurfaceValidationContext, policy testplanning.Policy, activeTrackers map[string]struct{}) []string {
	var problems []string
	label := "store parity claim " + strings.TrimSpace(claim.ID)
	if len(claim.SemanticOwners) == 0 {
		problems = append(problems, label+" missing semantic_owners")
	}
	for _, backend := range []string{"default_sqlite", "explicit_postgres"} {
		disposition, ok := claim.BackendDispositions[backend]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s missing %s backend disposition", label, backend))
			continue
		}
		if _, ok := allowedPublicSurfaceStoreParityDispositions()[disposition]; !ok {
			problems = append(problems, fmt.Sprintf("%s %s disposition %q is not allowed", label, backend, disposition))
		}
	}
	if len(claim.BackendDispositions) != 2 {
		problems = append(problems, fmt.Sprintf("%s backend dispositions must contain exactly default_sqlite and explicit_postgres", label))
	}
	if len(claim.SpecRefs) == 0 {
		problems = append(problems, label+" missing spec_refs")
	}
	for _, specRef := range claim.SpecRefs {
		if err := publicSurfaceSpecRefExists(root, specRef); err != nil {
			problems = append(problems, fmt.Sprintf("%s spec_ref %s does not resolve: %v", label, specRef, err))
		}
	}
	if len(claim.RiskDimensions) == 0 {
		problems = append(problems, label+" missing risk_dimensions")
	}
	for _, dimension := range claim.RiskDimensions {
		if _, ok := allowedPublicSurfaceStoreParityRisks()[dimension]; !ok {
			problems = append(problems, fmt.Sprintf("%s risk_dimension %q is not allowed", label, dimension))
		}
	}
	problems = append(problems, validatePublicSurfaceStoreParityProofs(label, claim.ProofProfile, claim.ProofRefs, ctx, policy)...)

	needsTeaching := false
	needsSplit := false
	for _, disposition := range claim.BackendDispositions {
		needsTeaching = needsTeaching || disposition == "unsupported_with_exact_spec_and_teaching_proof" || disposition == "split_with_active_issue_and_fail_closed_proof"
		needsSplit = needsSplit || disposition == "split_with_active_issue_and_fail_closed_proof"
	}
	if needsTeaching {
		if len(claim.TeachingProofRefs) == 0 {
			problems = append(problems, label+" unsupported/split disposition missing teaching_proof_refs")
		} else {
			problems = append(problems, validatePublicSurfaceStoreParityProofs(label+" teaching", claim.ProofProfile, claim.TeachingProofRefs, ctx, policy)...)
		}
	}
	if needsSplit {
		if claim.SplitIssue == 0 {
			problems = append(problems, label+" split disposition missing split_issue")
		} else if !publicSurfaceStoreParityHasTrackerIssue(claim.RiskTrackers, claim.SplitIssue, activeTrackers) {
			problems = append(problems, fmt.Sprintf("%s split issue #%d is not active", label, claim.SplitIssue))
		}
	} else if claim.SplitIssue != 0 {
		problems = append(problems, fmt.Sprintf("%s has split_issue #%d without split disposition", label, claim.SplitIssue))
	}
	for _, tracker := range claim.RiskTrackers {
		if tracker.Kind != "tracker" || tracker.Issue == 0 {
			problems = append(problems, label+" risk tracker must be a concrete tracker proof_ref")
			continue
		}
		if _, ok := activeTrackers[trackerKey(tracker.Issue, tracker.Watchlist)]; !ok {
			problems = append(problems, fmt.Sprintf("%s risk tracker issue #%d watchlist %q is not active", label, tracker.Issue, tracker.Watchlist))
		}
	}
	return problems
}

func validatePublicSurfaceSelectedPurposes(purposes []publicSurfaceSelectedPurposeClaim, claims map[string]publicSurfaceStoreParityClaim, sources publicSurfaceStoreParitySourceSets, ctx publicSurfaceValidationContext, policy testplanning.Policy) []string {
	var problems []string
	constructors := map[string]struct{}{}
	consumers := map[string]struct{}{}
	seenIDs := map[string]struct{}{}
	for _, purpose := range purposes {
		label := "selected purpose " + strings.TrimSpace(purpose.ID)
		if purpose.ID == "" {
			problems = append(problems, "selected purpose missing id")
		}
		if _, ok := seenIDs[purpose.ID]; ok {
			problems = append(problems, label+" appears more than once")
		}
		seenIDs[purpose.ID] = struct{}{}
		constructors[strings.TrimSpace(purpose.Constructor)] = struct{}{}
		if len(purpose.Consumers) == 0 {
			problems = append(problems, label+" missing consumers")
		}
		for _, consumer := range purpose.Consumers {
			consumer = strings.TrimSpace(purpose.ID) + "@" + filepath.ToSlash(filepath.Clean(consumer))
			consumers[consumer] = struct{}{}
		}
		if strings.TrimSpace(purpose.SemanticOwner) == "" {
			problems = append(problems, label+" missing semantic_owner")
		}
		if len(purpose.ClaimIDs) == 0 {
			problems = append(problems, label+" missing claim_ids")
		}
		for _, claimID := range purpose.ClaimIDs {
			if _, ok := claims[claimID]; !ok {
				problems = append(problems, fmt.Sprintf("%s references unknown claim %s", label, claimID))
			}
		}
		problems = append(problems, validatePublicSurfaceStoreParityProofs(label, purpose.ProofProfile, purpose.ProofRefs, ctx, policy)...)
		problems = append(problems, validatePublicSurfaceStoreParityProofs(label+" close", purpose.ProofProfile, purpose.CloseProofRefs, ctx, policy)...)
	}
	problems = append(problems, validatePublicSurfaceStoreParitySet("selected production constructor", constructors, sources.constructors)...)
	problems = append(problems, validatePublicSurfaceStoreParitySet("selected production consumer", consumers, sources.consumers)...)
	return problems
}

func validatePublicSurfaceStoreParityProofs(label, profile string, refs []publicSurfaceProofRef, ctx publicSurfaceValidationContext, policy testplanning.Policy) []string {
	var problems []string
	if len(refs) == 0 {
		return []string{label + " missing executable proof_refs"}
	}
	if _, ok := policy.Profiles[profile]; !ok {
		problems = append(problems, fmt.Sprintf("%s proof_profile %q is not canonical", label, profile))
		return problems
	}
	for _, ref := range refs {
		if ref.Kind != "go_test" || strings.TrimSpace(ref.Name) == "" {
			problems = append(problems, label+" proof_refs must be executable go_test selectors")
			continue
		}
		path, ok := ctx.goTests[ref.Name]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s go_test proof_ref %s does not resolve", label, ref.Name))
			continue
		}
		if !publicSurfaceStoreParityProofScheduled(policy, profile, ref.Name, path) {
			problems = append(problems, fmt.Sprintf("%s go_test proof_ref %s is not scheduled by profile %s", label, ref.Name, profile))
		}
	}
	return problems
}

func publicSurfaceStoreParityProofScheduled(policy testplanning.Policy, profile, testName, sourcePath string) bool {
	packagePath := policy.Module
	dir := filepath.ToSlash(filepath.Dir(sourcePath))
	if dir != "." && dir != "" {
		packagePath += "/" + strings.TrimPrefix(dir, "./")
	}
	special := false
	for _, candidate := range policy.SpecialPackages {
		if candidate == packagePath {
			special = true
			break
		}
	}
	if !special {
		return true
	}
	profilePolicy := policy.Profiles[profile]
	for _, unitID := range profilePolicy.Units {
		unit := policy.Units[unitID]
		if !publicSurfaceHasValue(unit.Packages, packagePath) {
			continue
		}
		if unit.Run == "" {
			return true
		}
		matched, err := regexp.MatchString(unit.Run, testName)
		if err == nil && matched {
			return true
		}
	}
	return false
}

func validatePublicSurfaceStoreParityPartition(label string, claimed, different []string, source map[string]struct{}) []string {
	combined := append(append([]string(nil), claimed...), different...)
	problems := validatePublicSurfaceStoreParityValues(label, combined, source)
	claimedSet := stringSliceSet(claimed)
	for _, value := range different {
		if _, ok := claimedSet[value]; ok {
			problems = append(problems, fmt.Sprintf("%s %s is both selected and different-construction", label, value))
		}
	}
	return problems
}

func validatePublicSurfaceStoreParityValues(label string, actualValues []string, expected map[string]struct{}) []string {
	actual := map[string]struct{}{}
	var problems []string
	for _, value := range actualValues {
		value = strings.TrimSpace(value)
		if value == "" {
			problems = append(problems, label+" census contains an empty value")
			continue
		}
		if _, ok := actual[value]; ok {
			problems = append(problems, fmt.Sprintf("%s census contains duplicate %s", label, value))
		}
		actual[value] = struct{}{}
	}
	return append(problems, validatePublicSurfaceStoreParitySet(label, actual, expected)...)
}

func validatePublicSurfaceStoreParitySet(label string, actual, expected map[string]struct{}) []string {
	var problems []string
	for value := range expected {
		if _, ok := actual[value]; !ok {
			problems = append(problems, fmt.Sprintf("%s census missing %s", label, value))
		}
	}
	for value := range actual {
		if _, ok := expected[value]; !ok {
			problems = append(problems, fmt.Sprintf("%s census contains stale %s", label, value))
		}
	}
	return problems
}

func stringSliceSet(values []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func collectStoreParityClaimValues(claims []publicSurfaceStoreParityClaim, values func(publicSurfaceStoreParityClaim) []string) []string {
	var out []string
	for _, claim := range claims {
		out = append(out, values(claim)...)
	}
	return out
}

func loadPublicSurfaceStoreParityProofPolicy(root, relative string) (testplanning.Policy, error) {
	file, err := os.Open(filepath.Join(root, filepath.Clean(relative)))
	if err != nil {
		return testplanning.Policy{}, fmt.Errorf("load store parity proof policy: %w", err)
	}
	defer file.Close()
	policy, err := testplanning.LoadPolicy(file)
	if err != nil {
		return testplanning.Policy{}, fmt.Errorf("load store parity proof policy: %w", err)
	}
	return policy, nil
}

func loadPublicSurfaceStoreParitySourceSets(root string) (publicSurfaceStoreParitySourceSets, error) {
	constructors, err := sourceExportedOpenFunctions(filepath.Join(root, "internal/store/selected"))
	if err != nil {
		return publicSurfaceStoreParitySourceSets{}, err
	}
	consumers, err := sourceSelectedPurposeConsumers(root)
	if err != nil {
		return publicSurfaceStoreParitySourceSets{}, err
	}
	required, err := sourceNamedTypeMembers(filepath.Join(root, "internal/store/selected/selected.go"), "requiredPorts", false)
	if err != nil {
		return publicSurfaceStoreParitySourceSets{}, err
	}
	products, err := sourceNamedTypeMembers(filepath.Join(root, "internal/store/selected/selected.go"), "productPorts", false)
	if err != nil {
		return publicSurfaceStoreParitySourceSets{}, err
	}
	for value := range products {
		if strings.HasSuffix(value, "Available") {
			delete(products, value)
		}
	}
	runtimeDeps, err := sourceNamedTypeMembers(filepath.Join(root, "internal/runtime/runtime.go"), "RuntimeDeps", false)
	if err != nil {
		return publicSurfaceStoreParitySourceSets{}, err
	}
	eventBus, err := sourceNamedTypeMembers(filepath.Join(root, "internal/runtime/bus/eventbus.go"), "DurableDependencies", false)
	if err != nil {
		return publicSurfaceStoreParitySourceSets{}, err
	}
	manager, err := sourceNamedTypeMembers(filepath.Join(root, "internal/runtime/manager/types.go"), "PersistenceRoles", false)
	if err != nil {
		return publicSurfaceStoreParitySourceSets{}, err
	}
	workflow, err := sourceNamedTypeMembers(filepath.Join(root, "internal/runtime/pipeline/workflow_instance_store.go"), "WorkflowPersistenceOwner", true)
	if err != nil {
		return publicSurfaceStoreParitySourceSets{}, err
	}
	optionalSubroles := map[string]struct{}{}
	for _, typeName := range []string{"BundleDelete", "ConversationFork", "DestructiveReset", "StartupRecovery", "RunFork"} {
		members, memberErr := sourceNamedTypeMembers(filepath.Join(root, "internal/store/selected/selected.go"), typeName, false)
		if memberErr != nil {
			return publicSurfaceStoreParitySourceSets{}, memberErr
		}
		for member := range members {
			optionalSubroles[typeName+"."+member] = struct{}{}
		}
	}
	tables, err := sourcePlatformTables(filepath.Join(root, "platform-spec.yaml"))
	if err != nil {
		return publicSurfaceStoreParitySourceSets{}, err
	}
	return publicSurfaceStoreParitySourceSets{
		constructors: constructors, consumers: consumers, requiredPorts: required,
		optionalProducts: products, runtimeDeps: runtimeDeps, eventBusDurableRoles: eventBus,
		managerPersistenceRoles: manager, workflowPersistenceRoles: workflow,
		optionalSubroles: optionalSubroles, platformTables: tables,
	}, nil
}

func sourceExportedOpenFunctions(dir string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Recv == nil && ast.IsExported(fn.Name.Name) && strings.HasPrefix(fn.Name.Name, "Open") {
				out[fn.Name.Name] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("enumerate selected constructors: %w", err)
	}
	return out, nil
}

func sourceSelectedPurposeConsumers(root string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || strings.Contains(filepath.ToSlash(path), "/internal/store/selected/") {
			return nil
		}
		set := token.NewFileSet()
		file, parseErr := parser.ParseFile(set, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		aliases := map[string]struct{}{}
		for _, importSpec := range file.Imports {
			importPath, unquoteErr := strconv.Unquote(importSpec.Path.Value)
			if unquoteErr != nil || importPath != selectedPackageImportPath {
				continue
			}
			alias := "selected"
			if importSpec.Name != nil {
				alias = importSpec.Name.Name
			}
			aliases[alias] = struct{}{}
		}
		if len(aliases) == 0 {
			return nil
		}
		relative, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		purposes := map[string]struct{}{}
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			identifier, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			if _, ok := aliases[identifier.Name]; !ok {
				return true
			}
			switch selector.Sel.Name {
			case "OpenRuntime", "Owner":
				purposes["runtime"] = struct{}{}
			case "OpenAuthorityInspection", "AuthorityInspection":
				purposes["authority_inspection"] = struct{}{}
			case "OpenAuthorityMaintenance", "AuthorityMaintenance":
				purposes["authority_maintenance"] = struct{}{}
			}
			return true
		})
		for purpose := range purposes {
			out[purpose+"@"+filepath.ToSlash(relative)] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("enumerate selected purpose consumers: %w", err)
	}
	return out, nil
}

func sourceNamedTypeMembers(path, typeName string, interfaceMembers bool) (map[string]struct{}, error) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", filepath.Base(path), err)
	}
	for _, decl := range file.Decls {
		generic, ok := decl.(*ast.GenDecl)
		if !ok || generic.Tok != token.TYPE {
			continue
		}
		for _, spec := range generic.Specs {
			typeSpec := spec.(*ast.TypeSpec)
			if typeSpec.Name.Name != typeName {
				continue
			}
			var fields *ast.FieldList
			switch typed := typeSpec.Type.(type) {
			case *ast.StructType:
				fields = typed.Fields
			case *ast.InterfaceType:
				fields = typed.Methods
			default:
				return nil, fmt.Errorf("type %s in %s is not a struct or interface", typeName, path)
			}
			out := map[string]struct{}{}
			for _, field := range fields.List {
				if len(field.Names) > 0 {
					for _, name := range field.Names {
						out[name.Name] = struct{}{}
					}
					continue
				}
				if interfaceMembers {
					var rendered strings.Builder
					if err := format.Node(&rendered, set, field.Type); err != nil {
						return nil, err
					}
					out[rendered.String()] = struct{}{}
				}
			}
			return out, nil
		}
	}
	return nil, fmt.Errorf("type %s not found in %s", typeName, path)
}

func sourcePlatformTables(path string) (map[string]struct{}, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var spec struct {
		PlatformTables struct {
			Tables map[string]any `yaml:"tables"`
		} `yaml:"platform_tables"`
	}
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		return nil, err
	}
	out := map[string]struct{}{}
	for table := range spec.PlatformTables.Tables {
		out[table] = struct{}{}
	}
	return out, nil
}

func publicSurfaceStoreParityHasTrackerIssue(refs []publicSurfaceProofRef, issue int, activeTrackers map[string]struct{}) bool {
	for _, ref := range refs {
		if ref.Kind == "tracker" && ref.Issue == issue {
			if _, ok := activeTrackers[trackerKey(ref.Issue, ref.Watchlist)]; ok {
				return true
			}
		}
	}
	return false
}

func allowedPublicSurfaceStoreParityDispositions() map[string]struct{} {
	return complianceStringSet([]string{
		"supported_with_dual_backend_proof",
		"unsupported_with_exact_spec_and_teaching_proof",
		"split_with_active_issue_and_fail_closed_proof",
		"not_applicable_different_semantic_concept_with_proof",
	})
}

func allowedPublicSurfaceStoreParityRisks() map[string]struct{} {
	return complianceStringSet([]string{
		"structural_census", "selected_construction", "real_v1_handler", "served_lifecycle",
		"restart", "forced_death", "contention", "n_load", "store_size", "fail_closed",
		"schema_coherence", "proof_schedule", "read_snapshot", "writer_ownership",
	})
}
