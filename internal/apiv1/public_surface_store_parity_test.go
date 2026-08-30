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
	ProofCatalog          []publicSurfaceStoreParityProof         `yaml:"proof_catalog"`
	SelectedPurposes      []publicSurfaceSelectedPurposeClaim     `yaml:"selected_purposes"`
	DifferentConstruction publicSurfaceDifferentConstructionRoles `yaml:"different_construction"`
	CompositeOwners       []publicSurfaceStoreCompositeOwner      `yaml:"composite_owners"`
	Claims                []publicSurfaceStoreParityClaim         `yaml:"claims"`
}

type publicSurfaceSelectedPurposeClaim struct {
	ID            string   `yaml:"id"`
	Constructor   string   `yaml:"constructor"`
	Consumers     []string `yaml:"consumers"`
	SemanticOwner string   `yaml:"semantic_owner"`
	ProofIDs      []string `yaml:"proof_ids"`
	CloseProofIDs []string `yaml:"close_proof_ids"`
}

type publicSurfaceDifferentConstructionRoles struct {
	RuntimeDeps []string `yaml:"runtime_deps"`
}

type publicSurfaceStoreParityClaim struct {
	ID                  string                             `yaml:"id"`
	SemanticOwners      []string                           `yaml:"semantic_owners"`
	SelectedPurposes    []string                           `yaml:"selected_purposes,omitempty"`
	BackendDispositions map[string]string                  `yaml:"backend_dispositions"`
	SplitIssue          int                                `yaml:"split_issue,omitempty"`
	SplitProofID        string                             `yaml:"split_proof_id,omitempty"`
	SpecRefs            []string                           `yaml:"spec_refs"`
	Evidence            []publicSurfaceStoreParityEvidence `yaml:"evidence"`
	RiskDimensions      []string                           `yaml:"risk_dimensions"`
	RequiredPorts       []string                           `yaml:"required_ports,omitempty"`
	RuntimeDeps         []string                           `yaml:"runtime_deps,omitempty"`
	NestedRoles         map[string][]string                `yaml:"nested_roles,omitempty"`
	OptionalProducts    []string                           `yaml:"optional_products,omitempty"`
	PlatformTables      []string                           `yaml:"platform_tables,omitempty"`
	PublicMethods       []string                           `yaml:"public_methods,omitempty"`
}

type publicSurfaceStoreParityEvidence struct {
	Role           string   `yaml:"role"`
	Backends       []string `yaml:"backends,omitempty"`
	RiskDimensions []string `yaml:"risk_dimensions,omitempty"`
	ProofIDs       []string `yaml:"proof_ids"`
}

type publicSurfaceStoreParityProof struct {
	ID           string                              `yaml:"id"`
	Kind         string                              `yaml:"kind"`
	Name         string                              `yaml:"name,omitempty"`
	Path         string                              `yaml:"path,omitempty"`
	Profile      string                              `yaml:"profile,omitempty"`
	Issue        int                                 `yaml:"issue,omitempty"`
	Watchlist    string                              `yaml:"watchlist,omitempty"`
	Backends     []string                            `yaml:"backends,omitempty"`
	PurposeIDs   []string                            `yaml:"purpose_ids,omitempty"`
	Capabilities []publicSurfaceStoreProofCapability `yaml:"capabilities,omitempty"`
}

type publicSurfaceStoreProofCapability struct {
	ClaimID        string   `yaml:"claim_id"`
	RiskDimensions []string `yaml:"risk_dimensions,omitempty"`
}

type publicSurfaceStoreCompositeOwner struct {
	ID                    string   `yaml:"id"`
	Parent                string   `yaml:"parent"`
	RoleSourcePath        string   `yaml:"role_source_path"`
	RoleType              string   `yaml:"role_type"`
	InterfaceMembers      bool     `yaml:"interface_members,omitempty"`
	ProjectionConstructor string   `yaml:"projection_constructor,omitempty"`
	DifferentConstruction []string `yaml:"different_construction,omitempty"`
}

type publicSurfaceStoreCompositeParent struct {
	SourcePath string
	TypeName   string
}

type publicSurfaceStoreParitySourceSets struct {
	constructors     map[string]struct{}
	consumers        map[string]struct{}
	requiredPorts    map[string]struct{}
	optionalProducts map[string]struct{}
	runtimeDeps      map[string]struct{}
	compositeParents map[string]publicSurfaceStoreCompositeParent
	platformTables   map[string]struct{}
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
			name: "claim linked from selected purpose",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "event_delivery")
				claim.SelectedPurposes = storeParityStringsExcept(claim.SelectedPurposes, "runtime")
			},
			want: "store parity claim event_delivery is not linked to any selected purpose",
		},
		{
			name: "duplicate claim within selected purpose",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "event_delivery")
				claim.SelectedPurposes = append(claim.SelectedPurposes, "runtime")
			},
			want: "store parity claim event_delivery references selected purpose runtime more than once",
		},
		{
			name: "purpose semantic reassignment",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				startup := storeParityClaimByID(t, matrix, "startup_authority")
				startup.SelectedPurposes = storeParityStringsExcept(startup.SelectedPurposes, "authority_inspection")
				events := storeParityClaimByID(t, matrix, "event_delivery")
				events.SelectedPurposes = append(events.SelectedPurposes, "authority_inspection")
			},
			want: "selected purpose authority_inspection has no catalog-scoped proof for claim event_delivery",
		},
		{
			name: "optional product claim borrows generic runtime projection",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				purpose := storeParityPurposeByID(t, matrix, "runtime")
				purpose.ProofIDs = storeParityStringsExcept(purpose.ProofIDs, "served-selected-contract-fork")
				projection := storeParityProofByID(t, matrix, "runtime-port-projection")
				projection.Capabilities = append(projection.Capabilities, publicSurfaceStoreProofCapability{ClaimID: "selected_contract_fork"})
			},
			want: "selected purpose runtime optional-product claim selected_contract_fork has no catalog-scoped execution proof",
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
				claim.NestedRoles["runtime_event_bus"] = storeParityStringsExcept(claim.NestedRoles["runtime_event_bus"], "PreparedEvents")
			},
			want: "composite owner runtime_event_bus role census missing PreparedEvents",
		},
		{
			name: "nested manager role",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "agent_lifecycle")
				claim.NestedRoles["runtime_manager"] = storeParityStringsExcept(claim.NestedRoles["runtime_manager"], "LifecycleCensus")
			},
			want: "composite owner runtime_manager role census missing LifecycleCensus",
		},
		{
			name: "nested workflow role",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "workflow_runtime")
				claim.NestedRoles["runtime_workflow"] = storeParityStringsExcept(claim.NestedRoles["runtime_workflow"], "WorkflowEngineMutationOwner")
			},
			want: "composite owner runtime_workflow role census missing WorkflowEngineMutationOwner",
		},
		{
			name: "optional product",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "selected_contract_fork")
				claim.OptionalProducts = storeParityStringsExcept(claim.OptionalProducts, "runFork")
			},
			want: "optional product census missing runFork",
		},
		{
			name: "optional product subrole",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "selected_contract_fork")
				claim.NestedRoles["product_run_fork"] = storeParityStringsExcept(claim.NestedRoles["product_run_fork"], "executionOwner")
			},
			want: "composite owner product_run_fork role census missing executionOwner",
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
			name: "supported backend evidence",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				evidence := storeParityEvidenceByRole(t, storeParityClaimByID(t, matrix, "event_delivery"), "backend_support")
				evidence.Backends = storeParityStringsExcept(evidence.Backends, "default_sqlite")
			},
			want: "store parity claim event_delivery default_sqlite disposition requires exactly one backend_support evidence record, got 0",
		},
		{
			name: "backend proof substitution",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				evidence := storeParityEvidenceByRole(t, storeParityClaimByID(t, matrix, "api_idempotency"), "backend_support")
				evidence.ProofIDs = []string{"api-idempotency-sqlite"}
			},
			want: "backend explicit_postgres lacks catalog-owned proof",
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
			name: "stale split tracker",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				storeParityProofByID(t, matrix, "fanout-contention-2274").Issue = 999999
			},
			want: "store parity proof fanout-contention-2274 issue #999999",
		},
		{
			name: "teaching proof",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "destructive_reset")
				claim.Evidence = storeParityEvidenceExceptRole(claim.Evidence, "teaching_failure")
			},
			want: "store parity claim destructive_reset default_sqlite disposition requires exactly one teaching_failure evidence record, got 0",
		},
		{
			name: "executable selector",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				storeParityProofByID(t, matrix, "served-event-publish").Name = "TestMissingStoreParityProof"
			},
			want: "go_test proof_ref TestMissingStoreParityProof does not resolve",
		},
		{
			name: "duplicate proof identity",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				duplicate := *storeParityProofByID(t, matrix, "served-event-publish")
				duplicate.ID = "duplicate-event-publish"
				matrix.StoreParity.ProofCatalog = append(matrix.StoreParity.ProofCatalog, duplicate)
				storeParityEvidenceByRole(t, storeParityClaimByID(t, matrix, "event_delivery"), "backend_support").ProofIDs = append(storeParityEvidenceByRole(t, storeParityClaimByID(t, matrix, "event_delivery"), "backend_support").ProofIDs, duplicate.ID)
			},
			want: "store parity proof duplicate-event-publish duplicates selector identity owned by proof served-event-publish",
		},
		{
			name: "package-exact selector",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				storeParityProofByID(t, matrix, "served-event-publish").Path = "internal/apiv1/wrong_package_test.go"
			},
			want: "does not resolve at package-exact path internal/apiv1/wrong_package_test.go",
		},
		{
			name: "risk proof omitted",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "event_delivery")
				claim.Evidence = storeParityEvidenceExceptRisk(claim.Evidence, "contention")
			},
			want: "store parity claim event_delivery risk_dimension contention requires exactly one evidence owner, got 0",
		},
		{
			name: "risk proof skipped profile",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				storeParityProofByID(t, matrix, "golden-forced-restart").Profile = "pr-common"
			},
			want: "risk_dimension restart proof golden-forced-restart profile \"pr-common\" does not execute escalated risk proof",
		},
		{
			name: "risk proof claim overreach",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "source_artifact")
				claim.RiskDimensions = append(claim.RiskDimensions, "forced_death")
				claim.Evidence = append(claim.Evidence, publicSurfaceStoreParityEvidence{Role: "risk_proof", RiskDimensions: []string{"forced_death"}, ProofIDs: []string{"golden-forced-restart"}})
			},
			want: "proof golden-forced-restart does not own claim source_artifact",
		},
		{
			name: "unused risk capability",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				proof := storeParityProofByID(t, matrix, "golden-forced-restart")
				proof.Capabilities = append(proof.Capabilities, publicSurfaceStoreProofCapability{ClaimID: "mailbox_decision", RiskDimensions: []string{"restart"}})
			},
			want: "store parity proof golden-forced-restart claim mailbox_decision risk capability restart is not consumed",
		},
		{
			name: "risk tracker claim overreach",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				claim := storeParityClaimByID(t, matrix, "mailbox_decision")
				claim.RiskDimensions = append(claim.RiskDimensions, "contention")
				claim.Evidence = append(claim.Evidence, publicSurfaceStoreParityEvidence{Role: "risk_split", RiskDimensions: []string{"contention"}, ProofIDs: []string{"fanout-contention-2274"}})
			},
			want: "tracker fanout-contention-2274 does not own claim mailbox_decision risk_dimension contention",
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

func TestPublicSurfaceStoreParityRejectsNewCompositeOwnerFamily(t *testing.T) {
	root := repoRoot(t)
	ctx := newPublicSurfaceValidationContext(t, root)
	parents := make(map[string]publicSurfaceStoreCompositeParent, len(ctx.storeParity.sources.compositeParents)+1)
	for parent, source := range ctx.storeParity.sources.compositeParents {
		parents[parent] = source
	}
	parents["RuntimeDeps.NewSelectedComposite"] = publicSurfaceStoreCompositeParent{
		SourcePath: "internal/runtime/new_selected_composite.go",
		TypeName:   "NewSelectedComposite",
	}
	ctx.storeParity.sources.compositeParents = parents
	problems := validatePublicSurfaceBackendMatrix(root, loadPublicSurfaceBackendMatrix(t, root), ctx)
	want := "store composite parent census missing RuntimeDeps.NewSelectedComposite"
	if !publicSurfaceProblemsContain(problems, want) {
		t.Fatalf("validation problems missing %q:\n- %s", want, strings.Join(problems, "\n- "))
	}
}

func TestPublicSurfaceStoreParityRejectsAmbiguousUnqualifiedSelector(t *testing.T) {
	root := repoRoot(t)
	ctx := newPublicSurfaceValidationContext(t, root)
	goTests := make(map[string][]string, len(ctx.goTests))
	for name, paths := range ctx.goTests {
		goTests[name] = append([]string(nil), paths...)
	}
	const selector = "TestServedParityHarnessEventPublishDynamicAutoEmitLifecycle"
	goTests[selector] = append(goTests[selector], "internal/other/collision_test.go")
	ctx.goTests = goTests
	matrix := loadPublicSurfaceBackendMatrix(t, root)
	storeParityProofByID(t, &matrix, "served-event-publish").Path = ""
	problems := validatePublicSurfaceBackendMatrix(root, matrix, ctx)
	want := "go_test proof_ref " + selector + " missing package-exact path"
	if !publicSurfaceProblemsContain(problems, want) {
		t.Fatalf("validation problems missing %q:\n- %s", want, strings.Join(problems, "\n- "))
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

func storeParityProofByID(t *testing.T, matrix *publicSurfaceBackendMatrix, id string) *publicSurfaceStoreParityProof {
	t.Helper()
	for index := range matrix.StoreParity.ProofCatalog {
		if matrix.StoreParity.ProofCatalog[index].ID == id {
			return &matrix.StoreParity.ProofCatalog[index]
		}
	}
	t.Fatalf("store parity proof %s not found", id)
	return nil
}

func storeParityEvidenceByRole(t *testing.T, claim *publicSurfaceStoreParityClaim, role string) *publicSurfaceStoreParityEvidence {
	t.Helper()
	for index := range claim.Evidence {
		if claim.Evidence[index].Role == role {
			return &claim.Evidence[index]
		}
	}
	t.Fatalf("store parity claim %s evidence role %s not found", claim.ID, role)
	return nil
}

func storeParityEvidenceByRisk(t *testing.T, claim *publicSurfaceStoreParityClaim, risk string) *publicSurfaceStoreParityEvidence {
	t.Helper()
	for index := range claim.Evidence {
		if publicSurfaceHasValue(claim.Evidence[index].RiskDimensions, risk) {
			return &claim.Evidence[index]
		}
	}
	t.Fatalf("store parity claim %s risk evidence %s not found", claim.ID, risk)
	return nil
}

func storeParityEvidenceExceptRole(values []publicSurfaceStoreParityEvidence, role string) []publicSurfaceStoreParityEvidence {
	out := make([]publicSurfaceStoreParityEvidence, 0, len(values))
	for _, value := range values {
		if value.Role != role {
			out = append(out, value)
		}
	}
	return out
}

func storeParityEvidenceExceptRisk(values []publicSurfaceStoreParityEvidence, risk string) []publicSurfaceStoreParityEvidence {
	out := make([]publicSurfaceStoreParityEvidence, 0, len(values))
	for _, value := range values {
		if !publicSurfaceHasValue(value.RiskDimensions, risk) {
			out = append(out, value)
		}
	}
	return out
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
	}
	if len(claimsByID) == 0 {
		problems = append(problems, "store parity claims are required")
	}
	purposeIDs := map[string]struct{}{}
	for _, purpose := range layer.SelectedPurposes {
		purposeIDs[strings.TrimSpace(purpose.ID)] = struct{}{}
	}
	proofsByID, proofProblems := validatePublicSurfaceStoreParityProofCatalog(layer.ProofCatalog, claimsByID, purposeIDs, ctx, policy, activeTrackers)
	problems = append(problems, proofProblems...)
	for _, claim := range layer.Claims {
		problems = append(problems, validatePublicSurfaceStoreParityClaim(root, claim, ctx, policy, proofsByID)...)
	}

	problems = append(problems, validatePublicSurfaceSelectedPurposes(layer.SelectedPurposes, claimsByID, proofsByID, sources)...)
	problems = append(problems, validatePublicSurfaceStoreParityProofReferences(layer, proofsByID)...)
	problems = append(problems, validatePublicSurfaceStoreParityValues("required port", collectStoreParityClaimValues(layer.Claims, func(c publicSurfaceStoreParityClaim) []string { return c.RequiredPorts }), sources.requiredPorts)...)
	problems = append(problems, validatePublicSurfaceStoreParityValues("optional product", collectStoreParityClaimValues(layer.Claims, func(c publicSurfaceStoreParityClaim) []string { return c.OptionalProducts }), sources.optionalProducts)...)
	problems = append(problems, validatePublicSurfaceStoreParityPartition("RuntimeDeps", collectStoreParityClaimValues(layer.Claims, func(c publicSurfaceStoreParityClaim) []string { return c.RuntimeDeps }), layer.DifferentConstruction.RuntimeDeps, sources.runtimeDeps)...)
	compositeParents := make(map[string]publicSurfaceStoreCompositeParent, len(sources.compositeParents))
	for parent, source := range sources.compositeParents {
		compositeParents[parent] = source
	}
	for _, field := range layer.DifferentConstruction.RuntimeDeps {
		delete(compositeParents, "RuntimeDeps."+field)
	}
	problems = append(problems, validatePublicSurfaceStoreCompositeOwners(root, layer.CompositeOwners, layer.Claims, compositeParents)...)
	problems = append(problems, validatePublicSurfaceStoreParityValues("platform table", collectStoreParityClaimValues(layer.Claims, func(c publicSurfaceStoreParityClaim) []string { return c.PlatformTables }), sources.platformTables)...)
	problems = append(problems, validatePublicSurfaceStoreParityValues("public method", collectStoreParityClaimValues(layer.Claims, func(c publicSurfaceStoreParityClaim) []string { return c.PublicMethods }), ctx.apiMethods)...)
	problems = append(problems, validatePublicSurfaceStoreParityMethodLinks(layer.Claims, mutating, reads)...)
	sort.Strings(problems)
	return problems
}

func validatePublicSurfaceStoreParityProofCatalog(catalog []publicSurfaceStoreParityProof, claims map[string]publicSurfaceStoreParityClaim, purposes map[string]struct{}, ctx publicSurfaceValidationContext, policy testplanning.Policy, activeTrackers map[string]struct{}) (map[string]publicSurfaceStoreParityProof, []string) {
	proofs := map[string]publicSurfaceStoreParityProof{}
	goTestOwners := map[string]string{}
	var problems []string
	for _, proof := range catalog {
		id := strings.TrimSpace(proof.ID)
		label := "store parity proof " + id
		if id == "" {
			problems = append(problems, "store parity proof missing id")
			continue
		}
		if _, ok := proofs[id]; ok {
			problems = append(problems, label+" appears more than once")
		}
		proofs[id] = proof

		seenBackends := map[string]struct{}{}
		for _, backend := range proof.Backends {
			if backend != "default_sqlite" && backend != "explicit_postgres" {
				problems = append(problems, fmt.Sprintf("%s backend %q is not allowed", label, backend))
			}
			if _, ok := seenBackends[backend]; ok {
				problems = append(problems, fmt.Sprintf("%s repeats backend %s", label, backend))
			}
			seenBackends[backend] = struct{}{}
		}
		seenPurposes := map[string]struct{}{}
		for _, purposeID := range proof.PurposeIDs {
			if _, ok := purposes[purposeID]; !ok {
				problems = append(problems, fmt.Sprintf("%s references unknown selected purpose %s", label, purposeID))
			}
			if _, ok := seenPurposes[purposeID]; ok {
				problems = append(problems, fmt.Sprintf("%s repeats selected purpose %s", label, purposeID))
			}
			seenPurposes[purposeID] = struct{}{}
		}
		seenClaims := map[string]struct{}{}
		for _, capability := range proof.Capabilities {
			if _, ok := claims[capability.ClaimID]; !ok {
				problems = append(problems, fmt.Sprintf("%s capability references unknown claim %s", label, capability.ClaimID))
			}
			if _, ok := seenClaims[capability.ClaimID]; ok {
				problems = append(problems, fmt.Sprintf("%s repeats claim capability %s", label, capability.ClaimID))
			}
			seenClaims[capability.ClaimID] = struct{}{}
			seenRisks := map[string]struct{}{}
			for _, risk := range capability.RiskDimensions {
				if _, ok := allowedPublicSurfaceStoreParityRisks()[risk]; !ok {
					problems = append(problems, fmt.Sprintf("%s claim %s risk_dimension %q is not allowed", label, capability.ClaimID, risk))
				}
				if _, ok := seenRisks[risk]; ok {
					problems = append(problems, fmt.Sprintf("%s claim %s repeats risk_dimension %s", label, capability.ClaimID, risk))
				}
				seenRisks[risk] = struct{}{}
			}
		}

		switch proof.Kind {
		case "go_test":
			if proof.Issue != 0 || proof.Watchlist != "" {
				problems = append(problems, label+" executable proof must not contain tracker identity")
			}
			ref := publicSurfaceProofRef{Kind: "go_test", Name: proof.Name, Path: proof.Path}
			problems = append(problems, validatePublicSurfaceStoreParityProofs(label, proof.Profile, []publicSurfaceProofRef{ref}, ctx, policy)...)
			identity := strings.TrimSpace(proof.Name) + "@" + filepath.ToSlash(filepath.Clean(proof.Path))
			if owner, ok := goTestOwners[identity]; ok {
				problems = append(problems, fmt.Sprintf("%s duplicates selector identity owned by proof %s", label, owner))
			} else {
				goTestOwners[identity] = id
			}
		case "tracker":
			if proof.Issue == 0 || proof.Watchlist == "" {
				problems = append(problems, label+" missing concrete tracker identity")
			} else if _, ok := activeTrackers[trackerKey(proof.Issue, proof.Watchlist)]; !ok {
				problems = append(problems, fmt.Sprintf("%s issue #%d watchlist %q is not active", label, proof.Issue, proof.Watchlist))
			}
			if proof.Name != "" || proof.Path != "" || proof.Profile != "" || len(proof.Backends) != 0 || len(proof.PurposeIDs) != 0 {
				problems = append(problems, label+" tracker must not claim executable or selected-purpose scope")
			}
		default:
			problems = append(problems, fmt.Sprintf("%s kind %q is not allowed", label, proof.Kind))
		}
	}
	if len(proofs) == 0 {
		problems = append(problems, "store parity proof_catalog is required")
	}
	return proofs, problems
}

func validatePublicSurfaceStoreParityProofReferences(layer publicSurfaceStoreParity, proofs map[string]publicSurfaceStoreParityProof) []string {
	references := map[string]int{}
	claimUses := map[string]map[string]bool{}
	riskUses := map[string]map[string]map[string]bool{}
	markClaim := func(proofID, claimID string) {
		if claimUses[proofID] == nil {
			claimUses[proofID] = map[string]bool{}
		}
		claimUses[proofID][claimID] = true
	}
	markRisk := func(proofID, claimID, risk string) {
		if riskUses[proofID] == nil {
			riskUses[proofID] = map[string]map[string]bool{}
		}
		if riskUses[proofID][claimID] == nil {
			riskUses[proofID][claimID] = map[string]bool{}
		}
		riskUses[proofID][claimID][risk] = true
	}
	for _, purpose := range layer.SelectedPurposes {
		for _, id := range append(append([]string(nil), purpose.ProofIDs...), purpose.CloseProofIDs...) {
			references[id]++
		}
		for _, claim := range layer.Claims {
			if !publicSurfaceHasValue(claim.SelectedPurposes, purpose.ID) {
				continue
			}
			for _, id := range purpose.ProofIDs {
				if proof, ok := proofs[id]; ok && findPublicSurfaceStoreProofCapability(proof, claim.ID) != nil {
					markClaim(id, claim.ID)
				}
			}
		}
	}
	for _, claim := range layer.Claims {
		if claim.SplitProofID != "" {
			references[claim.SplitProofID]++
			markClaim(claim.SplitProofID, claim.ID)
		}
		for _, evidence := range claim.Evidence {
			for _, id := range evidence.ProofIDs {
				references[id]++
				markClaim(id, claim.ID)
				for _, risk := range evidence.RiskDimensions {
					markRisk(id, claim.ID, risk)
				}
			}
		}
	}
	var problems []string
	for id := range proofs {
		if references[id] == 0 {
			problems = append(problems, fmt.Sprintf("store parity proof %s is not referenced", id))
		}
		for _, capability := range proofs[id].Capabilities {
			if !claimUses[id][capability.ClaimID] {
				problems = append(problems, fmt.Sprintf("store parity proof %s claim capability %s is not consumed", id, capability.ClaimID))
			}
			for _, risk := range capability.RiskDimensions {
				if !riskUses[id][capability.ClaimID][risk] {
					problems = append(problems, fmt.Sprintf("store parity proof %s claim %s risk capability %s is not consumed", id, capability.ClaimID, risk))
				}
			}
		}
	}
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

func validatePublicSurfaceStoreParityClaim(root string, claim publicSurfaceStoreParityClaim, ctx publicSurfaceValidationContext, policy testplanning.Policy, proofs map[string]publicSurfaceStoreParityProof) []string {
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
	declaredRisks := map[string]struct{}{}
	if len(claim.RiskDimensions) == 0 {
		problems = append(problems, label+" missing risk_dimensions")
	}
	for _, dimension := range claim.RiskDimensions {
		if _, ok := allowedPublicSurfaceStoreParityRisks()[dimension]; !ok {
			problems = append(problems, fmt.Sprintf("%s risk_dimension %q is not allowed", label, dimension))
		}
		if _, ok := declaredRisks[dimension]; ok {
			problems = append(problems, fmt.Sprintf("%s risk_dimension %q appears more than once", label, dimension))
		}
		declaredRisks[dimension] = struct{}{}
	}
	if len(claim.Evidence) == 0 {
		problems = append(problems, label+" missing evidence")
	}
	backendCoverage := map[string]map[string]int{}
	riskCoverage := map[string]int{}
	for index, evidence := range claim.Evidence {
		evidenceLabel := fmt.Sprintf("%s evidence[%d]", label, index)
		if _, ok := allowedPublicSurfaceStoreParityEvidenceRoles()[evidence.Role]; !ok {
			problems = append(problems, fmt.Sprintf("%s role %q is not allowed", evidenceLabel, evidence.Role))
			continue
		}
		for _, backend := range evidence.Backends {
			if backend != "default_sqlite" && backend != "explicit_postgres" {
				problems = append(problems, fmt.Sprintf("%s backend %q is not allowed", evidenceLabel, backend))
				continue
			}
			if backendCoverage[backend] == nil {
				backendCoverage[backend] = map[string]int{}
			}
			backendCoverage[backend][evidence.Role]++
		}
		for _, dimension := range evidence.RiskDimensions {
			if _, ok := declaredRisks[dimension]; !ok {
				problems = append(problems, fmt.Sprintf("%s covers undeclared risk_dimension %q", evidenceLabel, dimension))
			}
			riskCoverage[dimension]++
		}
		switch evidence.Role {
		case "backend_support", "teaching_failure", "different_concept":
			if len(evidence.Backends) == 0 {
				problems = append(problems, evidenceLabel+" missing backends")
			}
		case "risk_proof", "risk_split":
			if len(evidence.RiskDimensions) == 0 {
				problems = append(problems, evidenceLabel+" missing risk_dimensions")
			}
			if len(evidence.Backends) != 0 {
				problems = append(problems, evidenceLabel+" risk evidence must not claim backend disposition coverage")
			}
		}
		problems = append(problems, validatePublicSurfaceStoreParityEvidenceProofs(evidenceLabel, claim.ID, evidence, proofs)...)
		for _, dimension := range evidence.RiskDimensions {
			problems = append(problems, validatePublicSurfaceStoreParityRiskExecution(evidenceLabel, claim.ID, dimension, evidence, proofs)...)
		}
	}

	needsSplit := false
	for backend, disposition := range claim.BackendDispositions {
		needsSplit = needsSplit || disposition == "split_with_active_issue_and_fail_closed_proof"
		wantRole := map[string]string{
			"supported_with_dual_backend_proof":                    "backend_support",
			"unsupported_with_exact_spec_and_teaching_proof":       "teaching_failure",
			"split_with_active_issue_and_fail_closed_proof":        "teaching_failure",
			"not_applicable_different_semantic_concept_with_proof": "different_concept",
		}[disposition]
		if wantRole == "" {
			continue
		}
		if backendCoverage[backend][wantRole] != 1 {
			problems = append(problems, fmt.Sprintf("%s %s disposition requires exactly one %s evidence record, got %d", label, backend, wantRole, backendCoverage[backend][wantRole]))
		}
		for role, count := range backendCoverage[backend] {
			if role != wantRole && count > 0 {
				problems = append(problems, fmt.Sprintf("%s %s disposition conflicts with %s evidence", label, backend, role))
			}
		}
	}
	if needsSplit {
		if claim.SplitIssue == 0 {
			problems = append(problems, label+" split disposition missing split_issue")
		} else if claim.SplitProofID == "" {
			problems = append(problems, fmt.Sprintf("%s split issue #%d missing exact split_tracker", label, claim.SplitIssue))
		} else if proof, ok := proofs[claim.SplitProofID]; !ok || proof.Kind != "tracker" || proof.Issue != claim.SplitIssue || findPublicSurfaceStoreProofCapability(proof, claim.ID) == nil {
			problems = append(problems, fmt.Sprintf("%s split issue #%d missing exact split_tracker", label, claim.SplitIssue))
		}
	} else if claim.SplitIssue != 0 || claim.SplitProofID != "" {
		problems = append(problems, fmt.Sprintf("%s has split tracker without split disposition", label))
	}
	for dimension := range declaredRisks {
		if riskCoverage[dimension] != 1 {
			problems = append(problems, fmt.Sprintf("%s risk_dimension %s requires exactly one evidence owner, got %d", label, dimension, riskCoverage[dimension]))
		}
	}
	return problems
}

func validatePublicSurfaceStoreParityEvidenceProofs(label, claimID string, evidence publicSurfaceStoreParityEvidence, proofs map[string]publicSurfaceStoreParityProof) []string {
	var problems []string
	if len(evidence.ProofIDs) == 0 {
		return []string{label + " missing proof_ids"}
	}
	coveredBackends := map[string]struct{}{}
	coveredRisks := map[string]struct{}{}
	seen := map[string]struct{}{}
	for _, proofID := range evidence.ProofIDs {
		if _, ok := seen[proofID]; ok {
			problems = append(problems, fmt.Sprintf("%s repeats proof %s", label, proofID))
		}
		seen[proofID] = struct{}{}
		proof, ok := proofs[proofID]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s references unknown proof %s", label, proofID))
			continue
		}
		capability := findPublicSurfaceStoreProofCapability(proof, claimID)
		if capability == nil {
			problems = append(problems, fmt.Sprintf("%s proof %s does not own claim %s", label, proofID, claimID))
			continue
		}
		wantKind := "go_test"
		if evidence.Role == "risk_split" {
			wantKind = "tracker"
		}
		if proof.Kind != wantKind {
			problems = append(problems, fmt.Sprintf("%s proof %s kind = %s, want %s", label, proofID, proof.Kind, wantKind))
			continue
		}
		for _, backend := range proof.Backends {
			coveredBackends[backend] = struct{}{}
		}
		for _, risk := range capability.RiskDimensions {
			coveredRisks[risk] = struct{}{}
		}
	}
	for _, backend := range evidence.Backends {
		if _, ok := coveredBackends[backend]; !ok {
			problems = append(problems, fmt.Sprintf("%s backend %s lacks catalog-owned proof", label, backend))
		}
	}
	for _, risk := range evidence.RiskDimensions {
		if _, ok := coveredRisks[risk]; !ok {
			owner := "proof"
			if evidence.Role == "risk_split" {
				owner = "tracker"
			}
			problems = append(problems, fmt.Sprintf("%s %s %s does not own claim %s risk_dimension %s", label, owner, strings.Join(evidence.ProofIDs, ","), claimID, risk))
		}
	}
	return problems
}

func validatePublicSurfaceStoreParityRiskExecution(label, claimID, dimension string, evidence publicSurfaceStoreParityEvidence, proofs map[string]publicSurfaceStoreParityProof) []string {
	if evidence.Role == "risk_split" {
		return nil
	}
	if dimension != "restart" && dimension != "forced_death" && dimension != "contention" {
		return nil
	}
	if evidence.Role != "risk_proof" {
		return []string{fmt.Sprintf("%s risk_dimension %s requires dedicated risk_proof evidence", label, dimension)}
	}
	coveredBackends := map[string]struct{}{}
	for _, proofID := range evidence.ProofIDs {
		proof, ok := proofs[proofID]
		if !ok || proof.Kind != "go_test" {
			continue
		}
		capability := findPublicSurfaceStoreProofCapability(proof, claimID)
		if capability == nil || !publicSurfaceHasValue(capability.RiskDimensions, dimension) {
			continue
		}
		if proof.Profile != "full" && proof.Profile != "nightly" {
			return []string{fmt.Sprintf("%s risk_dimension %s proof %s profile %q does not execute escalated risk proof", label, dimension, proofID, proof.Profile)}
		}
		for _, backend := range proof.Backends {
			coveredBackends[backend] = struct{}{}
		}
	}
	if _, sqlite := coveredBackends["default_sqlite"]; sqlite {
		if _, postgres := coveredBackends["explicit_postgres"]; postgres {
			return nil
		}
	}
	return []string{fmt.Sprintf("%s risk_dimension %s lacks exact dual-backend catalog proof", label, dimension)}
}

func findPublicSurfaceStoreProofCapability(proof publicSurfaceStoreParityProof, claimID string) *publicSurfaceStoreProofCapability {
	for index := range proof.Capabilities {
		if proof.Capabilities[index].ClaimID == claimID {
			return &proof.Capabilities[index]
		}
	}
	return nil
}

func validatePublicSurfaceSelectedPurposes(purposes []publicSurfaceSelectedPurposeClaim, claims map[string]publicSurfaceStoreParityClaim, proofs map[string]publicSurfaceStoreParityProof, sources publicSurfaceStoreParitySourceSets) []string {
	var problems []string
	constructors := map[string]struct{}{}
	consumers := map[string]struct{}{}
	seenIDs := map[string]struct{}{}
	purposesByID := map[string]publicSurfaceSelectedPurposeClaim{}
	for _, purpose := range purposes {
		label := "selected purpose " + strings.TrimSpace(purpose.ID)
		if purpose.ID == "" {
			problems = append(problems, "selected purpose missing id")
		}
		if _, ok := seenIDs[purpose.ID]; ok {
			problems = append(problems, label+" appears more than once")
		}
		seenIDs[purpose.ID] = struct{}{}
		purposesByID[purpose.ID] = purpose
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
		problems = append(problems, validatePublicSurfaceSelectedPurposeProofIDs(label, purpose.ID, purpose.ProofIDs, proofs)...)
		problems = append(problems, validatePublicSurfaceSelectedPurposeProofIDs(label+" close", purpose.ID, purpose.CloseProofIDs, proofs)...)
	}
	problems = append(problems, validatePublicSurfaceStoreParitySet("selected production constructor", constructors, sources.constructors)...)
	problems = append(problems, validatePublicSurfaceStoreParitySet("selected production consumer", consumers, sources.consumers)...)
	claimsByPurpose := map[string][]string{}
	for claimID, claim := range claims {
		seenPurposes := map[string]struct{}{}
		for _, purposeID := range claim.SelectedPurposes {
			if _, ok := seenPurposes[purposeID]; ok {
				problems = append(problems, fmt.Sprintf("store parity claim %s references selected purpose %s more than once", claimID, purposeID))
			}
			seenPurposes[purposeID] = struct{}{}
			if _, ok := purposesByID[purposeID]; !ok {
				problems = append(problems, fmt.Sprintf("store parity claim %s references unknown selected purpose %s", claimID, purposeID))
				continue
			}
			claimsByPurpose[purposeID] = append(claimsByPurpose[purposeID], claimID)
		}
		if len(claim.SelectedPurposes) == 0 && !publicSurfaceStoreParityClaimIsDifferentConcept(claim) {
			problems = append(problems, fmt.Sprintf("store parity claim %s is not linked to any selected purpose", claimID))
		}
	}
	for purposeID, purpose := range purposesByID {
		claimIDs := claimsByPurpose[purposeID]
		if len(claimIDs) == 0 {
			problems = append(problems, fmt.Sprintf("selected purpose %s is not linked to any store parity claim", purposeID))
			continue
		}
		for _, claimID := range claimIDs {
			owned := false
			executionOwned := false
			evidenceProofIDs := publicSurfaceStoreParityEvidenceProofIDs(claims[claimID].Evidence)
			for _, proofID := range purpose.ProofIDs {
				proof, ok := proofs[proofID]
				proofOwnsClaim := ok && publicSurfaceHasValue(proof.PurposeIDs, purposeID) && findPublicSurfaceStoreProofCapability(proof, claimID) != nil
				owned = owned || proofOwnsClaim
				_, isExecutionEvidence := evidenceProofIDs[proofID]
				executionOwned = executionOwned || (proofOwnsClaim && isExecutionEvidence)
			}
			if !owned {
				problems = append(problems, fmt.Sprintf("selected purpose %s has no catalog-scoped proof for claim %s", purposeID, claimID))
			}
			claim := claims[claimID]
			if len(claim.OptionalProducts) > 0 && len(claim.RequiredPorts) == 0 && len(claim.RuntimeDeps) == 0 && !executionOwned {
				problems = append(problems, fmt.Sprintf("selected purpose %s optional-product claim %s has no catalog-scoped execution proof", purposeID, claimID))
			}
		}
	}
	return problems
}

func publicSurfaceStoreParityEvidenceProofIDs(evidence []publicSurfaceStoreParityEvidence) map[string]struct{} {
	proofIDs := map[string]struct{}{}
	for _, record := range evidence {
		for _, proofID := range record.ProofIDs {
			proofIDs[proofID] = struct{}{}
		}
	}
	return proofIDs
}

func validatePublicSurfaceSelectedPurposeProofIDs(label, purposeID string, proofIDs []string, proofs map[string]publicSurfaceStoreParityProof) []string {
	if len(proofIDs) == 0 {
		return []string{label + " missing proof_ids"}
	}
	var problems []string
	seen := map[string]struct{}{}
	for _, proofID := range proofIDs {
		if _, ok := seen[proofID]; ok {
			problems = append(problems, fmt.Sprintf("%s repeats proof %s", label, proofID))
		}
		seen[proofID] = struct{}{}
		proof, ok := proofs[proofID]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s references unknown proof %s", label, proofID))
			continue
		}
		if proof.Kind != "go_test" || !publicSurfaceHasValue(proof.PurposeIDs, purposeID) {
			problems = append(problems, fmt.Sprintf("%s proof %s does not own selected purpose %s", label, proofID, purposeID))
		}
	}
	return problems
}

func publicSurfaceStoreParityClaimIsDifferentConcept(claim publicSurfaceStoreParityClaim) bool {
	for _, disposition := range claim.BackendDispositions {
		if disposition != "not_applicable_different_semantic_concept_with_proof" {
			return false
		}
	}
	return len(claim.BackendDispositions) != 0
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
		paths := ctx.goTests[ref.Name]
		if len(paths) == 0 {
			problems = append(problems, fmt.Sprintf("%s go_test proof_ref %s does not resolve", label, ref.Name))
			continue
		}
		path := filepath.ToSlash(filepath.Clean(ref.Path))
		if strings.TrimSpace(ref.Path) == "" {
			problems = append(problems, fmt.Sprintf("%s go_test proof_ref %s missing package-exact path", label, ref.Name))
			continue
		}
		if !publicSurfaceHasValue(paths, path) {
			problems = append(problems, fmt.Sprintf("%s go_test proof_ref %s does not resolve at package-exact path %s", label, ref.Name, path))
			continue
		}
		if !publicSurfaceStoreParityProofScheduled(policy, profile, ref.Name, path) {
			problems = append(problems, fmt.Sprintf("%s go_test proof_ref %s at %s is not scheduled by profile %s", label, ref.Name, path, profile))
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

func validatePublicSurfaceStoreCompositeOwners(root string, owners []publicSurfaceStoreCompositeOwner, claims []publicSurfaceStoreParityClaim, expected map[string]publicSurfaceStoreCompositeParent) []string {
	var problems []string
	ownersByID := map[string]publicSurfaceStoreCompositeOwner{}
	parents := map[string]struct{}{}
	for _, owner := range owners {
		label := "store composite owner " + strings.TrimSpace(owner.ID)
		if strings.TrimSpace(owner.ID) == "" {
			problems = append(problems, "store composite owner missing id")
			continue
		}
		if _, ok := ownersByID[owner.ID]; ok {
			problems = append(problems, label+" appears more than once")
		}
		ownersByID[owner.ID] = owner
		if _, ok := parents[owner.Parent]; ok {
			problems = append(problems, fmt.Sprintf("store composite parent %s appears more than once", owner.Parent))
		}
		parents[owner.Parent] = struct{}{}
		parent, ok := expected[owner.Parent]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s references stale parent %s", label, owner.Parent))
			continue
		}
		rolePath := filepath.ToSlash(filepath.Clean(owner.RoleSourcePath))
		if filepath.IsAbs(owner.RoleSourcePath) || strings.HasPrefix(rolePath, "../") || rolePath == ".." {
			problems = append(problems, fmt.Sprintf("%s role_source_path %s must be repo-relative", label, owner.RoleSourcePath))
			continue
		}
		if owner.ProjectionConstructor == "" {
			if rolePath != parent.SourcePath || owner.RoleType != parent.TypeName {
				problems = append(problems, fmt.Sprintf("%s role %s#%s does not match direct parent type %s#%s", label, rolePath, owner.RoleType, parent.SourcePath, parent.TypeName))
			}
		} else if err := sourceProjectionConstructorConnects(filepath.Join(root, rolePath), owner.ProjectionConstructor, owner.RoleType, parent.TypeName); err != nil {
			problems = append(problems, fmt.Sprintf("%s projection constructor does not connect role to parent: %v", label, err))
		}
		members, err := sourceNamedTypeMembers(filepath.Join(root, rolePath), owner.RoleType, owner.InterfaceMembers)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s role census failed: %v", label, err))
			continue
		}
		var claimed []string
		for _, claim := range claims {
			claimed = append(claimed, claim.NestedRoles[owner.ID]...)
		}
		problems = append(problems, validatePublicSurfaceStoreParityPartition("composite owner "+owner.ID+" role", claimed, owner.DifferentConstruction, members)...)
	}
	for parent := range expected {
		if _, ok := parents[parent]; !ok {
			problems = append(problems, fmt.Sprintf("store composite parent census missing %s", parent))
		}
	}
	for _, claim := range claims {
		for ownerID := range claim.NestedRoles {
			if _, ok := ownersByID[ownerID]; !ok {
				problems = append(problems, fmt.Sprintf("store parity claim %s references unknown composite owner %s", claim.ID, ownerID))
			}
		}
	}
	return problems
}

func sourceProjectionConstructorConnects(path, constructor, roleType, parentType string) error {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, nil, 0)
	if err != nil {
		return err
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name.Name != constructor {
			continue
		}
		parameterFound := false
		for _, field := range fn.Type.Params.List {
			if identifier, ok := field.Type.(*ast.Ident); ok && identifier.Name == roleType {
				parameterFound = true
			}
		}
		resultFound := false
		if fn.Type.Results != nil {
			for _, field := range fn.Type.Results.List {
				if identifier, ok := field.Type.(*ast.Ident); ok && identifier.Name == parentType {
					resultFound = true
				}
			}
		}
		if parameterFound && resultFound {
			return nil
		}
		return fmt.Errorf("%s must accept %s and return %s", constructor, roleType, parentType)
	}
	return fmt.Errorf("constructor %s not found in %s", constructor, filepath.Base(path))
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
	runtimeComposite, err := sourceCompositeStructFields(root, "internal/runtime/runtime.go", "RuntimeDeps")
	if err != nil {
		return publicSurfaceStoreParitySourceSets{}, err
	}
	productComposite, err := sourceCompositeStructFields(root, "internal/store/selected/selected.go", "productPorts")
	if err != nil {
		return publicSurfaceStoreParitySourceSets{}, err
	}
	compositeParents := map[string]publicSurfaceStoreCompositeParent{}
	for field, parent := range runtimeComposite {
		compositeParents["RuntimeDeps."+field] = parent
	}
	for field, parent := range productComposite {
		compositeParents["productPorts."+field] = parent
	}
	tables, err := sourcePlatformTables(filepath.Join(root, "platform-spec.yaml"))
	if err != nil {
		return publicSurfaceStoreParitySourceSets{}, err
	}
	return publicSurfaceStoreParitySourceSets{
		constructors: constructors, consumers: consumers, requiredPorts: required,
		optionalProducts: products, runtimeDeps: runtimeDeps, compositeParents: compositeParents,
		platformTables: tables,
	}, nil
}

func sourceCompositeStructFields(root, relativePath, typeName string) (map[string]publicSurfaceStoreCompositeParent, error) {
	path := filepath.Join(root, relativePath)
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, nil, 0)
	if err != nil {
		return nil, err
	}
	imports := map[string]string{}
	for _, spec := range file.Imports {
		importPath, unquoteErr := strconv.Unquote(spec.Path.Value)
		if unquoteErr != nil {
			return nil, unquoteErr
		}
		alias := filepath.Base(importPath)
		if spec.Name != nil {
			alias = spec.Name.Name
		}
		imports[alias] = importPath
	}
	for _, decl := range file.Decls {
		generic, ok := decl.(*ast.GenDecl)
		if !ok || generic.Tok != token.TYPE {
			continue
		}
		for _, genericSpec := range generic.Specs {
			typeSpec := genericSpec.(*ast.TypeSpec)
			if typeSpec.Name.Name != typeName {
				continue
			}
			structure, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				return nil, fmt.Errorf("type %s in %s is not a struct", typeName, relativePath)
			}
			out := map[string]publicSurfaceStoreCompositeParent{}
			for _, field := range structure.Fields.List {
				if len(field.Names) == 0 {
					continue
				}
				parent, composite, resolveErr := sourceResolveStructType(root, filepath.Dir(relativePath), imports, field.Type)
				if resolveErr != nil {
					return nil, resolveErr
				}
				if !composite {
					continue
				}
				for _, name := range field.Names {
					out[name.Name] = parent
				}
			}
			return out, nil
		}
	}
	return nil, fmt.Errorf("type %s not found in %s", typeName, relativePath)
}

func sourceResolveStructType(root, currentDir string, imports map[string]string, expression ast.Expr) (publicSurfaceStoreCompositeParent, bool, error) {
	for {
		switch typed := expression.(type) {
		case *ast.StarExpr:
			expression = typed.X
			continue
		case *ast.ParenExpr:
			expression = typed.X
			continue
		}
		break
	}
	typeName := ""
	typeDir := currentDir
	switch typed := expression.(type) {
	case *ast.Ident:
		typeName = typed.Name
		if _, builtin := map[string]struct{}{
			"bool": {}, "byte": {}, "complex64": {}, "complex128": {}, "error": {}, "float32": {}, "float64": {},
			"int": {}, "int8": {}, "int16": {}, "int32": {}, "int64": {}, "rune": {}, "string": {},
			"uint": {}, "uint8": {}, "uint16": {}, "uint32": {}, "uint64": {}, "uintptr": {},
		}[typeName]; builtin {
			return publicSurfaceStoreCompositeParent{}, false, nil
		}
	case *ast.SelectorExpr:
		alias, ok := typed.X.(*ast.Ident)
		if !ok {
			return publicSurfaceStoreCompositeParent{}, false, nil
		}
		importPath := imports[alias.Name]
		const modulePrefix = "github.com/division-sh/swarm/"
		if !strings.HasPrefix(importPath, modulePrefix) {
			return publicSurfaceStoreCompositeParent{}, false, nil
		}
		typeDir = strings.TrimPrefix(importPath, modulePrefix)
		typeName = typed.Sel.Name
	default:
		return publicSurfaceStoreCompositeParent{}, false, nil
	}
	relative, kind, err := sourceNamedTypeDeclaration(root, typeDir, typeName)
	if err != nil {
		return publicSurfaceStoreCompositeParent{}, false, err
	}
	if kind != "struct" {
		return publicSurfaceStoreCompositeParent{}, false, nil
	}
	return publicSurfaceStoreCompositeParent{SourcePath: relative, TypeName: typeName}, true, nil
}

func sourceNamedTypeDeclaration(root, relativeDir, typeName string) (string, string, error) {
	dir := filepath.Join(root, relativeDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", "", err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		file, parseErr := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if parseErr != nil {
			return "", "", parseErr
		}
		for _, decl := range file.Decls {
			generic, ok := decl.(*ast.GenDecl)
			if !ok || generic.Tok != token.TYPE {
				continue
			}
			for _, genericSpec := range generic.Specs {
				typeSpec := genericSpec.(*ast.TypeSpec)
				if typeSpec.Name.Name != typeName {
					continue
				}
				kind := "other"
				switch typeSpec.Type.(type) {
				case *ast.StructType:
					kind = "struct"
				case *ast.InterfaceType:
					kind = "interface"
				}
				return filepath.ToSlash(filepath.Join(relativeDir, entry.Name())), kind, nil
			}
		}
	}
	return "", "", fmt.Errorf("type %s not found in %s", typeName, relativeDir)
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

func allowedPublicSurfaceStoreParityDispositions() map[string]struct{} {
	return complianceStringSet([]string{
		"supported_with_dual_backend_proof",
		"unsupported_with_exact_spec_and_teaching_proof",
		"split_with_active_issue_and_fail_closed_proof",
		"not_applicable_different_semantic_concept_with_proof",
	})
}

func allowedPublicSurfaceStoreParityEvidenceRoles() map[string]struct{} {
	return complianceStringSet([]string{
		"backend_support", "teaching_failure", "different_concept", "risk_proof", "risk_split",
	})
}

func allowedPublicSurfaceStoreParityRisks() map[string]struct{} {
	return complianceStringSet([]string{
		"structural_census", "selected_construction", "real_v1_handler", "served_lifecycle",
		"restart", "forced_death", "contention", "n_load", "store_size", "fail_closed",
		"schema_coherence", "proof_schedule", "read_snapshot", "writer_ownership",
		"dev_forced_restart_abandonment",
	})
}
