package apiv1

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/servedparity"
	"gopkg.in/yaml.v3"
)

const publicSurfaceBackendMatrixPath = "internal/apiv1/testdata/public_surface_backend_matrix.yaml"

type publicSurfaceBackendMatrix struct {
	Version            int                                       `yaml:"version"`
	Kind               string                                    `yaml:"kind"`
	Issue              int                                       `yaml:"issue"`
	IssueRole          string                                    `yaml:"issue_role"`
	Source             publicSurfaceMatrixSource                 `yaml:"source"`
	Policy             publicSurfaceMatrixPolicy                 `yaml:"policy"`
	ActiveTrackers     []complianceActiveTracker                 `yaml:"active_trackers"`
	MutatingLedger     []publicSurfaceMutatingAPIParityEntry     `yaml:"mutating_api_parity_ledger"`
	OperatorReadLedger []publicSurfaceOperatorReadAPIParityEntry `yaml:"operator_read_api_parity_ledger"`
	Rows               []publicSurfaceMatrixRow                  `yaml:"rows"`
}

type publicSurfaceMatrixSource struct {
	PlatformSpec          string `yaml:"platform_spec"`
	OpenRPCArtifact       string `yaml:"openrpc_artifact"`
	AdjacentOpenRPCMatrix string `yaml:"adjacent_openrpc_matrix"`
}

type publicSurfaceMatrixPolicy struct {
	ClosureLevel                              string `yaml:"closure_level"`
	ClaimsParentClosure                       bool   `yaml:"claims_parent_closure"`
	NamedFullConformanceCommand               string `yaml:"named_full_conformance_command"`
	RequiredSmokePolicy                       string `yaml:"required_smoke_policy"`
	MutatingAPIParityClassificationPolicy     string `yaml:"mutating_api_parity_classification_policy"`
	OperatorReadAPIParityClassificationPolicy string `yaml:"operator_read_api_parity_classification_policy"`
}

type publicSurfaceMatrixRow struct {
	ID                   string                  `yaml:"id"`
	Surface              string                  `yaml:"surface"`
	Classification       string                  `yaml:"classification"`
	Tier                 string                  `yaml:"tier"`
	SplitIssue           int                     `yaml:"split_issue"`
	Backends             []string                `yaml:"backends"`
	CLICommands          []string                `yaml:"cli_commands"`
	APIMethods           []string                `yaml:"api_methods"`
	OpenRPCMatrixMethods []string                `yaml:"openrpc_matrix_methods"`
	StoreOwners          []string                `yaml:"store_owners"`
	ProofDimensions      []string                `yaml:"proof_dimensions"`
	ProofRefs            []publicSurfaceProofRef `yaml:"proof_refs"`
	Notes                string                  `yaml:"notes"`
}

type publicSurfaceProofRef struct {
	Kind      string   `yaml:"kind"`
	Name      string   `yaml:"name,omitempty"`
	Path      string   `yaml:"path,omitempty"`
	Issue     int      `yaml:"issue,omitempty"`
	Watchlist string   `yaml:"watchlist,omitempty"`
	Command   string   `yaml:"command,omitempty"`
	Backends  []string `yaml:"backends,omitempty"`
}

type publicSurfaceMutatingAPIParityEntry struct {
	Method            string                  `yaml:"method"`
	Classification    string                  `yaml:"classification"`
	Backends          []string                `yaml:"backends,omitempty"`
	Scenario          string                  `yaml:"scenario,omitempty"`
	CoveredByMethod   string                  `yaml:"covered_by_method,omitempty"`
	CoveredByScenario string                  `yaml:"covered_by_scenario,omitempty"`
	SpecRef           string                  `yaml:"spec_ref,omitempty"`
	SplitIssue        int                     `yaml:"split_issue,omitempty"`
	UnsupportedIssue  int                     `yaml:"unsupported_issue,omitempty"`
	ProofRefs         []publicSurfaceProofRef `yaml:"proof_refs"`
	Notes             string                  `yaml:"notes,omitempty"`
}

type publicSurfaceOperatorReadAPIParityEntry struct {
	Method           string                  `yaml:"method"`
	Classification   string                  `yaml:"classification"`
	Backends         []string                `yaml:"backends,omitempty"`
	SpecRef          string                  `yaml:"spec_ref,omitempty"`
	SplitIssue       int                     `yaml:"split_issue,omitempty"`
	UnsupportedIssue int                     `yaml:"unsupported_issue,omitempty"`
	ProofRefs        []publicSurfaceProofRef `yaml:"proof_refs"`
	Notes            string                  `yaml:"notes,omitempty"`
}

type publicSurfaceValidationContext struct {
	apiMethods             map[string]struct{}
	apiMethodInfo          map[string]publicSurfaceAPIMethodInfo
	mutatingAPIMethods     map[string]struct{}
	operatorReadAPIMethods map[string]struct{}
	openRPCMethods         map[string]struct{}
	openRPCMatrixMethods   map[string]struct{}
	openRPCMatrixTransport map[string]string
	cliCommands            map[string]struct{}
	goTests                map[string]string
	servedScenarios        map[string]servedparity.Scenario
}

type publicSurfaceSelectedOperatorReadAPIProof struct {
	Methods  []string
	Backends []string
}

type publicSurfaceAPIMethodInfo struct {
	Deprecated  bool
	Description string
}

func TestPublicSurfaceBackendMatrixCoversSelectedBackendRows(t *testing.T) {
	root := repoRoot(t)
	matrix := loadPublicSurfaceBackendMatrix(t, root)
	ctx := newPublicSurfaceValidationContext(t, root)

	if problems := validatePublicSurfaceBackendMatrix(root, matrix, ctx); len(problems) > 0 {
		t.Fatalf("public surface backend matrix validation failed:\n- %s", strings.Join(problems, "\n- "))
	}
}

func TestPublicSurfaceBackendMatrixRejectsStaleReferences(t *testing.T) {
	root := repoRoot(t)
	ctx := newPublicSurfaceValidationContext(t, root)
	tests := []struct {
		name   string
		mutate func(*publicSurfaceBackendMatrix)
		want   string
	}{
		{
			name: "stale api method",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "entity_read_api")
				row.APIMethods = []string{"entity.missing"}
			},
			want: "entity_read_api api_method entity.missing missing from platform method_catalog",
		},
		{
			name: "stale cli command",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "entity_read_cli")
				row.CLICommands = []string{"entity_missing"}
			},
			want: "entity_read_cli cli_command entity_missing missing from platform cli command_catalog",
		},
		{
			name: "stale go test proof",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "event_publish_api")
				row.ProofRefs[0].Name = "TestMissingPublicSurfaceProof"
			},
			want: "event_publish_api go_test proof_ref TestMissingPublicSurfaceProof does not resolve",
		},
		{
			name: "status count cannot revert to stale #1254 split",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "status_run_get_entity_count")
				row.Classification = "split_to_existing_issue"
				row.Tier = "split_open"
				row.SplitIssue = 1254
				row.ProofDimensions = []string{"split_tracker", "openrpc_publication", "cli_v1_path"}
				row.ProofRefs = []publicSurfaceProofRef{
					{Kind: "go_test", Name: "TestStatusUsesDiagnoseAndRunGet"},
					{Kind: "tracker", Issue: 1254, Watchlist: "runtime_operations.runtime_store_backend_default_and_sqlite_portability"},
				}
			},
			want: "status_run_get_entity_count must remain post-#1254 covered proof, not split-open issue #1254",
		},
		{
			name: "status count requires closed proof refs",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "status_run_get_entity_count")
				row.ProofRefs = []publicSurfaceProofRef{
					{Kind: "go_test", Name: "TestStatusUsesDiagnoseAndRunGet"},
				}
			},
			want: "status_run_get_entity_count missing post-#1254 proof_ref TestSQLiteRunAPIReadSurface_LoadListAndDiagnoseEvidence",
		},
		{
			name: "closed #1254 cannot be active tracker",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				matrix.ActiveTrackers = append(matrix.ActiveTrackers, complianceActiveTracker{
					Kind:      "github_issue",
					Issue:     1254,
					Watchlist: "runtime_operations.runtime_store_backend_default_and_sqlite_portability",
				})
			},
			want: "active_trackers must not include closed #1254 runtime_store_backend_default_and_sqlite_portability",
		},
		{
			name: "fail closed row requires executable proof",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "bundle_hash_catalog_boot_postgres_only")
				row.ProofRefs = []publicSurfaceProofRef{
					{Kind: "artifact", Path: "platform-spec.yaml"},
					{Kind: "tracker", Issue: 1239, Watchlist: "runtime_operations.runtime_store_backend_default_and_sqlite_portability"},
				}
			},
			want: "bundle_hash_catalog_boot_postgres_only fail-closed row requires at least one go_test proof_ref",
		},
		{
			name: "api idempotency row requires run.start proof",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "api_idempotency_selected_store")
				row.ProofRefs = publicSurfaceProofRefsExcept(row.ProofRefs, "TestOperatorRunStartHandlersPersistRootEventAndReplayIdempotency")
			},
			want: "api_idempotency_selected_store missing #1402 proof_ref TestOperatorRunStartHandlersPersistRootEventAndReplayIdempotency",
		},
		{
			name: "api idempotency row classification is pinned",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "api_idempotency_selected_store")
				row.Classification = "different_semantic_concept_with_proof"
			},
			want: "api_idempotency_selected_store classification = \"different_semantic_concept_with_proof\", want \"already_covered_by_existing_proof\"",
		},
		{
			name: "api idempotency row keeps both selected backends",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "api_idempotency_selected_store")
				row.Backends = []string{"explicit_postgres"}
			},
			want: "api_idempotency_selected_store backends = [explicit_postgres], want [default_sqlite explicit_postgres]",
		},
		{
			name: "api idempotency row keeps run.start method dimensions",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "api_idempotency_selected_store")
				row.APIMethods = []string{"event.publish"}
				row.OpenRPCMatrixMethods = []string{"event.publish"}
			},
			want: "api_idempotency_selected_store api_methods = [event.publish], want [event.publish run.start]",
		},
		{
			name: "runtime log readback row requires sqlite proof",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "runtime_log_readback_api")
				row.ProofRefs = publicSurfaceProofRefsExcept(row.ProofRefs, "TestSQLiteRuntimeLogPersistenceWritesLoggerRowsForObservability")
			},
			want: "runtime_log_readback_api missing #1402 proof_ref TestSQLiteRuntimeLogPersistenceWritesLoggerRowsForObservability",
		},
		{
			name: "runtime log readback row keeps subscribe logs method",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "runtime_log_readback_api")
				row.APIMethods = publicSurfaceStringsExcept(row.APIMethods, "runtime.subscribe_logs")
				row.OpenRPCMatrixMethods = publicSurfaceStringsExcept(row.OpenRPCMatrixMethods, "runtime.subscribe_logs")
			},
			want: "runtime_log_readback_api api_methods = [run.trace runtime.logs], want [run.trace runtime.logs runtime.subscribe_logs]",
		},
		{
			name: "runtime log readback row keeps selected store dimension",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "runtime_log_readback_api")
				row.ProofDimensions = publicSurfaceStringsExcept(row.ProofDimensions, "selected_store")
			},
			want: "runtime_log_readback_api proof_dimensions = [canonical_store_owner openrpc_publication real_v1_handler], want [canonical_store_owner openrpc_publication real_v1_handler selected_store]",
		},
		{
			name: "served lifecycle row is required",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				matrix.Rows = publicSurfaceMatrixRowsExcept(matrix.Rows, "event_publish_dynamic_auto_emit_served_lifecycle")
			},
			want: "matrix missing required row event_publish_dynamic_auto_emit_served_lifecycle",
		},
		{
			name: "served lifecycle row keeps default sqlite backend",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "event_publish_dynamic_auto_emit_served_lifecycle")
				row.Backends = publicSurfaceStringsExcept(row.Backends, "default_sqlite")
			},
			want: "event_publish_dynamic_auto_emit_served_lifecycle served mutating lifecycle row missing default_sqlite backend",
		},
		{
			name: "served lifecycle row keeps explicit postgres backend",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "event_publish_dynamic_auto_emit_served_lifecycle")
				row.Backends = publicSurfaceStringsExcept(row.Backends, "explicit_postgres")
			},
			want: "event_publish_dynamic_auto_emit_served_lifecycle served mutating lifecycle row missing explicit_postgres backend",
		},
		{
			name: "served lifecycle row keeps runtime startup dimension",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "event_publish_dynamic_auto_emit_served_lifecycle")
				row.ProofDimensions = publicSurfaceStringsExcept(row.ProofDimensions, "real_runtime_startup")
			},
			want: "event_publish_dynamic_auto_emit_served_lifecycle served mutating lifecycle row missing real_runtime_startup proof_dimension",
		},
		{
			name: "served lifecycle row keeps real v1 handler dimension",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "event_publish_dynamic_auto_emit_served_lifecycle")
				row.ProofDimensions = publicSurfaceStringsExcept(row.ProofDimensions, "real_v1_handler")
			},
			want: "event_publish_dynamic_auto_emit_served_lifecycle served mutating lifecycle row missing real_v1_handler proof_dimension",
		},
		{
			name: "served lifecycle row keeps selected store dimension",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "event_publish_dynamic_auto_emit_served_lifecycle")
				row.ProofDimensions = publicSurfaceStringsExcept(row.ProofDimensions, "selected_store")
			},
			want: "event_publish_dynamic_auto_emit_served_lifecycle served mutating lifecycle row missing selected_store proof_dimension",
		},
		{
			name: "served lifecycle row keeps canonical store owner dimension",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "event_publish_dynamic_auto_emit_served_lifecycle")
				row.ProofDimensions = publicSurfaceStringsExcept(row.ProofDimensions, "canonical_store_owner")
			},
			want: "event_publish_dynamic_auto_emit_served_lifecycle served mutating lifecycle row missing canonical_store_owner proof_dimension",
		},
		{
			name: "served lifecycle row keeps openrpc publication dimension",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "event_publish_dynamic_auto_emit_served_lifecycle")
				row.ProofDimensions = publicSurfaceStringsExcept(row.ProofDimensions, "openrpc_publication")
			},
			want: "event_publish_dynamic_auto_emit_served_lifecycle served mutating lifecycle row missing openrpc_publication proof_dimension",
		},
		{
			name: "served lifecycle row keeps mutating api method",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "event_publish_dynamic_auto_emit_served_lifecycle")
				row.APIMethods = nil
			},
			want: "event_publish_dynamic_auto_emit_served_lifecycle served mutating lifecycle row missing mutating lifecycle api_method",
		},
		{
			name: "served lifecycle row keeps matching openrpc matrix method",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "event_publish_dynamic_auto_emit_served_lifecycle")
				row.OpenRPCMatrixMethods = nil
			},
			want: "event_publish_dynamic_auto_emit_served_lifecycle served mutating lifecycle row api_method event.publish missing from openrpc_matrix_methods",
		},
		{
			name: "served lifecycle row keeps sqlite served proof",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "event_publish_dynamic_auto_emit_served_lifecycle")
				row.ProofRefs = publicSurfaceProofRefsExcept(row.ProofRefs, "TestRunServeRuntimeEventPublishDynamicAutoEmitServedPathDefaultSQLite")
			},
			want: "event_publish_dynamic_auto_emit_served_lifecycle missing default SQLite served-runtime go_test proof_ref for api_method event.publish",
		},
		{
			name: "served lifecycle row keeps postgres served proof",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "event_publish_dynamic_auto_emit_served_lifecycle")
				row.ProofRefs = publicSurfaceProofRefsExcept(row.ProofRefs, "TestRunServeRuntimeEventPublishDynamicAutoEmitServedPathPostgres")
			},
			want: "event_publish_dynamic_auto_emit_served_lifecycle missing explicit Postgres served-runtime go_test proof_ref for api_method event.publish",
		},
		{
			name: "served lifecycle row rejects unrelated serve boot proofs",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "event_publish_dynamic_auto_emit_served_lifecycle")
				row.ProofRefs = []publicSurfaceProofRef{
					{Kind: "go_test", Name: "TestRunServeRuntimeFreshEmptySQLiteBootsWithDevAbandon"},
					{Kind: "go_test", Name: "TestRunServeRuntimeFreshEmptyPostgresBootstrapsSchemaBeforeDevAbandon"},
				}
			},
			want: "event_publish_dynamic_auto_emit_served_lifecycle missing default SQLite served-runtime go_test proof_ref for api_method event.publish",
		},
		{
			name: "dynamic served lifecycle row rejects existing run proof substitution",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "event_publish_dynamic_auto_emit_served_lifecycle")
				row.ProofRefs = []publicSurfaceProofRef{
					{Kind: "go_test", Name: "TestRunServeRuntimeEventPublishRunIDFollowUpServedPathDefaultSQLite"},
					{Kind: "go_test", Name: "TestRunServeRuntimeEventPublishRunIDFollowUpServedPathPostgres"},
				}
			},
			want: "event_publish_dynamic_auto_emit_served_lifecycle missing required go_test proof_ref TestRunServeRuntimeEventPublishDynamicAutoEmitServedPathDefaultSQLite",
		},
		{
			name: "existing run served lifecycle row rejects dynamic proof substitution",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := publicSurfaceMatrixRowByID(t, matrix, "event_publish_existing_run_followup_served_path")
				row.ProofRefs = []publicSurfaceProofRef{
					{Kind: "go_test", Name: "TestRunServeRuntimeEventPublishDynamicAutoEmitServedPathDefaultSQLite"},
					{Kind: "go_test", Name: "TestRunServeRuntimeEventPublishDynamicAutoEmitServedPathPostgres"},
				}
			},
			want: "event_publish_existing_run_followup_served_path missing required go_test proof_ref TestRunServeRuntimeEventPublishRunIDFollowUpServedPathDefaultSQLite",
		},
		{
			name: "future served lifecycle row must opt into guarded class",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				row := *publicSurfaceMatrixRowByID(t, matrix, "event_publish_dynamic_auto_emit_served_lifecycle")
				row.ID = "future_served_event_publish_lifecycle_row"
				row.ProofDimensions = publicSurfaceStringsExcept(append([]string(nil), row.ProofDimensions...), "served_mutating_lifecycle")
				matrix.Rows = append(matrix.Rows, row)
			},
			want: "future_served_event_publish_lifecycle_row served mutating lifecycle row missing served_mutating_lifecycle proof_dimension",
		},
		{
			name: "mutating api ledger rejects unclassified method",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				matrix.MutatingLedger = publicSurfaceMutatingLedgerExcept(matrix.MutatingLedger, "bundle.register")
			},
			want: "mutating api parity ledger missing method bundle.register",
		},
		{
			name: "mutating api ledger rejects stale spec ref",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				entry := publicSurfaceMutatingLedgerEntryByMethod(t, matrix, "bundle.register")
				entry.Classification = "postgres_only_with_spec_ref"
				entry.Backends = []string{"postgres_only"}
				entry.SpecRef = "platform-spec.yaml#api_specification.missing_anchor"
			},
			want: "ledger method bundle.register spec_ref platform-spec.yaml#api_specification.missing_anchor does not resolve",
		},
		{
			name: "mutating api ledger rejects broad postgres only publication ref",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				entry := publicSurfaceMutatingLedgerEntryByMethod(t, matrix, "bundle.register")
				entry.Classification = "postgres_only_with_spec_ref"
				entry.Backends = []string{"postgres_only"}
				entry.SpecRef = "platform-spec.yaml#api_specification.method_catalog"
				entry.ProofRefs = []publicSurfaceProofRef{
					{Kind: "artifact", Path: "platform-spec.yaml"},
				}
			},
			want: "ledger method bundle.register postgres_only_with_spec_ref spec_ref platform-spec.yaml#api_specification.method_catalog is publication-only, not backend-support authority",
		},
		{
			name: "mutating api ledger rejects stale issue ref",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				entry := publicSurfaceMutatingLedgerEntryByMethod(t, matrix, "event.replay")
				entry.Classification = "split_with_issue_ref"
				entry.Backends = nil
				entry.Scenario = ""
				entry.SplitIssue = 999999
				entry.ProofRefs = []publicSurfaceProofRef{
					{Kind: "tracker", Issue: 999999, Watchlist: "runtime_operations.runtime_store_backend_default_and_sqlite_portability"},
				}
			},
			want: "ledger method event.replay split issue #999999 is not active",
		},
		{
			name: "mutating api ledger rejects non-harness dual proof",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				entry := publicSurfaceMutatingLedgerEntryByMethod(t, matrix, "event.publish")
				entry.Scenario = ""
				entry.ProofRefs = []publicSurfaceProofRef{
					{Kind: "go_test", Name: "TestRunServeRuntimeEventPublishDynamicAutoEmitServedPathDefaultSQLite"},
					{Kind: "go_test", Name: "TestRunServeRuntimeEventPublishDynamicAutoEmitServedPathPostgres"},
				}
			},
			want: "ledger method event.publish dual_backend_served_proof must reference served parity harness scenario",
		},
		{
			name: "mutating api ledger rejects one backend dual proof",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				entry := publicSurfaceMutatingLedgerEntryByMethod(t, matrix, "event.publish")
				entry.Backends = []string{"default_sqlite"}
			},
			want: "ledger method event.publish dual_backend_served_proof backends = [default_sqlite], want [default_sqlite explicit_postgres]",
		},
		{
			name: "operator-read api ledger rejects unclassified method",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				matrix.OperatorReadLedger = publicSurfaceOperatorReadLedgerExcept(matrix.OperatorReadLedger, "agent.list")
			},
			want: "operator-read api parity ledger missing method agent.list",
		},
		{
			name: "operator-read api ledger rejects stale split issue ref",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				entry := publicSurfaceOperatorReadLedgerEntryByMethod(t, matrix, "conversation.fork_list")
				entry.SplitIssue = 999999
				entry.ProofRefs = []publicSurfaceProofRef{
					{Kind: "tracker", Issue: 999999, Watchlist: "runtime_operations.runtime_store_backend_default_and_sqlite_portability"},
				}
			},
			want: "operator-read ledger method conversation.fork_list split issue #999999 is not active",
		},
		{
			name: "operator-read api ledger rejects stale spec ref",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				entry := publicSurfaceOperatorReadLedgerEntryByMethod(t, matrix, "agent.list")
				entry.Classification = "postgres_only_with_spec_ref"
				entry.Backends = []string{"postgres_only"}
				entry.SpecRef = "platform-spec.yaml#api_specification.missing_operator_read_anchor"
			},
			want: "operator-read ledger method agent.list spec_ref platform-spec.yaml#api_specification.missing_operator_read_anchor does not resolve",
		},
		{
			name: "operator-read api ledger rejects broad postgres only publication ref",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				entry := publicSurfaceOperatorReadLedgerEntryByMethod(t, matrix, "agent.list")
				entry.Classification = "postgres_only_with_spec_ref"
				entry.Backends = []string{"postgres_only"}
				entry.SpecRef = "platform-spec.yaml#api_specification.method_catalog"
				entry.ProofRefs = []publicSurfaceProofRef{
					{Kind: "artifact", Path: "platform-spec.yaml"},
				}
			},
			want: "operator-read ledger method agent.list postgres_only_with_spec_ref spec_ref platform-spec.yaml#api_specification.method_catalog is publication-only, not backend-support authority",
		},
		{
			name: "operator-read api ledger rejects fake-store-only dual proof",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				entry := publicSurfaceOperatorReadLedgerEntryByMethod(t, matrix, "agent.list")
				entry.ProofRefs = []publicSurfaceProofRef{
					{Kind: "go_test", Name: "TestOpenRPCReadOnlyHTTPRuntimeProbes", Backends: []string{"default_sqlite", "explicit_postgres"}},
				}
			},
			want: "operator-read ledger method agent.list dual_backend_api_proof missing default_sqlite backend-scoped selected API proof_ref",
		},
		{
			name: "operator-read api ledger rejects missing postgres-scoped dual proof",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				entry := publicSurfaceOperatorReadLedgerEntryByMethod(t, matrix, "agent.list")
				entry.ProofRefs = []publicSurfaceProofRef{
					{Kind: "go_test", Name: "TestSQLiteAgentConversationOwnerBacksSupportedAPISurface", Backends: []string{"default_sqlite"}},
					{Kind: "go_test", Name: "TestOperatorAgentConversationHandlersExposeReadOwner"},
				}
			},
			want: "operator-read ledger method agent.list dual_backend_api_proof missing explicit_postgres backend-scoped selected API proof_ref",
		},
		{
			name: "operator-read api ledger rejects backend-labeled store-only dual proof",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				entry := publicSurfaceOperatorReadLedgerEntryByMethod(t, matrix, "runtime.logs")
				entry.ProofRefs = []publicSurfaceProofRef{
					{Kind: "go_test", Name: "TestSQLiteRuntimeLogPersistenceWritesLoggerRowsForObservability", Backends: []string{"default_sqlite"}},
					{Kind: "go_test", Name: "TestPostgresRuntimeLogPersistencePreservesRunSourceAndLineage", Backends: []string{"explicit_postgres"}},
				}
			},
			want: "operator-read ledger method runtime.logs proof_ref TestSQLiteRuntimeLogPersistenceWritesLoggerRowsForObservability scoped to default_sqlite is not a registered selected API proof for method runtime.logs",
		},
		{
			name: "operator-read api ledger rejects one backend dual proof",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				entry := publicSurfaceOperatorReadLedgerEntryByMethod(t, matrix, "agent.list")
				entry.Backends = []string{"default_sqlite"}
			},
			want: "operator-read ledger method agent.list dual_backend_api_proof backends = [default_sqlite], want [default_sqlite explicit_postgres]",
		},
		{
			name: "mutating api ledger rejects transitive coverage without covered method",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				entry := publicSurfaceMutatingLedgerEntryByMethod(t, matrix, "run.start")
				entry.Classification = "covered_transitively"
				entry.Backends = []string{"default_sqlite", "explicit_postgres"}
				entry.CoveredByMethod = ""
				entry.CoveredByScenario = "event_publish_dynamic_auto_emit_lifecycle"
				entry.ProofRefs = []publicSurfaceProofRef{
					{Kind: "go_test", Name: "TestServedParityHarnessEventPublishDynamicAutoEmitLifecycle"},
				}
			},
			want: "ledger method run.start covered_transitively missing covered_by_method",
		},
		{
			name: "mutating api ledger rejects transitive coverage without scenario",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				entry := publicSurfaceMutatingLedgerEntryByMethod(t, matrix, "run.start")
				entry.Classification = "covered_transitively"
				entry.Backends = []string{"default_sqlite", "explicit_postgres"}
				entry.CoveredByMethod = "event.publish"
				entry.CoveredByScenario = ""
				entry.ProofRefs = []publicSurfaceProofRef{
					{Kind: "go_test", Name: "TestServedParityHarnessEventPublishDynamicAutoEmitLifecycle"},
				}
			},
			want: "ledger method run.start covered_transitively missing covered_by_scenario",
		},
		{
			name: "mutating api ledger rejects transitive coverage over split method",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				covered := publicSurfaceMutatingLedgerEntryByMethod(t, matrix, "event.replay")
				covered.Classification = "split_with_issue_ref"
				covered.Backends = nil
				covered.Scenario = ""
				covered.SplitIssue = 1927
				covered.ProofRefs = []publicSurfaceProofRef{
					{Kind: "tracker", Issue: 1927, Watchlist: "runtime_operations.shutdown_and_runtime_lifecycle"},
				}

				entry := publicSurfaceMutatingLedgerEntryByMethod(t, matrix, "run.start")
				entry.Classification = "covered_transitively"
				entry.Backends = []string{"default_sqlite", "explicit_postgres"}
				entry.CoveredByMethod = "event.replay"
				entry.CoveredByScenario = "event_replay_live_agent_lifecycle"
				entry.ProofRefs = []publicSurfaceProofRef{
					{Kind: "go_test", Name: "TestServedParityHarnessLiveAgentEventReplayLifecycle"},
				}
			},
			want: "ledger method run.start covered_by_method event.replay classification = \"split_with_issue_ref\", want dual_backend_served_proof",
		},
		{
			name: "mutating api ledger rejects transitive coverage with wrong scenario method",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				entry := publicSurfaceMutatingLedgerEntryByMethod(t, matrix, "run.start")
				entry.Classification = "covered_transitively"
				entry.Backends = []string{"default_sqlite", "explicit_postgres"}
				entry.CoveredByMethod = "run.start"
				entry.CoveredByScenario = "event_publish_dynamic_auto_emit_lifecycle"
				entry.ProofRefs = []publicSurfaceProofRef{
					{Kind: "go_test", Name: "TestServedParityHarnessEventPublishDynamicAutoEmitLifecycle"},
				}
			},
			want: "ledger method run.start covered_by_method must differ from method",
		},
		{
			name: "mutating api ledger rejects transitive coverage for non-deprecated mutator",
			mutate: func(matrix *publicSurfaceBackendMatrix) {
				entry := publicSurfaceMutatingLedgerEntryByMethod(t, matrix, "run.stop")
				entry.Classification = "covered_transitively"
				entry.Backends = []string{"default_sqlite", "explicit_postgres"}
				entry.CoveredByMethod = "event.publish"
				entry.CoveredByScenario = "event_publish_dynamic_auto_emit_lifecycle"
				entry.SplitIssue = 0
				entry.ProofRefs = []publicSurfaceProofRef{
					{Kind: "go_test", Name: "TestServedParityHarnessEventPublishDynamicAutoEmitLifecycle"},
				}
				entry.Notes = "Deprecated wrapper over event.publish."
			},
			want: "ledger method run.stop covered_transitively requires deprecated method_catalog entry",
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

func loadPublicSurfaceBackendMatrix(t *testing.T, root string) publicSurfaceBackendMatrix {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, publicSurfaceBackendMatrixPath))
	if err != nil {
		t.Fatalf("read public surface backend matrix: %v", err)
	}
	var matrix publicSurfaceBackendMatrix
	if err := yaml.Unmarshal(raw, &matrix); err != nil {
		t.Fatalf("parse public surface backend matrix: %v", err)
	}
	return matrix
}

func newPublicSurfaceValidationContext(t *testing.T, root string) publicSurfaceValidationContext {
	t.Helper()
	api := loadComplianceAPISpec(t, root)
	doc, _ := loadComplianceOpenRPC(t, complianceOpenRPCPath(root))
	openRPCMatrix := loadComplianceMatrix(t, filepath.Join(root, "internal", "apiv1", "testdata", "openrpc_compliance_matrix.yaml"))
	goTests, err := loadPublicSurfaceGoTests(root)
	if err != nil {
		t.Fatalf("load public surface go test symbols: %v", err)
	}

	apiMethods := map[string]struct{}{}
	apiMethodInfo := map[string]publicSurfaceAPIMethodInfo{}
	for name, method := range api.MethodCatalog {
		apiMethods[name] = struct{}{}
		apiMethodInfo[name] = publicSurfaceAPIMethodInfo{
			Deprecated:  method.Deprecated,
			Description: method.Description,
		}
	}
	mutatingAPIMethods := map[string]struct{}{}
	for _, method := range api.Conventions.Idempotency.MutatingMethods {
		mutatingAPIMethods[strings.TrimSpace(method)] = struct{}{}
	}
	operatorReadAPIMethods := map[string]struct{}{}
	for method := range apiMethods {
		if _, mutating := mutatingAPIMethods[method]; mutating {
			continue
		}
		operatorReadAPIMethods[method] = struct{}{}
	}
	openRPCMethods := map[string]struct{}{}
	for _, method := range doc.Methods {
		openRPCMethods[strings.TrimSpace(method.Name)] = struct{}{}
	}
	openRPCMatrixMethods := map[string]struct{}{}
	openRPCMatrixTransport := map[string]string{}
	for _, row := range openRPCMatrix.Methods {
		method := strings.TrimSpace(row.Method)
		openRPCMatrixMethods[method] = struct{}{}
		openRPCMatrixTransport[method] = strings.TrimSpace(row.Transport)
	}

	return publicSurfaceValidationContext{
		apiMethods:             apiMethods,
		apiMethodInfo:          apiMethodInfo,
		mutatingAPIMethods:     mutatingAPIMethods,
		operatorReadAPIMethods: operatorReadAPIMethods,
		openRPCMethods:         openRPCMethods,
		openRPCMatrixMethods:   openRPCMatrixMethods,
		openRPCMatrixTransport: openRPCMatrixTransport,
		cliCommands:            loadPublicSurfaceCLICommands(t, root),
		goTests:                goTests,
		servedScenarios:        loadPublicSurfaceServedParityScenarios(),
	}
}

func loadPublicSurfaceServedParityScenarios() map[string]servedparity.Scenario {
	out := map[string]servedparity.Scenario{}
	for _, scenario := range servedparity.Scenarios() {
		out[scenario.ID] = scenario
	}
	return out
}

func validatePublicSurfaceBackendMatrix(root string, matrix publicSurfaceBackendMatrix, ctx publicSurfaceValidationContext) []string {
	var problems []string
	if matrix.Version != 1 {
		problems = append(problems, fmt.Sprintf("matrix version = %d, want 1", matrix.Version))
	}
	if matrix.Kind != "public_surface_backend_matrix" {
		problems = append(problems, fmt.Sprintf("matrix kind = %q, want public_surface_backend_matrix", matrix.Kind))
	}
	if matrix.Issue != 1268 {
		problems = append(problems, fmt.Sprintf("matrix issue = %d, want 1268", matrix.Issue))
	}
	if matrix.IssueRole != "canonical_owner" {
		problems = append(problems, fmt.Sprintf("matrix issue_role = %q, want canonical_owner", matrix.IssueRole))
	}
	problems = append(problems, validatePublicSurfaceSources(root, matrix.Source)...)
	problems = append(problems, validatePublicSurfacePolicy(matrix.Policy)...)

	activeTrackers := map[string]struct{}{}
	for _, tracker := range matrix.ActiveTrackers {
		kind := strings.TrimSpace(tracker.Kind)
		switch kind {
		case "github_issue":
			if tracker.Issue == 0 {
				problems = append(problems, "active github_issue tracker missing issue")
			}
		case "watchlist":
			if tracker.Issue != 0 {
				problems = append(problems, fmt.Sprintf("watchlist active tracker issue = %d, want 0", tracker.Issue))
			}
		default:
			problems = append(problems, fmt.Sprintf("active tracker issue #%d kind = %q is not allowed", tracker.Issue, tracker.Kind))
		}
		if strings.TrimSpace(tracker.Watchlist) == "" {
			problems = append(problems, fmt.Sprintf("active tracker issue #%d missing watchlist", tracker.Issue))
		}
		activeTrackers[trackerKey(tracker.Issue, tracker.Watchlist)] = struct{}{}
	}
	if _, ok := activeTrackers[trackerKey(1239, "runtime_operations.runtime_store_backend_default_and_sqlite_portability")]; !ok {
		problems = append(problems, "active_trackers missing #1239 runtime_store_backend_default_and_sqlite_portability")
	}
	if _, ok := activeTrackers[trackerKey(1254, "runtime_operations.runtime_store_backend_default_and_sqlite_portability")]; ok {
		problems = append(problems, "active_trackers must not include closed #1254 runtime_store_backend_default_and_sqlite_portability")
	}
	if _, ok := activeTrackers[trackerKey(1255, "runtime_operations.runtime_store_backend_default_and_sqlite_portability")]; ok {
		problems = append(problems, "active_trackers must not include closed #1255 runtime_store_backend_default_and_sqlite_portability")
	}
	if _, ok := activeTrackers[trackerKey(1783, "runtime_operations.runtime_store_backend_default_and_sqlite_portability")]; ok {
		problems = append(problems, "active_trackers must not include closed #1783 runtime_store_backend_default_and_sqlite_portability")
	}
	if _, ok := activeTrackers[trackerKey(1864, "runtime_operations.runtime_store_backend_default_and_sqlite_portability")]; ok {
		problems = append(problems, "active_trackers must not include closed #1864 runtime_store_backend_default_and_sqlite_portability")
	}
	if _, ok := activeTrackers[trackerKey(1386, "runtime_operations.runtime_store_backend_default_and_sqlite_portability")]; ok {
		problems = append(problems, "active_trackers must not include closed #1386 runtime_store_backend_default_and_sqlite_portability")
	}
	if _, ok := activeTrackers[trackerKey(1927, "runtime_operations.shutdown_and_runtime_lifecycle")]; !ok {
		problems = append(problems, "active_trackers missing #1927 shutdown_and_runtime_lifecycle")
	}
	if _, ok := activeTrackers[trackerKey(1932, "runtime_operations.atomic_runtime_state_mutation")]; ok {
		problems = append(problems, "active_trackers must not include closure-bearing #1932 atomic_runtime_state_mutation")
	}
	if _, ok := activeTrackers[trackerKey(0, "operator_surfaces.v1_openrpc_api_conformance")]; !ok {
		problems = append(problems, "active_trackers missing operator_surfaces.v1_openrpc_api_conformance watchlist")
	}
	problems = append(problems, validatePublicSurfaceMutatingAPIParityLedger(root, matrix.MutatingLedger, ctx, activeTrackers)...)
	problems = append(problems, validatePublicSurfaceOperatorReadAPIParityLedger(root, matrix.OperatorReadLedger, ctx, activeTrackers)...)

	rowsByID := map[string]publicSurfaceMatrixRow{}
	requiredRows := requiredPublicSurfaceRows()
	defaultSQLiteSmoke := false
	explicitPostgresSmoke := false
	for _, row := range matrix.Rows {
		id := strings.TrimSpace(row.ID)
		if id == "" {
			problems = append(problems, "matrix row missing id")
			continue
		}
		if _, exists := rowsByID[id]; exists {
			problems = append(problems, fmt.Sprintf("matrix row %s appears more than once", id))
		}
		rowsByID[id] = row
		if row.Tier == "required_smoke" && publicSurfaceHasValue(row.Backends, "default_sqlite") {
			defaultSQLiteSmoke = true
		}
		if row.Tier == "required_smoke" && publicSurfaceHasValue(row.Backends, "explicit_postgres") {
			explicitPostgresSmoke = true
		}
		problems = append(problems, validatePublicSurfaceRow(root, row, ctx, activeTrackers)...)
	}
	for rowID := range requiredRows {
		if _, ok := rowsByID[rowID]; !ok {
			problems = append(problems, fmt.Sprintf("matrix missing required row %s", rowID))
		}
	}
	if !defaultSQLiteSmoke {
		problems = append(problems, "matrix missing required_smoke default_sqlite row")
	}
	if !explicitPostgresSmoke {
		problems = append(problems, "matrix missing required_smoke explicit_postgres row")
	}
	if row, ok := rowsByID["status_run_get_entity_count"]; ok {
		if row.Classification != "already_covered_by_existing_proof" || row.SplitIssue != 0 || row.Tier != "required_smoke" {
			problems = append(problems, "status_run_get_entity_count must remain post-#1254 covered proof, not split-open issue #1254")
		}
		for _, proof := range []string{
			"TestSQLiteRunAPIReadSurface_LoadListAndDiagnoseEvidence",
			"TestRunAPIReadSurface_LoadAndListRunHeaders",
			"TestOpenRPCReadOnlyHTTPRuntimeProbes",
			"TestStatusUsesDiagnoseAndRunGet",
		} {
			if !publicSurfaceHasGoTestProof(row.ProofRefs, proof) {
				problems = append(problems, fmt.Sprintf("status_run_get_entity_count missing post-#1254 proof_ref %s", proof))
			}
		}
	}
	if row, ok := rowsByID["api_idempotency_selected_store"]; ok {
		for _, proof := range []string{
			"TestOperatorRunStartHandlersPersistRootEventAndReplayIdempotency",
			"TestOperatorEventPublishSQLiteIdempotentFirstEventPublishesWithoutLock",
			"TestOperatorEventPublishHandlersPersistEventReportDeliveriesAndReplayIdempotency",
		} {
			if !publicSurfaceHasGoTestProof(row.ProofRefs, proof) {
				problems = append(problems, fmt.Sprintf("api_idempotency_selected_store missing #1402 proof_ref %s", proof))
			}
		}
	}
	if row, ok := rowsByID["runtime_log_readback_api"]; ok {
		for _, proof := range []string{
			"TestSQLiteRuntimeLogPersistenceWritesLoggerRowsForObservability",
			"TestPostgresRuntimeLogPersistencePreservesRunSourceAndLineage",
			"TestOpenRPCWebSocketRuntimeProbes",
			"TestHandlerWebSocketRuntimeSubscribeLogsUsesOwnerFiltersAndReplay",
			"TestHandlerWebSocketRunSubscribeTraceUsesOwnerReplayAndRunNotFound",
		} {
			if !publicSurfaceHasGoTestProof(row.ProofRefs, proof) {
				problems = append(problems, fmt.Sprintf("runtime_log_readback_api missing #1402 proof_ref %s", proof))
			}
		}
	}
	problems = append(problems, validatePublicSurfaceServedMutatingLifecycleRows(rowsByID, activeTrackers, ctx)...)
	problems = append(problems, validatePublicSurfaceExpectedRowShapes(rowsByID)...)
	sort.Strings(problems)
	return problems
}

type publicSurfaceExpectedRowShape struct {
	Classification       string
	Tier                 string
	Backends             []string
	APIMethods           []string
	OpenRPCMatrixMethods []string
	ProofDimensions      []string
	GoTestProofRefs      []string
}

func validatePublicSurfaceExpectedRowShapes(rowsByID map[string]publicSurfaceMatrixRow) []string {
	var problems []string
	for id, want := range expectedPublicSurfaceRowShapes() {
		row, ok := rowsByID[id]
		if !ok {
			continue
		}
		if row.Classification != want.Classification {
			problems = append(problems, fmt.Sprintf("%s classification = %q, want %q", id, row.Classification, want.Classification))
		}
		if row.Tier != want.Tier {
			problems = append(problems, fmt.Sprintf("%s tier = %q, want %q", id, row.Tier, want.Tier))
		}
		if !publicSurfaceSameStringSet(row.Backends, want.Backends) {
			problems = append(problems, fmt.Sprintf("%s backends = %v, want %v", id, publicSurfaceSortedStrings(row.Backends), publicSurfaceSortedStrings(want.Backends)))
		}
		if !publicSurfaceSameStringSet(row.APIMethods, want.APIMethods) {
			problems = append(problems, fmt.Sprintf("%s api_methods = %v, want %v", id, publicSurfaceSortedStrings(row.APIMethods), publicSurfaceSortedStrings(want.APIMethods)))
		}
		if !publicSurfaceSameStringSet(row.OpenRPCMatrixMethods, want.OpenRPCMatrixMethods) {
			problems = append(problems, fmt.Sprintf("%s openrpc_matrix_methods = %v, want %v", id, publicSurfaceSortedStrings(row.OpenRPCMatrixMethods), publicSurfaceSortedStrings(want.OpenRPCMatrixMethods)))
		}
		if !publicSurfaceSameStringSet(row.ProofDimensions, want.ProofDimensions) {
			problems = append(problems, fmt.Sprintf("%s proof_dimensions = %v, want %v", id, publicSurfaceSortedStrings(row.ProofDimensions), publicSurfaceSortedStrings(want.ProofDimensions)))
		}
		for _, proof := range want.GoTestProofRefs {
			if !publicSurfaceHasGoTestProof(row.ProofRefs, proof) {
				problems = append(problems, fmt.Sprintf("%s missing required go_test proof_ref %s", id, proof))
			}
		}
	}
	return problems
}

func validatePublicSurfaceMutatingAPIParityLedger(root string, entries []publicSurfaceMutatingAPIParityEntry, ctx publicSurfaceValidationContext, activeTrackers map[string]struct{}) []string {
	var problems []string
	if len(entries) == 0 {
		return []string{"mutating api parity ledger is required"}
	}
	seen := map[string]struct{}{}
	for _, entry := range entries {
		method := strings.TrimSpace(entry.Method)
		label := fmt.Sprintf("ledger method %s", method)
		if method == "" {
			problems = append(problems, "mutating api parity ledger entry missing method")
			continue
		}
		if _, exists := seen[method]; exists {
			problems = append(problems, fmt.Sprintf("%s appears more than once", label))
		}
		seen[method] = struct{}{}
		if _, ok := ctx.mutatingAPIMethods[method]; !ok {
			problems = append(problems, fmt.Sprintf("%s is not declared in platform idempotency mutating_methods", label))
		}
		if _, ok := ctx.apiMethods[method]; !ok {
			problems = append(problems, fmt.Sprintf("%s missing from platform method_catalog", label))
		}
		if _, ok := ctx.openRPCMethods[method]; !ok {
			problems = append(problems, fmt.Sprintf("%s missing from generated openrpc.json", label))
		}
		if len(entry.ProofRefs) == 0 {
			problems = append(problems, fmt.Sprintf("%s missing proof_refs", label))
		}
		problems = append(problems, validatePublicSurfaceMutatingLedgerProofRefs(root, label, entry.ProofRefs, ctx, activeTrackers)...)
		if _, ok := allowedPublicSurfaceMutatingLedgerClassifications()[entry.Classification]; !ok {
			problems = append(problems, fmt.Sprintf("%s classification %q is not allowed", label, entry.Classification))
			continue
		}
		switch entry.Classification {
		case "dual_backend_served_proof":
			problems = append(problems, validatePublicSurfaceMutatingLedgerDualProof(label, entry, ctx)...)
		case "covered_transitively":
			problems = append(problems, validatePublicSurfaceMutatingLedgerTransitiveCoverage(label, entry, entries, ctx)...)
		case "postgres_only_with_spec_ref":
			if !publicSurfaceSameStringSet(entry.Backends, []string{"postgres_only"}) {
				problems = append(problems, fmt.Sprintf("%s postgres_only_with_spec_ref backends = %v, want [postgres_only]", label, publicSurfaceSortedStrings(entry.Backends)))
			}
			if strings.TrimSpace(entry.SpecRef) == "" {
				problems = append(problems, fmt.Sprintf("%s postgres_only_with_spec_ref missing spec_ref", label))
			} else if err := publicSurfaceSpecRefExists(root, entry.SpecRef); err != nil {
				problems = append(problems, fmt.Sprintf("%s spec_ref %s does not resolve: %v", label, entry.SpecRef, err))
			} else if publicSurfaceSpecRefIsPublicationOnly(entry.SpecRef) {
				problems = append(problems, fmt.Sprintf("%s postgres_only_with_spec_ref spec_ref %s is publication-only, not backend-support authority", label, entry.SpecRef))
			}
		case "split_with_issue_ref":
			if entry.SplitIssue == 0 {
				problems = append(problems, fmt.Sprintf("%s split_with_issue_ref missing split_issue", label))
			} else if !publicSurfaceLedgerHasTrackerIssue(entry.ProofRefs, entry.SplitIssue, activeTrackers) {
				problems = append(problems, fmt.Sprintf("%s split issue #%d is not active", label, entry.SplitIssue))
			}
		case "unsupported_with_issue_ref":
			if entry.UnsupportedIssue == 0 {
				problems = append(problems, fmt.Sprintf("%s unsupported_with_issue_ref missing unsupported_issue", label))
			} else if !publicSurfaceLedgerHasTrackerIssue(entry.ProofRefs, entry.UnsupportedIssue, activeTrackers) {
				problems = append(problems, fmt.Sprintf("%s unsupported issue #%d is not active", label, entry.UnsupportedIssue))
			}
		}
	}
	for method := range ctx.mutatingAPIMethods {
		if _, ok := seen[method]; !ok {
			problems = append(problems, fmt.Sprintf("mutating api parity ledger missing method %s", method))
		}
	}
	return problems
}

func validatePublicSurfaceOperatorReadAPIParityLedger(root string, entries []publicSurfaceOperatorReadAPIParityEntry, ctx publicSurfaceValidationContext, activeTrackers map[string]struct{}) []string {
	var problems []string
	if len(entries) == 0 {
		return []string{"operator-read api parity ledger is required"}
	}
	seen := map[string]struct{}{}
	for _, entry := range entries {
		method := strings.TrimSpace(entry.Method)
		label := fmt.Sprintf("operator-read ledger method %s", method)
		if method == "" {
			problems = append(problems, "operator-read api parity ledger entry missing method")
			continue
		}
		if _, exists := seen[method]; exists {
			problems = append(problems, fmt.Sprintf("%s appears more than once", label))
		}
		seen[method] = struct{}{}
		if _, ok := ctx.operatorReadAPIMethods[method]; !ok {
			problems = append(problems, fmt.Sprintf("%s is not a non-mutating platform method_catalog method", label))
		}
		if _, ok := ctx.apiMethods[method]; !ok {
			problems = append(problems, fmt.Sprintf("%s missing from platform method_catalog", label))
		}
		if _, ok := ctx.openRPCMethods[method]; !ok {
			problems = append(problems, fmt.Sprintf("%s missing from generated openrpc.json", label))
		}
		if len(entry.ProofRefs) == 0 {
			problems = append(problems, fmt.Sprintf("%s missing proof_refs", label))
		}
		problems = append(problems, validatePublicSurfaceMutatingLedgerProofRefs(root, label, entry.ProofRefs, ctx, activeTrackers)...)
		if _, ok := allowedPublicSurfaceOperatorReadLedgerClassifications()[entry.Classification]; !ok {
			problems = append(problems, fmt.Sprintf("%s classification %q is not allowed", label, entry.Classification))
			continue
		}
		switch entry.Classification {
		case "dual_backend_api_proof":
			problems = append(problems, validatePublicSurfaceOperatorReadLedgerDualProof(label, entry)...)
		case "different_semantic_concept_with_proof":
			if !publicSurfaceOperatorReadLedgerHasGoTestProof(entry.ProofRefs) {
				problems = append(problems, fmt.Sprintf("%s different_semantic_concept_with_proof requires executable go_test proof_ref", label))
			}
			if transport := ctx.openRPCMatrixTransport[method]; transport == "http" && !strings.Contains(strings.ToLower(entry.Notes), "non-store") && !strings.Contains(strings.ToLower(entry.Notes), "static") {
				problems = append(problems, fmt.Sprintf("%s different_semantic_concept_with_proof over HTTP method requires notes naming non-store/static reason", label))
			}
		case "postgres_only_with_spec_ref":
			if !publicSurfaceSameStringSet(entry.Backends, []string{"postgres_only"}) {
				problems = append(problems, fmt.Sprintf("%s postgres_only_with_spec_ref backends = %v, want [postgres_only]", label, publicSurfaceSortedStrings(entry.Backends)))
			}
			if strings.TrimSpace(entry.SpecRef) == "" {
				problems = append(problems, fmt.Sprintf("%s postgres_only_with_spec_ref missing spec_ref", label))
			} else if err := publicSurfaceSpecRefExists(root, entry.SpecRef); err != nil {
				problems = append(problems, fmt.Sprintf("%s spec_ref %s does not resolve: %v", label, entry.SpecRef, err))
			} else if publicSurfaceSpecRefIsPublicationOnly(entry.SpecRef) {
				problems = append(problems, fmt.Sprintf("%s postgres_only_with_spec_ref spec_ref %s is publication-only, not backend-support authority", label, entry.SpecRef))
			}
		case "split_with_issue_ref":
			if entry.SplitIssue == 0 {
				problems = append(problems, fmt.Sprintf("%s split_with_issue_ref missing split_issue", label))
			} else if !publicSurfaceLedgerHasTrackerIssue(entry.ProofRefs, entry.SplitIssue, activeTrackers) {
				problems = append(problems, fmt.Sprintf("%s split issue #%d is not active", label, entry.SplitIssue))
			}
		case "unsupported_with_issue_ref":
			if entry.UnsupportedIssue == 0 {
				problems = append(problems, fmt.Sprintf("%s unsupported_with_issue_ref missing unsupported_issue", label))
			} else if !publicSurfaceLedgerHasTrackerIssue(entry.ProofRefs, entry.UnsupportedIssue, activeTrackers) {
				problems = append(problems, fmt.Sprintf("%s unsupported issue #%d is not active", label, entry.UnsupportedIssue))
			}
		}
	}
	for method := range ctx.operatorReadAPIMethods {
		if _, ok := seen[method]; !ok {
			problems = append(problems, fmt.Sprintf("operator-read api parity ledger missing method %s", method))
		}
	}
	return problems
}

func validatePublicSurfaceOperatorReadLedgerDualProof(label string, entry publicSurfaceOperatorReadAPIParityEntry) []string {
	var problems []string
	if !publicSurfaceSameStringSet(entry.Backends, []string{"default_sqlite", "explicit_postgres"}) {
		problems = append(problems, fmt.Sprintf("%s dual_backend_api_proof backends = %v, want [default_sqlite explicit_postgres]", label, publicSurfaceSortedStrings(entry.Backends)))
	}
	for _, backend := range []string{"default_sqlite", "explicit_postgres"} {
		if !publicSurfaceOperatorReadLedgerHasSelectedBackendProof(entry.ProofRefs, entry.Method, backend) {
			problems = append(problems, fmt.Sprintf("%s dual_backend_api_proof missing %s backend-scoped selected API proof_ref", label, backend))
		}
	}
	for _, ref := range entry.ProofRefs {
		if ref.Kind != "go_test" || len(ref.Backends) == 0 {
			continue
		}
		for _, backend := range ref.Backends {
			if !publicSurfaceOperatorReadProofRefCoversMethodBackend(ref, entry.Method, backend) {
				problems = append(problems, fmt.Sprintf("%s proof_ref %s scoped to %s is not a registered selected API proof for method %s", label, ref.Name, backend, entry.Method))
			}
		}
	}
	return problems
}

func publicSurfaceOperatorReadLedgerHasGoTestProof(refs []publicSurfaceProofRef) bool {
	for _, ref := range refs {
		if ref.Kind == "go_test" && strings.TrimSpace(ref.Name) != "" {
			return true
		}
	}
	return false
}

func publicSurfaceOperatorReadLedgerHasSelectedBackendProof(refs []publicSurfaceProofRef, method, backend string) bool {
	for _, ref := range refs {
		if ref.Kind != "go_test" {
			continue
		}
		if publicSurfaceOperatorReadProofRefCoversMethodBackend(ref, method, backend) {
			return true
		}
	}
	return false
}

func publicSurfaceOperatorReadProofRefCoversMethodBackend(ref publicSurfaceProofRef, method, backend string) bool {
	if ref.Kind != "go_test" || !publicSurfaceHasValue(ref.Backends, backend) {
		return false
	}
	proof, ok := publicSurfaceSelectedOperatorReadAPIProofs()[strings.TrimSpace(ref.Name)]
	if !ok {
		return false
	}
	return publicSurfaceHasValue(proof.Backends, backend) && publicSurfaceHasValue(proof.Methods, method)
}

func publicSurfaceSelectedOperatorReadAPIProofs() map[string]publicSurfaceSelectedOperatorReadAPIProof {
	return map[string]publicSurfaceSelectedOperatorReadAPIProof{
		"TestSQLiteAgentConversationOwnerBacksSupportedAPISurface": {
			Backends: []string{"default_sqlite"},
			Methods: []string{
				"agent.delivery_diagnostics",
				"agent.diagnose",
				"agent.get",
				"agent.list",
				"conversation.current_for_agent",
				"conversation.get",
				"conversation.get_turn",
				"conversation.list",
			},
		},
		"TestOperatorAgentReadSurfaceLoadAgentDeliveryDiagnosticsPromotesCanonicalOwner": {
			Backends: []string{"explicit_postgres"},
			Methods:  []string{"agent.delivery_diagnostics"},
		},
		"TestSQLiteAgentDeliveryLifecycleOwnerBacksSupportedAPISurface": {
			Backends: []string{"default_sqlite"},
			Methods:  []string{"agent.delivery_lifecycle"},
		},
		"TestOperatorAgentReadSurfaceLoadAgentDeliveryLifecyclePostgres": {
			Backends: []string{"explicit_postgres"},
			Methods:  []string{"agent.delivery_lifecycle"},
		},
		"TestOperatorAgentReadSurfaceLoadAgentDiagnosisUsesSelectedOwners": {
			Backends: []string{"explicit_postgres"},
			Methods:  []string{"agent.diagnose"},
		},
		"TestOperatorAgentReadSurfaceLoadAgentProjectsSessionAndTurnRefs": {
			Backends: []string{"explicit_postgres"},
			Methods:  []string{"agent.get", "conversation.current_for_agent", "conversation.get", "conversation.get_turn"},
		},
		"TestOperatorAgentReadSurfaceListAgentsDoesNotDeriveStatusFromActiveLease": {
			Backends: []string{"explicit_postgres"},
			Methods:  []string{"agent.list", "conversation.list"},
		},
		"TestSQLiteAgentUsageOwnerBacksSupportedAPISurface": {
			Backends: []string{"default_sqlite"},
			Methods:  []string{"agent.usage"},
		},
		"TestOperatorAgentReadSurfaceLoadAgentUsageSplitsExactAndEstimated": {
			Backends: []string{"explicit_postgres"},
			Methods:  []string{"agent.usage"},
		},
		"TestSQLiteBundleCatalogOwnerBacksSupportedAPISurface": {
			Backends: []string{"default_sqlite"},
			Methods:  []string{"bundle.agents", "bundle.get", "bundle.list"},
		},
		"TestBundleCatalogReadSurfaceListGetAgentsAndCursor": {
			Backends: []string{"explicit_postgres"},
			Methods:  []string{"bundle.agents", "bundle.get", "bundle.list"},
		},
		"TestOperatorEntityHandlersServeContractEntityTypesFromSQLite": {
			Backends: []string{"default_sqlite"},
			Methods:  []string{"entity.aggregate", "entity.get", "entity.list"},
		},
		"TestOperatorEntityHandlersServeContractEntityTypesFromPostgres": {
			Backends: []string{"explicit_postgres"},
			Methods:  []string{"entity.aggregate", "entity.get", "entity.list"},
		},
		"TestOperatorEventPublishSQLiteIdempotentFirstEventPublishesWithoutLock": {
			Backends: []string{"default_sqlite"},
			Methods:  []string{"event.get", "event.list"},
		},
		"TestOperatorEventPublishHandlersPersistEventReportDeliveriesAndReplayIdempotency": {
			Backends: []string{"explicit_postgres"},
			Methods:  []string{"event.get", "event.list"},
		},
		"TestBuildStoresAcceptsSQLiteSelectedCoreRuntimeStore": {
			Backends: []string{"default_sqlite"},
			Methods:  []string{"health.check"},
		},
		"TestRunServeRuntimeFreshEmptyPostgresBootstrapsSchemaBeforeDiskContractsServe": {
			Backends: []string{"explicit_postgres"},
			Methods:  []string{"health.check"},
		},
		"TestOperatorMailboxWriteSupportedSurfacePublishesAndReadsAcrossBackends": {
			Backends: []string{"default_sqlite", "explicit_postgres"},
			Methods:  []string{"mailbox.get", "mailbox.list"},
		},
		"TestSQLiteRunAPIReadSurface_LoadListAndDiagnoseEvidence": {
			Backends: []string{"default_sqlite"},
			Methods:  []string{"run.diagnose", "run.get", "run.list"},
		},
		"TestRunAPIReadSurface_LoadAndListRunHeaders": {
			Backends: []string{"explicit_postgres"},
			Methods:  []string{"run.diagnose", "run.get", "run.list"},
		},
		"TestSQLiteObservabilityOwnerBacksSupportedAPISurfaces": {
			Backends: []string{"default_sqlite"},
			Methods:  []string{"run.trace", "runtime.incidents", "runtime.logs"},
		},
		"TestPostgresObservabilityOwnerBacksSupportedAPISurfaces": {
			Backends: []string{"explicit_postgres"},
			Methods:  []string{"run.trace", "runtime.incidents", "runtime.logs"},
		},
	}
}

func validatePublicSurfaceMutatingLedgerProofRefs(root, label string, refs []publicSurfaceProofRef, ctx publicSurfaceValidationContext, activeTrackers map[string]struct{}) []string {
	var problems []string
	for _, ref := range refs {
		kind := strings.TrimSpace(ref.Kind)
		if _, ok := allowedPublicSurfaceProofKinds()[kind]; !ok {
			problems = append(problems, fmt.Sprintf("%s proof_ref kind %q is not allowed", label, kind))
			continue
		}
		for _, backend := range ref.Backends {
			if backend != "default_sqlite" && backend != "explicit_postgres" {
				problems = append(problems, fmt.Sprintf("%s proof_ref backend %q is not allowed", label, backend))
			}
		}
		switch kind {
		case "go_test":
			if strings.TrimSpace(ref.Name) == "" {
				problems = append(problems, fmt.Sprintf("%s go_test proof_ref missing name", label))
				continue
			}
			if _, ok := ctx.goTests[ref.Name]; !ok {
				problems = append(problems, fmt.Sprintf("%s go_test proof_ref %s does not resolve", label, ref.Name))
			}
		case "artifact":
			if strings.TrimSpace(ref.Path) == "" {
				problems = append(problems, fmt.Sprintf("%s artifact proof_ref missing path", label))
				continue
			}
			if filepath.IsAbs(ref.Path) || strings.HasPrefix(filepath.Clean(ref.Path), "..") {
				problems = append(problems, fmt.Sprintf("%s artifact proof_ref path %s must be repo-relative", label, ref.Path))
				continue
			}
			if _, err := os.Stat(filepath.Join(root, filepath.Clean(ref.Path))); err != nil {
				problems = append(problems, fmt.Sprintf("%s artifact proof_ref path %s does not exist", label, ref.Path))
			}
		case "tracker":
			if ref.Issue == 0 {
				problems = append(problems, fmt.Sprintf("%s tracker proof_ref missing issue", label))
			}
			if _, ok := activeTrackers[trackerKey(ref.Issue, ref.Watchlist)]; !ok {
				problems = append(problems, fmt.Sprintf("%s tracker proof_ref issue #%d watchlist %q is not in active_trackers", label, ref.Issue, ref.Watchlist))
			}
		case "manual_command":
			if !strings.HasPrefix(strings.TrimSpace(ref.Command), "go test ") {
				problems = append(problems, fmt.Sprintf("%s manual_command proof_ref command = %q, want go test command", label, ref.Command))
			}
		}
	}
	return problems
}

func validatePublicSurfaceMutatingLedgerDualProof(label string, entry publicSurfaceMutatingAPIParityEntry, ctx publicSurfaceValidationContext) []string {
	var problems []string
	if !publicSurfaceSameStringSet(entry.Backends, []string{"default_sqlite", "explicit_postgres"}) {
		problems = append(problems, fmt.Sprintf("%s dual_backend_served_proof backends = %v, want [default_sqlite explicit_postgres]", label, publicSurfaceSortedStrings(entry.Backends)))
	}
	if strings.TrimSpace(entry.Scenario) == "" {
		problems = append(problems, fmt.Sprintf("%s dual_backend_served_proof must reference served parity harness scenario", label))
		return problems
	}
	scenario, ok := ctx.servedScenarios[entry.Scenario]
	if !ok {
		problems = append(problems, fmt.Sprintf("%s dual_backend_served_proof scenario %s is not registered in served parity harness", label, entry.Scenario))
		return problems
	}
	if scenario.APIMethod != entry.Method {
		problems = append(problems, fmt.Sprintf("%s served parity scenario %s api_method = %s, want %s", label, scenario.ID, scenario.APIMethod, entry.Method))
	}
	for _, backend := range servedparity.RequiredBackends {
		if !publicSurfaceScenarioHasBackend(scenario, backend) {
			problems = append(problems, fmt.Sprintf("%s served parity scenario %s missing backend %s", label, scenario.ID, backend))
		}
	}
	for _, postcondition := range []servedparity.Postcondition{
		servedparity.PostconditionNoNonTerminalDeliveries,
		servedparity.PostconditionNoPendingPipelineEvents,
		servedparity.PostconditionNoUnfiredDueTimers,
	} {
		if !publicSurfaceScenarioHasPostcondition(scenario, postcondition) {
			problems = append(problems, fmt.Sprintf("%s served parity scenario %s missing postcondition %s", label, scenario.ID, postcondition))
		}
	}
	if !publicSurfaceHasGoTestProof(entry.ProofRefs, scenario.TestName) {
		problems = append(problems, fmt.Sprintf("%s dual_backend_served_proof missing served parity harness go_test proof_ref %s", label, scenario.TestName))
	}
	for _, ref := range entry.ProofRefs {
		if ref.Kind == "go_test" && ref.Name != scenario.TestName {
			problems = append(problems, fmt.Sprintf("%s dual_backend_served_proof go_test proof_ref %s is not the served parity harness test %s", label, ref.Name, scenario.TestName))
		}
	}
	return problems
}

func validatePublicSurfaceMutatingLedgerTransitiveCoverage(label string, entry publicSurfaceMutatingAPIParityEntry, entries []publicSurfaceMutatingAPIParityEntry, ctx publicSurfaceValidationContext) []string {
	var problems []string
	if !publicSurfaceSameStringSet(entry.Backends, []string{"default_sqlite", "explicit_postgres"}) {
		problems = append(problems, fmt.Sprintf("%s covered_transitively backends = %v, want [default_sqlite explicit_postgres]", label, publicSurfaceSortedStrings(entry.Backends)))
	}
	coveredByMethod := strings.TrimSpace(entry.CoveredByMethod)
	if coveredByMethod == "" {
		problems = append(problems, fmt.Sprintf("%s covered_transitively missing covered_by_method", label))
	}
	if coveredByMethod == strings.TrimSpace(entry.Method) {
		problems = append(problems, fmt.Sprintf("%s covered_by_method must differ from method", label))
	}
	if methodInfo, ok := ctx.apiMethodInfo[strings.TrimSpace(entry.Method)]; ok {
		if !methodInfo.Deprecated {
			problems = append(problems, fmt.Sprintf("%s covered_transitively requires deprecated method_catalog entry", label))
		}
		if coveredByMethod != "" && !publicSurfaceMethodCatalogDeclaresDeprecatedWrapper(methodInfo, coveredByMethod) {
			problems = append(problems, fmt.Sprintf("%s method_catalog description must declare deprecated wrapper coverage through %s", label, coveredByMethod))
		}
	}
	if coveredByMethod != "" {
		if _, ok := ctx.mutatingAPIMethods[coveredByMethod]; !ok {
			problems = append(problems, fmt.Sprintf("%s covered_by_method %s is not declared in platform idempotency mutating_methods", label, coveredByMethod))
		}
		if _, ok := ctx.apiMethods[coveredByMethod]; !ok {
			problems = append(problems, fmt.Sprintf("%s covered_by_method %s missing from platform method_catalog", label, coveredByMethod))
		}
		if _, ok := ctx.openRPCMethods[coveredByMethod]; !ok {
			problems = append(problems, fmt.Sprintf("%s covered_by_method %s missing from generated openrpc.json", label, coveredByMethod))
		}
		coveredEntry, ok := publicSurfaceMutatingLedgerEntry(entries, coveredByMethod)
		if !ok {
			problems = append(problems, fmt.Sprintf("%s covered_by_method %s missing from mutating api parity ledger", label, coveredByMethod))
		} else if coveredEntry.Classification != "dual_backend_served_proof" {
			problems = append(problems, fmt.Sprintf("%s covered_by_method %s classification = %q, want dual_backend_served_proof", label, coveredByMethod, coveredEntry.Classification))
		}
	}
	coveredByScenario := strings.TrimSpace(entry.CoveredByScenario)
	if coveredByScenario == "" {
		problems = append(problems, fmt.Sprintf("%s covered_transitively missing covered_by_scenario", label))
		return problems
	}
	if coveredEntry, ok := publicSurfaceMutatingLedgerEntry(entries, coveredByMethod); ok &&
		coveredEntry.Classification == "dual_backend_served_proof" &&
		strings.TrimSpace(coveredEntry.Scenario) != coveredByScenario {
		problems = append(problems, fmt.Sprintf("%s covered_by_scenario %s does not match covered_by_method %s scenario %s", label, coveredByScenario, coveredByMethod, coveredEntry.Scenario))
	}
	scenario, ok := ctx.servedScenarios[coveredByScenario]
	if !ok {
		problems = append(problems, fmt.Sprintf("%s covered_transitively covered_by_scenario %s is not registered in served parity harness", label, coveredByScenario))
		return problems
	}
	if scenario.APIMethod != coveredByMethod {
		problems = append(problems, fmt.Sprintf("%s covered_by_scenario %s api_method = %s, want covered_by_method %s", label, scenario.ID, scenario.APIMethod, coveredByMethod))
	}
	for _, backend := range servedparity.RequiredBackends {
		if !publicSurfaceScenarioHasBackend(scenario, backend) {
			problems = append(problems, fmt.Sprintf("%s covered_by_scenario %s missing backend %s", label, scenario.ID, backend))
		}
	}
	for _, postcondition := range []servedparity.Postcondition{
		servedparity.PostconditionNoNonTerminalDeliveries,
		servedparity.PostconditionNoPendingPipelineEvents,
		servedparity.PostconditionNoUnfiredDueTimers,
	} {
		if !publicSurfaceScenarioHasPostcondition(scenario, postcondition) {
			problems = append(problems, fmt.Sprintf("%s covered_by_scenario %s missing postcondition %s", label, scenario.ID, postcondition))
		}
	}
	if !publicSurfaceHasGoTestProof(entry.ProofRefs, scenario.TestName) {
		problems = append(problems, fmt.Sprintf("%s covered_transitively missing served parity harness go_test proof_ref %s", label, scenario.TestName))
	}
	for _, ref := range entry.ProofRefs {
		if ref.Kind == "go_test" && ref.Name != scenario.TestName {
			problems = append(problems, fmt.Sprintf("%s covered_transitively go_test proof_ref %s is not the covered served parity harness test %s", label, ref.Name, scenario.TestName))
		}
	}
	return problems
}

func publicSurfaceMethodCatalogDeclaresDeprecatedWrapper(method publicSurfaceAPIMethodInfo, coveredByMethod string) bool {
	description := strings.ToLower(method.Description)
	return strings.Contains(description, "deprecated wrapper") &&
		strings.Contains(method.Description, coveredByMethod)
}

func publicSurfaceSpecRefExists(root, specRef string) error {
	path, yamlPath, ok := strings.Cut(strings.TrimSpace(specRef), "#")
	if !ok || strings.TrimSpace(path) == "" || strings.TrimSpace(yamlPath) == "" {
		return fmt.Errorf("want repo-relative yaml path plus #anchor")
	}
	clean := filepath.Clean(path)
	if filepath.IsAbs(path) || strings.HasPrefix(clean, "..") {
		return fmt.Errorf("path must be repo-relative")
	}
	raw, err := os.ReadFile(filepath.Join(root, clean))
	if err != nil {
		return err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return err
	}
	node := yamlDocumentRoot(&doc)
	for _, part := range strings.Split(strings.Trim(strings.TrimSpace(yamlPath), "."), ".") {
		if part == "" {
			continue
		}
		node = yamlMappingValue(node, part)
		if node == nil {
			return fmt.Errorf("anchor path component %q missing", part)
		}
	}
	return nil
}

func publicSurfaceSpecRefIsPublicationOnly(specRef string) bool {
	_, yamlPath, ok := strings.Cut(strings.TrimSpace(specRef), "#")
	if !ok {
		return false
	}
	switch strings.Trim(strings.TrimSpace(yamlPath), ".") {
	case "api_specification.method_catalog",
		"api_specification.conventions",
		"api_specification.conventions.idempotency",
		"api_specification.conventions.idempotency.mutating_methods":
		return true
	default:
		return false
	}
}

func publicSurfaceLedgerHasTrackerIssue(refs []publicSurfaceProofRef, issue int, activeTrackers map[string]struct{}) bool {
	for _, ref := range refs {
		if ref.Kind != "tracker" || ref.Issue != issue {
			continue
		}
		if _, ok := activeTrackers[trackerKey(ref.Issue, ref.Watchlist)]; ok {
			return true
		}
	}
	return false
}

func publicSurfaceScenarioHasBackend(scenario servedparity.Scenario, backend servedparity.Backend) bool {
	for _, actual := range scenario.Backends {
		if actual == backend {
			return true
		}
	}
	return false
}

func publicSurfaceScenarioHasPostcondition(scenario servedparity.Scenario, postcondition servedparity.Postcondition) bool {
	for _, actual := range scenario.Postconditions {
		if actual == postcondition {
			return true
		}
	}
	return false
}

func validatePublicSurfaceServedMutatingLifecycleRows(rowsByID map[string]publicSurfaceMatrixRow, activeTrackers map[string]struct{}, ctx publicSurfaceValidationContext) []string {
	var problems []string
	for id, row := range rowsByID {
		guarded := publicSurfaceHasValue(row.ProofDimensions, "served_mutating_lifecycle")
		if publicSurfaceLooksLikeServedMutatingLifecycle(row, ctx) && !guarded {
			problems = append(problems, fmt.Sprintf("%s served mutating lifecycle row missing served_mutating_lifecycle proof_dimension", id))
		}
		if !guarded {
			continue
		}

		if publicSurfaceExplicitServedLifecycleSplit(row) {
			if !publicSurfaceHasActiveTrackerProof(row.ProofRefs, activeTrackers) {
				problems = append(problems, fmt.Sprintf("%s served mutating lifecycle split/postgres-only row missing active tracker proof_ref", id))
			}
			continue
		}
		if !publicSurfaceSupported(row) {
			problems = append(problems, fmt.Sprintf("%s served mutating lifecycle row classification %q must be supported or an explicit split/postgres-only row", id, row.Classification))
			continue
		}
		if !publicSurfaceHasServedMutatingLifecycleMethod(row.APIMethods, ctx) {
			problems = append(problems, fmt.Sprintf("%s served mutating lifecycle row missing mutating lifecycle api_method", id))
		}
		for _, method := range row.APIMethods {
			method = strings.TrimSpace(method)
			if _, ok := ctx.mutatingAPIMethods[method]; ok && !publicSurfaceHasValue(row.OpenRPCMatrixMethods, method) {
				problems = append(problems, fmt.Sprintf("%s served mutating lifecycle row api_method %s missing from openrpc_matrix_methods", id, method))
			}
		}
		for _, backend := range []string{"default_sqlite", "explicit_postgres"} {
			if !publicSurfaceHasValue(row.Backends, backend) {
				problems = append(problems, fmt.Sprintf("%s served mutating lifecycle row missing %s backend", id, backend))
			}
		}
		for _, dimension := range []string{
			"served_mutating_lifecycle",
			"real_runtime_startup",
			"real_v1_handler",
			"selected_store",
			"canonical_store_owner",
			"openrpc_publication",
		} {
			if !publicSurfaceHasValue(row.ProofDimensions, dimension) {
				problems = append(problems, fmt.Sprintf("%s served mutating lifecycle row missing %s proof_dimension", id, dimension))
			}
		}
		for method, proofToken := range publicSurfaceServedMutatingLifecycleProofTokens(row.APIMethods, ctx) {
			if !publicSurfaceHasServedRuntimeBackendProof(row.ProofRefs, proofToken, "SQLite") {
				problems = append(problems, fmt.Sprintf("%s missing default SQLite served-runtime go_test proof_ref for api_method %s", id, method))
			}
			if !publicSurfaceHasServedRuntimeBackendProof(row.ProofRefs, proofToken, "Postgres") {
				problems = append(problems, fmt.Sprintf("%s missing explicit Postgres served-runtime go_test proof_ref for api_method %s", id, method))
			}
		}
	}
	return problems
}

func expectedPublicSurfaceRowShapes() map[string]publicSurfaceExpectedRowShape {
	return map[string]publicSurfaceExpectedRowShape{
		"api_idempotency_selected_store": {
			Classification:       "already_covered_by_existing_proof",
			Tier:                 "required_smoke",
			Backends:             []string{"default_sqlite", "explicit_postgres"},
			APIMethods:           []string{"event.publish", "run.start"},
			OpenRPCMatrixMethods: []string{"event.publish", "run.start"},
			ProofDimensions:      []string{"canonical_store_owner", "openrpc_publication", "real_v1_handler", "selected_store"},
		},
		"runtime_log_readback_api": {
			Classification:       "already_covered_by_existing_proof",
			Tier:                 "required_smoke",
			Backends:             []string{"default_sqlite", "explicit_postgres"},
			APIMethods:           []string{"runtime.logs", "runtime.subscribe_logs", "run.trace"},
			OpenRPCMatrixMethods: []string{"runtime.logs", "runtime.subscribe_logs", "run.trace"},
			ProofDimensions:      []string{"canonical_store_owner", "openrpc_publication", "real_v1_handler", "selected_store"},
		},
		"event_publish_existing_run_followup_served_path": {
			Classification:       "already_covered_by_existing_proof",
			Tier:                 "required_smoke",
			Backends:             []string{"default_sqlite", "explicit_postgres"},
			APIMethods:           []string{"event.publish"},
			OpenRPCMatrixMethods: []string{"event.publish"},
			ProofDimensions:      []string{"canonical_store_owner", "cli_v1_path", "openrpc_publication", "real_runtime_startup", "real_v1_handler", "selected_store", "served_mutating_lifecycle"},
			GoTestProofRefs: []string{
				"TestRunServeRuntimeEventPublishRunIDFollowUpServedPathDefaultSQLite",
				"TestRunServeRuntimeEventPublishRunIDFollowUpServedPathPostgres",
				"TestRunServeRuntimeEventPublishExistingRunActiveLoadServedPathDefaultSQLite",
				"TestRunServeRuntimeEventPublishExistingRunActiveLoadServedPathPostgres",
			},
		},
		"event_publish_existing_run_target_route_served_path": {
			Classification:       "add_to_matrix",
			Tier:                 "required_smoke",
			Backends:             []string{"default_sqlite", "explicit_postgres"},
			APIMethods:           []string{"event.publish"},
			OpenRPCMatrixMethods: []string{"event.publish"},
			ProofDimensions:      []string{"canonical_store_owner", "cli_v1_path", "openrpc_publication", "real_runtime_startup", "real_v1_handler", "selected_store", "served_mutating_lifecycle"},
			GoTestProofRefs: []string{
				"TestEventPublishSerializesTargetRouteParam",
				"TestOperatorEventPublishExistingRunTargetRouteValidatesAndPersistsCanonicalTarget",
				"TestOperatorEventPublishExistingRunTargetRouteRejectsInvalidTargetBeforePersistence",
				"TestRunServeRuntimeEventPublishTargetRouteServedPathDefaultSQLite",
				"TestRunServeRuntimeEventPublishTargetRouteServedPathPostgres",
			},
		},
		"event_publish_dynamic_auto_emit_served_lifecycle": {
			Classification:       "add_to_matrix",
			Tier:                 "required_smoke",
			Backends:             []string{"default_sqlite", "explicit_postgres"},
			APIMethods:           []string{"event.publish"},
			OpenRPCMatrixMethods: []string{"event.publish"},
			ProofDimensions:      []string{"canonical_store_owner", "openrpc_publication", "real_runtime_startup", "real_v1_handler", "selected_store", "served_mutating_lifecycle"},
			GoTestProofRefs: []string{
				"TestServedParityHarnessEventPublishDynamicAutoEmitLifecycle",
				"TestRunServeRuntimeEventPublishDynamicAutoEmitServedPathDefaultSQLite",
				"TestRunServeRuntimeEventPublishDynamicAutoEmitServedPathPostgres",
			},
		},
	}
}

func validatePublicSurfaceSources(root string, source publicSurfaceMatrixSource) []string {
	var problems []string
	expected := map[string]string{
		"platform_spec":           "platform-spec.yaml",
		"openrpc_artifact":        "openrpc.json",
		"adjacent_openrpc_matrix": "internal/apiv1/testdata/openrpc_compliance_matrix.yaml",
	}
	actual := map[string]string{
		"platform_spec":           source.PlatformSpec,
		"openrpc_artifact":        source.OpenRPCArtifact,
		"adjacent_openrpc_matrix": source.AdjacentOpenRPCMatrix,
	}
	for field, want := range expected {
		got := strings.TrimSpace(actual[field])
		if got != want {
			problems = append(problems, fmt.Sprintf("source %s = %q, want %q", field, got, want))
			continue
		}
		if _, err := os.Stat(filepath.Join(root, filepath.Clean(got))); err != nil {
			problems = append(problems, fmt.Sprintf("source %s path %s does not exist", field, got))
		}
	}
	return problems
}

func validatePublicSurfacePolicy(policy publicSurfaceMatrixPolicy) []string {
	var problems []string
	if policy.ClosureLevel != "matrix_owner_first_slice_complete" {
		problems = append(problems, fmt.Sprintf("policy closure_level = %q, want matrix_owner_first_slice_complete", policy.ClosureLevel))
	}
	if policy.ClaimsParentClosure {
		problems = append(problems, "policy claims_parent_closure = true, want false")
	}
	if !strings.HasPrefix(strings.TrimSpace(policy.NamedFullConformanceCommand), "go test ./internal/runtime/cataloge2e") {
		problems = append(problems, fmt.Sprintf("policy named_full_conformance_command = %q, want cataloge2e go test command", policy.NamedFullConformanceCommand))
	}
	if strings.TrimSpace(policy.RequiredSmokePolicy) == "" {
		problems = append(problems, "policy required_smoke_policy missing")
	}
	classificationPolicy := strings.TrimSpace(policy.MutatingAPIParityClassificationPolicy)
	if classificationPolicy == "" {
		problems = append(problems, "policy mutating_api_parity_classification_policy missing")
	} else if !strings.Contains(classificationPolicy, "covered_transitively") || !strings.Contains(strings.ToLower(classificationPolicy), "deprecated wrapper") {
		problems = append(problems, "policy mutating_api_parity_classification_policy must document covered_transitively deprecated wrapper handling")
	}
	operatorReadPolicy := strings.TrimSpace(policy.OperatorReadAPIParityClassificationPolicy)
	if operatorReadPolicy == "" {
		problems = append(problems, "policy operator_read_api_parity_classification_policy missing")
	} else if !strings.Contains(operatorReadPolicy, "dual_backend_api_proof") || !strings.Contains(operatorReadPolicy, "fake-store") || !strings.Contains(operatorReadPolicy, "different_semantic_concept_with_proof") {
		problems = append(problems, "policy operator_read_api_parity_classification_policy must document dual backend proof, fake-store exclusion, and different concept classification")
	}
	return problems
}

func validatePublicSurfaceRow(root string, row publicSurfaceMatrixRow, ctx publicSurfaceValidationContext, activeTrackers map[string]struct{}) []string {
	var problems []string
	id := strings.TrimSpace(row.ID)
	if strings.TrimSpace(row.Surface) == "" {
		problems = append(problems, fmt.Sprintf("%s missing surface", id))
	}
	if _, ok := allowedPublicSurfaceClassifications()[row.Classification]; !ok {
		problems = append(problems, fmt.Sprintf("%s classification %q is not allowed", id, row.Classification))
	}
	if _, ok := allowedPublicSurfaceTiers()[row.Tier]; !ok {
		problems = append(problems, fmt.Sprintf("%s tier %q is not allowed", id, row.Tier))
	}
	if len(row.Backends) == 0 {
		problems = append(problems, fmt.Sprintf("%s missing backends", id))
	}
	for _, backend := range row.Backends {
		if _, ok := allowedPublicSurfaceBackends()[backend]; !ok {
			problems = append(problems, fmt.Sprintf("%s backend %q is not allowed", id, backend))
		}
	}
	for _, dimension := range row.ProofDimensions {
		if _, ok := allowedPublicSurfaceProofDimensions()[dimension]; !ok {
			problems = append(problems, fmt.Sprintf("%s proof_dimension %q is not allowed", id, dimension))
		}
	}
	if len(row.StoreOwners) == 0 {
		problems = append(problems, fmt.Sprintf("%s missing store_owners", id))
	}
	if len(row.ProofRefs) == 0 {
		problems = append(problems, fmt.Sprintf("%s missing proof_refs", id))
	}

	for _, method := range row.APIMethods {
		method = strings.TrimSpace(method)
		if _, ok := ctx.apiMethods[method]; !ok {
			problems = append(problems, fmt.Sprintf("%s api_method %s missing from platform method_catalog", id, method))
		}
		if _, ok := ctx.openRPCMethods[method]; !ok {
			problems = append(problems, fmt.Sprintf("%s api_method %s missing from generated openrpc.json", id, method))
		}
	}
	for _, method := range row.OpenRPCMatrixMethods {
		method = strings.TrimSpace(method)
		if _, ok := ctx.openRPCMatrixMethods[method]; !ok {
			problems = append(problems, fmt.Sprintf("%s openrpc_matrix_method %s missing from adjacent OpenRPC matrix", id, method))
		}
	}
	for _, command := range row.CLICommands {
		command = strings.TrimSpace(command)
		if _, ok := ctx.cliCommands[command]; !ok {
			problems = append(problems, fmt.Sprintf("%s cli_command %s missing from platform cli command_catalog", id, command))
		}
	}

	if publicSurfaceSupported(row) && len(row.APIMethods) > 0 {
		if !publicSurfaceHasValue(row.ProofDimensions, "real_v1_handler") {
			problems = append(problems, fmt.Sprintf("%s supported api row missing real_v1_handler proof_dimension", id))
		}
		if !publicSurfaceHasValue(row.ProofDimensions, "openrpc_publication") {
			problems = append(problems, fmt.Sprintf("%s supported api row missing openrpc_publication proof_dimension", id))
		}
	}
	if publicSurfaceSupported(row) && len(row.CLICommands) > 0 {
		if !publicSurfaceHasValue(row.ProofDimensions, "cli_v1_path") && !publicSurfaceHasValue(row.ProofDimensions, "real_runtime_startup") {
			problems = append(problems, fmt.Sprintf("%s supported cli row missing cli_v1_path or real_runtime_startup proof_dimension", id))
		}
	}
	if publicSurfaceSupported(row) && (publicSurfaceHasValue(row.Backends, "default_sqlite") || publicSurfaceHasValue(row.Backends, "explicit_postgres")) {
		if !publicSurfaceHasValue(row.ProofDimensions, "selected_store") && !publicSurfaceHasValue(row.ProofDimensions, "backend_selection") {
			problems = append(problems, fmt.Sprintf("%s selected-backend row missing selected_store or backend_selection proof_dimension", id))
		}
	}
	if row.Classification == "split_to_existing_issue" {
		if row.SplitIssue == 0 {
			problems = append(problems, fmt.Sprintf("%s split row missing split_issue", id))
		}
		if !publicSurfaceHasValue(row.ProofDimensions, "split_tracker") {
			problems = append(problems, fmt.Sprintf("%s split row missing split_tracker proof_dimension", id))
		}
	}
	problems = append(problems, validatePublicSurfaceProofRefs(root, id, row, ctx, activeTrackers)...)
	return problems
}

func validatePublicSurfaceProofRefs(root, rowID string, row publicSurfaceMatrixRow, ctx publicSurfaceValidationContext, activeTrackers map[string]struct{}) []string {
	var problems []string
	seenSplitTracker := false
	seenGoTest := false
	for _, ref := range row.ProofRefs {
		kind := strings.TrimSpace(ref.Kind)
		if _, ok := allowedPublicSurfaceProofKinds()[kind]; !ok {
			problems = append(problems, fmt.Sprintf("%s proof_ref kind %q is not allowed", rowID, kind))
			continue
		}
		switch kind {
		case "go_test":
			seenGoTest = true
			if strings.TrimSpace(ref.Name) == "" {
				problems = append(problems, fmt.Sprintf("%s go_test proof_ref missing name", rowID))
				continue
			}
			if _, ok := ctx.goTests[ref.Name]; !ok {
				problems = append(problems, fmt.Sprintf("%s go_test proof_ref %s does not resolve", rowID, ref.Name))
			}
		case "artifact":
			if strings.TrimSpace(ref.Path) == "" {
				problems = append(problems, fmt.Sprintf("%s artifact proof_ref missing path", rowID))
				continue
			}
			if filepath.IsAbs(ref.Path) || strings.HasPrefix(filepath.Clean(ref.Path), "..") {
				problems = append(problems, fmt.Sprintf("%s artifact proof_ref path %s must be repo-relative", rowID, ref.Path))
				continue
			}
			if _, err := os.Stat(filepath.Join(root, filepath.Clean(ref.Path))); err != nil {
				problems = append(problems, fmt.Sprintf("%s artifact proof_ref path %s does not exist", rowID, ref.Path))
			}
		case "tracker":
			if ref.Issue == 0 {
				problems = append(problems, fmt.Sprintf("%s tracker proof_ref missing issue", rowID))
			}
			if _, ok := activeTrackers[trackerKey(ref.Issue, ref.Watchlist)]; !ok {
				problems = append(problems, fmt.Sprintf("%s tracker proof_ref issue #%d watchlist %q is not in active_trackers", rowID, ref.Issue, ref.Watchlist))
			}
			if ref.Issue == row.SplitIssue {
				seenSplitTracker = true
			}
		case "manual_command":
			if !strings.HasPrefix(strings.TrimSpace(ref.Command), "go test ") {
				problems = append(problems, fmt.Sprintf("%s manual_command proof_ref command = %q, want go test command", rowID, ref.Command))
			}
		}
	}
	if row.Classification == "split_to_existing_issue" && row.SplitIssue != 0 && !seenSplitTracker {
		problems = append(problems, fmt.Sprintf("%s split row missing tracker proof_ref for issue #%d", rowID, row.SplitIssue))
	}
	if publicSurfaceFailClosedRow(row) && !seenGoTest {
		problems = append(problems, fmt.Sprintf("%s fail-closed row requires at least one go_test proof_ref", rowID))
	}
	return problems
}

func loadPublicSurfaceCLICommands(t *testing.T, root string) map[string]struct{} {
	t.Helper()
	raw, err := os.ReadFile(compliancePlatformSpecPath(root))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse platform spec yaml: %v", err)
	}
	rootNode := yamlDocumentRoot(&doc)
	commandCatalog := yamlMappingValue(yamlMappingValue(rootNode, "cli_specification"), "command_catalog")
	if commandCatalog == nil || commandCatalog.Kind != yaml.MappingNode {
		t.Fatal("platform spec cli_specification.command_catalog missing")
	}
	out := map[string]struct{}{}
	for i := 0; i+1 < len(commandCatalog.Content); i += 2 {
		out[commandCatalog.Content[i].Value] = struct{}{}
	}
	return out
}

func loadPublicSurfaceGoTests(root string) (map[string]string, error) {
	out := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".swarm", "node_modules", "vendor":
				return filepath.SkipDir
			default:
				return nil
			}
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fileSet := token.NewFileSet()
		parsed, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			if name := fn.Name.Name; strings.HasPrefix(name, "Test") {
				out[name] = rel
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func yamlDocumentRoot(doc *yaml.Node) *yaml.Node {
	if doc == nil {
		return nil
	}
	if doc.Kind == yaml.DocumentNode && len(doc.Content) > 0 {
		return doc.Content[0]
	}
	return doc
}

func yamlMappingValue(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}

func publicSurfaceMatrixRowByID(t *testing.T, matrix *publicSurfaceBackendMatrix, id string) *publicSurfaceMatrixRow {
	t.Helper()
	for i := range matrix.Rows {
		if matrix.Rows[i].ID == id {
			return &matrix.Rows[i]
		}
	}
	t.Fatalf("matrix row %s not found", id)
	return nil
}

func publicSurfaceMutatingLedgerEntryByMethod(t *testing.T, matrix *publicSurfaceBackendMatrix, method string) *publicSurfaceMutatingAPIParityEntry {
	t.Helper()
	for i := range matrix.MutatingLedger {
		if matrix.MutatingLedger[i].Method == method {
			return &matrix.MutatingLedger[i]
		}
	}
	t.Fatalf("mutating api parity ledger method %s not found", method)
	return nil
}

func publicSurfaceOperatorReadLedgerEntryByMethod(t *testing.T, matrix *publicSurfaceBackendMatrix, method string) *publicSurfaceOperatorReadAPIParityEntry {
	t.Helper()
	for i := range matrix.OperatorReadLedger {
		if matrix.OperatorReadLedger[i].Method == method {
			return &matrix.OperatorReadLedger[i]
		}
	}
	t.Fatalf("operator-read api parity ledger method %s not found", method)
	return nil
}

func publicSurfaceMutatingLedgerEntry(entries []publicSurfaceMutatingAPIParityEntry, method string) (publicSurfaceMutatingAPIParityEntry, bool) {
	for _, entry := range entries {
		if entry.Method == method {
			return entry, true
		}
	}
	return publicSurfaceMutatingAPIParityEntry{}, false
}

func publicSurfaceProblemsContain(problems []string, want string) bool {
	for _, problem := range problems {
		if strings.Contains(problem, want) {
			return true
		}
	}
	return false
}

func publicSurfaceSupported(row publicSurfaceMatrixRow) bool {
	switch row.Classification {
	case "add_to_matrix", "already_covered_by_existing_proof":
		return true
	default:
		return false
	}
}

func publicSurfaceFailClosedRow(row publicSurfaceMatrixRow) bool {
	return row.Tier == "postgres_only_fail_closed" || publicSurfaceHasValue(row.ProofDimensions, "fail_closed")
}

func publicSurfaceLooksLikeServedMutatingLifecycle(row publicSurfaceMatrixRow, ctx publicSurfaceValidationContext) bool {
	if !publicSurfaceSupported(row) && !publicSurfaceExplicitServedLifecycleSplit(row) {
		return false
	}
	if !publicSurfaceHasServedMutatingLifecycleMethod(row.APIMethods, ctx) {
		return false
	}
	if strings.Contains(strings.ToLower(row.Surface), "served") {
		return true
	}
	return publicSurfaceHasValue(row.ProofDimensions, "real_runtime_startup") &&
		publicSurfaceHasValue(row.ProofDimensions, "real_v1_handler")
}

func publicSurfaceExplicitServedLifecycleSplit(row publicSurfaceMatrixRow) bool {
	return row.Classification == "split_to_existing_issue" ||
		row.Tier == "postgres_only_fail_closed" ||
		publicSurfaceHasValue(row.Backends, "postgres_only")
}

func publicSurfaceHasServedMutatingLifecycleMethod(methods []string, ctx publicSurfaceValidationContext) bool {
	for _, method := range methods {
		if _, ok := ctx.mutatingAPIMethods[strings.TrimSpace(method)]; ok {
			return true
		}
	}
	return false
}

func publicSurfaceServedMutatingLifecycleProofTokens(methods []string, ctx publicSurfaceValidationContext) map[string]string {
	out := map[string]string{}
	for _, method := range methods {
		method = strings.TrimSpace(method)
		if _, ok := ctx.mutatingAPIMethods[method]; !ok {
			continue
		}
		out[method] = publicSurfaceAPIMethodProofToken(method)
	}
	return out
}

func publicSurfaceAPIMethodProofToken(method string) string {
	var out strings.Builder
	for _, part := range strings.FieldsFunc(method, func(r rune) bool {
		return r == '.' || r == '_' || r == '-'
	}) {
		if part == "" {
			continue
		}
		out.WriteString(strings.ToUpper(part[:1]))
		if len(part) > 1 {
			out.WriteString(part[1:])
		}
	}
	return out.String()
}

func publicSurfaceHasServedRuntimeBackendProof(refs []publicSurfaceProofRef, methodToken, backend string) bool {
	for _, ref := range refs {
		if ref.Kind == "go_test" &&
			strings.Contains(ref.Name, "ServeRuntime") &&
			strings.Contains(ref.Name, methodToken) &&
			strings.Contains(ref.Name, backend) {
			return true
		}
	}
	return false
}

func publicSurfaceHasActiveTrackerProof(refs []publicSurfaceProofRef, activeTrackers map[string]struct{}) bool {
	for _, ref := range refs {
		if ref.Kind != "tracker" {
			continue
		}
		if _, ok := activeTrackers[trackerKey(ref.Issue, ref.Watchlist)]; ok {
			return true
		}
	}
	return false
}

func publicSurfaceHasValue(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func publicSurfaceHasGoTestProof(refs []publicSurfaceProofRef, want string) bool {
	for _, ref := range refs {
		if ref.Kind == "go_test" && ref.Name == want {
			return true
		}
	}
	return false
}

func publicSurfaceProofRefsExcept(refs []publicSurfaceProofRef, name string) []publicSurfaceProofRef {
	out := refs[:0]
	for _, ref := range refs {
		if ref.Kind == "go_test" && ref.Name == name {
			continue
		}
		out = append(out, ref)
	}
	return out
}

func publicSurfaceStringsExcept(values []string, remove string) []string {
	out := values[:0]
	for _, value := range values {
		if value == remove {
			continue
		}
		out = append(out, value)
	}
	return out
}

func publicSurfaceMatrixRowsExcept(rows []publicSurfaceMatrixRow, remove string) []publicSurfaceMatrixRow {
	out := rows[:0]
	for _, row := range rows {
		if row.ID == remove {
			continue
		}
		out = append(out, row)
	}
	return out
}

func publicSurfaceMutatingLedgerExcept(entries []publicSurfaceMutatingAPIParityEntry, remove string) []publicSurfaceMutatingAPIParityEntry {
	out := entries[:0]
	for _, entry := range entries {
		if entry.Method == remove {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func publicSurfaceOperatorReadLedgerExcept(entries []publicSurfaceOperatorReadAPIParityEntry, remove string) []publicSurfaceOperatorReadAPIParityEntry {
	out := entries[:0]
	for _, entry := range entries {
		if entry.Method == remove {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func publicSurfaceSameStringSet(actual, want []string) bool {
	actualSorted := publicSurfaceSortedStrings(actual)
	wantSorted := publicSurfaceSortedStrings(want)
	if len(actualSorted) != len(wantSorted) {
		return false
	}
	for i := range actualSorted {
		if actualSorted[i] != wantSorted[i] {
			return false
		}
	}
	return true
}

func publicSurfaceSortedStrings(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

func requiredPublicSurfaceRows() map[string]struct{} {
	return complianceStringSet([]string{
		"serve_contracts_default_sqlite",
		"serve_contracts_explicit_postgres",
		"run_contracts_default_sqlite",
		"run_contracts_explicit_postgres",
		"entity_read_api",
		"entity_read_cli",
		"status_run_get_entity_count",
		"event_publish_api",
		"api_idempotency_selected_store",
		"event_publish_cli",
		"event_publish_existing_run_followup_served_path",
		"event_publish_existing_run_target_route_served_path",
		"event_publish_dynamic_auto_emit_served_lifecycle",
		"runtime_log_readback_api",
		"mailbox_read_api_after_mailbox_write",
		"mailbox_read_cli",
		"serve_dev_abandon_active_runs",
		"handler_static_create_entity_retirement",
		"remaining_openrpc_selected_backend_tail",
		"bundle_hash_catalog_boot_postgres_only",
	})
}

func allowedPublicSurfaceClassifications() map[string]struct{} {
	return complianceStringSet([]string{
		"add_to_matrix",
		"already_covered_by_existing_proof",
		"split_to_existing_issue",
		"different_semantic_concept_with_proof",
	})
}

func allowedPublicSurfaceMutatingLedgerClassifications() map[string]struct{} {
	return complianceStringSet([]string{
		"dual_backend_served_proof",
		"covered_transitively",
		"postgres_only_with_spec_ref",
		"unsupported_with_issue_ref",
		"split_with_issue_ref",
	})
}

func allowedPublicSurfaceOperatorReadLedgerClassifications() map[string]struct{} {
	return complianceStringSet([]string{
		"dual_backend_api_proof",
		"different_semantic_concept_with_proof",
		"postgres_only_with_spec_ref",
		"unsupported_with_issue_ref",
		"split_with_issue_ref",
	})
}

func allowedPublicSurfaceTiers() map[string]struct{} {
	return complianceStringSet([]string{
		"required_smoke",
		"full_conformance",
		"split_open",
		"future_parent_tail",
		"postgres_only_fail_closed",
	})
}

func allowedPublicSurfaceBackends() map[string]struct{} {
	return complianceStringSet([]string{
		"default_sqlite",
		"explicit_postgres",
		"postgres_only",
	})
}

func allowedPublicSurfaceProofDimensions() map[string]struct{} {
	return complianceStringSet([]string{
		"backend_selection",
		"canonical_store_owner",
		"cli_v1_path",
		"fail_closed",
		"full_conformance",
		"openrpc_publication",
		"real_runtime_startup",
		"real_v1_handler",
		"selected_store",
		"served_mutating_lifecycle",
		"split_tracker",
	})
}

func allowedPublicSurfaceProofKinds() map[string]struct{} {
	return complianceStringSet([]string{
		"artifact",
		"go_test",
		"manual_command",
		"tracker",
	})
}
