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

	"gopkg.in/yaml.v3"
)

const publicSurfaceBackendMatrixPath = "internal/apiv1/testdata/public_surface_backend_matrix.yaml"

type publicSurfaceBackendMatrix struct {
	Version        int                       `yaml:"version"`
	Kind           string                    `yaml:"kind"`
	Issue          int                       `yaml:"issue"`
	IssueRole      string                    `yaml:"issue_role"`
	Source         publicSurfaceMatrixSource `yaml:"source"`
	Policy         publicSurfaceMatrixPolicy `yaml:"policy"`
	ActiveTrackers []complianceActiveTracker `yaml:"active_trackers"`
	Rows           []publicSurfaceMatrixRow  `yaml:"rows"`
}

type publicSurfaceMatrixSource struct {
	PlatformSpec          string `yaml:"platform_spec"`
	OpenRPCArtifact       string `yaml:"openrpc_artifact"`
	AdjacentOpenRPCMatrix string `yaml:"adjacent_openrpc_matrix"`
}

type publicSurfaceMatrixPolicy struct {
	ClosureLevel                string `yaml:"closure_level"`
	ClaimsParentClosure         bool   `yaml:"claims_parent_closure"`
	NamedFullConformanceCommand string `yaml:"named_full_conformance_command"`
	RequiredSmokePolicy         string `yaml:"required_smoke_policy"`
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
	Kind      string `yaml:"kind"`
	Name      string `yaml:"name,omitempty"`
	Path      string `yaml:"path,omitempty"`
	Issue     int    `yaml:"issue,omitempty"`
	Watchlist string `yaml:"watchlist,omitempty"`
	Command   string `yaml:"command,omitempty"`
}

type publicSurfaceValidationContext struct {
	apiMethods           map[string]struct{}
	openRPCMethods       map[string]struct{}
	openRPCMatrixMethods map[string]struct{}
	cliCommands          map[string]struct{}
	goTests              map[string]string
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
	for name := range api.MethodCatalog {
		apiMethods[name] = struct{}{}
	}
	openRPCMethods := map[string]struct{}{}
	for _, method := range doc.Methods {
		openRPCMethods[strings.TrimSpace(method.Name)] = struct{}{}
	}
	openRPCMatrixMethods := map[string]struct{}{}
	for _, row := range openRPCMatrix.Methods {
		openRPCMatrixMethods[strings.TrimSpace(row.Method)] = struct{}{}
	}

	return publicSurfaceValidationContext{
		apiMethods:           apiMethods,
		openRPCMethods:       openRPCMethods,
		openRPCMatrixMethods: openRPCMatrixMethods,
		cliCommands:          loadPublicSurfaceCLICommands(t, root),
		goTests:              goTests,
	}
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
	if _, ok := activeTrackers[trackerKey(0, "operator_surfaces.v1_openrpc_api_conformance")]; !ok {
		problems = append(problems, "active_trackers missing operator_surfaces.v1_openrpc_api_conformance watchlist")
	}

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
	sort.Strings(problems)
	return problems
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
		"event_publish_cli",
		"mailbox_read_api_after_mailbox_write",
		"mailbox_read_cli",
		"serve_dev_abandon_active_runs",
		"handler_create_entity_exact_once",
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
