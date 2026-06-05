package conformance

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const routeAuthorityMatrixPath = "internal/runtime/conformance/testdata/route_authority_matrix.yaml"

type routeAuthorityMatrix struct {
	Version        int                           `yaml:"version"`
	Kind           string                        `yaml:"kind"`
	Issue          int                           `yaml:"issue"`
	ParentIssue    int                           `yaml:"parent_issue"`
	Source         routeAuthorityMatrixSource    `yaml:"source"`
	Policy         routeAuthorityMatrixPolicy    `yaml:"policy"`
	ActiveTrackers []routeAuthorityActiveTracker `yaml:"active_trackers"`
	Rows           []routeAuthorityMatrixRow     `yaml:"rows"`
}

type routeAuthorityMatrixSource struct {
	PlatformSpec               string `yaml:"platform_spec"`
	OpenRPCArtifact            string `yaml:"openrpc_artifact"`
	PublicSurfaceBackendMatrix string `yaml:"public_surface_backend_matrix"`
	MultiBundleMatrix          string `yaml:"multi_bundle_matrix"`
	CatalogTiers               string `yaml:"catalog_tiers"`
}

type routeAuthorityMatrixPolicy struct {
	ClosureLevel                string `yaml:"closure_level"`
	ClaimsParentClosure         bool   `yaml:"claims_parent_closure"`
	NamedFullConformanceCommand string `yaml:"named_full_conformance_command"`
	RequiredSmokePolicy         string `yaml:"required_smoke_policy"`
}

type routeAuthorityActiveTracker struct {
	Kind      string `yaml:"kind"`
	Issue     int    `yaml:"issue"`
	Watchlist string `yaml:"watchlist"`
}

type routeAuthorityMatrixRow struct {
	ID               string                   `yaml:"id"`
	Concept          string                   `yaml:"concept"`
	Classification   string                   `yaml:"classification"`
	Tier             string                   `yaml:"tier"`
	SplitIssue       int                      `yaml:"split_issue"`
	Replacement      string                   `yaml:"replacement"`
	ProofDimensions  []string                 `yaml:"proof_dimensions"`
	InvalidAuthority []string                 `yaml:"invalid_authority"`
	OwnerRefs        []routeAuthorityProofRef `yaml:"owner_refs"`
	ProofRefs        []routeAuthorityProofRef `yaml:"proof_refs"`
	Notes            string                   `yaml:"notes"`
}

type routeAuthorityProofRef struct {
	Kind      string `yaml:"kind"`
	Name      string `yaml:"name,omitempty"`
	Path      string `yaml:"path,omitempty"`
	Ref       string `yaml:"ref,omitempty"`
	Method    string `yaml:"method,omitempty"`
	Issue     int    `yaml:"issue,omitempty"`
	PR        int    `yaml:"pr,omitempty"`
	Watchlist string `yaml:"watchlist,omitempty"`
	Command   string `yaml:"command,omitempty"`
}

type routeAuthorityValidationContext struct {
	openRPCMethods map[string]bool
	goTests        map[string]bool
}

func TestRouteAuthorityMatrixCoversApprovedRows(t *testing.T) {
	root := conformanceRepoRoot(t)
	matrix := loadRouteAuthorityMatrix(t, root)
	ctx := newRouteAuthorityValidationContext(t, root, matrix)

	if problems := validateRouteAuthorityMatrix(root, matrix, ctx); len(problems) > 0 {
		t.Fatalf("route authority matrix validation failed:\n- %s", strings.Join(problems, "\n- "))
	}
}

func TestRouteAuthorityMatrixRejectsStaleReferences(t *testing.T) {
	root := conformanceRepoRoot(t)
	base := loadRouteAuthorityMatrix(t, root)
	ctx := newRouteAuthorityValidationContext(t, root, base)

	tests := []struct {
		name   string
		mutate func(*routeAuthorityMatrix)
		want   string
	}{
		{
			name: "stale go test",
			mutate: func(matrix *routeAuthorityMatrix) {
				row := routeAuthorityMatrixRowByID(t, matrix, "eventbus_route_plan_model")
				row.ProofRefs[1].Name = "TestMissingRouteAuthorityProof"
			},
			want: "eventbus_route_plan_model go_test proof_ref TestMissingRouteAuthorityProof does not resolve",
		},
		{
			name: "split sibling cannot be reclassified as covered",
			mutate: func(matrix *routeAuthorityMatrix) {
				row := routeAuthorityMatrixRowByID(t, matrix, "workflow_node_execution_admission")
				row.Classification = "already_covered_by_existing_proof"
				row.Tier = "required_pr_smoke"
				row.SplitIssue = 0
				row.ProofDimensions = []string{"persistence_authority"}
			},
			want: "workflow_node_execution_admission must remain split-open issue #1293",
		},
		{
			name: "parent closure stays false",
			mutate: func(matrix *routeAuthorityMatrix) {
				matrix.Policy.ClaimsParentClosure = true
			},
			want: "policy claims_parent_closure = true, want false",
		},
		{
			name: "negative authority row must name invalid carriers",
			mutate: func(matrix *routeAuthorityMatrix) {
				row := routeAuthorityMatrixRowByID(t, matrix, "live_carriers_handler_lookup_descriptors_not_authority")
				row.InvalidAuthority = nil
			},
			want: "live_carriers_handler_lookup_descriptors_not_authority negative authority row missing invalid_authority",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			matrix := base
			matrix.Rows = append([]routeAuthorityMatrixRow(nil), base.Rows...)
			tc.mutate(&matrix)
			problems := validateRouteAuthorityMatrix(root, matrix, ctx)
			if !routeAuthorityProblemsContain(problems, tc.want) {
				t.Fatalf("validation problems missing %q:\n- %s", tc.want, strings.Join(problems, "\n- "))
			}
		})
	}
}

func loadRouteAuthorityMatrix(t *testing.T, root string) routeAuthorityMatrix {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, routeAuthorityMatrixPath))
	if err != nil {
		t.Fatalf("read route authority matrix: %v", err)
	}
	var matrix routeAuthorityMatrix
	if err := yaml.Unmarshal(raw, &matrix); err != nil {
		t.Fatalf("parse route authority matrix yaml: %v", err)
	}
	return matrix
}

func newRouteAuthorityValidationContext(t *testing.T, root string, matrix routeAuthorityMatrix) routeAuthorityValidationContext {
	t.Helper()
	return routeAuthorityValidationContext{
		openRPCMethods: loadMatrixOpenRPCMethods(t, filepath.Join(root, matrix.Source.OpenRPCArtifact)),
		goTests:        collectGoTestNames(t, root),
	}
}

func validateRouteAuthorityMatrix(root string, matrix routeAuthorityMatrix, ctx routeAuthorityValidationContext) []string {
	var problems []string
	if matrix.Version != 1 {
		problems = append(problems, fmt.Sprintf("matrix version = %d, want 1", matrix.Version))
	}
	if matrix.Kind != "route_authority_conformance_matrix" {
		problems = append(problems, fmt.Sprintf("matrix kind = %q, want route_authority_conformance_matrix", matrix.Kind))
	}
	if matrix.Issue != 1346 || matrix.ParentIssue != 1340 {
		problems = append(problems, fmt.Sprintf("matrix issue/parent = #%d/#%d, want #1346/#1340", matrix.Issue, matrix.ParentIssue))
	}
	problems = append(problems, validateRouteAuthoritySources(root, matrix.Source)...)
	problems = append(problems, validateRouteAuthorityPolicy(matrix.Policy)...)

	activeTrackers := map[string]struct{}{}
	for _, tracker := range matrix.ActiveTrackers {
		if tracker.Kind != "github_issue" {
			problems = append(problems, fmt.Sprintf("active tracker issue #%d kind = %q, want github_issue", tracker.Issue, tracker.Kind))
		}
		if tracker.Issue <= 0 {
			problems = append(problems, "active github_issue tracker missing issue")
		}
		if strings.TrimSpace(tracker.Watchlist) == "" {
			problems = append(problems, fmt.Sprintf("active tracker issue #%d missing watchlist", tracker.Issue))
		}
		key := routeAuthorityTrackerKey(tracker.Issue, tracker.Watchlist)
		if _, exists := activeTrackers[key]; exists {
			problems = append(problems, fmt.Sprintf("active tracker %s appears more than once", key))
		}
		activeTrackers[key] = struct{}{}
	}
	for _, issue := range []int{1340, 1337, 1293, 1299, 1301} {
		key := routeAuthorityTrackerKey(issue, "runtime_operations.delivery_and_replay_ownership")
		if _, ok := activeTrackers[key]; !ok {
			problems = append(problems, fmt.Sprintf("active_trackers missing #%d runtime_operations.delivery_and_replay_ownership", issue))
		}
	}

	rowsByID := map[string]routeAuthorityMatrixRow{}
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
		problems = append(problems, validateRouteAuthorityRow(root, row, ctx, activeTrackers)...)
	}
	for _, rowID := range requiredRouteAuthorityRows() {
		if _, ok := rowsByID[rowID]; !ok {
			problems = append(problems, fmt.Sprintf("matrix missing required row %s", rowID))
		}
	}
	for rowID, splitIssue := range requiredRouteAuthoritySplitRows() {
		if row, ok := rowsByID[rowID]; ok {
			if row.Classification != "split_to_existing_issue" || row.Tier != "split_open" || row.SplitIssue != splitIssue {
				problems = append(problems, fmt.Sprintf("%s must remain split-open issue #%d", rowID, splitIssue))
			}
		}
	}
	for _, rowID := range requiredRouteAuthorityNegativeRows() {
		if row, ok := rowsByID[rowID]; ok && len(row.InvalidAuthority) == 0 {
			problems = append(problems, fmt.Sprintf("%s negative authority row missing invalid_authority", rowID))
		}
	}
	sort.Strings(problems)
	return problems
}

func validateRouteAuthoritySources(root string, source routeAuthorityMatrixSource) []string {
	expected := map[string]string{
		"platform_spec":                 "platform-spec.yaml",
		"openrpc_artifact":              "openrpc.json",
		"public_surface_backend_matrix": "internal/apiv1/testdata/public_surface_backend_matrix.yaml",
		"multi_bundle_matrix":           "internal/runtime/conformance/testdata/multi_bundle_option_a_matrix.yaml",
		"catalog_tiers":                 "internal/runtime/cataloge2e/README.md",
	}
	actual := map[string]string{
		"platform_spec":                 source.PlatformSpec,
		"openrpc_artifact":              source.OpenRPCArtifact,
		"public_surface_backend_matrix": source.PublicSurfaceBackendMatrix,
		"multi_bundle_matrix":           source.MultiBundleMatrix,
		"catalog_tiers":                 source.CatalogTiers,
	}
	var problems []string
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

func validateRouteAuthorityPolicy(policy routeAuthorityMatrixPolicy) []string {
	var problems []string
	if policy.ClosureLevel != "matrix_owner_first_slice_complete" {
		problems = append(problems, fmt.Sprintf("policy closure_level = %q, want matrix_owner_first_slice_complete", policy.ClosureLevel))
	}
	if policy.ClaimsParentClosure {
		problems = append(problems, "policy claims_parent_closure = true, want false")
	}
	command := strings.TrimSpace(policy.NamedFullConformanceCommand)
	for _, fragment := range []string{
		"go test",
		"./internal/runtime/bus",
		"./internal/runtime/pipeline",
		"./internal/apiv1",
		"./cmd/swarm",
		"./internal/dashboard/server",
		"./internal/runtime/cataloge2e",
		"-count=1",
	} {
		if !strings.Contains(command, fragment) {
			problems = append(problems, fmt.Sprintf("policy named_full_conformance_command missing %q", fragment))
		}
	}
	if strings.TrimSpace(policy.RequiredSmokePolicy) == "" {
		problems = append(problems, "policy required_smoke_policy missing")
	}
	return problems
}

func validateRouteAuthorityRow(root string, row routeAuthorityMatrixRow, ctx routeAuthorityValidationContext, activeTrackers map[string]struct{}) []string {
	var problems []string
	id := strings.TrimSpace(row.ID)
	if strings.TrimSpace(row.Concept) == "" {
		problems = append(problems, fmt.Sprintf("%s missing concept", id))
	}
	if _, ok := allowedRouteAuthorityClassifications()[row.Classification]; !ok {
		problems = append(problems, fmt.Sprintf("%s classification %q is not allowed", id, row.Classification))
	}
	if _, ok := allowedRouteAuthorityTiers()[row.Tier]; !ok {
		problems = append(problems, fmt.Sprintf("%s tier %q is not allowed", id, row.Tier))
	}
	for _, dimension := range row.ProofDimensions {
		if _, ok := allowedRouteAuthorityProofDimensions()[dimension]; !ok {
			problems = append(problems, fmt.Sprintf("%s proof_dimension %q is not allowed", id, dimension))
		}
	}
	if len(row.OwnerRefs) == 0 {
		problems = append(problems, fmt.Sprintf("%s missing owner_refs", id))
	}
	if len(row.ProofRefs) == 0 {
		problems = append(problems, fmt.Sprintf("%s missing proof_refs", id))
	}
	if routeAuthorityHasValue(row.ProofDimensions, "negative_authority") && len(row.InvalidAuthority) == 0 {
		problems = append(problems, fmt.Sprintf("%s negative authority row missing invalid_authority", id))
	}
	if row.Classification == "split_to_existing_issue" {
		if row.SplitIssue == 0 {
			problems = append(problems, fmt.Sprintf("%s split row missing split_issue", id))
		}
		if row.Tier != "split_open" {
			problems = append(problems, fmt.Sprintf("%s split row tier = %q, want split_open", id, row.Tier))
		}
		if !routeAuthorityHasValue(row.ProofDimensions, "split_tracker") {
			problems = append(problems, fmt.Sprintf("%s split row missing split_tracker proof_dimension", id))
		}
	}
	if row.Classification == "obsolete_duplicate" && strings.TrimSpace(row.Replacement) == "" {
		problems = append(problems, fmt.Sprintf("%s obsolete duplicate row missing replacement", id))
	}

	seenSplitTracker := false
	seenExecutableProof := false
	for _, ref := range append(row.OwnerRefs, row.ProofRefs...) {
		problems = append(problems, validateRouteAuthorityProofRef(root, id, ref, ctx, activeTrackers)...)
		if routeAuthorityExecutableProofKind(ref.Kind) {
			seenExecutableProof = true
		}
		if ref.Kind == "tracker" && row.SplitIssue != 0 && ref.Issue == row.SplitIssue {
			seenSplitTracker = true
		}
	}
	if row.Classification == "split_to_existing_issue" && row.SplitIssue != 0 && !seenSplitTracker {
		problems = append(problems, fmt.Sprintf("%s split row missing tracker proof_ref for issue #%d", id, row.SplitIssue))
	}
	if row.Tier == "required_pr_smoke" && row.Classification != "obsolete_duplicate" && !seenExecutableProof {
		problems = append(problems, fmt.Sprintf("%s required_pr_smoke row requires at least one executable proof ref", id))
	}
	return problems
}

func validateRouteAuthorityProofRef(root, rowID string, ref routeAuthorityProofRef, ctx routeAuthorityValidationContext, activeTrackers map[string]struct{}) []string {
	var problems []string
	switch ref.Kind {
	case "artifact":
		if strings.TrimSpace(ref.Path) == "" {
			problems = append(problems, fmt.Sprintf("%s artifact proof_ref missing path", rowID))
			return problems
		}
		cleaned := filepath.Clean(ref.Path)
		if filepath.IsAbs(ref.Path) || strings.HasPrefix(cleaned, "..") {
			problems = append(problems, fmt.Sprintf("%s artifact proof_ref path %s must be repo-relative", rowID, ref.Path))
			return problems
		}
		if _, err := os.Stat(filepath.Join(root, cleaned)); err != nil {
			problems = append(problems, fmt.Sprintf("%s artifact proof_ref path %s does not exist", rowID, ref.Path))
		}
	case "spec_ref":
		if strings.TrimSpace(ref.Path) == "" || strings.TrimSpace(ref.Ref) == "" {
			problems = append(problems, fmt.Sprintf("%s spec_ref proof_ref missing path/ref", rowID))
			return problems
		}
		raw, err := os.ReadFile(filepath.Join(root, filepath.Clean(ref.Path)))
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s spec_ref path %s cannot be read: %v", rowID, ref.Path, err))
			return problems
		}
		if !strings.Contains(string(raw), ref.Ref) {
			problems = append(problems, fmt.Sprintf("%s spec_ref %s#%s does not resolve by text search", rowID, ref.Path, ref.Ref))
		}
	case "go_test":
		if strings.TrimSpace(ref.Name) == "" {
			problems = append(problems, fmt.Sprintf("%s go_test proof_ref missing name", rowID))
			return problems
		}
		if !ctx.goTests[ref.Name] {
			problems = append(problems, fmt.Sprintf("%s go_test proof_ref %s does not resolve", rowID, ref.Name))
		}
	case "openrpc_method":
		if strings.TrimSpace(ref.Method) == "" {
			problems = append(problems, fmt.Sprintf("%s openrpc_method proof_ref missing method", rowID))
			return problems
		}
		if !ctx.openRPCMethods[ref.Method] {
			problems = append(problems, fmt.Sprintf("%s openrpc_method proof_ref %s does not resolve", rowID, ref.Method))
		}
	case "tracker":
		if ref.Issue <= 0 {
			problems = append(problems, fmt.Sprintf("%s tracker proof_ref missing issue", rowID))
		}
		if _, ok := activeTrackers[routeAuthorityTrackerKey(ref.Issue, ref.Watchlist)]; !ok {
			problems = append(problems, fmt.Sprintf("%s tracker proof_ref issue #%d watchlist %q is not in active_trackers", rowID, ref.Issue, ref.Watchlist))
		}
	case "issue":
		if ref.Issue <= 0 {
			problems = append(problems, fmt.Sprintf("%s issue proof_ref missing issue", rowID))
		}
	case "pr":
		if ref.PR <= 0 {
			problems = append(problems, fmt.Sprintf("%s pr proof_ref missing pr", rowID))
		}
	case "command":
		command := strings.TrimSpace(ref.Command)
		if command == "" {
			problems = append(problems, fmt.Sprintf("%s command proof_ref missing command", rowID))
		}
		if command != "" && !strings.Contains(command, "go test ") && !strings.HasPrefix(command, "go run ") {
			problems = append(problems, fmt.Sprintf("%s command proof_ref command = %q, want go test or go run command", rowID, command))
		}
	default:
		problems = append(problems, fmt.Sprintf("%s proof_ref kind %q is not allowed", rowID, ref.Kind))
	}
	return problems
}

func requiredRouteAuthorityRows() []string {
	return []string{
		"route_plan_spec_contract",
		"eventbus_route_plan_model",
		"publish_preflight_deferred_outbox_consumers",
		"public_operator_typed_readback",
		"served_root_input_event_publish_supported_surfaces",
		"workflow_node_execution_admission",
		"wildcard_static_service_route_production",
		"runtime_callback_flow_instance_localization",
		"live_carriers_handler_lookup_descriptors_not_authority",
		"receipts_settlement_backfill_not_authority",
		"replay_markers_not_authority",
		"readback_not_authority",
		"subscriber_id_only_not_authority",
		"replay_recovery_fork_consumers",
		"catalog_e2e_route_behavior",
		"diagnostic_catalog_probe_duplicate",
	}
}

func requiredRouteAuthoritySplitRows() map[string]int {
	return map[string]int{
		"served_root_input_event_publish_supported_surfaces": 1337,
		"workflow_node_execution_admission":                  1293,
		"wildcard_static_service_route_production":           1299,
		"runtime_callback_flow_instance_localization":        1301,
	}
}

func requiredRouteAuthorityNegativeRows() []string {
	return []string{
		"live_carriers_handler_lookup_descriptors_not_authority",
		"receipts_settlement_backfill_not_authority",
		"replay_markers_not_authority",
		"readback_not_authority",
		"subscriber_id_only_not_authority",
	}
}

func allowedRouteAuthorityClassifications() map[string]struct{} {
	return map[string]struct{}{
		"add_to_matrix":                     {},
		"already_covered_by_existing_proof": {},
		"obsolete_duplicate":                {},
		"split_to_existing_issue":           {},
	}
}

func allowedRouteAuthorityTiers() map[string]struct{} {
	return map[string]struct{}{
		"required_pr_smoke":               {},
		"full_conformance_manual_nightly": {},
		"touched_surface_only":            {},
		"split_open":                      {},
		"obsolete_duplicate":              {},
	}
}

func allowedRouteAuthorityProofDimensions() map[string]struct{} {
	return map[string]struct{}{
		"catalog_tiering":       {},
		"cli_v1_path":           {},
		"default_sqlite":        {},
		"explicit_postgres":     {},
		"full_conformance":      {},
		"negative_authority":    {},
		"openrpc_publication":   {},
		"obsolete_duplicate":    {},
		"persistence_authority": {},
		"public_projection":     {},
		"real_v1_handler":       {},
		"replay_recovery":       {},
		"required_pr_smoke":     {},
		"route_plan_model":      {},
		"source_authority":      {},
		"split_tracker":         {},
	}
}

func routeAuthorityMatrixRowByID(t *testing.T, matrix *routeAuthorityMatrix, id string) *routeAuthorityMatrixRow {
	t.Helper()
	for i := range matrix.Rows {
		if matrix.Rows[i].ID == id {
			return &matrix.Rows[i]
		}
	}
	t.Fatalf("matrix row %s not found", id)
	return nil
}

func routeAuthorityTrackerKey(issue int, watchlist string) string {
	return fmt.Sprintf("%d:%s", issue, strings.TrimSpace(watchlist))
}

func routeAuthorityHasValue(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func routeAuthorityExecutableProofKind(kind string) bool {
	switch kind {
	case "go_test", "openrpc_method", "command":
		return true
	default:
		return false
	}
}

func routeAuthorityProblemsContain(problems []string, want string) bool {
	for _, problem := range problems {
		if strings.Contains(problem, want) {
			return true
		}
	}
	return false
}
