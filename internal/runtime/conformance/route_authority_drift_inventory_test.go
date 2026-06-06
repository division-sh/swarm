package conformance

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const routeAuthorityDriftInventoryPath = "internal/runtime/conformance/testdata/route_authority_drift_inventory.yaml"

type routeAuthorityDriftInventory struct {
	Version            int                                  `yaml:"version"`
	Kind               string                               `yaml:"kind"`
	Issue              int                                  `yaml:"issue"`
	ParentIssues       []int                                `yaml:"parent_issues"`
	Watchlist          string                               `yaml:"watchlist"`
	Policy             routeAuthorityDriftInventoryPolicy   `yaml:"policy"`
	Sources            routeAuthorityDriftInventorySources  `yaml:"sources"`
	SearchDimensions   []routeAuthorityDriftSearchDimension `yaml:"search_dimensions"`
	SeamFamilies       []routeAuthorityDriftSeamFamily      `yaml:"seam_families"`
	GuardrailProposals []routeAuthorityDriftGuardrail       `yaml:"guardrail_proposals"`
	FollowUpDecision   routeAuthorityDriftFollowUpDecision  `yaml:"follow_up_decision"`
}

type routeAuthorityDriftInventoryPolicy struct {
	ClosureLevel                  string `yaml:"closure_level"`
	ClaimsParentClosure           bool   `yaml:"claims_parent_closure"`
	ClaimsRuntimeBehaviorClosure  bool   `yaml:"claims_runtime_behavior_closure"`
	RuntimeBehaviorChangesAllowed bool   `yaml:"runtime_behavior_changes_allowed"`
	ExhaustiveRequirement         string `yaml:"exhaustive_requirement"`
}

type routeAuthorityDriftInventorySources struct {
	PlatformSpec               string `yaml:"platform_spec"`
	RouteAuthorityMatrix       string `yaml:"route_authority_matrix"`
	OpenRPCArtifact            string `yaml:"openrpc_artifact"`
	PublicSurfaceBackendMatrix string `yaml:"public_surface_backend_matrix"`
	WatchlistNote              string `yaml:"watchlist_note"`
}

type routeAuthorityDriftSearchDimension struct {
	ID                   string   `yaml:"id"`
	Pattern              string   `yaml:"pattern"`
	MinimumMatchingFiles int      `yaml:"minimum_matching_files"`
	RequiredPaths        []string `yaml:"required_paths"`
	CanonicalLayer       string   `yaml:"canonical_layer"`
}

type routeAuthorityDriftSeamFamily struct {
	ID               string   `yaml:"id"`
	Layer            string   `yaml:"layer"`
	Classification   string   `yaml:"classification"`
	Paths            []string `yaml:"paths"`
	SearchDimensions []string `yaml:"search_dimensions"`
	InvalidAuthority []string `yaml:"invalid_authority"`
	Notes            string   `yaml:"notes"`
}

type routeAuthorityDriftGuardrail struct {
	ID                  string   `yaml:"id"`
	ImplementationState string   `yaml:"implementation_state"`
	Target              string   `yaml:"target"`
	Prevents            []string `yaml:"prevents"`
}

type routeAuthorityDriftFollowUpDecision struct {
	TrackerState                    string `yaml:"tracker_state"`
	NewChildRequiredBeforeCoding    bool   `yaml:"new_child_required_before_coding"`
	RuntimeBehaviorChildRequiredNow bool   `yaml:"runtime_behavior_child_required_now"`
	Notes                           string `yaml:"notes"`
}

type routeAuthorityDriftRepoFile struct {
	Path string
	Raw  []byte
}

type routeAuthorityDriftValidationCorpus struct {
	Files         []routeAuthorityDriftRepoFile
	MatchesByExpr map[string][]string
}

func TestRouteAuthorityDriftInventoryCoversRepoWideSearchDimensions(t *testing.T) {
	root := conformanceRepoRoot(t)
	inventory := loadRouteAuthorityDriftInventory(t, root)
	corpus := routeAuthorityDriftNewValidationCorpus(root)
	if problems := validateRouteAuthorityDriftInventoryWithCorpus(root, corpus, inventory); len(problems) > 0 {
		t.Fatalf("route authority drift inventory validation failed:\n- %s", strings.Join(problems, "\n- "))
	}
}

func TestRouteAuthorityDriftInventoryRejectsNarrowOrStaleAudit(t *testing.T) {
	root := conformanceRepoRoot(t)
	base := loadRouteAuthorityDriftInventory(t, root)
	corpus := routeAuthorityDriftNewValidationCorpus(root)

	tests := []struct {
		name   string
		mutate func(*routeAuthorityDriftInventory)
		want   string
	}{
		{
			name: "missing required search dimension",
			mutate: func(inventory *routeAuthorityDriftInventory) {
				inventory.SearchDimensions = routeAuthorityDriftSearchDimensionsExcept(inventory.SearchDimensions, "event_deliveries")
			},
			want: "missing required search_dimension event_deliveries",
		},
		{
			name: "required path must match the audited pattern",
			mutate: func(inventory *routeAuthorityDriftInventory) {
				dim := routeAuthorityDriftSearchDimensionByID(t, inventory, "event_deliveries")
				dim.RequiredPaths = []string{"go.mod"}
			},
			want: "event_deliveries required_path go.mod does not match pattern",
		},
		{
			name: "no implemented guardrail is not enough",
			mutate: func(inventory *routeAuthorityDriftInventory) {
				for i := range inventory.GuardrailProposals {
					inventory.GuardrailProposals[i].ImplementationState = "ready_for_followup"
				}
			},
			want: "guardrail_proposals missing implemented_in_this_pr guardrail",
		},
		{
			name: "runtime behavior closure claim is invalid",
			mutate: func(inventory *routeAuthorityDriftInventory) {
				inventory.Policy.ClaimsRuntimeBehaviorClosure = true
			},
			want: "policy claims_runtime_behavior_closure = true, want false",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inventory := base
			inventory.SearchDimensions = append([]routeAuthorityDriftSearchDimension(nil), base.SearchDimensions...)
			inventory.SeamFamilies = append([]routeAuthorityDriftSeamFamily(nil), base.SeamFamilies...)
			inventory.GuardrailProposals = append([]routeAuthorityDriftGuardrail(nil), base.GuardrailProposals...)
			tc.mutate(&inventory)
			problems := validateRouteAuthorityDriftInventoryWithCorpus(root, corpus, inventory)
			if !routeAuthorityProblemsContain(problems, tc.want) {
				t.Fatalf("validation problems missing %q:\n- %s", tc.want, strings.Join(problems, "\n- "))
			}
		})
	}
}

func loadRouteAuthorityDriftInventory(t *testing.T, root string) routeAuthorityDriftInventory {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, routeAuthorityDriftInventoryPath))
	if err != nil {
		t.Fatalf("read route authority drift inventory: %v", err)
	}
	var inventory routeAuthorityDriftInventory
	if err := yaml.Unmarshal(raw, &inventory); err != nil {
		t.Fatalf("parse route authority drift inventory yaml: %v", err)
	}
	return inventory
}

func validateRouteAuthorityDriftInventory(root string, inventory routeAuthorityDriftInventory) []string {
	corpus := routeAuthorityDriftNewValidationCorpus(root)
	return validateRouteAuthorityDriftInventoryWithCorpus(root, corpus, inventory)
}

func validateRouteAuthorityDriftInventoryWithCorpus(root string, corpus *routeAuthorityDriftValidationCorpus, inventory routeAuthorityDriftInventory) []string {
	var problems []string
	if inventory.Version != 1 {
		problems = append(problems, fmt.Sprintf("inventory version = %d, want 1", inventory.Version))
	}
	if inventory.Kind != "route_authority_drift_inventory" {
		problems = append(problems, fmt.Sprintf("inventory kind = %q, want route_authority_drift_inventory", inventory.Kind))
	}
	if inventory.Issue != 1364 {
		problems = append(problems, fmt.Sprintf("inventory issue = #%d, want #1364", inventory.Issue))
	}
	for _, issue := range []int{1340, 1353} {
		if !routeAuthorityDriftHasInt(inventory.ParentIssues, issue) {
			problems = append(problems, fmt.Sprintf("inventory parent_issues missing #%d", issue))
		}
	}
	if inventory.Watchlist != "runtime_operations.delivery_and_replay_ownership" {
		problems = append(problems, fmt.Sprintf("inventory watchlist = %q, want runtime_operations.delivery_and_replay_ownership", inventory.Watchlist))
	}
	problems = append(problems, validateRouteAuthorityDriftPolicy(inventory.Policy)...)
	problems = append(problems, validateRouteAuthorityDriftSources(root, inventory.Sources)...)

	dimensionsByID := map[string]routeAuthorityDriftSearchDimension{}
	for _, dimension := range inventory.SearchDimensions {
		id := strings.TrimSpace(dimension.ID)
		if id == "" {
			problems = append(problems, "search_dimension missing id")
			continue
		}
		if _, exists := dimensionsByID[id]; exists {
			problems = append(problems, fmt.Sprintf("search_dimension %s appears more than once", id))
		}
		dimensionsByID[id] = dimension
		problems = append(problems, validateRouteAuthorityDriftSearchDimension(root, corpus, dimension)...)
	}
	for _, id := range requiredRouteAuthorityDriftSearchDimensions() {
		if _, ok := dimensionsByID[id]; !ok {
			problems = append(problems, fmt.Sprintf("missing required search_dimension %s", id))
		}
	}

	familiesByID := map[string]routeAuthorityDriftSeamFamily{}
	for _, family := range inventory.SeamFamilies {
		id := strings.TrimSpace(family.ID)
		if id == "" {
			problems = append(problems, "seam_family missing id")
			continue
		}
		if _, exists := familiesByID[id]; exists {
			problems = append(problems, fmt.Sprintf("seam_family %s appears more than once", id))
		}
		familiesByID[id] = family
		problems = append(problems, validateRouteAuthorityDriftSeamFamily(root, family, dimensionsByID)...)
	}
	for _, id := range requiredRouteAuthorityDriftSeamFamilies() {
		if _, ok := familiesByID[id]; !ok {
			problems = append(problems, fmt.Sprintf("missing required seam_family %s", id))
		}
	}
	problems = append(problems, validateRouteAuthorityDriftGuardrails(root, inventory.GuardrailProposals)...)
	if inventory.FollowUpDecision.NewChildRequiredBeforeCoding {
		problems = append(problems, "follow_up_decision new_child_required_before_coding = true, want false for approved audit slice")
	}
	if inventory.FollowUpDecision.RuntimeBehaviorChildRequiredNow {
		problems = append(problems, "follow_up_decision runtime_behavior_child_required_now = true, want false for approved audit slice")
	}
	if strings.TrimSpace(inventory.FollowUpDecision.TrackerState) == "" {
		problems = append(problems, "follow_up_decision tracker_state missing")
	}
	if strings.TrimSpace(inventory.FollowUpDecision.Notes) == "" {
		problems = append(problems, "follow_up_decision notes missing")
	}
	sort.Strings(problems)
	return problems
}

func validateRouteAuthorityDriftPolicy(policy routeAuthorityDriftInventoryPolicy) []string {
	var problems []string
	if policy.ClosureLevel != "repo_wide_inventory_and_guardrail_proposal" {
		problems = append(problems, fmt.Sprintf("policy closure_level = %q, want repo_wide_inventory_and_guardrail_proposal", policy.ClosureLevel))
	}
	if policy.ClaimsParentClosure {
		problems = append(problems, "policy claims_parent_closure = true, want false")
	}
	if policy.ClaimsRuntimeBehaviorClosure {
		problems = append(problems, "policy claims_runtime_behavior_closure = true, want false")
	}
	if policy.RuntimeBehaviorChangesAllowed {
		problems = append(problems, "policy runtime_behavior_changes_allowed = true, want false")
	}
	if strings.TrimSpace(policy.ExhaustiveRequirement) == "" {
		problems = append(problems, "policy exhaustive_requirement missing")
	}
	for _, fragment := range []string{"producers", "consumers", "interpreters", "test fixtures"} {
		if !strings.Contains(policy.ExhaustiveRequirement, fragment) {
			problems = append(problems, fmt.Sprintf("policy exhaustive_requirement missing %q", fragment))
		}
	}
	return problems
}

func validateRouteAuthorityDriftSources(root string, source routeAuthorityDriftInventorySources) []string {
	expected := map[string]string{
		"platform_spec":                 source.PlatformSpec,
		"route_authority_matrix":        source.RouteAuthorityMatrix,
		"openrpc_artifact":              source.OpenRPCArtifact,
		"public_surface_backend_matrix": source.PublicSurfaceBackendMatrix,
	}
	var problems []string
	for name, path := range expected {
		if strings.TrimSpace(path) == "" {
			problems = append(problems, fmt.Sprintf("source %s missing", name))
			continue
		}
		if _, err := os.Stat(filepath.Join(root, filepath.Clean(path))); err != nil {
			problems = append(problems, fmt.Sprintf("source %s path %s does not exist", name, path))
		}
	}
	if !strings.HasPrefix(source.WatchlistNote, "private:") || !strings.Contains(source.WatchlistNote, "delivery_and_replay_ownership") {
		problems = append(problems, "source watchlist_note must reference private delivery_and_replay_ownership mapping")
	}
	return problems
}

func validateRouteAuthorityDriftSearchDimension(root string, corpus *routeAuthorityDriftValidationCorpus, dimension routeAuthorityDriftSearchDimension) []string {
	var problems []string
	id := strings.TrimSpace(dimension.ID)
	pattern := strings.TrimSpace(dimension.Pattern)
	if pattern == "" {
		return append(problems, fmt.Sprintf("%s search_dimension missing pattern", id))
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return append(problems, fmt.Sprintf("%s search_dimension pattern %q does not compile: %v", id, pattern, err))
	}
	if _, ok := allowedRouteAuthorityDriftLayers()[dimension.CanonicalLayer]; !ok {
		problems = append(problems, fmt.Sprintf("%s canonical_layer %q is not allowed", id, dimension.CanonicalLayer))
	}
	matches := routeAuthorityDriftMatchingFiles(corpus, pattern, re)
	if len(matches) < dimension.MinimumMatchingFiles {
		problems = append(problems, fmt.Sprintf("%s matched %d files, want at least %d", id, len(matches), dimension.MinimumMatchingFiles))
	}
	for _, path := range dimension.RequiredPaths {
		if strings.TrimSpace(path) == "" {
			problems = append(problems, fmt.Sprintf("%s required_path missing", id))
			continue
		}
		if !routeAuthorityDriftPathMatches(root, path, re) {
			problems = append(problems, fmt.Sprintf("%s required_path %s does not match pattern", id, path))
		}
	}
	return problems
}

func validateRouteAuthorityDriftSeamFamily(root string, family routeAuthorityDriftSeamFamily, dimensions map[string]routeAuthorityDriftSearchDimension) []string {
	var problems []string
	id := strings.TrimSpace(family.ID)
	if _, ok := allowedRouteAuthorityDriftLayers()[family.Layer]; !ok {
		problems = append(problems, fmt.Sprintf("%s layer %q is not allowed", id, family.Layer))
	}
	if _, ok := allowedRouteAuthorityDriftClassifications()[family.Classification]; !ok {
		problems = append(problems, fmt.Sprintf("%s classification %q is not allowed", id, family.Classification))
	}
	if len(family.Paths) == 0 {
		problems = append(problems, fmt.Sprintf("%s missing paths", id))
	}
	for _, path := range family.Paths {
		if _, err := os.Stat(filepath.Join(root, filepath.Clean(path))); err != nil {
			problems = append(problems, fmt.Sprintf("%s path %s does not exist", id, path))
		}
	}
	if len(family.SearchDimensions) == 0 {
		problems = append(problems, fmt.Sprintf("%s missing search_dimensions", id))
	}
	for _, dimension := range family.SearchDimensions {
		if _, ok := dimensions[dimension]; !ok {
			problems = append(problems, fmt.Sprintf("%s references unknown search_dimension %s", id, dimension))
		}
	}
	if strings.Contains(family.Layer, "non_authority") && len(family.InvalidAuthority) == 0 {
		problems = append(problems, fmt.Sprintf("%s non-authority family missing invalid_authority", id))
	}
	if strings.TrimSpace(family.Notes) == "" {
		problems = append(problems, fmt.Sprintf("%s missing notes", id))
	}
	return problems
}

func validateRouteAuthorityDriftGuardrails(root string, guardrails []routeAuthorityDriftGuardrail) []string {
	var problems []string
	if len(guardrails) == 0 {
		return append(problems, "guardrail_proposals missing")
	}
	implemented := false
	readyForFollowup := false
	for _, guardrail := range guardrails {
		id := strings.TrimSpace(guardrail.ID)
		if id == "" {
			problems = append(problems, "guardrail missing id")
		}
		switch guardrail.ImplementationState {
		case "implemented_in_this_pr":
			implemented = true
		case "ready_for_followup":
			readyForFollowup = true
		default:
			problems = append(problems, fmt.Sprintf("%s implementation_state %q is not allowed", id, guardrail.ImplementationState))
		}
		if strings.TrimSpace(guardrail.Target) == "" {
			problems = append(problems, fmt.Sprintf("%s guardrail missing target", id))
		} else if !strings.Contains(guardrail.Target, "conformance") {
			problems = append(problems, fmt.Sprintf("%s guardrail target %q must be conformance-backed", id, guardrail.Target))
		} else if guardrail.ImplementationState == "implemented_in_this_pr" {
			if _, err := os.Stat(filepath.Join(root, filepath.Clean(guardrail.Target))); err != nil {
				problems = append(problems, fmt.Sprintf("%s implemented guardrail target %s does not exist", id, guardrail.Target))
			}
		}
		if len(guardrail.Prevents) == 0 {
			problems = append(problems, fmt.Sprintf("%s guardrail missing prevents", id))
		}
	}
	if !implemented {
		problems = append(problems, "guardrail_proposals missing implemented_in_this_pr guardrail")
	}
	if !readyForFollowup {
		problems = append(problems, "guardrail_proposals missing ready_for_followup guardrail")
	}
	return problems
}

func routeAuthorityDriftNewValidationCorpus(root string) *routeAuthorityDriftValidationCorpus {
	return &routeAuthorityDriftValidationCorpus{
		Files:         routeAuthorityDriftRepoFiles(root),
		MatchesByExpr: map[string][]string{},
	}
}

func routeAuthorityDriftRepoFiles(root string) []routeAuthorityDriftRepoFile {
	var files []routeAuthorityDriftRepoFile
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor", "node_modules", "tmp":
				return filepath.SkipDir
			}
			return nil
		}
		if !routeAuthorityDriftScannableFile(path) {
			return nil
		}
		if rel, err := filepath.Rel(root, path); err == nil {
			relPath := filepath.ToSlash(rel)
			if routeAuthorityDriftSelfAuditFile(relPath) {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return nil
			}
			files = append(files, routeAuthorityDriftRepoFile{Path: relPath, Raw: raw})
		}
		return nil
	})
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})
	return files
}

func routeAuthorityDriftMatchingFiles(corpus *routeAuthorityDriftValidationCorpus, pattern string, re *regexp.Regexp) []string {
	if matches, ok := corpus.MatchesByExpr[pattern]; ok {
		return matches
	}
	var matches []string
	for _, file := range corpus.Files {
		if re.Match(file.Raw) {
			matches = append(matches, file.Path)
		}
	}
	sort.Strings(matches)
	corpus.MatchesByExpr[pattern] = matches
	return matches
}

func routeAuthorityDriftSelfAuditFile(path string) bool {
	switch path {
	case routeAuthorityDriftInventoryPath, "internal/runtime/conformance/route_authority_drift_inventory_test.go":
		return true
	default:
		return false
	}
}

func routeAuthorityDriftPathMatches(root, path string, re *regexp.Regexp) bool {
	raw, err := os.ReadFile(filepath.Join(root, filepath.Clean(path)))
	if err != nil {
		return false
	}
	return re.Match(raw)
}

func routeAuthorityDriftScannableFile(path string) bool {
	switch filepath.Ext(path) {
	case ".go", ".yaml", ".yml", ".json", ".md":
		return true
	default:
		return false
	}
}

func routeAuthorityDriftSearchDimensionByID(t *testing.T, inventory *routeAuthorityDriftInventory, id string) *routeAuthorityDriftSearchDimension {
	t.Helper()
	for i := range inventory.SearchDimensions {
		if inventory.SearchDimensions[i].ID == id {
			return &inventory.SearchDimensions[i]
		}
	}
	t.Fatalf("search dimension %s not found", id)
	return nil
}

func routeAuthorityDriftSearchDimensionsExcept(dimensions []routeAuthorityDriftSearchDimension, exclude string) []routeAuthorityDriftSearchDimension {
	out := make([]routeAuthorityDriftSearchDimension, 0, len(dimensions))
	for _, dimension := range dimensions {
		if dimension.ID != exclude {
			out = append(out, dimension)
		}
	}
	return out
}

func routeAuthorityDriftHasInt(values []int, want int) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func requiredRouteAuthorityDriftSearchDimensions() []string {
	return []string{
		"event_deliveries",
		"delivery_route_model",
		"route_plan",
		"flow_instance",
		"event_name_type",
		"semantic_scope",
		"entity_context",
		"run_source_event_context",
		"route_event_key_derivation",
		"target_set",
		"target_context",
		"workflow_runtime_carrier",
		"internal_subscriptions",
		"event_receipts",
		"settlement_backfill",
		"replay_scope",
		"handler_descriptor_lookup",
		"public_readback",
		"delivery_target_route",
		"delivery_authority_writers",
		"execution_admission_settlement",
		"publish_plan_consumers",
	}
}

func requiredRouteAuthorityDriftSeamFamilies() []string {
	return []string{
		"spec_route_authority_contract",
		"event_context_inputs",
		"route_topology_and_table",
		"eventbus_route_plan_authority",
		"durable_delivery_authority_writers",
		"workflow_node_execution_admission",
		"live_carriers_internal_subscriptions",
		"receipts_settlement_backfill",
		"replay_recovery_and_fork_consumers",
		"public_operator_readback_projection",
		"store_and_cli_direct_sql_fixtures",
		"catalog_and_fixture_route_behavior",
		"route_authority_conformance_artifacts",
	}
}

func allowedRouteAuthorityDriftLayers() map[string]struct{} {
	return map[string]struct{}{
		"spec_authority":                    {},
		"event_context":                     {},
		"route_topology":                    {},
		"route_plan_authority":              {},
		"route_plan_consumers":              {},
		"durable_delivery_authority":        {},
		"execution_admission":               {},
		"live_dispatch_non_authority":       {},
		"completion_evidence_non_authority": {},
		"replay_recovery_consumers":         {},
		"public_projection_non_authority":   {},
		"invalid_old_authority":             {},
		"test_fixture_semantics":            {},
		"conformance_guardrails":            {},
	}
}

func allowedRouteAuthorityDriftClassifications() map[string]struct{} {
	return map[string]struct{}{
		"canonical_owner":                       {},
		"valid_consumer":                        {},
		"non_authoritative_observer_projection": {},
		"invalid_old_authority_path":            {},
		"separate_semantic_concept":             {},
		"split_open_sibling":                    {},
	}
}
