package conformance

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const deterministicWorkLadderStage0Path = "internal/runtime/conformance/testdata/deterministic_work_ladder_stage0.yaml"

type deterministicWorkLadderStage0 struct {
	Version              int                                      `yaml:"version"`
	Kind                 string                                   `yaml:"kind"`
	Issue                int                                      `yaml:"issue"`
	ParentIssue          int                                      `yaml:"parent_issue"`
	Discussion           int                                      `yaml:"discussion"`
	SourceBoundary       deterministicWorkLadderSourceBoundary    `yaml:"source_boundary"`
	Policy               deterministicWorkLadderPolicy            `yaml:"policy"`
	WatchlistNode        deterministicWorkLadderWatchlist         `yaml:"watchlist_node"`
	ClassificationValues []string                                 `yaml:"classification_values"`
	MeasurementPolicy    deterministicWorkLadderMeasurementPolicy `yaml:"measurement_policy"`
	EvidenceMatrix       []deterministicWorkLadderEvidenceRow     `yaml:"evidence_matrix"`
	GateCriteria         []deterministicWorkLadderGate            `yaml:"gate_criteria"`
}

type deterministicWorkLadderSourceBoundary struct {
	PlatformSpec           string `yaml:"platform_spec"`
	PreAuditComment        string `yaml:"pre_audit_comment"`
	GateComment            string `yaml:"gate_comment"`
	LockedDesignDiscussion string `yaml:"locked_design_discussion"`
	EmpireEvidenceRoot     string `yaml:"empire_evidence_root"`
}

type deterministicWorkLadderPolicy struct {
	ClosureLevel                  string `yaml:"closure_level"`
	ClaimsParentClosure           bool   `yaml:"claims_parent_closure"`
	ClaimsRuntimeBehaviorClosure  bool   `yaml:"claims_runtime_behavior_closure"`
	RuntimeBehaviorChangesAllowed bool   `yaml:"runtime_behavior_changes_allowed"`
	PlatformSpecChangesAllowed    bool   `yaml:"platform_spec_changes_allowed"`
	RuntimeSemanticOwner          string `yaml:"runtime_semantic_owner"`
	Stage0Owner                   string `yaml:"stage0_owner"`
	NextAfterStage1               string `yaml:"next_after_stage1"`
	OrderingBasis                 string `yaml:"ordering_basis"`
	OrderingDecision              string `yaml:"ordering_decision"`
}

type deterministicWorkLadderWatchlist struct {
	ID                   string                                 `yaml:"id"`
	Status               string                                 `yaml:"status"`
	PrivateWatchlistNote string                                 `yaml:"private_watchlist_note"`
	BroaderClass         string                                 `yaml:"broader_class"`
	NonClosureRule       string                                 `yaml:"non_closure_rule"`
	ActiveTrackers       []deterministicWorkLadderActiveTracker `yaml:"active_trackers"`
}

type deterministicWorkLadderActiveTracker struct {
	Issue     int    `yaml:"issue"`
	Role      string `yaml:"role"`
	Watchlist string `yaml:"watchlist"`
}

type deterministicWorkLadderMeasurementPolicy struct {
	RoughEstimatesAllowed    bool     `yaml:"rough_estimates_allowed"`
	AllowedStatuses          []string `yaml:"allowed_statuses"`
	DefaultUnavailableReason string   `yaml:"default_unavailable_reason"`
}

type deterministicWorkLadderEvidenceRow struct {
	InventoryID    int                                   `yaml:"inventory_id"`
	DedupeGroup    string                                `yaml:"dedupe_group"`
	Title          string                                `yaml:"title"`
	Classification string                                `yaml:"classification"`
	Disposition    string                                `yaml:"disposition"`
	DuplicateOf    int                                   `yaml:"duplicate_of"`
	ConsumerIssue  int                                   `yaml:"consumer_issue"`
	ProofRefs      []string                              `yaml:"proof_refs"`
	MetricEvidence deterministicWorkLadderMetricEvidence `yaml:"metric_evidence"`
	Notes          string                                `yaml:"notes"`
}

type deterministicWorkLadderMetricEvidence struct {
	Status string `yaml:"status"`
	Reason string `yaml:"reason"`
}

type deterministicWorkLadderGate struct {
	ID                  string   `yaml:"id"`
	ConsumerIssue       int      `yaml:"consumer_issue"`
	PromotionState      string   `yaml:"promotion_state"`
	RequiredConditions  []string `yaml:"required_conditions"`
	BlockingConditions  []string `yaml:"blocking_conditions"`
	CurrentMatrixResult string   `yaml:"current_matrix_result"`
}

func TestDeterministicWorkLadderStage0CoversGateScope(t *testing.T) {
	root := conformanceRepoRoot(t)
	artifact := loadDeterministicWorkLadderStage0(t, root)
	if problems := validateDeterministicWorkLadderStage0(artifact); len(problems) > 0 {
		t.Fatalf("deterministic work ladder Stage 0 validation failed:\n- %s", strings.Join(problems, "\n- "))
	}
}

func TestDeterministicWorkLadderStage0RejectsNarrowOrRuntimeClosure(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*deterministicWorkLadderStage0)
		want   string
	}{
		{
			name: "parent closure claim",
			mutate: func(artifact *deterministicWorkLadderStage0) {
				artifact.Policy.ClaimsParentClosure = true
			},
			want: "policy claims_parent_closure = true, want false",
		},
		{
			name: "runtime behavior claim",
			mutate: func(artifact *deterministicWorkLadderStage0) {
				artifact.Policy.ClaimsRuntimeBehaviorClosure = true
			},
			want: "policy claims_runtime_behavior_closure = true, want false",
		},
		{
			name: "missing child tracker",
			mutate: func(artifact *deterministicWorkLadderStage0) {
				artifact.WatchlistNode.ActiveTrackers = deterministicWorkLadderTrackersExcept(artifact.WatchlistNode.ActiveTrackers, 1671)
			},
			want: "watchlist active_trackers missing #1671",
		},
		{
			name: "missing inventory row",
			mutate: func(artifact *deterministicWorkLadderStage0) {
				artifact.EvidenceMatrix = deterministicWorkLadderRowsExcept(artifact.EvidenceMatrix, 13)
			},
			want: "evidence_matrix missing inventory_id 13",
		},
		{
			name: "rough estimate metric",
			mutate: func(artifact *deterministicWorkLadderStage0) {
				row := deterministicWorkLadderRowByID(t, artifact, 1)
				row.MetricEvidence.Status = "rough_estimate"
			},
			want: "inventory_id 1 metric_evidence.status = \"rough_estimate\" is not allowed",
		},
		{
			name: "self-authorized metric status",
			mutate: func(artifact *deterministicWorkLadderStage0) {
				artifact.MeasurementPolicy.AllowedStatuses = append(artifact.MeasurementPolicy.AllowedStatuses, "estimated")
				row := deterministicWorkLadderRowByID(t, artifact, 1)
				row.MetricEvidence.Status = "estimated"
			},
			want: "measurement_policy allowed_statuses must exactly equal measured, unavailable, not_applicable",
		},
		{
			name: "duplicate loses canonical row",
			mutate: func(artifact *deterministicWorkLadderStage0) {
				row := deterministicWorkLadderRowByID(t, artifact, 11)
				row.DuplicateOf = 0
			},
			want: "inventory_id 11 duplicate_of = 0, want 1",
		},
		{
			name: "ordering by intuition",
			mutate: func(artifact *deterministicWorkLadderStage0) {
				artifact.Policy.OrderingBasis = "intuition"
			},
			want: "policy ordering_basis = \"intuition\", want evidence_matrix",
		},
		{
			name: "pure function row under helper-first decision",
			mutate: func(artifact *deterministicWorkLadderStage0) {
				row := deterministicWorkLadderRowByID(t, artifact, 13)
				row.Classification = "pure_function_compute_module"
			},
			want: "pure function rows = 1, want 0 while next_after_stage1 is declarative_readability_helpers_before_pure_function",
		},
	}

	root := conformanceRepoRoot(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			artifact := loadDeterministicWorkLadderStage0(t, root)
			tc.mutate(&artifact)
			problems := validateDeterministicWorkLadderStage0(artifact)
			if !routeAuthorityProblemsContain(problems, tc.want) {
				t.Fatalf("validation problems missing %q:\n- %s", tc.want, strings.Join(problems, "\n- "))
			}
		})
	}
}

func loadDeterministicWorkLadderStage0(t *testing.T, root string) deterministicWorkLadderStage0 {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, deterministicWorkLadderStage0Path))
	if err != nil {
		t.Fatalf("read deterministic work ladder Stage 0 artifact: %v", err)
	}
	var artifact deterministicWorkLadderStage0
	if err := yaml.Unmarshal(raw, &artifact); err != nil {
		t.Fatalf("parse deterministic work ladder Stage 0 artifact: %v", err)
	}
	return artifact
}

func validateDeterministicWorkLadderStage0(artifact deterministicWorkLadderStage0) []string {
	var problems []string

	if artifact.Version != 1 {
		problems = append(problems, fmt.Sprintf("version = %d, want 1", artifact.Version))
	}
	if artifact.Kind != "deterministic_work_ladder_stage0" {
		problems = append(problems, fmt.Sprintf("kind = %q, want deterministic_work_ladder_stage0", artifact.Kind))
	}
	if artifact.Issue != 1664 {
		problems = append(problems, fmt.Sprintf("issue = %d, want 1664", artifact.Issue))
	}
	if artifact.ParentIssue != 1663 {
		problems = append(problems, fmt.Sprintf("parent_issue = %d, want 1663", artifact.ParentIssue))
	}
	if artifact.Discussion != 1460 {
		problems = append(problems, fmt.Sprintf("discussion = %d, want 1460", artifact.Discussion))
	}

	if artifact.SourceBoundary.PlatformSpec != "platform-spec.yaml" {
		problems = append(problems, fmt.Sprintf("source_boundary platform_spec = %q, want platform-spec.yaml", artifact.SourceBoundary.PlatformSpec))
	}
	if artifact.SourceBoundary.PreAuditComment == "" {
		problems = append(problems, "source_boundary pre_audit_comment missing")
	}
	if artifact.SourceBoundary.GateComment == "" {
		problems = append(problems, "source_boundary gate_comment missing")
	}
	if strings.Contains(artifact.SourceBoundary.EmpireEvidenceRoot, "/Users/") {
		problems = append(problems, "source_boundary empire_evidence_root must not use an absolute local path")
	}

	if artifact.Policy.ClaimsParentClosure {
		problems = append(problems, "policy claims_parent_closure = true, want false")
	}
	if artifact.Policy.ClaimsRuntimeBehaviorClosure {
		problems = append(problems, "policy claims_runtime_behavior_closure = true, want false")
	}
	if artifact.Policy.RuntimeBehaviorChangesAllowed {
		problems = append(problems, "policy runtime_behavior_changes_allowed = true, want false")
	}
	if artifact.Policy.PlatformSpecChangesAllowed {
		problems = append(problems, "policy platform_spec_changes_allowed = true, want false")
	}
	if artifact.Policy.RuntimeSemanticOwner != "platform-spec.yaml" {
		problems = append(problems, fmt.Sprintf("policy runtime_semantic_owner = %q, want platform-spec.yaml", artifact.Policy.RuntimeSemanticOwner))
	}
	if artifact.Policy.Stage0Owner != deterministicWorkLadderStage0Path {
		problems = append(problems, fmt.Sprintf("policy stage0_owner = %q, want %s", artifact.Policy.Stage0Owner, deterministicWorkLadderStage0Path))
	}
	if artifact.Policy.NextAfterStage1 != "declarative_readability_helpers_before_pure_function" {
		problems = append(problems, fmt.Sprintf("policy next_after_stage1 = %q, want declarative_readability_helpers_before_pure_function", artifact.Policy.NextAfterStage1))
	}
	if artifact.Policy.OrderingBasis != "evidence_matrix" {
		problems = append(problems, fmt.Sprintf("policy ordering_basis = %q, want evidence_matrix", artifact.Policy.OrderingBasis))
	}

	problems = append(problems, validateDeterministicWorkLadderWatchlist(artifact.WatchlistNode)...)
	problems = append(problems, validateDeterministicWorkLadderEvidence(artifact)...)
	problems = append(problems, validateDeterministicWorkLadderGates(artifact.GateCriteria)...)

	return problems
}

func validateDeterministicWorkLadderWatchlist(watchlist deterministicWorkLadderWatchlist) []string {
	var problems []string
	const wantNode = "runtime_operations.deterministic_work_ladder"
	if watchlist.ID != wantNode {
		problems = append(problems, fmt.Sprintf("watchlist id = %q, want %s", watchlist.ID, wantNode))
	}
	if !strings.Contains(watchlist.NonClosureRule, "#1663") || !strings.Contains(watchlist.NonClosureRule, "must remain open") {
		problems = append(problems, "watchlist non_closure_rule must keep #1663 open")
	}

	trackers := map[int]deterministicWorkLadderActiveTracker{}
	for _, tracker := range watchlist.ActiveTrackers {
		if tracker.Watchlist != wantNode {
			problems = append(problems, fmt.Sprintf("watchlist active tracker #%d watchlist = %q, want %s", tracker.Issue, tracker.Watchlist, wantNode))
		}
		if tracker.Role == "" {
			problems = append(problems, fmt.Sprintf("watchlist active tracker #%d missing role", tracker.Issue))
		}
		trackers[tracker.Issue] = tracker
	}
	for issue := 1663; issue <= 1672; issue++ {
		if _, ok := trackers[issue]; !ok {
			problems = append(problems, fmt.Sprintf("watchlist active_trackers missing #%d", issue))
		}
	}
	return problems
}

func validateDeterministicWorkLadderEvidence(artifact deterministicWorkLadderStage0) []string {
	var problems []string
	allowedClassifications := stringSet(artifact.ClassificationValues)
	allowedMetricStatuses := map[string]bool{
		"measured":       true,
		"unavailable":    true,
		"not_applicable": true,
	}
	if !sameStringSet(artifact.MeasurementPolicy.AllowedStatuses, allowedMetricStatuses) {
		problems = append(problems, "measurement_policy allowed_statuses must exactly equal measured, unavailable, not_applicable")
	}
	if artifact.MeasurementPolicy.RoughEstimatesAllowed {
		problems = append(problems, "measurement_policy rough_estimates_allowed = true, want false")
	}

	rows := map[int]deterministicWorkLadderEvidenceRow{}
	for _, row := range artifact.EvidenceMatrix {
		if _, ok := rows[row.InventoryID]; ok {
			problems = append(problems, fmt.Sprintf("evidence_matrix inventory_id %d appears more than once", row.InventoryID))
		}
		rows[row.InventoryID] = row
		if row.DedupeGroup == "" {
			problems = append(problems, fmt.Sprintf("inventory_id %d missing dedupe_group", row.InventoryID))
		}
		if row.Title == "" {
			problems = append(problems, fmt.Sprintf("inventory_id %d missing title", row.InventoryID))
		}
		if !allowedClassifications[row.Classification] {
			problems = append(problems, fmt.Sprintf("inventory_id %d classification = %q is not allowed", row.InventoryID, row.Classification))
		}
		if row.ConsumerIssue == 0 {
			problems = append(problems, fmt.Sprintf("inventory_id %d missing consumer_issue", row.InventoryID))
		}
		if len(row.ProofRefs) == 0 {
			problems = append(problems, fmt.Sprintf("inventory_id %d missing proof_refs", row.InventoryID))
		}
		for _, ref := range row.ProofRefs {
			if strings.Contains(ref, "/Users/") {
				problems = append(problems, fmt.Sprintf("inventory_id %d proof_ref %q must not use an absolute local path", row.InventoryID, ref))
			}
		}
		if !allowedMetricStatuses[row.MetricEvidence.Status] {
			problems = append(problems, fmt.Sprintf("inventory_id %d metric_evidence.status = %q is not allowed", row.InventoryID, row.MetricEvidence.Status))
		}
		if row.MetricEvidence.Status == "unavailable" && row.MetricEvidence.Reason == "" {
			problems = append(problems, fmt.Sprintf("inventory_id %d unavailable metric missing reason", row.InventoryID))
		}
		if strings.Contains(strings.ToLower(row.MetricEvidence.Status), "rough") {
			problems = append(problems, fmt.Sprintf("inventory_id %d metric_evidence.status = %q is not allowed", row.InventoryID, row.MetricEvidence.Status))
		}
	}
	for id := 1; id <= 16; id++ {
		if _, ok := rows[id]; !ok {
			problems = append(problems, fmt.Sprintf("evidence_matrix missing inventory_id %d", id))
		}
	}

	problems = append(problems, requireDuplicateRow(rows, 11, 1)...)
	problems = append(problems, requireDuplicateRow(rows, 12, 5)...)
	problems = append(problems, requireDispositionContains(rows, 4, "planned_refactor")...)
	problems = append(problems, requireDispositionContains(rows, 15, "draft_only")...)
	problems = append(problems, requireDispositionContains(rows, 16, "architecture_not_node_shape")...)

	helperRows := 0
	durableRows := 0
	pureFunctionRows := 0
	for _, row := range rows {
		switch row.Classification {
		case "declarative_readability_helper":
			helperRows++
		case "durable_activity":
			durableRows++
		case "pure_function_compute_module":
			pureFunctionRows++
		}
	}
	if helperRows < 5 {
		problems = append(problems, fmt.Sprintf("helper evidence rows = %d, want at least 5 for post-Stage-1 ordering", helperRows))
	}
	if durableRows < 2 {
		problems = append(problems, fmt.Sprintf("durable activity evidence rows = %d, want at least 2", durableRows))
	}
	if pureFunctionRows != 0 && artifact.Policy.NextAfterStage1 == "declarative_readability_helpers_before_pure_function" {
		problems = append(problems, fmt.Sprintf("pure function rows = %d, want 0 while next_after_stage1 is declarative_readability_helpers_before_pure_function", pureFunctionRows))
	}

	return problems
}

func validateDeterministicWorkLadderGates(gates []deterministicWorkLadderGate) []string {
	var problems []string
	byID := map[string]deterministicWorkLadderGate{}
	for _, gate := range gates {
		byID[gate.ID] = gate
		if gate.ConsumerIssue == 0 {
			problems = append(problems, fmt.Sprintf("gate %s missing consumer_issue", gate.ID))
		}
		if gate.PromotionState == "" {
			problems = append(problems, fmt.Sprintf("gate %s missing promotion_state", gate.ID))
		}
		if len(gate.RequiredConditions) == 0 {
			problems = append(problems, fmt.Sprintf("gate %s missing required_conditions", gate.ID))
		}
	}
	required := map[string]int{
		"stage1_durable_activity_proof":    1666,
		"stage2_stage3_ordering":           1668,
		"orchestration_function_promotion": 1671,
	}
	for id, issue := range required {
		gate, ok := byID[id]
		if !ok {
			problems = append(problems, fmt.Sprintf("gate_criteria missing %s", id))
			continue
		}
		if gate.ConsumerIssue != issue {
			problems = append(problems, fmt.Sprintf("gate %s consumer_issue = %d, want %d", id, gate.ConsumerIssue, issue))
		}
	}
	stage2 := byID["stage2_stage3_ordering"]
	if stage2.PromotionState != "declarative_helpers_before_pure_function" {
		problems = append(problems, fmt.Sprintf("gate stage2_stage3_ordering promotion_state = %q, want declarative_helpers_before_pure_function", stage2.PromotionState))
	}
	return problems
}

func requireDuplicateRow(rows map[int]deterministicWorkLadderEvidenceRow, id, duplicateOf int) []string {
	row, ok := rows[id]
	if !ok {
		return nil
	}
	var problems []string
	if row.DuplicateOf != duplicateOf {
		problems = append(problems, fmt.Sprintf("inventory_id %d duplicate_of = %d, want %d", id, row.DuplicateOf, duplicateOf))
	}
	if !strings.Contains(row.Disposition, "duplicate") {
		problems = append(problems, fmt.Sprintf("inventory_id %d disposition = %q, want duplicate disposition", id, row.Disposition))
	}
	return problems
}

func requireDispositionContains(rows map[int]deterministicWorkLadderEvidenceRow, id int, want string) []string {
	row, ok := rows[id]
	if !ok {
		return nil
	}
	if !strings.Contains(row.Disposition, want) {
		return []string{fmt.Sprintf("inventory_id %d disposition = %q, want to contain %q", id, row.Disposition, want)}
	}
	return nil
}

func deterministicWorkLadderRowByID(t *testing.T, artifact *deterministicWorkLadderStage0, id int) *deterministicWorkLadderEvidenceRow {
	t.Helper()
	for i := range artifact.EvidenceMatrix {
		if artifact.EvidenceMatrix[i].InventoryID == id {
			return &artifact.EvidenceMatrix[i]
		}
	}
	t.Fatalf("inventory_id %d not found", id)
	return nil
}

func deterministicWorkLadderRowsExcept(rows []deterministicWorkLadderEvidenceRow, id int) []deterministicWorkLadderEvidenceRow {
	out := rows[:0]
	for _, row := range rows {
		if row.InventoryID != id {
			out = append(out, row)
		}
	}
	return out
}

func deterministicWorkLadderTrackersExcept(trackers []deterministicWorkLadderActiveTracker, issue int) []deterministicWorkLadderActiveTracker {
	out := trackers[:0]
	for _, tracker := range trackers {
		if tracker.Issue != issue {
			out = append(out, tracker)
		}
	}
	return out
}

func stringSet(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

func sameStringSet(values []string, want map[string]bool) bool {
	if len(values) != len(want) {
		return false
	}
	for _, value := range values {
		if !want[value] {
			return false
		}
	}
	return true
}
