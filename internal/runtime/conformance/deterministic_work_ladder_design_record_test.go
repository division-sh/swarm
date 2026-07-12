package conformance

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

const deterministicWorkLadderDesignRecordPath = "internal/runtime/conformance/testdata/deterministic_work_ladder_design_record.yaml"

type deterministicWorkLadderDesignRecord struct {
	Version               int                                          `yaml:"version"`
	Kind                  string                                       `yaml:"kind"`
	Issue                 int                                          `yaml:"issue"`
	ParentIssue           int                                          `yaml:"parent_issue"`
	Stage0Issue           int                                          `yaml:"stage0_issue"`
	Discussion            int                                          `yaml:"discussion"`
	Authority             deterministicWorkLadderDesignAuthority       `yaml:"authority"`
	SourceRefs            deterministicWorkLadderDesignSourceRefs      `yaml:"source_refs"`
	Watchlist             deterministicWorkLadderDesignWatchlist       `yaml:"watchlist"`
	LockedBoundary        deterministicWorkLadderLockedBoundary        `yaml:"locked_boundary"`
	Repairs               []deterministicWorkLadderDesignRepair        `yaml:"repairs"`
	LaunchLanguage        deterministicWorkLadderLaunchLanguage        `yaml:"launch_language"`
	ChildConsumers        []deterministicWorkLadderDesignChildConsumer `yaml:"child_consumers"`
	InvalidatedClaims     []string                                     `yaml:"invalidated_claims"`
	ManifestationCoverage []deterministicWorkLadderDesignManifestation `yaml:"manifestation_coverage"`
}

type deterministicWorkLadderDesignAuthority struct {
	Status                        string   `yaml:"status"`
	RuntimeSemanticOwner          string   `yaml:"runtime_semantic_owner"`
	PlatformSpecChangesAllowed    bool     `yaml:"platform_spec_changes_allowed"`
	RuntimeBehaviorChangesAllowed bool     `yaml:"runtime_behavior_changes_allowed"`
	ClaimsImplementedSemantics    bool     `yaml:"claims_implemented_semantics"`
	PromotionRule                 string   `yaml:"promotion_rule"`
	ProhibitedUses                []string `yaml:"prohibited_uses"`
}

type deterministicWorkLadderDesignSourceRefs struct {
	Issue                  string   `yaml:"issue"`
	PreAuditComment        string   `yaml:"pre_audit_comment"`
	GateComment            string   `yaml:"gate_comment"`
	ParentTracker          string   `yaml:"parent_tracker"`
	Stage0Artifact         string   `yaml:"stage0_artifact"`
	LockedDesignDiscussion string   `yaml:"locked_design_discussion"`
	DiscussionAmendment    string   `yaml:"discussion_amendment"`
	AuthoringUXDiscussion  string   `yaml:"authoring_ux_discussion"`
	AdjacentContext        []string `yaml:"adjacent_context"`
}

type deterministicWorkLadderDesignWatchlist struct {
	ID               string `yaml:"id"`
	Owner            string `yaml:"owner"`
	Role             string `yaml:"role"`
	ParentNonClosure bool   `yaml:"parent_non_closure"`
	ParentIssue      int    `yaml:"parent_issue"`
}

type deterministicWorkLadderLockedBoundary struct {
	GenericLogicNode          string `yaml:"generic_logic_node"`
	SystemNodeRename          string `yaml:"system_node_rename"`
	SystemNodeVocabularyOwner string `yaml:"system_node_vocabulary_owner"`
	ExternalIOPolicy          string `yaml:"external_io_policy"`
	CodeContractRelation      string `yaml:"code_contract_relation"`
	PlatformSpecPolicy        string `yaml:"platform_spec_policy"`
	ExpressionLanguagePolicy  string `yaml:"expression_language_policy"`
	ContractVisibilityRule    string `yaml:"contract_visibility_rule"`
}

type deterministicWorkLadderDesignRepair struct {
	ID                                   string   `yaml:"id"`
	OwnerIssue                           int      `yaml:"owner_issue"`
	Decision                             string   `yaml:"decision"`
	HiddenRuntimeSurfaceAllowed          bool     `yaml:"hidden_runtime_surface_allowed"`
	RequiredSurfaces                     []string `yaml:"required_surfaces"`
	SelectedName                         string   `yaml:"selected_name"`
	ExistingComputeOwner                 string   `yaml:"existing_compute_owner"`
	ExistingComputePreserved             bool     `yaml:"existing_compute_preserved"`
	ForbiddenOverloads                   []string `yaml:"forbidden_overloads"`
	ProseOnlyPlacementAllowed            bool     `yaml:"prose_only_placement_allowed"`
	MCPSelfClassificationAuthoritative   bool     `yaml:"mcp_self_classification_authoritative"`
	DefaultWithoutAuthoredClassification string   `yaml:"default_without_authored_classification"`
	CanonicalOwner                       string   `yaml:"canonical_owner"`
	CurrentDisposition                   string   `yaml:"current_disposition"`
	ActivityOwnerStatus                  string   `yaml:"activity_owner_status"`
	MigrationBlocker                     string   `yaml:"migration_blocker"`
	FutureMigrationCondition             string   `yaml:"future_migration_condition"`
	CurrentActionSurvivesUntilMigration  bool     `yaml:"current_action_survives_until_migration"`
	EvidenceOwner                        string   `yaml:"evidence_owner"`
	Note                                 string   `yaml:"note"`
}

type deterministicWorkLadderLaunchLanguage struct {
	OwnerIssue                  int    `yaml:"owner_issue"`
	Selected                    string `yaml:"selected"`
	Status                      string `yaml:"status"`
	OpenEndedMultiLanguageScope bool   `yaml:"open_ended_multi_language_scope"`
	ImplementationNote          string `yaml:"implementation_note"`
}

type deterministicWorkLadderDesignChildConsumer struct {
	Issue    int      `yaml:"issue"`
	Role     string   `yaml:"role"`
	Consumes []string `yaml:"consumes"`
}

type deterministicWorkLadderDesignManifestation struct {
	ID     string `yaml:"id"`
	Status string `yaml:"status"`
	Proof  string `yaml:"proof"`
}

func TestDeterministicWorkLadderDesignRecordLocksGateBoundary(t *testing.T) {
	root := conformanceRepoRoot(t)
	record := loadDeterministicWorkLadderDesignRecord(t, root)
	if problems := validateDeterministicWorkLadderDesignRecord(record); len(problems) > 0 {
		t.Fatalf("deterministic work ladder design record validation failed:\n- %s", strings.Join(problems, "\n- "))
	}
}

func TestDeterministicWorkLadderDesignRecordRejectsStaleOrRuntimeClaims(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*deterministicWorkLadderDesignRecord)
		want   string
	}{
		{
			name: "runtime authority claim",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				record.Authority.ClaimsImplementedSemantics = true
			},
			want: "authority claims_implemented_semantics = true, want false",
		},
		{
			name: "platform spec promotion claim",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				record.Authority.PlatformSpecChangesAllowed = true
			},
			want: "authority platform_spec_changes_allowed = true, want false",
		},
		{
			name: "generic logic node accepted",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				record.LockedBoundary.GenericLogicNode = "accepted"
			},
			want: "locked_boundary generic_logic_node = \"accepted\", want rejected",
		},
		{
			name: "system node rename accepted",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				record.LockedBoundary.SystemNodeRename = "accepted"
			},
			want: "locked_boundary system_node_rename = \"accepted\", want rejected",
		},
		{
			name: "hidden activity result events",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				repair := deterministicWorkLadderDesignRepairByID(t, record, "activity_result_event_materialization")
				repair.HiddenRuntimeSurfaceAllowed = true
			},
			want: "repair activity_result_event_materialization hidden_runtime_surface_allowed = true, want false",
		},
		{
			name: "compute overload selected",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				repair := deterministicWorkLadderDesignRepairByID(t, record, "compute_name_collision")
				repair.SelectedName = "compute"
			},
			want: "repair compute_name_collision selected_name = \"compute\", want compute_module",
		},
		{
			name: "missing minimal activity form",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				repair := deterministicWorkLadderDesignRepairByID(t, record, "activity_minimal_authoring_form")
				repair.Decision = "defer_minimal_form"
			},
			want: "repair activity_minimal_authoring_form decision = \"defer_minimal_form\", want tool_input_minimal_form_with_platform_defaults",
		},
		{
			name: "fork policy invented downstream",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				repair := deterministicWorkLadderDesignRepairByID(t, record, "effect_class_fork_policy_defaults")
				repair.Decision = "orchestration_decides_later"
			},
			want: "repair effect_class_fork_policy_defaults decision = \"orchestration_decides_later\", want default_fork_policy_must_be_specified_with_effect_classes",
		},
		{
			name: "policy sheet row model split into standalone keywords",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				repair := deterministicWorkLadderDesignRepairByID(t, record, "policy_sheet_row_model")
				repair.Decision = "standalone_switch_lookup_threshold_keywords"
			},
			want: "repair policy_sheet_row_model decision = \"standalone_switch_lookup_threshold_keywords\", want first_selection_helpers_are_policy_sheet_row_types",
		},
		{
			name: "duplicate repair id",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				record.Repairs = append(record.Repairs, *deterministicWorkLadderDesignRepairByID(t, record, "compute_name_collision"))
			},
			want: "repairs duplicate id compute_name_collision",
		},
		{
			name: "open ended languages",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				record.LaunchLanguage.OpenEndedMultiLanguageScope = true
			},
			want: "launch_language open_ended_multi_language_scope = true, want false",
		},
		{
			name: "missing discussion amendment",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				record.SourceRefs.DiscussionAmendment = ""
			},
			want: "source_refs discussion_amendment missing #1460 discussion comment URL",
		},
		{
			name: "missing child consumer",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				record.ChildConsumers = deterministicWorkLadderDesignConsumersExcept(record.ChildConsumers, 1671)
			},
			want: "child_consumers missing #1671",
		},
		{
			name: "duplicate child consumer issue",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				record.ChildConsumers = append(record.ChildConsumers, deterministicWorkLadderDesignConsumerByIssue(t, record, 1666))
			},
			want: "child_consumers duplicate issue #1666",
		},
		{
			name: "zero child consumer issue",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				record.ChildConsumers = append(record.ChildConsumers, deterministicWorkLadderDesignChildConsumer{
					Role:     "invalid_zero_issue",
					Consumes: []string{"authority.promotion_rule"},
				})
			},
			want: "child_consumers contains entry with zero issue",
		},
		{
			name: "missing tool call invalidated claim",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				record.InvalidatedClaims = deterministicWorkLadderStringsExcept(record.InvalidatedClaims, "tool_call_action_is_the_whole_deterministic_io_solution")
			},
			want: "invalidated_claims missing tool_call_action_is_the_whole_deterministic_io_solution",
		},
		{
			name: "duplicate manifestation id",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				record.ManifestationCoverage = append(record.ManifestationCoverage, deterministicWorkLadderManifestationByID(t, record, "compute_name_collision"))
			},
			want: "manifestation_coverage duplicate id compute_name_collision",
		},
		{
			name: "empty manifestation id",
			mutate: func(record *deterministicWorkLadderDesignRecord) {
				record.ManifestationCoverage = append(record.ManifestationCoverage, deterministicWorkLadderDesignManifestation{
					Status: "closed_by_design_record",
					Proof:  "authority",
				})
			},
			want: "manifestation_coverage contains entry with empty id",
		},
	}

	root := conformanceRepoRoot(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			record := loadDeterministicWorkLadderDesignRecord(t, root)
			tc.mutate(&record)
			problems := validateDeterministicWorkLadderDesignRecord(record)
			if !routeAuthorityProblemsContain(problems, tc.want) {
				t.Fatalf("validation problems missing %q:\n- %s", tc.want, strings.Join(problems, "\n- "))
			}
		})
	}
}

func TestDeterministicWorkLadderArtifactRepoDispositionPromotedToPlatformSpec(t *testing.T) {
	root := conformanceRepoRoot(t)
	source, err := yamlsource.LoadFile(filepath.Join(root, "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	var spec map[string]any
	if err := source.Decode(&spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
	artifact := yamlMapAt(t, spec,
		"handler_specification",
		"handler_fields",
		"action",
		"valid_values",
		"artifact_repo_commit",
	)
	disposition := yamlMapValue(t, artifact, "durable_activity_disposition")
	for field, wants := range map[string][]string{
		"current_owner": {
			"canonical platform owner",
			"action.id: artifact_repo_commit",
			"not an alternate",
		},
		"activity_non_owner_paths": {
			"platform_builtin",
			"MCP",
			"native/generated",
			"shell",
			"HTTP-tool activity",
			"fail closed",
		},
		"migration_blocker": {
			"read_only authored HTTP",
			"idempotent_write",
			"non_idempotent_write",
			"stable activity attempt/result journal",
			"idempotency execution owner",
		},
		"future_migration_condition": {
			"separately gated migration",
			"provider/root/path allowlist",
			"provider request history",
			"output-state repair",
			"success/failure result-event guarantees",
		},
	} {
		got := yamlStringValue(t, disposition, field)
		for _, want := range wants {
			if !strings.Contains(got, want) {
				t.Fatalf("platform spec artifact_repo_commit durable_activity_disposition.%s missing %q in:\n%s", field, want, got)
			}
		}
	}

	platformBuiltin := yamlStringAt(t, spec,
		"handler_specification",
		"handler_fields",
		"activity",
		"supported_tool_sources",
		"platform_builtin",
	)
	for _, want := range []string{"Not callable from activity", "artifact_repo_commit", "action-owned", "write-effect activity migration"} {
		if !strings.Contains(platformBuiltin, want) {
			t.Fatalf("platform spec activity.supported_tool_sources.platform_builtin missing %q in:\n%s", want, platformBuiltin)
		}
	}
}

func loadDeterministicWorkLadderDesignRecord(t *testing.T, root string) deterministicWorkLadderDesignRecord {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, deterministicWorkLadderDesignRecordPath))
	if err != nil {
		t.Fatalf("read deterministic work ladder design record: %v", err)
	}
	var record deterministicWorkLadderDesignRecord
	if err := yaml.Unmarshal(raw, &record); err != nil {
		t.Fatalf("parse deterministic work ladder design record: %v", err)
	}
	return record
}

func validateDeterministicWorkLadderDesignRecord(record deterministicWorkLadderDesignRecord) []string {
	var problems []string
	if record.Version != 1 {
		problems = append(problems, fmt.Sprintf("version = %d, want 1", record.Version))
	}
	if record.Kind != "deterministic_work_ladder_design_record" {
		problems = append(problems, fmt.Sprintf("kind = %q, want deterministic_work_ladder_design_record", record.Kind))
	}
	if record.Issue != 1665 {
		problems = append(problems, fmt.Sprintf("issue = %d, want 1665", record.Issue))
	}
	if record.ParentIssue != 1663 {
		problems = append(problems, fmt.Sprintf("parent_issue = %d, want 1663", record.ParentIssue))
	}
	if record.Stage0Issue != 1664 {
		problems = append(problems, fmt.Sprintf("stage0_issue = %d, want 1664", record.Stage0Issue))
	}
	if record.Discussion != 1460 {
		problems = append(problems, fmt.Sprintf("discussion = %d, want 1460", record.Discussion))
	}

	problems = append(problems, validateDeterministicWorkLadderDesignAuthority(record.Authority)...)
	problems = append(problems, validateDeterministicWorkLadderDesignRefs(record.SourceRefs)...)
	problems = append(problems, validateDeterministicWorkLadderDesignWatchlist(record.Watchlist)...)
	problems = append(problems, validateDeterministicWorkLadderLockedBoundary(record.LockedBoundary)...)
	problems = append(problems, validateDeterministicWorkLadderDesignRepairs(record.Repairs)...)
	problems = append(problems, validateDeterministicWorkLadderLaunchLanguage(record.LaunchLanguage)...)
	problems = append(problems, validateDeterministicWorkLadderDesignConsumers(record.ChildConsumers)...)
	problems = append(problems, validateDeterministicWorkLadderInvalidatedClaims(record.InvalidatedClaims)...)
	problems = append(problems, validateDeterministicWorkLadderManifestationCoverage(record.ManifestationCoverage)...)
	return problems
}

func validateDeterministicWorkLadderDesignAuthority(authority deterministicWorkLadderDesignAuthority) []string {
	var problems []string
	if authority.Status != "non_authoritative_design_record" {
		problems = append(problems, fmt.Sprintf("authority status = %q, want non_authoritative_design_record", authority.Status))
	}
	if authority.RuntimeSemanticOwner != "platform-spec.yaml" {
		problems = append(problems, fmt.Sprintf("authority runtime_semantic_owner = %q, want platform-spec.yaml", authority.RuntimeSemanticOwner))
	}
	if authority.PlatformSpecChangesAllowed {
		problems = append(problems, "authority platform_spec_changes_allowed = true, want false")
	}
	if authority.RuntimeBehaviorChangesAllowed {
		problems = append(problems, "authority runtime_behavior_changes_allowed = true, want false")
	}
	if authority.ClaimsImplementedSemantics {
		problems = append(problems, "authority claims_implemented_semantics = true, want false")
	}
	for _, want := range []string{"platform-spec.yaml", "same PR", "true on master"} {
		if !strings.Contains(authority.PromotionRule, want) {
			problems = append(problems, fmt.Sprintf("authority promotion_rule missing %q", want))
		}
	}
	for _, want := range []string{"runtime_source_authority", "platform_spec_authority", "compatibility_seam_or_legacy_behavior_approval"} {
		if !stringSet(recordStringSlice(authority.ProhibitedUses))[want] {
			problems = append(problems, fmt.Sprintf("authority prohibited_uses missing %s", want))
		}
	}
	return problems
}

func validateDeterministicWorkLadderDesignRefs(refs deterministicWorkLadderDesignSourceRefs) []string {
	var problems []string
	if refs.Stage0Artifact != deterministicWorkLadderStage0Path {
		problems = append(problems, fmt.Sprintf("source_refs stage0_artifact = %q, want %s", refs.Stage0Artifact, deterministicWorkLadderStage0Path))
	}
	if !strings.Contains(refs.DiscussionAmendment, "github.com/division-sh/swarm/discussions/1460#discussioncomment-") {
		problems = append(problems, "source_refs discussion_amendment missing #1460 discussion comment URL")
	}
	if refs.AuthoringUXDiscussion != "https://github.com/division-sh/swarm/discussions/1711" {
		problems = append(problems, fmt.Sprintf("source_refs authoring_ux_discussion = %q, want https://github.com/division-sh/swarm/discussions/1711", refs.AuthoringUXDiscussion))
	}
	for _, ref := range []string{refs.Issue, refs.PreAuditComment, refs.GateComment, refs.ParentTracker, refs.LockedDesignDiscussion} {
		if strings.TrimSpace(ref) == "" {
			problems = append(problems, "source_refs contains empty required URL")
		}
	}
	for _, want := range []string{"1652", "1654", "platform-spec.yaml#spec_authority.review_artifact_boundary", "platform-spec.yaml#handler_specification.handler_fields.compute"} {
		if !containsSubstring(refs.AdjacentContext, want) {
			problems = append(problems, fmt.Sprintf("source_refs adjacent_context missing %s", want))
		}
	}
	return problems
}

func validateDeterministicWorkLadderDesignWatchlist(watchlist deterministicWorkLadderDesignWatchlist) []string {
	var problems []string
	if watchlist.ID != "runtime_operations.deterministic_work_ladder" {
		problems = append(problems, fmt.Sprintf("watchlist id = %q, want runtime_operations.deterministic_work_ladder", watchlist.ID))
	}
	if watchlist.Owner != deterministicWorkLadderStage0Path {
		problems = append(problems, fmt.Sprintf("watchlist owner = %q, want %s", watchlist.Owner, deterministicWorkLadderStage0Path))
	}
	if watchlist.Role != "adr_design_record" {
		problems = append(problems, fmt.Sprintf("watchlist role = %q, want adr_design_record", watchlist.Role))
	}
	if !watchlist.ParentNonClosure {
		problems = append(problems, "watchlist parent_non_closure = false, want true")
	}
	if watchlist.ParentIssue != 1663 {
		problems = append(problems, fmt.Sprintf("watchlist parent_issue = %d, want 1663", watchlist.ParentIssue))
	}
	return problems
}

func validateDeterministicWorkLadderLockedBoundary(boundary deterministicWorkLadderLockedBoundary) []string {
	var problems []string
	if boundary.GenericLogicNode != "rejected" {
		problems = append(problems, fmt.Sprintf("locked_boundary generic_logic_node = %q, want rejected", boundary.GenericLogicNode))
	}
	if boundary.SystemNodeRename != "rejected" {
		problems = append(problems, fmt.Sprintf("locked_boundary system_node_rename = %q, want rejected", boundary.SystemNodeRename))
	}
	if boundary.SystemNodeVocabularyOwner != "platform-spec.yaml#vocabulary.agent_role.types.system_node" {
		problems = append(problems, fmt.Sprintf("locked_boundary system_node_vocabulary_owner = %q, want platform-spec.yaml#vocabulary.agent_role.types.system_node", boundary.SystemNodeVocabularyOwner))
	}
	if boundary.ExternalIOPolicy != "durable_activities_only" {
		problems = append(problems, fmt.Sprintf("locked_boundary external_io_policy = %q, want durable_activities_only", boundary.ExternalIOPolicy))
	}
	if boundary.CodeContractRelation != "code_subordinate_to_contract" {
		problems = append(problems, fmt.Sprintf("locked_boundary code_contract_relation = %q, want code_subordinate_to_contract", boundary.CodeContractRelation))
	}
	if boundary.PlatformSpecPolicy != "no_speculative_v2_semantics" {
		problems = append(problems, fmt.Sprintf("locked_boundary platform_spec_policy = %q, want no_speculative_v2_semantics", boundary.PlatformSpecPolicy))
	}
	if boundary.ExpressionLanguagePolicy != "no_middle_language_no_richer_cel" {
		problems = append(problems, fmt.Sprintf("locked_boundary expression_language_policy = %q, want no_middle_language_no_richer_cel", boundary.ExpressionLanguagePolicy))
	}
	for _, want := range []string{"states", "emitted events", "entity writes", "external effects"} {
		if !strings.Contains(boundary.ContractVisibilityRule, want) {
			problems = append(problems, fmt.Sprintf("locked_boundary contract_visibility_rule missing %q", want))
		}
	}
	return problems
}

func validateDeterministicWorkLadderDesignRepairs(repairs []deterministicWorkLadderDesignRepair) []string {
	var problems []string
	byID := map[string]deterministicWorkLadderDesignRepair{}
	for _, repair := range repairs {
		if strings.TrimSpace(repair.ID) == "" {
			problems = append(problems, "repairs contains entry with empty id")
			continue
		}
		if _, exists := byID[repair.ID]; exists {
			problems = append(problems, fmt.Sprintf("repairs duplicate id %s", repair.ID))
			continue
		}
		byID[repair.ID] = repair
	}

	resultEvents := byID["activity_result_event_materialization"]
	if resultEvents.Decision != "generated_materialized_contract_outputs" {
		problems = append(problems, fmt.Sprintf("repair activity_result_event_materialization decision = %q, want generated_materialized_contract_outputs", resultEvents.Decision))
	}
	if resultEvents.HiddenRuntimeSurfaceAllowed {
		problems = append(problems, "repair activity_result_event_materialization hidden_runtime_surface_allowed = true, want false")
	}
	for _, want := range []string{"bundle", "catalog", "verify"} {
		if !stringSet(resultEvents.RequiredSurfaces)[want] {
			problems = append(problems, fmt.Sprintf("repair activity_result_event_materialization required_surfaces missing %s", want))
		}
	}

	computeName := byID["compute_name_collision"]
	if computeName.SelectedName != "compute_module" {
		problems = append(problems, fmt.Sprintf("repair compute_name_collision selected_name = %q, want compute_module", computeName.SelectedName))
	}
	if computeName.ExistingComputeOwner != "platform-spec.yaml#handler_specification.handler_fields.compute" {
		problems = append(problems, fmt.Sprintf("repair compute_name_collision existing_compute_owner = %q, want platform-spec.yaml#handler_specification.handler_fields.compute", computeName.ExistingComputeOwner))
	}
	if !computeName.ExistingComputePreserved {
		problems = append(problems, "repair compute_name_collision existing_compute_preserved = false, want true")
	}
	for _, forbidden := range []string{"compute", "handler_fields.compute"} {
		if !stringSet(computeName.ForbiddenOverloads)[forbidden] {
			problems = append(problems, fmt.Sprintf("repair compute_name_collision forbidden_overloads missing %s", forbidden))
		}
	}

	graphPlacement := byID["activity_graph_placement"]
	if graphPlacement.Decision != "dependency_graph_delta_required" {
		problems = append(problems, fmt.Sprintf("repair activity_graph_placement decision = %q, want dependency_graph_delta_required", graphPlacement.Decision))
	}
	if graphPlacement.ProseOnlyPlacementAllowed {
		problems = append(problems, "repair activity_graph_placement prose_only_placement_allowed = true, want false")
	}

	effectClass := byID["tool_effect_class_authority"]
	if effectClass.Decision != "ship_with_first_consumer" {
		problems = append(problems, fmt.Sprintf("repair tool_effect_class_authority decision = %q, want ship_with_first_consumer", effectClass.Decision))
	}
	if effectClass.MCPSelfClassificationAuthoritative {
		problems = append(problems, "repair tool_effect_class_authority mcp_self_classification_authoritative = true, want false")
	}
	if effectClass.DefaultWithoutAuthoredClassification != "deny" {
		problems = append(problems, fmt.Sprintf("repair tool_effect_class_authority default_without_authored_classification = %q, want deny", effectClass.DefaultWithoutAuthoredClassification))
	}

	minimalForm := byID["activity_minimal_authoring_form"]
	if minimalForm.OwnerIssue != 1666 {
		problems = append(problems, fmt.Sprintf("repair activity_minimal_authoring_form owner_issue = %d, want 1666", minimalForm.OwnerIssue))
	}
	if minimalForm.Decision != "tool_input_minimal_form_with_platform_defaults" {
		problems = append(problems, fmt.Sprintf("repair activity_minimal_authoring_form decision = %q, want tool_input_minimal_form_with_platform_defaults", minimalForm.Decision))
	}
	for _, want := range []string{"spec", "pilot"} {
		if !stringSet(minimalForm.RequiredSurfaces)[want] {
			problems = append(problems, fmt.Sprintf("repair activity_minimal_authoring_form required_surfaces missing %s", want))
		}
	}
	for _, want := range []string{"tool plus input", "effect class", "not an implementation claim"} {
		if !strings.Contains(minimalForm.Note, want) {
			problems = append(problems, fmt.Sprintf("repair activity_minimal_authoring_form note missing %q", want))
		}
	}

	forkPolicy := byID["effect_class_fork_policy_defaults"]
	if forkPolicy.OwnerIssue != 1666 {
		problems = append(problems, fmt.Sprintf("repair effect_class_fork_policy_defaults owner_issue = %d, want 1666", forkPolicy.OwnerIssue))
	}
	if forkPolicy.Decision != "default_fork_policy_must_be_specified_with_effect_classes" {
		problems = append(problems, fmt.Sprintf("repair effect_class_fork_policy_defaults decision = %q, want default_fork_policy_must_be_specified_with_effect_classes", forkPolicy.Decision))
	}
	for _, want := range []string{"spec", "verify", "trace"} {
		if !stringSet(forkPolicy.RequiredSurfaces)[want] {
			problems = append(problems, fmt.Sprintf("repair effect_class_fork_policy_defaults required_surfaces missing %s", want))
		}
	}
	for _, want := range []string{"#1671", "#1672", "must not invent"} {
		if !strings.Contains(forkPolicy.Note, want) {
			problems = append(problems, fmt.Sprintf("repair effect_class_fork_policy_defaults note missing %q", want))
		}
	}

	artifactDisposition := byID["artifact_repo_commit_disposition"]
	if artifactDisposition.OwnerIssue != 1667 {
		problems = append(problems, fmt.Sprintf("repair artifact_repo_commit_disposition owner_issue = %d, want 1667", artifactDisposition.OwnerIssue))
	}
	if artifactDisposition.Decision != "canonical_action_owner_until_write_activity_journal" {
		problems = append(problems, fmt.Sprintf("repair artifact_repo_commit_disposition decision = %q, want canonical_action_owner_until_write_activity_journal", artifactDisposition.Decision))
	}
	if artifactDisposition.CanonicalOwner != "platform-spec.yaml#handler_specification.handler_fields.action.valid_values.artifact_repo_commit" {
		problems = append(problems, fmt.Sprintf("repair artifact_repo_commit_disposition canonical_owner = %q, want platform-spec.yaml#handler_specification.handler_fields.action.valid_values.artifact_repo_commit", artifactDisposition.CanonicalOwner))
	}
	if artifactDisposition.CurrentDisposition != "canonical_platform_action_owner" {
		problems = append(problems, fmt.Sprintf("repair artifact_repo_commit_disposition current_disposition = %q, want canonical_platform_action_owner", artifactDisposition.CurrentDisposition))
	}
	if artifactDisposition.ActivityOwnerStatus != "non_owner_for_artifact_commits" {
		problems = append(problems, fmt.Sprintf("repair artifact_repo_commit_disposition activity_owner_status = %q, want non_owner_for_artifact_commits", artifactDisposition.ActivityOwnerStatus))
	}
	if artifactDisposition.MigrationBlocker != "stable_activity_attempt_result_journal_and_idempotency_execution_owner" {
		problems = append(problems, fmt.Sprintf("repair artifact_repo_commit_disposition migration_blocker = %q, want stable_activity_attempt_result_journal_and_idempotency_execution_owner", artifactDisposition.MigrationBlocker))
	}
	for _, want := range []string{"separately gated", "artifact root/path safety", "provider request history", "output-state repair", "result-event guarantees"} {
		if !strings.Contains(artifactDisposition.FutureMigrationCondition, want) {
			problems = append(problems, fmt.Sprintf("repair artifact_repo_commit_disposition future_migration_condition missing %q", want))
		}
	}
	if !artifactDisposition.CurrentActionSurvivesUntilMigration {
		problems = append(problems, "repair artifact_repo_commit_disposition current_action_survives_until_migration = false, want true")
	}

	stageOrder := byID["stage_order_after_activity"]
	if stageOrder.Decision != "declarative_helpers_before_compute_module" {
		problems = append(problems, fmt.Sprintf("repair stage_order_after_activity decision = %q, want declarative_helpers_before_compute_module", stageOrder.Decision))
	}
	if stageOrder.EvidenceOwner != deterministicWorkLadderStage0Path {
		problems = append(problems, fmt.Sprintf("repair stage_order_after_activity evidence_owner = %q, want %s", stageOrder.EvidenceOwner, deterministicWorkLadderStage0Path))
	}

	policySheet := byID["policy_sheet_row_model"]
	if policySheet.OwnerIssue != 1668 {
		problems = append(problems, fmt.Sprintf("repair policy_sheet_row_model owner_issue = %d, want 1668", policySheet.OwnerIssue))
	}
	if policySheet.Decision != "first_selection_helpers_are_policy_sheet_row_types" {
		problems = append(problems, fmt.Sprintf("repair policy_sheet_row_model decision = %q, want first_selection_helpers_are_policy_sheet_row_types", policySheet.Decision))
	}
	if policySheet.EvidenceOwner != deterministicWorkLadderStage0Path {
		problems = append(problems, fmt.Sprintf("repair policy_sheet_row_model evidence_owner = %q, want %s", policySheet.EvidenceOwner, deterministicWorkLadderStage0Path))
	}
	for _, forbidden := range []string{"handler_fields.switch", "handler_fields.lookup", "handler_fields.threshold", "handler_fields.policy"} {
		if !stringSet(policySheet.ForbiddenOverloads)[forbidden] {
			problems = append(problems, fmt.Sprintf("repair policy_sheet_row_model forbidden_overloads missing %s", forbidden))
		}
	}
	for _, surface := range []string{"design_record", "future_platform_spec", "authoring_ux"} {
		if !stringSet(policySheet.RequiredSurfaces)[surface] {
			problems = append(problems, fmt.Sprintf("repair policy_sheet_row_model required_surfaces missing %s", surface))
		}
	}
	for _, want := range []string{"ordered policy-sheet authoring construct", "not standalone handler", "current spelling", "enhance rules in place", "closed and statically verifiable", "rules selected-branch model", "value-derivation rows lower to compute", "Stable row IDs", "Discovery row 13", "Treasury row 9", "not an implementation claim"} {
		if !strings.Contains(policySheet.Note, want) {
			problems = append(problems, fmt.Sprintf("repair policy_sheet_row_model note missing %q", want))
		}
	}
	for _, want := range []string{"selection rows lower to platform-spec.yaml#handler_specification.handler_fields.rules", "value rows lower to platform-spec.yaml#handler_specification.handler_fields.compute"} {
		if !strings.Contains(policySheet.CanonicalOwner, want) {
			problems = append(problems, fmt.Sprintf("repair policy_sheet_row_model canonical_owner missing %q", want))
		}
	}

	for _, id := range []string{"activity_result_event_materialization", "compute_name_collision", "activity_graph_placement", "tool_effect_class_authority", "activity_minimal_authoring_form", "effect_class_fork_policy_defaults", "artifact_repo_commit_disposition", "stage_order_after_activity", "policy_sheet_row_model"} {
		if _, ok := byID[id]; !ok {
			problems = append(problems, fmt.Sprintf("repairs missing %s", id))
		}
	}
	return problems
}

func validateDeterministicWorkLadderLaunchLanguage(language deterministicWorkLadderLaunchLanguage) []string {
	var problems []string
	if language.OwnerIssue != 1670 {
		problems = append(problems, fmt.Sprintf("launch_language owner_issue = %d, want 1670", language.OwnerIssue))
	}
	if language.Selected != "python" {
		problems = append(problems, fmt.Sprintf("launch_language selected = %q, want python", language.Selected))
	}
	if language.Status != "decided_and_revisable" {
		problems = append(problems, fmt.Sprintf("launch_language status = %q, want decided_and_revisable", language.Status))
	}
	if language.OpenEndedMultiLanguageScope {
		problems = append(problems, "launch_language open_ended_multi_language_scope = true, want false")
	}
	return problems
}

func validateDeterministicWorkLadderDesignConsumers(consumers []deterministicWorkLadderDesignChildConsumer) []string {
	var problems []string
	byIssue := map[int]deterministicWorkLadderDesignChildConsumer{}
	for _, consumer := range consumers {
		if consumer.Issue == 0 {
			problems = append(problems, "child_consumers contains entry with zero issue")
			continue
		}
		if _, exists := byIssue[consumer.Issue]; exists {
			problems = append(problems, fmt.Sprintf("child_consumers duplicate issue #%d", consumer.Issue))
			continue
		}
		byIssue[consumer.Issue] = consumer
		if consumer.Role == "" {
			problems = append(problems, fmt.Sprintf("child_consumers #%d missing role", consumer.Issue))
		}
		if len(consumer.Consumes) == 0 {
			problems = append(problems, fmt.Sprintf("child_consumers #%d missing consumes", consumer.Issue))
		}
	}
	for issue := 1666; issue <= 1672; issue++ {
		if _, ok := byIssue[issue]; !ok {
			problems = append(problems, fmt.Sprintf("child_consumers missing #%d", issue))
		}
	}
	if !containsSubstring(byIssue[1669].Consumes, "compute_name_collision") {
		problems = append(problems, "child_consumers #1669 does not consume compute_name_collision")
	}
	if !containsSubstring(byIssue[1666].Consumes, "activity_result_event_materialization") {
		problems = append(problems, "child_consumers #1666 does not consume activity_result_event_materialization")
	}
	if !containsSubstring(byIssue[1666].Consumes, "activity_minimal_authoring_form") {
		problems = append(problems, "child_consumers #1666 does not consume activity_minimal_authoring_form")
	}
	if !containsSubstring(byIssue[1668].Consumes, "policy_sheet_row_model") {
		problems = append(problems, "child_consumers #1668 does not consume policy_sheet_row_model")
	}
	if !containsSubstring(byIssue[1671].Consumes, "effect_class_fork_policy_defaults") {
		problems = append(problems, "child_consumers #1671 does not consume effect_class_fork_policy_defaults")
	}
	if !containsSubstring(byIssue[1672].Consumes, "effect_class_fork_policy_defaults") {
		problems = append(problems, "child_consumers #1672 does not consume effect_class_fork_policy_defaults")
	}
	return problems
}

func validateDeterministicWorkLadderInvalidatedClaims(claims []string) []string {
	var problems []string
	set := stringSet(claims)
	for _, claim := range []string{
		"generic_logic_node_is_accepted_direction",
		"system_node_should_be_renamed_to_routing_node",
		"tool_call_action_is_the_whole_deterministic_io_solution",
		"activity_result_events_can_be_hidden_runtime_only_surface",
		"handler_fields_compute_can_be_overloaded_for_modules",
		"minimal_activity_form_can_be_deferred_from_stage1",
		"fork_policy_defaults_can_be_invented_by_later_consumers",
		"launch_language_scope_is_open_ended_multi_language",
		"design_discussion_or_private_docs_are_runtime_authority",
		"speculative_platform_spec_notes_are_allowed_without_implementation",
		"switch_lookup_threshold_are_standalone_handler_keywords",
	} {
		if !set[claim] {
			problems = append(problems, fmt.Sprintf("invalidated_claims missing %s", claim))
		}
	}
	return problems
}

func validateDeterministicWorkLadderManifestationCoverage(coverage []deterministicWorkLadderDesignManifestation) []string {
	var problems []string
	byID := map[string]deterministicWorkLadderDesignManifestation{}
	for _, manifestation := range coverage {
		if strings.TrimSpace(manifestation.ID) == "" {
			problems = append(problems, "manifestation_coverage contains entry with empty id")
			continue
		}
		if _, exists := byID[manifestation.ID]; exists {
			problems = append(problems, fmt.Sprintf("manifestation_coverage duplicate id %s", manifestation.ID))
			continue
		}
		byID[manifestation.ID] = manifestation
		if manifestation.Status == "" {
			problems = append(problems, fmt.Sprintf("manifestation %s missing status", manifestation.ID))
		}
		if manifestation.Proof == "" {
			problems = append(problems, fmt.Sprintf("manifestation %s missing proof", manifestation.ID))
		}
	}
	for _, id := range []string{
		"stale_logic_node_language",
		"system_node_rename_language",
		"hidden_result_event_risk",
		"compute_name_collision",
		"prose_only_activity_placement",
		"mcp_effect_class_self_classification",
		"minimal_activity_form_ergonomics_gap",
		"fork_policy_definition_gap",
		"artifact_repo_commit_silence",
		"python_launch_language_decision",
		"stale_discussion_authority",
		"speculative_platform_spec_authority",
		"standalone_helper_keyword_drift",
	} {
		if _, ok := byID[id]; !ok {
			problems = append(problems, fmt.Sprintf("manifestation_coverage missing %s", id))
		}
	}
	if artifactSilence, ok := byID["artifact_repo_commit_silence"]; ok && artifactSilence.Status != "closed_by_child_issue" {
		problems = append(problems, fmt.Sprintf("manifestation artifact_repo_commit_silence status = %q, want closed_by_child_issue", artifactSilence.Status))
	}
	return problems
}

func deterministicWorkLadderDesignRepairByID(t *testing.T, record *deterministicWorkLadderDesignRecord, id string) *deterministicWorkLadderDesignRepair {
	t.Helper()
	for i := range record.Repairs {
		if record.Repairs[i].ID == id {
			return &record.Repairs[i]
		}
	}
	t.Fatalf("repair %s not found", id)
	return nil
}

func deterministicWorkLadderDesignConsumersExcept(consumers []deterministicWorkLadderDesignChildConsumer, issue int) []deterministicWorkLadderDesignChildConsumer {
	out := consumers[:0]
	for _, consumer := range consumers {
		if consumer.Issue != issue {
			out = append(out, consumer)
		}
	}
	return out
}

func deterministicWorkLadderDesignConsumerByIssue(t *testing.T, record *deterministicWorkLadderDesignRecord, issue int) deterministicWorkLadderDesignChildConsumer {
	t.Helper()
	for _, consumer := range record.ChildConsumers {
		if consumer.Issue == issue {
			return consumer
		}
	}
	t.Fatalf("child consumer #%d not found", issue)
	return deterministicWorkLadderDesignChildConsumer{}
}

func deterministicWorkLadderManifestationByID(t *testing.T, record *deterministicWorkLadderDesignRecord, id string) deterministicWorkLadderDesignManifestation {
	t.Helper()
	for _, manifestation := range record.ManifestationCoverage {
		if manifestation.ID == id {
			return manifestation
		}
	}
	t.Fatalf("manifestation %s not found", id)
	return deterministicWorkLadderDesignManifestation{}
}

func yamlMapAt(t *testing.T, root map[string]any, path ...string) map[string]any {
	t.Helper()
	current := root
	for _, key := range path {
		current = yamlMapValue(t, current, key)
	}
	return current
}

func yamlStringAt(t *testing.T, root map[string]any, path ...string) string {
	t.Helper()
	if len(path) == 0 {
		t.Fatal("empty yaml string path")
	}
	parent := yamlMapAt(t, root, path[:len(path)-1]...)
	return yamlStringValue(t, parent, path[len(path)-1])
}

func yamlMapValue(t *testing.T, parent map[string]any, key string) map[string]any {
	t.Helper()
	raw, ok := parent[key]
	if !ok {
		t.Fatalf("yaml map missing key %s", key)
	}
	value, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("yaml key %s type = %T, want map[string]any", key, raw)
	}
	return value
}

func yamlStringValue(t *testing.T, parent map[string]any, key string) string {
	t.Helper()
	raw, ok := parent[key]
	if !ok {
		t.Fatalf("yaml map missing key %s", key)
	}
	value, ok := raw.(string)
	if !ok {
		t.Fatalf("yaml key %s type = %T, want string", key, raw)
	}
	return value
}

func deterministicWorkLadderStringsExcept(values []string, remove string) []string {
	out := values[:0]
	for _, value := range values {
		if value != remove {
			out = append(out, value)
		}
	}
	return out
}

func containsSubstring(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}

func recordStringSlice(values []string) []string {
	return values
}
