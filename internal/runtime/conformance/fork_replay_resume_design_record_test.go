package conformance

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/store"
	"gopkg.in/yaml.v3"
)

const forkReplayResumeDesignRecordPath = "internal/runtime/conformance/testdata/fork_replay_resume_design_record.yaml"

type forkReplayResumeDesignRecord struct {
	Version             int                         `yaml:"version"`
	Kind                string                      `yaml:"kind"`
	Issue               int                         `yaml:"issue"`
	ParentIssue         int                         `yaml:"parent_issue"`
	SourceRefs          forkReplayResumeSourceRefs  `yaml:"source_refs"`
	Policy              forkReplayResumePolicy      `yaml:"policy"`
	Watchlist           forkReplayResumeWatchlist   `yaml:"watchlist"`
	StaleLanguagePolicy forkReplayResumeStalePolicy `yaml:"stale_language_policy"`
	Successors          []forkReplayResumeSuccessor `yaml:"successors"`
	Rows                []forkReplayResumeDesignRow `yaml:"rows"`
}

type forkReplayResumeSourceRefs struct {
	PreAuditComment     string `yaml:"pre_audit_comment"`
	GateComment         string `yaml:"gate_comment"`
	LockedDesignComment string `yaml:"locked_design_comment"`
	PlatformSpec        string `yaml:"platform_spec"`
}

type forkReplayResumePolicy struct {
	ClosureLevel                 string `yaml:"closure_level"`
	ClaimsRuntimeBehaviorChange  bool   `yaml:"claims_runtime_behavior_change"`
	ClaimsFullHistoricalResume   bool   `yaml:"claims_full_historical_resume"`
	RuntimeBehaviorChangeAllowed bool   `yaml:"runtime_behavior_change_allowed"`
	RequiredFullTestCommand      string `yaml:"required_full_test_command"`
}

type forkReplayResumeWatchlist struct {
	Node                string `yaml:"node"`
	PrivateRepo         string `yaml:"private_repo"`
	ExternalRepairOwner string `yaml:"external_repair_owner"`
	Mapping             string `yaml:"mapping"`
}

type forkReplayResumeStalePolicy struct {
	ForbiddenPhrases            []string                           `yaml:"forbidden_phrases"`
	ForbiddenPublicReadbackKeys []string                           `yaml:"forbidden_public_readback_keys"`
	LegacyIdentifierSentinels   []forkReplayResumeLegacyIdentifier `yaml:"legacy_identifier_sentinels"`
}

type forkReplayResumeLegacyIdentifier struct {
	Identifier string `yaml:"identifier"`
	Reason     string `yaml:"reason"`
}

type forkReplayResumeSuccessor struct {
	Issue   int    `yaml:"issue"`
	Role    string `yaml:"role"`
	Concept string `yaml:"concept"`
}

type forkReplayResumeDesignRow struct {
	ID               string                     `yaml:"id"`
	Fact             string                     `yaml:"fact"`
	Disposition      string                     `yaml:"disposition"`
	Owner            string                     `yaml:"owner"`
	BlockerCodes     []string                   `yaml:"blocker_codes"`
	EnforcementPoint string                     `yaml:"enforcement_point"`
	ProofRefs        []forkReplayResumeProofRef `yaml:"proof_refs"`
	Rationale        string                     `yaml:"rationale"`
}

type forkReplayResumeProofRef struct {
	Kind  string `yaml:"kind"`
	Name  string `yaml:"name,omitempty"`
	Issue int    `yaml:"issue,omitempty"`
}

func TestForkReplayResumeDesignRecordCoversBoundedModel(t *testing.T) {
	root := conformanceRepoRoot(t)
	record := loadForkReplayResumeDesignRecord(t, root)
	ctx := forkReplayResumeValidationContext{
		goTests: collectGoTestNames(t, root),
	}
	if problems := validateForkReplayResumeDesignRecord(root, record, ctx); len(problems) > 0 {
		t.Fatalf("fork replay/resume design record validation failed:\n- %s", strings.Join(problems, "\n- "))
	}
}

func TestForkReplayResumeDesignRecordRejectsStaleOrUnmappedClaims(t *testing.T) {
	root := conformanceRepoRoot(t)
	base := loadForkReplayResumeDesignRecord(t, root)
	ctx := forkReplayResumeValidationContext{
		goTests: collectGoTestNames(t, root),
	}
	tests := []struct {
		name   string
		mutate func(*forkReplayResumeDesignRecord)
		want   string
	}{
		{
			name: "runtime behavior claim",
			mutate: func(record *forkReplayResumeDesignRecord) {
				record.Policy.ClaimsRuntimeBehaviorChange = true
			},
			want: "policy claims_runtime_behavior_change = true, want false",
		},
		{
			name: "full resume claim",
			mutate: func(record *forkReplayResumeDesignRecord) {
				record.Policy.ClaimsFullHistoricalResume = true
			},
			want: "policy claims_full_historical_resume = true, want false",
		},
		{
			name: "missing delivery pending row",
			mutate: func(record *forkReplayResumeDesignRecord) {
				record.Rows = forkReplayResumeRowsExcept(record.Rows, "delivery_pending_history")
			},
			want: "rows missing required fact delivery_pending_history",
		},
		{
			name: "fail closed row without blocker",
			mutate: func(record *forkReplayResumeDesignRecord) {
				row := forkReplayResumeRowByID(t, record, "delivery_in_progress_history")
				row.BlockerCodes = nil
			},
			want: "delivery_in_progress_history fail-closed row missing blocker_codes",
		},
		{
			name: "unmapped blocker",
			mutate: func(record *forkReplayResumeDesignRecord) {
				for i := range record.Rows {
					record.Rows[i].BlockerCodes = forkReplayResumeStringsExcept(record.Rows[i].BlockerCodes, store.RunForkBlockerTimerHistoryUnproven)
				}
			},
			want: "blocker code timer_history_unproven is not mapped by any row",
		},
		{
			name: "stale proof test",
			mutate: func(record *forkReplayResumeDesignRecord) {
				row := forkReplayResumeRowByID(t, record, "delivery_pending_history")
				row.ProofRefs[0].Name = "TestMissingForkReplayResumeProof"
			},
			want: "delivery_pending_history go_test proof_ref TestMissingForkReplayResumeProof does not resolve",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			record := base
			record.Rows = append([]forkReplayResumeDesignRow(nil), base.Rows...)
			tc.mutate(&record)
			problems := validateForkReplayResumeDesignRecord(root, record, ctx)
			if !routeAuthorityProblemsContain(problems, tc.want) {
				t.Fatalf("validation problems missing %q:\n- %s", tc.want, strings.Join(problems, "\n- "))
			}
		})
	}
}

type forkReplayResumeValidationContext struct {
	goTests map[string]bool
}

func loadForkReplayResumeDesignRecord(t *testing.T, root string) forkReplayResumeDesignRecord {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, forkReplayResumeDesignRecordPath))
	if err != nil {
		t.Fatalf("read fork replay/resume design record: %v", err)
	}
	var record forkReplayResumeDesignRecord
	if err := yaml.Unmarshal(raw, &record); err != nil {
		t.Fatalf("parse fork replay/resume design record: %v", err)
	}
	return record
}

func validateForkReplayResumeDesignRecord(root string, record forkReplayResumeDesignRecord, ctx forkReplayResumeValidationContext) []string {
	var problems []string
	if record.Version != 1 {
		problems = append(problems, fmt.Sprintf("version = %d, want 1", record.Version))
	}
	if record.Kind != "fork_replay_resume_design_record" {
		problems = append(problems, fmt.Sprintf("kind = %q, want fork_replay_resume_design_record", record.Kind))
	}
	if record.Issue != 1723 || record.ParentIssue != 564 {
		problems = append(problems, fmt.Sprintf("issue/parent = #%d/#%d, want #1723/#564", record.Issue, record.ParentIssue))
	}
	problems = append(problems, validateForkReplayResumeRefs(root, record.SourceRefs)...)
	problems = append(problems, validateForkReplayResumePolicy(record.Policy)...)
	problems = append(problems, validateForkReplayResumeWatchlist(record.Watchlist)...)
	problems = append(problems, validateForkReplayResumeStalePolicy(root, record.StaleLanguagePolicy)...)
	problems = append(problems, validateForkReplayResumeSuccessors(record.Successors)...)

	rowsByFact := map[string]forkReplayResumeDesignRow{}
	blockerRows := map[string]string{}
	for _, row := range record.Rows {
		problems = append(problems, validateForkReplayResumeRow(root, row, ctx)...)
		fact := strings.TrimSpace(row.Fact)
		if _, exists := rowsByFact[fact]; exists {
			problems = append(problems, fmt.Sprintf("rows duplicate fact %s", fact))
		}
		rowsByFact[fact] = row
		for _, blocker := range row.BlockerCodes {
			blocker = strings.TrimSpace(blocker)
			if blocker == "" {
				continue
			}
			blockerRows[blocker] = row.ID
		}
	}
	for _, fact := range requiredForkReplayResumeFacts() {
		if _, ok := rowsByFact[fact]; !ok {
			problems = append(problems, fmt.Sprintf("rows missing required fact %s", fact))
		}
	}
	for _, blocker := range requiredForkReplayResumeBlockers() {
		if _, ok := blockerRows[blocker]; !ok {
			problems = append(problems, fmt.Sprintf("blocker code %s is not mapped by any row", blocker))
		}
	}
	sort.Strings(problems)
	return problems
}

func validateForkReplayResumeRefs(root string, refs forkReplayResumeSourceRefs) []string {
	var problems []string
	for label, value := range map[string]string{
		"pre_audit_comment":     refs.PreAuditComment,
		"gate_comment":          refs.GateComment,
		"locked_design_comment": refs.LockedDesignComment,
	} {
		if !strings.Contains(value, "github.com/division-sh/swarm/issues/1723#issuecomment-") {
			problems = append(problems, fmt.Sprintf("source_refs %s missing #1723 issue comment URL", label))
		}
	}
	if refs.PlatformSpec != "platform-spec.yaml" {
		problems = append(problems, fmt.Sprintf("source_refs platform_spec = %q, want platform-spec.yaml", refs.PlatformSpec))
	} else if _, err := os.Stat(filepath.Join(root, refs.PlatformSpec)); err != nil {
		problems = append(problems, "source_refs platform_spec path does not exist")
	}
	return problems
}

func validateForkReplayResumePolicy(policy forkReplayResumePolicy) []string {
	var problems []string
	if policy.ClosureLevel != "spec_design_conformance_closure" {
		problems = append(problems, fmt.Sprintf("policy closure_level = %q, want spec_design_conformance_closure", policy.ClosureLevel))
	}
	if policy.ClaimsRuntimeBehaviorChange {
		problems = append(problems, "policy claims_runtime_behavior_change = true, want false")
	}
	if policy.ClaimsFullHistoricalResume {
		problems = append(problems, "policy claims_full_historical_resume = true, want false")
	}
	if policy.RuntimeBehaviorChangeAllowed {
		problems = append(problems, "policy runtime_behavior_change_allowed = true, want false")
	}
	if strings.TrimSpace(policy.RequiredFullTestCommand) != "go test ./..." {
		problems = append(problems, fmt.Sprintf("policy required_full_test_command = %q, want go test ./...", policy.RequiredFullTestCommand))
	}
	return problems
}

func validateForkReplayResumeWatchlist(watchlist forkReplayResumeWatchlist) []string {
	var problems []string
	if watchlist.Node != "timestamp_fork_replay_resume_ownership" {
		problems = append(problems, fmt.Sprintf("watchlist node = %q, want timestamp_fork_replay_resume_ownership", watchlist.Node))
	}
	if watchlist.PrivateRepo != "swarm-docs" {
		problems = append(problems, fmt.Sprintf("watchlist private_repo = %q, want swarm-docs", watchlist.PrivateRepo))
	}
	if watchlist.ExternalRepairOwner != "lead_after_merge" {
		problems = append(problems, fmt.Sprintf("watchlist external_repair_owner = %q, want lead_after_merge", watchlist.ExternalRepairOwner))
	}
	for _, fragment := range []string{"#1723", "#564", "#1721", "#1722"} {
		if !strings.Contains(watchlist.Mapping, fragment) {
			problems = append(problems, fmt.Sprintf("watchlist mapping missing %s", fragment))
		}
	}
	return problems
}

func validateForkReplayResumeStalePolicy(root string, policy forkReplayResumeStalePolicy) []string {
	var problems []string
	if len(policy.LegacyIdentifierSentinels) == 0 {
		problems = append(problems, "stale_language_policy missing legacy_identifier_sentinels")
	}
	for _, sentinel := range policy.LegacyIdentifierSentinels {
		if strings.TrimSpace(sentinel.Identifier) == "" || strings.TrimSpace(sentinel.Reason) == "" {
			problems = append(problems, "legacy_identifier_sentinel missing identifier or reason")
		}
	}
	scanPaths := []string{
		"platform-spec.yaml",
		"internal/store",
		"internal/runtime/runforkexecution",
		"cmd/swarm",
	}
	for _, rel := range scanPaths {
		path := filepath.Join(root, rel)
		if err := filepath.WalkDir(path, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if filepath.Ext(path) != ".go" && filepath.Base(path) != "platform-spec.yaml" {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			lower := strings.ToLower(string(raw))
			for _, phrase := range policy.ForbiddenPhrases {
				phrase = strings.ToLower(strings.TrimSpace(phrase))
				if phrase != "" && strings.Contains(lower, phrase) {
					problems = append(problems, fmt.Sprintf("forbidden stale phrase %q still appears in %s", phrase, conformanceRelPath(root, path)))
				}
			}
			for _, key := range policy.ForbiddenPublicReadbackKeys {
				key = strings.TrimSpace(key)
				if key != "" && strings.Contains(string(raw), key) {
					problems = append(problems, fmt.Sprintf("forbidden public readback key %q still appears in %s", key, conformanceRelPath(root, path)))
				}
			}
			return nil
		}); err != nil {
			problems = append(problems, fmt.Sprintf("scan stale language under %s: %v", rel, err))
		}
	}
	return problems
}

func validateForkReplayResumeSuccessors(successors []forkReplayResumeSuccessor) []string {
	var problems []string
	seen := map[int]forkReplayResumeSuccessor{}
	for _, successor := range successors {
		seen[successor.Issue] = successor
		if strings.TrimSpace(successor.Role) == "" || strings.TrimSpace(successor.Concept) == "" {
			problems = append(problems, fmt.Sprintf("successor #%d missing role or concept", successor.Issue))
		}
	}
	if _, ok := seen[1721]; !ok {
		problems = append(problems, "successors missing #1721")
	}
	if _, ok := seen[1722]; !ok {
		problems = append(problems, "successors missing #1722")
	}
	return problems
}

func validateForkReplayResumeRow(root string, row forkReplayResumeDesignRow, ctx forkReplayResumeValidationContext) []string {
	var problems []string
	id := strings.TrimSpace(row.ID)
	if id == "" {
		return []string{"row missing id"}
	}
	if strings.TrimSpace(row.Fact) == "" {
		problems = append(problems, fmt.Sprintf("%s missing fact", id))
	}
	if _, ok := allowedForkReplayResumeDispositions()[row.Disposition]; !ok {
		problems = append(problems, fmt.Sprintf("%s disposition %q is not allowed", id, row.Disposition))
	}
	if strings.TrimSpace(row.Owner) == "" {
		problems = append(problems, fmt.Sprintf("%s missing owner", id))
	}
	if strings.TrimSpace(row.EnforcementPoint) == "" {
		problems = append(problems, fmt.Sprintf("%s missing enforcement_point", id))
	} else if row.EnforcementPoint != "issue-tracker" {
		path := filepath.Join(root, filepath.Clean(row.EnforcementPoint))
		if filepath.IsAbs(row.EnforcementPoint) || strings.HasPrefix(filepath.Clean(row.EnforcementPoint), "..") {
			problems = append(problems, fmt.Sprintf("%s enforcement_point %s must be repo-relative", id, row.EnforcementPoint))
		} else if _, err := os.Stat(path); err != nil {
			problems = append(problems, fmt.Sprintf("%s enforcement_point %s does not exist", id, row.EnforcementPoint))
		}
	}
	if strings.Contains(row.Disposition, "fail_closed") && len(row.BlockerCodes) == 0 {
		problems = append(problems, fmt.Sprintf("%s fail-closed row missing blocker_codes", id))
	}
	if len(row.ProofRefs) == 0 {
		problems = append(problems, fmt.Sprintf("%s missing proof_refs", id))
	}
	if strings.TrimSpace(row.Rationale) == "" {
		problems = append(problems, fmt.Sprintf("%s missing rationale", id))
	}
	for _, ref := range row.ProofRefs {
		problems = append(problems, validateForkReplayResumeProofRef(id, ref, ctx)...)
	}
	return problems
}

func validateForkReplayResumeProofRef(rowID string, ref forkReplayResumeProofRef, ctx forkReplayResumeValidationContext) []string {
	var problems []string
	switch ref.Kind {
	case "go_test":
		if strings.TrimSpace(ref.Name) == "" {
			problems = append(problems, fmt.Sprintf("%s go_test proof_ref missing name", rowID))
			return problems
		}
		if !ctx.goTests[ref.Name] {
			problems = append(problems, fmt.Sprintf("%s go_test proof_ref %s does not resolve", rowID, ref.Name))
		}
	case "issue":
		if ref.Issue <= 0 {
			problems = append(problems, fmt.Sprintf("%s issue proof_ref missing issue", rowID))
		}
	default:
		problems = append(problems, fmt.Sprintf("%s proof_ref kind %q is not allowed", rowID, ref.Kind))
	}
	return problems
}

func requiredForkReplayResumeFacts() []string {
	return []string{
		store.RunForkReplayResumeFactEntityStateSnapshot,
		store.RunForkReplayResumeFactDeliveryCompletedHistory,
		store.RunForkReplayResumeFactDeliveryPendingHistory,
		store.RunForkReplayResumeFactDeliveryInProgressHistory,
		store.RunForkReplayResumeFactDeliveryFailedHistory,
		store.RunForkReplayResumeFactDeliveryDeadLetterHistory,
		store.RunForkReplayResumeFactCommittedReplayScope,
		store.RunForkReplayResumeFactTimerHistory,
		store.RunForkReplayResumeFactRouteHistory,
		store.RunForkReplayResumeFactSessionHistory,
		store.RunForkReplayResumeFactConversationAuditHistory,
		store.RunForkReplayResumeFactActiveTurnHistory,
		store.RunForkReplayResumeFactSourceAdvanced,
		store.RunForkReplayResumeFactForkReplayState,
		store.RunForkReplayResumeFactContractSwap,
		store.RunForkReplayResumeFactHistoricalReplayExecution,
		store.RunForkHistoricalReplayFactSourceEvents,
		store.RunForkHistoricalReplayFactEventDeliveries,
		store.RunForkHistoricalReplayFactReceipts,
		store.RunForkHistoricalReplayFactDeadLetters,
		store.RunForkHistoricalReplayFactRetryIdempotency,
		store.RunForkHistoricalReplayFactEmittedFollowUps,
		store.RunForkHistoricalReplayFactTimers,
		store.RunForkHistoricalReplayFactRoutes,
		store.RunForkHistoricalReplayFactSessions,
		store.RunForkHistoricalReplayFactTurns,
		store.RunForkHistoricalReplayFactAudits,
		store.RunForkHistoricalReplayFactNonAgentNodeSystemWork,
		store.RunForkHistoricalReplayFactSourceAdvancedPostTFacts,
		store.RunForkHistoricalReplayFactRuntimeRestartRecovery,
		store.RunForkHistoricalReplayFactCLIApiDashboardOperator,
		"selected_contract_prerequisite_blockers",
		"memoized_reexecution_reserved",
	}
}

func requiredForkReplayResumeBlockers() []string {
	return []string{
		store.RunForkBlockerDeliveryHistoryUnproven,
		store.RunForkBlockerNonAgentDeliveryReplayUnsupported,
		store.RunForkBlockerCommittedReplayScopeReplayUnsupported,
		store.RunForkBlockerTimerHistoryUnproven,
		store.RunForkBlockerFlowRouteHistoryUnproven,
		store.RunForkBlockerSessionHistoryUnproven,
		store.RunForkBlockerConversationAuditUnproven,
		store.RunForkBlockerActiveTurnHistoryUnproven,
		store.RunForkBlockerEntitySnapshotMetadataUnproven,
		store.RunForkBlockerSelectedContractExecutionModelNonMutating,
		store.RunForkBlockerSelectedContractExecutionAdmissionNonMutating,
		store.RunForkBlockerSelectedContractSourceReplayUnsupported,
		store.RunForkBlockerSelectedContractRouteAdmissionNonMutating,
		store.RunForkBlockerSelectedContractRouteTopologyNonMutating,
		store.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven,
		store.RunForkBlockerSelectedContractRecipientPlanningNonMutating,
		store.RunForkBlockerSelectedContractAgentHandlerMaterializationUnsupported,
		store.RunForkBlockerContractSwapBootResumeAdmissionNonMutating,
		store.RunForkBlockerContractSwapRouteRecoveryMissing,
		store.RunForkBlockerHistoricalReplayExecutionAdmissionNonMutating,
		store.RunForkBlockerContractFrontierExecutionUnsupported,
		store.RunForkBlockerContractFrontierRouteUnresolved,
		"source_events_advanced_after_fork_point",
		"source_timers_advanced_after_fork_point",
		"source_routes_advanced_after_fork_point",
		"source_committed_replay_scope_advanced_after_fork_point",
		"source_active_conversation_session_coupling_after_fork_point",
		"fork_events_already_exist",
		"fork_sessions_already_exist",
		"fork_conversation_audits_already_exist",
		"fork_turns_already_exist",
	}
}

func allowedForkReplayResumeDispositions() map[string]struct{} {
	values := []string{
		store.RunForkReplayResumeDispositionReconstruct,
		store.RunForkReplayResumeDispositionForkReplay,
		store.RunForkReplayResumeDispositionLineageOnly,
		store.RunForkReplayResumeDispositionFailClosedBlocker,
		store.RunForkReplayResumeDispositionSplitSibling,
		store.RunForkReplayResumeDispositionNoHistoricalAction,
		store.RunForkHistoricalReplayAdmissionExecutableForkWork,
		store.RunForkHistoricalReplayAdmissionReconstructedForkState,
		store.RunForkHistoricalReplayAdmissionLineageOnlyEvidence,
		store.RunForkHistoricalReplayAdmissionFailClosedBlocker,
		store.RunForkHistoricalReplayAdmissionSplitSibling,
		"fail_closed_or_reconstruct",
		"fail_closed_or_lineage",
		"reserved_future",
	}
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

func forkReplayResumeRowByID(t *testing.T, record *forkReplayResumeDesignRecord, id string) *forkReplayResumeDesignRow {
	t.Helper()
	for i := range record.Rows {
		if record.Rows[i].ID == id {
			return &record.Rows[i]
		}
	}
	t.Fatalf("row %s not found", id)
	return nil
}

func forkReplayResumeRowsExcept(rows []forkReplayResumeDesignRow, id string) []forkReplayResumeDesignRow {
	out := make([]forkReplayResumeDesignRow, 0, len(rows))
	for _, row := range rows {
		if row.ID != id {
			out = append(out, row)
		}
	}
	return out
}

func forkReplayResumeStringsExcept(values []string, unwanted string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != unwanted {
			out = append(out, value)
		}
	}
	return out
}

func conformanceRelPath(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return path
	}
	return rel
}
