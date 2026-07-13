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

const temporalFrontierDesignRecordPath = "internal/runtime/conformance/testdata/temporal_frontier_design_record.yaml"

type temporalFrontierDesignRecord struct {
	Version             int                             `yaml:"version"`
	Kind                string                          `yaml:"kind"`
	Issue               int                             `yaml:"issue"`
	ParentIssue         int                             `yaml:"parent_issue"`
	ImplementationIssue int                             `yaml:"implementation_issue"`
	SourceRefs          temporalFrontierSourceRefs      `yaml:"source_refs"`
	Policy              temporalFrontierPolicy          `yaml:"policy"`
	Selectors           temporalFrontierSelectors       `yaml:"selectors"`
	Transaction         temporalFrontierTransaction     `yaml:"transaction"`
	Objects             temporalFrontierObjects         `yaml:"objects"`
	Roles               map[string]temporalFrontierRole `yaml:"roles"`
	Grants              temporalFrontierGrants          `yaml:"grants"`
	FactFamilies        []temporalFrontierFactFamily    `yaml:"fact_families"`
	Operations          map[string]string               `yaml:"operations"`
	Migration           temporalFrontierMigration       `yaml:"migration"`
	Prototype           temporalFrontierPrototype       `yaml:"prototype"`
	Manifestations      []temporalFrontierManifestation `yaml:"manifestations"`
}

type temporalFrontierSourceRefs struct {
	ParentPreAudit   string `yaml:"parent_pre_audit"`
	FinalAuditRepair string `yaml:"final_audit_repair"`
	Gate             string `yaml:"gate"`
	PlatformSpec     string `yaml:"platform_spec"`
}

type temporalFrontierPolicy struct {
	ClosureLevel                       string `yaml:"closure_level"`
	ClaimsRuntimeBehaviorChange        bool   `yaml:"claims_runtime_behavior_change"`
	ClaimsProductionFailureClassClosed bool   `yaml:"claims_production_failure_class_closed"`
	ProductionActivationAllowed        bool   `yaml:"production_activation_allowed"`
	ProductionActivationIssue          int    `yaml:"production_activation_issue"`
	SQLiteSemanticsChanged             bool   `yaml:"sqlite_semantics_changed"`
}

type temporalFrontierSelectors struct {
	Supported     []string `yaml:"supported"`
	Invalid       []string `yaml:"invalid"`
	BookmarkOwner string   `yaml:"bookmark_owner"`
}

type temporalFrontierTransaction struct {
	DeclarationTable                     string   `yaml:"declaration_table"`
	XIDType                              string   `yaml:"xid_type"`
	Dispositions                         []string `yaml:"dispositions"`
	SealedFields                         []string `yaml:"sealed_fields"`
	LockOrder                            []string `yaml:"lock_order"`
	OrdinaryRevisionOnDestructiveCleanup bool     `yaml:"ordinary_revision_on_destructive_cleanup"`
	OrdinalStart                         int      `yaml:"ordinal_start"`
	HeaderRetention                      string   `yaml:"header_retention"`
}

type temporalFrontierObjects struct {
	Tables            []string `yaml:"tables"`
	HistoryTables     []string `yaml:"history_tables"`
	Functions         []string `yaml:"functions"`
	TriggerProperties []string `yaml:"trigger_properties"`
}

type temporalFrontierRole struct {
	Kind                string   `yaml:"kind"`
	DistinctFromRuntime bool     `yaml:"distinct_from_runtime"`
	Invocation          string   `yaml:"invocation"`
	Attributes          []string `yaml:"attributes"`
}

type temporalFrontierGrants struct {
	RuntimeExecute                []string `yaml:"runtime_execute"`
	RuntimeDeniedExecute          []string `yaml:"runtime_denied_execute"`
	CleanupAuthorizerExecute      []string `yaml:"cleanup_authorizer_execute"`
	RuntimeInsertOnly             []string `yaml:"runtime_insert_only"`
	RuntimeGuardedUpdate          []string `yaml:"runtime_guarded_update"`
	RuntimeGuardedDML             []string `yaml:"runtime_guarded_dml"`
	RuntimeDeniedDML              []string `yaml:"runtime_denied_dml"`
	RuntimeDeniedDirectOperations []string `yaml:"runtime_denied_direct_operations"`
}

type temporalFrontierFactFamily struct {
	Name        string `yaml:"name"`
	Disposition string `yaml:"disposition"`
}

type temporalFrontierMigration struct {
	Generation                      string   `yaml:"generation"`
	MigrationID                     string   `yaml:"migration_id"`
	RecognizedLegacyPlatformVersion string   `yaml:"recognized_legacy_platform_version"`
	ActiveStatuses                  []string `yaml:"active_statuses"`
	LegacyRunDisposition            string   `yaml:"legacy_run_disposition"`
	UnknownShape                    string   `yaml:"unknown_shape"`
	HeuristicBackfill               string   `yaml:"heuristic_backfill"`
	Isolation                       string   `yaml:"isolation"`
	DeploymentLocks                 []string `yaml:"deployment_locks"`
	MetadataUpdatedLast             bool     `yaml:"metadata_updated_last"`
	RollbackIsAtomic                bool     `yaml:"rollback_is_atomic"`
	Reapply                         string   `yaml:"reapply"`
	LegacyAdmission                 string   `yaml:"legacy_admission"`
	ServeSchemaMutation             string   `yaml:"serve_schema_mutation"`
}

type temporalFrontierPrototype struct {
	Test            string   `yaml:"test"`
	RunnerOwner     string   `yaml:"runner_owner"`
	ProofProjection string   `yaml:"proof_projection"`
	Assertions      []string `yaml:"assertions"`
}

type temporalFrontierManifestation struct {
	ID     string `yaml:"id"`
	Issue  int    `yaml:"issue"`
	Status string `yaml:"status"`
}

type temporalFrontierSpecDocument struct {
	PlatformTables struct {
		Tables map[string]yaml.Node `yaml:"tables"`
	} `yaml:"platform_tables"`
	RunModel struct {
		Fork struct {
			TemporalFrontier temporalFrontierSpec `yaml:"temporal_frontier"`
		} `yaml:"fork"`
	} `yaml:"run_model"`
}

type temporalFrontierSpec struct {
	PromotedBy                string `yaml:"promoted_by"`
	ParentTracker             string `yaml:"parent_tracker"`
	ProductionActivationOwner string `yaml:"production_activation_owner"`
	Status                    string `yaml:"status"`
	ClosureLevel              string `yaml:"closure_level"`
	CanonicalOwner            string `yaml:"canonical_owner"`
	Selectors                 struct {
		Public  []string `yaml:"public"`
		Invalid []string `yaml:"invalid"`
	} `yaml:"selectors"`
	TransactionContract struct {
		XIDType          string   `yaml:"xid_type"`
		DeclarationOwner string   `yaml:"declaration_owner"`
		LockOrder        []string `yaml:"lock_order"`
	} `yaml:"transaction_contract"`
	SchemaContract struct {
		Generation string `yaml:"generation"`
		DDLStatus  string `yaml:"ddl_status"`
		Tables     map[string]struct {
			DDL string `yaml:"ddl"`
		} `yaml:"tables"`
	} `yaml:"schema_contract"`
	TriggerContract struct {
		Functions  []string `yaml:"functions"`
		Properties []string `yaml:"properties"`
	} `yaml:"trigger_contract"`
	RequiredConformance struct {
		CommittedRecord string `yaml:"committed_record"`
		PostgresTest    string `yaml:"postgres_test"`
		Runner          string `yaml:"runner"`
	} `yaml:"required_conformance"`
}

func TestTemporalFrontierDesignRecordLocksCompleteInactiveContract(t *testing.T) {
	root := conformanceRepoRoot(t)
	record := loadTemporalFrontierDesignRecord(t, root)
	spec := loadTemporalFrontierSpec(t, root)
	if problems := validateTemporalFrontierDesignRecord(root, record, spec); len(problems) > 0 {
		t.Fatalf("temporal frontier design validation failed:\n- %s", strings.Join(problems, "\n- "))
	}
}

func TestTemporalFrontierDesignRecordRejectsClosureAndGuardDrift(t *testing.T) {
	root := conformanceRepoRoot(t)
	base := loadTemporalFrontierDesignRecord(t, root)
	spec := loadTemporalFrontierSpec(t, root)
	tests := []struct {
		name   string
		mutate func(*temporalFrontierDesignRecord)
		want   string
	}{
		{name: "runtime behavior claim", mutate: func(r *temporalFrontierDesignRecord) { r.Policy.ClaimsRuntimeBehaviorChange = true }, want: "claims runtime behavior change"},
		{name: "production activation", mutate: func(r *temporalFrontierDesignRecord) { r.Policy.ProductionActivationAllowed = true }, want: "permits production activation"},
		{name: "missing event delete denial", mutate: func(r *temporalFrontierDesignRecord) {
			r.Grants.RuntimeDeniedDirectOperations = []string{"events.UPDATE"}
		}, want: "runtime denied direct operations"},
		{name: "missing old new guard", mutate: func(r *temporalFrontierDesignRecord) { delete(r.Operations, "mutable_update") }, want: "operations missing mutable_update"},
		{name: "destructive ordinary revision", mutate: func(r *temporalFrontierDesignRecord) { r.Transaction.OrdinaryRevisionOnDestructiveCleanup = true }, want: "destructive cleanup fabricates an ordinary revision"},
		{name: "truncated xid", mutate: func(r *temporalFrontierDesignRecord) { r.Transaction.XIDType = "bigint" }, want: "transaction xid_type"},
		{name: "missing history family", mutate: func(r *temporalFrontierDesignRecord) {
			r.Objects.HistoryTables = stringsExcept(r.Objects.HistoryTables, "event_delivery_history")
		}, want: "history tables missing event_delivery_history"},
		{name: "missing proof", mutate: func(r *temporalFrontierDesignRecord) {
			r.Prototype.Assertions = stringsExcept(r.Prototype.Assertions, "update_move_requires_old_and_new_runs")
		}, want: "prototype assertions missing update_move_requires_old_and_new_runs"},
		{name: "heuristic backfill", mutate: func(r *temporalFrontierDesignRecord) { r.Migration.HeuristicBackfill = "allowed" }, want: "heuristic backfill"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			record := cloneTemporalFrontierRecord(t, base)
			tc.mutate(&record)
			problems := validateTemporalFrontierDesignRecord(root, record, spec)
			if !problemsContain(problems, tc.want) {
				t.Fatalf("validation problems missing %q:\n- %s", tc.want, strings.Join(problems, "\n- "))
			}
		})
	}
}

func loadTemporalFrontierDesignRecord(t *testing.T, root string) temporalFrontierDesignRecord {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, temporalFrontierDesignRecordPath))
	if err != nil {
		t.Fatalf("read temporal frontier design record: %v", err)
	}
	var record temporalFrontierDesignRecord
	if err := yaml.Unmarshal(raw, &record); err != nil {
		t.Fatalf("parse temporal frontier design record: %v", err)
	}
	return record
}

func loadTemporalFrontierSpec(t *testing.T, root string) temporalFrontierSpecDocument {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	var spec temporalFrontierSpecDocument
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
	return spec
}

func cloneTemporalFrontierRecord(t *testing.T, record temporalFrontierDesignRecord) temporalFrontierDesignRecord {
	t.Helper()
	raw, err := yaml.Marshal(record)
	if err != nil {
		t.Fatalf("marshal temporal frontier record: %v", err)
	}
	var cloned temporalFrontierDesignRecord
	if err := yaml.Unmarshal(raw, &cloned); err != nil {
		t.Fatalf("clone temporal frontier record: %v", err)
	}
	return cloned
}

func validateTemporalFrontierDesignRecord(root string, record temporalFrontierDesignRecord, specDoc temporalFrontierSpecDocument) []string {
	var problems []string
	if record.Version != 1 || record.Kind != "temporal_frontier_design_record" {
		problems = append(problems, fmt.Sprintf("record identity = version %d kind %q", record.Version, record.Kind))
	}
	if record.Issue != 2050 || record.ParentIssue != 2049 || record.ImplementationIssue != 2051 {
		problems = append(problems, fmt.Sprintf("issue chain = #%d/#%d/#%d, want #2050/#2049/#2051", record.Issue, record.ParentIssue, record.ImplementationIssue))
	}
	for name, ref := range map[string]string{"parent_pre_audit": record.SourceRefs.ParentPreAudit, "final_audit_repair": record.SourceRefs.FinalAuditRepair, "gate": record.SourceRefs.Gate} {
		if !strings.Contains(ref, "github.com/division-sh/swarm/issues/2049#issuecomment-") {
			problems = append(problems, "source ref "+name+" does not resolve to a #2049 comment")
		}
	}
	if record.SourceRefs.PlatformSpec != "platform-spec.yaml#run_model.fork.temporal_frontier" {
		problems = append(problems, "source_refs platform_spec is not the canonical temporal owner")
	}
	if record.Policy.ClosureLevel != "spec_schema_design_locked" {
		problems = append(problems, "closure level is not spec_schema_design_locked")
	}
	if record.Policy.ClaimsRuntimeBehaviorChange {
		problems = append(problems, "policy claims runtime behavior change")
	}
	if record.Policy.ClaimsProductionFailureClassClosed {
		problems = append(problems, "policy claims production failure class closed")
	}
	if record.Policy.ProductionActivationAllowed {
		problems = append(problems, "policy permits production activation")
	}
	if record.Policy.ProductionActivationIssue != 2051 || record.Policy.SQLiteSemanticsChanged {
		problems = append(problems, "policy activation boundary is not #2051 with unchanged SQLite semantics")
	}

	problems = append(problems, requireExactSet("supported selectors", record.Selectors.Supported, []string{"default_latest_event", "explicit_event_uuid"})...)
	problems = append(problems, requireSet("invalid selectors", record.Selectors.Invalid, []string{"rfc3339_timestamp", "created_at_event_id_cutoff", "caller_supplied_revision"})...)
	if record.Selectors.BookmarkOwner != "event_uuid_resolves_temporal_revision" {
		problems = append(problems, "selector bookmark owner is not event UUID to temporal revision")
	}
	if record.Transaction.DeclarationTable != "run_temporal_transactions" || record.Transaction.XIDType != "xid8" {
		problems = append(problems, "transaction xid_type/declaration owner is not xid8/run_temporal_transactions")
	}
	if record.Transaction.OrdinaryRevisionOnDestructiveCleanup {
		problems = append(problems, "destructive cleanup fabricates an ordinary revision")
	}
	if record.Transaction.OrdinalStart != 1 || record.Transaction.HeaderRetention != "referenced_by_revision_or_tombstone" {
		problems = append(problems, "transaction ordinal/header retention contract drifted")
	}
	problems = append(problems, requireExactSet("transaction dispositions", record.Transaction.Dispositions, []string{"normal", "destructive"})...)
	problems = append(problems, requireExactOrder("transaction lock order", record.Transaction.LockOrder, []string{"nonlocking_identity_snapshot", "sealed_frontier_declaration", "sorted_frontier_locks", "deterministic_fact_locks", "identity_revalidation", "captured_fact_mutation"})...)

	requiredTables := []string{"run_temporal_transactions", "run_temporal_transaction_runs", "run_temporal_frontiers", "run_temporal_revisions", "runtime_store_migrations", "run_cleanup_authorizations", "run_deletion_tombstones"}
	requiredHistory := []string{"run_lifecycle_history", "event_delivery_history", "event_receipt_history", "timer_history", "entity_state_history", "agent_session_history", "conversation_audit_history", "reply_context_history", "activity_attempt_history"}
	requiredFunctions := []string{"swarm_declare_temporal_runs", "swarm_create_run", "swarm_claim_temporal_runs", "swarm_authorize_run_cleanup", "swarm_claim_authorized_run_cleanup", "swarm_delete_authorized_runs", "swarm_next_temporal_ordinal", "swarm_resolve_temporal_run", "swarm_guard_append_fact", "swarm_guard_mutable_fact"}
	problems = append(problems, requireExactSet("tables", record.Objects.Tables, requiredTables)...)
	problems = append(problems, requireExactSet("history tables", record.Objects.HistoryTables, requiredHistory)...)
	problems = append(problems, requireExactSet("functions", record.Objects.Functions, requiredFunctions)...)
	problems = append(problems, requireSet("trigger properties", record.Objects.TriggerProperties, []string{"security_definer", "owner_controlled", "schema_qualified", "pinned_search_path", "public_execute_revoked", "enable_always"})...)

	migrationRole, migrationOK := record.Roles["migration"]
	runtimeRole, runtimeOK := record.Roles["runtime"]
	cleanupAuthorizerRole, cleanupAuthorizerOK := record.Roles["cleanup_authorizer"]
	if !migrationOK || migrationRole.Kind != "schema_owner_login" || !migrationRole.DistinctFromRuntime || !strings.HasPrefix(migrationRole.Invocation, "swarm schema apply ") {
		problems = append(problems, "migration role/invocation contract is incomplete")
	}
	if !runtimeOK || runtimeRole.Kind != "restricted_login" || runtimeRole.Invocation != "swarm serve --config <runtime-config>" {
		problems = append(problems, "runtime role/invocation contract is incomplete")
	}
	if !cleanupAuthorizerOK || cleanupAuthorizerRole.Kind != "restricted_cleanup_authorizer_login" || !cleanupAuthorizerRole.DistinctFromRuntime || cleanupAuthorizerRole.Invocation != "canonical destructive-reset plan/quiescence/directive owner" {
		problems = append(problems, "cleanup authorizer role/invocation contract is incomplete")
	}
	problems = append(problems, requireExactSet("runtime denied direct operations", record.Grants.RuntimeDeniedDirectOperations, []string{"events.UPDATE", "events.DELETE", "runs.INSERT", "runs.DELETE"})...)
	problems = append(problems, requireExactSet("runtime guarded update", record.Grants.RuntimeGuardedUpdate, []string{"runs"})...)
	problems = append(problems, requireExactSet("runtime execute", record.Grants.RuntimeExecute, []string{"swarm_create_run", "swarm_claim_temporal_runs", "swarm_claim_authorized_run_cleanup", "swarm_delete_authorized_runs"})...)
	problems = append(problems, requireSet("runtime denied execute", record.Grants.RuntimeDeniedExecute, []string{"swarm_declare_temporal_runs", "swarm_authorize_run_cleanup", "swarm_next_temporal_ordinal", "swarm_resolve_temporal_run", "swarm_guard_append_fact", "swarm_guard_mutable_fact"})...)
	problems = append(problems, requireExactSet("cleanup authorizer execute", record.Grants.CleanupAuthorizerExecute, []string{"swarm_authorize_run_cleanup"})...)

	requiredFacts := []string{"runs", "events", "event_deliveries", "event_receipts", "dead_letters", "entity_mutations", "entity_state", "timers", "agent_sessions", "agent_turns", "agent_conversation_audits", "reply_contexts", "activity_attempts", "selected_fork_lineage", "routing_rules", "runless_events"}
	facts := make([]string, 0, len(record.FactFamilies))
	for _, family := range record.FactFamilies {
		facts = append(facts, family.Name)
		if strings.TrimSpace(family.Disposition) == "" {
			problems = append(problems, "fact family "+family.Name+" has no disposition")
		}
	}
	problems = append(problems, requireExactSet("fact families", facts, requiredFacts)...)
	for _, operation := range []string{"run_insert", "event_insert", "event_update", "event_delete", "mutable_insert", "mutable_update", "mutable_delete", "whole_run_delete", "mixed_cleanup", "derived_lineage"} {
		if strings.TrimSpace(record.Operations[operation]) == "" {
			problems = append(problems, "operations missing "+operation)
		}
	}
	if record.Operations["mutable_update"] != "every_distinct_old_and_new_nonnull_run_declared" {
		problems = append(problems, "mutable_update does not require OLD and NEW non-null runs")
	}

	if record.Migration.Generation != "temporal-frontier-v1" || record.Migration.MigrationID != "temporal-frontier-v1" || record.Migration.RecognizedLegacyPlatformVersion != "0.7.0" {
		problems = append(problems, "migration generation/edge drifted")
	}
	if record.Migration.HeuristicBackfill != "forbidden" {
		problems = append(problems, "migration heuristic backfill is not forbidden")
	}
	if record.Migration.LegacyAdmission != "complete_registered_catalog_checksum" || record.Migration.Reapply != "full_catalog_revalidation_then_exact_checksum_noop_else_fail_closed" {
		problems = append(problems, "migration catalog admission/reapply contract drifted")
	}
	if record.Migration.Isolation != "SERIALIZABLE" || !record.Migration.MetadataUpdatedLast || !record.Migration.RollbackIsAtomic || record.Migration.ServeSchemaMutation != "forbidden" {
		problems = append(problems, "migration atomicity/admission contract drifted")
	}
	problems = append(problems, requireExactSet("active statuses", record.Migration.ActiveStatuses, []string{"running", "paused"})...)
	problems = append(problems, requireExactSet("deployment locks", record.Migration.DeploymentLocks, []string{"runtime_shared_session_advisory", "migration_exclusive_advisory", "first_upgrade_access_exclusive_tables"})...)

	requiredAssertions := []string{"fresh_schema_creation", "restricted_runtime_atomic_run_creation", "active_legacy_run_rejected_without_ddl", "complete_legacy_catalog_drift_rejected", "failed_migration_rolls_back_schema_and_metadata", "exact_reapply_is_idempotent", "drifted_target_reapply_rejected", "runtime_shared_lock_excludes_migration", "runtime_cannot_assume_owner_or_disable_trigger", "runtime_cannot_mutate_authority_or_history_tables", "undeclared_insert_update_delete_rejected", "every_guarded_fact_family_insert_update_delete_proven", "derived_lineage_mismatch_rejected", "destructive_declaration_not_runtime_callable", "event_delivery_receipt_share_revision_and_ordinals", "rollback_publishes_no_revision", "update_move_requires_old_and_new_runs", "runless_lineage_remains_unversioned", "authorized_unversioned_destructive_cleanup_creates_no_revision", "arbitrary_run_deletion_rejected", "cleanup_authorization_is_not_runtime_mintable", "cleanup_authorizer_has_no_fact_or_schema_privileges", "mixed_cleanup_revisions_survivors_and_tombstones_deleted_runs", "reverse_order_claims_serialize_frontier_first", "trigger_functions_are_not_runtime_bypass_surfaces", "second_runtime_boot_is_read_only"}
	if record.Prototype.Test != "TestTemporalFrontierPostgresDesignConformance" || record.Prototype.RunnerOwner != "platform-spec.yaml#run_model.fork.temporal_frontier.required_conformance.runner" || record.Prototype.ProofProjection != "required-full" {
		problems = append(problems, "prototype test/runner owner/proof projection does not match the dedicated supported command")
	}
	problems = append(problems, requireExactSet("prototype assertions", record.Prototype.Assertions, requiredAssertions)...)

	spec := specDoc.RunModel.Fork.TemporalFrontier
	if spec.PromotedBy != "#2050" || spec.ParentTracker != "#2049" || spec.ProductionActivationOwner != "#2051" || spec.Status != "design_locked_not_activated" || spec.ClosureLevel != "spec_schema_design_locked" {
		problems = append(problems, "platform spec issue/status boundary drifted")
	}
	if strings.TrimSpace(spec.CanonicalOwner) == "" || spec.SchemaContract.Generation != record.Migration.Generation || spec.SchemaContract.DDLStatus != "exact_target_ddl_inactive_until_2051" {
		problems = append(problems, "platform spec canonical owner/schema generation drifted")
	}
	problems = append(problems, requireExactSet("spec supported selectors", spec.Selectors.Public, record.Selectors.Supported)...)
	problems = append(problems, requireExactSet("spec invalid selectors", spec.Selectors.Invalid, record.Selectors.Invalid)...)
	if spec.TransactionContract.XIDType != "xid8" || spec.TransactionContract.DeclarationOwner != record.Transaction.DeclarationTable {
		problems = append(problems, "platform spec transaction owner/xid drifted")
	}
	for _, table := range append(append([]string(nil), requiredTables...), requiredHistory...) {
		entry, ok := spec.SchemaContract.Tables[table]
		if !ok || strings.TrimSpace(entry.DDL) == "" {
			problems = append(problems, "platform spec exact DDL missing "+table)
		}
		if strings.HasSuffix(table, "_history") && (!strings.Contains(entry.DDL, "FOREIGN KEY (run_id, revision)") || !strings.Contains(entry.DDL, "REFERENCES run_temporal_revisions(run_id, revision)")) {
			problems = append(problems, "platform spec history DDL missing revision foreign key "+table)
		}
		if _, active := specDoc.PlatformTables.Tables[table]; active {
			problems = append(problems, "inactive temporal table appears in production platform_tables: "+table)
		}
	}
	problems = append(problems, requireExactSet("spec trigger functions", spec.TriggerContract.Functions, record.Objects.Functions)...)
	if spec.RequiredConformance.CommittedRecord != temporalFrontierDesignRecordPath || spec.RequiredConformance.PostgresTest != record.Prototype.Test || strings.TrimSpace(spec.RequiredConformance.Runner) == "" {
		problems = append(problems, "platform spec required conformance drifted from record")
	}
	if _, err := os.Stat(filepath.Join(root, temporalFrontierDesignRecordPath)); err != nil {
		problems = append(problems, "committed temporal frontier design record is missing")
	}

	sort.Strings(problems)
	return problems
}

func requireExactSet(label string, got, want []string) []string {
	problems := requireSet(label, got, want)
	problems = append(problems, requireSet(label+" unexpected", want, got)...)
	return problems
}

func requireSet(label string, got, want []string) []string {
	present := make(map[string]bool, len(got))
	for _, value := range got {
		present[value] = true
	}
	var problems []string
	for _, value := range want {
		if !present[value] {
			problems = append(problems, label+" missing "+value)
		}
	}
	return problems
}

func requireExactOrder(label string, got, want []string) []string {
	if strings.Join(got, "\x00") == strings.Join(want, "\x00") {
		return nil
	}
	return []string{fmt.Sprintf("%s = %v, want %v", label, got, want)}
}

func stringsExcept(values []string, excluded string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != excluded {
			out = append(out, value)
		}
	}
	return out
}

func problemsContain(problems []string, want string) bool {
	for _, problem := range problems {
		if strings.Contains(problem, want) {
			return true
		}
	}
	return false
}
