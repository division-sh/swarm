package store

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	stdruntime "runtime"
	"sort"
	"strings"
	"testing"
)

var runInsertPattern = regexp.MustCompile(`(?is)INSERT\s+INTO\s+runs\s*\(([^)]*)\)`)

func TestRepositoryRunCreationHasCanonicalOwnersOnly(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	wantOwners := map[string]bool{
		"internal/store/runlifecycle/fork.go":  false,
		"internal/store/runlifecycle/owner.go": false,
	}
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel := repositoryRelativePath(t, root, path)
		if rel == "internal/store/storetest/event.go" {
			return nil
		}
		source := readRepositoryFile(t, path)
		if !runInsertPattern.MatchString(source) {
			return nil
		}
		if _, ok := wantOwners[rel]; !ok {
			t.Errorf("%s can create runs outside the canonical run lifecycle owner", rel)
			return nil
		}
		wantOwners[rel] = true
		return nil
	})
	if err != nil {
		t.Fatalf("walk production run writers: %v", err)
	}
	for owner, found := range wantOwners {
		if !found {
			t.Errorf("canonical run lifecycle owner %s no longer contains its run insert", owner)
		}
	}
}

func TestRepositoryRunInsertFixturesHaveExplicitCanonicalIdentity(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	for _, rel := range bundleIdentityFixtureLedger {
		path := filepath.Join(root, filepath.FromSlash(rel))
		switch rel {
		case "internal/store/run_bundle_fingerprint_test.go":
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Errorf("retired compatibility fixture %s still exists", rel)
			}
		default:
			if _, err := os.Stat(path); err != nil {
				t.Errorf("fixture ledger row %s is missing: %v", rel, err)
			}
		}
	}

	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		rel := repositoryRelativePath(t, root, path)
		if rel == "internal/store/runlifecycle/fork.go" || rel == "internal/store/runlifecycle/owner.go" {
			return nil
		}
		source := readRepositoryFile(t, path)
		for _, match := range runInsertPattern.FindAllStringSubmatch(source, -1) {
			columns := normalizedSQLColumns(match[1])
			if rel == "internal/store/schema_compatibility_bootstrap_test.go" {
				if columns["bundle_hash"] != columns["bundle_source"] {
					t.Errorf("%s contains a partially specified old-store run insert", rel)
				}
				continue
			}
			if !columns["bundle_hash"] || !columns["bundle_source"] {
				t.Errorf("%s contains a run fixture insert without explicit bundle_hash and bundle_source", rel)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk run fixture inserts: %v", err)
	}
}

func TestRepositoryContainsNoLegacyBundleIdentityInterpreter(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	prohibited := []string{
		"bundle_" + "ref",
		"Bundle" + "Ref",
		"Bundle" + "Fingerprint",
		"bundle_" + "fingerprint",
		"BundleSource" + "Legacy",
		"bundle-" + "fingerprint",
		"legacy_bundle_" + "fingerprint",
		"UNSUPPORTED_BUNDLE_" + "REF",
	}
	var files []string
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk production sources: %v", err)
	}
	files = append(files, filepath.Join(root, "platform-spec.yaml"), filepath.Join(root, "openrpc.json"))
	sort.Strings(files)
	for _, path := range files {
		source := readRepositoryFile(t, path)
		for _, retired := range prohibited {
			if strings.Contains(source, retired) {
				t.Errorf("%s retains retired bundle identity interpreter %q", repositoryRelativePath(t, root, path), retired)
			}
		}
	}
}

func repositoryRootForBundleIdentityTest(t *testing.T) string {
	t.Helper()
	_, file, _, ok := stdruntime.Caller(0)
	if !ok {
		t.Fatal("locate bundle identity repository test")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func repositoryRelativePath(t *testing.T, root, path string) string {
	t.Helper()
	rel, err := filepath.Rel(root, path)
	if err != nil {
		t.Fatalf("relative path for %s: %v", path, err)
	}
	return filepath.ToSlash(rel)
}

func readRepositoryFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func normalizedSQLColumns(raw string) map[string]bool {
	columns := map[string]bool{}
	for _, column := range strings.Split(raw, ",") {
		columns[strings.ToLower(strings.TrimSpace(column))] = true
	}
	return columns
}

var bundleIdentityFixtureLedger = []string{
	"internal/apiv1/agent_diagnose_delivery_pagination_parity_test.go",
	"internal/apiv1/operator_entity_test.go",
	"internal/apiv1/operator_event_replay_test.go",
	"internal/apiv1/operator_human_task_ack_loss_test.go",
	"internal/apiv1/operator_mailbox_proposed_effect_supported_surface_test.go",
	"internal/apiv1/sqlite_agent_usage_supported_surface_test.go",
	"internal/apiv1/sqlite_observability_supported_surface_test.go",
	"internal/apiv1/sqlite_operator_read_supported_surface_test.go",
	"internal/cliapp/entities_test.go",
	"internal/dashboard/server/observability_sql_test.go",
	"internal/dashboard/server/server_test.go",
	"internal/digest/source_test.go",
	"internal/runtime/budget_recovery_parity_test.go",
	"internal/runtime/bus/event_identity_dispatch_surface_test.go",
	"internal/runtime/bus/eventbus_publish_test.go",
	"internal/runtime/cataloge2e/assertions_test.go",
	"internal/runtime/conformance/fan_in_barrier_runtime_conformance_test.go",
	"internal/runtime/conformance/persisted_surfaces_test.go",
	"internal/runtime/conformance/reply_resolution_conformance_test.go",
	"internal/runtime/inbound_postgres_test.go",
	"internal/runtime/manager/flow_activation_test.go",
	"internal/runtime/mutationlog/mutationlog_test.go",
	"internal/runtime/node_delivery_startup_recovery_test.go",
	"internal/runtime/pipeline/activity_boring_proof_test.go",
	"internal/runtime/pipeline/activity_engine_test.go",
	"internal/runtime/pipeline/create_entity_exact_once_test.go",
	"internal/runtime/pipeline/forked_source_claimants_test.go",
	"internal/runtime/pipeline/handler_engine_transaction_test.go",
	"internal/runtime/pipeline/human_task_expiry_transaction_test.go",
	"internal/runtime/pipeline/run_scoped_test_helpers_test.go",
	"internal/runtime/pipeline/select_entity_test.go",
	"internal/runtime/pipeline/workflow_gate_lifecycle_test.go",
	"internal/runtime/pipeline/workflow_gate_recovery_external_test.go",
	"internal/runtime/pipeline/workflow_instance_store_run_scope_test.go",
	"internal/runtime/pipeline/workflow_instance_store_sqlite_test.go",
	"internal/runtime/pipeline/workflow_join_lifecycle_test.go",
	"internal/runtime/runforkadmission/revision_source_route_test.go",
	"internal/runtime/runforkexecution/execution_test.go",
	"internal/runtime/template_instance_delivery_test.go",
	"internal/serveapp/main_runtime_test.go",
	"internal/serveapp/provider_trigger_smoke_helpers_test.go",
	"internal/serveapp/run_fork_runtime_test.go",
	"internal/store/agent_directive_operations_test.go",
	"internal/store/agent_directive_run_target_test.go",
	"internal/store/agent_lifecycle_effects_test.go",
	"internal/store/agent_lifecycle_read_surface_test.go",
	"internal/store/agent_lifecycle_subordinate_test.go",
	"internal/store/author_activity_receipt_parity_test.go",
	"internal/store/budget_spend_test.go",
	"internal/store/bundle_delete_test.go",
	"internal/store/completion_settlement_test.go",
	"internal/store/decision_cards_test.go",
	"internal/store/destructive_reset_cleanup_test.go",
	"internal/store/destructive_reset_directive_integration_test.go",
	"internal/store/diagnostic_direct_replay_test.go",
	"internal/store/directive_acknowledgment_test.go",
	"internal/store/entity_state_run_scope_test.go",
	"internal/store/event_admission_persistence_test.go",
	"internal/store/flow_instance_descriptors_sqlite_test.go",
	"internal/store/flow_instance_routes_test.go",
	"internal/store/operator_agent_conversation_read_surface_test.go",
	"internal/store/operator_conversation_projection_test.go",
	"internal/store/operator_entity_read_surface_test.go",
	"internal/store/operator_observability_read_surface_test.go",
	"internal/store/postgres_helpers_test.go",
	"internal/store/postgres_smoke_test.go",
	"internal/store/postgres_store_additional_test.go",
	"internal/store/preservation_cleanup_test.go",
	"internal/store/run_bundle_fingerprint_test.go",
	"internal/store/run_completion_test.go",
	"internal/store/run_control_test.go",
	"internal/store/run_debug_read_surface_test.go",
	"internal/store/run_fork_gate_activation_test.go",
	"internal/store/run_fork_materializer_test.go",
	"internal/store/run_fork_planner_test.go",
	"internal/store/run_fork_revision_conformance_test.go",
	"internal/store/run_fork_selected_contract_route_recovery_test.go",
	"internal/store/run_fork_source_freeze_test.go",
	"internal/store/runtime_effects_neutral_schema_test.go",
	"internal/store/runtime_log_persistence_test.go",
	"internal/store/runtime_mutation_test.go",
	"internal/store/schema_compatibility_bootstrap_test.go",
	"internal/store/selected_contract_runtime_execution_test.go",
	"internal/store/sqlite_run_api_read_surface_test.go",
	"internal/store/sqlite_run_completion_test.go",
	"internal/store/sqlite_run_trace_parity_test.go",
	"internal/store/sqlite_runtime_test.go",
	"internal/store/storetest/event.go",
	"internal/store/workflow_timer_schedule_isolation_test.go",
}
