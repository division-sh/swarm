package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelanchor"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channeldecision"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channeleventcount"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelintent"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/deliverystatus"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/eventname"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/forkcompanion"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/forknotice"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicendpoint"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicjoin"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicrollback"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runidentity"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runscalar"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedeventcount"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/sourcefork"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/standingdiagnostic"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/workflowstate"
)

const base = "c96e20aa7484b23b017f1787a3ee74eced517c54"
const ledger = "tools/fixture-codemod/pipeline-observations/recipes.json"
const transitionLedger = "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
const targetDigest = "bb95872035794c2d9425adbfc2f5530b08e17010156d554a99ccb8bc2427cefe"

var sourceForkComparison = map[string]string{}

type recipe struct {
	Family, File, Function, Before, After string
	Successor                             string    `json:"Successor,omitempty"`
	Removed                               bool      `json:"Removed,omitempty"`
	Mechanical                            *snapshot `json:"Mechanical,omitempty"`
}
type snapshot struct{ SourceCommit, BeforeSHA256, After string }
type transition struct {
	File, Function, Before, After string
	Removed                       bool `json:"Removed,omitempty"`
}

var files = []string{
	"internal/store/durable_data_selected_store_test.go",
	"internal/runtime/budget_recovery_parity_test.go",
	"internal/serveapp/budget_stage_recovery_test.go",
	"internal/apiv1/conversation_fork_atomic_creation_test.go",
	"internal/apiv1/conversation_fork_commit_omission_probe_test.go",
	"internal/apiv1/runtime_nuke_durable_replay_test.go",
	"internal/apiv1/operator_administrative_layering_test.go",
	"internal/apiv1/operator_agent_control_test.go",
	"internal/apiv1/operator_data_test.go",
	"internal/apiv1/operator_event_publish_test.go",
	"internal/apiv1/operator_event_replay_test.go",
	"internal/apiv1/operator_run_start_test.go",
	"internal/apiv1/operator_run_completion_system_node_test.go",
	"internal/apiv1/operator_runtime_context_test.go",
	"internal/apiv1/operator_stage_completion_journey_test.go",
	"internal/apiv1/operator_stage_completion_read_test.go",
	"internal/apiv1/operator_test_setup_test.go",
	"internal/apiv1/agent_snapshot_atomicity_test.go",
	"internal/apiv1/selected_store_read_supported_surface_test.go",
	"internal/apiv1/sqlite_operator_read_supported_surface_test.go",
	"internal/apiv1/sqlite_observability_supported_surface_test.go",
	"internal/cliapp/data_show_selected_test.go",
	"internal/cliapp/runtime_log_filter_supported_test.go",
	"internal/runtime/conformance/reply_resolution_conformance_test.go",
	"internal/runtime/conformance/a2_repeated_fan_out_conformance_test.go",
	"internal/runtime/conformance/delivery_lifecycle_conformance_test.go",
	"internal/runtime/conformance/empty_handler_construction_test.go",
	"internal/runtime/conformance/fan_in_barrier_runtime_conformance_test.go",
	"internal/runtime/conformance/fan_in_stream_conformance_test.go",
	"internal/runtime/conformance/fan_out_cardinality_proof_test.go",
	"internal/runtime/conformance/fan_out_source_failure_proof_test.go",
	"internal/runtime/conformance/fan_out_b17_mixed_dependency_test.go",
	"internal/runtime/conformance/fan_out_chunk_history_test.go",
	"internal/runtime/conformance/fan_out_drain_failure_diagnostic_test.go",
	"internal/runtime/conformance/fan_out_expired_evaluation_conformance_test.go",
	"internal/runtime/conformance/fan_out_nested_capacity_conformance_test.go",
	"internal/runtime/conformance/fan_out_nested_cancellation_conformance_test.go",
	"internal/runtime/conformance/fan_out_nested_interrupted_group_test.go",
	"internal/runtime/conformance/fan_out_nested_prepared_release_test.go",
	"internal/runtime/conformance/fan_out_nested_readback_helpers_test.go",
	"internal/runtime/conformance/fan_out_nested_serving_helpers_test.go",
	"internal/runtime/conformance/fan_out_resource_selected_fork_test.go",
	"internal/runtime/conformance/fan_out_resource_selected_fork_pending_test.go",
	"internal/runtime/conformance/fan_out_resource_selected_fork_external_test.go",
	"internal/runtime/conformance/fan_out_resource_journey_test.go",
	"internal/runtime/conformance/data_text_file_journey_2456_test.go",
	"internal/runtime/conformance/fan_out_resource_source_proof_test.go",
	"internal/runtime/conformance/fan_out_semantic_proof_helpers_test.go",
	"internal/runtime/conformance/fan_out_serving_generation_replacement_test.go",
	"internal/runtime/conformance/fan_out_serving_lifetime_conformance_test.go",
	"internal/runtime/conformance/fan_out_serving_diagnostics_conformance_test.go",
	"internal/runtime/conformance/fan_out_d3_supported_read_test.go",
	"internal/runtime/conformance/fan_out_serving_capacity_conformance_test.go",
	"internal/runtime/conformance/fan_out_serving_matrix_helpers_test.go",
	"internal/runtime/conformance/fan_out_shared_serving_conformance_test.go",
	"internal/runtime/conformance/fan_out_supplemental_helpers_test.go",
	"internal/runtime/conformance/fan_out_two_ten_suffix_test.go",
	"internal/runtime/conformance/fan_out_twenty_intent_contention_test.go",
	"internal/runtime/conformance/fan_out_pressure_reservation_test.go",
	"internal/runtime/conformance/fan_out_pressure_soak_test.go",
	"internal/runtime/conformance/lifecycle_diagnostic_overlap_test.go",
	"internal/runtime/conformance/persisted_surfaces_test.go",
	"internal/runtime/conformance/workflow_stage_lifecycle_identity_conformance_test.go",
	"internal/runtime/conformance/notify_all_children_runtime_conformance_test.go",
	"internal/runtime/deliverylifecycle/claim_commit_runtime_test.go",
	"internal/runtime/inbound_postgres_test.go",
	"internal/runtime/eventbus_receiver_authority_e2e_test.go",
	"internal/runtime/inbound_commit_ack_response_test.go",
	"internal/runtime/inbound_raw_settlement_store_test.go",
	"internal/runtime/manager/receipt_selected_store_test.go",
	"internal/runtime/manager/terminal_retirement_store_test.go",
	"internal/runtime/node_delivery_startup_recovery_test.go",
	"internal/runtime/workflow_timer_startup_recovery_test.go",
	"internal/runtime/template_instance_delivery_test.go",
	"internal/runtime/telegram_connector_supported_surface_test.go",
	"internal/runtime/slack_connector_managed_credential_supported_surface_test.go",
	"internal/runtime/microsoft_graph_connector_client_credentials_supported_surface_test.go",
	"internal/runtime/notion_connector_managed_credential_supported_surface_test.go",
	"internal/runtime/github_app_issue_comment_supported_surface_test.go",
	"internal/runtime/github_app_issue_workflow_supported_surface_test.go",
	"internal/runtime/channel_runtime_supported_surface_test.go",
	"internal/runtime/operator_channel_inbound_supported_surface_test.go",
	"internal/runtime/bus/eventbus_publish_test.go",
	"internal/runtime/pipeline/workflow_gate_recovery_external_test.go",
	"internal/runtime/pipeline/issue2564_publication_handoff_external_test.go",
	"internal/runtime/pipeline/workflow_timer_supported_surface_external_test.go",
	"internal/runtime/pipeline/workflow_join_supported_surface_external_test.go",
	"internal/runtime/pipeline/a2_count_join_execution_external_test.go",
	"internal/runtime/pipeline/a2_join_until_race_external_test.go",
	"internal/runtime/pipeline/a2_join_close_rollback_external_test.go",
	"internal/runtime/pipeline/a2_join_commit_ack_loss_external_test.go",
	"internal/runtime/pipeline/a2_activation_join_seam_external_test.go",
	"internal/runtime/pipeline/a2_join_result_types_external_test.go",
	"internal/runtime/pipeline/a2_join_hydration_external_test.go",
	"internal/runtime/pipeline/a2_join_cancellation_external_test.go",
	"internal/runtime/pipeline/a2_membership_snapshot_admission_external_test.go",
	"internal/runtime/pipeline/a2_join_deadline_execution_external_test.go",
	"internal/runtime/pipeline/a2_accumulator_canonical_evidence_external_test.go",
	"internal/runtime/pipeline/a2_accumulator_persistence_external_test.go",
	"internal/runtime/pipeline/a2_fieldless_reply_preservation_external_test.go",
	"internal/runtime/pipeline/a2_map_fan_out_execution_external_test.go",
	"internal/runtime/pipeline/a2_map_fan_out_source_corruption_external_test.go",
	"internal/runtime/pipeline/a2_publication_binding_external_test.go",
	"internal/runtime/pipeline/a2_multi_until_binding_external_test.go",
	"internal/runtime/pipeline/pause_eligibility_test.go",
	"internal/runtime/pipeline/prospective_publication_external_test.go",
	"internal/runtime/pipeline/receiver_future_appearance_external_test.go",
	"internal/runtime/pipeline/delivery_target_declared_key_supported_surface_external_test.go",
	"internal/runtime/pipeline/a2_bound_reply_binding_external_test.go",
	"internal/runtime/pipeline/a2_known_target_binding_external_test.go",
	"internal/runtime/pipeline/a2_same_commit_join_binding_external_test.go",
	"internal/runtime/pipeline/selection_retry_cas_external_test.go",
	"internal/runtime/template_flow_pilot_runtime_test.go",
	"internal/runtime/final_flow_instance_authoring_runtime_test.go",
	"internal/runtime/artifact_action_result_delivery_test.go",
	"internal/runtime/runforkadmission/revision_source_route_test.go",
	"internal/runtime/tools/executor_entity_test.go",
	"internal/serveapp/startup_creation_handoff_test.go",
	"internal/store/internal/runtimepersistence/contextual_runtime_lineage_parity_test.go",
	"internal/runtime/runforkexecution/activity_fork_execution_test.go",
	"internal/runtime/runforkexecution/agent_runtime_materialization_test.go",
	"internal/runtime/runforkexecution/operation_lifetime_parity_test.go",
	"internal/runtime/runforkexecution/module_fork_w5_test.go",
	"internal/runtime/runforkexecution/selected_input_execution_test.go",
	"internal/runtime/runforkexecution/recovery_process_test.go",
	"internal/runtime/runforkexecution/pack_platform_retained_proof_test.go",
	"internal/runtime/runforkexecution/source_cleanup_test.go",
	"internal/runtime/runforkexecution/preparation_process_parity_test.go",
	"internal/runtime/runforkexecution/verify_pack_generation_test.go",
	"internal/runtime/runforkexecution/control_lifetime_parity_test.go",
	"internal/runtime/runforkexecution/constructed_agent_input_execution_test.go",
	"internal/runtime/runforkexecution/workspace_preparation_test.go",
	"internal/runtime/runforkexecution/activation_gate_test.go",
	"internal/runtime/runforkexecution/constructed_source_fixture_test.go",
	"internal/runtime/runforkexecution/execution_test.go",
	"internal/runtime/runforkexecution/runtime_outcome_test.go",
	"internal/runtime/runforkexecution/selected_execution_authority_fixture_test.go",
	"internal/serveapp/main_runtime_test.go",
	"internal/serveapp/gate_completion_diagnostic_test.go",
	"internal/serveapp/lifecycle_diagnostic_recovery_test.go",
	"internal/serveapp/lifecycle_emitter_admission_test.go",
	"internal/serveapp/lifecycle_emitter_competing_exit_test.go",
	"internal/serveapp/lifecycle_emitter_test.go",
	"internal/serveapp/mailbox_completion_process_test.go",
	"internal/serveapp/mailbox_entity_filter_test.go",
	"internal/serveapp/paused_publication_test.go",
	"internal/serveapp/postgres_commit_loss_contract_test.go",
	"internal/serveapp/postgres_three_contracts_test.go",
	"internal/serveapp/receiver_composition_probe_test.go",
	"internal/serveapp/receiver_entityless_onward_test.go",
	"internal/serveapp/receiver_failure_settlement_test.go",
	"internal/serveapp/receiver_fork_test.go",
	"internal/serveapp/selection_process_recovery_test.go",
	"internal/serveapp/semantic_numeric_setup_test.go",
	"internal/serveapp/source_admission_fork_proof_test.go",
	"internal/serveapp/transition_receiver_probe_test.go",
	"internal/serveapp/mailbox_completion_fault_witness_test.go",
	"internal/serveapp/mailbox_card_completion_matrix_test.go",
	"internal/serveapp/mailbox_human_budget_completion_test.go",
	"internal/serveapp/mailbox_human_topology_test.go",
	"internal/serveapp/mailbox_mutation_admission_test.go",
	"internal/serveapp/mailbox_notice_completion_test.go",
	"internal/serveapp/mailbox_principal_transport_test.go",
	"internal/serveapp/mailbox_projection_fault_test.go",
	"internal/serveapp/mailbox_refusal_cut_test.go",
	"internal/serveapp/mailbox_source_admission_test.go",
	"internal/serveapp/materializing_sender_test.go",
	"internal/serveapp/publication_sites_test.go",
	"internal/serveapp/publication_activity_test.go",
	"internal/serveapp/publication_state_result_test.go",
	"internal/serveapp/receiver_initialization_ingress_test.go",
	"internal/serveapp/connection_edge_policy_test.go",
	"internal/serveapp/fan_out_one_second_preservation_test.go",
	"internal/serveapp/fan_out_original_reporter_http_test.go",
	"internal/serveapp/fan_out_served_incident_test.go",
	"internal/serveapp/fan_out_supported_surface_test.go",
	"internal/serveapp/mailbox_completion_test.go",
	"internal/serveapp/lifecycle_transition_restart_test.go",
	"internal/serveapp/lifecycle_transition_served_test.go",
	"internal/serveapp/lifecycle_transition_nested_test.go",
	"internal/serveapp/lifecycle_transition_static_fork_test.go",
	"internal/serveapp/lifecycle_transition_gate_restart_test.go",
	"internal/serveapp/lifecycle_transition_gate_test.go",
	"internal/serveapp/lifecycle_transition_node_recovery_test.go",
	"internal/serveapp/lifecycle_transition_replay_test.go",
	"internal/serveapp/lifecycle_transition_gate_process_test.go",
	"internal/serveapp/lifecycle_release_process_test.go",
	"internal/serveapp/receiver_restart_test.go",
	"internal/serveapp/mailbox_cursor_restart_test.go",
	"internal/serveapp/issue2277_audit_probe_test.go",
	"internal/serveapp/fork_receiver_ownership_test.go",
	"internal/serveapp/fork_receiver_execution_effect_test.go",
	"internal/serveapp/fork_receiver_acquisition_effect_only_test.go",
	"internal/serveapp/fork_receiver_post_revision_refusal_test.go",
	"internal/serveapp/typed_receiver_fork_refusal_test.go",
	"internal/serveapp/selected_fork_pending_input_mixed_test.go",
	"internal/serveapp/fork_retained_join_test.go",
	"internal/serveapp/constructed_decision_deferral_test.go",
	"internal/serveapp/selected_fork_pending_input_test.go",
	"internal/serveapp/selected_fork_notice_controls_test.go",
	"internal/serveapp/lifecycle_transition_fork_test.go",
	"internal/serveapp/lifecycle_transition_template_test.go",
	"internal/serveapp/fork_historical_context_test.go",
	"internal/serveapp/fork_recipient_authority_test.go",
	"internal/serveapp/receiver_initialization_test.go",
	"internal/serveapp/fork_loop_generation_notice_test.go",
	"internal/serveapp/fork_absent_source_test.go",
	"internal/serveapp/fork_loop_accumulator_test.go",
	"internal/serveapp/semantic_numeric_activity_test.go",
	"internal/serveapp/selected_fork_mailbox_controls_test.go",
	"internal/serveapp/selected_fork_pending_input_evidence_test.go",
	"internal/serveapp/mailbox_completion_fixture_test.go",
	"internal/serveapp/agent_mock_supported_surface_test.go",
	"internal/serveapp/agent_memory_supported_surface_test.go",
	"internal/serveapp/standing_ingress_supported_surface_test.go",
	"internal/serveapp/numeric_timer_inspection_test.go",
	"internal/serveapp/run_fork_runtime_test.go",
	"internal/serveapp/run_fork_constructor_fixture_test.go",
	"internal/serveapp/run_state_runtime_test.go",
	"internal/serveapp/reset_projection_real_docker_test.go",
	"internal/serveapp/startup_integrity_test.go",
	"internal/serveapp/receiver_mixed_agent_test.go",
	"internal/store/internal/runtimepersistence/flow_activation_attempt_test.go",
	"internal/store/internal/runtimepersistence/flow_activation_rebind_test.go",
	"internal/store/internal/runtimepersistence/flow_instance_descriptor_authority_test.go",
	"internal/store/internal/runtimepersistence/flow_route_topology_ack_consumer_external_test.go",
	"internal/store/internal/runtimepersistence/workflow_route_recovery_owner_test.go",
	"internal/store/storetest/event.go",
}

func main() {
	includeRunIdentityTargets()
	includeRunPhysicalTargets()
	includeStandingDiagnosticTargets()
	includeServedDebugTargets()
	includePublicA2EndpointTargets()
	includeReceiverPublicReadbackTargets()
	includePublicJoinTargets()
	includeForkCompanionTargets()
	includeForkNoticeTargets()
	includeChannelIntentTargets()
	includeRequestKeyTargets()
	includeRunScalarTargets()
	includeDeliveryStatusTargets()
	includeEventNameTargets()
	includeChannelAnchorTargets()
	includeWorkflowStateTargets()
	includeChannelDecisionTargets()
	includeChannelEventCountTargets()
	includeServedEventCountTargets()
	includeStopWitnessTargets()
	sourceForkPins := sourceForkHistoryPins()
	var rows []recipe
	must(json.Unmarshal(commit(ledger), &rows))
	var changes []transition
	for _, path := range files {
		before := functions(commit(path))
		data, err := os.ReadFile(path)
		must(err)
		after := functions(data)
		keys := make([]string, 0, len(before))
		for key := range before {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if targets, limited := gateRecoveryTargets[path]; limited && !targets[key] {
				continue
			}
			if path == "internal/runtime/bus/eventbus_publish_test.go" && key != "TestEventBusPublish_AgentOnlyConnectDoesNotAuthorizeUnrelatedNode" && key != "TestEventBusPublish_NestedDescendantCompletionFollowsDeclaredAncestorConnects" && key != "TestEventBusPublish_UndeclaredDescendantEmissionFailsClosedBeforeChildMutation" {
				continue
			}
			original := before[key]
			replacement, removed := currentReplacement(path, key, after)
			if original == replacement {
				continue
			}
			change := transition{File: path, Function: functionName(original), Before: original, After: replacement, Removed: removed}
			changes = append(changes, change)
			rows = extend(rows, change)
		}
	}
	rollbackPins := publicRollbackHistoryPins()
	rows = restoreGeneratedPublicRollbackRecipes(rows, rollbackPins)
	changes = restoreGeneratedPublicRollbackTransitions(changes, rollbackPins)
	rows = composePublicRollbackRecipes(preserveRunIdentityRecipes(rows), rollbackPins)
	changes = composePublicRollbackTransitions(preserveRunIdentityTransitions(changes), rollbackPins)
	rows, changes = composeSourceForkHistory(rows, changes, sourceForkPins)
	raw, err := json.Marshal(changes)
	must(err)
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	fmt.Printf("transitions=%d digest=%s\n", len(changes), digest)
	for _, change := range changes {
		fmt.Printf("%s\t%s\n", change.File, change.Function)
	}
	if len(os.Args) == 2 && os.Args[1] == "-write" {
		if digest != targetDigest {
			panic("family145 transition differs from its inspected exact source")
		}
		writeJSON(ledger, rows)
		writeJSON(transitionLedger, changes)
	} else if len(os.Args) != 1 {
		panic("usage: reconcile-family145 [-write]")
	}
}

func sourceForkHistoryPins() []sourcefork.Pin {
	raw, err := os.ReadFile(sourcefork.Path)
	must(err)
	pins, err := sourcefork.Decode(raw)
	must(err)
	for _, pin := range pins {
		current, err := os.ReadFile(pin.File)
		must(err)
		if functions(current)[pin.Function] != pin.After || functions(commitSourceFork(pin.File))[pin.Function] != pin.Before {
			panic("source fork exact live/predecessor endpoint changed: " + pin.Function)
		}
		sourceForkComparison[pin.File+"/"+pin.Function] = pin.Before
	}
	return pins
}

func commitSourceFork(path string) []byte {
	raw, err := exec.Command("git", "show", sourcefork.Predecessor+":"+path).Output()
	must(err)
	return raw
}

func composeSourceForkHistory(rows []recipe, changes []transition, pins []sourcefork.Pin) ([]recipe, []transition) {
	raw, err := json.Marshal(rows)
	must(err)
	raw, err = sourcefork.ComposeHistory(raw, pins, true)
	must(err)
	must(json.Unmarshal(raw, &rows))
	raw, err = json.Marshal(changes)
	must(err)
	raw, err = sourcefork.ComposeHistory(raw, pins, false)
	must(err)
	must(json.Unmarshal(raw, &changes))
	return rows, changes
}

func publicRollbackHistoryPins() []publicrollback.Pin {
	raw, err := os.ReadFile(publicrollback.Path)
	must(err)
	pins, err := publicrollback.Decode(raw)
	must(err)
	return pins
}

func restoreGeneratedPublicRollbackRecipes(rows []recipe, pins []publicrollback.Pin) []recipe {
	for i, row := range rows {
		for _, pin := range pins {
			if row.File == pin.File && row.Function == pin.Function {
				if row.After != pin.After || row.Successor != "" {
					panic("rollback generated recipe escaped exact endpoint")
				}
				rows[i].After = pin.Before
			}
		}
	}
	return rows
}

func restoreGeneratedPublicRollbackTransitions(rows []transition, pins []publicrollback.Pin) []transition {
	for i, row := range rows {
		for _, pin := range pins {
			if row.File == pin.File && row.Function == pin.Function {
				if row.After != pin.After || row.Removed {
					panic("rollback generated transition escaped exact endpoint")
				}
				rows[i].After = pin.Before
			}
		}
	}
	return rows
}

func composePublicRollbackRecipes(rows []recipe, pins []publicrollback.Pin) []recipe {
	for _, pin := range pins {
		matches := 0
		for i, row := range rows {
			if row.File != pin.File || row.Function != pin.Function {
				continue
			}
			matches++
			if row.After != pin.Before || row.Successor != "" || row.Removed {
				panic("rollback immutable recipe changed")
			}
			rows[i].Successor = pin.After
		}
		if matches != 1 {
			panic("rollback recipe lacks exact unique history")
		}
	}
	return rows
}

func composePublicRollbackTransitions(rows []transition, pins []publicrollback.Pin) []transition {
	for _, pin := range pins {
		matches := 0
		for i, row := range rows {
			if row.File != pin.File || row.Function != pin.Function {
				continue
			}
			matches++
			if row.After != pin.Before || row.Removed {
				panic("rollback immutable transition changed")
			}
			rows[i].After = pin.After
		}
		if matches != 1 {
			panic("rollback transition lacks exact unique history")
		}
	}
	return rows
}

func preserveRunIdentityRecipes(generated []recipe) []recipe {
	var previous []recipe
	raw, err := exec.Command("git", "show", runidentity.Predecessor+":"+ledger).Output()
	must(err)
	must(json.Unmarshal(raw, &previous))
	index := map[string]recipe{}
	for _, row := range generated {
		key := row.File + "/" + row.Function + "/" + row.Before
		if _, duplicate := index[key]; duplicate {
			panic("ambiguous complete run identity history key: " + row.File + "/" + row.Function)
		}
		index[key] = row
	}
	for i, prior := range previous {
		key := prior.File + "/" + prior.Function + "/" + prior.Before
		candidate, found := index[key]
		if !found {
			panic("run identity lost prior immutable recipe: " + prior.File + "/" + prior.Function)
		}
		previous[i] = composeRunIdentityRecipe(prior, candidate)
		delete(index, key)
	}
	if len(index) != 1 {
		panic("run identity introduced an unmanifested recipe")
	}
	for _, row := range index {
		if row.Function != "TestServedResetPendingStartupUsesAdmittedSourcesBothStores" || row.Family != "native-postgres-admission-family145" {
			panic("run identity new recipe is not its pending-startup consumer")
		}
		previous = append(previous, row)
	}
	return previous
}

func composeRunIdentityRecipe(prior, candidate recipe) recipe {
	if reflect.DeepEqual(prior, candidate) {
		return prior
	}
	for _, rule := range runidentity.Rules {
		if rule.File != prior.File || rule.Function != prior.Function || prior.Successor != "" {
			continue
		}
		after, err := runphysical.Transform(rule, prior.After)
		must(err)
		if candidate.After != after {
			panic("run identity Successor is not its finite propagation")
		}
		candidate.After = prior.After
		if !reflect.DeepEqual(prior, candidate) {
			panic("run identity changed immutable recipe metadata")
		}
		prior.Successor = after
		return prior
	}
	panic("unreviewed recipe change outside run identity: " + prior.File + "/" + prior.Function)
}

func preserveRunIdentityTransitions(generated []transition) []transition {
	var previous []transition
	raw, err := exec.Command("git", "show", runidentity.Predecessor+":"+transitionLedger).Output()
	must(err)
	must(json.Unmarshal(raw, &previous))
	index := map[string]transition{}
	for _, row := range generated {
		key := row.File + "/" + row.Function + "/" + row.Before
		if _, duplicate := index[key]; duplicate {
			panic("ambiguous complete run identity transition key")
		}
		index[key] = row
	}
	for i, prior := range previous {
		key := prior.File + "/" + prior.Function + "/" + prior.Before
		candidate, found := index[key]
		if !found {
			panic("run identity lost a prior source transition")
		}
		previous[i] = composeRunIdentityTransition(prior, candidate)
		delete(index, key)
	}
	if len(index) != 1 {
		panic("run identity introduced an unmanifested source transition")
	}
	for _, row := range index {
		if row.Function != "TestServedResetPendingStartupUsesAdmittedSourcesBothStores" {
			panic("run identity new transition is not pending-startup")
		}
		previous = append(previous, row)
	}
	return previous
}

func composeRunIdentityTransition(prior, candidate transition) transition {
	if reflect.DeepEqual(prior, candidate) {
		return prior
	}
	for _, rule := range runidentity.Rules {
		if rule.File != prior.File || rule.Function != prior.Function || prior.Removed {
			continue
		}
		after, err := runphysical.Transform(rule, prior.After)
		must(err)
		if candidate.After != after {
			panic("run identity transition is not its finite propagation")
		}
		candidate.After = prior.After
		if !reflect.DeepEqual(prior, candidate) {
			panic("run identity changed original transition metadata")
		}
		prior.After = after
		return prior
	}
	panic("unreviewed source transition outside run identity")
}

func includeRunIdentityTargets() {
	raw, err := os.ReadFile(runidentity.Path)
	must(err)
	pins, err := runidentity.Decode(raw)
	must(err)
	for _, pin := range pins {
		if !slices.Contains(files, pin.File) {
			files = append(files, pin.File)
			gateRecoveryTargets[pin.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[pin.File]; limited {
			targets[pin.Function] = true
		}
	}
}

func includeRunPhysicalTargets() {
	raw, err := os.ReadFile(runphysical.Path)
	must(err)
	pins, err := runphysical.Decode(raw)
	must(err)
	for _, pin := range pins {
		if !slices.Contains(files, pin.File) {
			files = append(files, pin.File)
			gateRecoveryTargets[pin.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[pin.File]; limited {
			targets[pin.Function] = true
		}
	}
}

func includeStandingDiagnosticTargets() {
	raw, err := os.ReadFile(standingdiagnostic.Path)
	must(err)
	manifest, err := standingdiagnostic.Decode(raw)
	must(err)
	for _, pin := range manifest.Pins {
		if pin.Before == pin.After {
			continue
		}
		if !slices.Contains(files, pin.File) {
			files = append(files, pin.File)
			gateRecoveryTargets[pin.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[pin.File]; limited {
			targets[pin.Function] = true
		}
	}
}

func includeRequestKeyTargets() {
	raw, err := os.ReadFile(requestkey.Path)
	must(err)
	pins, err := requestkey.Decode(raw)
	must(err)
	for _, pin := range pins {
		found := false
		for _, path := range files {
			found = found || path == pin.File
		}
		if !found {
			files = append(files, pin.File)
			gateRecoveryTargets[pin.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[pin.File]; limited {
			targets[pin.Function] = true
		}
	}
}

var channelIntentRetirements = map[string]map[string]bool{}

var runScalarRetirements = map[string]map[string]bool{}

func includeDeliveryStatusTargets() {
	raw, err := os.ReadFile(deliverystatus.Path)
	must(err)
	pins, err := deliverystatus.Decode(raw)
	must(err)
	for _, pin := range pins {
		if !slices.Contains(files, pin.File) {
			files = append(files, pin.File)
			gateRecoveryTargets[pin.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[pin.File]; limited {
			targets[pin.Function] = true
		}
	}
}

func includeEventNameTargets() {
	raw, err := os.ReadFile(eventname.Path)
	must(err)
	pins, err := eventname.Decode(raw)
	must(err)
	for _, pin := range pins {
		if !slices.Contains(files, pin.File) {
			files = append(files, pin.File)
			gateRecoveryTargets[pin.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[pin.File]; limited {
			targets[pin.Function] = true
		}
	}
}

func includeChannelAnchorTargets() {
	raw, err := os.ReadFile(channelanchor.Path)
	must(err)
	pins, err := channelanchor.Decode(raw)
	must(err)
	for _, pin := range pins {
		if !slices.Contains(files, pin.File) {
			files = append(files, pin.File)
			gateRecoveryTargets[pin.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[pin.File]; limited {
			targets[pin.Function] = true
		}
	}
}

func includeWorkflowStateTargets() {
	raw, err := os.ReadFile(workflowstate.Path)
	must(err)
	pins, err := workflowstate.Decode(raw)
	must(err)
	for _, pin := range pins {
		if !slices.Contains(files, pin.File) {
			files = append(files, pin.File)
			gateRecoveryTargets[pin.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[pin.File]; limited {
			targets[pin.Function] = true
		}
	}
}

func includeChannelDecisionTargets() {
	raw, err := os.ReadFile(channeldecision.Path)
	must(err)
	pins, err := channeldecision.Decode(raw)
	must(err)
	for _, pin := range pins {
		if !slices.Contains(files, pin.File) {
			files = append(files, pin.File)
			gateRecoveryTargets[pin.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[pin.File]; limited {
			targets[pin.Function] = true
		}
	}
}

func includeServedEventCountTargets() {
	raw, err := os.ReadFile(servedeventcount.Path)
	must(err)
	pins, err := servedeventcount.Decode(raw)
	must(err)
	for _, pin := range pins {
		if !slices.Contains(files, pin.File) {
			files = append(files, pin.File)
			gateRecoveryTargets[pin.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[pin.File]; limited {
			targets[pin.Function] = true
		}
	}
}

func includeStopWitnessTargets() {
	raw, err := os.ReadFile("tools/fixture-codemod/pipeline-observations/family145-stop-witness-amendment.json")
	must(err)
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != "87b00ec86800b2f78e9af3947b503d9c60bee78640bef7e20d52d01966441400" {
		panic("stop witness semantic amendment escaped its complete source")
	}
	var pins []struct{ File, Function string }
	must(json.Unmarshal(raw, &pins))
	for _, pin := range pins {
		if !slices.Contains(files, pin.File) {
			files = append(files, pin.File)
			gateRecoveryTargets[pin.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[pin.File]; limited {
			targets[pin.Function] = true
		}
	}
}

func includeChannelEventCountTargets() {
	raw, err := os.ReadFile(channeleventcount.Path)
	must(err)
	pins, err := channeleventcount.Decode(raw)
	must(err)
	for _, pin := range pins {
		if !slices.Contains(files, pin.File) {
			files = append(files, pin.File)
			gateRecoveryTargets[pin.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[pin.File]; limited {
			targets[pin.Function] = true
		}
	}
}

func includeRunScalarTargets() {
	raw, err := os.ReadFile(runscalar.Path)
	must(err)
	pins, err := runscalar.Decode(raw)
	must(err)
	for _, pin := range pins {
		if !slices.Contains(files, pin.File) {
			files = append(files, pin.File)
			gateRecoveryTargets[pin.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[pin.File]; limited {
			targets[pin.Function] = true
		}
		if pin.After == "" {
			if runScalarRetirements[pin.File] == nil {
				runScalarRetirements[pin.File] = map[string]bool{}
			}
			runScalarRetirements[pin.File][pin.Function] = true
		}
	}
}

func includeChannelIntentTargets() {
	raw, err := os.ReadFile(channelintent.Path)
	must(err)
	pins, err := channelintent.Decode(raw)
	must(err)
	for _, pin := range pins {
		if pin.Before == "" || strings.HasPrefix(pin.Function, "type/") {
			continue
		}
		found := false
		for _, path := range files {
			found = found || path == pin.File
		}
		if !found {
			files = append(files, pin.File)
			gateRecoveryTargets[pin.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[pin.File]; limited {
			targets[pin.Function] = true
		}
		if pin.After == "" {
			if channelIntentRetirements[pin.File] == nil {
				channelIntentRetirements[pin.File] = map[string]bool{}
			}
			channelIntentRetirements[pin.File][pin.Function] = true
		}
	}
}

func includePublicA2EndpointTargets() {
	for _, pin := range publicendpoint.Pins() {
		found := false
		for _, path := range files {
			found = found || path == pin.File
		}
		if !found {
			files = append(files, pin.File)
			gateRecoveryTargets[pin.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[pin.File]; limited {
			targets[pin.Function] = true
		}
	}
}

func includeForkNoticeTargets() {
	raw, err := os.ReadFile(forknotice.Path)
	must(err)
	rows, err := forknotice.Decode(raw)
	must(err)
	for _, row := range rows {
		found := false
		for _, path := range files {
			found = found || path == row.File
		}
		if !found {
			files = append(files, row.File)
			gateRecoveryTargets[row.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[row.File]; limited {
			targets[row.Function] = true
		}
	}
}

func includeForkCompanionTargets() {
	raw, err := os.ReadFile(forkcompanion.Path)
	must(err)
	rows, err := forkcompanion.Decode(raw)
	must(err)
	for _, row := range rows {
		found := false
		for _, path := range files {
			found = found || path == row.File
		}
		if !found {
			files = append(files, row.File)
			gateRecoveryTargets[row.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[row.File]; limited {
			targets[row.Function] = true
		}
	}
}

func includePublicJoinTargets() {
	raw, err := os.ReadFile(publicjoin.Path)
	must(err)
	rows, err := publicjoin.Decode(raw)
	must(err)
	for _, row := range rows {
		found := false
		for _, path := range files {
			found = found || path == row.File
		}
		if !found {
			files = append(files, row.File)
			gateRecoveryTargets[row.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[row.File]; limited {
			targets[row.Function] = true
		}
	}
}

func includeReceiverPublicReadbackTargets() {
	raw, err := os.ReadFile("tools/fixture-codemod/pipeline-observations/family145-receiver-public-readback-preimages.json")
	must(err)
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != "a75db13f04646436e22522c83c9a04d929d12e384d9b48bfa585fe5bdf499709" {
		panic("receiver public readback complete source inventory changed")
	}
	var rows []struct{ File, Function string }
	must(json.Unmarshal(raw, &rows))
	if len(rows) != 15 {
		panic("receiver public readback declaration inventory changed")
	}
	for _, row := range rows {
		found := false
		for _, path := range files {
			found = found || path == row.File
		}
		if !found {
			files = append(files, row.File)
			gateRecoveryTargets[row.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[row.File]; limited {
			targets[row.Function] = true
		}
	}
}

var renamedHelpers = map[string]map[string]string{
	"internal/store/storetest/event.go": {
		"InsertCanonicalEventRecord": "InsertNativeCanonicalEventRecord",
	},
	"internal/runtime/template_instance_delivery_test.go": {
		"waitRuntimeDBCount":   "waitRuntimeStorageCount",
		"waitRuntimeEventID":   "waitRuntimeStoredEventID",
		"assertRuntimeDBCount": "assertRuntimeStorageCount",
	},
	"internal/runtime/artifact_action_result_delivery_test.go": {
		"waitArtifactActionResultDBCount": "waitArtifactActionResultSettledCount",
	},
}

var retiredEventFixtureHelpers = map[string]map[string]bool{
	"internal/serveapp/main_runtime_test.go": {
		"servedEventPublishEntityState":  true,
		"servedEventPublishDebugSummary": true,
		"servedEventPublishDebugQuery":   true,
	},
	"internal/serveapp/fork_receiver_ownership_test.go": {
		"readForkReceiverRows": true,
	},
	"internal/store/storetest/event.go": {
		"InsertExistingRunRootEventRecord": true,
		"InsertChildEventRecord":           true,
		"runCanonicalEventMutation":        true,
	},
	"internal/store/internal/runtimepersistence/contextual_runtime_lineage_parity_test.go": {
		"selectedDatabase": true,
	},
}

func includeServedDebugTargets() {
	raw, err := os.ReadFile("tools/fixture-codemod/pipeline-observations/family145-served-debug-preimages.json")
	must(err)
	if fmt.Sprintf("%x", sha256.Sum256([]byte(strings.TrimSuffix(string(raw), "\n")))) != "31f62531eda187d22fa6d08b1379ab1df88a4d4f75aa6c997b00fdb06d89700e" {
		panic("served diagnostic dependency differs from exact inspected source")
	}
	var rows []struct{ File, Function string }
	must(json.Unmarshal(raw, &rows))
	known := map[string]bool{}
	for _, file := range files {
		known[file] = true
	}
	for _, row := range rows {
		if strings.HasPrefix(row.Function, "type/") {
			continue
		}
		if !known[row.File] {
			files = append(files, row.File)
			known[row.File] = true
			gateRecoveryTargets[row.File] = map[string]bool{}
		}
		if targets, limited := gateRecoveryTargets[row.File]; limited {
			targets[row.Function] = true
		}
	}
}

var gateRecoveryTargets = map[string]map[string]bool{
	"internal/serveapp/constructed_decision_deferral_test.go":         {"TestServedFieldlessGateDeferralBothStores": true},
	"internal/serveapp/lifecycle_transition_served_test.go":           {"lifecycleStoredSnapshot": true, "TestServedCompiledTransitionSelectedCarrierEvidenceOnBothStores": true},
	"internal/serveapp/lifecycle_transition_nested_test.go":           {"TestServedCompiledTransitionNestedCarrierCollisionOnBothStores": true},
	"internal/serveapp/lifecycle_transition_static_fork_test.go":      {"TestServedCompiledTransitionStaticForkEvidenceOnBothStores": true},
	"internal/serveapp/lifecycle_transition_gate_restart_test.go":     {"proveServedCompiledGateOutcomeRestart": true},
	"internal/serveapp/lifecycle_transition_gate_test.go":             {"TestServedCompiledGateAdvanceOnlyOnBothStores": true},
	"internal/serveapp/lifecycle_transition_node_recovery_test.go":    {"TestServedCompiledLoopNodeRecoveryReexecutionOnBothStores": true},
	"internal/serveapp/lifecycle_transition_replay_test.go":           {"TestServedCompiledLoopTransitionReplayOnBothStores": true},
	"internal/serveapp/lifecycle_transition_gate_process_test.go":     {"TestServedCompiledGateOutcomeRestartOnBothStores": true},
	"internal/serveapp/lifecycle_release_process_test.go":             {"TestReleaseCompiledLifecycleJourneysBothStores": true},
	"internal/store/durable_data_selected_store_test.go":              {"forEachDurableDataStore": true},
	"internal/runtime/budget_recovery_parity_test.go":                 {"TestCompletionBudgetRecoveryProjectionParity": true},
	"internal/serveapp/budget_stage_recovery_test.go":                 {"TestBudgetRecoveryLoadsExactRetainedStagesAndStatelessPostureBothStores": true},
	"internal/serveapp/standing_ingress_supported_surface_test.go":    {"TestStandingRestartMixedHealthyAndTerminalProcessParity": true},
	"internal/serveapp/agent_mock_supported_surface_test.go":          {"runMockAgentSupportedSurface": true},
	"internal/serveapp/agent_memory_supported_surface_test.go":        {"runStandingTelegramMemorySupportedSurface": true},
	"internal/serveapp/mailbox_completion_fixture_test.go":            {"newRetainedMailboxCompletionRuntimeConfigured": true, "mailboxCompletionNotice": true, "mailboxCompletionFixtureInRuntime": true},
	"internal/serveapp/fork_receiver_ownership_test.go":               {"readForkReceiverProducerEvidence": true, "readForkReceiverRows": true, "requireForkReceiverDelivery": true, "TestSelectedForkIndependentReceiverBusinessMutationBothStores": true, "snapshotForkReceiverApplication": true, "runForkReceiverOwnershipJourney": true, "TestSelectedForkReceiverAcquisitionCapabilityRefusalBothStores": true, "TestServedForkSourceCompletionWithoutForkBothStores": true, "waitForkReceiverSourceCompletion": true},
	"internal/serveapp/fork_receiver_execution_effect_test.go":        {"requireForkReceiverExecution": true},
	"internal/serveapp/fork_receiver_acquisition_effect_only_test.go": {"requireSupplementalForkAcquisitionSettlement": true, "TestSelectedForkSupplementalReceiverAcquisitionWithoutPostRevisionEmissionBothStores": true},
	"internal/serveapp/fork_receiver_post_revision_refusal_test.go":   {"TestSelectedForkReceiverPostRevisionPolicyRefusalBothStores": true, "requireForkReceiverPostRevisionPolicyRefusal": true},
	"internal/serveapp/selected_fork_pending_input_test.go":           {"TestSelectedForkPendingInputBothStores": true, "TestExplicitTargetRestrictsConnectedSiblingsBothStores": true},
	"internal/serveapp/selected_fork_notice_controls_test.go":         {"TestSupportedHumanNoticeAcknowledgmentAfterRetirementBothStores": true},
	"internal/serveapp/lifecycle_transition_fork_test.go":             {"requireLifecycleRootLoopForkRevision": true, "requireLifecycleFrozenGateControlRefusal": true, "runLifecycleRootForkPolicy": true},
	"internal/serveapp/lifecycle_transition_template_test.go":         {"TestServedCompiledTransitionTemplateSiblingForkCapabilityRefusalOnBothStores": true, "TestServedCompiledTransitionNestedTemplatesFirstJourneyOnBothStores": true},
	"internal/serveapp/fork_historical_context_test.go":               {"TestRunForkHistoricalIdentityPublicExecutionBothStores": true},
	"internal/serveapp/fork_recipient_authority_test.go":              {"readServedForkRecipientSourceDomain": true, "requireServedForkConsumerAuthority": true, "TestServedForkConnectedRecipientAuthorityOnBothStores": true},
	"internal/serveapp/receiver_initialization_test.go":               {"requireTypedReceiverInitializationCases": true},
	"internal/serveapp/fork_loop_generation_notice_test.go":           {"TestServedForkLoopGenerationStateEffectBothStores": true},
	"internal/serveapp/fork_absent_source_test.go":                    {"TestServedForkRootSourceIngressBothStores": true},
	"internal/serveapp/fork_loop_accumulator_test.go":                 {"TestServedForkAccumulatorRetainedGenerationBothStores": true},
	"internal/serveapp/semantic_numeric_activity_test.go":             {"proveActivitySemanticResultExecutionParity": true},
	"internal/serveapp/selected_fork_mailbox_controls_test.go":        {"TestSelectedForkMailboxControlRefusalsBothStores": true},
	"internal/serveapp/selected_fork_pending_input_evidence_test.go":  {"TestSelectedForkPendingInputEvidenceBothStores": true},
	"internal/serveapp/typed_receiver_fork_refusal_test.go":           {"forkReceiverChangedTables": true},
	"internal/serveapp/selected_fork_pending_input_mixed_test.go":     {"TestSelectedForkPendingInputMixedCompletionBothStores": true},
	"internal/serveapp/fork_retained_join_test.go":                    {"testServedJoinWriterCompletedHistoryRefusal": true, "readRetainedRootJoins": true},
	"internal/apiv1/agent_snapshot_atomicity_test.go": {
		"seedTurnWithOutputContract/agentSnapshotBoundaryFixture": true,
	},
	"internal/apiv1/selected_store_read_supported_surface_test.go": {
		"TestPostgresAgentConversationOwnerBacksSupportedAPISurface": true,
	},
	"internal/apiv1/sqlite_operator_read_supported_surface_test.go": {
		"TestSQLiteAgentConversationOwnerBacksSupportedAPISurface": true,
	},
	"internal/runtime/tools/executor_entity_test.go": {
		"TestEntityTools_GetEntityReturnsForkLocalMaterializedRevision":   true,
		"TestEntityTools_SaveEntityFieldAfterForkActivationUsesForkRunID": true,
	},
	"internal/serveapp/startup_creation_handoff_test.go": {
		"TestComposedStartupCreationPublicationHandoffOnBothStores": true,
	},
	"internal/store/internal/runtimepersistence/contextual_runtime_lineage_parity_test.go": {
		"TestContextualRuntimeProducerLineageReadbackParity": true,
		"selectedDatabase": true,
	},
	"internal/runtime/conformance/fan_out_serving_lifetime_conformance_test.go": {
		"TestFanOutServingM06GenerationRetirementOnBothBackends":           true,
		"TestFanOutServingM07ActualProcessLossJoinsHeldTurnOnBothBackends": true,
		"readServingLifetimeState":                                         true,
		"assertServingLifetimeClaimOwner":                                  true,
		"waitServingMatrixTriggerReceipt":                                  true,
	},
	"internal/runtime/conformance/fan_out_serving_diagnostics_conformance_test.go": {
		"TestIssue2394D3IncidentBothStores":                true,
		"TestIssue2394D3EvidenceControlsBothStores":        true,
		"TestIssue2394D3FailedReadEpisodeBothStores":       true,
		"TestIssue2394D3CorruptHeaderNoPositiveBothStores": true,
	},
	"internal/runtime/conformance/fan_out_d3_supported_read_test.go":                            {"assertServingD3SupportedReadback": true},
	"internal/runtime/pipeline/issue2564_publication_handoff_external_test.go":                  {"selected/*issue2564PublicationFixture": true},
	"internal/runtime/pipeline/a2_join_hydration_external_test.go":                              {"TestA2PersistedJoinHydrationRefusesCorruptionOnBothStores": true},
	"internal/runtime/pipeline/a2_join_cancellation_external_test.go":                           {"TestA2JoinCancellationSettlesAlreadyBoundWorkOnBothStores": true, "a2CancellationFootprint": true},
	"internal/runtime/pipeline/workflow_join_supported_surface_external_test.go":                {"assertExactJoinDeliveryStatus": true, "assertExactJoinDeliveryCount": true, "exactJoinOccurrenceEventID": true},
	"internal/runtime/pipeline/workflow_timer_supported_surface_external_test.go":               {"workflowTimerPersistedActivation": true, "workflowTimerPersistedOccurrence": true, "assertWorkflowTimerServedRows": true, "waitWorkflowTimerRowStatus": true, "workflowTimerPersistedEventID": true, "workflowTimerEventCount": true, "workflowTimerDeliveryTrace": true, "TestRecurringWorkflowTimerFiresRestoresAndCancelsOnBothStores": true},
	"internal/serveapp/receiver_mixed_agent_test.go":                                            {"TestReceiverCompositionMixedAgentBothStores": true},
	"internal/runtime/pipeline/prospective_publication_external_test.go":                        {"TestProspectivePublicationRealPlannerTerminalAndOrdinaryWriterFenceBothStores": true, "CommitWorkflowEngineMutation/*observedProspectivePersistence": true},
	"internal/runtime/pipeline/receiver_future_appearance_external_test.go":                     {"TestReceiverCompositionActivationReuseAndConflictBothStores": true},
	"internal/runtime/pipeline/delivery_target_declared_key_supported_surface_external_test.go": {"TestTargetedDeclaredKeyAgreementAndConflictExecuteThroughDurableEventBusOnBothStores": true},
	"internal/runtime/pipeline/a2_bound_reply_binding_external_test.go":                         {"testA2BoundReplyJourney": true, "a2CorruptBoundReplyEntry": true, "a2BoundReplySourceSnapshot": true},
	"internal/runtime/pipeline/a2_known_target_binding_external_test.go":                        {"TestA2KnownTargetWorkIssuedBeforeArmPublishesOutputBoundToActualArmOnBothStores": true, "a2KnownTargetEarlyRefusal": true},
	"internal/runtime/pipeline/a2_same_commit_join_binding_external_test.go":                    {"TestA2SameBusinessCommitTransitionArmAndPublicationOnBothStores": true},
	"internal/runtime/pipeline/a2_count_join_execution_external_test.go":                        {"TestA2CountJoinRealExecutionAndRestartOnBothStores": true},
	"internal/runtime/pipeline/a2_join_until_race_external_test.go":                             {"TestA2UntilAndFinalArrivalHaveOneDurableCloseWinnerOnBothStores": true},
	"internal/runtime/pipeline/a2_join_close_rollback_external_test.go":                         {"TestA2JoinCloseSQLFailureRollsBackAndRecoversOnBothStores": true},
	"internal/runtime/pipeline/a2_join_commit_ack_loss_external_test.go":                        {"TestA2JoinCloseLostOwnerResultRecoversOnBothStores": true},
	"internal/runtime/pipeline/a2_activation_join_seam_external_test.go":                        {"TestA2ActivationCarriesInitialJoinAtomicallyOnBothStores": true, "a2ActivationJoinScheduleFault": true},
	"internal/runtime/pipeline/a2_join_result_types_external_test.go":                           {"TestA2JoinResultTypesRealExecutionAndRestartOnBothStores": true},
	"internal/runtime/pipeline/a2_membership_snapshot_admission_external_test.go":               {"failure/*a2MembershipAdmission": true, "footprint/*a2MembershipAdmission": true, "TestA2MembershipSnapshotValidationOnBothStores": true},
	"internal/runtime/pipeline/a2_join_deadline_execution_external_test.go":                     {"TestA2JoinDeadlineExecutionRetainsEntryAndPartialContextOnBothStores": true},
	"internal/runtime/pipeline/a2_accumulator_canonical_evidence_external_test.go":              {"TestA2AccumulatorSchemaAdmittedCanonicalEvidenceOnBothStores": true, "a2CanonicalAccumulatorEvidence": true, "TestA2AccumulatorPersistedDuplicatedKeyEvidenceRefusesOnBothStores": true, "requireA2CanonicalAccumulatorClaim": true},
	"internal/runtime/pipeline/a2_accumulator_persistence_external_test.go":                     {"evidence/*a2AccumulatorPersistenceProof": true, "TestA2AccumulatorKeyedExecutionPersistenceOnBothStores": true, "TestA2AccumulatorUnkeyedDurableMultiplicityAndOrderOnBothStores": true, "TestA2AccumulatorPublicationFailureRollsBackOnBothStores": true},
	"internal/runtime/pipeline/a2_fieldless_reply_preservation_external_test.go":                {"TestA2FieldlessPairedReplyPreservesConstructedExecutionOnBothStores": true},
	"internal/runtime/pipeline/a2_map_fan_out_execution_external_test.go":                       {"assertProgress/*a2MapFanOutExecution": true, "runA2MapFanOutCompiledImmutableSource": true, "TestA2MapFanOutRetainedProjectionAndRangeRefuseOnBothStores": true},
	"internal/runtime/pipeline/a2_map_fan_out_source_corruption_external_test.go":               {"readRaw/*a2CorruptMapSource": true, "writeRaw/*a2CorruptMapSource": true, "footprint/*a2CorruptMapSource": true, "TestA2MapFanOutRawBusinessSourceReaderRefusesOnBothStores": true, "TestA2MapFanOutRawBusinessSourcePumpRefusesWithoutProgressOnBothStores": true},
	"internal/runtime/pipeline/a2_publication_binding_external_test.go":                         {"testA2StageEntryPublicationBinding": true},
	"internal/runtime/pipeline/a2_multi_until_binding_external_test.go":                         {"a2MultiUntilChildren": true, "TestA2MultiUntilIndependentRecipientEntriesAndRestartOnBothStores": true},
	"internal/runtime/pipeline/pause_eligibility_test.go":                                       {"TestPausedHandedNodeParksUntilContinueBothStores": true},
	"internal/runtime/conformance/persisted_surfaces_test.go": {
		"TestCanonicalMutationSurface_FailsOnMalformedCanonicalMutationField":                        true,
		"TestStartupPipelineReplayAftermathSurface_RoundTripsThroughObservabilityReader":             true,
		"TestReusedLiveSessionKeepsDeliveryFrontierBoundToCanonicalSession":                          true,
		"TestCLISessionFailureDoesNotRotateFromStderrProse":                                          true,
		"TestStartupRecoveryFailurePlatformEventSurface_PreservesRecoveryFailedWithoutPlatformReset": true,
		"TestResetOrphanedSessionAftermathSurface_RoundTripsThroughObservabilityReader":              true,
	},
	"internal/runtime/pipeline/workflow_gate_recovery_external_test.go": {
		"openSQLiteGateRecoveryStore":                                                          true,
		"openPostgresGateRecoveryStore":                                                        true,
		"TestApprovedActivityHoldsThenDispatchesExactFrozenInputOnBothStores":                  true,
		"TestApprovedActivityProposalCreationRollsBackWorkflowCardAndContinuationOnBothStores": true,
		"TestDecisionRouteObligationFairnessAdmitsNewWorkBehindFullDeferredPageOnBothStores":   true,
		"installProposedEffectCreateFailure":                                                   true,
		"setGateRecoveryRouteAttempt":                                                          true,
		"testWorkflowGateStartupTerminalRecovery":                                              true,
		"testWorkflowGateUnavailablePinRecovery":                                               true,
		"makeGateRecoveryRouteDue":                                                             true,
		"assertProposedEffectOutcomeCount":                                                     true,
		"proposedEffectProofFailure":                                                           true,
		"gateRecoveryOutcomeEventID":                                                           true,
		"gateRecoveryOutcomeEventCount":                                                        true,
		"gateRecoveryDeliveryCount":                                                            true,
	},
	"internal/runtime/pipeline/selection_retry_cas_external_test.go": {
		"TestSelectionRetryAfterRealCASConflictBothStores":      true,
		"CommitWorkflowEngineMutation/*selectionCASPersistence": true,
	},
}

func retiredFixtureReplacement(path, key string, after map[string]string) bool {
	if channelIntentRetirements[path][key] || runScalarRetirements[path][key] {
		if after[key] != "" {
			panic("retired channel-intent interpreter survived: " + path + "/" + key)
		}
		return true
	}
	if retiredEventFixtureHelpers[path][key] {
		if after[key] != "" {
			panic("retired raw event fixture helper survived: " + path + "/" + key)
		}
		return true
	}
	return false
}

func currentReplacement(path, key string, after map[string]string) (string, bool) {
	if retiredFixtureReplacement(path, key, after) {
		return "", true
	}
	if replacement := after[key]; replacement != "" {
		if previous := sourceForkComparison[path+"/"+key]; previous != "" {
			return previous, false
		}
		return replacement, false
	}
	if replacement := after[renamedHelpers[path][key]]; replacement != "" {
		return replacement, false
	}
	if path == "internal/runtime/runforkadmission/revision_source_route_test.go" && key == "captureRunForkRevision" {
		return "", true
	}
	if path == "internal/runtime/telegram_connector_supported_surface_test.go" && key == "seedTelegramConnectorSupportedSurfaceWorkflowVersion" {
		return "", true
	}
	if path == "internal/runtime/channel_runtime_supported_surface_test.go" && key == "startConfiguredChannelActivityNode" && after["assertConfiguredChannelClaimedDispatch"] != "" {
		return after["assertConfiguredChannelClaimedDispatch"], false
	}
	if path == "internal/apiv1/operator_event_publish_test.go" {
		if key == "sqliteEventDeliveryTargetRoutes" {
			return "", true
		}
		if key == "postgresEventDeliveryTargetRoutes" && after["publicationDeliveryTargetRoutes"] != "" {
			return after["publicationDeliveryTargetRoutes"], false
		}
	}
	if path == "internal/apiv1/operator_event_replay_test.go" && (key == "updateCompleteReplayChainDepth" || key == "updateCompleteReplayTargetRoute") {
		return "", true
	}
	if path == "internal/store/storetest/event.go" && key == "InsertDiagnosticDirectEventRecordForRun" {
		return "", true
	}
	if path == "internal/serveapp/main_runtime_test.go" && key == "captureRunForkCLIRevision" {
		return "", true
	}
	if path == "internal/runtime/runforkexecution/activity_fork_execution_test.go" {
		switch key {
		case "QueryRowContext/activityForkFixtureDB", "ExecContext/activityForkFixtureDB", "query/activityForkFixtureDB", "seedSelectedContractActivityRequest", "selectedContractActivityJSON":
			return "", true
		}
	}
	if path == "internal/serveapp/receiver_public_readback_test.go" && key == "requireWorkspaceProofReceiverPublicReadback" {
		return "", true
	}
	panic("unreviewed retirement: " + path + "/" + key)
}

func extend(rows []recipe, change transition) []recipe {
	match := -1
	for i, row := range rows {
		if row.File != change.File || functionKey(row.After) != functionKey(change.Before) {
			continue
		}
		if match != -1 {
			panic("ambiguous current recipe: " + change.File + "/" + change.Function)
		}
		match = i
	}
	if match < 0 {
		if change.Removed {
			return append(rows, recipe{Family: "native-postgres-admission-family145", File: change.File, Function: change.Function, Before: change.Before, After: change.Before, Removed: true})
		}
		return append(rows, recipe{Family: "native-postgres-admission-family145", File: change.File, Function: change.Function, Before: change.Before, After: change.After})
	}
	row := &rows[match]
	current := row.After
	if row.Successor != "" {
		current = row.Successor
	}
	if node(parseFunction(current)) != change.Before || row.Removed {
		panic("noncanonical predecessor: " + change.File + "/" + change.Function)
	}
	if change.Removed {
		row.Successor, row.Removed = "", true
	} else {
		row.Successor = change.After
	}
	return rows
}

func functions(source []byte) map[string]string {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", source, parser.AllErrors)
	must(err)
	result := make(map[string]string)
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		key := key(fn)
		if _, duplicate := result[key]; duplicate {
			panic("ambiguous function: " + key)
		}
		result[key] = node(fn)
	}
	return result
}

func functionKey(source string) string {
	fn := parseFunction(source)
	return key(fn)
}

func functionName(source string) string { return parseFunction(source).Name.Name }

func parseFunction(source string) *ast.FuncDecl {
	file, err := parser.ParseFile(token.NewFileSet(), "function.go", "package proof\n"+source, parser.AllErrors)
	must(err)
	if len(file.Decls) != 1 {
		panic("noncanonical function snapshot")
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok {
		panic("snapshot is not a function")
	}
	return fn
}

func key(fn *ast.FuncDecl) string {
	if fn.Recv == nil {
		return fn.Name.Name
	}
	return fn.Name.Name + "/" + node(fn.Recv.List[0].Type)
}

func node(n ast.Node) string {
	var out bytes.Buffer
	must(format.Node(&out, token.NewFileSet(), n))
	return strings.TrimSpace(out.String())
}

func commit(path string) []byte {
	data, err := exec.Command("git", "show", base+":"+path).Output()
	must(err)
	return data
}

func writeJSON(path string, value any) {
	data, err := json.MarshalIndent(value, "", "  ")
	must(err)
	must(os.WriteFile(path, append(data, '\n'), 0644))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
