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
	"sort"
	"strings"
)

const approved = "2b699b9af7c05fb8f319ae190f9c2f802bd7bd10"
const ledger = "tools/fixture-codemod/pipeline-observations/recipes.json"
const targetDigest = "b12bb6d4cffc0c685be0a6f53f12a0077d4880ad541c4693818d2c03d466d168"

type recipe struct {
	Family, File, Function, Before, After string
	Successor                             string    `json:"Successor,omitempty"`
	Removed                               bool      `json:"Removed,omitempty"`
	Mechanical                            *snapshot `json:"Mechanical,omitempty"`
}
type snapshot struct{ SourceCommit, BeforeSHA256, After string }
type transition struct {
	File, Function, Before, After string
	Removed                       bool
}
type function struct{ Name, Receiver, Body string }

var files = []string{
	"internal/apiv1/operator_agent_control_test.go",
	"internal/apiv1/operator_event_replay_test.go",
	"internal/apiv1/runtime_nuke_durable_replay_test.go",
	"internal/runtime/author_activity_test_context_test.go",
	"internal/runtime/budget_recovery_parity_test.go",
	"internal/runtime/bus/eventbus_agent_route_test.go",
	"internal/runtime/bus/pipeline_terminal_release_test.go",
	"internal/runtime/bus/publication_finalizer_admission_test.go",
	"internal/runtime/bus/source_artifact_mutation_roots_test.go",
	"internal/runtime/conformance/fan_in_barrier_runtime_conformance_test.go",
	"internal/runtime/conformance/fan_in_stream_conformance_test.go",
	"internal/runtime/manager/delivery_native_owner_external_test.go",
	"internal/runtime/manager/delivery_native_selected_external_test.go",
	"internal/runtime/pipeline/a2_map_fan_out_execution_external_test.go",
	"internal/runtime/pipeline/workflow_gate_recovery_external_test.go",
	"internal/runtime/pipeline/workflow_timer_supported_surface_external_test.go",
	"internal/runtime/runforkexecution/activity_fork_execution_test.go",
	"internal/runtime/runforkexecution/constructed_agent_input_execution_test.go",
	"internal/runtime/runforkexecution/constructed_source_fixture_test.go",
	"internal/runtime/runforkexecution/execution_test.go",
	"internal/runtime/runforkexecution/operation_lifetime_parity_test.go",
	"internal/runtime/runforkexecution/runtime_outcome_test.go",
	"internal/runtime/runforkexecution/selected_execution_authority_fixture_test.go",
	"internal/runtime/runtime_log_native_external_test.go",
	"internal/runtime/runtime_log_native_fixture_test.go",
	"internal/runtime/runtime_recovery_diagnostics_test.go",
	"internal/runtime/runtime_shutdown_admission_test.go",
	"internal/runtime/runtime_shutdown_fan_out_test.go",
	"internal/runtime/runtime_startup_readiness_test.go",
	"internal/runtime/tools/executor_entity_test.go",
	"internal/runtime/tools/executor_sqlite_persistence_test.go",
	"internal/runtime/workflow_timer_startup_recovery_test.go",
	"internal/serveapp/dynamic_topology_startup_context_test.go",
	"internal/serveapp/main_runtime_test.go",
	"internal/serveapp/paused_startup_context_test.go",
	"internal/serveapp/run_fork_runtime_test.go",
	"internal/serveapp/startup_creation_handoff_test.go",
	"internal/serveapp/startup_integrity_test.go",
	"internal/store/internal/backend/eventrecord/sqlite/single_event_reader_test.go",
	"internal/store/internal/backend/llmpersistence/postgres_exact_coordinates_test.go",
	"internal/store/internal/backend/pipelinepersistence/descriptor_fixed_reads_test.go",
	"internal/store/internal/backend/pipelinepersistence/fan_out_owner_test.go",
	"internal/store/internal/backend/pipelinepersistence/fan_out_read_surface_test.go",
	"internal/store/internal/backend/pipelinepersistence/flow_instance_route_postgres_statements_native_test.go",
	"internal/store/internal/backend/pipelinepersistence/flow_instance_route_statements_test.go",
	"internal/store/internal/backend/pipelinepersistence/flow_instance_scoped_reads_test.go",
	"internal/store/internal/backend/runforkpersistence/run_fork_writer_settlement_test.go",
	"internal/store/internal/runtimepersistence/dynamic_flow_creation_atomicity_test.go",
	"internal/store/internal/runtimepersistence/flow_activation_attempt_test.go",
	"internal/store/internal/runtimepersistence/flow_activation_rebind_test.go",
	"internal/store/internal/runtimepersistence/flow_instance_descriptor_authority_test.go",
	"internal/store/internal/runtimepersistence/flow_route_topology_ack_consumer_external_test.go",
	"internal/store/internal/runtimepersistence/reset_coordinator_recovery_parity_test.go",
	"internal/store/internal/runtimepersistence/reset_guard_parity_test.go",
	"internal/store/internal/runtimepersistence/reset_inventory_parity_test.go",
	"internal/store/internal/runtimepersistence/reset_operation_parity_test.go",
	"internal/store/internal/runtimepersistence/reset_source_cleanup_parity_test.go",
	"internal/store/internal/runtimepersistence/run_debug_read_surface_test.go",
	"internal/store/internal/runtimepersistence/source_artifact_selected_store_parity_test.go",
	"internal/store/internal/runtimepersistence/sqlite_run_api_read_surface_test.go",
	"internal/testutil/runlifecyclefixture/fixture.go",
}
var renamed = map[string]string{
	"TestRuntimeShutdownDeliveryFixtureClaimsThroughCanonicalAdapter":                 "VerifyRuntimeShutdownDeliveryFixtureClaimsThroughCanonicalAdapterForTest",
	"TestRuntimeShutdown_ClosesAdmissionBeforeManagerDrainAndInboundIngress":          "VerifyRuntimeShutdown_ClosesAdmissionBeforeManagerDrainAndInboundIngressForTest",
	"TestRuntimeShutdownWithOptions_PropagatesConfiguredGraceToManagerDrain":          "VerifyRuntimeShutdownWithOptions_PropagatesConfiguredGraceToManagerDrainForTest",
	"TestRuntimeShutdownFanOutCommittedTurnKeepsDependenciesLive":                     "VerifyRuntimeShutdownFanOutCommittedTurnKeepsDependenciesLiveForTest",
	"TestSelectedDeliveryTransfersAcceptCommittedIsAtomic":                            "VerifySelectedDeliveryTransfersAcceptCommittedIsAtomicForTest",
	"TestEventBusSnapshottedAgentRouteSendLinearizesWithRemoval":                      "VerifyEventBusSnapshottedAgentRouteSendLinearizesWithRemovalForTest",
	"TestCommittedFinalizersScopeIngressAdmissionWithoutLosingRuntimeAuthority":       "VerifyCommittedFinalizersScopeIngressAdmissionWithoutLosingRuntimeAuthorityForTest",
	"TestDeliverySessionBindingRejectsForeignSourceWithExactClaimBeforeStoreMutation": "VerifyDeliverySessionBindingRejectsForeignSourceWithExactClaimBeforeStoreMutationForTest",
	"TestEventBusResetPreservesPendingOperationUntilPriorRetirementSucceeds":          "VerifyEventBusResetPreservesPendingOperationUntilPriorRetirementSucceedsForTest",
}

func main() {
	rows := decode(commit(approved, ledger))
	existing := map[string]int{}
	for i, row := range rows {
		current := row.After
		if row.Successor != "" {
			current = row.Successor
		}
		f := parseFunction(current)
		existing[row.File+"/"+row.Function+"/"+f.Receiver] = i
		existing[row.File+"/"+f.Name+"/"+f.Receiver] = i
	}
	var transitions []transition
	for _, path := range files {
		before := functions(commit(approved, path))
		data, err := os.ReadFile(path)
		must(err)
		after := functions(data)
		keys := make([]string, 0, len(before))
		for key := range before {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			original := before[key]
			replacement, found := after[key]
			if !found && renamed[original.Name] != "" {
				replacement, found = after[renamed[original.Name]+"/"+original.Receiver]
			}
			if found && replacement.Body == original.Body {
				continue
			}
			change := transition{File: path, Function: original.Name, Before: original.Body, After: replacement.Body, Removed: !found}
			transitions = append(transitions, change)
			rows = applyTransition(rows, existing, path, original, replacement, found)
		}
	}
	raw, err := json.Marshal(transitions)
	must(err)
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	fmt.Printf("transitions=%d digest=%s\n", len(transitions), digest)
	for _, change := range transitions {
		fmt.Printf("%s\t%s\tremoved=%t\n", change.File, change.Function, change.Removed)
	}
	if len(os.Args) > 1 && os.Args[1] == "-write" {
		if digest != targetDigest {
			panic("unreviewed family transition")
		}
		data, err := json.MarshalIndent(rows, "", "  ")
		must(err)
		must(os.WriteFile(ledger, append(data, '\n'), 0644))
		raw, err = json.MarshalIndent(transitions, "", "  ")
		must(err)
		must(os.WriteFile("tools/fixture-codemod/pipeline-observations/family144-transitions.json", append(raw, '\n'), 0644))
	}
}
func applyTransition(rows []recipe, existing map[string]int, path string, original, replacement function, found bool) []recipe {
	identity := path + "/" + original.Name + "/" + original.Receiver
	if index, ok := existing[identity]; ok {
		row := &rows[index]
		oldCurrent := row.After
		if row.Successor != "" {
			oldCurrent = row.Successor
		}
		if parseFunction(oldCurrent).Body != original.Body || row.Removed {
			panic("noncanonical source predecessor: " + identity)
		}
		row.Successor, row.Removed = replacement.Body, !found
		return rows
	}
	afterBody := replacement.Body
	if !found {
		afterBody = original.Body
	}
	return append(rows, recipe{Family: "native-run-setup-family144", File: path, Function: original.Name, Before: original.Body, After: afterBody, Removed: !found})
}

func functions(source []byte) map[string]function {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", source, parser.AllErrors)
	must(err)
	result := map[string]function{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		receiver := ""
		if fn.Recv != nil {
			receiver = node(fn.Recv.List[0].Type)
		}
		f := function{Name: fn.Name.Name, Receiver: receiver, Body: node(fn)}
		key := f.Name + "/" + receiver
		if _, duplicate := result[key]; duplicate {
			panic("ambiguous function: " + key)
		}
		result[key] = f
	}
	return result
}
func parseFunction(source string) function {
	found := functions([]byte("package proof\n" + source))
	if len(found) != 1 {
		panic("invalid recipe function")
	}
	for _, f := range found {
		return f
	}
	panic("missing recipe function")
}
func node(value ast.Node) string {
	var b bytes.Buffer
	must(format.Node(&b, token.NewFileSet(), value))
	return strings.TrimSpace(b.String())
}
func commit(ref, path string) []byte {
	b, e := exec.Command("git", "show", ref+":"+path).Output()
	must(e)
	return b
}
func decode(data []byte) []recipe { var rows []recipe; must(json.Unmarshal(data, &rows)); return rows }
func must(err error) {
	if err != nil {
		panic(err)
	}
}
