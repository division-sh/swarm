package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
)

// #2698/6205327685, decision8: one finite source composition for the settled
// native-fixture/owner repairs. This never edits any historical recipe endpoint.
const repair2698Before = "e857a1e482a2e08ab63f2e8169b4472e608cfdc2"
const repair2698After = "dcd9143c69"
const repair2698Manifest = "tools/fixture-codemod/pipeline-observations/internal/selectedreadrole/testdata/repair_2698_sources.json"

var repair2698Sources = []string{
	"internal/apiv1/agent_snapshot_atomicity_test.go",
	"internal/apiv1/conversation_fork_commit_omission_probe_test.go",
	"internal/apiv1/operator_event_replay_test.go",
	"internal/apiv1/retired_dashboard_preservation_test.go",
	"internal/apiv1/selected_store_read_supported_surface_test.go",
	"internal/apiv1/sqlite_observability_supported_surface_test.go",
	"internal/apiv1/sqlite_operator_read_supported_surface_test.go",
	"internal/runtime/bus/event_identity_dispatch_surface_test.go",
	"internal/runtime/bus/eventbus_agent_route_test.go",
	"internal/runtime/bus/eventbus_publish_test.go",
	"internal/runtime/bus/pause_eligibility_test.go",
	"internal/runtime/conformance/a2_repeated_fan_out_conformance_test.go",
	"internal/runtime/conformance/managed_capability_test.go",
	"internal/runtime/conformance/persisted_surfaces_test.go",
	"internal/runtime/manager/delivery_claim_ack_test.go",
	"internal/runtime/manager/delivery_native_owner_external_test.go",
	"internal/runtime/manager/delivery_native_selected_external_test.go",
	"internal/runtime/manager/execution_projection_test.go",
	"internal/runtime/manager/failure_test.go",
	"internal/runtime/manager/lifecycle_coordinator_test.go",
	"internal/runtime/manager/receipt_selected_store_test.go",
	"internal/runtime/manager/receipt_settlement_outcome_test.go",
	"internal/runtime/manager/receipts_test.go",
	"internal/runtime/node_delivery_startup_recovery_test.go",
	"internal/runtime/runforkexecution/execution_test.go",
	"internal/runtime/runtime_log_native_external_test.go",
	"internal/runtime/runtime_log_native_fixture_test.go",
	"internal/runtime/runtime_shutdown_admission_test.go",
	"internal/runtime/runtime_shutdown_fan_out_test.go",
	"internal/serveapp/a2_join_public_projection_agreement_test.go",
	"internal/serveapp/main_runtime_test.go",
	"internal/serveapp/receiver_initialization_test.go",
	"internal/store/internal/backend/delivery/read_projections.go",
	"internal/store/internal/backend/pipelinepersistence/owner_operations.go",
	"internal/store/internal/backend/pipelinepersistence/template_storage_observation.go",
	"internal/store/internal/runtimepersistence/run_debug_read_surface_test.go",
	"internal/store/internal/runtimepersistence/sqlite_run_api_read_surface_test.go",
	"internal/store/internal/runtimepersistence/test_deployment_resource_observation.go",
	"internal/store/internal/runtimepersistence/test_event_support.go",
	"internal/store/internal/runtimepersistence/test_served_delivery_observation.go",
	"internal/store/internal/runtimepersistence/workflow_route_recovery_owner_test.go",
}

func capture2698SourceCommand(args []string) bool {
	if len(args) < 2 || args[1] != "-repair-2698-sources" {
		return false
	}
	if len(args) != 3 || args[2] != "-write" {
		panic("usage: reconcile-family145 -repair-2698-sources -write")
	}
	type source struct{ File, BeforeSHA256, AfterSHA256 string }
	packet := struct {
		BeforeCommit, AfterCommit, Authorization string
		Files                                    []source
	}{BeforeCommit: repair2698Before, Authorization: "https://github.com/division-sh/swarm/pull/2698#issuecomment-6205327685"}
	ref, err := exec.Command("git", "rev-parse", repair2698After).Output()
	must(err)
	packet.AfterCommit = string(bytes.TrimSpace(ref))
	for _, path := range repair2698Sources {
		before, err := exec.Command("git", "show", repair2698Before+":"+path).Output()
		must(err)
		after, err := exec.Command("git", "show", packet.AfterCommit+":"+path).Output()
		must(err)
		current, err := os.ReadFile(path)
		must(err)
		if !bytes.Equal(current, after) || bytes.Equal(before, after) {
			panic("source is not its exact changed signed endpoint: " + path)
		}
		packet.Files = append(packet.Files, source{path, fmt.Sprintf("%x", sha256.Sum256(before)), fmt.Sprintf("%x", sha256.Sum256(after))})
	}
	writeJSON(repair2698Manifest, packet)
	fmt.Printf("native repair source intersections=%d\n", len(packet.Files))
	return true
}
