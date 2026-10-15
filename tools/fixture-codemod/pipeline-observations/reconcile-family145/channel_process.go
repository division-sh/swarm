package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"slices"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

var channelProcessFiles = []string{
	"internal/serveapp/channel_ack_process_supported_surface_test.go",
	"internal/serveapp/channel_delivery_anchor_supported_surface_test.go",
	"internal/serveapp/channel_delivery_draft_temporal_supported_surface_test.go",
	"internal/serveapp/channel_delivery_inbound_supported_surface_test.go",
	"internal/serveapp/channel_delivery_process_supported_surface_test.go",
	"internal/serveapp/channel_delivery_recovery_supported_surface_test.go",
	"internal/serveapp/channel_delivery_scan_isolation_supported_surface_test.go",
	"internal/serveapp/channel_delivery_settlement_diagnostic_test.go",
	"internal/serveapp/channel_delivery_temporal_supported_surface_test.go",
	"internal/serveapp/channel_delivery_uncertain_copy_supported_surface_test.go",
	"internal/serveapp/channel_delivery_uncertain_notice_supported_surface_test.go",
	"internal/serveapp/channel_input_process_supported_surface_test.go",
	"internal/serveapp/channel_native_entry_disposition_supported_surface_test.go",
	"internal/serveapp/channel_native_process_supported_surface_test.go",
	"internal/serveapp/channel_object_anchor_supported_surface_test.go",
	"internal/serveapp/channel_object_input_supported_surface_test.go",
	"internal/serveapp/channel_object_response_supported_surface_test.go",
	"internal/serveapp/channel_object_supported_surface_test.go",
	"internal/serveapp/channel_object_temporal_supported_surface_test.go",
	"internal/serveapp/channel_onboarding_crash_boundaries_e2e_test.go",
	"internal/serveapp/channel_onboarding_effect_restart_test.go",
	"internal/serveapp/channel_onboarding_identity_e2e_test.go",
	"internal/serveapp/channel_onboarding_supported_surface_test.go",
	"internal/serveapp/channel_receptionist_supported_surface_test.go",
	"internal/serveapp/channel_selected_control_supported_surface_test.go",
	"internal/serveapp/channel_source_lifecycle_supported_surface_test.go",
	"internal/testutil/postgres.go",
}

func captureRecentFamilyCommand(args []string) bool {
	if captureAgentSnapshotCommand(args) {
		return true
	}
	if len(args) != 2 {
		return false
	}
	switch args[1] {
	case "-served-scenario-capture":
		captureServedScenario()
	case "-served-scenario-history":
		composeServedScenarioHistory()
	case "-delivery-recovery-schema":
		rewriteDeliveryRecoverySchema()
	case "-delivery-recovery-capture":
		captureDeliveryRecovery()
	case "-delivery-recovery-history":
		composeDeliveryRecoveryHistory()
	case "-delivery-recovery-clock":
		rewriteDeliveryRecoveryClock()
	case "-selected-fork-execution-capture":
		captureSelectedForkExecution()
	case "-selected-fork-execution-history":
		composeSelectedForkExecutionHistory()
	case "-durable-resource-capture":
		captureDurableResource()
	case "-durable-resource-history":
		composeDurableResourceHistory()
	case "-channel-process-capture":
		captureChannelProcess()
	case "-channel-process-history":
		composeChannelProcessHistory()
	default:
		return false
	}
	return true
}

func captureChannelProcess() {
	var pins []transition
	for _, path := range channelProcessFiles {
		raw, err := exec.Command("git", "show", selectedreadrole.ChannelProcessPredecessor+":"+path).Output()
		must(err)
		prior := functions(raw)
		raw, err = os.ReadFile(path)
		must(err)
		current := functions(raw)
		keys := make([]string, 0, len(prior))
		for key := range prior {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			if prior[key] != current[key] {
				pins = append(pins, transition{File: path, Function: functionName(prior[key]), Before: prior[key], After: current[key], Removed: current[key] == ""})
			}
		}
	}
	writeJSON(selectedreadrole.ChannelProcessPath, pins)
	raw, err := os.ReadFile(selectedreadrole.ChannelProcessPath)
	must(err)
	canonical, err := json.Marshal(pins)
	must(err)
	fmt.Printf("channel process declarations=%d file=%x canonical=%x\n", len(pins), sha256.Sum256(raw), sha256.Sum256(canonical))
}

func composeChannelProcessHistory() {
	for _, recipes := range []bool{true, false} {
		prior, err := selectedreadrole.PriorChannelProcessHistory(".", recipes)
		must(err)
		candidate, err := selectedreadrole.ComposeChannelProcessHistory(".", prior, recipes)
		must(err)
		path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
		if recipes {
			path = "tools/fixture-codemod/pipeline-observations/recipes.json"
			var rows []recipe
			must(json.Unmarshal(candidate, &rows))
			writeJSON(path, rows)
		} else {
			var rows []transition
			must(json.Unmarshal(candidate, &rows))
			writeJSON(path, rows)
		}
	}
}
