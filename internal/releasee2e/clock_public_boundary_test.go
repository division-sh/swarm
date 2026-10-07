package releasee2e

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type clockPublicHeader struct {
	RunID          string           `json:"run_id"`
	Status         string           `json:"status"`
	ClockSchedules []map[string]any `json:"clock_schedules"`
}

func TestDeclaredClockNestedBinaryDeliveryBothStores(t *testing.T) {
	base := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, base)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := filepath.Join(base, backend)
			config, env := clockReleaseConfig(t, root, backend)
			source := filepath.Join(root, "contracts")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal/runtime/testfixtures/canonicalrouting/testdata/clock-deployment/nested"), source)
			writeReleaseFile(t, filepath.Join(root, "api-token"), goldenAPIToken+"\n")
			verify := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, "verify", source, "--config", config, "--portable", "--json")
			assertFullLifecycleVerifySuccess(t, verify)
			process := startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, WorkingDir: root, Source: source,
				ConfigPath: config, Store: backend, TokenFile: "api-token", Token: goldenAPIToken, Env: env})
			ctx, cancel := context.WithTimeout(t.Context(), goldenStartupTimeout)
			defer cancel()
			if err := process.waitReady(ctx); err != nil {
				t.Fatalf("nested binary clock startup: %v\n%s", err, process.output.String())
			}
			hash := goldenServedBundleHash(t, process.rpc, "live")
			standing := waitForFullLifecycleStandingRun(t, process.rpc, hash, "", 1, "")
			clocks := readPublicClockRun(t, process, standing.RunID).ClockSchedules
			if len(clocks) != 1 || clocks[0]["flow_id"] != "clock" || clocks[0]["flow_instance"] != "clock" || clocks[0]["emit"] != "clock/poll.tick" {
				t.Fatalf("nested binary clock publication coordinate=%+v", clocks)
			}
			var matched goldenEvent
			if err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
				rows, err := listGoldenEvents(ctx, process.rpc, standing.RunID)
				if err != nil {
					return false, err
				}
				for _, event := range rows {
					if event.EventName != "clock/poll.tick" {
						continue
					}
					if len(event.DeadLetters) != 0 || event.Source != "clock" || event.ProducerType != "instance" || len(event.Payload) != 0 || len(event.Deliveries) != 1 {
						return false, fmt.Errorf("nested binary clock lost exact publication/delivery: %+v", event)
					}
					delivery := event.Deliveries[0]
					if delivery.Target.FlowID != "consumer" || delivery.Target.FlowInstance != "consumer" {
						return false, fmt.Errorf("nested clock escaped its connect edge: %+v", delivery)
					}
					if delivery.Status == "delivered" && delivery.Terminal {
						matched = event
						return true, nil
					}
				}
				return false, nil
			}); err != nil {
				t.Fatal(err)
			}
			assertFullEventPayloadReadback(t, process, matched, binary, root, config, "api-token", env)
			if err := process.stopAndWait(goldenShutdownGrace); err != nil {
				t.Fatalf("nested binary clock shutdown: %v\n%s", err, process.output.String())
			}
		})
	}
}

func TestDeclaredClockFiniteAndPublicReadbackBothStores(t *testing.T) {
	base := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, base)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := filepath.Join(base, backend)
			config, env := clockReleaseConfig(t, root, backend)
			source := filepath.Join(root, "contracts")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal/runtime/testfixtures/canonicalrouting/testdata/clock-deployment/finite"), source)
			writeReleaseFile(t, filepath.Join(root, "api-token"), goldenAPIToken+"\n")
			writeReleaseFile(t, filepath.Join(root, "home", ".config", "swarm", "swarm.yaml"),
				fmt.Sprintf("serve:\n  api_token_file: %q\nconnection:\n  api_token_file: %q\n",
					filepath.Join(root, "api-token"), filepath.Join(root, "api-token")))
			writeReleaseFile(t, filepath.Join(root, "payload.json"), "{}\n")
			assertGoldenProcessHasNoExternalExecutables(t, env)
			verify := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary,
				"verify", source, "--config", config, "--portable", "--json")
			assertFullLifecycleVerifySuccess(t, verify)
			describe := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary,
				"describe", source, "--config", config, "--json")
			var described struct {
				SourceHash string `json:"source_hash"`
			}
			if err := json.Unmarshal([]byte(describe.output), &described); describe.err != nil || err != nil || described.SourceHash == "" {
				t.Fatalf("offline clock describe: command=%v decode=%v\n%s", describe.err, err, describe.output)
			}
			scenario := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary,
				"test", source, "--config", config)
			if scenario.err != nil || !strings.Contains(scenario.output, "swarm test ok: scenarios=1") {
				t.Fatalf("finite clock scenario: %v\n%s", scenario.err, scenario.output)
			}
			localID := uuid.NewString()
			finite := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary,
				"run", "start", source, "--config", config, "--event", "start.requested",
				"--payload", "payload.json", "--run-id", localID, "--mcp-port", fmt.Sprint(freeReleaseTCPPort(t)))
			if finite.err != nil || !strings.Contains(finite.output, "status=completed") {
				t.Fatalf("local finite clock run: %v\n%s", finite.err, finite.output)
			}
			deploymentStarted := time.Now().UTC()
			spec := releaseProcessSpec{
				BinaryPath: binary, WorkingDir: root, Source: source, ConfigPath: config,
				Store: backend, TokenFile: "api-token", Token: goldenAPIToken, Env: env,
			}
			process := startReleaseServe(t, spec)
			ctx, cancel := context.WithTimeout(t.Context(), goldenStartupTimeout)
			defer cancel()
			if err := process.waitReady(ctx); err != nil {
				t.Fatalf("public clock serve: %v\n%s", err, process.output.String())
			}
			if hash := goldenServedBundleHash(t, process.rpc, "live"); hash != described.SourceHash {
				t.Fatalf("offline/served clock source differs: offline=%s served=%s", described.SourceHash, hash)
			}
			assertFiniteClockRun(t, process, localID)
			standing := waitForFullLifecycleStandingRun(t, process.rpc, described.SourceHash, "", 1, "")
			if standing.StartedAt.Before(deploymentStarted) {
				t.Fatalf("finite host created a clock-only standing generation before deployment: %+v deployment=%s", standing, deploymentStarted)
			}
			clock := readPublicClockRun(t, process, standing.RunID).ClockSchedules
			if len(clock) != 1 || clock[0]["status"] != "active" || clock[0]["retains_run"] != true {
				t.Fatalf("serve did not arm the declaring generation: %+v", clock)
			}
			clockEvent := waitPublicClockEvent(t, process, standing.RunID)
			assertFullEventPayloadReadback(t, process, clockEvent, binary, root, config, "api-token", env)
			connectedID := uuid.NewString()
			connected := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary,
				"run", "start", "--connect", process.apiBase, "--event", "start.requested", "--payload", "payload.json",
				"--run-id", connectedID, "--no-follow")
			if connected.err != nil {
				t.Fatalf("connected finite clock run: %v\n%s", connected.err, connected.output)
			}
			waitForFullLifecycleRunStatus(t, process.rpc, connectedID, "completed")
			assertFiniteClockRun(t, process, connectedID)
			var suspended standingRuntimePublicResult
			if err := process.rpc.call(ctx, "standing.suspend", map[string]any{
				"service_id": standing.Origin.ServiceID, "idempotency_key": "clock-public-suspend",
			}, &suspended); err != nil || suspended.RunID != standing.RunID || suspended.Generation != 1 || suspended.EffectiveState != "suspended" {
				t.Fatalf("public clock suspension: %+v err=%v", suspended, err)
			}
			parked := readPublicClockRun(t, process, standing.RunID)
			if len(parked.ClockSchedules) != 1 || parked.ClockSchedules[0]["activation_id"] != clock[0]["activation_id"] ||
				parked.ClockSchedules[0]["status"] != "parked" || parked.ClockSchedules[0]["retains_run"] != false ||
				parked.ClockSchedules[0]["next_due_at"] != nil || parked.ClockSchedules[0]["suspension"] == nil {
				t.Fatalf("public parked clock lost durable identity: %+v", parked)
			}
			assertClockTransportReadback(t, process, binary, root, config, env, parked)
			if err := process.stopAndWait(goldenShutdownGrace); err != nil {
				t.Fatalf("clock public shutdown: %v\n%s", err, process.output.String())
			}
			finiteAgain := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary,
				"run", "start", source, "--config", config, "--event", "start.requested",
				"--payload", "payload.json", "--run-id", uuid.NewString(), "--mcp-port", fmt.Sprint(freeReleaseTCPPort(t)))
			if finiteAgain.err != nil || !strings.Contains(finiteAgain.output, "status=completed") {
				t.Fatalf("finite host with a parked deployment: %v\n%s", finiteAgain.err, finiteAgain.output)
			}
			restarted := startReleaseServe(t, spec)
			if err := restarted.waitReady(ctx); err != nil {
				t.Fatalf("parked deployment after finite host: %v\n%s", err, restarted.output.String())
			}
			if after := readPublicClockRun(t, restarted, standing.RunID); !reflect.DeepEqual(after.ClockSchedules, parked.ClockSchedules) {
				t.Fatalf("finite host mutated the parked deployment: before=%+v after=%+v", parked.ClockSchedules, after.ClockSchedules)
			}
			if err := restarted.stopAndWait(goldenShutdownGrace); err != nil {
				t.Fatalf("parked clock restart shutdown: %v\n%s", err, restarted.output.String())
			}
			t.Log("proof_surface=real binary verify/describe/private scenario/local finite run/serve/connected finite run; parked HTTP+CLI clock inventory and HTTP+WebSocket+CLI occurrence readback; no external executor, provider or SQL test access")
		})
	}
}

func readPublicClockRun(t *testing.T, process *releaseServeProcess, runID string) clockPublicHeader {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), goldenStartupTimeout)
	defer cancel()
	var result struct {
		Run clockPublicHeader `json:"run"`
	}
	if err := process.rpc.call(ctx, "run.get", map[string]any{"run_id": runID}, &result); err != nil || result.Run.RunID != runID {
		t.Fatalf("public clock run header: %+v err=%v", result, err)
	}
	return result.Run
}

func assertFiniteClockRun(t *testing.T, process *releaseServeProcess, runID string) {
	t.Helper()
	header := readPublicClockRun(t, process, runID)
	if header.Status != "completed" || len(header.ClockSchedules) != 0 {
		t.Fatalf("finite execution acquired a deployment clock: %+v", header)
	}
	ctx, cancel := context.WithTimeout(t.Context(), goldenStartupTimeout)
	defer cancel()
	events, err := listGoldenEvents(ctx, process.rpc, runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.EventName == "poll.tick" {
			t.Fatalf("finite execution published a clock occurrence: %+v", event)
		}
	}
}

func assertClockTransportReadback(t *testing.T, process *releaseServeProcess, binary, root, config string, env []string, expected clockPublicHeader) {
	t.Helper()
	cli := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary,
		"run", "status", expected.RunID, "--config", config, "--api-server", process.apiBase,
		"--api-token-file", "api-token", "--no-diagnose", "--json")
	var result struct {
		Run clockPublicHeader `json:"run"`
	}
	if err := json.Unmarshal([]byte(cli.output), &result); cli.err != nil || err != nil || !reflect.DeepEqual(result.Run.ClockSchedules, expected.ClockSchedules) {
		t.Fatalf("CLI clock readback differs: command=%v decode=%v\n%s", cli.err, err, cli.output)
	}
	text := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary,
		"run", "status", expected.RunID, "--config", config, "--api-server", process.apiBase,
		"--api-token-file", "api-token", "--no-diagnose")
	if text.err != nil || !strings.Contains(text.output, "./poll: parked; suspended since ") || strings.Contains(text.output, "next due") {
		t.Fatalf("CLI invented an armed parked clock: %v\n%s", text.err, text.output)
	}
	if after := readPublicClockRun(t, process, expected.RunID); !reflect.DeepEqual(after.ClockSchedules, expected.ClockSchedules) {
		t.Fatalf("clock inspection changed durable state: before=%+v after=%+v", expected.ClockSchedules, after.ClockSchedules)
	}
}

func waitPublicClockEvent(t *testing.T, process *releaseServeProcess, runID string) goldenEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var matched goldenEvent
	err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
		events, err := listGoldenEvents(ctx, process.rpc, runID)
		if err != nil {
			return false, err
		}
		for _, event := range events {
			if event.EventName == "poll.tick" {
				matched = event
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil || matched.EventID == "" || matched.RunID != runID || len(matched.Payload) != 0 {
		t.Fatalf("public clock occurrence is not a bare business event: %+v err=%v\n%s", matched, err, process.output.String())
	}
	return matched
}
