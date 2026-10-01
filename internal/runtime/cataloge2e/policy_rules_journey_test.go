package cataloge2e

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testcatalog"
)

func r3RuntimeFixture(t *testing.T) testcatalog.Fixture {
	t.Helper()
	return testcatalog.Fixture{Name: "policy-rules", RelativePath: "examples/routing/policy-rules", Root: filepath.Join(repoRootFromCatalogE2E(t), "examples/routing/policy-rules")}
}

func assertR3RuntimeOutput(t *testing.T, h *runtimeHarness, run, event string, count int) {
	t.Helper()
	rows, err := h.db.QueryContext(context.WithoutCancel(catalogRunContext(h, run)), `SELECT payload FROM events WHERE run_id=$1 AND event_name=$2`, run, event)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	observed := 0
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["proof"] != "business" || payload["limit"] != float64(3) {
			t.Fatalf("literal user object changed through execution: %s", raw)
		}
		observed++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if observed != count {
		t.Fatalf("%s %s count=%d want=%d", run, event, observed, count)
	}
}

func assertR3DeliveredCriteria(t *testing.T, h *runtimeHarness) {
	t.Helper()
	h.llm.mu.Lock()
	defer h.llm.mu.Unlock()
	for _, prompt := range h.llm.systemPrompts {
		if strings.Contains(prompt, "R1") && strings.Contains(prompt, "Compare the supplied numbers.") {
			return
		}
	}
	t.Fatal("real managed actor did not receive the rules.yaml criteria")
}

func TestR3RulesNormalActorMachineRestartReplayBothStores(t *testing.T) {
	fixture := r3RuntimeFixture(t)
	transcript := buildCatalogExecutionTranscript(t, fixture)
	loadYAML(t, filepath.Join(fixture.Root, "tests/fixtures.yaml"), &transcript.agentFixtures)
	transcript.frozen = catalogTranscriptBytes(t, transcript)
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h, baseline := executeCatalogTranscript(t, fixture, backend, transcript)
			assertR3RuntimeOutput(t, h, catalogRuntimeRunID, "work.accepted", 1)
			assertR3DeliveredCriteria(t, h)
			reopenCatalogTranscript(t, fixture, transcript, h, baseline)
			_, replay := executeCatalogTranscript(t, fixture, backend, transcript)
			if string(replay.projection) != string(baseline.projection) {
				t.Fatal("fresh retained transcript replay diverged")
			}
		})
	}
}

func TestR3RulesSelectedActorMachineBothStores(t *testing.T) {
	fixture := r3RuntimeFixture(t)
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, fixture.Root, backend, true)
			h.seedInitialState(pipeline.FlowInstanceEntityID(catalogRuntimeRunID))
			ctx := worklifetime.WithOccurrence(catalogRunContext(h, catalogRuntimeRunID), h.rt.WorkOccurrence())
			if _, err := runScopedCatalogStore(t, h).PauseRunControlOutcome(ctx, runcontrol.TransitionRequest{RunID: catalogRuntimeRunID, Reason: "retained rules proof", ControlledBy: "cataloge2e"}); err != nil {
				t.Fatal(err)
			}
			if err := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "work.requested", Payload: map[string]any{"left": 1, "right": 1}}, 10*time.Second, true); err != nil {
				t.Fatal(err)
			}
			var point string
			if err := h.db.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name='work.requested'`, catalogRuntimeRunID).Scan(&point); err != nil {
				t.Fatal(err)
			}
			var artifact interface {
				storetest.DurableDataCatalogStore
				runforkexecution.SourceArtifactSelectedContractSourceStore
			} = h.pg
			if h.sqlite != nil {
				artifact = h.sqlite
			}
			loader, selection, _ := selectedContractForkFixtureSelection(t, ctx, repoRootFromCatalogE2E(t), fixture.Root, artifact)
			cfg := testRuntimeConfig()
			cfg.LLM.Backend = "anthropic"
			result, err := runforkexecution.ExecuteSelectedContractRunFork(ctx, runforkexecution.SelectedContractExecutionRequest{
				SourceRunID: catalogRuntimeRunID, At: point, AllowSourceFreeze: true,
				Owner: selectedContractExecutionOwnerForCatalogHarness(t, h), SourceLoader: loader, ContractSelection: selection,
				AgentRuntime: selectedContractAgentRuntimeOptionsForCatalogHarness(h, cfg),
			})
			if err != nil {
				t.Fatal(err)
			}
			if !result.Activation.Activated || result.ExecutedEventCount != 1 || len(result.ForkEvents) != 1 {
				t.Fatalf("selected chronology=%+v", result)
			}
			assertR3RuntimeOutput(t, h, result.Materialization.ForkRunID, "work.accepted", 1)
			assertR3RuntimeOutput(t, h, catalogRuntimeRunID, "work.accepted", 0)
			assertR3DeliveredCriteria(t, h)
			var pending int
			if err := h.db.QueryRowContext(ctx, `SELECT count(*) FROM event_deliveries WHERE run_id=$1 AND event_id=$2 AND subscriber_type='agent' AND subscriber_id='review-agent' AND status='pending'`, catalogRuntimeRunID, point).Scan(&pending); err != nil {
				t.Fatal(err)
			}
			if pending != 1 {
				t.Fatalf("fork mutated source delivery: %d", pending)
			}
		})
	}
}

func TestR3RulesHostileCitationAndUnequalMachineBothStores(t *testing.T) {
	fixture := r3RuntimeFixture(t)
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, kind := range []string{"unknown citation", "unequal machine"} {
			t.Run(string(backend)+"/"+kind, func(t *testing.T) {
				h := newRuntimeHarnessForBackend(t, fixture.Root, backend, true)
				h.seedInitialState(pipeline.FlowInstanceEntityID(catalogRuntimeRunID))
				cite, right := "R1", 2
				if kind == "unknown citation" {
					cite, right = "UNKNOWN", 1
				}
				h.llm.mu.Lock()
				h.llm.agentEventFlow["review-agent"] = []scriptedAgentFixtureStep{{On: "work.requested", Emits: []agentFixtureEmit{{Event: "review.reported", Payload: map[string]any{"left": 1, "right": right, "cite": cite}}}}}
				h.llm.mu.Unlock()
				_ = h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "work.requested", Payload: map[string]any{"left": 1, "right": right}}, 10*time.Second, true)
				h.waitForCatalogStoreQuiescence(10 * time.Second)
				assertR3RuntimeOutput(t, h, catalogRuntimeRunID, "work.accepted", 0)
				if kind == "unequal machine" {
					assertR3RuntimeOutput(t, h, catalogRuntimeRunID, "work.rejected", 1)
				} else {
					var count int
					if err := h.db.QueryRowContext(h.ctx, `SELECT count(*) FROM events WHERE run_id=$1 AND event_name='review.reported'`, catalogRuntimeRunID).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != 0 {
						t.Fatal("hostile citation published")
					}
				}
			})
		}
	}
}
