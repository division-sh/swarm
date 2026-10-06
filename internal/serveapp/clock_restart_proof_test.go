package serveapp

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func readServedClockHeader(t *testing.T, endpoint, runID string) operatorread.RunHeader {
	t.Helper()
	var result struct {
		Run operatorread.RunHeader `json:"run"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.get", map[string]any{"run_id": runID}, &result)
	if result.Run.RunID != runID || len(result.Run.ClockSchedules) != 1 {
		t.Fatalf("exact clock header=%+v", result)
	}
	return result.Run
}

func servedClockEvents(t *testing.T, endpoint, runID, name string) []operatorread.OperatorEventFull {
	t.Helper()
	var page operatorread.OperatorEventListResult
	requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID}, "limit": 100}, &page)
	var out []operatorread.OperatorEventFull
	for _, event := range page.Events {
		if event.EventName == name {
			out = append(out, event)
		}
	}
	return out
}

func TestServedClockActiveDowntimeAndRetainedSourceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, nested := range []bool{false, true} {
			t.Run(backend+fmt.Sprintf("/nested=%t", nested), func(t *testing.T) {
				root := canonicalrouting.CopyClockDeployment(t, nested)
				opts, start := clockDeploymentHarness(t, backend, root)
				setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
				first, served := start()
				rt := servedTestProcessRuntime(t, first)
				statuses, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
				if err != nil || len(statuses) != 1 {
					t.Fatalf("initial clock generation=%+v err=%v", statuses, err)
				}
				status := statuses[0]
				initial := readServedClockHeader(t, served.Endpoint, status.RunID).ClockSchedules[0]
				if code := first.stop(); code != 0 {
					t.Fatalf("active clock shutdown=%d", code)
				}
				// There is no operator suspension: every elapsed due coordinate
				// remains recoverable, unlike the deliberately skipped park interval.
				time.Sleep(time.Second)
				cutoff := time.Now().UTC()
				if err := os.RemoveAll(root); err != nil {
					t.Fatal(err)
				}
				opts.SourceRoot = ""
				retained := t.TempDir()
				barrier := &clockBootBarrier{entered: make(chan struct{}, 1), release: make(chan struct{})}
				opts.TestLifecycleProbe = barrier
				second := startRuntimeTestProcessWithRunner(t, repoRootForTest(), *opts, func(ctx context.Context, repo string, options cliapp.ServeOptions) int {
					code, err := runOwnedLifecycle(ctx, repo, retained, options, apiv1.AuthTokenResolution{
						Tokens: []string{apiv1.DefaultLoopbackAPIToken}, Explicit: true, Source: "internal-lifecycle-parent",
					}, executionposture.Live, served.BundleHash)
					if err != nil {
						fmt.Fprintf(options.Output, "retained clock startup: %v\n", err)
					}
					return code
				})
				t.Cleanup(barrier.unblock)
				select {
				case <-barrier.entered:
				case <-time.After(10 * time.Second):
					t.Fatalf("retained restart never reached startup frontier:\n%s", second.outputString())
				}
				time.Sleep(300 * time.Millisecond)
				barrier.mu.Lock()
				premature := len(barrier.clocks)
				barrier.mu.Unlock()
				if premature != 0 {
					t.Fatal("instance catch-up ran before retained startup release")
				}
				barrier.unblock()
				second.waitForReadyLine()
				endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, second.outputString()) + "/v1/rpc"
				t.Log("proof_surface=public initial serve; H retained live-node restart after source deletion; public readback")
				clock := readServedClockHeader(t, endpoint, status.RunID).ClockSchedules[0]
				if clock.ActivationID != initial.ActivationID || !clock.InitialDueAt.Equal(initial.InitialDueAt) || clock.Suspension != nil || clock.Status != genericschedule.StatusActive {
					t.Fatalf("restart replaced/parked the durable clock: initial=%+v current=%+v", initial, clock)
				}
				eventName := "poll.tick"
				if nested {
					eventName = "clock/poll.tick"
				}
				deadline := time.Now().Add(10 * time.Second)
				for {
					rows := servedClockEvents(t, endpoint, status.RunID, eventName)
					seen := make(map[time.Time]string)
					for _, event := range rows {
						at := event.CreatedAt.UTC()
						if old := seen[at]; old != "" {
							t.Fatalf("due occurrence duplicated: at=%s events=%s/%s", at, old, event.EventID)
						}
						seen[at] = event.EventID
						if len(event.DeadLetters) != 0 {
							t.Fatalf("clock recovery lost its consumer: %+v", event)
						}
					}
					missing := time.Time{}
					for due := initial.InitialDueAt.UTC(); !due.After(cutoff); due = due.Add(250 * time.Millisecond) {
						if seen[due] == "" {
							missing = due
							break
						}
					}
					if missing.IsZero() {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("restart skipped due=%s initial=%s cutoff=%s events=%+v", missing, initial.InitialDueAt, cutoff, rows)
					}
					time.Sleep(10 * time.Millisecond)
				}
				invokeServedStandingOperation(t, endpoint, "standing.suspend", status.ServiceID, "clock-fork-park")
				before := readServedClockHeader(t, endpoint, status.RunID)
				rows := servedClockEvents(t, endpoint, status.RunID, eventName)
				if len(rows) == 0 {
					t.Fatal("fork proof lacks a persisted clock frontier")
				}
				response := requestServedJSONRPC(t, endpoint, "run.fork", map[string]any{
					"source_run_id": status.RunID, "fork_event_id": rows[0].EventID,
					"allow_source_freeze": true, "idempotency_key": "clock-fork-refusal",
				})
				if response.Error == nil || !strings.Contains(fmt.Sprint(response.Error), runfork.RunForkBlockerTimerHistoryUnproven) {
					t.Fatalf("unsupported clock fork was not refused: %+v", response)
				}
				after := readServedClockHeader(t, endpoint, status.RunID)
				if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(rows, servedClockEvents(t, endpoint, status.RunID, eventName)) {
					t.Fatal("refused clock fork mutated source run, clock or events")
				}
				if code := second.stop(); code != 0 {
					t.Fatalf("retained clock joined shutdown=%d\n%s", code, second.outputString())
				}
			})
		}
	}
}
