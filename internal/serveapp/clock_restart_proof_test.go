package serveapp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"

	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
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
				statuses, err := readServedLandingRuns(t.Context(), rt)
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
				if code := second.stop(); code != 0 {
					t.Fatalf("retained clock joined shutdown=%d\n%s", code, second.outputString())
				}
			})
		}
	}
}

func TestServedPausedClockRetainsFencedDueTicksBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			opts, start := clockDeploymentHarness(t, backend, canonicalrouting.CopyClockDeployment(t, false))
			ready := make(chan *runtimepkg.Runtime, 1)
			opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) { ready <- rt }
			process, served := start()
			select {
			case served.Runtime = <-ready:
			case <-time.After(2 * time.Second):
				t.Fatal("clock runtime did not reach the ready hook")
			}
			statuses, err := readServedLandingRuns(t.Context(), served.Runtime)
			if err != nil || len(statuses) != 1 {
				t.Fatalf("clock run selection=%+v err=%v", statuses, err)
			}
			runID := statuses[0].RunID
			requireServedOKJSONRPC(t, served.Endpoint, "run.pause", map[string]any{"run_id": runID, "idempotency_key": "fenced-clock-pause"})
			wait, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			release, err := served.Runtime.WorkOccurrence().BeginRunTransition(wait, runID)
			if err != nil {
				t.Fatalf("pause did not join already-started clock work: %v", err)
			}
			defer release()
			frozen := readServedClockHeader(t, served.Endpoint, runID).ClockSchedules[0]
			if frozen.Status != genericschedule.StatusActive || frozen.NextDueAt == nil {
				t.Fatalf("paused clock lost its durable active cursor: %+v", frozen)
			}
			before := servedClockEvents(t, served.Endpoint, runID, "poll.tick")
			time.Sleep(750 * time.Millisecond)
			cutoff := time.Now().UTC()
			still := readServedClockHeader(t, served.Endpoint, runID).ClockSchedules[0]
			if still.NextDueAt == nil || !still.NextDueAt.Equal(*frozen.NextDueAt) || len(servedClockEvents(t, served.Endpoint, runID, "poll.tick")) != len(before) {
				t.Fatalf("fenced clock advanced instead of retaining due ticks: before=%+v after=%+v", frozen, still)
			}
			release()
			requireServedOKJSONRPC(t, served.Endpoint, "run.continue", map[string]any{"run_id": runID, "idempotency_key": "fenced-clock-continue"})
			deadline := time.Now().Add(10 * time.Second)
			for {
				seen := make(map[time.Time]string)
				for _, event := range servedClockEvents(t, served.Endpoint, runID, "poll.tick") {
					at := event.CreatedAt.UTC()
					if prior := seen[at]; prior != "" {
						t.Fatalf("continued clock duplicated due=%s events=%s/%s", at, prior, event.EventID)
					}
					seen[at] = event.EventID
				}
				missing := time.Time{}
				for due := frozen.NextDueAt.UTC(); !due.After(cutoff); due = due.Add(250 * time.Millisecond) {
					if seen[due] == "" {
						missing = due
						break
					}
				}
				if missing.IsZero() {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("continued clock skipped fenced due=%s cursor=%+v", missing, frozen)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if code := process.stop(); code != 0 {
				t.Fatalf("clock process did not join: code=%d output=%s", code, process.outputString())
			}
		})
	}
}

type continuedClockCensusFault struct {
	genericschedule.Store
	fail atomic.Bool
}

func (s *continuedClockCensusFault) ListActiveGenericScheduleActivationsForRun(ctx context.Context, runID string) ([]genericschedule.Activation, error) {
	if s.fail.Load() {
		return nil, errors.New("injected continued clock census failure")
	}
	reader, ok := s.Store.(interface {
		ListActiveGenericScheduleActivationsForRun(context.Context, string) ([]genericschedule.Activation, error)
	})
	if !ok {
		return nil, errors.New("clock store lacks its native run-scoped census")
	}
	return reader.ListActiveGenericScheduleActivationsForRun(ctx, runID)
}

func TestServedContinuedClockRearmFailureIsReadableBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			previous := projectRuntimePersistenceForServe
			var fault *continuedClockCensusFault
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				projection := previous(owner)
				fault = &continuedClockCensusFault{Store: projection.deps.GenericScheduleStore}
				projection.deps.GenericScheduleStore = fault
				return projection
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			_, start := clockDeploymentHarness(t, backend, canonicalrouting.CopyClockDeployment(t, false))
			process, served := start()
			if fault == nil {
				t.Fatal("clock census fault did not bind to the served runtime")
			}
			var runList struct {
				Runs []struct {
					RunID string `json:"run_id"`
				} `json:"runs"`
			}
			requireServedJSONRPCResult(t, served.Endpoint, "run.list", map[string]any{}, &runList)
			if len(runList.Runs) != 1 {
				t.Fatalf("clock deployment run list=%+v", runList)
			}
			runID := runList.Runs[0].RunID
			requireServedOKJSONRPC(t, served.Endpoint, "run.pause", map[string]any{"run_id": runID, "idempotency_key": "clock-census-pause"})
			fault.fail.Store(true)
			requireServedOKJSONRPC(t, served.Endpoint, "run.continue", map[string]any{"run_id": runID, "idempotency_key": "clock-census-continue"})
			deadline := time.Now().Add(5 * time.Second)
			for {
				var read struct {
					Run struct {
						Status string `json:"status"`
					} `json:"run"`
					RuntimeReadiness *struct {
						Ready  bool   `json:"ready"`
						Reason string `json:"reason"`
						Stage  string `json:"stage"`
					} `json:"runtime_readiness"`
				}
				requireServedJSONRPCResult(t, served.Endpoint, "run.get", map[string]any{"run_id": runID}, &read)
				if read.RuntimeReadiness != nil && read.RuntimeReadiness.Reason == "runtime_failed" {
					if read.Run.Status != "live" || read.RuntimeReadiness.Ready || read.RuntimeReadiness.Stage != "rearm_clocks" {
						t.Fatalf("clock rearm failure lost committed run/readable stage: %+v", read)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("continued run did not report clock rearm failure: %+v", read)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if code := process.stop(); code != 0 {
				t.Fatalf("clock failure process did not join: code=%d output=%s", code, process.outputString())
			}
		})
	}
}
