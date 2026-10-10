package serveapp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
)

type issue2353H2OracleRequest struct {
	Process  issue2353H2ProcessRequest
	RunID    string
	Accepted map[string]string
}

func TestIssue2353H2CounterOracleProcessHelper(t *testing.T) {
	path := os.Getenv(issue2353H2OracleEnv)
	if path == "" {
		t.Skip("parent-owned counterexample oracle")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var request issue2353H2OracleRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		t.Fatal(err)
	}
	reader := storetest.OpenIssue2564WorkloadObservation(t, request.Process.Backend, request.Process.Location)
	rt := issue2564H2Fixture{selected: reader.Reader, cuts: map[issue2564H2CutCoordinate]issue2564H2CutWitness{}}
	snapshot, err := issue2564H2Read(t.Context(), rt, request.RunID)
	if err != nil {
		t.Fatal(err)
	}
	issue2564H2CounterEvidence(t, rt, request.RunID, request.Accepted, snapshot)
}

func issue2353H2CaptureOracle(t *testing.T, request issue2353H2OracleRequest) {
	t.Helper()
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "oracle.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := issue2564H2QualificationContext(t)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestIssue2353H2CounterOracleProcessHelper$", "-test.v")
	cmd.Env = append(os.Environ(), issue2353H2OracleEnv+"="+path)
	output, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "H2 exact per-hub bump/stage counts:") {
		t.Fatalf("unchanged H2 positivity oracle did not reject the valid trace: %v\n%s", err, output)
	}
	t.Logf("H2_ORACLE_EXPECTED_REFUSAL original_assertion_unchanged=true\n%s", output)
}

func issue2353H2PreserveCounters(t *testing.T, before, after issue2564H2Snapshot) {
	t.Helper()
	for id, old := range before.Hubs {
		next := after.Hubs[id]
		if old.Entity != next.Entity || old.Instance != next.Instance || old.Count != next.Count || old.C1 != next.C1 || old.C2 != next.C2 || !issue2564H2CutsPreserved(old.Cuts, next.Cuts) {
			t.Fatalf("H2 controlled recovery changed exact effects/history: hub=%s before=%+v after=%+v", id, old, next)
		}
	}
}

func issue2353H2Close(t *testing.T, rt issue2564H2Fixture, seed servedEventPublishRPCResult) {
	t.Helper()
	ctx, cancel := issue2564H2QualificationContext(t)
	defer cancel()
	var joined sync.WaitGroup
	errors := make(chan error, 6)
	for hub := 1; hub <= 6; hub++ {
		joined.Add(1)
		go func(hub int) {
			defer joined.Done()
			_, err := issue2564H2Publish(ctx, &http.Client{}, rt.Endpoint, map[string]any{"event_name": "hub.close", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"hub_id": fmt.Sprintf("h%02d", hub)}, "idempotency_key": fmt.Sprintf("h2-close-%d", hub)})
			errors <- err
		}(hub)
	}
	joined.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	rt.waitDeliveries(t, seed.RunID)
}

func TestIssue2353H2ControlledSingleStageBothStores(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			root := issue2564H2Source(t)
			keys := issue2564H2DeclarationKeys(t, root)
			start, control, request := issue2353H2Harness(t, backend, root)
			first, rt := start(true)
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "hub.start", "bundle_hash": rt.BundleHash, "payload": map[string]any{"hub_id": "h01"}, "idempotency_key": "h2-start-1"})
			for hub := 2; hub <= 6; hub++ {
				requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "hub.start", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"hub_id": fmt.Sprintf("h%02d", hub)}, "idempotency_key": fmt.Sprintf("h2-start-%d", hub)})
			}
			rt.waitDeliveries(t, seed.RunID)
			initial, err := issue2564H2Read(t.Context(), rt, seed.RunID)
			if err != nil || len(initial.Hubs) != 6 {
				t.Fatalf("controlled H2 construction: hubs=%d err=%v", len(initial.Hubs), err)
			}
			target := initial.Hubs["h05"]
			if err := control.input.Encode(issue2353H2Command{Action: "arm", RunID: seed.RunID, Entity: target.Entity, Instance: target.Instance, Declaration: keys["s2"]}); err != nil {
				t.Fatal(err)
			}
			control.wait(t, "armed")
			heldSignal := control.wait(t, "held").Signal
			held, err := issue2564H2Read(t.Context(), rt, seed.RunID)
			if err != nil {
				t.Fatal(err)
			}
			event := held.Events[heldSignal.EventID]
			timer := held.Timers[event.Occurrence.Activation.ActivationID]
			target = held.Hubs["h05"]
			if event.ID == "" || event.Outcome != "" || timer.Entity != target.Entity || timer.Instance != target.Instance || timer.Ref.DeclarationKey != keys["s2"] || timer.Status != "fired" || timer.Fired.IsZero() || target.Stage != "s2" || len(target.History) != 1 || target.History[0].From != "s1" || target.History[0].To != "s2" {
				t.Fatalf("controlled cut is not exact acknowledged s2 then published occurrence: event=%+v timer=%+v target=%+v", event, timer, target)
			}
			t.Logf("H2_EXACT_HELD hub=h05 run=%s entity=%s instance=%s occurrence=%s activation=%s prior_transition=%s", seed.RunID, target.Entity, target.Instance, event.ID, timer.ID, target.History[0].TriggerEventID)
			accepted := issue2564H2Bumps(t, rt, seed, 900, 40)
			issue2564H2AdmissionEvidence(t, first.output.String(), accepted)
			rt.waitDeliveries(t, seed.RunID)
			settled, err := issue2564H2Read(t.Context(), rt, seed.RunID)
			if err != nil {
				t.Fatal(err)
			}
			target = settled.Hubs["h05"]
			if target.Stage != "s2" || target.Count != 150 || target.C1 != 0 || target.C2 != 150 || len(target.Cuts) != len(held.Hubs["h05"].Cuts) || settled.Events[event.ID].Outcome != "" || !reflect.DeepEqual(settled.Timers[timer.ID], timer) {
				t.Fatalf("held occurrence did not preserve a single-stage trace: %+v", target)
			}
			for id, hub := range settled.Hubs {
				if hub.Count != 150 || hub.C1+hub.C2 != 150 {
					t.Fatalf("900 exact effects not settled: hub=%s %+v", id, hub)
				}
				if id != "h05" && len(hub.Cuts) <= len(held.Hubs[id].Cuts) {
					t.Fatalf("targeted gate also stalled sibling timers: hub=%s", id)
				}
			}
			issue2564H2Deliveries(t, rt, seed.RunID, accepted, 906)
			issue2353H2ObserveCommitStages(t, rt, seed.RunID, accepted)
			issue2353H2CaptureOracle(t, issue2353H2OracleRequest{Process: request, RunID: seed.RunID, Accepted: accepted})
			if err := control.input.Encode(issue2353H2Command{Action: "release"}); err != nil {
				t.Fatal(err)
			}
			joined := control.wait(t, "joined")
			if joined.Signal.EventID != event.ID {
				t.Fatal("joined another occurrence")
			}
			t.Logf("H2_HELD_RELEASED occurrence=%s original_callback_error=%s", event.ID, joined.Error)
			released := issue2564H2WaitAccounting(t, rt, seed.RunID, false, keys)
			if released.Events[event.ID].Outcome != "success" || !reflect.DeepEqual(released.Timers[timer.ID], timer) {
				t.Fatal("held occurrence did not recover exactly once")
			}
			issue2353H2PreserveCounters(t, settled, released)
			if err := control.input.Encode(issue2353H2Command{Action: "crash"}); err != nil {
				t.Fatal(err)
			}
			crash := control.wait(t, "crash_cut").Signal
			interrupted, err := issue2564H2Read(t.Context(), rt, seed.RunID)
			if err != nil || interrupted.Events[crash.EventID].ID == "" || interrupted.Events[crash.EventID].Outcome != "" {
				t.Fatalf("original restart cut not published-but-unadvanced: %+v %v", crash, err)
			}
			crashEvent := interrupted.Events[crash.EventID]
			crashTimer := interrupted.Timers[crashEvent.Occurrence.Activation.ActivationID]
			if crashTimer.Status != "fired" || crashTimer.Fired.IsZero() {
				t.Fatal("restart cut lost persisted fired timer")
			}
			for _, hub := range interrupted.Hubs {
				if _, advanced := hub.Cuts[crash.EventID]; advanced {
					t.Fatal("restart cut already advanced")
				}
			}
			if err := first.kill(); err != nil || first.waitError() == nil {
				t.Fatalf("abrupt process death failed: kill=%v wait=%v", err, first.waitError())
			}
			second, reopened := start(false)
			recovered := issue2564H2WaitAccounting(t, reopened, seed.RunID, false, keys)
			if recovered.Events[crash.EventID].Task != crashEvent.Task || !reflect.DeepEqual(recovered.Timers[crashTimer.ID], crashTimer) {
				t.Fatal("restart reset/reminted the exact committed occurrence")
			}
			issue2353H2PreserveCounters(t, interrupted, recovered)
			issue2353H2ObserveCommitStages(t, reopened, seed.RunID, accepted)
			issue2353H2Close(t, reopened, seed)
			closed, err := issue2564H2Read(t.Context(), reopened, seed.RunID)
			if err != nil {
				t.Fatal(err)
			}
			issue2353H2PreserveCounters(t, recovered, closed)
			for _, hub := range closed.Hubs {
				if hub.Stage != "closed" {
					t.Fatal("explicit cleanup did not close every hub")
				}
			}
			for _, timer := range closed.Timers {
				if timer.Status == "active" {
					t.Fatal("explicit cleanup retained an active timer")
				}
			}
			for id, old := range recovered.Events {
				if !reflect.DeepEqual(old, closed.Events[id]) {
					t.Fatal("explicit cleanup changed an acknowledged timer occurrence")
				}
			}
			issue2564H2Deliveries(t, reopened, seed.RunID, accepted, 912)
			if err := second.stop(); err != nil {
				t.Fatalf("explicit-close shutdown: %v\n%s", err, second.output.String())
			}
			t.Logf("H2_CONTROLLED_PROOF accepted=900 target=h05 c1=0 c2=150 exact_held_occurrence=%s unrelated_hubs_progress=true release_transition_and_successor=true SIGKILL_recovery=true explicit_close=6 cleanup_joined=true", event.ID)
		})
	}
}
