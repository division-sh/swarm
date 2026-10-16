package serveapp

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestH2CancellationCaptureExactCommittedCut(t *testing.T) {
	run, entity, instance := uuid.NewString(), uuid.NewString(), "ti-h2-capture"
	old := timeridentity.WorkflowTimerActivationRef{ActivationID: uuid.NewString(), DeclarationKey: "stage:hub:hub.h2.s2_to_s1", DeclarationRevision: "fixture", Cause: timeridentity.WorkflowTimerActivationCauseTransition}
	next := old
	next.ActivationID = uuid.NewString()
	due := time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)
	occurrence := timeridentity.WorkflowTimerOccurrenceRef{Activation: old, DueAt: due}
	event := timeridentity.WorkflowTimerOccurrenceEventID(occurrence)
	row := func(family, key string, revision int64, present bool, value any) storetest.H2CommittedFactEvidence {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return storetest.H2CommittedFactEvidence{Family: family, Key: key, Revision: revision, Present: present, Fact: raw}
	}
	timer := func(ref timeridentity.WorkflowTimerActivationRef, status string, fired any) map[string]any {
		return map[string]any{"timer_id": ref.ActivationID, "timer_name": ref.TaskID(), "run_id": run, "entity_id": entity, "flow_scope_key": "hub", "flow_instance": instance, "status": status, "created_at": due.Add(-time.Second), "fire_at": due, "fired_at": fired}
	}
	header := func(stage, from, to, trigger string) map[string]any {
		return map[string]any{"entity_id": entity, "flow_template": "hub", "flow_instance": instance, "current_state": stage, "flow_config": map[string]any{"transition_history": []h2CancelTransition{{TransitionID: "cut", From: from, To: to, TriggerEventID: trigger}}}}
	}
	facts := []storetest.H2CommittedFactEvidence{
		row("timers", old.ActivationID, 1, true, timer(old, "fired", due)),
		row("entity_metadata", entity, 1, true, header("s2", "s1", "s2", "entry-old")),
		row("timers", next.ActivationID, 2, true, timer(next, "active", nil)),
		row("entity_metadata", entity, 2, true, header("s2", "s1", "s2", "entry-new")),
		row("events", event, 2, true, map[string]any{"event_id": event, "run_id": run, "event_name": "platform.stage_timer", "task_id": occurrence.TaskID()}),
		row("timers", next.ActivationID, 3, true, timer(next, "cancelled", nil)),
		row("entity_metadata", entity, 3, true, header("s1", "s2", "s1", event)),
		// Later live state must not become cancellation provenance, and a repeated
		// cancelled copy is not another first cancellation.
		row("entity_metadata", entity, 4, true, header("closed", "s1", "closed", "later-close")),
		row("timers", next.ActivationID, 4, true, timer(next, "cancelled", nil)),
		row("timers", old.ActivationID, 4, false, nil),
	}
	evidence := storetest.H2CommittedHistoryEvidence{RunID: run, Complete: true, Facts: facts}
	report := h2CancellationCuts(t.Context(), run, evidence)
	if !report.Complete || len(report.Cancellations) != 1 {
		t.Fatalf("exact cut: %+v", report)
	}
	cut := report.Cancellations[0]
	if cut.Revision != 3 || cut.Predecessor != 2 || cut.HeaderBefore.Stage != "s2" || cut.HeaderAfter.Stage != "s1" || cut.TriggerID != event || cut.Occurrence == nil || cut.Site == "UNKNOWN" || len(cut.SiblingsAfter) != 2 {
		t.Fatalf("cut borrowed later state: %+v", cut)
	}
	for _, sibling := range cut.SiblingsAfter {
		if sibling.Timer.ID == old.ActivationID && (sibling.Timer.Status != "fired" || sibling.CreationRevision != 1) {
			t.Fatal("fired sibling lost its exact creation cut")
		}
		if sibling.Timer.ID == next.ActivationID && sibling.CreationRevision != 2 {
			t.Fatal("different stage visit was merged with fired sibling")
		}
	}
	t.Run("deleted_trigger", func(t *testing.T) {
		copy := evidence
		copy.Facts = append(append([]storetest.H2CommittedFactEvidence(nil), facts...), row("events", event, 3, false, nil))
		got := h2CancellationCuts(t.Context(), run, copy)
		if got.Complete || got.Cancellations[0].Occurrence != nil || got.Cancellations[0].Site != "UNKNOWN" {
			t.Fatal("tombstone resurrected older trigger")
		}
	})
	t.Run("missing_predecessor", func(t *testing.T) {
		copy := evidence
		copy.Facts = append([]storetest.H2CommittedFactEvidence(nil), facts...)
		copy.Facts[2].Present = false
		got := h2CancellationCuts(t.Context(), run, copy)
		if got.Complete || len(got.Cancellations[0].Incomplete) == 0 {
			t.Fatal("missing predecessor claimed complete provenance")
		}
	})
	t.Run("unknown_site", func(t *testing.T) {
		copy := evidence
		copy.Facts = append([]storetest.H2CommittedFactEvidence(nil), facts...)
		copy.Facts[6] = row("entity_metadata", entity, 3, true, header("s2", "s2", "s2", event))
		if got := h2CancellationCuts(t.Context(), run, copy); got.Cancellations[0].Site != "UNKNOWN" {
			t.Fatal("co-committed header was treated as observed call frame")
		}
	})
	t.Run("wrong_run", func(t *testing.T) {
		if got := h2CancellationCuts(t.Context(), uuid.NewString(), evidence); got.Complete || len(got.Cancellations) != 0 {
			t.Fatal("foreign run borrowed provenance")
		}
	})
	t.Run("wrong_timer_identity", func(t *testing.T) {
		copy := evidence
		copy.Facts = append([]storetest.H2CommittedFactEvidence(nil), facts...)
		bad := timer(next, "cancelled", nil)
		bad["entity_id"] = ""
		copy.Facts[5] = row("timers", next.ActivationID, 3, true, bad)
		if got := h2CancellationCuts(t.Context(), run, copy); got.Complete || len(got.Errors) == 0 {
			t.Fatal("invalid identity claimed completeness")
		}
	})
	t.Run("truncated", func(t *testing.T) {
		copy := evidence
		copy.Truncated = true
		if got := h2CancellationCuts(t.Context(), run, copy); got.Complete {
			t.Fatal("truncated read claimed complete")
		}
	})
	t.Run("deleted_header", func(t *testing.T) {
		copy := evidence
		copy.Facts = append([]storetest.H2CommittedFactEvidence(nil), facts...)
		copy.Facts[6] = row("entity_metadata", entity, 3, false, nil)
		got := h2CancellationCuts(t.Context(), run, copy)
		if got.Complete || got.Cancellations[0].HeaderAfter.Present || got.Cancellations[0].Site != "UNKNOWN" {
			t.Fatal("deleted header borrowed its older transition")
		}
	})
	t.Run("wrong_instance", func(t *testing.T) {
		copy := evidence
		copy.Facts = append([]storetest.H2CommittedFactEvidence(nil), facts...)
		foreign := header("s1", "s2", "s1", event)
		foreign["flow_instance"] = "foreign"
		copy.Facts[6] = row("entity_metadata", entity, 3, true, foreign)
		got := h2CancellationCuts(t.Context(), run, copy)
		if got.Complete || got.Cancellations[0].Site != "UNKNOWN" {
			t.Fatal("foreign header borrowed trigger authority")
		}
	})
	t.Run("cancelled_reconstruction", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if got := h2CancellationCuts(ctx, run, evidence); got.Complete || len(got.Cancellations) != 0 {
			t.Fatal("cancelled reconstruction claimed completeness")
		}
	})
}

func TestH2CancellationCaptureNativeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			start, _, _ := issue2564H2Harness(t, backend, issue2564H2Source(t))
			process, rt := start(false)
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "hub.start", "bundle_hash": rt.BundleHash, "payload": map[string]any{"hub_id": "h01"}, "idempotency_key": "capture-start"})
			rt.waitDeliveries(t, seed.RunID)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			for {
				snapshot, err := issue2564H2Read(ctx, rt, seed.RunID)
				if err != nil {
					t.Fatal(err)
				}
				if snapshot.Hubs["h01"].Stage == "s2" {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("native transition did not settle")
				case <-time.After(10 * time.Millisecond):
				}
			}
			close := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "hub.close", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"hub_id": "h01"}, "idempotency_key": "capture-close"})
			rt.waitDeliveries(t, seed.RunID)
			evidence, err := storetest.ObserveH2CommittedHistory(t.Context(), rt.selected, seed.RunID)
			if err != nil || !evidence.Complete {
				t.Fatalf("native history: %v", err)
			}
			report := h2CancellationCuts(t.Context(), seed.RunID, evidence)
			if !report.Complete || len(report.Cancellations) == 0 {
				t.Fatalf("native cancellation not reconstructed: %s", h2CaptureJSON(report))
			}
			derived := false
			for _, cut := range report.Cancellations {
				if cut.TriggerID != close.EventID || cut.TriggerType != "hub.close" || cut.HeaderAfter.Stage != "closed" || cut.After.Fired != nil {
					t.Fatalf("native close cut: %s", h2CaptureJSON(cut))
				}
				derived = derived || cut.Site != "UNKNOWN"
			}
			if !derived {
				t.Fatalf("real transition-created cancellation lacks derived cut: %s", h2CaptureJSON(report))
			}
			if _, err := json.Marshal(report); err != nil {
				t.Fatalf("report cannot enter retained JSON log: %v", err)
			}
			before, err := issue2564H2Read(t.Context(), rt, seed.RunID)
			if err != nil {
				t.Fatal(err)
			}
			issue2564H2CaptureCancellation(t, rt, seed.RunID, true, before)
			after, err := issue2564H2Read(t.Context(), rt, seed.RunID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("failure capture changed state")
			}
			foreign, err := storetest.ObserveH2CommittedHistory(t.Context(), rt.selected, uuid.NewString())
			if err != nil || !foreign.Complete || len(foreign.Facts) != 0 {
				t.Fatal("foreign run borrowed history")
			}
			cancelled, stop := context.WithCancel(t.Context())
			stop()
			if got, err := storetest.ObserveH2CommittedHistory(cancelled, rt.selected, seed.RunID); err == nil || got.Complete {
				t.Fatal("cancelled read claimed complete")
			}
			if err := process.stop(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
