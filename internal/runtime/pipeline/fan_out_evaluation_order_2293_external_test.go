package pipeline_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func Test2293SelectedFanOutRetainsEvaluationSourceAcrossRuleWriteBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, after := range []string{"equal_length", "shorter", "longer", "removed"} {
			t.Run(backend.name+"/"+after, func(t *testing.T) {
				original := make([]any, fanoutobligation.MaxChunkSize+1)
				for i := range original {
					original[i] = fmt.Sprintf("old-%03d", i)
				}
				final := append([]any(nil), original...)
				final[0] = "new"
				if after == "shorter" {
					final = final[:1]
				}
				if after == "longer" {
					final = append(final, "extra")
				}
				files := a2MapFanOutFiles(false)
				files["events.yaml"] += "\nhistory.cut:\n"
				files["schema.yaml"] = strings.Replace(files["schema.yaml"], "    - seed\n", "    - seed\n    - history.cut\n", 1)
				files["entities.yaml"] = "work:\n  items:\n    type: '[text]?'\n    initial: " + a2OrderYAMLJSON(t, original) + "\n"
				files["nodes.yaml"] = `writer:
  execution_type: system_node
  event_handlers:
    seed: {}
    batch.ready:
      rules:
        - when: has(entity.items)
          fan_out:
            items_from: entity.items
            as: key
            emit:
              event: item.ready
              fields:
                member_id: key
                index: fan_out.index
                count: fan_out.count
                result: >-
                  [7]
`
				if after == "removed" {
					files["nodes.yaml"] += "          data_accumulation:\n            writes:\n              - op: clear\n                target: entity.items\n"
				} else {
					files["nodes.yaml"] += "          data_accumulation:\n            writes:\n              - target_field: items\n                value: " + a2OrderYAMLJSON(t, final) + "\n"
				}
				p := newA2MapFanOutExecutionFromFiles(t, backend.open(t), false, files)
				reader := p.selected.events.(interface {
					InspectRunMutationDrift(context.Context, string) (mutationlog.DriftReport, error)
				})
				before, err := reader.InspectRunMutationDrift(p.ctx, p.runID)
				if err != nil || len(before.Rows) != 0 {
					t.Fatalf("baseline mutation drift: %+v error=%v", before, err)
				}
				trigger := p.publish(t, "batch.ready", map[string]any{})
				afterReport, err := reader.InspectRunMutationDrift(p.ctx, p.runID)
				if err != nil || len(afterReport.Rows) != 0 {
					t.Errorf("fan-out introduced mutation drift: %+v error=%v", afterReport, err)
				}
				if got, present := p.instance(t).Fields["items"]; after == "removed" {
					if present {
						t.Fatalf("clear left business source: %v", got)
					}
				} else if !reflect.DeepEqual(got, final) {
					t.Fatalf("final state=%v want=%v", got, final)
				}
				r := p.compositeReceipt(t, trigger, "writer", original)
				assert2293ForkKeepsBusinessStateAndFrozenSource(t, p, r.source, p.instance(t).Fields)
				handoff := p.handoff(t)
				if _, err := p.pc.ServeFanOutCandidate(p.ctx, handoff.owner, r.key); err != nil {
					t.Fatal(err)
				}
				first := p.outputs(t)
				if len(first) == 0 || len(first) >= len(original) {
					t.Fatalf("expected nonzero bounded prefix: %d/%d", len(first), len(original))
				}
				p.restart(t)
				handoff = p.handoff(t)
				if _, err := p.pc.ServeFanOutCandidate(p.ctx, handoff.owner, r.key); err != nil {
					t.Fatal(err)
				}
				outputs := p.outputs(t)
				if len(outputs) != len(original) {
					t.Fatalf("resumed output count=%d want=%d", len(outputs), len(original))
				}
				for i, output := range outputs {
					if output.Ordinal != i || output.Payload.Key != original[i] || output.Payload.Index != i || output.Payload.Count != len(original) || !reflect.DeepEqual(output.Payload.Result, []int64{7}) {
						t.Fatalf("ordinal %d source/output changed: %+v", i, output)
					}
					if i < len(first) && output.EventID != first[i].EventID {
						t.Fatal("restart republished a committed prefix")
					}
				}
				p.assertProgress(t, len(original), len(original), "closed", r.source.MutationID)
				settled, err := reader.InspectRunMutationDrift(p.ctx, p.runID)
				if err != nil || len(settled.Rows) != 0 {
					t.Fatalf("settled fan-out introduced mutation drift: %+v error=%v", settled, err)
				}
			})
		}
	}
}

func assert2293ForkKeepsBusinessStateAndFrozenSource(t *testing.T, p *a2MapFanOutExecution, source fanoutobligation.SourceRef, fields map[string]any) {
	t.Helper()
	// A trigger's publication revision precedes its handler. This real no-route
	// publication selects a committed cut after that handler's state and intent.
	cut := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "history.cut", "operator", "", []byte(`{}`), 0,
		p.runID, events.EnvelopeForEntityID(events.EventEnvelope{}, p.runID), eventtest.RootRoutingSource(p.runID), time.Now().UTC())
	if err := p.bus.PublishAcknowledged(p.ctx, cut); err != nil {
		t.Fatal(err)
	}
	eventID := cut.ID()
	owner := p.selected.events.(interface {
		PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
		MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
		InspectRunMutationDrift(context.Context, string) (mutationlog.DriftReport, error)
	})
	plan, err := owner.PlanRunFork(p.ctx, runfork.RunForkPlanRequest{SourceRunID: p.runID, At: eventID})
	if err != nil || len(plan.Entities) != 1 || !reflect.DeepEqual(plan.Entities[0].Fields, fields) || len(plan.FanOutObligations) != 1 || plan.FanOutObligations[0].Intent.Source != source {
		t.Fatalf("fork plan confused final business state and frozen source: entities=%+v fan_out=%+v blockers=%+v error=%v", plan.Entities, plan.FanOutObligations, plan.UnsupportedBlockers, err)
	}
	carriage, err := semanticview.CompileOriginalLoopCarriage(p.source)
	if err != nil {
		t.Fatal(err)
	}
	child, err := owner.MaterializeRunFork(p.ctx, runfork.RunForkMaterializeRequest{
		SourceRunID: p.runID, At: eventID, SourceArtifactFact: p.fact, OriginalLoopCarriage: carriage,
	})
	if err != nil || child.MaterializedEntityCount != 1 || child.MaterializedFanOutCount != 1 || child.ForkRunID == p.runID || child.ForkRunStatus != runfork.RunForkMaterializedStatus {
		t.Fatalf("materialize real fork: child=%+v err=%v", child, err)
	}
	ctx := correlation.WithRunID(p.ctx, child.ForkRunID)
	state, found, err := p.pc.Load(ctx, testRunScopedWorkflowInstanceForRun(child.ForkRunID, child.ForkRunID))
	if err != nil || !found || !reflect.DeepEqual(state.Fields, fields) {
		t.Fatalf("materialized fork business state: found=%v fields=%+v want=%+v err=%v", found, state.Fields, fields, err)
	}
	report, err := owner.InspectRunMutationDrift(ctx, child.ForkRunID)
	if err != nil || len(report.Rows) != 0 {
		t.Fatalf("materialized fork mutation drift: %+v err=%v", report, err)
	}
	stored, err := storetest.ReadFanOutRunProgressStorage(ctx, p.selected.events, child.ForkRunID)
	if err != nil || len(stored.Intents) != 1 || stored.Intents[0].SourceMutationID != source.MutationID {
		t.Fatalf("fork lost the original immutable source: %+v err=%v", stored, err)
	}
	intent := plan.FanOutObligations[0].Intent
	node := externalPipelineSourceNode(t, p.source, ".", "writer")
	rows, err := storetest.ReadFanOutTriggeredIntentStorage(ctx, p.selected.events, child.ForkRunID, intent.Request.Capsule.Lineage.ParentEventID, node, intent.Request.Key.ElementRef)
	if err != nil || len(rows) != 1 || rows[0].Source != source {
		t.Fatalf("fork remapped retained source ownership: %+v err=%v", rows, err)
	}
}

func Test2293HandlerAndSelectedEntityFanOutKeepIndependentHistoryBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			files := a2MapFanOutFiles(false)
			files["entities.yaml"] = "work:\n  items: {type: '[text]?', initial: [first]}\n  other_items: {type: '[text]', initial: [second]}\n"
			files["nodes.yaml"] = `writer:
  execution_type: system_node
  event_handlers:
    seed: {}
    batch.ready:
      guard: {id: source_present, check: has(entity.items), on_fail: reject}
      fan_out:
        items_from: entity.items
        as: key
        emit:
          event: item.ready
          fields:
            member_id: key
            index: fan_out.index
            count: fan_out.count
            result: >-
              [7]
      rules:
        - when: has(entity.items)
          fan_out:
            items_from: entity.other_items
            as: key
            emit:
              event: item.ready
              fields:
                member_id: key
                index: fan_out.index
                count: fan_out.count
                result: >-
                  [7]
          data_accumulation:
            writes:
              - op: clear
                target: entity.items
              - target_field: other_items
                value: ["final"]
`
			p := newA2MapFanOutExecutionFromFiles(t, backend.open(t), false, files)
			reader := p.selected.events.(interface {
				InspectRunMutationDrift(context.Context, string) (mutationlog.DriftReport, error)
			})
			baseline, err := reader.InspectRunMutationDrift(p.ctx, p.runID)
			if err != nil || len(baseline.Rows) != 0 {
				t.Fatalf("composed baseline drift: %+v err=%v", baseline, err)
			}
			trigger := p.publish(t, "batch.ready", map[string]any{})
			node := externalPipelineSourceNode(t, p.source, ".", "writer")
			plans := p.source.FanOutPlansForHandler(node, "batch.ready")
			if len(plans) != 2 {
				t.Fatalf("composed entity sites=%d", len(plans))
			}
			sources := map[string]bool{}
			for _, plan := range plans {
				rows, err := storetest.ReadFanOutTriggeredIntentStorage(p.ctx, p.selected.events, p.runID, trigger.ID(), node, plan.Ref.ElementRef)
				if err != nil || len(rows) != 1 || rows[0].Source.Kind != fanoutobligation.SourceEntityField || rows[0].Cardinality != 1 || rows[0].Source.RunID != p.runID || rows[0].SemanticDigest != plan.Ref.SemanticDigest {
					t.Fatalf("composed source changed: %+v err=%v", rows, err)
				}
				id := rows[0].Source.MutationID
				if id == "" || sources[id] {
					t.Fatal("independent sites reused an observation identity")
				}
				sources[id] = true
			}
			handoff := p.handoff(t)
			if _, err := p.pc.ServeFanOutCandidate(p.ctx, handoff.owner, handoff.key); err != nil {
				t.Fatal(err)
			}
			first := p.outputs(t)
			if len(first) != 1 {
				summary, summaryErr := handoff.owner.FanOutRunSummary(p.ctx, p.runID, time.Now().UTC())
				t.Fatalf("composed prefix=%+v summary=%+v rejection=%+v err=%v", first, summary, summary.SemanticRejectionSample, summaryErr)
			}
			p.restart(t)
			handoff = p.handoff(t)
			if _, err := p.pc.ServeFanOutCandidate(p.ctx, handoff.owner, handoff.key); err != nil {
				t.Fatal(err)
			}
			seen := map[string]bool{}
			outputs := p.outputs(t)
			for _, output := range outputs {
				if seen[output.Payload.Key] || output.Ordinal != 0 || output.Payload.Count != 1 || !reflect.DeepEqual(output.Payload.Result, []int64{7}) {
					t.Fatalf("composed source payload changed: %+v", output)
				}
				seen[output.Payload.Key] = true
				if output.Payload.Key == first[0].Payload.Key && output.EventID != first[0].EventID {
					t.Fatal("restart republished a committed site")
				}
			}
			if len(outputs) != 2 || !seen["first"] || !seen["second"] {
				t.Fatalf("composed source observations were rebound: %+v", outputs)
			}
			if fields := p.instance(t).Fields; len(fields) != 1 || !reflect.DeepEqual(fields["other_items"], []any{"final"}) {
				t.Fatalf("composed source changed final business fields: %+v", fields)
			}
			settled, err := reader.InspectRunMutationDrift(p.ctx, p.runID)
			if err != nil || len(settled.Rows) != 0 {
				t.Fatalf("composed settlement drift: %+v err=%v", settled, err)
			}
		})
	}
}

func a2OrderYAMLJSON(t *testing.T, value any) string {
	t.Helper()
	// Quoted list items are authored text; the list itself is a typed value.
	items := value.([]any)
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = fmt.Sprintf("%q", item)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func Test2293HandlerAndSelectedFanOutCommitDistinctSitesBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			files := a2MapFanOutFiles(false)
			files["nodes.yaml"] = `writer:
  execution_type: system_node
  event_handlers:
    seed: {}
    batch.ready:
      fan_out:
        items_from: payload.items
        as: key
        emit:
          event: item.ready
          fields:
            member_id: key
            index: fan_out.index
            count: fan_out.count
            result: payload.items[?key].orValue([])
      rules:
        - fan_out:
            items_from: payload.items
            as: key
            emit:
              event: item.ready
              fields:
                member_id: key
                index: fan_out.index
                count: fan_out.count
                result: payload.items[?key].orValue([])
`
			p := newA2MapFanOutExecutionFromFiles(t, backend.open(t), false, files)
			trigger := p.publish(t, "batch.ready", map[string]any{"a": []any{7}})
			node := externalPipelineSourceNode(t, p.source, ".", "writer")
			plans := p.source.FanOutPlansForHandler(node, "batch.ready")
			if len(plans) != 2 {
				t.Fatalf("admitted site count=%d", len(plans))
			}
			for _, plan := range plans {
				rows, err := storetest.ReadFanOutTriggeredIntentStorage(p.ctx, p.selected.events, p.runID, trigger.ID(), node, plan.Ref.ElementRef)
				if err != nil || len(rows) != 1 || rows[0].SemanticDigest != plan.Ref.SemanticDigest || rows[0].Cardinality != 1 || rows[0].Source.Kind != fanoutobligation.SourceEventPayloadField || rows[0].Source.EventID != trigger.ID() {
					t.Fatalf("site intent/source changed: %+v err=%v", rows, err)
				}
			}
			for i := 0; i < 2; i++ {
				handoff := p.handoff(t)
				if _, err := p.pc.ServeFanOutCandidate(p.ctx, handoff.owner, handoff.key); err != nil {
					failure, _ := failures.EnvelopeFromError(err)
					t.Fatalf("serve exact composed site: %v; failure=%+v", err, failure)
				}
			}
			outputs := p.outputs(t)
			if len(outputs) != 2 || outputs[0].EventID == outputs[1].EventID {
				t.Fatalf("distinct sites lost an ordinal: %+v", outputs)
			}
			for _, output := range outputs {
				if output.Ordinal != 0 || output.Payload.Key != "a" || output.Payload.Count != 1 || !reflect.DeepEqual(output.Payload.Result, []int64{7}) {
					t.Fatalf("site source changed: %+v", output)
				}
			}
		})
	}
}
