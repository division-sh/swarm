package pipeline_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
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
				trigger := p.publish(t, "batch.ready", map[string]any{})
				if got, present := p.instance(t).Fields["items"]; after == "removed" {
					if present {
						t.Fatalf("clear left business source: %v", got)
					}
				} else if !reflect.DeepEqual(got, final) {
					t.Fatalf("final state=%v want=%v", got, final)
				}
				r := p.compositeReceipt(t, trigger, "writer", original)
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
			})
		}
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
