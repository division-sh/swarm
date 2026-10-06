package runstart

import (
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
)

func TestReviewer2566NestedFeedReceiverMustEnterFiniteClosure(t *testing.T) {
	source := finiteTestSource(t, map[string]string{
		"schema.yaml":            "stages: {done: {final: true}}\nconnect:\n  - {event: work.requested, from: producer, to: worker, resolution: create}\n",
		"producer/schema.yaml":   "instance: producer_id\nstages: {done: {final: true}}\npins:\n  outputs: [work.requested]\n",
		"producer/entities.yaml": "Producer:\n  producer_id: {type: text, _unused_reason: dormant identity}\n",
		"producer/events.yaml":   "work.requested:\n  worker_id: text\n",
		"worker/schema.yaml":     "instance: worker_id\nstages: {active: {}}\npins:\n  inputs: [work.requested]\n",
		"worker/entities.yaml":   "Worker:\n  worker_id: {type: text, _unused_reason: constructor identity}\n",
		"worker/nodes.yaml":      "worker:\n  execution_type: system_node\n  event_handlers:\n    work.requested: {}\n",
	})
	feed, err := events.NewDeploymentFeedRoutingSource("producer")
	if err != nil {
		t.Fatal(err)
	}
	selected, err := pinrouting.AdmitDeploymentFeedDeclaration(source, events.EventType("producer/work.requested"), feed)
	if err != nil {
		t.Fatal(err)
	}
	graph := pinrouting.CompileConnectGraph(source)
	if len(graph.Plans()) != 1 || len(graph.Issues()) != 0 {
		t.Fatalf("expected admitted feed route: %v", graph.Issues())
	}
	if err := ValidateFinite(source, []pinrouting.SourceEvent{selected}); err == nil {
		t.Fatal("finite admission accepted an admissible nested feed whose connected worker has no final stage")
	}
}

func TestFiniteStartSelectedFeedsUseExactRoutesAndRecursiveConstructors(t *testing.T) {
	for _, offender := range []string{"worker", "worker/support/leaf", "none"} {
		t.Run(offender, func(t *testing.T) {
			worker, leaf := "stages: {done: {final: true}}\n", "stages: {done: {final: true}}\n"
			if offender == "worker" {
				worker = "stages: {active: {}}\n"
			}
			if offender == "worker/support/leaf" {
				leaf = "stages: {active: {}}\n"
			}
			source := finiteTestSource(t, map[string]string{
				"schema.yaml": "stages: {done: {final: true}}\nconnect:\n  - {event: work.requested, from: producer, to: worker, resolution: create}\n  - {event: work.safe, from: producer, to: safe, resolution: create}\n",
				// A feed does not instantiate its keyed source, even a service.
				"producer/schema.yaml":            "instance: producer_id\nstages: {active: {}}\npins:\n  outputs: [work.requested, work.safe]\n",
				"producer/entities.yaml":          "Producer:\n  producer_id: {type: text, _unused_reason: dormant identity}\n",
				"producer/events.yaml":            "work.requested:\n  worker_id: text\nwork.safe:\n  worker_id: text\n",
				"worker/schema.yaml":              "instance: worker_id\n" + worker + "pins:\n  inputs: [work.requested]\n",
				"worker/entities.yaml":            "Worker:\n  worker_id: {type: text, _unused_reason: constructor identity}\n",
				"worker/nodes.yaml":               "worker:\n  execution_type: system_node\n  event_handlers:\n    work.requested: {}\n",
				"worker/support/schema.yaml":      "stages: {done: {final: true}}\n",
				"worker/support/leaf/schema.yaml": leaf,
				"safe/schema.yaml":                "instance: worker_id\nstages: {done: {final: true}}\npins:\n  inputs: [work.safe]\n",
				"safe/entities.yaml":              "Worker:\n  worker_id: {type: text, _unused_reason: constructor identity}\n",
				"safe/nodes.yaml":                 "worker:\n  execution_type: system_node\n  event_handlers:\n    work.safe: {}\n",
			})
			routing, err := events.NewDeploymentFeedRoutingSource("producer")
			if err != nil {
				t.Fatal(err)
			}
			var feeds []pinrouting.SourceEvent
			for _, name := range []string{"producer/work.safe", "producer/work.requested"} {
				feed, err := pinrouting.AdmitDeploymentFeedDeclaration(source, events.EventType(name), routing)
				if err != nil {
					t.Fatal(err)
				}
				feeds = append(feeds, feed)
			}
			if err := ValidateFinite(source, feeds[:1]); err != nil {
				t.Fatalf("unselected source or route entered the closure: %v", err)
			}
			err = ValidateFinite(source, feeds)
			if offender == "none" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var refusal *FiniteStartError
			if !errors.As(err, &refusal) || refusal.FlowID != offender {
				t.Fatalf("selected feed closure missed %s: %v", offender, err)
			}
		})
	}
}
