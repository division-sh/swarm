package bootverify

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRun_WarnsForAccumulateAllWithoutBoundedEscape(t *testing.T) {
	root := writeAccumulatorSafetyFixture(t, accumulatorSafetyFixtureOptions{
		eventSource: "external",
		completion:  "all",
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), checkIDAccumulateAllBoundedEscape, "without a bounded timeout escape") {
		t.Fatalf("expected %s warning, got %#v", checkIDAccumulateAllBoundedEscape, report.Warnings())
	}
}

func TestRun_WarnsForDefaultAccumulateWithoutBoundedEscape(t *testing.T) {
	root := writeAccumulatorSafetyFixture(t, accumulatorSafetyFixtureOptions{
		eventSource:    "external",
		omitCompletion: true,
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), checkIDAccumulateAllBoundedEscape, "default/all") {
		t.Fatalf("expected %s warning for omitted completion, got %#v", checkIDAccumulateAllBoundedEscape, report.Warnings())
	}
}

func TestRun_WarnsForAccumulateAllOnTimeoutWithoutSchedulableTimeout(t *testing.T) {
	root := writeAccumulatorSafetyFixture(t, accumulatorSafetyFixtureOptions{
		eventSource: "external",
		completion:  "all",
		onTimeout:   true,
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), checkIDAccumulateAllBoundedEscape, "without a bounded timeout escape") {
		t.Fatalf("expected %s warning for bare on_timeout, got %#v", checkIDAccumulateAllBoundedEscape, report.Warnings())
	}
}

func TestRun_DoesNotWarnForAccumulateAllWithSchedulableOnTimeout(t *testing.T) {
	root := writeAccumulatorSafetyFixture(t, accumulatorSafetyFixtureOptions{
		eventSource: "external",
		completion:  "all",
		onTimeout:   true,
		timeoutMS:   5000,
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), checkIDAccumulateAllBoundedEscape, "") {
		t.Fatalf("unexpected %s warning with schedulable on_timeout: %#v", checkIDAccumulateAllBoundedEscape, report.Warnings())
	}
	if reportContains(report.Errors(), checkIDAccumulatorInputProducer, "") {
		t.Fatalf("unexpected %s error with external source: %#v", checkIDAccumulatorInputProducer, report.Errors())
	}
}

func TestRun_FailsClosedForAccumulateTimeoutWithoutTimeoutMS(t *testing.T) {
	root := writeAccumulatorSafetyFixture(t, accumulatorSafetyFixtureOptions{
		eventSource: "external",
		completion:  "timeout",
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), checkIDAccumulatorTimeoutRequiresTimeout, "without positive timeout_ms") {
		t.Fatalf("expected %s hard invalidity, got %#v", checkIDAccumulatorTimeoutRequiresTimeout, report.Errors())
	}
	if reportContains(report.Warnings(), checkIDAccumulateAllBoundedEscape, "") {
		t.Fatalf("unexpected %s warning for timeout completion: %#v", checkIDAccumulateAllBoundedEscape, report.Warnings())
	}
	if reportContains(report.Errors(), checkIDAccumulatorInputProducer, "") {
		t.Fatalf("unexpected %s error with external source: %#v", checkIDAccumulatorInputProducer, report.Errors())
	}
}

func TestRun_DoesNotWarnForAccumulateTimeoutWithSchedulableTimeout(t *testing.T) {
	root := writeAccumulatorSafetyFixture(t, accumulatorSafetyFixtureOptions{
		eventSource: "external",
		completion:  "timeout",
		timeoutMS:   5000,
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), checkIDAccumulateAllBoundedEscape, "") {
		t.Fatalf("unexpected %s warning for timeout completion: %#v", checkIDAccumulateAllBoundedEscape, report.Warnings())
	}
	if reportContains(report.Errors(), checkIDAccumulatorInputProducer, "") {
		t.Fatalf("unexpected %s error with external source: %#v", checkIDAccumulatorInputProducer, report.Errors())
	}
}

func TestRun_FailsClosedForAccumulatorInputWithoutProducerPath(t *testing.T) {
	root := writeAccumulatorSafetyFixture(t, accumulatorSafetyFixtureOptions{
		completion: "timeout",
		timeoutMS:  5000,
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), checkIDAccumulatorInputProducer, "no accepted producer/source path") {
		t.Fatalf("expected %s error, got %#v", checkIDAccumulatorInputProducer, report.Errors())
	}
	if !reportContains(report.Errors(), checkIDAccumulatorInputProducer, "Parent connect: not found") ||
		!reportContains(report.Errors(), checkIDAccumulatorInputProducer, "Same-flow node handler emits: not found") {
		t.Fatalf("expected producer path audit details, got %#v", report.Errors())
	}
}

func TestRun_DoesNotErrorForAccumulatorAcceptedProducerPaths(t *testing.T) {
	cases := []struct {
		name string
		root func(*testing.T) string
	}{
		{
			name: "external source metadata",
			root: func(t *testing.T) string {
				return writeAccumulatorSafetyFixture(t, accumulatorSafetyFixtureOptions{
					eventSource: "external",
					completion:  "timeout",
					timeoutMS:   5000,
				})
			},
		},
		{
			name: "same-flow node handler emit",
			root: func(t *testing.T) string {
				return writeAccumulatorSafetyFixture(t, accumulatorSafetyFixtureOptions{
					sameFlowNodeProducer: true,
					completion:           "timeout",
					timeoutMS:            5000,
				})
			},
		},
		{
			name: "same-flow node handler wildcard emit",
			root: func(t *testing.T) string {
				return writeAccumulatorWildcardSameFlowNodeFixture(t)
			},
		},
		{
			name: "same-flow agent emit_events",
			root: func(t *testing.T) string {
				return writeAccumulatorSafetyFixture(t, accumulatorSafetyFixtureOptions{
					sameFlowAgentProducer: true,
					completion:            "timeout",
					timeoutMS:             5000,
				})
			},
		},
		{
			name: "same-flow agent wildcard emit_events",
			root: func(t *testing.T) string {
				return writeAccumulatorWildcardSameFlowAgentFixture(t)
			},
		},
		{
			name: "same-flow timer declaration",
			root: func(t *testing.T) string {
				return writeAccumulatorSafetyFixture(t, accumulatorSafetyFixtureOptions{
					sameFlowTimer: true,
					completion:    "timeout",
					timeoutMS:     5000,
				})
			},
		},
		{
			name: "platform event catalog",
			root: func(t *testing.T) string {
				return writeAccumulatorSafetyFixture(t, accumulatorSafetyFixtureOptions{
					eventType:  "platform.runtime_log",
					completion: "timeout",
					timeoutMS:  5000,
				})
			},
		},
		{
			name: "root agent emit_events",
			root: func(t *testing.T) string {
				return writeAccumulatorRootAgentFixture(t)
			},
		},
		{
			name: "root node handler emit",
			root: func(t *testing.T) string {
				return writeAccumulatorRootNodeFixture(t)
			},
		},
		{
			name: "sibling output pin",
			root: func(t *testing.T) string {
				return writeAccumulatorCrossFlowFixture(t, false)
			},
		},
		{
			name: "parent connect",
			root: func(t *testing.T) string {
				return writeAccumulatorCrossFlowFixture(t, true)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), tc.root(t), runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if reportContains(report.Errors(), checkIDAccumulatorInputProducer, "") {
				t.Fatalf("unexpected %s error: %#v", checkIDAccumulatorInputProducer, report.Errors())
			}
		})
	}
}

func TestRun_AccumulatorProducerProofRejectsProducesAndPlannedStatus(t *testing.T) {
	root := writeAccumulatorSafetyFixture(t, accumulatorSafetyFixtureOptions{
		completion:      "timeout",
		timeoutMS:       5000,
		nodeProduces:    true,
		eventStatusPlan: true,
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), checkIDAccumulatorInputProducer, "no accepted producer/source path") {
		t.Fatalf("expected %s error despite produces/planned status, got %#v", checkIDAccumulatorInputProducer, report.Errors())
	}
}

type accumulatorSafetyFixtureOptions struct {
	eventType             string
	eventSource           string
	eventStatusPlan       bool
	completion            string
	omitCompletion        bool
	timeoutMS             int
	onTimeout             bool
	sameFlowNodeProducer  bool
	sameFlowAgentProducer bool
	sameFlowTimer         bool
	nodeProduces          bool
}

func writeAccumulatorSafetyFixture(t *testing.T, opts accumulatorSafetyFixtureOptions) string {
	t.Helper()
	root := t.TempDir()
	eventType := strings.TrimSpace(opts.eventType)
	if eventType == "" {
		eventType = "item.arrived"
	}
	completion := strings.TrimSpace(opts.completion)
	if completion == "" {
		completion = "all"
	}
	completionLine := ""
	if !opts.omitCompletion {
		completionLine = "        completion: " + completion + "\n"
	}
	producesLine := ""
	if opts.nodeProduces {
		producesLine = "  produces: [" + eventType + "]\n"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: accumulator-safety
version: "1.0.0"
platform: ">=1.6.0"
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: accumulator-safety
initial_state: collecting
terminal_states: [done, partial]
states: [collecting, done, partial]
pins:
  inputs:
    events: [`+eventType+`]
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeAccumulatorEventsFile(t, filepath.Join(root, "events.yaml"), eventType, opts.eventSource, opts.eventStatusPlan)
	writeAccumulatorAgentsFile(t, filepath.Join(root, "agents.yaml"), eventType, opts.sameFlowAgentProducer)

	timeoutLine := ""
	if opts.timeoutMS > 0 {
		timeoutLine = fmt.Sprintf("        timeout_ms: %d\n", opts.timeoutMS)
	}
	onTimeoutBlock := ""
	if opts.onTimeout {
		onTimeoutBlock = `
        on_timeout:
          advances_to: partial
          emit:
            event: collection.partial
`
	}
	producerBlock := ""
	if opts.sameFlowNodeProducer {
		producerBlock = `
producer-node:
  id: producer-node
  execution_type: system_node
  subscribes_to: [producer.start]
  event_handlers:
    producer.start:
      emit:
        event: ` + eventType + `
`
	}
	timerBlock := ""
	if opts.sameFlowTimer {
		timerBlock = `
  timers:
    - id: accumulation
      owner: accumulator-node
      event: ` + eventType + `
      delay: 1m
      start_on: boot
`
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), producerBlock+`
accumulator-node:
  id: accumulator-node
  execution_type: system_node
  subscribes_to: [`+eventType+`]
`+producesLine+timerBlock+`  event_handlers:
    `+eventType+`:
      accumulate:
        expected_from: entity.expected_count
`+completionLine+timeoutLine+onTimeoutBlock+`      advances_to: done
  state_schema:
    fields:
      expected_count: integer
`)
	return root
}

func writeAccumulatorEventsFile(t *testing.T, path, eventType, source string, planned bool) {
	t.Helper()
	sourceBlock := ""
	if strings.TrimSpace(source) != "" || planned {
		sourceBlock += "  swarm:\n"
		if strings.TrimSpace(source) != "" {
			sourceBlock += "    source: " + source + "\n"
		}
		if planned {
			sourceBlock += "    status: planned\n"
		}
	}
	writeBootverifyFixtureFile(t, path, eventType+`:
`+sourceBlock+`  expected_count: integer
producer.start:
  {}
accumulate.timeout:
  {}
collection.done:
  {}
collection.partial:
  {}
`)
}

func writeAccumulatorAgentsFile(t *testing.T, path, eventType string, includeProducer bool) {
	t.Helper()
	if !includeProducer {
		writeBootverifyFixtureFile(t, path, "{}\n")
		return
	}
	writeBootverifyFixtureFile(t, path, `
producer-agent:
  id: producer-agent
  role: producer
  mode: task
  emit_events:
    - `+eventType+`
`)
}

func writeAccumulatorRootAgentFixture(t *testing.T) string {
	t.Helper()
	root := writeAccumulatorSafetyFixture(t, accumulatorSafetyFixtureOptions{
		completion: "timeout",
		timeoutMS:  5000,
	})
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), `
producer-agent:
  id: producer-agent
  role: producer
  mode: task
  emit_events:
    - item.arrived
`)
	return root
}

func writeAccumulatorRootNodeFixture(t *testing.T) string {
	t.Helper()
	return writeAccumulatorSafetyFixture(t, accumulatorSafetyFixtureOptions{
		sameFlowNodeProducer: true,
		completion:           "timeout",
		timeoutMS:            5000,
	})
}

func writeAccumulatorWildcardSameFlowNodeFixture(t *testing.T) string {
	t.Helper()
	root := writeAccumulatorWildcardSameFlowFixture(t)
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
producer-node:
  id: producer-node
  execution_type: system_node
  subscribes_to: [producer.start]
  event_handlers:
    producer.start:
      emit:
        event: task.done
accumulator-node:
  id: accumulator-node
  execution_type: system_node
  subscribes_to: [task.*]
  event_handlers:
    task.*:
      accumulate:
        expected_from: entity.expected_count
        completion: timeout
        timeout_ms: 5000
      advances_to: done
  state_schema:
    fields:
      expected_count: integer
`)
	return root
}

func writeAccumulatorWildcardSameFlowAgentFixture(t *testing.T) string {
	t.Helper()
	root := writeAccumulatorWildcardSameFlowFixture(t)
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), `
producer-agent:
  id: producer-agent
  role: producer
  mode: task
  emit_events:
    - task.done
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
accumulator-node:
  id: accumulator-node
  execution_type: system_node
  subscribes_to: [task.*]
  event_handlers:
    task.*:
      accumulate:
        expected_from: entity.expected_count
        completion: timeout
        timeout_ms: 5000
      advances_to: done
  state_schema:
    fields:
      expected_count: integer
`)
	return root
}

func writeAccumulatorWildcardSameFlowFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: accumulator-wildcard-safety
version: "1.0.0"
platform: ">=1.6.0"
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: accumulator-wildcard-safety
initial_state: collecting
terminal_states: [done]
states: [collecting, done]
pins:
  inputs:
    events: [task.done]
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), `
task.done:
  expected_count: integer
producer.start:
  {}
`)
	return root
}

func writeAccumulatorCrossFlowFixture(t *testing.T, withConnect bool) string {
	t.Helper()
	root := t.TempDir()
	connectBlock := ""
	if withConnect {
		connectBlock = `
connect:
  - from: producer.item.arrived
    to: consumer.item.arrived
`
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: accumulator-cross-flow
version: "1.0.0"
platform: ">=1.6.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: static
`+connectBlock)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: accumulator-cross-flow\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
pins:
  outputs:
    events: [item.arrived]
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
item.arrived:
  expected_count: integer
producer.start:
  {}
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "entities.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), `
producer-node:
  id: producer-node
  execution_type: system_node
  subscribes_to: [producer.start]
  event_handlers:
    producer.start:
      emit:
        event: item.arrived
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
initial_state: collecting
terminal_states: [done]
states: [collecting, done]
pins:
  inputs:
    events: [item.arrived]
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
item.arrived:
  expected_count: integer
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), `
consumer-node:
  id: consumer-node
  execution_type: system_node
  subscribes_to: [item.arrived]
  event_handlers:
    item.arrived:
      accumulate:
        expected_from: entity.expected_count
        completion: timeout
        timeout_ms: 5000
      advances_to: done
`)
	return root
}
