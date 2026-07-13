package bootverify

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestEventMetadataAuthorityRejectsInternalSwarmRestatements(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    eventMetadataAuthorityFixtureOptions
		want    string
		wantMsg string
	}{
		{
			name:    "producer names emitting node",
			opts:    eventMetadataAuthorityFixtureOptions{taskDoneSwarm: "producer: worker"},
			want:    "swarm.producer",
			wantMsg: "system node worker handler emits",
		},
		{
			name:    "consumer names subscribing node",
			opts:    eventMetadataAuthorityFixtureOptions{taskDoneSwarm: "consumer: observer"},
			want:    "swarm.consumer",
			wantMsg: "system node observer handler subscribes",
		},
		{
			name:    "source names internal producer",
			opts:    eventMetadataAuthorityFixtureOptions{taskDoneSwarm: "source: worker"},
			want:    "swarm.source",
			wantMsg: "derived internal producer system node worker handler emits",
		},
		{
			name: "producer names agent emit_events role",
			opts: eventMetadataAuthorityFixtureOptions{
				taskDoneSwarm: "producer: reviewer",
				agents: `
reviewer-agent:
  id: reviewer-agent
  role: reviewer
  mode: task
  emit_events: [task.done]
`,
			},
			want:    "swarm.producer",
			wantMsg: "agent role reviewer emit_events",
		},
		{
			name: "consumer names agent subscription role",
			opts: eventMetadataAuthorityFixtureOptions{
				taskDoneSwarm: "consumer: reviewer",
				agents: `
reviewer-agent:
  id: reviewer-agent
  role: reviewer
  mode: task
  subscriptions: [task.done]
`,
			},
			want:    "swarm.consumer",
			wantMsg: "agent role reviewer subscriptions",
		},
		{
			name: "producer names timer",
			opts: eventMetadataAuthorityFixtureOptions{
				taskDoneSwarm: "producer: reminder",
				timerBlock: `
  timers:
    - id: reminder
      owner: worker
      event: task.done
      delay: 1m
      start_on: event:task.start
`,
			},
			want:    "swarm.producer",
			wantMsg: "timer reminder fires event",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := loadEventMetadataAuthorityFixture(t, tc.opts)

			report := Run(context.Background(), source, Options{})

			if !reportContains(report.HardInvalidities(), "event_metadata_authority", tc.want) ||
				!reportContains(report.HardInvalidities(), "event_metadata_authority", tc.wantMsg) {
				t.Fatalf("expected event_metadata_authority hard invalidity containing %q and %q, got %#v", tc.want, tc.wantMsg, report.HardInvalidities())
			}
		})
	}
}

func TestEventMetadataAuthorityRejectsFlowSurfaceRestatements(t *testing.T) {
	for _, tc := range []struct {
		name       string
		invalidity canonicalrouting.ParentConnectEventMetadataInvalidity
		want       string
		wantMsg    string
	}{
		{
			name:       "producer names flow auto emit",
			invalidity: canonicalrouting.ParentConnectMetadataProducerFlowAutoEmit,
			want:       "swarm.producer",
			wantMsg:    "flow producer auto_emit_on_create producer",
		},
		{
			name:       "producer names flow output pin",
			invalidity: canonicalrouting.ParentConnectMetadataProducerFlowOutput,
			want:       "swarm.producer",
			wantMsg:    "flow producer output pin producer",
		},
		{
			name:       "consumer names flow input pin",
			invalidity: canonicalrouting.ParentConnectMetadataConsumerFlowInput,
			want:       "swarm.consumer",
			wantMsg:    "flow consumer input pin consumer",
		},
		{
			name:       "producer names parent connect output",
			invalidity: canonicalrouting.ParentConnectMetadataProducerConnectOutput,
			want:       "swarm.producer",
			wantMsg:    "parent connect output producer",
		},
		{
			name:       "consumer names parent connect input",
			invalidity: canonicalrouting.ParentConnectMetadataConsumerConnectInput,
			want:       "swarm.consumer",
			wantMsg:    "parent connect input consumer",
		},
		{
			name:       "producer rejects wrong-event flow output pin",
			invalidity: canonicalrouting.ParentConnectMetadataProducerWrongFlowEvent,
			want:       "swarm.producer",
			wantMsg:    "flow producer output pin producer",
		},
		{
			name:       "consumer rejects wrong-event flow input pin",
			invalidity: canonicalrouting.ParentConnectMetadataConsumerWrongFlowEvent,
			want:       "swarm.consumer",
			wantMsg:    "flow consumer input pin consumer",
		},
		{
			name:       "producer rejects wrong-event parent connect output",
			invalidity: canonicalrouting.ParentConnectMetadataProducerWrongConnectEvent,
			want:       "swarm.producer",
			wantMsg:    "parent connect output producer",
		},
		{
			name:       "consumer rejects wrong-event parent connect input",
			invalidity: canonicalrouting.ParentConnectMetadataConsumerWrongConnectEvent,
			want:       "swarm.consumer",
			wantMsg:    "parent connect input consumer",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := loadEventMetadataFlowAuthorityFixture(t, tc.invalidity)

			report := Run(context.Background(), source, Options{})

			if !reportContains(report.HardInvalidities(), "event_metadata_authority", tc.want) ||
				!reportContains(report.HardInvalidities(), "event_metadata_authority", tc.wantMsg) {
				t.Fatalf("expected event_metadata_authority hard invalidity containing %q and %q, got %#v", tc.want, tc.wantMsg, report.HardInvalidities())
			}
		})
	}
}

func TestEventMetadataAuthorityAcceptsExternalProof(t *testing.T) {
	source := loadEventMetadataAuthorityFixture(t, eventMetadataAuthorityFixtureOptions{
		externalRequestedSwarm: `
    source: external webhook
    producer: mailbox_human
    consumer: external_ui
`,
		taskDoneSwarm: `
    source: platform timer
    producer: mailbox_human
    consumer: external_ui
`,
	})

	report := Run(context.Background(), source, Options{})

	if reportContains(report.HardInvalidities(), "event_metadata_authority", "") {
		t.Fatalf("external/platform metadata proof should be accepted, got %#v", report.HardInvalidities())
	}
}

func TestEventMetadataAuthorityNarrowReadbackExplainsDerivedAndExternalProof(t *testing.T) {
	source := loadEventMetadataAuthorityFixture(t, eventMetadataAuthorityFixtureOptions{})
	report := Run(context.Background(), source, Options{})

	if reportContains(report.Warnings(), "event_producer_exists", "task.done") ||
		reportContains(report.Warnings(), "event_consumer_exists", "task.done") {
		t.Fatalf("derived handler roles should satisfy producer/consumer checks, got warnings %#v", report.Warnings())
	}
	if reportContains(report.Warnings(), "semantic_drift_dead_event_schema", "task.done") {
		t.Fatalf("derived handler roles should keep event schema alive, got warnings %#v", report.Warnings())
	}

	entry, _, ok := source.ResolveFlowEventCatalogEntry("", "task.done")
	if !ok {
		t.Fatalf("task.done event entry missing")
	}
	producers, consumers := eventMetadataRoleNames(source, deadEventDeclaration{
		Canonical: "task.done",
		Entry:     entry,
	})
	if label, ok := producers.match("worker"); !ok || !strings.Contains(label, "handler emits") {
		t.Fatalf("producer role readback = %q/%v, want worker handler emit proof", label, ok)
	}
	if label, ok := consumers.match("observer"); !ok || !strings.Contains(label, "handler subscribes") {
		t.Fatalf("consumer role readback = %q/%v, want observer handler subscription proof", label, ok)
	}
}

type eventMetadataAuthorityFixtureOptions struct {
	externalRequestedSwarm string
	taskDoneSwarm          string
	agents                 string
	timerBlock             string
}

func loadEventMetadataAuthorityFixture(t *testing.T, opts eventMetadataAuthorityFixtureOptions) semanticview.Source {
	t.Helper()
	root := writeEventMetadataAuthorityFixture(t, opts)
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	return semanticview.Wrap(loadFixtureBundleAt(t, repoRoot, root, platformSpec))
}

func writeEventMetadataAuthorityFixture(t *testing.T, opts eventMetadataAuthorityFixtureOptions) string {
	t.Helper()
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: event-metadata-authority
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: event-metadata-authority\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	if strings.TrimSpace(opts.agents) == "" {
		opts.agents = "{}\n"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), opts.agents)
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), eventMetadataAuthorityEventsYAML(opts))
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
worker:
  id: worker
  execution_type: system_node
`+opts.timerBlock+`  event_handlers:
    task.start:
      emit:
        event: task.done
observer:
  id: observer
  execution_type: system_node
  event_handlers:
    task.done: {}
`)
	return root
}

func loadEventMetadataFlowAuthorityFixture(t *testing.T, invalidity canonicalrouting.ParentConnectEventMetadataInvalidity) semanticview.Source {
	t.Helper()
	root := canonicalrouting.CopyParentConnectEventMetadataInvalidity(t, invalidity)
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	return semanticview.Wrap(loadFixtureBundleAt(t, repoRoot, root, platformSpec))
}

func eventMetadataAuthorityEventsYAML(opts eventMetadataAuthorityFixtureOptions) string {
	// routing-example-census: parser-only issue=none owner=bootverify.event_metadata_authority proof=internal/runtime/bootverify/workflow_event_metadata_authority_checks_test.go:TestEventMetadataAuthorityAcceptsExternalProof
	externalSwarm := indentEventMetadataAuthoritySwarm(opts.externalRequestedSwarm)
	if strings.TrimSpace(externalSwarm) == "" {
		externalSwarm = "    source: external"
	}
	taskDoneSwarm := indentEventMetadataAuthoritySwarm(opts.taskDoneSwarm)
	events := `external.requested:
  swarm:
` + externalSwarm + `
task.start:
  swarm:
    source: external
task.done:
`
	if strings.TrimSpace(taskDoneSwarm) != "" {
		events += "  swarm:\n" + taskDoneSwarm + "\n"
	}
	return events
}

func indentEventMetadataAuthoritySwarm(raw string) string {
	raw = strings.Trim(raw, "\n")
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	lines := strings.Split(raw, "\n")
	for idx, line := range lines {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "    ") {
			continue
		}
		lines[idx] = "    " + strings.TrimLeft(line, " ")
	}
	return strings.Join(lines, "\n")
}
