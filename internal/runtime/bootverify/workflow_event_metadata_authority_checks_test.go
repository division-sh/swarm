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
		name    string
		opts    eventMetadataFlowAuthorityFixtureOptions
		want    string
		wantMsg string
	}{
		{
			name:    "producer names flow auto emit",
			opts:    eventMetadataFlowAuthorityFixtureOptions{flowStartedSwarm: "producer: producer"},
			want:    "swarm.producer",
			wantMsg: "flow producer auto_emit_on_create producer",
		},
		{
			name:    "producer names flow output pin",
			opts:    eventMetadataFlowAuthorityFixtureOptions{deployDoneSwarm: "producer: deploy_done"},
			want:    "swarm.producer",
			wantMsg: "flow producer output pin producer",
		},
		{
			name:    "consumer names flow input pin",
			opts:    eventMetadataFlowAuthorityFixtureOptions{deployCompletedSwarm: "consumer: consumer"},
			want:    "swarm.consumer",
			wantMsg: "flow consumer input pin consumer",
		},
		{
			name:    "producer names parent connect output",
			opts:    eventMetadataFlowAuthorityFixtureOptions{deployDoneSwarm: "producer: producer.deploy_done"},
			want:    "swarm.producer",
			wantMsg: "parent connect output producer",
		},
		{
			name:    "consumer names parent connect input",
			opts:    eventMetadataFlowAuthorityFixtureOptions{deployCompletedSwarm: "consumer: consumer.deploy_completed"},
			want:    "swarm.consumer",
			wantMsg: "parent connect input consumer",
		},
		{
			name:    "producer rejects wrong-event flow output pin",
			opts:    eventMetadataFlowAuthorityFixtureOptions{flowStartedSwarm: "producer: deploy_done"},
			want:    "swarm.producer",
			wantMsg: "flow producer output pin producer",
		},
		{
			name:    "consumer rejects wrong-event flow input pin",
			opts:    eventMetadataFlowAuthorityFixtureOptions{deployDoneSwarm: "consumer: deploy_completed"},
			want:    "swarm.consumer",
			wantMsg: "flow consumer input pin consumer",
		},
		{
			name:    "producer rejects wrong-event parent connect output",
			opts:    eventMetadataFlowAuthorityFixtureOptions{flowStartedSwarm: "producer: producer.deploy_done"},
			want:    "swarm.producer",
			wantMsg: "parent connect output producer",
		},
		{
			name:    "consumer rejects wrong-event parent connect input",
			opts:    eventMetadataFlowAuthorityFixtureOptions{deployDoneSwarm: "consumer: consumer.deploy_completed"},
			want:    "swarm.consumer",
			wantMsg: "parent connect input consumer",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := loadEventMetadataFlowAuthorityFixture(t, tc.opts)

			report := Run(context.Background(), source, Options{})

			if !reportContains(report.HardInvalidities(), "event_metadata_authority", tc.want) ||
				!reportContains(report.HardInvalidities(), "event_metadata_authority", tc.wantMsg) {
				t.Fatalf("expected event_metadata_authority hard invalidity containing %q and %q, got %#v", tc.want, tc.wantMsg, report.HardInvalidities())
			}
		})
	}
}

func TestEventMetadataAuthorityAcceptsExternalProof(t *testing.T) {
	// routing-example-census: parser-only issue=none owner=bootverify.event_metadata_authority proof=TestEventMetadataAuthorityAcceptsExternalProof
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

type eventMetadataFlowAuthorityFixtureOptions struct {
	flowStartedSwarm     string
	deployDoneSwarm      string
	deployCompletedSwarm string
}

func loadEventMetadataFlowAuthorityFixture(t *testing.T, opts eventMetadataFlowAuthorityFixtureOptions) semanticview.Source {
	t.Helper()
	root := writeEventMetadataFlowAuthorityFixture(t, opts)
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	return semanticview.Wrap(loadFixtureBundleAt(t, repoRoot, root, platformSpec))
}

func writeEventMetadataFlowAuthorityFixture(t *testing.T, opts eventMetadataFlowAuthorityFixtureOptions) string {
	t.Helper()
	opts.flowStartedSwarm = canonicalParentConnectMetadataRole(opts.flowStartedSwarm)
	opts.deployDoneSwarm = canonicalParentConnectMetadataRole(opts.deployDoneSwarm)
	opts.deployCompletedSwarm = canonicalParentConnectMetadataRole(opts.deployCompletedSwarm)
	root := canonicalrouting.CopyExample(t, canonicalrouting.ParentConnect)
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), "pins:\n", `auto_emit_on_create:
  event: flow.started
pins:
`)
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `      - name: work_ready
        event: work.ready
`, `      - name: work_ready
        event: work.ready
      - name: deploy_done
        event: deploy.done
`)
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `      - name: work_ready
        event: work.ready
`, `      - name: work_ready
        event: work.ready
      - name: deploy_completed
        event: deploy.completed
        source: external
`)
	canonicalrouting.WriteFile(t, root, "flows/producer/events.yaml",
		eventMetadataAuthorityEventEntry("flow.started", opts.flowStartedSwarm)+
			eventMetadataAuthorityRoutedEventEntry("work.requested", "")+
			eventMetadataAuthorityRoutedEventEntry("work.ready", opts.deployDoneSwarm)+
			eventMetadataAuthorityRoutedEventEntry("deploy.done", ""))
	canonicalrouting.WriteFile(t, root, "flows/consumer/events.yaml",
		eventMetadataAuthorityRoutedEventEntry("work.ready", opts.deployCompletedSwarm)+
			eventMetadataAuthorityRoutedEventEntry("deploy.completed", ""))
	return root
}

func canonicalParentConnectMetadataRole(raw string) string {
	replacer := strings.NewReplacer(
		"producer.deploy_done", "producer.work_ready",
		"consumer.deploy_completed", "consumer.work_ready",
	)
	return replacer.Replace(raw)
}

func writeEventMetadataFlowAuthorityFlow(t *testing.T, root, flowID, schema, events string) {
	t.Helper()
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), schema)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), events)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "nodes.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "entities.yaml"), "{}\n")
}

func eventMetadataAuthorityEventEntry(eventName, swarm string) string {
	eventName = strings.TrimSpace(eventName)
	swarm = indentEventMetadataAuthoritySwarm(swarm)
	if swarm == "" {
		return eventName + ": {}\n"
	}
	return eventName + ":\n  swarm:\n" + swarm + "\n"
}

func eventMetadataAuthorityRoutedEventEntry(eventName, swarm string) string {
	entry := eventMetadataAuthorityEventEntry(eventName, swarm)
	if strings.HasSuffix(entry, ": {}\n") {
		return strings.TrimSuffix(entry, " {}\n") + "\n  work_id: text\n"
	}
	return entry + "  work_id: text\n"
}

func eventMetadataAuthorityEventsYAML(opts eventMetadataAuthorityFixtureOptions) string {
	// routing-example-census: parser-only issue=none owner=bootverify.event_metadata_authority proof=TestEventMetadataAuthorityAcceptsExternalProof
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
