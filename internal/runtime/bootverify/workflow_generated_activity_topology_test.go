package bootverify

import (
	"context"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRunAcceptsGeneratedActivityResultsWithoutAuthoredSchemas(t *testing.T) {
	report := runGeneratedActivityFixture(t, false)
	for _, checkID := range []string{"event_chain_integrity", "event_consumer_exists", "transition_reference_validation"} {
		if reportContains(report.Findings, checkID, "send") {
			t.Fatalf("unexpected %s generated-result finding: %#v", checkID, report.Findings)
		}
	}
}

func TestRunAcceptsSubscriptionsToGeneratedActivityResultSchemas(t *testing.T) {
	report := runGeneratedActivityFixture(t, true)
	for _, checkID := range []string{"event_chain_integrity", "event_consumer_exists", "event_producer_exists", "transition_reference_validation", "condition_payload_alignment"} {
		if reportContains(report.Findings, checkID, "send") {
			t.Fatalf("unexpected %s generated-result finding: %#v", checkID, report.Findings)
		}
	}
}

func TestRunAcceptsNestedFlowGeneratedActivityResultOwnership(t *testing.T) {
	source := loadNestedGeneratedActivityFixture(t)
	report := Run(context.Background(), source, Options{})
	for _, checkID := range []string{"event_chain_integrity", "event_consumer_exists", "event_producer_exists", "transition_reference_validation", "condition_payload_alignment"} {
		if reportContains(report.Findings, checkID, "child.send") {
			t.Fatalf("unexpected %s nested generated-result finding: %#v", checkID, report.Findings)
		}
	}

	generated := generatedActivityResultEventNamesLocal(source)
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	for _, eventType := range []string{"child.send.succeeded", "child.send.failed"} {
		if _, ok := generated[eventType]; !ok {
			t.Fatalf("nested generated identities = %#v, missing %s", generated, eventType)
		}
		proof := semanticview.ResolveFlowEventProof(source, "child", eventType)
		if !proof.HasSchema {
			t.Fatalf("nested generated event %s has no engine-owned payload schema: %#v", eventType, proof)
		}
		routed := false
		for _, endpoint := range census.MatchingConsumers("child", proof.EventKey()) {
			if endpoint.Kind == semanticview.EventEndpointNodeHandler && endpoint.NodeID == "observer-node" && endpoint.HandlerEvent == eventType {
				routed = true
				break
			}
		}
		if !routed {
			t.Fatalf("nested generated event %s has no canonical observer route", eventType)
		}
	}
}

func TestGeneratedActivityResultNamesCoverHandlerAndRuleSites(t *testing.T) {
	handlers := map[string]runtimecontracts.SystemNodeEventHandler{
		"request": {
			Activity: runtimecontracts.ActivitySpec{ID: "direct", Tool: "send"},
			Rules: []runtimecontracts.HandlerRuleEntry{{
				ID:       "fallback",
				Activity: runtimecontracts.ActivitySpec{Tool: "send"},
			}},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Nodes:     map[string]runtimecontracts.SystemNodeContract{"activity-node": {EventHandlers: handlers}},
		Semantics: runtimecontracts.WorkflowSemanticView{NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{"activity-node": handlers}},
	}
	names := generatedActivityResultEventNamesLocal(semanticview.Wrap(bundle))
	sites := runtimecontracts.ActivitySitesForNode("", "activity-node", handlers)
	if len(sites) != 2 {
		t.Fatalf("activity sites = %#v", sites)
	}
	for _, site := range sites {
		results := runtimecontracts.ActivityResultEventsForSite(site)
		for _, eventType := range []string{results.SuccessEvent, results.FailureEvent} {
			if _, ok := names[eventidentity.Normalize(eventType)]; !ok {
				t.Fatalf("generated names = %#v, missing %s", names, eventType)
			}
		}
	}
}

func runGeneratedActivityFixture(t *testing.T, subscribeResults bool) Report {
	t.Helper()
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: generated-activity-topology
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: generated-activity-topology\nstages: []\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), `
request:
  message: text
  swarm:
    source: external
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), `
send:
  description: send one message
  handler_type: http
  effect_class: read_only
  input_schema:
    type: object
    properties:
      message: {type: string}
    required: [message]
  output_schema:
    type: object
    properties:
      delivered: {type: boolean}
  response_success: {kind: http_status_2xx}
  http:
    method: POST
    url: https://example.invalid/send
    body:
      message: "{{input.message}}"
`)
	resultHandlers := ""
	resultSubscriptions := ""
	if subscribeResults {
		resultSubscriptions = ", send.succeeded, send.failed"
		resultHandlers = `
    send.succeeded:
      rules:
        - id: observe_success
          condition: payload.result != null
    send.failed:
      rules:
        - id: observe_failure
          condition: payload.failure != null
`
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
activity-node:
  id: activity-node
  execution_type: system_node
  subscribes_to: [request`+resultSubscriptions+`]
  event_handlers:
    request:
      activity:
        id: send
        tool: send
        input:
          message:
            ref: payload.message
`+resultHandlers)
	repoRoot := repoRootForBootverifyTest(t)
	bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	return Run(context.Background(), semanticview.Wrap(bundle), Options{})
}

func loadNestedGeneratedActivityFixture(t *testing.T) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: nested-generated-activity-topology
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: child
    flow: child
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: nested-generated-activity-topology\nstages: []\n")
	for _, file := range []string{"entities.yaml", "policy.yaml", "tools.yaml", "agents.yaml", "events.yaml", "nodes.yaml"} {
		writeBootverifyFixtureFile(t, filepath.Join(root, file), "{}\n")
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "schema.yaml"), "name: child\nmode: static\nstages: []\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "entities.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "events.yaml"), `
request:
  message: text
  swarm:
    source: external
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "tools.yaml"), `
send:
  description: send one message
  handler_type: http
  effect_class: read_only
  input_schema:
    type: object
    properties:
      message: {type: string}
    required: [message]
  output_schema:
    type: object
    properties:
      delivered: {type: boolean}
  response_success: {kind: http_status_2xx}
  http:
    method: POST
    url: https://example.invalid/send
    body:
      message: "{{input.message}}"
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "nodes.yaml"), `
activity-node:
  id: activity-node
  execution_type: system_node
  subscribes_to: [request]
  event_handlers:
    request:
      activity:
        id: send
        tool: send
        input:
          message:
            ref: payload.message
observer-node:
  id: observer-node
  execution_type: system_node
  subscribes_to: [child.send.succeeded, child.send.failed]
  event_handlers:
    child.send.succeeded:
      rules:
        - id: observe_success
          condition: payload.result.delivered == true
    child.send.failed:
      rules:
        - id: observe_failure
          condition: payload.failure != null
`)
	repoRoot := repoRootForBootverifyTest(t)
	bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	return semanticview.Wrap(bundle)
}
