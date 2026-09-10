package runforkexecution

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestSelectedContractSourceProjectionPreservesProducerRoutingAcrossDifferentRecipientOwner(t *testing.T) {
	producer := eventtest.ConcreteTemplateRoutingSource("producer", "producer/one", "entity-one")
	eventsIn := []runfork.RunForkSelectedContractSourceEvent{{
		SourceEventID: "source-event", EventName: "work.ready", RoutingSource: producer, ExecutionMode: executionmode.Live,
	}}

	projected, projection, err := projectSelectedContractSourceEvents("source-run", testWorkflowRecipientRoot(t), eventsIn)
	if err != nil {
		t.Fatalf("project selected-contract source event: %v", err)
	}
	if len(projected) != 1 || projected[0].RoutingSource != producer {
		t.Fatalf("projected producer routing = %#v, want exact persisted source %#v", projected, producer)
	}
	if !reflect.DeepEqual(projected, eventsIn) {
		t.Fatalf("recipient ownership rehomed producer entity/flow: got %#v want %#v", projected, eventsIn)
	}
	rootRecipient := testNodeFrontierRecipient(mustRunForkRootNode("test-node"), "work.ready", ".", "subscription")
	bound, err := projection.BindRecipient("source-event", rootRecipient)
	if err != nil || bound.Path != "fork-run" {
		t.Fatalf("independent root recipient binding failed: %#v, %v", bound, err)
	}
	if !reflect.DeepEqual(projected, eventsIn) || eventsIn[0].RoutingSource != producer {
		t.Fatal("root recipient binding changed producer ownership")
	}
	forkEvent, err := selectedContractForkEvent("source-run", "fork-run", "fork-event", projected[0], runfork.RunForkSelectedContractExecutionOwner)
	if err != nil {
		t.Fatal(err)
	}
	if forkEvent.RoutingSource() != producer || forkEvent.SourceRoute() != producer.Route() {
		t.Fatalf("fork event lost producer identity: %#v", forkEvent.RoutingSource())
	}
	if forkEvent.HasTargetRoute() || len(forkEvent.TargetRoutes()) != 0 {
		t.Fatalf("fork event promoted producer or recipient ownership into historical targets: %#v", forkEvent.NormalizedEnvelope())
	}
}

func TestSelectedContractActivityProjectionRejectsMissingProducerRoutingAuthority(t *testing.T) {
	_, _, err := projectSelectedContractSourceEvents(
		"source-run", testWorkflowRecipientRoot(t),
		[]runfork.RunForkSelectedContractSourceEvent{{
			SourceEventID: "source-event", EventName: runfork.RunForkSelectedContractPlatformActivityEvent, RoutingSource: events.NoRoutingSource(),
		}},
	)
	if err == nil || !strings.Contains(err.Error(), "persisted producer routing authority") {
		t.Fatalf("missing producer routing error = %v", err)
	}
}

func TestSelectedContractSourceProjectionPreservesAdmittedAbsence(t *testing.T) {
	input := []runfork.RunForkSelectedContractSourceEvent{{
		SourceEventID: "source-event", EventName: "work.ready", RoutingSource: events.NoRoutingSource(), ExecutionMode: executionmode.Live,
	}}
	projected, projection, err := projectSelectedContractSourceEvents("source-run", testWorkflowRecipientRoot(t), input)
	if err != nil || !reflect.DeepEqual(projected, input) {
		t.Fatalf("ordinary source absence changed: projected=%#v err=%v", projected, err)
	}
	recipient := testNodeFrontierRecipient(mustRunForkRootNode("test-node"), "work.ready", ".", "subscription")
	bound, err := projection.BindRecipient("source-event", recipient)
	if err != nil || bound.Path != "fork-run" {
		t.Fatalf("admitted independent receiver = %#v, %v", bound, err)
	}
	event, err := selectedContractForkEvent("source-run", "fork-run", "fork-event", projected[0], runfork.RunForkSelectedContractExecutionOwner)
	if err != nil {
		t.Fatal(err)
	}
	if !event.RoutingSource().Empty() || !event.SourceRoute().Empty() || event.HasTargetRoute() || len(event.TargetRoutes()) != 0 {
		t.Fatal("ordinary absence acquired producer or historical receiver authority")
	}
}

func TestSelectedContractSourceProjectionIsIdempotentAndDoesNotMutateInput(t *testing.T) {
	root := testWorkflowRecipientRoot(t)
	input := []runfork.RunForkSelectedContractSourceEvent{testRecipientWorkflowSourceEvent()}
	before := append([]runfork.RunForkSelectedContractSourceEvent(nil), input...)
	first, _, err := projectSelectedContractSourceEvents("source-run", root, input)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].RoutingSource != eventtest.RootRoutingSource("fork-run") || first[0].RoutingSource.Route().EntityID != "fork-run" || first[0].RoutingSource.Route().FlowInstance != "" {
		t.Fatalf("root source was not projected exactly: %#v", first)
	}
	second, projection, err := projectSelectedContractSourceEvents("source-run", root, first)
	if err != nil || !reflect.DeepEqual(first, second) || !reflect.DeepEqual(input, before) {
		t.Fatalf("double projection changed source evidence: first=%#v second=%#v err=%v", first, second, err)
	}
	selected := testNodeFrontierRecipient(mustRunForkRootNode("test-node"), "item.received", ".", "subscription")
	if bound, err := projection.BindRecipient("source-event", selected); err != nil || bound.Path != "fork-run" {
		t.Fatalf("already-projected source lost admitted membership: %#v, %v", bound, err)
	}
}

func TestSelectedContractContainerAcceptsMixedEffectiveAgentSelections(t *testing.T) {
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatalf("resolve profile: %v", err)
	}
	records := []runtimemanager.PersistedAgent{
		{Config: runtimeactors.AgentConfig{
			ID: "live-agent", LLMBackend: llmselection.BackendClaudeCLI,
			ExecutionMode: runtimeeffects.ExecutionModeLive,
		}},
		{Config: runtimeactors.AgentConfig{
			ID: "mock-agent", LLMBackend: llmselection.BackendMock,
			ExecutionMode: runtimeeffects.ExecutionModeMock,
			Mock: mockperformance.Performance{
				Kind: mockperformance.KindPython, Module: "mocks/agent.py",
				Source: []byte("def handle(input):\n    return {'text': 'ok'}\n"), Digest: "sha256:selected-contract-mock",
			},
		}},
	}
	if err := validateSelectedContractAgentExecutionSelections(profile, records); err != nil {
		t.Fatalf("validate mixed selections: %v", err)
	}

	records[1].Config.ExecutionMode = runtimeeffects.ExecutionModeLive
	records[1].Config.ResolvedLLMBackend = llmselection.BackendClaudeCLI
	if err := validateSelectedContractAgentExecutionSelections(profile, records); err != nil {
		t.Fatalf("stale derived descriptor must be recomputed by the manager: %v", err)
	}

	records[0].Config.LLMBackend = llmselection.BackendAnthropic
	if err := validateSelectedContractAgentExecutionSelections(profile, records); err == nil || !strings.Contains(err.Error(), "conflicts with configured runtime backend") {
		t.Fatalf("authored backend conflict error = %v", err)
	}
}

func TestSelectedContractContainerDefersRawDescriptorMaterializationToManagerOwner(t *testing.T) {
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatalf("resolve profile: %v", err)
	}
	records := []runtimemanager.PersistedAgent{{Config: runtimeactors.AgentConfig{
		ID: "raw-mock-agent",
		Mock: mockperformance.Performance{
			Kind: mockperformance.KindPython, Module: "mocks/agent.py",
		},
	}}}
	if err := validateSelectedContractAgentExecutionSelections(profile, records); err != nil {
		t.Fatalf("raw selected-contract descriptor: %v", err)
	}
}
