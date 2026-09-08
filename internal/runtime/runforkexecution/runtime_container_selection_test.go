package runforkexecution

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestSelectedContractSourceProjectionPreservesProducerRoutingAcrossDifferentRecipientOwner(t *testing.T) {
	producer := eventtest.ConcreteTemplateRoutingSource("producer", "producer/one", "entity-one")
	states := []runfork.RunForkSelectedContractWorkflowState{{
		SourceEventID: "source-event", EntityID: "entity-one", FlowID: "root",
		AddressKind: runfork.RunForkSelectedContractWorkflowStateRunScope,
	}}
	eventsIn := []runfork.RunForkSelectedContractSourceEvent{{
		SourceEventID: "source-event", EventName: "work.ready", EntityID: "entity-one", RoutingSource: producer,
	}}

	projected, err := projectSelectedContractSourceEventWorkflowStates("fork-run", states, eventsIn)
	if err != nil {
		t.Fatalf("project selected-contract source event: %v", err)
	}
	if len(projected) != 1 || projected[0].RoutingSource != producer {
		t.Fatalf("projected producer routing = %#v, want exact persisted source %#v", projected, producer)
	}
	if projected[0].FlowInstance != "fork-run" {
		t.Fatalf("projected execution target = %q, want fork-run", projected[0].FlowInstance)
	}
}

func TestSelectedContractSourceProjectionRejectsMissingProducerRoutingAuthority(t *testing.T) {
	_, err := projectSelectedContractSourceEventWorkflowStates(
		"fork-run",
		[]runfork.RunForkSelectedContractWorkflowState{{
			SourceEventID: "source-event", EntityID: "entity-one", FlowID: "root",
			AddressKind: runfork.RunForkSelectedContractWorkflowStateRunScope,
		}},
		[]runfork.RunForkSelectedContractSourceEvent{{
			SourceEventID: "source-event", EventName: "work.ready", EntityID: "entity-one", RoutingSource: events.NoRoutingSource(),
		}},
	)
	if err == nil || !strings.Contains(err.Error(), "persisted producer routing authority") {
		t.Fatalf("missing producer routing error = %v", err)
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
