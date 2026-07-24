package bus

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type normalRunCompletionTestStore struct {
	InMemoryEventStore
	standaloneEvents []string
	normalEvents     []string
	workflowTerms    []string
	flowTerms        map[string][]string
}

func (s *normalRunCompletionTestStore) ConvergeStandaloneRuntimePlatformRun(_ context.Context, evt events.Event) error {
	s.standaloneEvents = append(s.standaloneEvents, evt.ID())
	return nil
}

func (s *normalRunCompletionTestStore) ConvergeNormalRunCompletion(_ context.Context, eventID string, workflowTerminals []string, flowTerminals map[string][]string) error {
	s.normalEvents = append(s.normalEvents, eventID)
	s.workflowTerms = append([]string{}, workflowTerminals...)
	if flowTerminals != nil {
		s.flowTerms = make(map[string][]string, len(flowTerminals))
		for key, states := range flowTerminals {
			s.flowTerms[key] = append([]string{}, states...)
		}
	}
	return nil
}

func TestEventBusStandalonePlatformConvergenceAlsoProbesNormalRunCompletion(t *testing.T) {
	store := &normalRunCompletionTestStore{}
	eb, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	evt := eventtest.RunCreatingRootIngress("event-2", events.EventType("platform.boot"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})
	if err := eb.convergeStandaloneRuntimePlatformRun(context.Background(), evt); err != nil {
		t.Fatalf("convergeStandaloneRuntimePlatformRun: %v", err)
	}
	if len(store.standaloneEvents) != 1 || store.standaloneEvents[0] != "event-2" {
		t.Fatalf("standalone events = %#v, want event-2", store.standaloneEvents)
	}
	if len(store.normalEvents) != 1 || store.normalEvents[0] != "event-2" {
		t.Fatalf("normal completion events = %#v, want event-2", store.normalEvents)
	}
}

func TestEventBusNormalRunCompletionUsesRootTerminalStatesNotChildAggregate(t *testing.T) {
	store := &normalRunCompletionTestStore{}
	eb, err := newScopedTestEventBus(store, EventBusOptions{
		ContractBundle: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
			Package: runtimecontracts.ProjectPackageDocument{Name: "root-workflow"},
			RootSchema: &runtimecontracts.FlowSchemaDocument{
				StageDeclarations: runtimecontracts.FlowStageDeclarations{
					Declared: true,
					Entries: []runtimecontracts.FlowStageDeclaration{
						{ID: "ready", Initial: true},
						{ID: "done"},
						{ID: "archived", Terminal: true},
					},
				},
			},
			Paths: runtimecontracts.ContractPaths{Flows: []runtimecontracts.FlowContractPaths{
				{ID: "child", Flow: "child"},
			}},
			FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
				"child": {
					StageDeclarations: runtimecontracts.FlowStageDeclarations{
						Declared: true,
						Entries: []runtimecontracts.FlowStageDeclaration{
							{ID: "ready", Initial: true},
							{ID: "done", Terminal: true},
						},
					},
				},
			},
			Semantics: runtimecontracts.WorkflowSemanticView{
				Name:           "root-workflow",
				TerminalStages: []string{"done", "archived"},
				FlowTerminal: map[string][]string{
					"child": {"done"},
				},
			},
		}),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}

	if err := eb.ConvergeNormalRunCompletionForEvent(context.Background(), "event-3"); err != nil {
		t.Fatalf("ConvergeNormalRunCompletionForEvent: %v", err)
	}

	if got, want := store.workflowTerms, []string{"archived"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("workflow terminals = %#v, want root-only %#v", got, want)
	}
	if got, want := store.flowTerms["child"], []string{"done"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("child flow terminals = %#v, want %#v", got, want)
	}
}
