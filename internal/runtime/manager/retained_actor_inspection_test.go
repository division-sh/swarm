package manager

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type retainedActorReaderProbe struct {
	actors []PersistedAgent
	owned  bool
	err    error
	reads  int
}

func (p *retainedActorReaderProbe) LoadAgents(ctx context.Context) ([]PersistedAgent, error) {
	p.reads++
	return append([]PersistedAgent(nil), p.actors...), errors.Join(p.err, ctx.Err())
}

func (p *retainedActorReaderProbe) ObserveOrdinaryRunSource(ctx context.Context, _ correlation.SourceArtifactFact, _ string) (bool, error) {
	return p.owned, errors.Join(p.err, ctx.Err())
}

func TestRetainedActorInspectionUsesDeclarationProjectionBeforeHydration(t *testing.T) {
	source := loadRootAndFlowStaticAgentSource(t)
	bundle, _ := semanticview.Bundle(source)
	fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	for _, backend := range []string{llmselection.BackendAnthropic, llmselection.BackendClaudeCLI, llmselection.BackendOpenAICompatible, llmselection.BackendOpenAIResponses} {
		t.Run(backend, func(t *testing.T) {
			options := AgentManagerOptions{ExecutionPosture: executionposture.Live, LLMBackend: backend, RequireModelResolution: true}
			blueprints, err := ResolveStaticTopologyBlueprints(options, source)
			if err != nil || len(blueprints) != 1 {
				t.Fatalf("exact source blueprint: %d %v", len(blueprints), err)
			}
			oldOptions := options
			oldOptions.LLMBackend = llmselection.BackendAnthropic
			if backend == oldOptions.LLMBackend {
				oldOptions.LLMBackend = llmselection.BackendOpenAIResponses
			}
			oldBlueprints, err := ResolveStaticTopologyBlueprints(oldOptions, source)
			if err != nil {
				t.Fatal(err)
			}
			actor, err := oldBlueprints[0].Materialize(uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			coordinate := agenttopology.SourceCoordinate{BundleHash: fact.BundleHash()}
			oldDesired, err := desiredAgentsFromBlueprints(oldBlueprints, coordinate)
			if err != nil {
				t.Fatal(err)
			}
			oldPlan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, oldDesired)
			if err != nil {
				t.Fatal(err)
			}
			actor.Topology, err = agenttopology.StaticAdmission(oldPlan.Revision, fact.BundleHash(), agenttopology.LifetimeDurableManaged)
			if err != nil {
				t.Fatal(err)
			}
			actor.LifecyclePhase = AgentLifecycleRunning
			if err := ValidatePersistedAgentExecution(oldOptions, actor.Config); err != nil {
				t.Fatalf("predecessor must be a supported complete descriptor: %v", err)
			}
			if err := ValidatePersistedAgentExecution(options, actor.Config); err == nil {
				t.Fatal("the counterexample must reject before declaration projection")
			}
			reader := &retainedActorReaderProbe{actors: []PersistedAgent{actor}, owned: true}
			before := reader.actors[0]
			inspection, err := InspectRetainedActors(context.Background(), reader, source, fact, options)
			if err != nil || inspection.Observed != 1 || inspection.Reconciliations != 1 || len(inspection.Configurations) != 1 {
				t.Fatalf("declaration projection: %+v %v", inspection, err)
			}
			if inspection.Configurations[0].ResolvedLLMBackend != backend || !reflect.DeepEqual(before, reader.actors[0]) {
				t.Fatalf("inspection kept stale input or mutated persistence: %+v", inspection)
			}
			reader.owned = false
			inspection, err = InspectRetainedActors(context.Background(), reader, source, fact, options)
			if err != nil || inspection.Foreign != 1 || len(inspection.Configurations) != 0 {
				t.Fatalf("foreign run adopted the configured backend: %+v %v", inspection, err)
			}
		})
	}
}

func TestRetainedActorInspectionDoesNotRewriteReadinessActorsOrHideReadFailure(t *testing.T) {
	source := loadRootAndFlowStaticAgentSource(t)
	bundle, _ := semanticview.Bundle(source)
	fact, _ := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	options := AgentManagerOptions{ExecutionPosture: executionposture.Live, LLMBackend: llmselection.BackendAnthropic, RequireModelResolution: true}
	runID := uuid.NewString()
	root := flowidentity.Stored(source, ".", runID, runID, runID, "")
	child, err := flowidentity.KeylessChild(source, root, "ops-flow")
	if err != nil {
		t.Fatal(err)
	}
	constructed, err := ConstructedFlowMaterialization(source, runID, child, map[string]any{})
	if err != nil || len(constructed.Agents) != 1 {
		t.Fatalf("constructed readiness actor: %+v %v", constructed, err)
	}
	blueprint, err := ResolveAgentMaterializationBlueprint(options, constructed.Agents[0])
	if err != nil {
		t.Fatal(err)
	}
	actor, err := blueprint.Materialize(runID)
	if err != nil {
		t.Fatal(err)
	}
	actor.Topology, err = agenttopology.FlowReadinessAdmission(actor.Config.Identity.RunID, actor.Config.CanonicalFlowPath(), strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	reader := &retainedActorReaderProbe{actors: []PersistedAgent{actor}, owned: true}
	if inspection, err := InspectRetainedActors(context.Background(), reader, source, fact, options); err != nil || len(inspection.Configurations) != 1 {
		t.Fatalf("valid readiness actor: %+v %v", inspection, err)
	}
	reader.actors[0].Config.ResolvedLLMBackend = "invalid-stale-descriptor"
	if _, err := InspectRetainedActors(context.Background(), reader, source, fact, options); err == nil {
		t.Fatal("readiness actors borrowed declaration reconciliation")
	}
	witness := errors.New("retained actor read failed")
	reader.err = witness
	if _, err := InspectRetainedActors(context.Background(), reader, source, fact, options); !errors.Is(err, witness) {
		t.Fatalf("required read failure became an empty inventory: %v", err)
	}
	reader.reads = 0
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := InspectRetainedActors(ctx, reader, source, fact, options); !errors.Is(err, context.Canceled) || reader.reads != 0 {
		t.Fatalf("canceled actor observation reached reads: %v %d", err, reader.reads)
	}
}
