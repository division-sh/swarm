package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// CompleteLiveFlowStartup gives a keyless live flow its no-argument construction
// before ingress opens. Reopening a run reuses the immutable original receipt.
func (rt *Runtime) CompleteLiveFlowStartup(ctx context.Context) error {
	if rt == nil || rt.Manager == nil || rt.Pipeline == nil || rt.Options.WorkflowModule == nil {
		return fmt.Errorf("live flow startup requires its admitted runtime and source")
	}
	var err error
	ctx, err = rt.mutationContext(ctx)
	if err != nil {
		return err
	}
	ctx = rt.authorActivityContext(ctx)
	if _, present := worklifetime.OccurrenceFromContext(ctx); !present {
		ctx = worklifetime.WithRuntimeOccurrence(ctx, rt.workOccurrence)
	}
	source := rt.Options.WorkflowModule.SemanticSource()
	for _, flow := range source.FlowScopes() {
		run, found, err := rt.Pipeline.LoadLiveRun(ctx, runlifecycle.LiveRunScope{FlowPath: flowidentity.ScopeKey(source, flow.ID)})
		if err != nil {
			return err
		}
		if !found || !run.Source.Matches(rt.Options.SourceArtifactFact) {
			continue
		}
		keyless, err := pipeline.StandingConstructionIsKeyless(source, flow.ID)
		if err != nil {
			return err
		}
		if !keyless {
			continue
		}
		instance, err := flowidentity.StandingForGeneration(source, semanticview.RootExecutionFlowID(source), run.RunID)
		if err != nil {
			return err
		}
		ctx = effects.WithExecutionMode(correlation.WithRunID(ctx, run.RunID), rt.ExecutionPosture.RootMode())
		if _, err := rt.Manager.EnsureFlowInstance(ctx, pipeline.FlowInstanceActivationRequest{
			ContractBundle: source, Instance: instance, OccurredAt: time.Now().UTC(),
		}); err != nil {
			return err
		}
	}
	return nil
}
