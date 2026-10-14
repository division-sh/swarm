package pipeline_test

import (
	"context"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

// Timer component proofs own native attachment admission, not executable
// topology. Their caller joins timer/publication work before retiring this owner.
func newTimerReplayAttachmentOwner(t *testing.T, ctx context.Context, selected any) (func(pipeline.DynamicFlowRuntimeReadinessPlan) pipeline.DynamicFlowRuntimeActivationAttempt, func()) {
	t.Helper()
	fact, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found || fact.Validate() != nil {
		t.Fatal("native timer attachment requires its admitted source")
	}
	workflows := selected.(pipeline.DynamicFlowRuntimeReadinessPersistence)
	process, err := selected.(startupownership.Store).AcquireProcessCapability(ctx, startupownership.AcquireRequest{
		OwnerID: "timer-replay-proof", BootID: uuid.NewString(), RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	var request *pipeline.DynamicFlowRuntimeActivationRequest
	var attempt pipeline.DynamicFlowRuntimeActivationAttempt
	var once sync.Once
	closeOwner := func() {
		t.Helper()
		once.Do(func() {
			cleanup := context.Background()
			if request != nil && attempt.ID() == "" {
				resolved, err := workflows.ResolveDynamicFlowRuntimeActivation(cleanup, *request)
				if err != nil || request.ValidateResolution(resolved) != nil || resolved.Disposition == pipeline.FlowActivationUnresolved {
					t.Errorf("resolve retained native timer admission: %+v, %v", resolved, err)
				} else if resolved.Disposition == pipeline.FlowActivationAdmitted {
					attempt = resolved.Attempt
				}
			}
			if attempt.ID() != "" {
				if err := workflows.RetireDynamicFlowRuntimeActivationAttempt(cleanup, attempt); err != nil {
					t.Errorf("retire joined native timer attachment: %v", err)
				}
			}
			if err := process.Release(cleanup); err != nil {
				t.Errorf("release native timer process owner: %v", err)
			}
		})
	}
	t.Cleanup(closeOwner)
	set, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: fact.BundleHash()}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	current, exists, err := process.CurrentSourceSet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		if current.Revision != set.Revision {
			t.Fatal("native timer restart changed its admitted source set")
		}
	} else if _, err := process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: set}); err != nil {
		t.Fatal(err)
	}
	grant, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{
		BundleHash: fact.BundleHash(), RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: set.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	return func(plan pipeline.DynamicFlowRuntimeReadinessPlan) pipeline.DynamicFlowRuntimeActivationAttempt {
		t.Helper()
		if request != nil {
			t.Fatal("timer component already owns an attachment request")
		}
		observed, found, err := workflows.LoadDynamicFlowRuntimeReadiness(ctx, plan.RunID, plan.Identity.Route())
		if err != nil || !found {
			t.Fatalf("read native timer attachment predecessor: %v, %v", found, err)
		}
		binding, err := grant.ProcessExecutionBinding()
		if err != nil {
			t.Fatal(err)
		}
		pending := pipeline.NewDynamicFlowRuntimeActivationRequest(plan, observed.AttemptOrdinal, observed.AttemptState, binding)
		request = &pending
		admitted, err := workflows.BeginDynamicFlowRuntimeActivation(ctx, pending)
		if err != nil || !admitted.Acknowledged {
			t.Fatalf("admit native timer attachment: %+v, %v", admitted, err)
		}
		attempt = admitted.Attempt
		if err := workflows.VerifyDynamicFlowRuntimeActivationAttempt(ctx, attempt); err != nil {
			t.Fatal(err)
		}
		return attempt
	}, closeOwner
}
