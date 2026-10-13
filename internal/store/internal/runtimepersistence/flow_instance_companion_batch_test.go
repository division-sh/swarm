package runtimepersistence

import (
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func TestIndexCompanionBatchCannotHideCorruptUnselectedSiblingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithAgents(t, backend, false)
			f.constructKeylessRoot(t)
			request := f.request("retained-business-key", "retained-slot", "initial")
			plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil {
				t.Fatal(err)
			}
			runID := correlation.RunIDFromContext(f.ctx)
			fact, present := correlation.SourceArtifactFactFromContext(f.ctx)
			if !present {
				t.Fatal("sibling proof requires its admitted source")
			}
			parent := flowidentity.Stored(request.ContractBundle, ".", runID, runID, runID, "")
			keys, err := pipeline.AdmitFlowInstanceKeyMaterial(request.ContractBundle, "review", "unselected-key")
			if err != nil {
				t.Fatal(err)
			}
			lookup, err := pipeline.NewDeclaredFlowInstanceLookup(request.ContractBundle, fact, runID, "review", parent, keys)
			if err != nil {
				t.Fatal(err)
			}
			index := f.store.(pipeline.FlowInstanceIndexReader)
			if observation, found, err := index.LookupFlowInstance(f.ctx, lookup); err != nil || found || observation.Valid() {
				t.Fatalf("healthy non-selected sibling prevented genuine absence: found=%t err=%v", found, err)
			}
			owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: plan.Identity.Route()}
			for _, fault := range []struct {
				field pipelinepersistence.FlowConstructorHeaderFaultField
				value any
			}{
				{"parent_instance", nil}, {"instance_key", "foreign-key"},
			} {
				t.Run(string(fault.field), func(t *testing.T) {
					restore, err := FaultFlowConstructorHeaderForTest(f.ctx, f.store, owner, fault.field, fault.value)
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := restore(f.ctx); err != nil {
							t.Fatal(err)
						}
					}()
					before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
					if err != nil {
						t.Fatal(err)
					}
					observed, found, err := index.LookupFlowInstance(f.ctx, lookup)
					var corruption *pipeline.FlowInstanceConstructionCorruption
					if !errors.As(err, &corruption) || found || observed.Valid() {
						t.Fatalf("batch hid a corrupt non-selected sibling: found=%t observation=%+v err=%v", found, observed, err)
					}
					// Parent-only scope is independent; a different declaration's
					// corruption cannot poison its otherwise valid observation.
					rootScope, err := pipeline.NewFlowInstanceLookupScope(request.ContractBundle, fact, runID, []string{"."}, nil)
					if err != nil {
						t.Fatal(err)
					}
					roots, err := index.ListFlowInstances(f.ctx, rootScope)
					if err != nil || len(roots) != 1 || roots[0].Identity() != parent {
						t.Fatalf("sibling corruption contaminated an unrelated exact scope: %+v %v", roots, err)
					}
					// Repeating the missing-key request takes another owned read;
					// it cannot reuse the healthy result captured before the fault.
					if _, found, err := index.LookupFlowInstance(f.ctx, lookup); !errors.As(err, &corruption) || found {
						t.Fatalf("later read cached pre-fault absence: found=%t err=%v", found, err)
					}
					after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(before, after) {
						t.Fatal("corruption refusal or independent scope changed durable evidence")
					}
				})
			}
		})
	}
}
