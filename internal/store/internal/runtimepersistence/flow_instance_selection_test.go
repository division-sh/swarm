package runtimepersistence

import (
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func newR7SelectionProof(t *testing.T, backend string) (receiverConfigActivationFixture, pipeline.FlowInstanceSelectionRequest) {
	t.Helper()
	f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
		"schema.yaml":          "name: selection-proof\n",
		"worker/schema.yaml":   "name: worker\ninstance: item_id\npins:\n  inputs:\n    - item.created\n",
		"worker/entities.yaml": "item:\n  item_id: text\n",
		"worker/events.yaml":   "item.created:\n  item_id: text\n",
	}, nil)
	runID := correlation.RunIDFromContext(f.ctx)
	rootRequest := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
	rootRequest.Instance = flowidentity.Stored(rootRequest.ContractBundle, ".", runID, runID, runID, "")
	root, err := f.manager.PrepareFlowInstanceActivation(f.ctx, rootRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, root); err != nil {
		t.Fatal(err)
	}
	fact, present := correlation.SourceArtifactFactFromContext(f.ctx)
	if !present {
		t.Fatal("selection proof requires admitted source")
	}
	keys, err := pipeline.AdmitFlowInstanceKeyMaterial(rootRequest.ContractBundle, "worker", "business-key")
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := pipeline.NewDeclaredFlowInstanceLookup(rootRequest.ContractBundle, fact, runID, "worker", root.Identity, keys)
	if err != nil {
		t.Fatal(err)
	}
	constructor := sqliteFlowActivationRequest(f.bundle, "worker", "", "", "")
	constructor.Instance = flowidentity.Instance{}
	constructor.ConstructorInput, constructor.ResolvedKey = "item.created", "business-key"
	constructor.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "item.created", "constructor-fixture", "", []byte(`{"item_id":"business-key"}`), 0, runID, events.EventEnvelope{}, constructor.OccurredAt)
	return f, pipeline.FlowInstanceSelectionRequest{
		Lookup: lookup, Mode: contracts.FlowInputResolutionModeSelectOrCreate,
		MissingInstanceID: "proposed-receiver", Constructor: constructor,
	}
}

func requireR7SelectionNoMutation(t *testing.T, f receiverConfigActivationFixture, operation func()) {
	t.Helper()
	before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	operation()
	after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("selection preparation or refusal mutated durable evidence")
	}
}

func TestR7SelectCreateSelectOrCreateBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, request := newR7SelectionProof(t, backend)
			index := f.store.(pipeline.FlowInstanceIndexReader)
			for _, mode := range []contracts.FlowInputResolutionMode{contracts.FlowInputResolutionModeSelect, contracts.FlowInputResolutionModeCreate, contracts.FlowInputResolutionModeSelectOrCreate} {
				t.Run("absent_"+contracts.FlowInputResolutionModeCode(mode), func(t *testing.T) {
					request := request
					request.Mode = mode
					requireR7SelectionNoMutation(t, f, func() {
						selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request)
						if mode == contracts.FlowInputResolutionModeSelect {
							var missing *pipeline.WorkflowInstanceLookupMiss
							if !errors.As(err, &missing) || selected.Activation != nil || selected.Observation.Valid() {
								t.Fatalf("select created an absent receiver: %+v %v", selected, err)
							}
							return
						}
						if err != nil || selected.Activation == nil || selected.Observation.Valid() || selected.Identity().InstanceID != request.MissingInstanceID || selected.Activation.Instance.InstanceKey != "business-key" {
							t.Fatalf("missing receiver preparation: %+v %v", selected, err)
						}
					})
				})
			}
			storedRequest := request.Constructor
			var err error
			storedRequest.Instance, err = flowidentity.KeyedChild(request.Lookup.Source(), request.Lookup.ParentIdentity(), "worker", "actual-stored-coordinate")
			if err != nil {
				t.Fatal(err)
			}
			storedRequest.Instance.EntityID = uuid.NewString()
			winner, err := f.manager.PrepareFlowInstanceActivation(f.ctx, storedRequest)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, winner); err != nil {
				t.Fatal(err)
			}
			for _, mode := range []contracts.FlowInputResolutionMode{contracts.FlowInputResolutionModeSelect, contracts.FlowInputResolutionModeCreate, contracts.FlowInputResolutionModeSelectOrCreate} {
				t.Run("occupied_"+contracts.FlowInputResolutionModeCode(mode), func(t *testing.T) {
					request := request
					request.Mode = mode
					requireR7SelectionNoMutation(t, f, func() {
						selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request)
						if mode == contracts.FlowInputResolutionModeCreate {
							var conflict *pipeline.FlowInstanceActivationConflict
							if !errors.As(err, &conflict) || conflict.Owner.Route != winner.Identity.Route() || selected.Activation != nil {
								t.Fatalf("create hid occupied actual identity: %+v %v", selected, err)
							}
							return
						}
						if err != nil || selected.Activation != nil || !selected.Observation.Valid() || selected.Identity() != winner.Identity {
							t.Fatalf("selection reconstructed an existing receiver: %+v %v", selected, err)
						}
					})
				})
			}
			owner := flowidentity.RunScopedFlowInstance{RunID: request.Lookup.RunID(), Route: winner.Identity.Route()}
			if err := f.workflows.MarkTerminated(f.ctx, owner, identity.NormalizeEntityID(winner.Identity.EntityID), winner.OccurredAt.AddDate(0, 0, 1)); err != nil {
				t.Fatal(err)
			}
			for _, mode := range []contracts.FlowInputResolutionMode{contracts.FlowInputResolutionModeSelect, contracts.FlowInputResolutionModeCreate, contracts.FlowInputResolutionModeSelectOrCreate} {
				t.Run("terminal_"+contracts.FlowInputResolutionModeCode(mode), func(t *testing.T) {
					request := request
					request.Mode = mode
					requireR7SelectionNoMutation(t, f, func() {
						selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request)
						if err == nil || selected.Activation != nil || selected.Observation.Valid() {
							t.Fatalf("terminal occupancy became creation or execution: %+v %v", selected, err)
						}
					})
				})
			}
		})
	}
}

func TestR7ConstructionCommitRechecksSelectorAndPathBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, request := newR7SelectionProof(t, backend)
			index := f.store.(pipeline.FlowInstanceIndexReader)
			first, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request)
			if err != nil || first.Activation == nil {
				t.Fatalf("first pure creation: %+v %v", first, err)
			}
			requireR7SelectionNoMutation(t, f, func() {
				preview := request
				preview.Prepared = []pipeline.FlowInstanceActivationPlan{*first.Activation}
				for _, mode := range []contracts.FlowInputResolutionMode{contracts.FlowInputResolutionModeSelect, contracts.FlowInputResolutionModeSelectOrCreate} {
					preview.Mode = mode
					selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, preview)
					if err != nil || selected.Activation != nil || selected.Observation.Valid() || selected.Proposed == nil || selected.Identity() != first.Identity() {
						t.Fatalf("prepared creation did not feed later selection: %+v %v", selected, err)
					}
				}
				preview.Mode = contracts.FlowInputResolutionModeCreate
				if _, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, preview); err == nil {
					t.Fatal("second explicit create reused an operation-local proposal")
				}
			})
			request.MissingInstanceID = "different-proposed-path"
			second, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request)
			if err != nil || second.Activation == nil {
				t.Fatalf("second pure creation: %+v %v", second, err)
			}
			committer := agentFixtureFlowActivationCommitter{store: f.store}
			if _, err := committer.CommitFlowInstanceActivation(f.ctx, *first.Activation); err != nil {
				t.Fatal(err)
			}
			requireR7SelectionNoMutation(t, f, func() {
				committed, err := committer.CommitFlowInstanceActivation(f.ctx, *second.Activation)
				var conflict *pipeline.FlowInstanceActivationConflict
				if !errors.As(err, &conflict) || committed.Acknowledged {
					t.Fatalf("stale absent selection created another coordinate: %+v %v", committed, err)
				}
			})
			selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request)
			if err != nil || selected.Activation != nil || selected.Identity() != first.Identity() {
				t.Fatalf("rolled-back preparation did not reuse durable winner: %+v %v", selected, err)
			}
			request.Mode = contracts.FlowInputResolutionModeCreate
			requireR7SelectionNoMutation(t, f, func() {
				if _, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request); err == nil {
					t.Fatal("explicit create became implicit winner reuse")
				}
			})
		})
	}
}
