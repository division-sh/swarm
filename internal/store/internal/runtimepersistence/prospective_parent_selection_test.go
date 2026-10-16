package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type nativeProspectiveSelectionReader struct {
	pipeline.FlowInstanceIndexReader
	inventory []pipeline.FlowInstanceObservation
	listCalls int
	exact     []pipeline.FlowInstanceLookupRequest
}

func (r *nativeProspectiveSelectionReader) ListFlowInstances(ctx context.Context, scope pipeline.FlowInstanceLookupScope) ([]pipeline.FlowInstanceObservation, error) {
	r.listCalls++
	current, err := r.FlowInstanceIndexReader.ListFlowInstances(ctx, scope)
	if err != nil {
		return nil, err
	}
	if r.inventory != nil {
		return r.inventory, nil
	}
	return current, nil
}

func (r *nativeProspectiveSelectionReader) LookupFlowInstance(ctx context.Context, request pipeline.FlowInstanceLookupRequest) (pipeline.FlowInstanceObservation, bool, error) {
	r.exact = append(r.exact, request)
	return r.FlowInstanceIndexReader.LookupFlowInstance(ctx, request)
}

func newNativeProspectiveSelection(t *testing.T, backend string) (receiverConfigActivationFixture, pipeline.FlowInstanceSelectionRequest, pipeline.FlowInstanceActivationPlan) {
	t.Helper()
	f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
		"schema.yaml":        "name: prospective-selection-proof\n",
		"worker/schema.yaml": "name: worker\ninstance: item_id\nstages:\n  pending: {}\n  done: {final: true}\npins:\n  inputs:\n    - item.created\n",
		"worker/state.yaml":  "item:\n  item_id: text\n",
		"worker/events.yaml": "item.created:\n  item_id: text\nitem.finished:\n",
		"worker/nodes.yaml":  "progress:\n  execution_type: system_node\n  subscribes_to: [item.finished]\n  event_handlers:\n    item.finished:\n      advances_to: done\n",
	}, nil)
	runID := correlation.RunIDFromContext(f.ctx)
	rootRequest := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
	rootRequest.Instance = flowidentity.Stored(rootRequest.ContractBundle, ".", runID, runID, runID, "")
	root, err := f.manager.PrepareFlowInstanceActivation(f.ctx, rootRequest)
	if err != nil {
		t.Fatal(err)
	}
	fact, present := correlation.SourceArtifactFactFromContext(f.ctx)
	if !present {
		t.Fatal("selection requires its actual admitted source")
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
		Lookup: lookup, Mode: contracts.FlowInputResolutionModeSelect,
		MissingInstanceID: "not-stored-child", Constructor: constructor, Prepared: []pipeline.FlowInstanceActivationPlan{root},
	}, root
}

func TestR7ProspectiveParentSelectionUsesCurrentNativeReceiverBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, state := range []string{"active", "final", "terminated"} {
			t.Run(backend+"/"+state, func(t *testing.T) {
				f, request, root := newNativeProspectiveSelection(t, backend)
				committer := agentFixtureFlowActivationCommitter{store: f.store}
				if _, err := committer.CommitFlowInstanceActivation(f.ctx, root); err != nil {
					t.Fatal(err)
				}
				constructor := request.Constructor
				var err error
				constructor.Instance, err = flowidentity.KeyedChild(request.Lookup.Source(), root.Identity, "worker", "actual-stored-child")
				if err != nil {
					t.Fatal(err)
				}
				constructor.Instance.EntityID = uuid.NewString()
				child, err := f.manager.PrepareFlowInstanceActivation(f.ctx, constructor)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := committer.CommitFlowInstanceActivation(f.ctx, child); err != nil {
					t.Fatal(err)
				}
				index := f.store.(pipeline.FlowInstanceIndexReader)
				scope, err := pipeline.NewFlowInstanceLookupScope(request.Lookup.Source(), request.Lookup.SourceFact(), request.Lookup.RunID(), []string{"worker"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				inventory, err := index.ListFlowInstances(f.ctx, scope)
				if err != nil || len(inventory) != 1 || inventory[0].Identity() != child.Identity {
					t.Fatalf("original native inventory: %+v err=%v", inventory, err)
				}
				owner := inventory[0].Owner()
				switch state {
				case "final":
					persisted, err := child.PersistenceRecord()
					if err != nil {
						t.Fatal(err)
					}
					mutation := persisted.State
					mutation.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
					mutation.ExpectedRevision, mutation.ExpectedState = 1, mutation.CurrentState
					mutation.CurrentState = "done"
					mutation.EnteredStageAt, mutation.UpdatedAt = child.OccurredAt.Add(time.Second), child.OccurredAt.Add(time.Second)
					mutation = workflowTargetMutationEntry(t, f, mutation, "item.finished")
					if _, err := f.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{State: mutation}); err != nil {
						t.Fatal(err)
					}
				case "terminated":
					if err := f.workflows.MarkTerminated(f.ctx, owner, identity.NormalizeEntityID(child.Identity.EntityID), child.OccurredAt.Add(time.Hour)); err != nil {
						t.Fatal(err)
					}
				}
				reader := &nativeProspectiveSelectionReader{FlowInstanceIndexReader: index, inventory: inventory}
				requireR7SelectionNoMutation(t, f, func() {
					selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, reader, f.manager, request)
					if reader.listCalls != 1 || len(reader.exact) != 1 || reader.exact[0].DeclaredSelection() || reader.exact[0].ExactPath() != child.Identity.InstancePath {
						t.Fatalf("native selection did not bind the narrowed stored owner: list=%d exact=%+v err=%v", reader.listCalls, reader.exact, err)
					}
					if state == "active" {
						if err != nil || selected.Identity() != child.Identity || selected.Activation != nil || !selected.Observation.Valid() {
							t.Fatalf("actual native receiver was not selected: %+v err=%v", selected, err)
						}
						return
					}
					var terminal *pipeline.TerminalReceiverError
					var terminated *pipeline.TerminatedReceiverError
					if state == "final" && (!errors.As(err, &terminal) || terminal.Stage != "done") || state == "terminated" && !errors.As(err, &terminated) {
						t.Fatalf("stale inventory hid the current native refusal: %+v err=%v", selected, err)
					}
					if unavailable, found := selected.UnavailableReceiver(); !found || unavailable != owner || selected.Observation.Valid() || selected.Activation != nil {
						t.Fatalf("refusal minted output/creation or lost exact occupancy: %+v", selected)
					}
				})
			})
		}
	}
}

func TestR7ProspectiveParentAbsenceUsesOnlyAdmittedInventoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, request, root := newNativeProspectiveSelection(t, backend)
			index := f.store.(pipeline.FlowInstanceIndexReader)
			exactParent, err := pipeline.NewExactFlowInstanceLookup(request.Lookup.Source(), request.Lookup.SourceFact(), flowidentity.RunScopedFlowInstance{RunID: request.Lookup.RunID(), Route: root.Identity.Route()})
			if err != nil {
				t.Fatal(err)
			}
			parent, present, err := index.LookupFlowInstance(f.ctx, exactParent)
			if err != nil || present || parent.Valid() {
				t.Fatalf("fixture accidentally persisted the prospective parent: %+v present=%t err=%v", parent, present, err)
			}
			reader := &nativeProspectiveSelectionReader{FlowInstanceIndexReader: index}
			requireR7SelectionNoMutation(t, f, func() {
				selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, reader, f.manager, request)
				var missing *pipeline.WorkflowInstanceLookupMiss
				if !errors.As(err, &missing) || selected.Observation.Valid() || selected.Activation != nil || selected.Proposed != nil || reader.listCalls != 1 || len(reader.exact) != 0 {
					t.Fatalf("absence borrowed parent permission: %+v calls=%d/%d err=%v", selected, reader.listCalls, len(reader.exact), err)
				}
			})
			parentAfter, presentAfter, err := index.LookupFlowInstance(f.ctx, exactParent)
			if err != nil || presentAfter || !reflect.DeepEqual(parent, parentAfter) {
				t.Fatalf("absence created or borrowed its prospective parent: %+v err=%v", parentAfter, err)
			}
		})
	}
}
