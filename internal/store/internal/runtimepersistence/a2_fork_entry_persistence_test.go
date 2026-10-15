package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/accumulator"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

// The actual mutation owner and fork ledger admit the source snapshot. This is
// history/materialization coverage, not child lifecycle execution permission.
func TestA2ForkStageEntryPersistenceOnBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, root := range []bool{true, false} {
				name := "nonroot"
				if root {
					name = "root"
				}
				t.Run(name, func(t *testing.T) {
					opened := backend.open(t)
					flowID, path := "owner", "owner/one"
					files := map[string]string{
						"schema.yaml":         "name: fork-entry-history\n",
						"owner/schema.yaml":   "name: owner\ninstance: owner_key\nstages:\n  active: {}\npins:\n  inputs:\n    - construct.requested\n",
						"owner/entities.yaml": "review_item:\n  owner_key: text\n",
						"owner/events.yaml":   "construct.requested:\n",
					}
					if root {
						flowID, path = ".", "."
						files = map[string]string{
							"schema.yaml":   "name: fork-entry-history\nstages:\n  active: {}\n",
							"entities.yaml": "review_item:\n  owner_key: {type: text, initial: one}\n",
						}
					}
					construction := newReceiverConfigActivationFixtureForStore(t, opened.store.(agentFixtureFlowStore), false, files, nil, ownStoreTestAgentManager, nil)
					f := snapshotOwnershipFixture{store: opened.store.(snapshotOwnershipStore), db: opened.db, ctx: construction.ctx, runID: correlation.RunIDFromContext(construction.ctx)}
					parentFixture := f
					parentFixture.entityID = f.runID
					var parentRecord pipeline.FlowInstanceActivationRecord
					var parentSource pipeline.WorkflowTargetPersistenceRecord
					if !root {
						parentRecord = constructA2ForkStructuralParent(t, &construction)
						parentFixture.entityID = parentRecord.State.EntityID
						parentSource = readA2ForkStructuralParent(t, parentFixture, parentRecord.Identity)
						if parentSource.Lifecycle.State.CurrentState != "pending" || !parentSource.Lifecycle.State.EnteredStageAt.Equal(parentRecord.EnteredStageAt) {
							t.Fatalf("constructed parent is not the exact pending structural root: %#v", parentSource)
						}
					}
					at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
					instanceID := "one"
					if root {
						instanceID = f.runID
					}
					req := sqliteFlowActivationRequest(construction.bundle, flowID, instanceID, "", path)
					req.OccurredAt = at
					if root {
						req.Instance = flowidentity.Stored(req.ContractBundle, ".", f.runID, f.runID, f.runID, "")
					} else {
						parent := flowidentity.Stored(req.ContractBundle, ".", f.runID, f.runID, "", "")
						child, err := flowidentity.KeyedChild(req.ContractBundle, parent, flowID, instanceID)
						if err != nil {
							t.Fatal(err)
						}
						req.Instance = child
						req.ConstructorInput, req.ResolvedKey = "construct.requested", "one"
						req.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "construct.requested", "constructor-fixture", "", []byte(`{}`), 0, f.runID, events.EventEnvelope{}, at)
					}
					activation, err := construction.manager.PrepareFlowInstanceActivation(f.ctx, req)
					if err != nil {
						t.Fatal(err)
					}
					committed, err := (agentFixtureFlowActivationCommitter{store: construction.store}).CommitFlowInstanceActivation(f.ctx, activation)
					if err != nil || !committed.Acknowledged || !committed.Created {
						t.Fatalf("construct fork source: %+v %v", committed, err)
					}
					persisted, err := activation.PersistenceRecord()
					if err != nil {
						t.Fatal(err)
					}
					f.entityID = persisted.State.EntityID
					if activation.Lifecycle.StageEntry == nil || activation.Lifecycle.StageEntry.OriginRunID != "" {
						t.Fatalf("fresh construction = %#v", activation.Lifecycle)
					}
					entry := *activation.Lifecycle.StageEntry
					bookkeeping := map[string]any{}
					if err := workflowlifecycle.StoreStageEntry(bookkeeping, entry); err != nil {
						t.Fatal(err)
					}
					loop, err := loopruntime.New(f.runID, f.entityID, flowID, "review", "revision", "construction", "active", 4, at)
					if err != nil {
						t.Fatal(err)
					}
					captured := loop.Generation()
					node, err := identity.AdmitExecutableNodeDeclaration(flowID, "collector")
					if err != nil {
						t.Fatal(err)
					}
					ref, err := timeridentity.NewJoinRef(node, "result", "active", "join")
					if err != nil {
						t.Fatal(err)
					}
					ref, err = ref.BindStageEntry(entry, captured)
					if err != nil {
						t.Fatal(err)
					}
					arm, err := joinruntime.NewActivation(ref, []string{"a", "b"}, nil, at, time.Time{})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := arm.Add("a", map[string]any{"event_id": "authored-business"}); err != nil {
						t.Fatal(err)
					}
					arm.Status, arm.CloseReason, arm.TimerCancelled, arm.OutcomePending = joinruntime.StatusClosed, joinruntime.CloseReasonUntil, true, true
					if _, err := loop.Repeat("active", "repeat", at.Add(time.Second)); err != nil {
						t.Fatal(err)
					}
					if err := loop.Close("done", "close", at.Add(2*time.Second)); err != nil {
						t.Fatal(err)
					}
					buckets := map[string]map[string]any{}
					if err := loopruntime.Store(buckets, loop); err != nil {
						t.Fatal(err)
					}
					if err := joinruntime.Store(buckets, arm); err != nil {
						t.Fatal(err)
					}
					bucket := timeridentity.NewAccumulatorBucketRefForGeneration(node, "result", captured)
					acc := accumulator.Load(nil)
					if duplicate, err := acc.Admit(&contracts.AccumulateSpec{Into: "items", From: "payload", Key: "payload.id"}, map[string]any{"id": " exact ", "event_id": "authored-business", "payload": map[string]any{"id": "nested-business"}}, "source-delivery"); err != nil || duplicate {
						t.Fatalf("source business state = duplicate %v %v", duplicate, err)
					}
					accRaw, err := json.Marshal(acc)
					if err != nil {
						t.Fatal(err)
					}
					var accValue map[string]any
					if err := json.Unmarshal(accRaw, &accValue); err != nil {
						t.Fatal(err)
					}
					buckets[node.Key()] = map[string]any{"handler_accumulators": map[string]any{bucket.Key(): accValue}}
					f.state = persisted.State
					f.state.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
					f.state.ExpectedState, f.state.ExpectedRevision = "active", 1
					f.state.UpdatedAt = at.Add(3 * time.Second)
					f.state.Slug, f.state.Name = "entry-subject", "Entry Subject"
					f.state.Bookkeeping, err = json.Marshal(bookkeeping)
					if err != nil {
						t.Fatal(err)
					}
					f.state.Accumulator, err = json.Marshal(runtimeengine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets())
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.store.CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{State: f.state}); err != nil {
						t.Fatal(err)
					}
					f.eventID = uuid.NewString()
					event := eventtest.ExistingRunRootIngress(f.eventID, "fork.entry_cut", "entry-proof", "", []byte(`{}`), 0, f.runID, events.EventEnvelope{}, time.Now().UTC())
					if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, event); err != nil {
						t.Fatal(err)
					}
					plan := f.plan(t)
					expected := map[string]pipeline.FlowInstanceActivationRecord{f.entityID: persisted}
					if !root {
						if f.entityID == f.runID {
							t.Fatal("business child aliases its constructed root")
						}
						expected[f.runID] = parentRecord
					}
					seen := map[string]bool{}
					for _, entity := range plan.Entities {
						record, found := expected[entity.EntityID]
						if !found || seen[entity.EntityID] {
							t.Fatalf("extra or duplicate fixed entity: %#v", entity)
						}
						seen[entity.EntityID] = true
						state := f.state
						if !root && entity.EntityID == f.runID {
							state = parentRecord.State
							if state.Identity.Route.ScopeKey != "." || state.ParentInstance != "" || state.InstanceKey != "" || len(entity.Fields) != 0 || entity.CurrentState != "pending" {
								t.Fatalf("fixed structural parent changed scope or business state: %#v", entity)
							}
						} else if !root && (state.Identity.Route.ScopeKey != "owner" || state.ParentInstance != f.runID || state.InstanceKey != "one" || entity.CurrentState != "active") {
							t.Fatalf("fixed business child changed constructor identity: %#v", entity)
						}
						assertA2ForkConstructionMetadata(t, entity, record, state)
					}
					if len(seen) != len(expected) {
						t.Fatalf("missing fixed entity identities: seen=%v expected=%v", seen, expected)
					}
					source := readSnapshotOwnershipEntity(t, f, f.runID)
					request := runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: f.eventID}
					result, err := f.store.MaterializeRunFork(f.ctx, request)
					if err != nil {
						t.Fatal(err)
					}
					childFixture := f
					if root {
						childFixture.entityID = result.ForkRunID
					}
					child := readSnapshotOwnershipEntity(t, childFixture, result.ForkRunID)
					if !root {
						assertA2ForkStructuralParent(t, parentFixture, result.ForkRunID, parentSource)
					}
					inherited, found, err := workflowlifecycle.LoadStageEntry(child.Buckets[2])
					want := entry
					want.RunID, want.OriginRunID = result.ForkRunID, f.runID
					if root {
						want.EntityID, want.InstanceID, want.InstancePath = result.ForkRunID, result.ForkRunID, result.ForkRunID
					}
					if err != nil || !found || inherited != want {
						t.Fatalf("persisted inherited owner = %#v want %#v: %v", inherited, want, err)
					}
					carrier, err := runtimeengine.StateCarrierFromPersisted(nil, nil, nil, child.Buckets[3])
					if err != nil {
						t.Fatal(err)
					}
					arms, err := joinruntime.List(carrier.StateBuckets)
					if err != nil || len(arms) != 1 {
						t.Fatalf("persisted retained arm = %#v %v", arms, err)
					}
					wantGeneration, err := loopruntime.ForkGeneration(captured, result.ForkRunID, childFixture.entityID)
					if err != nil {
						t.Fatal(err)
					}
					if arms[0].Generation() != wantGeneration || arms[0].Generation().Attempt != 1 || arms[0].JoinRef().StageEntry() != want || arms[0].Status != arm.Status || arms[0].CloseReason != arm.CloseReason || !reflect.DeepEqual(arms[0].Outputs, arm.Outputs) || !arms[0].OutcomePending {
						t.Fatal("materialized arm changed its exact history/business payload")
					}
					bucket.Generation = wantGeneration
					stored := carrier.StateBuckets[node.Key()]["handler_accumulators"].(map[string]any)
					gotAccumulator := accumulator.Load(stored[bucket.Key()].(map[string]any))
					if gotAccumulator.Err() != nil || !reflect.DeepEqual(gotAccumulator.Items, acc.Items) || !reflect.DeepEqual(gotAccumulator.Received, acc.Received) {
						t.Fatal("materialized accumulator lost exact business admission/history")
					}
					before := snapshotOwnershipCounts(t, f, result.ForkRunID)
					again, err := f.store.RequireMaterializedRunFork(f.ctx, result.ForkRunID, request)
					if err != nil || again.ForkRunID != result.ForkRunID || snapshotOwnershipCounts(t, f, result.ForkRunID) != before {
						t.Fatalf("fork exact replay changed history: %#v %v", again, err)
					}
					if !root {
						assertA2ForkStructuralParent(t, parentFixture, result.ForkRunID, parentSource)
						if !reflect.DeepEqual(parentSource, readA2ForkStructuralParent(t, parentFixture, parentRecord.Identity)) {
							t.Fatal("fork projection changed its constructed source parent")
						}
					}
					if !reflect.DeepEqual(source, readSnapshotOwnershipEntity(t, f, f.runID)) || !reflect.DeepEqual(plan.Entities, f.plan(t).Entities) {
						t.Fatal("fork projection changed source/fixed history")
					}
					var childEvents int
					if err := f.db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1`, result.ForkRunID).Scan(&childEvents); err != nil || childEvents != 0 {
						t.Fatalf("history projection invented child events: %d %v", childEvents, err)
					}
				})
			}
		})
	}
}

func constructA2ForkStructuralParent(t *testing.T, fixture *receiverConfigActivationFixture) pipeline.FlowInstanceActivationRecord {
	t.Helper()
	runID := correlation.RunIDFromContext(fixture.ctx)
	request := sqliteFlowActivationRequest(fixture.bundle, ".", runID, "", runID)
	request.Instance = flowidentity.Stored(request.ContractBundle, ".", runID, runID, runID, "")
	plan, err := fixture.manager.PrepareFlowInstanceActivation(fixture.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.ConstructionPlans()) != 1 || len(plan.Instance.Fields) != 0 {
		t.Fatal("fork fixture requires one entityless keyless root")
	}
	record, err := plan.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	committed, err := (agentFixtureFlowActivationCommitter{store: fixture.store}).CommitFlowInstanceActivation(fixture.ctx, plan)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct fork parent: %+v %v", committed, err)
	}
	fixture.rootConstructed = true
	return record
}

func assertA2ForkConstructionMetadata(t *testing.T, entity runfork.RunForkEntityState, record pipeline.FlowInstanceActivationRecord, state pipeline.WorkflowEngineStateRecord) {
	t.Helper()
	want := runfork.RunForkMaterializedEntitySnapshotMetadata{
		InitialMaterialization: canonicalSnapshotOwnershipJSON(t, record.InitialMaterialization),
		FlowConfig:             canonicalSnapshotOwnershipJSON(t, state.Config),
		Owner:                  runfork.RunForkMaterializedEntitySnapshotMetadataOwner,
		Source:                 runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance,
		FlowInstance:           state.Identity.Route.InstancePath, FlowTemplate: state.WorkflowName,
		ParentInstance: state.ParentInstance, InstanceKey: state.InstanceKey,
		EntityType: state.EntityType, Slug: state.Slug, Name: state.Name,
		StageDefined: state.StageDefined, Mode: state.Mode, Status: state.Status,
		CreatedAt: state.CreatedAt, UpdatedAt: state.UpdatedAt, TerminatedAt: state.TerminatedAt, EnteredStateAt: state.EnteredStageAt,
	}
	if entity.MaterializationMetadata == nil {
		t.Fatal("fixed entity lost source-bound construction metadata")
	}
	got := *entity.MaterializationMetadata
	got.InitialMaterialization = canonicalSnapshotOwnershipJSON(t, got.InitialMaterialization)
	got.FlowConfig = canonicalSnapshotOwnershipJSON(t, got.FlowConfig)
	if !reflect.DeepEqual(got, want) || entity.EnteredStateAt == nil || !entity.EnteredStateAt.Equal(state.EnteredStageAt) {
		t.Fatalf("fixed metadata differs from independently admitted setup: got=%#v want=%#v", got, want)
	}
}

func readA2ForkStructuralParent(t *testing.T, fixture snapshotOwnershipFixture, owner flowidentity.RunScopedFlowInstance) pipeline.WorkflowTargetPersistenceRecord {
	t.Helper()
	entityID := identity.NormalizeEntityID(fixture.entityID)
	target, err := fixture.store.(pipeline.WorkflowTargetPersistenceReader).LoadWorkflowTargetPersistence(fixture.ctx, owner, entityID)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Validate(owner.Route, entityID); err != nil || target.Presence != pipeline.WorkflowTargetPersistenceCompleteFieldless || target.Lifecycle.ParentInstance != "" || target.Lifecycle.InstanceKey != "" || target.Lifecycle.WorkflowName != "." {
		t.Fatalf("parent is not the exact constructed fieldless root: %#v %v", target, err)
	}
	return target
}

func assertA2ForkStructuralParent(t *testing.T, fixture snapshotOwnershipFixture, forkRunID string, source pipeline.WorkflowTargetPersistenceRecord) {
	t.Helper()
	forkFixture := fixture
	forkFixture.entityID = forkRunID
	owner, err := flowidentity.NewRunScopedFlowInstance(forkRunID, flowidentity.StoredRoute(".", forkRunID, forkRunID))
	if err != nil {
		t.Fatal(err)
	}
	got := readA2ForkStructuralParent(t, forkFixture, owner)
	header, sourceHeader := got.Lifecycle.State, source.Lifecycle.State
	if header.FlowInstance != forkRunID || header.EntityID != forkRunID || header.CurrentState != sourceHeader.CurrentState || header.Revision != 1 || !header.EnteredStageAt.Equal(sourceHeader.EnteredStageAt) || got.Lifecycle.Mode != source.Lifecycle.Mode || got.Lifecycle.StageDefined != source.Lifecycle.StageDefined {
		t.Fatalf("fork-local structural parent changed captured state/entry: got=%#v source=%#v", got, source)
	}
	if !reflect.DeepEqual(header.Gates, sourceHeader.Gates) || !reflect.DeepEqual(header.Accumulator, sourceHeader.Accumulator) {
		t.Fatal("fork-local structural parent changed gates or accumulator")
	}
	var sourceBookkeeping, forkBookkeeping map[string]any
	if err := json.Unmarshal(sourceHeader.Bookkeeping, &sourceBookkeeping); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(header.Bookkeeping, &forkBookkeeping); err != nil {
		t.Fatal(err)
	}
	sourceEntry, found, err := workflowlifecycle.LoadStageEntry(sourceBookkeeping)
	if err != nil {
		t.Fatal(err)
	}
	forkEntry, forkFound, err := workflowlifecycle.LoadStageEntry(forkBookkeeping)
	if err != nil || found != forkFound {
		t.Fatalf("structural parent stage-entry presence changed: source=%t fork=%t %v", found, forkFound, err)
	}
	if found {
		want := sourceEntry
		want.RunID, want.OriginRunID = forkRunID, fixture.runID
		want.EntityID, want.InstanceID, want.InstancePath = forkRunID, forkRunID, forkRunID
		if forkEntry != want {
			t.Fatalf("structural parent entry not rebased by historical owner: got=%#v want=%#v", forkEntry, want)
		}
	} else if !reflect.DeepEqual(forkBookkeeping, sourceBookkeeping) {
		t.Fatal("structural parent invented bookkeeping")
	}
}
