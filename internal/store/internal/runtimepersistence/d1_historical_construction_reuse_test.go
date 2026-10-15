package runtimepersistence

import (
	"bytes"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func TestD1HistoricalObservationKeepsLegitimateProgressBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newOrdinaryForkPredecessorFixture(t, backend)
			lookup, err := pipeline.NewExactFlowInstanceLookup(semanticview.Wrap(f.bundle), mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash()), f.owner)
			if err != nil {
				t.Fatal(err)
			}
			index := f.store.(pipeline.FlowInstanceIndexReader)
			original, found, err := index.LookupFlowInstance(f.ctx, lookup)
			if err != nil || !found {
				t.Fatalf("historical progress lacks its positive construction: found=%t err=%v", found, err)
			}
			initial, err := original.WorkflowInstance()
			fixed, historical := original.HistoricalConstruction()
			if err != nil || !historical {
				t.Fatalf("historical progress lost its fixed evidence: %+v err=%v", fixed, err)
			}
			result, err := f.workflows.ApplyEntityFieldMutation(f.ctx, pipeline.EntityFieldMutation{
				RunID: f.owner.RunID, EntityID: f.entityID, Owner: f.owner, FlowID: "review", Source: lookup.Source(),
				Mutation: entityruntime.Mutation{Target: "entity.label", Value: "legitimate-progress"},
				Writer:   mutationlog.Writer{Type: "agent", ID: "historical-progress-proof", HandlerStep: "save_entity_field"},
			})
			if err != nil || !result.Acknowledged || int64(result.Revision) != initial.Revision+1 {
				t.Fatalf("canonical historical progress was not acknowledged: %+v err=%v", result, err)
			}
			before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
			current, found, err := index.LookupFlowInstance(f.ctx, lookup)
			if err != nil || !found {
				t.Fatalf("legitimate progress became construction corruption: found=%t err=%v", found, err)
			}
			header, err := current.WorkflowInstance()
			observed, stillHistorical := current.HistoricalConstruction()
			if err != nil || !stillHistorical || observed != fixed || header.Revision != initial.Revision+1 || header.Fields["label"] != "legitimate-progress" || !header.CreatedAt.Equal(initial.CreatedAt) {
				t.Fatalf("observation replaced immutable construction or discarded progress: fixed=%+v header=%+v err=%v", observed, header, err)
			}
			if after := forkReceiverFaultSnapshot(t, f.ctx, f.store); !reflect.DeepEqual(before, after) {
				t.Fatal("historical observation wrote or repaired progressed state")
			}
			restore, err := FaultFlowConstructorHeaderForTest(f.ctx, f.store, f.owner, pipelinepersistence.FlowConstructorHeaderFaultField("created_at"), initial.CreatedAt.Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := restore(f.ctx); err != nil {
					t.Error(err)
				}
			}()
			before = forkReceiverFaultSnapshot(t, f.ctx, f.store)
			invalid, found, err := index.LookupFlowInstance(f.ctx, lookup)
			var corruption *pipeline.FlowInstanceConstructionCorruption
			if found || invalid.Valid() || !errors.As(err, &corruption) || !strings.Contains(err.Error(), "original materialization birth") {
				t.Fatalf("changed historical birth was not exact corruption: found=%t err=%v", found, err)
			}
			if after := forkReceiverFaultSnapshot(t, f.ctx, f.store); !reflect.DeepEqual(before, after) {
				t.Fatal("historical birth refusal changed durable evidence")
			}
		})
	}
}

func TestD1HistoricalReforkExactRequireSurvivesReopenBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, connector := newP16RaceStore(t, backend)
			files := map[string]string{
				"schema.yaml":          "name: historical-reopen\n",
				"review/schema.yaml":   "name: review\ninstance: request_id\nstages: {pending: {}}\npins:\n  inputs: [task.create]\n",
				"review/entities.yaml": "review_item:\n  request_id: text\n  label: text\n  nested: json\n  enabled: {type: boolean, initial: true}\n",
				"review/events.yaml":   "task.create:\n  request_id: text\n  label: text\n  nested: json\ntask.started:\n",
			}
			component := newReceiverConfigActivationFixtureForStore(t, any(selected).(agentFixtureFlowStore), false, files, nil, ownStoreTestAgentManager, nil)
			f := newOrdinaryForkPredecessorFixtureForReceiver(t, backend, component)
			component.ctx = f.ctx
			fork := materializeReceiverConstructionProofFork(t, component, f.owner.RunID)
			request := runfork.RunForkMaterializeRequest{SourceRunID: f.owner.RunID, At: fork.ForkPoint.Input}
			owner := flowidentity.RunScopedFlowInstance{RunID: fork.ForkRunID, Route: f.owner.Route}
			lookup, err := pipeline.NewExactFlowInstanceLookup(semanticview.Wrap(f.bundle), mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash()), owner)
			if err != nil {
				t.Fatal(err)
			}
			original, found, err := selected.(pipeline.FlowInstanceIndexReader).LookupFlowInstance(f.ctx, lookup)
			if err != nil || !found {
				t.Fatalf("historical reopen lacks admitted construction: found=%t err=%v", found, err)
			}
			header, err := original.WorkflowInstance()
			if err != nil {
				t.Fatal(err)
			}
			if err := f.manager.Shutdown(); err != nil {
				t.Fatal(err)
			}
			before := forkReceiverFaultSnapshot(t, f.ctx, selected)
			reopened, _ := c4ReopenCreationStore(t, backend, selected, db, connector)
			defer func() {
				if err := reopened.(interface{ Close() error }).Close(); err != nil {
					t.Error(err)
				}
			}()
			got, err := reopened.(c4ForkCreationOwner).RequireMaterializedRunFork(f.ctx, fork.ForkRunID, request)
			if err != nil || !reflect.DeepEqual(got, fork) {
				t.Fatalf("reopened exact historical child changed: %+v err=%v", got, err)
			}
			observed, found, err := reopened.(pipeline.FlowInstanceIndexReader).LookupFlowInstance(f.ctx, lookup)
			if err != nil || !found {
				t.Fatalf("reopened construction lost its exact source cut: found=%t err=%v", found, err)
			}
			fixed, historical := observed.HistoricalConstruction()
			prior, wasHistorical := original.HistoricalConstruction()
			actual, err := observed.WorkflowInstance()
			if err != nil || !historical || !wasHistorical || fixed != prior || !reflect.DeepEqual(actual, header) {
				t.Fatalf("reopened historical construction changed: fixed=%+v header=%+v err=%v", fixed, actual, err)
			}
			if _, err := reopened.(pipeline.FlowConstructionPublicationReader).LoadFlowConstructionPublication(f.ctx, owner, observed.Identity().EntityID); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("reopened history manufactured native construction: %v", err)
			}
			if after := forkReceiverFaultSnapshot(t, f.ctx, reopened); !reflect.DeepEqual(before, after) {
				t.Fatal("reopen/exact recovery changed complete durable evidence")
			}
		})
	}
}

func TestD1HistoricalReforkExactRequireKeepsFixedCutBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newOrdinaryForkPredecessorFixture(t, backend)
			component := f.receiverConfigActivationFixture
			component.ctx = f.ctx
			fork := materializeReceiverConstructionProofFork(t, component, f.owner.RunID)
			request := runfork.RunForkMaterializeRequest{SourceRunID: f.owner.RunID, At: fork.ForkPoint.Input}
			if request.At == "" || request.At != fork.ForkPoint.EventID || fork.SourceRunID != request.SourceRunID || fork.ForkRunID == request.SourceRunID {
				t.Fatalf("refork did not retain its explicit source event: %+v", fork)
			}
			forks := f.store.(runForkSelectedLifecycleStore)
			cutRequest := runfork.RunForkPlanRequest{SourceRunID: request.SourceRunID, At: request.At}
			cut, err := forks.PlanRunFork(f.ctx, cutRequest)
			if err != nil || cut.ForkPoint != fork.ForkPoint || len(cut.Entities) != 3 || fork.MaterializedEntityCount != len(cut.Entities) {
				t.Fatalf("refork lost root or one of its two captured receivers: plan=%+v fork=%+v err=%v", cut, fork, err)
			}
			var original runfork.RunForkEntityState
			for _, entity := range cut.Entities {
				if entity.EntityID == f.entityID {
					original = entity
				}
			}
			if original.MaterializationMetadata == nil || original.MaterializationMetadata.HistoricalConstruction == nil || !d1CapturedNativeConstructionAbsent(original.MaterializationMetadata.InitialMaterialization) {
				t.Fatal("refork fixture must capture historical construction without a fresh source receipt")
			}
			ancestor := original.MaterializationMetadata.HistoricalConstruction
			if ancestor.SourceRunID != f.sourceOwner.RunID || ancestor.SourceRevision != f.cutRevision || ancestor.SourceEntity.EntityID != f.sourceEntity || ancestor.SourceEntity.MaterializationMetadata == nil || len(ancestor.SourceEntity.MaterializationMetadata.InitialMaterialization) == 0 {
				t.Fatalf("refork fixture lost its original native constructor and exact first cut: %+v", ancestor)
			}
			childState, err := ReadRuntimeLogRunSnapshotForTest(f.ctx, f.store, fork.ForkRunID)
			if err != nil || childState.Validate() != nil || childState.State != runlifecycle.StatePaused || childState.StartedAt.IsZero() || childState.BundleHash != f.bundle.SourceArtifact.BundleHash() || childState.Origin.Kind() != runlifecycle.OriginForkMaterialization || childState.Origin.SourceRunID() != request.SourceRunID || childState.Origin.SourceEventID() != request.At || childState.Origin.ForkRevision() != cut.ForkPoint.Revision || childState.Origin.ForkPointKind() != cut.ForkPoint.Kind {
				t.Fatalf("refork lost its exact persisted lifecycle/origin: %+v err=%v", childState, err)
			}
			startRevision, start, err := c4LoadStartProjection(t, f.ctx, f.store, fork.ForkRunID)
			if err != nil || startRevision != 1 || start.SourceBundleHash != childState.BundleHash {
				t.Fatalf("refork lacks its original start projection: revision=%d projection=%+v err=%v", startRevision, start, err)
			}
			childCutRequest := runfork.RunForkPlanRequest{SourceRunID: fork.ForkRunID, AtStart: true}
			childCut, err := forks.PlanRunFork(f.ctx, childCutRequest)
			if err != nil || childCut.ForkPoint != (runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 1}) || len(childCut.Entities) != len(cut.Entities) {
				t.Fatalf("refork start omitted its captured construction: %+v err=%v", childCut, err)
			}
			for _, entity := range childCut.Entities {
				metadata := entity.MaterializationMetadata
				if metadata == nil || metadata.HistoricalConstruction == nil || !d1CapturedNativeConstructionAbsent(metadata.InitialMaterialization) {
					t.Fatalf("refork manufactured native construction: entity=%+v", entity)
				}
				history := metadata.HistoricalConstruction
				if history.SourceRunID != request.SourceRunID || history.SourceRevision != cut.ForkPoint.Revision {
					t.Fatalf("refork collapsed its immediate source cut: %+v", history)
				}
				matched := false
				for _, source := range cut.Entities {
					if history.SourceEntity.EntityID == source.EntityID {
						matched = reflect.DeepEqual(history.SourceEntity, source)
						break
					}
				}
				if !matched {
					t.Fatalf("refork changed the full captured source construction/state: %+v", history.SourceEntity)
				}
				owner := flowidentity.RunScopedFlowInstance{RunID: fork.ForkRunID, Route: flowidentity.StoredRoute(metadata.FlowTemplate, flowidentity.LogicalInstanceID(metadata.FlowInstance), metadata.FlowInstance)}
				if _, err := f.store.(pipeline.FlowConstructionPublicationReader).LoadFlowConstructionPublication(f.ctx, owner, entity.EntityID); !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("historical refork has a physical native receipt at %s: %v", metadata.FlowInstance, err)
				}
			}
			owner := flowidentity.RunScopedFlowInstance{RunID: fork.ForkRunID, Route: f.owner.Route}
			lookup, err := pipeline.NewExactFlowInstanceLookup(semanticview.Wrap(f.bundle), mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash()), owner)
			if err != nil {
				t.Fatal(err)
			}
			index := f.store.(pipeline.FlowInstanceIndexReader)
			observed, found, err := index.LookupFlowInstance(f.ctx, lookup)
			if err != nil || !found || !observed.Valid() || observed.Identity().EntityID != f.entityID || observed.InstanceKey() != "original" {
				t.Fatalf("historical refork lost its exact receiver: found=%t observation=%+v err=%v", found, observed, err)
			}
			fixed, historical := observed.HistoricalConstruction()
			if !historical || fixed.SourceOwner != f.owner || fixed.SourceRevision != cut.ForkPoint.Revision || fixed.Identity != observed.Identity() {
				t.Fatalf("historical refork observation lost immediate provenance: %+v", fixed)
			}
			if _, native, err := observed.NativeConstruction(); err != nil || native {
				t.Fatalf("historical observation became native: native=%t err=%v", native, err)
			}
			header, err := observed.WorkflowInstance()
			if err != nil || header.CurrentState != "pending" || header.InstanceKey != "original" || header.ParentFlowInstance != fork.ForkRunID || header.Fields["label"] != "source" {
				t.Fatalf("historical refork changed constructor/state/parent: %+v err=%v", header, err)
			}
			readExact := func() {
				t.Helper()
				before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
				got, err := forks.RequireMaterializedRunFork(f.ctx, fork.ForkRunID, request)
				if err != nil || !reflect.DeepEqual(got, fork) {
					t.Fatalf("exact historical materialization changed: got=%+v want=%+v err=%v", got, fork, err)
				}
				if current, err := ReadRuntimeLogRunSnapshotForTest(f.ctx, f.store, fork.ForkRunID); err != nil || !reflect.DeepEqual(current, childState) {
					t.Fatalf("exact read changed child lifecycle/origin/start: %+v err=%v", current, err)
				}
				if revision, current, err := c4LoadStartProjection(t, f.ctx, f.store, fork.ForkRunID); err != nil || revision != startRevision || !reflect.DeepEqual(current, start) {
					t.Fatalf("exact read changed original start/revision: revision=%d projection=%+v err=%v", revision, current, err)
				}
				if current, err := forks.PlanRunFork(f.ctx, cutRequest); err != nil || !reflect.DeepEqual(current, cut) {
					t.Fatalf("exact read changed original source cut: %+v err=%v", current, err)
				}
				if current, err := forks.PlanRunFork(f.ctx, childCutRequest); err != nil || !reflect.DeepEqual(current, childCut) {
					t.Fatalf("exact read changed original child state: %+v err=%v", current, err)
				}
				current, found, err := index.LookupFlowInstance(f.ctx, lookup)
				if err != nil || !found {
					t.Fatalf("repeat historical lookup: found=%t err=%v", found, err)
				}
				currentHeader, err := current.WorkflowInstance()
				currentFixed, historical := current.HistoricalConstruction()
				if err != nil || !historical || current.Identity() != observed.Identity() || !reflect.DeepEqual(currentHeader, header) || !reflect.DeepEqual(currentFixed, fixed) {
					t.Fatalf("repeat historical lookup changed exact construction/header: fixed=%+v header=%+v err=%v", currentFixed, currentHeader, err)
				}
				if after := forkReceiverFaultSnapshot(t, f.ctx, f.store); !reflect.DeepEqual(before, after) {
					t.Fatal("exact historical reads changed complete durable source/child evidence")
				}
			}
			readExact()
			readExact()

			later := constructHistoricalSourceFixture(t, f.ctx, f.store, component.request("after-cut", "ti-after-cut", "later"))
			laterRevision := ordinaryForkPredecessorCheckpoint(t, component, backend)
			if laterRevision <= cut.ForkPoint.Revision {
				t.Fatalf("source did not advance beyond the selected cut: revision=%d cut=%d", laterRevision, cut.ForkPoint.Revision)
			}
			latest, err := forks.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: request.SourceRunID})
			if err != nil || latest.ForkPoint.Revision != laterRevision || latest.ForkPoint.EventID == request.At || len(latest.Entities) != len(cut.Entities)+1 {
				t.Fatalf("post-cut source progress was not durable: %+v err=%v", latest, err)
			}
			before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
			laterLookup, err := pipeline.NewExactFlowInstanceLookup(lookup.Source(), lookup.SourceFact(), flowidentity.RunScopedFlowInstance{RunID: fork.ForkRunID, Route: later.Identity.Route()})
			if err != nil {
				t.Fatal(err)
			}
			if extra, found, err := index.LookupFlowInstance(f.ctx, laterLookup); err != nil || found || extra.Valid() {
				t.Fatalf("historical child borrowed a post-cut source receiver: found=%t err=%v", found, err)
			}
			omitted := request
			omitted.At = ""
			wantOmitted := fork
			wantOmitted.ForkPoint.Input = ""
			if current, err := forks.RequireMaterializedRunFork(f.ctx, fork.ForkRunID, omitted); err != nil || !reflect.DeepEqual(current, wantOmitted) {
				t.Fatalf("exact child followed the later source head for an omitted selector: %+v err=%v", current, err)
			}
			changed := request
			changed.At = latest.ForkPoint.EventID
			if current, err := forks.RequireMaterializedRunFork(f.ctx, fork.ForkRunID, changed); err == nil || !reflect.DeepEqual(current, runfork.RunForkMaterialization{}) {
				t.Fatalf("exact child accepted a different source cut: %+v err=%v", current, err)
			}
			readExact()
			readExact()
			if after := forkReceiverFaultSnapshot(t, f.ctx, f.store); !reflect.DeepEqual(before, after) {
				t.Fatal("post-cut exact recovery changed source or child storage")
			}
		})
	}
}

func d1CapturedNativeConstructionAbsent(raw []byte) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) == 0 || bytes.Equal(raw, []byte("null"))
}

func TestD1HistoricalConstructionReuseRefusesNativeConflictBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newOrdinaryForkPredecessorFixture(t, backend)
			lookup, err := pipeline.NewExactFlowInstanceLookup(semanticview.Wrap(f.bundle), mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash()), f.owner)
			if err != nil {
				t.Fatal(err)
			}
			index := f.store.(pipeline.FlowInstanceIndexReader)
			original, found, err := index.LookupFlowInstance(f.ctx, lookup)
			if err != nil || !found || !original.Valid() {
				t.Fatalf("native-conflict control lacks a valid historical receiver: found=%t err=%v", found, err)
			}
			fixed, historical := original.HistoricalConstruction()
			if !historical || fixed.SourceOwner != f.sourceOwner || fixed.SourceRevision != f.cutRevision {
				t.Fatalf("native-conflict control lost exact source/cut: %+v", fixed)
			}
			if _, native, err := original.NativeConstruction(); err != nil || native {
				t.Fatalf("native-conflict control was already native: native=%t err=%v", native, err)
			}
			captured, err := ReadReceiverHistoricalEntityStateForTest(f.ctx, f.store, f.sourceOwner, f.sourceEntity, f.cutRevision)
			if err != nil || captured.MaterializationMetadata == nil || len(captured.MaterializationMetadata.InitialMaterialization) == 0 {
				t.Fatalf("conflict fault lacks the real captured native source: %+v err=%v", captured, err)
			}
			if err := CopyReceiverConstructionReceiptForTest(f.ctx, f.store, f.sourceOwner, f.owner, f.sourceEntity, f.entityID); err != nil {
				t.Fatalf("apply exact named dual-evidence fault: %v", err)
			}
			before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
			invalid, found, err := index.LookupFlowInstance(f.ctx, lookup)
			if err == nil || found || invalid.Valid() || !strings.Contains(err.Error(), "conflicting native and historical construction evidence") {
				t.Fatalf("native conflict did not reach the historical-construction refusal: found=%t observation=%+v err=%v", found, invalid, err)
			}
			if current, err := ReadReceiverHistoricalEntityStateForTest(f.ctx, f.store, f.sourceOwner, f.sourceEntity, f.cutRevision); err != nil || !reflect.DeepEqual(current, captured) {
				t.Fatalf("native conflict changed the original immutable source construction: %+v err=%v", current, err)
			}
			if after := forkReceiverFaultSnapshot(t, f.ctx, f.store); !reflect.DeepEqual(before, after) {
				t.Fatal("native-conflict refusal repaired or mutated durable evidence")
			}
		})
	}
}
