package runtimepersistence

import (
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func TestR7HistoricalHeaderVersionMatchesCapturedCutBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newOrdinaryForkPredecessorFixture(t, backend)
			component := f.receiverConfigActivationFixture
			component.ctx = f.ctx
			fork := materializeReceiverConstructionProofFork(t, component, f.owner.RunID)
			owner := flowidentity.RunScopedFlowInstance{RunID: fork.ForkRunID, Route: f.owner.Route}
			lookup, err := pipeline.NewExactFlowInstanceLookup(semanticview.Wrap(f.bundle), mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash()), owner)
			if err != nil {
				t.Fatal(err)
			}
			index := f.store.(pipeline.FlowInstanceIndexReader)
			observed, found, err := index.LookupFlowInstance(f.ctx, lookup)
			if err != nil || !found {
				t.Fatalf("captured version positive failed: found=%t err=%v", found, err)
			}
			fixed, historical := observed.HistoricalConstruction()
			header, err := observed.WorkflowInstance()
			if err != nil || !historical || fixed.WorkflowVersion == "" || fixed.WorkflowVersion != header.WorkflowVersion {
				t.Fatalf("observation lost captured header version: fixed=%+v header=%+v err=%v", fixed, header, err)
			}
			captured, err := ReadReceiverHistoricalEntityStateForTest(f.ctx, f.store, fixed.SourceOwner, observed.Identity().EntityID, fixed.SourceRevision)
			if err != nil || captured.MaterializationMetadata == nil {
				t.Fatalf("version fault lacks original fixed evidence: %v", err)
			}
			recorded, err := pipeline.DecodeWorkflowInstanceRecordedHeader(fixed.SourceOwner.Route, captured.MaterializationMetadata.FlowConfig)
			if err != nil {
				t.Fatal(err)
			}
			projected, err := recorded.Project(owner.Route, observed.Identity().ParentRoute)
			if err != nil {
				t.Fatal(err)
			}
			var payload map[string]any
			if err := canonicaljson.DecodePreservingNumberLexemes(projected, &payload); err != nil {
				t.Fatal(err)
			}
			payload["workflow_version"] = "foreign-fixed-version"
			wrong, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			restore, err := FaultFlowConstructorHeaderForTest(f.ctx, f.store, owner, "config", string(wrong))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := restore(f.ctx); err != nil {
					t.Error(err)
				}
			}()
			before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
			invalid, found, err := index.LookupFlowInstance(f.ctx, lookup)
			var corrupt *pipeline.FlowInstanceConstructionCorruption
			if found || invalid.Valid() || !errors.As(err, &corrupt) || corrupt.RunID != owner.RunID || corrupt.InstancePath != owner.Route.InstancePath {
				t.Fatalf("historical version drift was not exact corruption: found=%t err=%v", found, err)
			}
			if !reflect.DeepEqual(before, forkReceiverFaultSnapshot(t, f.ctx, f.store)) {
				t.Fatal("version refusal mutated or repaired durable state")
			}
		})
	}
}

func TestR7ForkOfForkConstructorKeepsImmediateCutBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newOrdinaryForkPredecessorFixture(t, backend)
			component := f.receiverConfigActivationFixture
			component.ctx = f.ctx
			child := materializeReceiverConstructionProofFork(t, component, f.owner.RunID)
			owner := flowidentity.RunScopedFlowInstance{RunID: child.ForkRunID, Route: f.owner.Route}
			lookup, err := pipeline.NewExactFlowInstanceLookup(semanticview.Wrap(f.bundle), mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash()), owner)
			if err != nil {
				t.Fatal(err)
			}
			read := func() pipeline.FlowInstanceObservation {
				t.Helper()
				observed, found, err := f.store.(pipeline.FlowInstanceIndexReader).LookupFlowInstance(f.ctx, lookup)
				if err != nil || !found {
					t.Fatalf("observe fork of fork: found=%t err=%v", found, err)
				}
				fixed, historical := observed.HistoricalConstruction()
				if !historical || fixed.SourceOwner != f.owner || fixed.SourceRevision != child.ForkPoint.Revision || fixed.InstanceKey != "original" {
					t.Fatalf("dependency lost its immediate source cut: %+v", fixed)
				}
				if _, native, err := observed.NativeConstruction(); err != nil || native {
					t.Fatal("projected construction became a native receipt")
				}
				if _, err := f.store.(pipeline.FlowConstructionPublicationReader).LoadFlowConstructionPublication(f.ctx, owner, observed.Identity().EntityID); !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("historical observation manufactured a receipt: %v", err)
				}
				return observed
			}
			original := read()
			for _, source := range []flowidentity.RunScopedFlowInstance{f.sourceOwner, f.owner} {
				for _, field := range []string{"instance_key", "current_state"} {
					restore, err := FaultFlowConstructorHeaderForTest(f.ctx, f.store, source, pipelinepersistence.FlowConstructorHeaderFaultField(field), "post-cut-change")
					if err != nil {
						t.Fatal(err)
					}
					before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
					if observed := read(); observed.Identity() != original.Identity() || observed.InstanceKey() != original.InstanceKey() {
						t.Fatal("current ancestor header changed the fixed constructor")
					}
					if after := forkReceiverFaultSnapshot(t, f.ctx, f.store); !reflect.DeepEqual(before, after) {
						t.Fatal("fixed observation mutated durable evidence")
					}
					if err := restore(f.ctx); err != nil {
						t.Fatal(err)
					}
				}
			}
			for _, field := range []string{"instance_key", "parent_instance"} {
				value := "foreign-key"
				if field == "parent_instance" {
					value = f.otherSourceOwner.Route.InstancePath
				}
				restore, err := FaultFlowConstructorHeaderForTest(f.ctx, f.store, owner, pipelinepersistence.FlowConstructorHeaderFaultField(field), value)
				if err != nil {
					t.Fatal(err)
				}
				before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
				observed, found, err := f.store.(pipeline.FlowInstanceIndexReader).LookupFlowInstance(f.ctx, lookup)
				var corruption *pipeline.FlowInstanceConstructionCorruption
				if err == nil || found || observed.Valid() || !errors.As(err, &corruption) {
					t.Fatalf("wrong %s was not typed corruption: found=%t err=%v", field, found, err)
				}
				if !reflect.DeepEqual(before, forkReceiverFaultSnapshot(t, f.ctx, f.store)) {
					t.Fatal("corruption refusal changed durable state")
				}
				if err := restore(f.ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := CorruptReceiverConstructionHistoryCutForTest(f.ctx, f.store, f.owner, f.entityID, 1<<62); err != nil {
				t.Fatal(err)
			}
			before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
			observed, found, err := f.store.(pipeline.FlowInstanceIndexReader).LookupFlowInstance(f.ctx, lookup)
			if found || observed.Valid() || !errors.Is(err, sql.ErrNoRows) || !strings.Contains(err.Error(), "captured construction lineage cut") {
				t.Fatalf("missing intermediate cut became evidence: found=%t err=%v", found, err)
			}
			if !reflect.DeepEqual(before, forkReceiverFaultSnapshot(t, f.ctx, f.store)) {
				t.Fatal("missing-cut refusal changed durable state")
			}
		})
	}
}
