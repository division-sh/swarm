package runtimepersistence

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

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
