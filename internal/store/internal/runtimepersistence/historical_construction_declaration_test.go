package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type cancellingHistoricalDeclarationSource struct {
	semanticview.Source
	cancel  context.CancelFunc
	visited *[]string
}

func (s cancellingHistoricalDeclarationSource) FlowScopeByID(flowID string) (semanticview.FlowScope, bool) {
	*s.visited = append(*s.visited, flowID)
	if flowID == "review" {
		s.cancel()
	}
	return s.Source.FlowScopeByID(flowID)
}

func TestHistoricalDeclarationBatchCancellationDiscardsEarlierAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newOrdinaryForkPredecessorFixture(t, backend)
			source := semanticview.Wrap(f.bundle)
			scope, err := pipeline.NewFlowInstanceLookupScope(source, mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash()), f.owner.RunID, []string{".", "review"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			observed, err := f.store.(pipeline.FlowInstanceIndexReader).ListFlowInstances(f.ctx, scope)
			if err != nil {
				t.Fatal(err)
			}
			var root, child pipeline.WorkflowInstance
			for _, item := range observed {
				header, err := item.WorkflowInstance()
				if err != nil {
					t.Fatal(err)
				}
				if header.WorkflowName == "." {
					root = header
				} else if item.Owner() == f.owner {
					child = header
				}
			}
			if root.EntityID == "" || child.EntityID != f.entityID {
				t.Fatal("cancellation control lacks its ordered root/child positive inputs")
			}
			run, err := ReadRuntimeLogRunSnapshotForTest(f.ctx, f.store, f.owner.RunID)
			if err != nil {
				t.Fatal(err)
			}
			before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			var visited []string
			got, err := readHistoricalConstructionBatchTest(ctx, f.store, cancellingHistoricalDeclarationSource{Source: source, cancel: cancel, visited: &visited}, run, []pipeline.WorkflowInstance{root, child})
			if !errors.Is(err, context.Canceled) || got != nil || len(visited) < 2 || visited[0] != "." || visited[len(visited)-1] != "review" {
				t.Fatalf("later cancellation exposed earlier admission: got=%+v visited=%v err=%v", got, visited, err)
			}
			if after := forkReceiverFaultSnapshot(t, f.ctx, f.store); !reflect.DeepEqual(before, after) {
				t.Fatal("cancelled batch wrote durable state")
			}
		})
	}
}

func TestHistoricalDeclarationBatchKeepsListAndPlanningAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newOrdinaryForkPredecessorFixture(t, backend)
			component := f.receiverConfigActivationFixture
			component.ctx = f.ctx
			constructHistoricalSourceFixture(t, f.ctx, f.store, component.request("later", "ti-later", "native"))
			source := semanticview.Wrap(f.bundle)
			fact := mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash())
			scope, err := pipeline.NewFlowInstanceLookupScope(source, fact, f.owner.RunID, []string{"review"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			index := f.store.(pipeline.FlowInstanceIndexReader)
			original, err := index.ListFlowInstances(f.ctx, scope)
			if err != nil || len(original) != 3 {
				t.Fatalf("declaration lost mixed native/historical headers: %+v %v", original, err)
			}
			for _, observation := range original {
				request, err := pipeline.NewExactFlowInstanceLookup(source, fact, observation.Owner())
				if err != nil {
					t.Fatal(err)
				}
				scalar, found, err := index.LookupFlowInstance(f.ctx, request)
				want, wantErr := observation.WorkflowInstance()
				got, gotErr := scalar.WorkflowInstance()
				fixed, historical := observation.HistoricalConstruction()
				other, scalarHistorical := scalar.HistoricalConstruction()
				if err != nil || !found || wantErr != nil || gotErr != nil || !reflect.DeepEqual(got, want) || observation.Identity() != scalar.Identity() || fixed != other || historical != scalarHistorical {
					t.Fatalf("public declaration/scalar observations differ: path=%s header=%+v scalar=%+v err=%v/%v/%v", observation.Owner().Route.InstancePath, want, got, err, wantErr, gotErr)
				}
			}
			before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
			planning := f.store.(pipeline.FlowInstancePlanningSnapshotOwner)
			if err := planning.WithFlowInstancePlanningSnapshot(f.ctx, scope, func(ctx context.Context, reader pipeline.FlowInstanceIndexReader) error {
				for range 2 {
					got, err := reader.ListFlowInstances(ctx, scope)
					if err != nil || !reflect.DeepEqual(got, original) {
						return errors.Join(errors.New("planning declaration/cached observation changed"), err)
					}
				}
				wrongOwner := f.owner
				wrongOwner.Route = flowidentity.StoredRoute("review", "foreign-discriminator", f.owner.Route.InstancePath)
				wrongScope, err := pipeline.NewFlowInstanceLookupScope(source, fact, f.owner.RunID, []string{"review"}, []flowidentity.RunScopedFlowInstance{wrongOwner})
				if err != nil {
					return err
				}
				if got, err := reader.ListFlowInstances(ctx, wrongScope); err == nil || got != nil {
					return errors.New("cached declaration authorized a crossed exact coordinate")
				}
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				if got, err := reader.ListFlowInstances(cancelled, scope); !errors.Is(err, context.Canceled) || got != nil {
					return errors.New("cached declaration hid cancellation after earlier success")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if after := forkReceiverFaultSnapshot(t, f.ctx, f.store); !reflect.DeepEqual(before, after) {
				t.Fatal("planning admission/refusals changed durable state")
			}
			var selected pipeline.FlowInstanceObservation
			for _, observation := range original {
				if observation.Owner() == f.owner {
					selected = observation
				}
			}
			header, err := selected.WorkflowInstance()
			if err != nil {
				t.Fatal(err)
			}
			crossed := selected.Identity()
			crossed.ParentEntityID = uuid.NewString()
			crossed.ParentRoute.EntityID = crossed.ParentEntityID
			payload, err := pipeline.WorkflowInstanceHeaderPayloadForIdentity(crossed, header.WorkflowVersion)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := canonicaljson.MarshalPreservingNumberKinds(payload)
			if err != nil {
				t.Fatal(err)
			}
			restore, err := FaultFlowConstructorHeaderForTest(f.ctx, f.store, f.owner, "config", string(raw))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := restore(f.ctx); err != nil {
					t.Error(err)
				}
			}()
			before = forkReceiverFaultSnapshot(t, f.ctx, f.store)
			for _, surface := range []string{"list", "planning"} {
				var got []pipeline.FlowInstanceObservation
				if surface == "list" {
					got, err = index.ListFlowInstances(f.ctx, scope)
				} else {
					err = planning.WithFlowInstancePlanningSnapshot(f.ctx, scope, func(ctx context.Context, reader pipeline.FlowInstanceIndexReader) error {
						var readErr error
						got, readErr = reader.ListFlowInstances(ctx, scope)
						return readErr
					})
				}
				var corruption *pipeline.FlowInstanceConstructionCorruption
				if got != nil || !errors.As(err, &corruption) || corruption.InstancePath != f.owner.Route.InstancePath || corruption.RunID != f.owner.RunID {
					t.Fatalf("%s accepted a crossed stored parent: %+v %v", surface, got, err)
				}
			}
			if after := forkReceiverFaultSnapshot(t, f.ctx, f.store); !reflect.DeepEqual(before, after) {
				t.Fatal("crossed-parent refusal changed durable state")
			}
		})
	}
}

func TestHistoricalDeclarationBatchNeverPromotesEvidenceToSourceAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newOrdinaryForkPredecessorFixture(t, backend)
			files := make(map[string]string)
			for _, entry := range f.bundle.SourceArtifact.Entries() {
				files[entry.Label()] = string(entry.Bytes())
			}
			const declaration = "instance: request_id\n"
			if strings.Count(files["review/schema.yaml"], declaration) != 1 {
				t.Fatal("incompatible source lacks its exact keyed declaration")
			}
			files["review/schema.yaml"] = strings.Replace(files["review/schema.yaml"], declaration, "", 1)
			foreign := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, nil)
			source, incompatible := semanticview.Wrap(f.bundle), semanticview.Wrap(foreign.bundle)
			lookup, err := pipeline.NewExactFlowInstanceLookup(source, mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash()), f.owner)
			if err != nil {
				t.Fatal(err)
			}
			original, found, err := f.store.(pipeline.FlowInstanceIndexReader).LookupFlowInstance(f.ctx, lookup)
			if err != nil || !found {
				t.Fatalf("source admission positive missing: found=%t %v", found, err)
			}
			header, err := original.WorkflowInstance()
			if err != nil {
				t.Fatal(err)
			}
			run, err := ReadRuntimeLogRunSnapshotForTest(f.ctx, f.store, f.owner.RunID)
			if err != nil {
				t.Fatal(err)
			}
			var history pipelinepersistence.HistoricalFlowInstanceReader
			switch selected := f.store.(type) {
			case *SQLiteRuntimeStore:
				history = selected.runForkSQLiteOwner
			case *PostgresStore:
				history = selected.runForkPostgresOwner
			}
			if err := readServedDeliveryObservation(f.ctx, f.store, func(ctx context.Context, tx *sql.Tx) error {
				if got, err := history.ReadHistoricalFlowInstanceConstructionEvidenceBatchTx(ctx, tx, run, []pipeline.WorkflowInstance{header}); err != nil || len(got) != 1 || !got[0].Found {
					t.Fatalf("incompatible-source negative lacks captured evidence: %+v %v", got, err)
				}
				if got, err := history.ReadHistoricalFlowInstanceConstructionBatchTx(ctx, tx, source, run, []pipeline.WorkflowInstance{header}); err != nil || len(got) != 1 || !got[0].Found {
					t.Fatalf("admitted source positive failed: %+v %v", got, err)
				}
				_, _, scalarErr := history.ReadHistoricalFlowInstanceConstructionTx(ctx, tx, incompatible, run, header)
				got, batchErr := history.ReadHistoricalFlowInstanceConstructionBatchTx(ctx, tx, incompatible, run, []pipeline.WorkflowInstance{header, header})
				if scalarErr == nil || batchErr == nil || got != nil || scalarErr.Error() != batchErr.Error() || !strings.Contains(batchErr.Error(), "selected fixed-revision construction: construction identity disagrees with its canonical constructor") {
					t.Fatalf("batch promoted evidence to source authority: %+v scalar=%v batch=%v", got, scalarErr, batchErr)
				}
				if got, err := history.ReadHistoricalFlowInstanceConstructionBatchTx(ctx, tx, nil, run, []pipeline.WorkflowInstance{header}); err == nil || got != nil {
					t.Fatalf("missing source selected evidence-only admission: %+v %v", got, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			sourceartifactfixture.RequireArtifact(t, f.ctx, f.store.(sourceartifactfixture.Writer), foreign.bundle.SourceArtifact)
			fact := mustStoreTestSourceArtifactFact(foreign.bundle.SourceArtifact.BundleHash())
			if _, err := f.store.(runlifecycle.OperationOwner).ReviseRunSource(f.ctx, runlifecycle.SourceRevisionRequest{RunID: f.owner.RunID, Source: fact}); err != nil {
				t.Fatal(err)
			}
			scope, err := pipeline.NewFlowInstanceLookupScope(incompatible, fact, f.owner.RunID, []string{"review"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
			if got, err := f.store.(pipeline.FlowInstanceIndexReader).ListFlowInstances(f.ctx, scope); err == nil || got != nil || !strings.Contains(err.Error(), "selected fixed-revision construction: construction identity disagrees with its canonical constructor") {
				t.Fatalf("public list bypassed selected declaration compatibility: %+v %v", got, err)
			}
			if err := f.store.(pipeline.FlowInstancePlanningSnapshotOwner).WithFlowInstancePlanningSnapshot(f.ctx, scope, func(ctx context.Context, reader pipeline.FlowInstanceIndexReader) error {
				got, err := reader.ListFlowInstances(ctx, scope)
				if err == nil || got != nil || !strings.Contains(err.Error(), "selected fixed-revision construction: construction identity disagrees with its canonical constructor") {
					t.Fatalf("planning list bypassed selected declaration compatibility: %+v %v", got, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if after := forkReceiverFaultSnapshot(t, f.ctx, f.store); !reflect.DeepEqual(before, after) {
				t.Fatal("incompatible source refusal changed durable state")
			}
		})
	}
}
