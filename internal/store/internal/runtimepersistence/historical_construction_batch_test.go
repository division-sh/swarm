package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestHistoricalConstructionEvidenceBatchParityAndRefusalsBothStores(t *testing.T) {
	testHistoricalConstructionBatchParityAndRefusalsBothStores(t, false, false)
}

func TestHistoricalConstructionAdmissionBatchParityAndRefusalsBothStores(t *testing.T) {
	testHistoricalConstructionBatchParityAndRefusalsBothStores(t, true, false)
}

func TestHistoricalPlanningAdmissionKeepsScalarBatchAndParentFencesBothStores(t *testing.T) {
	testHistoricalConstructionBatchParityAndRefusalsBothStores(t, true, true)
}

func TestHistoricalPlanningEvidenceDoesNotGainAdmissionAuthorityBothStores(t *testing.T) {
	testHistoricalConstructionBatchParityAndRefusalsBothStores(t, false, true)
}

func testHistoricalConstructionBatchParityAndRefusalsBothStores(t *testing.T, sourceCompatible, planning bool) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newOrdinaryForkPredecessorFixture(t, backend)
			component := f.receiverConfigActivationFixture
			component.ctx = f.ctx
			constructHistoricalSourceFixture(t, f.ctx, f.store, component.request("later", "ti-later", "native"))
			scope, err := pipeline.NewFlowInstanceLookupScope(semanticview.Wrap(f.bundle), mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash()), f.owner.RunID, []string{".", "review"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			observed, err := f.store.(pipeline.FlowInstanceIndexReader).ListFlowInstances(f.ctx, scope)
			if err != nil || len(observed) != 4 {
				t.Fatalf("batch fixture lost its three historical/one native headers: count=%d err=%v", len(observed), err)
			}
			headers := make([]pipeline.WorkflowInstance, len(observed))
			var exact pipeline.WorkflowInstance
			for i, observation := range observed {
				headers[i], err = observation.WorkflowInstance()
				if err != nil {
					t.Fatal(err)
				}
				if headers[i].StorageRef == f.owner.Route.InstancePath {
					exact = headers[i]
				}
			}
			if exact.EntityID != f.entityID {
				t.Fatal("batch hostiles lack their exact constructed receiver")
			}
			run, err := ReadRuntimeLogRunSnapshotForTest(f.ctx, f.store, f.owner.RunID)
			if err != nil {
				t.Fatal(err)
			}
			var reader pipelinepersistence.HistoricalFlowInstanceReader
			switch selected := f.store.(type) {
			case *SQLiteRuntimeStore:
				reader = selected.runForkSQLiteOwner
			case *PostgresStore:
				reader = selected.runForkPostgresOwner
			default:
				t.Fatal("batch proof requires the original native history owner")
			}
			source := semanticview.Wrap(f.bundle)
			readTx := func(ctx context.Context, selectedRun runlifecycle.Snapshot, read func(context.Context, *sql.Tx) error) error {
				return readServedDeliveryObservation(ctx, f.store, func(ctx context.Context, tx *sql.Tx) error {
					if planning {
						return reader.WithHistoricalFlowInstancePlanningTx(ctx, tx, source, selectedRun, func(ctx context.Context) error {
							return read(ctx, tx)
						})
					}
					return read(ctx, tx)
				})
			}
			readScalar := reader.ReadHistoricalFlowInstanceConstructionEvidenceTx
			readBatch := reader.ReadHistoricalFlowInstanceConstructionEvidenceBatchTx
			if sourceCompatible {
				readScalar = func(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, header pipeline.WorkflowInstance) (pipeline.HistoricalFlowInstanceConstruction, bool, error) {
					return reader.ReadHistoricalFlowInstanceConstructionTx(ctx, tx, source, run, header)
				}
				readBatch = func(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, headers []pipeline.WorkflowInstance) ([]pipelinepersistence.HistoricalFlowInstanceConstructionResult, error) {
					return reader.ReadHistoricalFlowInstanceConstructionBatchTx(ctx, tx, source, run, headers)
				}
			}
			read := func(ctx context.Context, selectedRun runlifecycle.Snapshot, input []pipeline.WorkflowInstance) ([]pipelinepersistence.HistoricalFlowInstanceConstructionResult, error) {
				var result []pipelinepersistence.HistoricalFlowInstanceConstructionResult
				err := readTx(ctx, selectedRun, func(ctx context.Context, tx *sql.Tx) error {
					var err error
					result, err = readBatch(ctx, tx, selectedRun, input)
					return err
				})
				return result, err
			}
			before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			var original []pipelinepersistence.HistoricalFlowInstanceConstructionResult
			if err := readTx(f.ctx, run, func(ctx context.Context, tx *sql.Tx) error {
				var err error
				original, err = readBatch(ctx, tx, run, headers)
				if err != nil || len(original) != len(headers) {
					return errors.Join(errors.New("batch changed its exact header cardinality"), err)
				}
				for i, header := range headers {
					construction, found, err := readScalar(ctx, tx, run, header)
					if err != nil || original[i] != (pipelinepersistence.HistoricalFlowInstanceConstructionResult{Construction: construction, Found: found}) || found != (header.StorageRef != "review/ti-later") {
						t.Fatalf("scalar/batch changed exact evidence or native absence: path=%s batch=%+v scalar=%+v found=%t err=%v", header.StorageRef, original[i], construction, found, err)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			reversed := slices.Clone(headers)
			slices.Reverse(reversed)
			result, err := read(f.ctx, run, reversed)
			expected := slices.Clone(original)
			slices.Reverse(expected)
			if err != nil || !reflect.DeepEqual(result, expected) {
				t.Fatalf("batch reordered or crossed header evidence: %+v %v", result, err)
			}
			for _, field := range []string{"entity", "key", "parent", "mode", "version", "birth_time", "template", "instance"} {
				t.Run(field, func(t *testing.T) {
					bad := exact
					switch field {
					case "entity":
						bad.EntityID = uuid.NewString()
					case "key":
						bad.InstanceKey = "foreign"
					case "parent":
						bad.ParentEntityID = uuid.NewString()
					case "mode":
						bad.Mode = "static"
					case "version":
						bad.WorkflowVersion = "current-selected-not-captured"
					case "birth_time":
						bad.CreatedAt = bad.CreatedAt.Add(time.Second)
					case "template":
						bad.WorkflowName = "."
					case "instance":
						bad.InstanceID = "foreign"
					}
					if err := readTx(f.ctx, run, func(ctx context.Context, tx *sql.Tx) error {
						_, _, scalarErr := readScalar(ctx, tx, run, bad)
						got, batchErr := readBatch(ctx, tx, run, []pipeline.WorkflowInstance{exact, bad})
						if scalarErr == nil || batchErr == nil || got != nil || scalarErr.Error() != batchErr.Error() {
							t.Fatalf("crossed duplicate gained authority or partial output: field=%s result=%+v scalar=%v batch=%v", field, got, scalarErr, batchErr)
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				})
			}
			for _, coordinate := range []string{"source_run", "revision"} {
				wrong := run
				source, revision := run.Origin.SourceRunID(), run.Origin.ForkRevision()
				if coordinate == "source_run" {
					source = uuid.NewString()
				} else {
					revision = 1 << 60
				}
				wrong.Origin, err = runlifecycle.ForkMaterializationRunOrigin(source, run.Origin.ForkPointKind(), revision, run.Origin.SourceEventID())
				if err != nil {
					t.Fatal(err)
				}
				if got, err := read(f.ctx, wrong, headers); !errors.Is(err, sql.ErrNoRows) || got != nil {
					t.Fatalf("wrong %s gained batch evidence: %+v %v", coordinate, got, err)
				}
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if got, err := read(cancelled, run, headers); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled batch returned evidence: %+v %v", got, err)
			}
			if got, err := readBatch(f.ctx, nil, run, headers); err == nil || got != nil {
				t.Fatalf("batch accepted missing transaction authority: %+v %v", got, err)
			}
			if got, err := read(f.ctx, run, nil); err != nil || len(got) != 0 {
				t.Fatalf("empty batch invented evidence: %+v %v", got, err)
			}
			original[0].Construction.Identity.EntityID = "caller mutation"
			if got, err := read(f.ctx, run, reversed); err != nil || !reflect.DeepEqual(got, expected) {
				t.Fatalf("batch output retained caller-mutated state: %+v %v", got, err)
			}
			if after := forkReceiverFaultSnapshot(t, f.ctx, f.store); !reflect.DeepEqual(before, after) || probe.Snapshot().Total.WriteCommits != 0 {
				t.Fatal("batch/refusal controls changed durable state")
			}
		})
	}
}

func TestHistoricalConstructionBatchReadinessKeepsNativeEvidenceChecksBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, scenario := range []string{"missing_native", "conflicting_native"} {
			t.Run(backend+"/"+scenario, func(t *testing.T) {
				f := newOrdinaryForkPredecessorFixture(t, backend)
				component := f.receiverConfigActivationFixture
				component.ctx = f.ctx
				plan := constructHistoricalSourceFixture(t, f.ctx, f.store, component.request("later", "ti-later", "native"))
				fact := mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash())
				reader := f.store.(pipeline.DynamicFlowRuntimeReadinessPersistence)
				items, err := reader.InspectDynamicFlowRuntimeReadinessForRun(f.ctx, f.owner.RunID, fact)
				if err != nil {
					t.Fatal(err)
				}
				native := false
				for _, item := range items {
					if item.Plan.Identity == plan.Identity {
						native = true
					}
				}
				if !native {
					t.Fatal("mixed batch positive control omitted actual native readiness")
				}
				owner, entity := f.owner, f.entityID
				if scenario == "missing_native" {
					owner = flowidentity.RunScopedFlowInstance{RunID: f.owner.RunID, Route: plan.Identity.Route()}
					entity = plan.Identity.EntityID
					if err := runDeliveryFixtureAttempt(f.ctx, f.store.(deliveryFixtureStore), func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
						return pipelinepersistence.RemoveReceiverConstructionReceiptForTest(ctx, attempt, owner, entity)
					}); err != nil {
						t.Fatal(err)
					}
				} else if err := CopyReceiverConstructionReceiptForTest(f.ctx, f.store, f.sourceOwner, owner, f.sourceEntity, entity); err != nil {
					t.Fatal(err)
				}
				before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
				for _, surface := range []string{"source", "run"} {
					var corruption *pipeline.FlowInstanceConstructionCorruption
					err := inspectConstructedRouteReadiness(component, reader, fact, f.owner.RunID, surface)
					if !errors.As(err, &corruption) || corruption.RunID != owner.RunID || corruption.InstancePath != owner.Route.InstancePath {
						t.Fatalf("%s/%s bypassed actual per-header receipt checks: %v", scenario, surface, err)
					}
				}
				scope, err := pipeline.NewFlowInstanceLookupScope(semanticview.Wrap(f.bundle), fact, f.owner.RunID, []string{"review"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				for _, surface := range []string{"list", "planning"} {
					var got []pipeline.FlowInstanceObservation
					if surface == "list" {
						got, err = f.store.(pipeline.FlowInstanceIndexReader).ListFlowInstances(f.ctx, scope)
					} else {
						err = f.store.(pipeline.FlowInstancePlanningSnapshotOwner).WithFlowInstancePlanningSnapshot(f.ctx, scope, func(ctx context.Context, index pipeline.FlowInstanceIndexReader) error {
							var readErr error
							got, readErr = index.ListFlowInstances(ctx, scope)
							return readErr
						})
					}
					var corruption *pipeline.FlowInstanceConstructionCorruption
					if got != nil || !errors.As(err, &corruption) || corruption.RunID != owner.RunID || corruption.InstancePath != owner.Route.InstancePath {
						t.Fatalf("%s/%s bypassed actual per-header companions: %+v %v", scenario, surface, got, err)
					}
				}
				if after := forkReceiverFaultSnapshot(t, f.ctx, f.store); !reflect.DeepEqual(before, after) {
					t.Fatal("readiness receipt refusal changed durable state")
				}
			})
		}
	}
}

func TestHistoricalConstructionEvidenceBatchUsesFreshRunAndTransactionBothStores(t *testing.T) {
	testHistoricalConstructionBatchUsesFreshRunAndTransactionBothStores(t, false)
}

func TestHistoricalConstructionAdmissionBatchUsesFreshRunAndTransactionBothStores(t *testing.T) {
	testHistoricalConstructionBatchUsesFreshRunAndTransactionBothStores(t, true)
}

func testHistoricalConstructionBatchUsesFreshRunAndTransactionBothStores(t *testing.T, sourceCompatible bool) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newOrdinaryForkPredecessorFixture(t, backend)
			var source semanticview.Source
			if sourceCompatible {
				source = semanticview.Wrap(f.bundle)
			}
			workflows := f.store.(pipeline.WorkflowInstancePersistenceReader)
			header, found, err := workflows.LoadWorkflowInstance(f.ctx, f.owner)
			if err != nil || !found {
				t.Fatalf("fresh-read proof lacks physical header: found=%t err=%v", found, err)
			}
			run, err := ReadRuntimeLogRunSnapshotForTest(f.ctx, f.store, f.owner.RunID)
			if err != nil {
				t.Fatal(err)
			}
			first, err := readHistoricalConstructionBatchTest(f.ctx, f.store, source, run, []pipeline.WorkflowInstance{header, header})
			if err != nil || len(first) != 2 || !first[0].Found || first[0] != first[1] {
				t.Fatalf("exact duplicates changed ordered evidence: %+v %v", first, err)
			}
			restore, err := FaultFlowConstructorHeaderForTest(f.ctx, f.store, f.owner, "created_at", header.CreatedAt.Add(-time.Second))
			if err != nil {
				t.Fatal(err)
			}
			restored := false
			defer func() {
				if !restored {
					if err := restore(f.ctx); err != nil {
						t.Error(err)
					}
				}
			}()
			changed, found, err := workflows.LoadWorkflowInstance(f.ctx, f.owner)
			if err != nil || !found || changed.CreatedAt.Equal(header.CreatedAt) {
				t.Fatalf("intervening physical change was not observed: found=%t err=%v", found, err)
			}
			before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
			got, err := readHistoricalConstructionBatchTest(f.ctx, f.store, source, run, []pipeline.WorkflowInstance{header, changed})
			var corruption *pipeline.FlowInstanceConstructionCorruption
			if got != nil || !errors.As(err, &corruption) || corruption.RunID != f.owner.RunID || corruption.InstancePath != f.owner.Route.InstancePath {
				t.Fatalf("later transaction reused old physical authority or returned partial facts: %+v %v", got, err)
			}
			if after := forkReceiverFaultSnapshot(t, f.ctx, f.store); !reflect.DeepEqual(before, after) {
				t.Fatal("fresh-transaction refusal repaired its input")
			}
			if err := restore(f.ctx); err != nil {
				t.Fatal(err)
			}
			restored = true
			if got, err := readHistoricalConstructionBatchTest(f.ctx, f.store, source, run, []pipeline.WorkflowInstance{header, header}); err != nil || !reflect.DeepEqual(got, first) {
				t.Fatalf("restored fresh transaction retained refusal or mutated facts: %+v %v", got, err)
			}
			component := f.receiverConfigActivationFixture
			component.ctx = f.ctx
			fork := materializeReceiverConstructionProofFork(t, component, f.owner.RunID)
			otherOwner := flowidentity.RunScopedFlowInstance{RunID: fork.ForkRunID, Route: f.owner.Route}
			other, found, err := workflows.LoadWorkflowInstance(f.ctx, otherOwner)
			if err != nil || !found || other.StorageRef != header.StorageRef || other.ParentFlowInstance == header.ParentFlowInstance {
				t.Fatalf("colliding local coordinate proof lost its distinct run/parent: %+v found=%t err=%v", other, found, err)
			}
			otherRun, err := ReadRuntimeLogRunSnapshotForTest(f.ctx, f.store, otherOwner.RunID)
			if err != nil {
				t.Fatal(err)
			}
			second, err := readHistoricalConstructionBatchTest(f.ctx, f.store, source, otherRun, []pipeline.WorkflowInstance{other})
			if err != nil || len(second) != 1 || !second[0].Found || second[0].Construction.SourceOwner != f.owner ||
				second[0].Construction.SourceRevision != fork.ForkPoint.Revision || second[0].Construction.Identity.ParentRoute.FlowInstance != otherOwner.RunID ||
				first[0].Construction.SourceOwner != f.sourceOwner || first[0].Construction.SourceRevision != f.cutRevision {
				t.Fatalf("batch aliased colliding run/cut meanings: first=%+v second=%+v err=%v", first, second, err)
			}
			if got, err := readHistoricalConstructionBatchTest(f.ctx, f.store, source, otherRun, []pipeline.WorkflowInstance{other, header}); err == nil || got != nil {
				t.Fatalf("foreign physical header borrowed other run's authority: %+v %v", got, err)
			}
			if sourceCompatible {
				index := f.store.(pipeline.FlowInstanceIndexReader)
				fact := mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash())
				for _, expected := range []struct {
					owner        flowidentity.RunScopedFlowInstance
					construction pipeline.HistoricalFlowInstanceConstruction
				}{{f.owner, first[0].Construction}, {otherOwner, second[0].Construction}} {
					lookup, err := pipeline.NewExactFlowInstanceLookup(source, fact, expected.owner)
					if err != nil {
						t.Fatal(err)
					}
					observation, found, err := index.LookupFlowInstance(f.ctx, lookup)
					construction, historical := observation.HistoricalConstruction()
					if err != nil || !found || !historical || construction != expected.construction {
						t.Fatalf("direct re-fork lookup crossed source/cut/run: %+v found=%t historical=%t error=%v", construction, found, historical, err)
					}
					scope, err := pipeline.NewFlowInstanceLookupScope(source, fact, expected.owner.RunID, nil, []flowidentity.RunScopedFlowInstance{expected.owner})
					if err != nil {
						t.Fatal(err)
					}
					listed, err := index.ListFlowInstances(f.ctx, scope)
					if err != nil || len(listed) != 1 || !reflect.DeepEqual(listed[0], observation) {
						t.Fatalf("direct re-fork list changed exact lookup evidence: %+v error=%v", listed, err)
					}
				}
			}
		})
	}
}

func readHistoricalConstructionBatchTest(ctx context.Context, selected any, source semanticview.Source, run runlifecycle.Snapshot, headers []pipeline.WorkflowInstance) ([]pipelinepersistence.HistoricalFlowInstanceConstructionResult, error) {
	var reader pipelinepersistence.HistoricalFlowInstanceReader
	switch owner := selected.(type) {
	case *SQLiteRuntimeStore:
		reader = owner.runForkSQLiteOwner
	case *PostgresStore:
		reader = owner.runForkPostgresOwner
	default:
		return nil, errors.New("batch proof requires its native history owner")
	}
	var result []pipelinepersistence.HistoricalFlowInstanceConstructionResult
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		if source == nil {
			result, err = reader.ReadHistoricalFlowInstanceConstructionEvidenceBatchTx(ctx, tx, run, headers)
		} else {
			result, err = reader.ReadHistoricalFlowInstanceConstructionBatchTx(ctx, tx, source, run, headers)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
