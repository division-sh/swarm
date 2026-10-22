package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/google/uuid"
)

func TestProjectedReceiverReceiptCorrespondenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newOrdinaryForkPredecessorFixture(t, backend)
			expected := readProjectedReceiverReceiptFixture(t, f)
			raw, err := pipeline.EncodeFlowConstructionReceipt(expected)
			if err != nil {
				t.Fatal(err)
			}
			for _, variant := range []string{"absent", "projected", "captured_content", "numeric_kind", "foreign"} {
				t.Run(variant, func(t *testing.T) {
					var receipt []byte
					if variant != "absent" {
						actual, err := pipeline.DecodeStoredFlowConstructionReceipt(raw, f.owner.RunID, f.entityID, f.owner.Route.InstancePath, f.owner.Route.ScopeKey)
						if err != nil {
							t.Fatal(err)
						}
						switch variant {
						case "captured_content":
							actual.Persisted.Fields["label"] = "wrong-captured-label"
						case "numeric_kind":
							nested := actual.Persisted.Fields["nested"].([]any)
							if reflect.TypeOf(nested[0]) == reflect.TypeOf(float64(7)) {
								t.Fatal("numeric-kind negative lacks its integer precondition")
							}
							nested[0] = float64(7)
						}
						receipt, err = pipeline.EncodeFlowConstructionReceipt(actual)
						if err != nil {
							t.Fatalf("hostile receipt must pass the ordinary codec: %v", err)
						}
					}
					if variant == "foreign" {
						if err := CopyReceiverConstructionReceiptForTest(f.ctx, f.store, f.sourceOwner, f.owner, f.sourceEntity, f.entityID); err != nil {
							t.Fatal(err)
						}
					} else if receipt != nil {
						if _, err := f.db.ExecContext(f.ctx, `INSERT INTO workflow_instance_initial_materializations
							(run_id, entity_id, instance_path, projection_version, projection, occurred_at) VALUES ($1,$2,$3,$4,$5,$6)`,
							f.owner.RunID, f.entityID, f.owner.Route.InstancePath, pipeline.FlowConstructionReceiptVersion, string(receipt), expected.OccurredAt); err != nil {
							t.Fatal(err)
						}
					}
					defer func() {
						if _, err := f.db.ExecContext(f.ctx, `DELETE FROM workflow_instance_initial_materializations WHERE run_id=$1 AND entity_id=$2 AND instance_path=$3`, f.owner.RunID, f.entityID, f.owner.Route.InstancePath); err != nil {
							t.Error(err)
						}
					}()
					proveProjectedReceiverReceiptConsumers(t, f, variant)
				})
			}
		})
	}
}

func readProjectedReceiverReceiptFixture(t *testing.T, f ordinaryForkPredecessorFixture) pipeline.FlowConstructionReceipt {
	t.Helper()
	var receipt pipeline.FlowConstructionReceipt
	err := runReceiverConstructionObservationFault(f.ctx, f.store, func(ctx context.Context, tx *sql.Tx) error {
		_, postgres := f.store.(*PostgresStore)
		header, err := pipelinepersistence.ReadReceiverConstructionHeaderForTest(ctx, tx, f.owner, f.entityID, !postgres)
		if err != nil {
			return err
		}
		var found bool
		if store, ok := f.store.(*PostgresStore); ok {
			run, loadErr := store.runLifecyclePostgresOwner.LoadSnapshotTx(ctx, tx, f.owner.RunID, false)
			if loadErr != nil {
				return loadErr
			}
			receipt, found, err = store.runForkPostgresOwner.ReadProjectedReceiverConstructionReceiptTx(ctx, tx, run, header)
		} else {
			store := f.store.(*SQLiteRuntimeStore)
			run, loadErr := store.runLifecycleSQLiteOwner.LoadSnapshotTx(ctx, tx, f.owner.RunID)
			if loadErr != nil {
				return loadErr
			}
			receipt, found, err = store.runForkSQLiteOwner.ReadProjectedReceiverConstructionReceiptTx(ctx, tx, run, header)
		}
		if err == nil && !found {
			return errors.New("fixture lacks its exact independently projected receipt")
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func proveProjectedReceiverReceiptConsumers(t *testing.T, f ordinaryForkPredecessorFixture, variant string) {
	t.Helper()
	valid := variant == "absent" || variant == "projected"
	before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
	lookup, err := pipeline.NewExactFlowInstanceLookup(semanticview.Wrap(f.bundle), mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash()), f.owner)
	if err != nil {
		t.Fatal(err)
	}
	index := f.store.(pipeline.FlowInstanceIndexReader)
	observation, found, err := index.LookupFlowInstance(f.ctx, lookup)
	if valid != (err == nil && found && observation.Valid()) {
		t.Fatalf("exact index correspondence: found=%t err=%v", found, err)
	}
	if !valid && (err == nil || found || observation.Valid()) {
		t.Fatal("invalid receipt supplied partial index authority")
	}
	if !valid {
		requireProjectedReceiptCorruption(t, variant, err)
	}
	scope, err := pipeline.NewFlowInstanceLookupScope(lookup.Source(), lookup.SourceFact(), f.owner.RunID, []string{"review"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	items, err := index.ListFlowInstances(f.ctx, scope)
	if valid != (err == nil && len(items) == 2) || (!valid && (err == nil || len(items) != 0)) {
		t.Fatalf("batch index correspondence: items=%d err=%v", len(items), err)
	}
	if !valid {
		requireProjectedReceiptCorruption(t, variant, err)
	}
	_, err = f.store.(pipeline.DynamicFlowRuntimeReadinessPersistence).InspectDynamicFlowRuntimeReadinessForRun(f.ctx, f.owner.RunID, lookup.SourceFact())
	if valid != (err == nil) {
		t.Fatalf("readiness inspection correspondence: %v", err)
	}
	agentRoute, err := f.owner.Route.AgentIdentityRoute()
	if err != nil {
		t.Fatal(err)
	}
	name, err := agentidentity.RuntimeName("worker", "projected-receipt-proof")
	if err != nil {
		t.Fatal(err)
	}
	agent, err := agentidentity.New(f.owner.RunID, name, agentRoute)
	if err != nil {
		t.Fatal(err)
	}
	target := events.RouteIdentity{FlowID: f.owner.Route.ScopeKey, FlowInstance: f.owner.Route.InstancePath, EntityID: f.entityID}
	actor := target
	actor.EntityID = ""
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agent.AgentID()), AgentIdentity: agent, Target: events.MustEntitylessReceiverTarget(actor)}
	route.Initialization, err = events.AdmitHistoricalFlowReceiverInitialization(f.owner.RunID, events.MustExistingEntityTarget(target), agentRoute,
		events.HistoricalReceiverConstruction{SourceRunID: f.sourceOwner.RunID, SourceRevision: f.cutRevision, SourceReceiver: agentRoute})
	if err != nil {
		t.Fatal(err)
	}
	component := f.receiverConfigActivationFixture
	component.ctx = f.ctx
	ready, err := readReceiverConstructionReady(t, component, route)
	if valid != (err == nil && ready) || (!valid && (err == nil || ready)) {
		t.Fatalf("agent claim materialization correspondence: ready=%t err=%v", ready, err)
	}
	if !valid {
		requireProjectedReceiptCorruption(t, variant, err)
	}
	if after := forkReceiverFaultSnapshot(t, f.ctx, f.store); !reflect.DeepEqual(before, after) {
		t.Fatal("receipt inspection changed complete durable state")
	}
	// NODE claims exercise the same creating-predecessor boundary through all
	// singleton, batch, scan and direct-claim entrances, not an agent stand-in.
	route = events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceNode("review", "receipt-proof")), Target: events.MustExistingEntityTarget(target)}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "task.started", "fixture", "", []byte(`{}`), 0, f.owner.RunID, events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticEventFixtureWithRoutes(f.ctx, f.store, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	if err := acknowledgePipelineEventFixture(f.ctx, f.store, event.ID()); err != nil {
		t.Fatal(err)
	}
	want := deliverylifecycle.ClaimInvariantInvalid
	if valid {
		want = deliverylifecycle.ClaimAcquired
	}
	proveProjectedReceiptClaimEntrances(t, f, event, route, want)
}

func requireProjectedReceiptCorruption(t *testing.T, variant string, err error) {
	t.Helper()
	var corruption *pipeline.FlowInstanceConstructionCorruption
	if !errors.As(err, &corruption) {
		t.Fatalf("receipt refusal lost its typed corruption: %v", err)
	}
	if variant != "foreign" && !strings.Contains(err.Error(), "disagrees with admitted initialization") {
		t.Fatalf("same-identity receipt did not reach complete correspondence: %v", err)
	}
}

func proveProjectedReceiptClaimEntrances(t *testing.T, f ordinaryForkPredecessorFixture, event events.Event, route events.DeliveryRoute, want deliverylifecycle.ClaimDisposition) {
	t.Helper()
	store := f.store.(deliverylifecycle.Store)
	pending := loadDeliverySnapshotFixture(t, f.ctx, store, event.ID(), route)
	before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
	single, err := store.ObserveDeliveryContinuation(f.ctx, pending.Authority, pending.DeliveryID)
	if err != nil || single.Disposition != want {
		t.Fatalf("receipt singleton: %+v err=%v", single, err)
	}
	batch, err := store.ObserveDeliveryContinuations(f.ctx, pending.Authority, []string{pending.DeliveryID})
	if err != nil || len(batch) != 1 || batch[0].Disposition != want {
		t.Fatalf("receipt batch: %+v err=%v", batch, err)
	}
	page, err := store.ScanDeliveryContinuations(f.ctx, pending.Authority, deliverylifecycle.ContinuationCursor{}, 64)
	matched := false
	for _, item := range page.Items {
		if item.DeliveryID == pending.DeliveryID {
			matched = item.Disposition == want
		}
	}
	if err != nil || !matched {
		t.Fatalf("receipt scan: %+v err=%v", page, err)
	}
	if after := forkReceiverFaultSnapshot(t, f.ctx, f.store); !reflect.DeepEqual(before, after) {
		t.Fatal("receipt observations changed complete durable state")
	}
	claim, err := store.ClaimDelivery(f.ctx, pending.Authority, event, route)
	if err != nil || claim.Disposition != want {
		t.Fatalf("receipt direct claim: %+v err=%v", claim, err)
	}
	if want == deliverylifecycle.ClaimInvariantInvalid {
		if claim.Invariant == nil || !strings.Contains(claim.Invariant.Error(), "construction") || !reflect.DeepEqual(before, forkReceiverFaultSnapshot(t, f.ctx, f.store)) {
			t.Fatalf("invalid receipt lost precise refusal or changed durable state: %+v", claim)
		}
	}
}
