package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func requireFanOutOriginNamedAdmission(t *testing.T, fixture authorActivityReceiptFixture, backend string, eventBus *bus.EventBus, ctx context.Context, claim fanoutobligation.Claim, exact events.Event, first engine.DurablePublicationPlan) {
	t.Helper()
	origin, found := exact.InheritedFanOutOrigin()
	if !found {
		t.Fatal("named-origin proof requires an inherited emission")
	}
	for _, variant := range []string{"source_run", "trigger", "delivery", "declaration", "bundle", "digest", "ordinal"} {
		t.Run("hostile_origin_"+variant, func(t *testing.T) {
			sourceRun, trigger, delivery, declaration, bundle, digest, ordinal := origin.SourceRunID(), origin.TriggerEventID(), origin.TriggeringDeliveryID(), origin.Declaration(), origin.BundleHash(), origin.SemanticDigest(), origin.Ordinal()
			switch variant {
			case "source_run":
				sourceRun = uuid.NewString()
			case "trigger":
				trigger = uuid.NewString()
			case "delivery":
				delivery = uuid.NewString()
			case "declaration":
				var err error
				declaration, err = identity.AdmitDeclarationIdentity(".", "fan_out", "wrong/handlers/items.ready/fan_out")
				if err != nil {
					t.Fatal(err)
				}
			case "bundle":
				bundle += "-wrong"
			case "digest":
				digest += "-wrong"
			case "ordinal":
				ordinal++
			}
			hostile, err := events.NewInheritedFanOutOrigin(origin.RunID(), sourceRun, trigger, delivery, declaration, bundle, digest, ordinal)
			if err != nil {
				t.Fatal(err)
			}
			event, err := events.NewInheritedFanOutEvent(events.InheritedFanOutEventInput{Origin: hostile, Facts: events.EventFacts{
				ID: uuid.NewString(), Type: exact.Type(), Producer: events.ProducerClaim{Type: exact.Producer().Type(), ID: exact.Producer().ID()},
				TaskID: exact.TaskID(), Payload: exact.Payload(), ChainDepth: exact.ChainDepth(), Envelope: exact.NormalizedEnvelope(), RoutingSource: exact.RoutingSource(), CreatedAt: exact.CreatedAt(), ExecutionMode: exact.ExecutionMode(),
			}})
			if err != nil {
				t.Fatal(err)
			}
			plans, err := eventBus.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: event}})
			if err != nil || len(plans) != 1 {
				t.Fatalf("structural preparation must reach the named owner: %v", err)
			}
			defer func() {
				if err := eventBus.ReleaseEnginePublications(ctx, plans); err != nil {
					t.Error(err)
				}
			}()
			before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
			_, err = fixture.store.(selectedFanOutOwner).CommitFanOutChunk(ctx, pipeline.FanOutChunkCommand{
				Claim: claim, Outcomes: []pipeline.FanOutChunkOutcome{{Ordinal: origin.Ordinal(), Publication: plans[0]}}, Now: time.Now().UTC(),
			})
			if err == nil {
				t.Fatal("named transaction accepted mismatched origin")
			}
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
				t.Fatal("refused origin mutated event/outcome/obligation/cursor or source state")
			}
			if variant == "bundle" {
				// The valid first ordinal writes its event and obligations before
				// the second ordinal fails. The entire chunk must roll back.
				_, err := fixture.store.(selectedFanOutOwner).CommitFanOutChunk(ctx, pipeline.FanOutChunkCommand{
					Claim: claim, Outcomes: []pipeline.FanOutChunkOutcome{{Ordinal: origin.Ordinal(), Publication: first}, {Ordinal: origin.Ordinal() + 1, Publication: plans[0]}}, Now: time.Now().UTC(),
				})
				if err == nil || !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
					t.Fatalf("late ordinal rejection failed atomic event/origin/outcome/cursor rollback: %v", err)
				}
			}
		})
	}
	before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
	if err := eventBus.Publish(ctx, exact); err == nil {
		t.Fatal("generic publish self-authorized inherited origin")
	}
	if err := eventBus.EngineDispatcher().DispatchPostCommit(ctx, []engine.EmitIntent{{Event: exact}}); err == nil {
		t.Fatal("uncommitted callback self-authorized inherited origin")
	}
	if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
		t.Fatal("generic/uncommitted refusal changed durable state")
	}
}

func requireFanOutOriginReadback(t *testing.T, fixture authorActivityReceiptFixture, backend string, ctx context.Context, event events.Event) {
	t.Helper()
	origin, inherited := event.InheritedFanOutOrigin()
	if !inherited {
		t.Fatal("readback proof requires inherited origin")
	}
	reader := fixture.store.(interface {
		LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
	})
	full, err := reader.LoadOperatorEvent(ctx, event.ID())
	if err != nil || full.SourceEventID != "" || full.InheritedFanOutOrigin == nil || *full.InheritedFanOutOrigin != origin {
		t.Fatalf("public readback lost separate origin: %+v %v", full, err)
	}
	wire, err := json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	var public map[string]json.RawMessage
	if err := json.Unmarshal(wire, &public); err != nil || len(public["inherited_fan_out_origin"]) == 0 || len(public["source_event_id"]) != 0 {
		t.Fatalf("public wire conflated origin with causality: %s %v", wire, err)
	}
	var raw string
	if err := fixture.db.QueryRowContext(ctx, `SELECT CAST(inherited_fan_out_origin AS TEXT) FROM events WHERE event_id=$1`, event.ID()).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"source_run_id", "trigger_event_id", "triggering_delivery_id", "declaration", "bundle_hash", "semantic_digest", "ordinal", "unknown_field"} {
		t.Run("corrupt_readback_"+field, func(t *testing.T) {
			var value map[string]any
			if err := json.Unmarshal([]byte(raw), &value); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "ordinal":
				value[field] = origin.Ordinal() + 10
			case "declaration":
				declaration, err := identity.AdmitDeclarationIdentity(".", "fan_out", "other/handlers/items.ready/fan_out")
				if err != nil {
					t.Fatal(err)
				}
				value[field] = declaration
			default:
				value[field] = uuid.NewString()
			}
			corrupt, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.db.ExecContext(ctx, `UPDATE events SET inherited_fan_out_origin=$1 WHERE event_id=$2`, string(corrupt), event.ID()); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := fixture.db.ExecContext(ctx, `UPDATE events SET inherited_fan_out_origin=$1 WHERE event_id=$2`, raw, event.ID()); err != nil {
					t.Error(err)
				}
			}()
			before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
			if _, _, err := fixture.store.(storeTestDurableEventBusStore).LoadPreparedPublishEvent(ctx, event.ID()); err == nil {
				t.Fatal("canonical readback accepted contradictory origin")
			}
			if _, err := reader.LoadOperatorEvent(ctx, event.ID()); err == nil {
				t.Fatal("public readback accepted contradictory origin")
			}
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
				t.Fatal("readback corruption rejection mutated durable evidence")
			}
		})
	}
	if _, err := reader.LoadOperatorEvent(ctx, event.ID()); err != nil {
		t.Fatalf("restored exact origin no longer reads: %v", err)
	}
}
