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
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func seedFanOutCompositeStorageNodeClaim(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture) runtimedelivery.ClaimedObligation {
	t.Helper()
	runID := uuid.NewString()
	seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "recovery.requested", "gateway", "", nil, 0, runID, events.EventEnvelope{}, time.Now().UTC())
	route := testEntitylessNodeDeliveryRoute("composite-storage-source")
	if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(ctx, fixture.store.(deliveryFixtureStore), event, route)
	if err != nil {
		t.Fatal(err)
	}
	return claimed
}

func TestFanOutCompositeStoragePreservesTriggerKeySourceAndPhysicalCapsuleBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			active, sibling := seedFanOutCompositeStorageNodeClaim(t, ctx, fixture), seedFanOutCompositeStorageNodeClaim(t, ctx, fixture)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, active, 2)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, sibling, 1)
			ref := runtimecontracts.FanOutElementRef{FlowPath: "physical", Family: "fan_out", SemanticPath: "physical.source"}
			key := fanoutobligation.IntentKey{RunID: active.Claim.RunID(), TriggeringDeliveryID: active.Snapshot.DeliveryID, ElementRef: ref}
			node := mustPersistenceRootNode("composite-storage-source")
			var expectedCapsule []byte
			now := time.Now().UTC().Truncate(time.Microsecond)
			// Physical controls retain projected bytes and adjacent declarations;
			// actual compiled journeys separately prove lawful source/issuance.
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET capsule=$1 WHERE run_id=$2`, `{" key ": [1.0], "z": " padded "}`, key.RunID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_intents
					(run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,bundle_hash,semantic_digest,source_kind,source_event_id,
					source_field,cardinality,cursor,status,next_chunk_size,capsule,created_at,updated_at)
					VALUES ($1,$2,'physical','fan_out','physical.other',$3,$4,'event_payload_field',$5,'items',1,1,'closed',1,'{}',$6,$6)`,
					key.RunID, key.TriggeringDeliveryID, "bundle-v2:sha256:"+strings.Repeat("1", 64), "sha256:"+strings.Repeat("2", 64), active.Snapshot.EventID, now); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_outcomes
					(outcome_id,run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,event_id,created_at)
					VALUES ($1,$2,$3,'physical','fan_out','physical.other',0,'committed',$4,$5)`,
					uuid.NewString(), key.RunID, key.TriggeringDeliveryID, sibling.Snapshot.EventID, now); err != nil {
					return err
				}
				return tx.QueryRowContext(ctx, `SELECT capsule FROM fan_out_intents WHERE run_id=$1 AND semantic_path='physical.source'`, key.RunID).Scan(&expectedCapsule)
			}); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			rows, err := ReadFanOutTriggeredIntentStorageForTest(ctx, fixture.store, key.RunID, active.Snapshot.EventID, node, ref)
			wantSource := fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEventPayloadField, EventID: active.Snapshot.EventID, Field: "items"}
			if err != nil || len(rows) != 1 || rows[0].TriggeringDeliveryID != key.TriggeringDeliveryID || rows[0].SemanticDigest != "sha256:"+strings.Repeat("2", 64) ||
				string(rows[0].Capsule) != string(expectedCapsule) || !strings.Contains(string(rows[0].Capsule), "1.0") || rows[0].Source != wantSource ||
				rows[0].Cursor != 2 || rows[0].Cardinality != 2 || rows[0].Status != "closed" {
				t.Fatalf("lost exact trigger/declaration/source/physical capsule: %+v err=%v", rows, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("trigger/intent join bypassed original snapshot: %+v", counts)
			}
			completion, err := ReadFanOutIntentCompletionStorageForTest(ctx, fixture.store, key)
			if err != nil || len(completion.Outcomes) != 2 || completion.Cursor != 2 || completion.Cardinality != 2 || completion.Status != "closed" {
				t.Fatalf("completion broadened exact declaration: %+v %v", completion, err)
			}
			for ordinal, row := range completion.Outcomes {
				if row.Ordinal != ordinal || row.Kind != "committed" || row.EventID == "" || row.EventID == sibling.Snapshot.EventID {
					t.Fatalf("lost exact ordered publication evidence: %+v", completion)
				}
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 2 || counts.Total.ReadCommits != 2 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("completion observations bypassed original snapshot: %+v", counts)
			}
			for _, wrong := range []struct {
				runID, eventID string
				node           runtimeidentity.ExecutableNode
				ref            runtimecontracts.FanOutElementRef
			}{
				{sibling.Claim.RunID(), active.Snapshot.EventID, node, ref},
				{key.RunID, sibling.Snapshot.EventID, node, ref},
				{key.RunID, active.Snapshot.EventID, mustPersistenceRootNode("other-source"), ref},
				{key.RunID, active.Snapshot.EventID, node, runtimecontracts.FanOutElementRef{FlowPath: ".", Family: ref.Family, SemanticPath: ref.SemanticPath}},
				{key.RunID, active.Snapshot.EventID, node, runtimecontracts.FanOutElementRef{FlowPath: ref.FlowPath, Family: ref.Family, SemanticPath: "physical.absent"}},
			} {
				if got, err := ReadFanOutTriggeredIntentStorageForTest(ctx, fixture.store, wrong.runID, wrong.eventID, wrong.node, wrong.ref); err != nil || len(got) != 0 {
					t.Fatalf("trigger join inferred another scope: %+v %v", got, err)
				}
			}
			otherKey := key
			otherKey.ElementRef.SemanticPath = "physical.other"
			if got, err := ReadFanOutIntentCompletionStorageForTest(ctx, fixture.store, otherKey); err != nil || len(got.Outcomes) != 1 || got.Outcomes[0].EventID != sibling.Snapshot.EventID || got.Cardinality != 1 {
				t.Fatalf("adjacent declaration was lost or merged: %+v %v", got, err)
			}
			if got, err := ReadFanOutTriggeredIntentStorageForTest(ctx, fixture.store, key.RunID, active.Snapshot.EventID, node, ref); err != nil || !reflect.DeepEqual(got, rows) || probe.Snapshot().Total.WriteCommits != 0 {
				t.Fatalf("observation changed retained source: %+v %v", got, err)
			}
		})
	}
}

func TestFanOutCompositeStorageRejectsInvalidOwnersOriginsAndPartialEvidenceBothStores(t *testing.T) {
	ctx := context.Background()
	runID, eventID, deliveryID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	node := mustPersistenceRootNode("composite-storage-source")
	ref := runtimecontracts.FanOutElementRef{FlowPath: "physical", Family: "fan_out", SemanticPath: "physical.source"}
	key := fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: deliveryID, ElementRef: ref}
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadFanOutTriggeredIntentStorageForTest(ctx, owner, runID, eventID, node, ref); err == nil || got != nil {
			t.Errorf("invalid owner returned trigger evidence: %+v %v", got, err)
		}
		if got, err := ReadFanOutIntentCompletionStorageForTest(ctx, owner, key); err == nil || got.Outcomes != nil || got.Cursor != 0 || got.Cardinality != 0 || got.Status != "" {
			t.Errorf("invalid owner returned completion: %+v %v", got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			active := seedFanOutCompositeStorageNodeClaim(t, ctx, fixture)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, active, 2)
			key := fanoutobligation.IntentKey{RunID: active.Claim.RunID(), TriggeringDeliveryID: active.Snapshot.DeliveryID, ElementRef: ref}
			for _, invalid := range []string{"", "bad", uuid.Nil.String(), "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"} {
				if got, err := ReadFanOutTriggeredIntentStorageForTest(ctx, fixture.store, invalid, active.Snapshot.EventID, node, ref); err == nil || got != nil {
					t.Errorf("invalid run returned trigger evidence: %+v %v", got, err)
				}
				if got, err := ReadFanOutTriggeredIntentStorageForTest(ctx, fixture.store, key.RunID, invalid, node, ref); err == nil || got != nil {
					t.Errorf("invalid event returned trigger evidence: %+v %v", got, err)
				}
				badKey := key
				badKey.TriggeringDeliveryID = invalid
				if got, err := ReadFanOutIntentCompletionStorageForTest(ctx, fixture.store, badKey); err == nil || got.Outcomes != nil || got.Status != "" {
					t.Errorf("invalid delivery returned completion: %+v %v", got, err)
				}
				badKey = key
				badKey.RunID = invalid
				if got, err := ReadFanOutIntentCompletionStorageForTest(ctx, fixture.store, badKey); err == nil || got.Outcomes != nil || got.Status != "" {
					t.Errorf("invalid run returned completion: %+v %v", got, err)
				}
			}
			if got, err := ReadFanOutTriggeredIntentStorageForTest(ctx, fixture.store, key.RunID, active.Snapshot.EventID, node, runtimecontracts.FanOutElementRef{}); err == nil || got != nil {
				t.Fatalf("unadmitted declaration returned trigger evidence: %+v %v", got, err)
			}
			absent := key
			absent.TriggeringDeliveryID = uuid.NewString()
			if got, err := ReadFanOutIntentCompletionStorageForTest(ctx, fixture.store, absent); !errors.Is(err, sql.ErrNoRows) || got.Outcomes != nil || got.Status != "" {
				t.Fatalf("missing exact intent fabricated completion: %+v %v", got, err)
			}
			feed := fanoutobligation.IntentKey{RunID: key.RunID, DeploymentFeedID: uuid.NewString()}
			if got, err := ReadFanOutIntentCompletionStorageForTest(ctx, fixture.store, feed); err == nil || got.Outcomes != nil || got.Status != "" {
				t.Fatalf("deployment origin entered handler completion reader: %+v %v", got, err)
			}
			if got, err := ReadFanOutTriggeredIntentStorageForTest(ctx, fixture.store, key.RunID, active.Snapshot.EventID, runtimeidentity.ExecutableNode{}, ref); err == nil || got != nil {
				t.Fatalf("unadmitted node returned trigger evidence: %+v %v", got, err)
			}
			before, err := ReadFanOutIntentCompletionStorageForTest(ctx, fixture.store, key)
			if err != nil || len(before.Outcomes) != 2 {
				t.Fatalf("refusal checkpoint lacks actual outcomes: %+v %v", before, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadFanOutTriggeredIntentStorageForTest(cancelled, fixture.store, key.RunID, active.Snapshot.EventID, node, ref); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled trigger observation returned rows: %+v %v", got, err)
			}
			if got, err := ReadFanOutIntentCompletionStorageForTest(cancelled, fixture.store, key); !errors.Is(err, context.Canceled) || got.Outcomes != nil || got.Status != "" {
				t.Fatalf("cancelled completion returned rows: %+v %v", got, err)
			}
			rename := func(from, to string) {
				t.Helper()
				if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, "ALTER TABLE "+from+" RENAME TO "+to)
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			rename("fan_out_intents", "unavailable_composite_storage_intents")
			restored := false
			defer func() {
				if !restored {
					rename("unavailable_composite_storage_intents", "fan_out_intents")
				}
			}()
			if got, err := ReadFanOutIntentCompletionStorageForTest(ctx, fixture.store, key); err == nil || got.Outcomes != nil || got.Cursor != 0 || got.Cardinality != 0 || got.Status != "" {
				t.Fatalf("missing intent returned partial outcomes: %+v %v", got, err)
			}
			if got, err := ReadFanOutTriggeredIntentStorageForTest(ctx, fixture.store, key.RunID, active.Snapshot.EventID, node, ref); err == nil || got != nil {
				t.Fatalf("missing intent storage returned trigger evidence: %+v %v", got, err)
			}
			rename("unavailable_composite_storage_intents", "fan_out_intents")
			restored = true
			if got, err := ReadFanOutIntentCompletionStorageForTest(ctx, fixture.store, key); err != nil || !reflect.DeepEqual(got, before) {
				t.Fatalf("failed observation changed exact outcomes: %+v %v", got, err)
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET cardinality=3,cursor=3 WHERE run_id=$1`, key.RunID); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, `INSERT INTO fan_out_outcomes
					(outcome_id,run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,failure,created_at)
					VALUES ($1,$2,$3,'physical','fan_out','physical.source',2,'semantic_rejected','{"class":"internal_failure"}',$4)`,
					uuid.NewString(), key.RunID, key.TriggeringDeliveryID, time.Now().UTC())
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadFanOutIntentCompletionStorageForTest(ctx, fixture.store, key); err == nil || got.Outcomes != nil || got.Status != "" {
				t.Fatalf("NULL outcome event was filtered or fabricated: %+v %v", got, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadFanOutTriggeredIntentStorageForTest(ctx, fixture.store, key.RunID, active.Snapshot.EventID, node, ref); err == nil || got != nil {
				t.Fatalf("closed owner returned trigger evidence: %+v %v", got, err)
			}
			if got, err := ReadFanOutIntentCompletionStorageForTest(ctx, fixture.store, key); err == nil || got.Outcomes != nil || got.Status != "" {
				t.Fatalf("closed owner returned completion: %+v %v", got, err)
			}
		})
	}
}
