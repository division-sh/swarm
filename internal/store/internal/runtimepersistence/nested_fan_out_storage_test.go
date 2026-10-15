package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func nestedFanOutReadCases(run, event, delivery string) []fanOutSemanticReadCase {
	key := fanoutobligation.IntentKey{RunID: run, TriggeringDeliveryID: delivery,
		ElementRef: contracts.FanOutElementRef{FlowPath: "physical", Family: "fan_out", SemanticPath: "physical.source"}}
	return []fanOutSemanticReadCase{
		{"source_ids", func(ctx context.Context, owner any) (any, error) {
			return ReadNestedSourceEventIDsForTest(ctx, owner, run, "snapshot.fixture", event)
		}, []string(nil), false},
		{"run_named_ids", func(ctx context.Context, owner any) (any, error) {
			return ReadNestedRunNamedEventIDsForTest(ctx, owner, run, "snapshot.fixture")
		}, []string(nil), false},
		{"sibling_id", func(ctx context.Context, owner any) (any, error) {
			return ReadNestedSiblingSourceEventIDForTest(ctx, owner, run, event)
		}, "", true},
		{"event_count", func(ctx context.Context, owner any) (any, error) {
			return CountNestedRunEventIdentityForTest(ctx, owner, run, event)
		}, 0, false},
		{"barrier_status", func(ctx context.Context, owner any) (any, error) {
			return ReadNestedFanOutBarrierStatusForTest(ctx, owner, key)
		}, "", true},
		{"barrier", func(ctx context.Context, owner any) (any, error) {
			return ReadNestedFanOutBarrierForTest(ctx, owner, key)
		}, NestedFanOutBarrierStorage{}, true},
		{"receipt", func(ctx context.Context, owner any) (any, error) {
			return ReadNestedPipelineReceiptOutcomeForTest(ctx, owner, event)
		}, "", true},
		{"successful_receipt_count", func(ctx context.Context, owner any) (any, error) {
			return CountNestedSuccessfulPipelineReceiptForTest(ctx, owner, event)
		}, 0, false},
		{"cursor_claim", func(ctx context.Context, owner any) (any, error) {
			return ReadNestedFanOutCursorClaimForTest(ctx, owner, key)
		}, NestedFanOutCursorClaimStorage{}, true},
		{"run_outcome_count", func(ctx context.Context, owner any) (any, error) {
			return CountNestedRunFanOutOutcomesForTest(ctx, owner, run)
		}, 0, false},
		{"owed", func(ctx context.Context, owner any) (any, error) {
			return ReadNestedRunFanOutOwedForTest(ctx, owner, run)
		}, NestedFanOutOwedStorage{}, false},
		{"committed_prefix", func(ctx context.Context, owner any) (any, error) {
			return ReadNestedCommittedOutcomeEventIDsForTest(ctx, owner, run)
		}, []string(nil), false},
		{"account_keys", func(ctx context.Context, owner any) (any, error) {
			return ReadNestedAccountIntentKeysForTest(ctx, owner, run)
		}, []fanoutobligation.IntentKey(nil), false},
		{"handoffs", func(ctx context.Context, owner any) (any, error) {
			return CountNestedContinuationHandoffsForTest(ctx, owner, event)
		}, 0, false},
		{"entity_rows", func(ctx context.Context, owner any) (any, error) {
			return ReadNestedRunEntityStatesForTest(ctx, owner, run)
		}, []NestedFanOutEntityStorage(nil), false},
		{"entity_state_fields", func(ctx context.Context, owner any) (any, error) {
			return ReadNestedRunEntityStateFieldsForTest(ctx, owner, run)
		}, []NestedFanOutEntityStorage(nil), false},
		{"entity", func(ctx context.Context, owner any) (any, error) {
			return ReadNestedEntityFieldsRevisionForTest(ctx, owner, run, "physical/child")
		}, NestedFanOutEntityStorage{}, true},
		{"repeated_intent", func(ctx context.Context, owner any) (any, error) {
			return ReadRepeatedFanOutIntentForTest(ctx, owner, key)
		}, RepeatedFanOutIntentStorage{}, true},
		{"repeated_outcome_count", func(ctx context.Context, owner any) (any, error) {
			return CountRepeatedFanOutTriggerOutcomesForTest(ctx, owner, run, delivery)
		}, 0, false},
		{"repeated_snapshot", func(ctx context.Context, owner any) (any, error) {
			return ReadRepeatedFanOutSnapshotForTest(ctx, owner, run)
		}, [8]int64{}, false},
		{"repeated_completions", func(ctx context.Context, owner any) (any, error) {
			return ReadRepeatedFanOutCompletionEventIDsForTest(ctx, owner, run)
		}, []string(nil), false},
	}
}

func TestNestedFanOutStorageKeepsRequiredAbsenceZeroDefaultsAndOriginalReadsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f, ctx := backend.open(t), testAuthorActivityContext()
			cases := nestedFanOutReadCases(uuid.NewString(), uuid.NewString(), uuid.NewString())
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, tc := range cases {
				got, err := tc.read(ctx, f.store)
				if (err != nil) != tc.absentError || (err != nil && !errors.Is(err, sql.ErrNoRows)) || !reflect.DeepEqual(got, tc.empty) {
					t.Fatalf("missing %s changed absence/defaults or granted evidence: %+v %v", tc.name, got, err)
				}
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 21 || counts.Total.ReadCommits != 14 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("nested observations lost their independent original cuts: %+v", counts)
			}
		})
	}
}

func TestNestedFanOutStorageRefusesRawForeignCancelledAndClosedEvidenceBothStores(t *testing.T) {
	refuse := func(t *testing.T, ctx context.Context, selected any, run, event, delivery string) {
		t.Helper()
		for _, tc := range nestedFanOutReadCases(run, event, delivery) {
			got, err := tc.read(ctx, selected)
			if err == nil || !reflect.DeepEqual(got, tc.empty) {
				t.Fatalf("failed %s manufactured provisional evidence: %+v %v", tc.name, got, err)
			}
		}
	}
	run, event, delivery := uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, selected := range []any{nil, struct{}{}, &sql.DB{}, &sql.Tx{}, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil)} {
		refuse(t, context.Background(), selected, run, event, delivery)
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			refuse(t, context.Background(), f.store, "invalid", "invalid", "invalid")
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			refuse(t, ctx, f.store, run, event, delivery)
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(t, context.Background(), f.store, run, event, delivery)
		})
	}
}

func TestNestedEventDiscoveryAndRepeatedSnapshotKeepRunScopeAndNullParentBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f, ctx := backend.open(t), testAuthorActivityContext()
			first, foreign := runForkSnapshotFixtureEvent(t, ctx, f.store), runForkSnapshotFixtureEvent(t, ctx, f.store)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			ids, err := ReadNestedRunNamedEventIDsForTest(ctx, f.store, first.RunID(), string(first.Type()))
			if err != nil || !reflect.DeepEqual(ids, []string{first.ID()}) {
				t.Fatalf("named discovery borrowed another run: %v %v", ids, err)
			}
			ids, err = ReadNestedSourceEventIDsForTest(ctx, f.store, first.RunID(), string(first.Type()), first.ID())
			if err != nil || ids != nil {
				t.Fatalf("NULL parent became self-parent lineage: %v %v", ids, err)
			}
			if id, err := ReadNestedSiblingSourceEventIDForTest(ctx, f.store, first.RunID(), first.ID()); !errors.Is(err, sql.ErrNoRows) || id != "" {
				t.Fatalf("NULL parent inferred unrelated sibling: %s %v", id, err)
			}
			if count, err := CountNestedRunEventIdentityForTest(ctx, f.store, first.RunID(), foreign.ID()); err != nil || count != 0 {
				t.Fatalf("exact run/event count widened: %d %v", count, err)
			}
			if snapshot, err := ReadRepeatedFanOutSnapshotForTest(ctx, f.store, first.RunID()); err != nil || snapshot != [8]int64{1} {
				t.Fatalf("one-statement snapshot lost exact run/empty defaults: %v %v", snapshot, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 5 || counts.Total.ReadCommits != 4 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("discovery/snapshot escaped original independent reads: %+v", counts)
			}
		})
	}
}

func TestNestedSourceEventDiscoveryKeepsImmediateParentMultiplicityAndOrderBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f, ctx := backend.open(t), testAuthorActivityContext()
			first, foreign := runForkSnapshotFixtureEvent(t, ctx, f.store), runForkSnapshotFixtureEvent(t, ctx, f.store)
			other := eventtest.ExistingRunRootIngress(uuid.NewString(), "snapshot.fixture", "fixture", "", []byte(`{"value":2}`), 0, first.RunID(), events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixtureWithRoutes(ctx, f.store, other, nil); err != nil {
				t.Fatal(err)
			}
			name := events.EventType("snapshot.child")
			ids := []string{"11111111-1111-4111-8111-111111111112", "11111111-1111-4111-8111-111111111111", uuid.NewString(), uuid.NewString()}
			parents := []events.Event{first, first, other, foreign}
			for i, parent := range parents {
				child := eventtest.Child(ids[i], name, "fixture", "", []byte(`{"value":1}`), 1, parent, events.EventEnvelope{}, time.Now().UTC())
				if err := commitSemanticEventFixtureWithRoutes(ctx, f.store, child, nil); err != nil {
					t.Fatal(err)
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			got, err := ReadNestedSourceEventIDsForTest(ctx, f.store, first.RunID(), string(name), first.ID())
			if err != nil || !reflect.DeepEqual(got, []string{ids[1], ids[0]}) {
				t.Fatalf("immediate lineage lost sibling multiplicity, ID order or run/parent scope: %v %v", got, err)
			}
			named, err := ReadNestedRunNamedEventIDsForTest(ctx, f.store, first.RunID(), string(name))
			slices.Sort(named)
			want := append([]string(nil), ids[:3]...)
			slices.Sort(want)
			if err != nil || !reflect.DeepEqual(named, want) {
				t.Fatalf("run/name discovery gained a parent filter or foreign run: %v %v", named, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 2 || counts.Total.ReadCommits != 2 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("lineage observations escaped original read owner: %+v", counts)
			}
		})
	}
}
