package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

type fanOutSemanticReadCase struct {
	name        string
	read        func(context.Context, any) (any, error)
	empty       any
	absentError bool
}

func TestFanOutSemanticPayloadHydrationKeepsForeignRunDuplicatesNullsAndNativeSnapshotBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f, ctx := backend.open(t), testAuthorActivityContext()
			first, foreign := runForkSnapshotFixtureEvent(t, ctx, f.store), runForkSnapshotFixtureEvent(t, ctx, f.store)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			var observed []FanOutSemanticOutcomeStorage
			err = readServedDeliveryObservation(ctx, f.store, func(ctx context.Context, tx *sql.Tx) error {
				rows := []FanOutSemanticOutcomeStorage{
					{Ordinal: 0, Kind: "committed", EventID: first.ID(), EventIDPresent: true},
					{Ordinal: 0, Kind: "committed", EventID: foreign.ID(), EventIDPresent: true},
					{Ordinal: 1, Kind: "committed", EventID: first.ID(), EventIDPresent: true},
					{Ordinal: 2, Kind: "semantic_rejected", Failure: []byte(`{"class":"internal_failure"}`)},
				}
				var err error
				observed, err = hydrateFanOutOutcomePayloadBytesForTest(ctx, tx, f.store, rows)
				return err
			})
			if err != nil || len(observed) != 4 || observed[1].EventID != foreign.ID() || !observed[1].EventIDPresent ||
				!bytes.Equal(observed[0].Payload, first.Payload()) || !bytes.Equal(observed[1].Payload, foreign.Payload()) ||
				!bytes.Equal(observed[2].Payload, first.Payload()) || observed[3].EventIDPresent || observed[3].Payload != nil ||
				string(observed[3].Failure) != `{"class":"internal_failure"}` {
				t.Fatalf("physical hydration narrowed run scope, deduplicated ordinals or synthesized NULL data: %+v %v", observed, err)
			}
			observed[0].Payload[0] = '!'
			if !bytes.Equal(observed[2].Payload, first.Payload()) {
				t.Fatal("duplicate ordinal payloads share a mutable byte buffer")
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("joined byte hydration escaped its one original read snapshot: %+v", counts)
			}
			var missing []FanOutSemanticOutcomeStorage
			err = readServedDeliveryObservation(ctx, f.store, func(ctx context.Context, tx *sql.Tx) error {
				var err error
				missing, err = hydrateFanOutOutcomePayloadBytesForTest(ctx, tx, f.store, []FanOutSemanticOutcomeStorage{{EventID: uuid.NewString(), EventIDPresent: true}})
				return err
			})
			if err == nil || missing != nil {
				t.Fatalf("missing event in the retained snapshot became partial evidence: %+v %v", missing, err)
			}
		})
	}
}

func fanOutSemanticReadCases(run, event, mutation, entity string) []fanOutSemanticReadCase {
	return []fanOutSemanticReadCase{
		{"progress", func(ctx context.Context, owner any) (any, error) {
			return ReadFanOutSourceProgressStorageForTest(ctx, owner, run, event)
		}, FanOutSourceProgressStorage{}, false},
		{"outcomes", func(ctx context.Context, owner any) (any, error) {
			return ReadFanOutSourceOutcomesStorageForTest(ctx, owner, run, event)
		}, []FanOutSemanticOutcomeStorage(nil), false},
		{"outcome_count", func(ctx context.Context, owner any) (any, error) {
			return CountFanOutSourceOutcomesStorageForTest(ctx, owner, run, event)
		}, 0, false},
		{"blocked", func(ctx context.Context, owner any) (any, error) {
			return ReadFanOutBlockedSourceStorageForTest(ctx, owner, run, event)
		}, FanOutBlockedSourceStorage{}, true},
		{"trigger", func(ctx context.Context, owner any) (any, error) {
			return ReadFanOutTriggerDeliveryDiagnosticStorageForTest(ctx, owner, run, event)
		}, FanOutTriggerDeliveryStorage{}, true},
		{"delivered", func(ctx context.Context, owner any) (any, error) {
			return CountFanOutDeliveredTriggerStorageForTest(ctx, owner, run, event)
		}, 0, false},
		{"run_progress", func(ctx context.Context, owner any) (any, error) {
			return ReadRunFanOutProgressStorageForTest(ctx, owner, run)
		}, FanOutRunProgressStorage{}, true},
		{"leased", func(ctx context.Context, owner any) (any, error) {
			return CountLeasedRunFanOutStorageForTest(ctx, owner, run)
		}, 0, false},
		{"logs", func(ctx context.Context, owner any) (any, error) {
			return ReadLastThreeFanOutRuntimeLogPayloadsForTest(ctx, owner, run)
		}, [][]byte(nil), false},
		{"portfolio", func(ctx context.Context, owner any) (any, error) {
			return ReadFanOutPortfolioFieldsStorageForTest(ctx, owner, run)
		}, []byte(nil), true},
		{"mutation", func(ctx context.Context, owner any) (any, error) {
			return ReadFanOutAccountIDsMutationSourceStorageForTest(ctx, owner, run, mutation, entity)
		}, "", true},
	}
}

func TestFanOutSemanticStorageMissingFactsDoNotGrantProgressOrChangeStrictNullBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f, ctx := backend.open(t), testAuthorActivityContext()
			cases := fanOutSemanticReadCases(uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString())
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, tc := range cases {
				got, err := tc.read(ctx, f.store)
				if (err != nil) != tc.absentError || !reflect.DeepEqual(got, tc.empty) {
					t.Fatalf("missing %s changed original absence/strict SUM NULL or granted evidence: got=%+v err=%v", tc.name, got, err)
				}
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 11 || counts.Total.ReadCommits != 6 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("semantic observers bypassed one original snapshot each: %+v", counts)
			}
		})
	}
}

func TestFanOutSemanticStorageRefusesRawInvalidCancelledAndClosedOwnersBothStores(t *testing.T) {
	refuse := func(t *testing.T, ctx context.Context, selected any, ids ...string) {
		t.Helper()
		for _, tc := range fanOutSemanticReadCases(ids[0], ids[1], ids[2], ids[3]) {
			if out, err := tc.read(ctx, selected); err == nil || !reflect.DeepEqual(out, tc.empty) {
				t.Fatalf("failed %s manufactured native evidence: %+v %v", tc.name, out, err)
			}
		}
	}
	ids := []string{uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()}
	for _, selected := range []any{nil, struct{}{}, &sql.DB{}, &sql.Tx{}, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil)} {
		refuse(t, context.Background(), selected, ids...)
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			refuse(t, context.Background(), f.store, "invalid", ids[1], ids[2], ids[3])
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			refuse(t, ctx, f.store, ids...)
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(t, context.Background(), f.store, ids...)
		})
	}
}
