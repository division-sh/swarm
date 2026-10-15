package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

type templateStorageReadCase struct {
	name             string
	read             func(context.Context, any) (any, error)
	empty            any
	absent, postgres bool
}

func templateStorageReadCases(run, event string) []templateStorageReadCase {
	return []templateStorageReadCase{
		{"delivery_diagnostics", func(ctx context.Context, owner any) (any, error) {
			return ReadTemplateEventDeliveryDiagnosticsForTest(ctx, owner, event)
		}, []TemplateDeliveryDiagnosticStorage(nil), false, false},
		{"receipt_diagnostics", func(ctx context.Context, owner any) (any, error) {
			return ReadTemplateEventReceiptDiagnosticsForTest(ctx, owner, event)
		}, []TemplateReceiptDiagnosticStorage(nil), false, false},
		{"account_diagnostics", func(ctx context.Context, owner any) (any, error) {
			return ReadTemplateAccountDiagnosticsForTest(ctx, owner)
		}, []TemplateAccountDiagnosticStorage(nil), false, false},
		{"node", func(ctx context.Context, owner any) (any, error) {
			return CountTemplateNodeDeliveryForTest(ctx, owner, event, "node")
		}, 0, false, false},
		{"chronology", func(ctx context.Context, owner any) (any, error) {
			return CountTemplateNodeDeliveryChronologyForTest(ctx, owner, event, "node")
		}, 0, false, false},
		{"settled_attempt", func(ctx context.Context, owner any) (any, error) {
			return CountTemplateNodeDeliveredAttemptForTest(ctx, owner, event, "node")
		}, 0, false, false},
		{"agent", func(ctx context.Context, owner any) (any, error) {
			return CountTemplateAgentDeliveryForTest(ctx, owner, event, "agent")
		}, 0, false, false},
		{"placeholder_delivery", func(ctx context.Context, owner any) (any, error) {
			return CountTemplatePlaceholderDeliveryForTest(ctx, owner, event)
		}, 0, false, false},
		{"all_deliveries", func(ctx context.Context, owner any) (any, error) {
			return CountTemplateEventDeliveriesForTest(ctx, owner, event)
		}, 0, false, false},
		{"carrier", func(ctx context.Context, owner any) (any, error) {
			return CountTemplateCarrierDeliveriesForTest(ctx, owner, event)
		}, 0, false, false},
		{"node_target", func(ctx context.Context, owner any) (any, error) {
			return CountPostgresTemplateNodeTargetForTest(ctx, owner, event, `{}`, "node")
		}, 0, false, true},
		{"delivered_target", func(ctx context.Context, owner any) (any, error) {
			return CountPostgresTemplateDeliveredNodeTargetForTest(ctx, owner, event, "node", `{}`)
		}, 0, false, true},
		{"node_receipt", func(ctx context.Context, owner any) (any, error) {
			return CountTemplateNodeReceiptForTest(ctx, owner, event, "node")
		}, 0, false, false},
		{"scope", func(ctx context.Context, owner any) (any, error) {
			return CountTemplateSubscribedReplayScopeForTest(ctx, owner, event)
		}, 0, false, false},
		{"all_event_scopes", func(ctx context.Context, owner any) (any, error) {
			return CountTemplateEventReplayScopesForTest(ctx, owner, event)
		}, 0, false, false},
		{"instances", func(ctx context.Context, owner any) (any, error) {
			return CountTemplateInstancesForTest(ctx, owner, "consumer")
		}, 0, false, false},
		{"operating_count", func(ctx context.Context, owner any) (any, error) {
			return CountRunOperatingInstancesForTest(ctx, owner, run)
		}, 0, false, false},
		{"operating_fields", func(ctx context.Context, owner any) (any, error) {
			return ReadRunOperatingInstanceFieldsForTest(ctx, owner, run)
		}, TemplateInstanceFieldsStorage{}, true, false},
		{"accounts", func(ctx context.Context, owner any) (any, error) {
			return CountTemplateAccountEntitiesForTest(ctx, owner)
		}, 0, false, false},
		{"reviewed_fields", func(ctx context.Context, owner any) (any, error) {
			return CountPostgresReviewedTemplateAccountForTest(ctx, owner, `{}`)
		}, 0, false, true},
		{"latest_identity", func(ctx context.Context, owner any) (any, error) {
			return ReadLatestTemplateAccountIdentityForTest(ctx, owner)
		}, TemplateAccountIdentityStorage{}, true, false},
		{"named_event", func(ctx context.Context, owner any) (any, error) {
			return ReadTemplateNamedEventIdentityForTest(ctx, owner, "snapshot.fixture")
		}, "", true, false},
		{"source_event", func(ctx context.Context, owner any) (any, error) {
			return ReadTemplateSourceNamedEventIdentityForTest(ctx, owner, "snapshot.fixture", event)
		}, "", true, false},
		{"payload", func(ctx context.Context, owner any) (any, error) {
			return ReadTemplateEventPayloadTextForTest(ctx, owner, event)
		}, "", true, false},
		{"event_count", func(ctx context.Context, owner any) (any, error) {
			return CountTemplateEventIdentityForTest(ctx, owner, event)
		}, 0, false, false},
	}
}

func TestTemplateStorageObservationsKeepOriginalAbsenceAndIndependentCutsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f, ctx := backend.open(t), testAuthorActivityContext()
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			var begun, committed uint64
			for _, tc := range templateStorageReadCases(uuid.NewString(), uuid.NewString()) {
				got, err := tc.read(ctx, f.store)
				foreign := tc.postgres && backend.name != "postgres"
				if (err != nil) != (tc.absent || foreign) || !reflect.DeepEqual(got, tc.empty) || (tc.absent && !errors.Is(err, sql.ErrNoRows)) {
					t.Fatalf("%s lost absence/backend/default evidence: %+v / %v", tc.name, got, err)
				}
				if !foreign {
					begun++
					if !tc.absent {
						committed++
					}
				}
			}
			if receipt := probe.Snapshot(); receipt.Total.Begun != begun || receipt.Total.ReadCommits != committed || receipt.Total.WriteCommits != 0 || receipt.Active != 0 {
				t.Fatalf("template witnesses lost independent read cuts: %+v want %d/%d", receipt, begun, committed)
			}
		})
	}
}

func TestTemplateStorageObservationsRefuseRawCancelledAndClosedOwnersBothStores(t *testing.T) {
	cases := templateStorageReadCases(uuid.NewString(), uuid.NewString())
	refuse := func(t *testing.T, ctx context.Context, owner any) {
		t.Helper()
		for _, tc := range cases {
			got, err := tc.read(ctx, owner)
			if err == nil || !reflect.DeepEqual(got, tc.empty) {
				t.Fatalf("%s supplied provisional evidence: %+v / %v", tc.name, got, err)
			}
		}
	}
	for _, owner := range []any{nil, struct{}{}, &sql.DB{}, &sql.Tx{}, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil)} {
		refuse(t, context.Background(), owner)
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			refuse(t, ctx, f.store)
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(t, context.Background(), f.store)
		})
	}
}

func TestTemplateEventObservationsKeepGlobalNameAndExactParentBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f, ctx := backend.open(t), testAuthorActivityContext()
			first, foreign := runForkSnapshotFixtureEvent(t, ctx, f.store), runForkSnapshotFixtureEvent(t, ctx, f.store)
			id, err := ReadTemplateNamedEventIdentityForTest(ctx, f.store, string(first.Type()))
			if err != nil || (id != first.ID() && id != foreign.ID()) {
				t.Fatalf("global first row changed: %s %v", id, err)
			}
			if id, err := ReadTemplateSourceNamedEventIdentityForTest(ctx, f.store, string(first.Type()), first.ID()); !errors.Is(err, sql.ErrNoRows) || id != "" {
				t.Fatalf("NULL parent was inferred: %s %v", id, err)
			}
			if count, err := CountTemplateEventIdentityForTest(ctx, f.store, foreign.ID()); err != nil || count != 1 {
				t.Fatalf("physical event count gained a run filter: %d %v", count, err)
			}
			if raw, err := ReadTemplateEventPayloadTextForTest(ctx, f.store, first.ID()); err != nil || raw == "" {
				t.Fatalf("payload text disappeared: %s %v", raw, err)
			}
		})
	}
}
