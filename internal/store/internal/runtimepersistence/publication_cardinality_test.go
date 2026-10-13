package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestAPIEventPublicationScalarStoragePreservesPhysicalPredicatesBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f, ctx := backend.open(t), testAuthorActivityContext()
			at := time.Now().UTC().Truncate(time.Microsecond)
			root := eventtest.RunCreatingRootIngress(uuid.NewString(), "fixture.started", "gateway", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, at)
			child := eventtest.Child(uuid.NewString(), "fixture.finished", "gateway", "", []byte(`{}`), 1, root, events.EventEnvelope{}, at)
			platform := eventtest.Child(uuid.NewString(), "platform.fixture", "gateway", "", []byte(`{}`), 1, root, events.EventEnvelope{}, at)
			foreign := eventtest.RunCreatingRootIngress(uuid.NewString(), "fixture.started", "gateway", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, at)
			node, err := runtimeidentity.ParseExecutableNode("fixture", "node")
			if err != nil {
				t.Fatal(err)
			}
			routes := []events.DeliveryRoute{{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "fixture", FlowInstance: "fixture"})}}
			if err := commitSemanticEventFixtureWithRoutes(ctx, f.store, root, routes); err != nil {
				t.Fatal(err)
			}
			for _, e := range []events.Event{child, platform, foreign} {
				if err := commitSemanticEventFixtureWithAgents(ctx, f.store, e, nil); err != nil {
					t.Fatal(err)
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, cut := range []struct {
				run, event string
				want       [3]int
			}{
				{root.RunID(), root.ID(), [3]int{1, 2, 1}},
				{root.RunID(), platform.ID(), [3]int{1, 2, 0}},
				{foreign.RunID(), root.ID(), [3]int{1, 1, 0}},
				{uuid.NewString(), root.ID(), [3]int{0, 0, 0}},
			} {
				assertPublicationScalarCut(t, ctx, f, cut.run, cut.event, cut.want)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 12 || counts.Total.ReadCommits != 12 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("physical cardinality escaped the selected read owner or mutated rows: %+v", counts)
			}
		})
	}
}

func assertPublicationScalarCut(t *testing.T, ctx context.Context, f authorActivityReceiptFixture, run, event string, want [3]int) {
	t.Helper()
	for i, proof := range []struct {
		query string
		args  []any
		read  func(context.Context, any) (int, error)
	}{
		{`SELECT COUNT(*) FROM runs WHERE run_id=$1`, []any{run}, func(ctx context.Context, owner any) (int, error) {
			return CountPhysicalRunIdentityForTest(ctx, owner, run)
		}},
		{`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name NOT LIKE 'platform.%'`, []any{run}, func(ctx context.Context, owner any) (int, error) {
			return CountApplicationRunEventStorageForTest(ctx, owner, run)
		}},
		{`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND event_id=$2`, []any{run, event}, func(ctx context.Context, owner any) (int, error) {
			return CountExactRunEventDeliveryStorageForTest(ctx, owner, run, event)
		}},
	} {
		var original int
		if err := f.db.QueryRowContext(ctx, proof.query, proof.args...).Scan(&original); err != nil {
			t.Fatal(err)
		}
		count, err := proof.read(ctx, f.store)
		if err != nil || count != want[i] || count != original {
			t.Fatalf("physical cut %s/%s index=%d count=%d original=%d want=%d error=%v", run, event, i, count, original, want[i], err)
		}
	}
}

func TestAPIEventPublicationScalarStorageRefusesRawCancelledAndClosedOwnersBothStores(t *testing.T) {
	run, event := uuid.NewString(), uuid.NewString()
	readers := []func(context.Context, any) (int, error){
		func(ctx context.Context, owner any) (int, error) {
			return CountPhysicalRunIdentityForTest(ctx, owner, run)
		},
		func(ctx context.Context, owner any) (int, error) {
			return CountApplicationRunEventStorageForTest(ctx, owner, run)
		},
		func(ctx context.Context, owner any) (int, error) {
			return CountExactRunEventDeliveryStorageForTest(ctx, owner, run, event)
		},
	}
	for _, read := range readers {
		for _, owner := range []any{nil, (*SQLiteRuntimeStore)(nil), (*PostgresStore)(nil), &SQLiteRuntimeStore{}, &PostgresStore{}, &sql.DB{}, &sql.Tx{}} {
			if count, err := read(context.Background(), owner); err == nil || count != 0 {
				t.Fatal("invalid physical reader became successful absence:", count, err)
			}
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			for _, read := range readers {
				if count, err := read(ctx, f.store); !errors.Is(err, context.Canceled) || count != 0 {
					t.Fatal("cancelled physical reader lost refusal:", count, err)
				}
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, read := range readers {
				if count, err := read(context.Background(), f.store); err == nil || count != 0 {
					t.Fatal("closed physical reader became successful absence:", count, err)
				}
			}
		})
	}
}
