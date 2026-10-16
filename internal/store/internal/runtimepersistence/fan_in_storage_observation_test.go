package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestFanInStorageObservationsPreserveGlobalScopeOrderAndIndependentReadsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f, ctx := backend.open(t), testAuthorActivityContext()
			first := runForkSnapshotFixtureEvent(t, ctx, f.store)
			other := runForkSnapshotFixtureEvent(t, ctx, f.store)
			var operating, periods []string
			for i, name := range []string{"operating/a/operating.reported", "other/operating.reported", "a/period.reported", "period.reported", "operating.reported.extra"} {
				run := first.RunID()
				if i%2 == 1 {
					run = other.RunID()
				}
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType(name), "fixture", "", []byte(`{"revenue":75.0}`), 0, run, events.EventEnvelope{}, time.Unix(1700002600, 0).UTC())
				if err := commitSemanticEventFixture(ctx, f.store, event); err != nil {
					t.Fatal(err)
				}
				if i < 2 {
					operating = append(operating, event.ID())
				} else if i < 4 {
					periods = append(periods, event.ID())
				}
			}
			sort.Strings(operating)
			sort.Strings(periods)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, tc := range []struct {
				name string
				read func(context.Context, any) ([]string, error)
				want []string
			}{
				{"operating", ReadFanInOperatingReportedEventIDsForTest, operating},
				{"period", ReadFanInPeriodReportedEventIDsForTest, periods},
			} {
				if got, err := tc.read(ctx, f.store); err != nil || !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("global %s LIKE scope/order/multiplicity changed: got=%v want=%v err=%v", tc.name, got, tc.want, err)
				}
			}
			evidence, err := ReadFanInDiagnosticEventsForTest(ctx, f.store)
			if err != nil || len(evidence) != 7 {
				t.Fatalf("diagnostic event scope narrowed: %v %v", evidence, err)
			}
			for i := 1; i < 5; i++ {
				if evidence[i-1].EventID >= evidence[i].EventID {
					t.Fatalf("diagnostic event clock ties lost identity ordering: %v", evidence)
				}
			}
			if evidence[5].EventID != first.ID() || evidence[6].EventID != other.ID() {
				t.Fatalf("diagnostic events lost unrelated runs/names or clock order: %v", evidence)
			}
			if deliveries, err := ReadFanInDiagnosticDeliveriesForTest(ctx, f.store); err != nil || len(deliveries) != 0 {
				t.Fatalf("diagnostic reader manufactured deliveries: %v %v", deliveries, err)
			}
			if raw, err := ReadFanInEventRouteSettlementForTest(ctx, f.store, first.ID()); err != nil || raw == "" {
				t.Fatalf("global exact-event settlement missing: %q %v", raw, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 5 || counts.Total.ReadCommits != 5 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("fan-in observations escaped independent original read ownership: %+v", counts)
			}
			if raw, err := ReadFanInEventRouteSettlementForTest(ctx, f.store, uuid.NewString()); !errors.Is(err, sql.ErrNoRows) || raw != "" {
				t.Fatalf("absent physical settlement manufactured evidence: %q %v", raw, err)
			}
		})
	}
}

func TestFanInSelectedPeriodFaultKeepsExactNativeScopeAndOriginalWriterBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
				"schema.yaml":                             "name: fan-in-physical-fault\n",
				"portfolio/schema.yaml":                   "name: portfolio\n",
				"portfolio/selected-period/schema.yaml":   "name: selected-period\nstages:\n  active: {}\n",
				"portfolio/selected-period/entities.yaml": "test_entity:\n  amount: {type: json, initial: 75.0}\n",
				"portfolio/sibling/schema.yaml":           "name: sibling\nstages:\n  active: {}\n",
				"portfolio/sibling/entities.yaml":         "test_entity:\n  amount: {type: json, initial: 75.0}\n",
			}, nil)
			run := correlation.RunIDFromContext(f.ctx)
			req := sqliteFlowActivationRequest(f.bundle, ".", run, "", run)
			req.Instance = flowidentity.Stored(req.ContractBundle, ".", run, run, run, "")
			plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil {
				t.Fatal(err)
			}
			var children []pipeline.FlowInstanceActivationPlan
			var walk func(pipeline.FlowInstanceActivationPlan)
			walk = func(current pipeline.FlowInstanceActivationPlan) {
				if current.Identity.InstancePath == "portfolio/selected-period" || current.Identity.InstancePath == "portfolio/sibling" {
					children = append(children, current)
				}
				for _, child := range current.Children {
					walk(child)
				}
			}
			walk(plan)
			if len(children) != 2 {
				t.Fatalf("native source did not construct both exact fault scopes: %+v", plan)
			}
			read := func(child pipeline.FlowInstanceActivationPlan) ReceiverConstructionStorage {
				got, err := ReadReceiverConstructionStorageForTest(f.ctx, f.store, run, child.Identity.InstancePath, child.Identity.EntityID, child.Instance.EntityType)
				if err != nil || got.FieldRows != 1 || !got.ReadinessPresent {
					t.Fatalf("exact lawful construction missing: %+v %v", got, err)
				}
				return got
			}
			for _, child := range children {
				before := read(child)
				for _, runID := range []string{uuid.NewString(), run} {
					if runID == run && child.Identity.InstancePath == "portfolio/selected-period" {
						continue
					}
					if removed, err := DeleteFanInSelectedPeriodOwnerForTest(f.ctx, f.store, runID, child.Identity.EntityID); err != nil || removed != 0 {
						t.Fatalf("foreign run/path was deleted: rows=%d err=%v", removed, err)
					}
					if got := read(child); !reflect.DeepEqual(got, before) {
						t.Fatalf("zero-row fault changed header, fields, readiness or clocks: before=%+v after=%+v", before, got)
					}
				}
				if child.Identity.InstancePath != "portfolio/selected-period" {
					continue
				}
				probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				if removed, err := DeleteFanInSelectedPeriodOwnerForTest(f.ctx, f.store, run, child.Identity.EntityID); err != nil || removed != 1 {
					restore()
					t.Fatalf("exact existing selected period was not deleted: rows=%d err=%v", removed, err)
				}
				counts := probe.Snapshot()
				restore()
				if counts.Total.WriteCommits != 1 || counts.Total.Revision.Finalizations != 0 || counts.Active != 0 {
					t.Fatalf("fault escaped original unrevisioned coordinator: %+v", counts)
				}
				// The existing header observer's fieldless cut intentionally does
				// not demand the entity row that this exact fault removed.
				after, err := ReadReceiverConstructionStorageForTest(f.ctx, f.store, run, child.Identity.InstancePath, child.Identity.EntityID, "")
				want := before
				want.FieldRows, want.Fields = 0, nil
				if err != nil || !reflect.DeepEqual(after, want) {
					t.Fatalf("fault changed anything outside exact entity absence: before=%+v after=%+v err=%v", before, after, err)
				}
			}
		})
	}
}

func TestFanInSelectedPeriodFaultRespectsOriginalSQLiteWriterAdmission(t *testing.T) {
	f := openSQLiteAuthorActivityReceiptFixture(t)
	owner := f.store.(*SQLiteRuntimeStore)
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 5*time.Second)
	defer cancel()
	probe, restore, err := InstallTransactionProbeForTest(owner, transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	entered, release, holderDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		holderDone <- owner.backend.RunTransaction(ctx, "hold exact fan-in fault admission", func(context.Context, *sql.Tx) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer func() {
		unblock()
		if err := <-holderDone; err != nil {
			t.Errorf("join fan-in writer holder: %v", err)
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	faultCtx, cancelFault := context.WithCancel(ctx)
	faultDone := make(chan error, 1)
	go func() {
		removed, err := DeleteFanInSelectedPeriodOwnerForTest(faultCtx, owner, uuid.NewString(), uuid.NewString())
		if removed != 0 {
			err = errors.Join(err, errors.New("queued fan-in fault removed a row"))
		}
		faultDone <- err
	}()
	joined := false
	defer func() {
		cancelFault()
		unblock()
		if !joined {
			<-faultDone
		}
	}()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for owner.backend.QueuedWritersForTest() != 1 {
		select {
		case err := <-faultDone:
			joined = true
			t.Fatalf("fan-in fault bypassed held original admission: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	if counts := probe.Snapshot(); counts.Total.Begun != 0 || counts.Total.WriteCommits != 0 || counts.Active != 1 {
		t.Fatalf("queued fan-in fault began a shadow transaction: %+v", counts)
	}
	cancelFault()
	if err := <-faultDone; !errors.Is(err, context.Canceled) {
		joined = true
		t.Fatalf("queued fan-in fault did not cancel: %v", err)
	}
	joined = true
}

func TestFanInStoragePortsRefuseForeignCancelledAndClosedOwnersBothStores(t *testing.T) {
	refuse := func(t *testing.T, ctx context.Context, owner any) {
		t.Helper()
		for _, read := range []func(context.Context, any) ([]string, error){ReadFanInPeriodReportedEventIDsForTest, ReadFanInOperatingReportedEventIDsForTest} {
			if out, err := read(ctx, owner); err == nil || out != nil {
				t.Fatalf("failed fan-in ID read yielded evidence: %v %v", out, err)
			}
		}
		if out, err := ReadFanInDiagnosticEventsForTest(ctx, owner); err == nil || out != nil {
			t.Fatalf("failed fan-in event read yielded evidence: %v %v", out, err)
		}
		if out, err := ReadFanInDiagnosticDeliveriesForTest(ctx, owner); err == nil || out != nil {
			t.Fatalf("failed fan-in delivery read yielded evidence: %v %v", out, err)
		}
		if out, err := ReadFanInEventRouteSettlementForTest(ctx, owner, uuid.NewString()); err == nil || out != "" {
			t.Fatalf("failed fan-in settlement read yielded evidence: %q %v", out, err)
		}
		if removed, err := DeleteFanInSelectedPeriodOwnerForTest(ctx, owner, uuid.NewString(), uuid.NewString()); err == nil || removed != 0 {
			t.Fatalf("failed native fault yielded deletion evidence: %d %v", removed, err)
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

func TestFanInDiagnosticDeliveryStorageKeepsGlobalMultiplicityAndNativeOrderBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f, ctx := backend.open(t), testAuthorActivityContext()
			first := runForkSnapshotFixtureEvent(t, ctx, f.store)
			second := runForkSnapshotFixtureEvent(t, ctx, f.store)
			type expected struct {
				id  string
				at  time.Time
				row FanInDiagnosticDeliveryStorage
			}
			var want []expected
			for i, source := range []events.Event{first, second} {
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "fan-in.diagnostic", "fixture", "", []byte(`{"value":1}`), 0, source.RunID(), events.EventEnvelope{}, time.Unix(1700002600+int64(i), 0).UTC())
				routes := []events.DeliveryRoute{testEntitylessNodeDeliveryRoute("fan-in-first"), testEntitylessNodeDeliveryRoute("fan-in-second")}
				if err := commitSemanticEventFixtureWithRoutes(ctx, f.store, event, routes); err != nil {
					t.Fatal(err)
				}
				for _, route := range routes {
					id, err := deliverylifecycle.DeliveryID(event.ID(), route)
					if err != nil {
						t.Fatal(err)
					}
					snapshot, err := f.store.(deliveryFixtureStore).Snapshot(ctx, id)
					if err != nil || snapshot.CreatedAt.IsZero() {
						t.Fatalf("load canonical physical delivery clock: %+v %v", snapshot, err)
					}
					want = append(want, expected{id, snapshot.CreatedAt, FanInDiagnosticDeliveryStorage{EventID: event.ID(), SubscriberID: route.Recipient.ID(), Status: "pending"}})
				}
			}
			sort.Slice(want, func(i, j int) bool {
				if want[i].at.Equal(want[j].at) {
					return want[i].id < want[j].id
				}
				return want[i].at.Before(want[j].at)
			})
			rows, err := ReadFanInDiagnosticDeliveriesForTest(ctx, f.store)
			if err != nil || len(rows) != len(want) {
				t.Fatalf("whole-store delivery diagnostic narrowed run/name or multiplicity: %v %v", rows, err)
			}
			for i, row := range rows {
				if row != want[i].row {
					t.Fatalf("delivery physical fields, NULL defaults or timestamp/identity order changed: got=%+v want=%+v", rows, want)
				}
			}
		})
	}
}
