package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestSelectedDeploymentResourceWaitObservationsKeepExactRunBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			active, sibling := seedDeliveryRecoveryClaim(t, fixture, ctx), seedDeliveryRecoveryClaim(t, fixture, ctx)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, active, 2)
			seedWorkflowSideEffectStorageRows(t, ctx, fixture, sibling, 1)
			run := active.Claim.RunID()
			for index, eventID := range []string{"ffffffff-ffff-4fff-8fff-fffffffffff1", "11111111-1111-4111-8111-111111111111"} {
				event := eventtest.ExistingRunRootIngress(eventID, "root.ready", "gateway", "", nil, 0, run, events.EventEnvelope{}, time.Now().UTC().Add(time.Duration(index)*time.Second))
				if err := commitSemanticPipelineProcessedEventFixture(ctx, fixture.store, event); err != nil {
					t.Fatal(err)
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if eventID, err := ReadSelectedDeploymentResourceEventForTest(ctx, fixture.store, run); err != nil || eventID != "11111111-1111-4111-8111-111111111111" {
				t.Fatalf("resource event changed original event-id ordering: %q %v", eventID, err)
			}
			before, err := ReadSelectedDeploymentResourceWaitForTest(ctx, fixture.store, run)
			if err != nil || before.Intents != 1 || before.Cursor != 2 || before.Outcomes != 2 || before.Events != 5 || before.Deliveries != 1 || before.ReceiverInstances != 0 || before.Status != "closed" || before.SourceKind != (SelectedDeploymentForkDiagnosticCell{Text: "event_payload_field", Present: true}) || before.FeedID.Present || before.Blocked.Present || before.Claimant.Present || before.Retry.Present {
				t.Fatalf("wait diagnostic lost exact counts or NULL markers: %+v %v", before, err)
			}
			after, err := ReadSelectedDeploymentResourceWakeForTest(ctx, fixture.store, run)
			if err != nil || after.Events != 2 || after.RunStatus != "running" || after.BundleHash == "" {
				t.Fatalf("wake diagnostic lost exact run/event facts: %+v %v", after, err)
			}
			var grants int
			if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generation_grants WHERE bundle_hash=$1 AND state='admitted' AND selected_binding_id IS NULL`, after.BundleHash).Scan(&grants); err != nil || after.Grants != grants {
				t.Fatalf("wake diagnostic changed bundle-scoped admitted grant count: %+v %d %v", after, grants, err)
			}
			if got := probe.Snapshot(); got.Total.ReadCommits != 3 || got.Total.WriteCommits != 0 || got.Active != 0 {
				t.Fatalf("observations did not use their native read owners: %+v", got)
			}
			if eventID, err := ReadSelectedDeploymentResourceEventForTest(ctx, fixture.store, sibling.Claim.RunID()); err != sql.ErrNoRows || eventID != "" {
				t.Fatalf("event observation crossed run: %q %v", eventID, err)
			}
			if got, err := ReadSelectedDeploymentResourceWaitForTest(ctx, fixture.store, sibling.Claim.RunID()); err != nil || got.Outcomes != 1 || got.Cursor != 1 || got.Events != 2 {
				t.Fatalf("wait diagnostic crossed run: %+v %v", got, err)
			}
			if got, err := ReadSelectedDeploymentResourceWakeForTest(ctx, fixture.store, sibling.Claim.RunID()); err != nil || got.Events != 0 {
				t.Fatalf("wake diagnostic crossed run: %+v %v", got, err)
			}
			missing := uuid.NewString()
			if got, err := ReadSelectedDeploymentResourceWaitForTest(ctx, fixture.store, missing); !errors.Is(err, sql.ErrNoRows) || got != (SelectedDeploymentResourceWaitStorage{}) {
				t.Fatalf("missing intent returned partial counts: %+v %v", got, err)
			}
			if got, err := ReadSelectedDeploymentResourceWakeForTest(ctx, fixture.store, missing); !errors.Is(err, sql.ErrNoRows) || got != (SelectedDeploymentResourceWakeStorage{}) {
				t.Fatalf("missing run returned partial wake facts: %+v %v", got, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadSelectedDeploymentResourceEventForTest(cancelled, fixture.store, run); !errors.Is(err, context.Canceled) || got != "" {
				t.Fatalf("cancelled event observation returned facts: %q %v", got, err)
			}
			if got, err := ReadSelectedDeploymentResourceWaitForTest(cancelled, fixture.store, run); !errors.Is(err, context.Canceled) || got != (SelectedDeploymentResourceWaitStorage{}) {
				t.Fatalf("cancelled wait diagnostic returned facts: %+v %v", got, err)
			}
			if got, err := ReadSelectedDeploymentResourceWakeForTest(cancelled, fixture.store, run); !errors.Is(err, context.Canceled) || got != (SelectedDeploymentResourceWakeStorage{}) {
				t.Fatalf("cancelled wake diagnostic returned facts: %+v %v", got, err)
			}
			mutated := before
			mutated.Status = "caller mutation"
			if got, err := ReadSelectedDeploymentResourceWaitForTest(ctx, fixture.store, run); err != nil || !reflect.DeepEqual(got, before) || got == mutated {
				t.Fatalf("detached read changed durable facts: %+v %v", got, err)
			}
			if got := probe.Snapshot(); got.Total.WriteCommits != 0 || got.Active != 0 {
				t.Fatalf("read-only diagnostic wrote or retained its transaction: %+v", got)
			}
		})
	}
}

func TestSelectedDeploymentResourceWaitObservationsRefuseForeignAuthority(t *testing.T) {
	check := func(t *testing.T, ctx context.Context, owner any, run string) {
		t.Helper()
		if got, err := ReadSelectedDeploymentResourceEventForTest(ctx, owner, run); err == nil || got != "" {
			t.Fatalf("invalid event read authority%T: %q %v", owner, got, err)
		}
		if got, err := ReadSelectedDeploymentResourceWaitForTest(ctx, owner, run); err == nil || got != (SelectedDeploymentResourceWaitStorage{}) {
			t.Fatalf("invalid wait read authority%T: %+v %v", owner, got, err)
		}
		if got, err := ReadSelectedDeploymentResourceWakeForTest(ctx, owner, run); err == nil || got != (SelectedDeploymentResourceWakeStorage{}) {
			t.Fatalf("invalid wake read authority%T: %+v %v", owner, got, err)
		}
	}
	for _, owner := range []any{nil, struct{}{}, (*SQLiteRuntimeStore)(nil), (*PostgresStore)(nil), &SQLiteRuntimeStore{}, &PostgresStore{}, &sql.DB{}, &sql.Tx{}} {
		check(t, context.Background(), owner, uuid.NewString())
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, run := range []string{"", "bad-run", uuid.Nil.String(), "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"} {
				check(t, testAuthorActivityContext(), fixture.store, run)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			check(t, testAuthorActivityContext(), fixture.store, uuid.NewString())
		})
	}
}
