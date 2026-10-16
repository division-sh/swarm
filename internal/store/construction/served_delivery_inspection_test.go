package construction_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type statusInspectionWriter interface {
	storetest.DeliveryPublicationFixtureStore
	sourceartifactfixture.Writer
	ActivateDeliveryAuthority(context.Context, deliverylifecycle.ExecutionAuthority) error
}

func TestServedDeliveryStatusIndependentInspectionRetainsReadOnlyAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			writer, location := openStatusInspectionWriter(t, backend)
			ctx, event := seedStatusInspectionDelivery(t, writer)
			before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, writer)
			if err != nil {
				t.Fatal(err)
			}
			observer, err := storetest.OpenReleaseProcessReadOnlyInspection(backend, location)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := observer.Close(); err != nil {
					t.Error(err)
				}
			})
			requireReleaseInspectionUnaccepted(t, ctx, observer)
			retained := assertStatusInspectionSnapshot(t, ctx, observer, writer, event.ID())
			requireReleaseInspectionUnaccepted(t, ctx, observer)
			assertStatusInspectionRefusals(t, ctx, observer, writer, retained, event.ID())
			after, err := readReleaseInspectionStorageSnapshot(ctx, observer)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("status observation changed durable facts: %v", err)
			}
			// A failed PostgreSQL observation disposes only its independent transport.
			// Check durable equality before that cut, then verify the original writer.
			assertStatusInspectionCanceledTransaction(t, ctx, observer, event.ID())
			if err := observer.Close(); err != nil {
				t.Fatal(err)
			}
			if count, err := storetest.ReadServedDeliveryStatusCount(ctx, observer, event.ID(), "agent", "inspection-agent", "pending"); err == nil || count != 0 {
				t.Fatalf("closed inspection returned provisional evidence: %d %v", count, err)
			}
			unchanged, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, writer)
			if err != nil || !reflect.DeepEqual(before, unchanged) {
				t.Fatalf("inspection close changed original writer facts: %v", err)
			}
		})
	}
}

func seedStatusInspectionDelivery(t *testing.T, writer statusInspectionWriter) (context.Context, events.Event) {
	t.Helper()
	ctx := effects.WithDifferentOwner(context.Background(), effects.OwnerBuildTestInfrastructure)
	ctx = correlation.WithSourceArtifactFact(ctx, sourceartifactfixture.Fact())
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(uuid.NewString(), sourceartifactfixture.BundleHash))
	if err := sourceartifactfixture.Ensure(ctx, writer); err != nil {
		t.Fatal(err)
	}
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(sourceartifactfixture.Fact(), "inspection-status-proof", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.ActivateDeliveryAuthority(ctx, authority); err != nil {
		t.Fatal(err)
	}
	run := uuid.NewString()
	storetest.RequireRunningRun(t, ctx, writer, run, time.Now().UTC())
	event := eventtest.RuntimeControl(uuid.NewString(), "test.work", "inspection-status-proof", "", []byte(`{}`), 0, run, "", events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("inspection-agent"), AgentIdentity: agentidentitytest.RootDeclaredForRun(t, run, "inspection-agent", ".")}
	storetest.CommitNativeDeliveryPublication(t, ctx, writer, storetest.AdmitNativeDeliveryEvent(t, event), []events.DeliveryRoute{route}, authority, nil)
	return ctx, event
}

func assertStatusInspectionSnapshot(t *testing.T, ctx context.Context, observer storetest.ReleaseProcessReadOnlyInspection, writer statusInspectionWriter, eventID string) context.Context {
	t.Helper()
	probe := storetest.CollectTransactions(t, observer, storetest.TransactionProbeOptions{})
	var retained context.Context
	if err := observer.InspectSnapshot(ctx, func(scoped context.Context) error {
		retained = scoped
		if count, err := storetest.ReadServedDeliveryStatusCount(scoped, writer, eventID, "agent", "inspection-agent", "pending"); err == nil || count != 0 {
			t.Fatalf("live foreign snapshot returned provisional evidence: %d %v", count, err)
		}
		count, err := storetest.ReadServedDeliveryStatusCount(scoped, observer, eventID, "agent", "inspection-agent", "pending")
		if err != nil || count != 1 {
			t.Fatalf("independent inspection lost nonzero delivery evidence: %d %v", count, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
		t.Fatalf("status observation escaped its one native snapshot: %+v", counts)
	}
	return retained
}

func openStatusInspectionWriter(t *testing.T, backend string) (statusInspectionWriter, string) {
	t.Helper()
	if backend == "sqlite" {
		writer := storetest.StartSQLiteRuntimeStore(t)
		return writer, writer.Path()
	}
	dsn := testutil.StartPostgresDSN(t)
	writer, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, dsn)
	return writer, dsn
}

func assertStatusInspectionRefusals(t *testing.T, ctx context.Context, observer storetest.ReleaseProcessReadOnlyInspection, writer statusInspectionWriter, retained context.Context, eventID string) {
	t.Helper()
	for _, selected := range []any{observer, writer} {
		if count, err := storetest.ReadServedDeliveryStatusCount(retained, selected, eventID, "agent", "inspection-agent", "pending"); err == nil || count != 0 {
			t.Fatalf("expired/foreign snapshot returned provisional evidence: %d %v", count, err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if count, err := storetest.ReadServedDeliveryStatusCount(canceled, observer, eventID, "agent", "inspection-agent", "pending"); !errors.Is(err, context.Canceled) || count != 0 {
		t.Fatalf("canceled inspection returned provisional evidence: %d %v", count, err)
	}
}

func assertStatusInspectionCanceledTransaction(t *testing.T, ctx context.Context, observer storetest.ReleaseProcessReadOnlyInspection, eventID string) {
	t.Helper()
	commitContext, cancelCommit := context.WithCancel(ctx)
	defer cancelCommit()
	var provisional int
	err := observer.InspectSnapshot(commitContext, func(scoped context.Context) error {
		var err error
		provisional, err = storetest.ReadServedDeliveryStatusCount(scoped, observer, eventID, "agent", "inspection-agent", "pending")
		if err != nil || provisional != 1 {
			t.Fatalf("read did not reach cancellation-after-query cut: %d %v", provisional, err)
		}
		cancelCommit()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("failed inspection transaction accepted provisional count=%d: %v", provisional, err)
	}
}
