package bus_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestEventBusTerminalRefusalObservationSeesPresentEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected interface {
				runtimebus.EventStore
				runtimebus.RunLifecycleReadPersistence
				storetest.RunFixtureStore
			}
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				selected = storetest.StartPostgresRuntimeStore(t)
			}
			if _, err := newScopedTestEventBus(selected); err != nil {
				t.Fatal(err)
			}
			ctx, runID := testAuthorActivityContext(context.Background()), uuid.NewString()
			if err := storetest.EnsureEphemeralRun(ctx, selected, runID, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			event := exactDuplicateEventBusEvent(runID)
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("observation-agent"), AgentIdentity: runtimebustest.IdentityForRun(t, runID, "observation-agent", "")}
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeDirect)
			probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			status, eventsCount, deliveries, err := readEventBusTerminalRefusalState(ctx, selected, runID, event.ID())
			if err != nil || status != "running" || eventsCount != 1 || deliveries != 1 {
				t.Fatalf("present publication was hidden: %s/%d/%d err=%v", status, eventsCount, deliveries, err)
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("terminal witness wrote or retained selected work: %+v", counts)
			}
			status, eventsCount, deliveries, err = readEventBusTerminalRefusalState(ctx, selected, runID, uuid.NewString())
			if err != nil || status != "running" || eventsCount != 0 || deliveries != 0 {
				t.Fatalf("missing event borrowed evidence: %s/%d/%d err=%v", status, eventsCount, deliveries, err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			status, eventsCount, deliveries, err = readEventBusTerminalRefusalState(canceled, selected, runID, event.ID())
			if !errors.Is(err, context.Canceled) || status != "" || eventsCount != 0 || deliveries != 0 {
				t.Fatalf("failed read retained partial witness: %s/%d/%d err=%v", status, eventsCount, deliveries, err)
			}
		})
	}
}
