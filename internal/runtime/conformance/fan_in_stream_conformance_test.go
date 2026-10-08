package conformance

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestKeyedPortfolioStreamRoutesAndRetainsIndependentPeriodsOnBothStores(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.FanInStream)
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo,
		canonicalrouting.ExampleRoot(t, canonicalrouting.FanInStream), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	for _, setup := range []struct {
		name string
		open func(*testing.T) (fanInBarrierConformanceStore, *sql.DB)
	}{
		{"sqlite", func(t *testing.T) (fanInBarrierConformanceStore, *sql.DB) {
			s := storetest.StartSQLiteRuntimeStore(t)
			return s, storetest.Database(s)
		}},
		{"postgres", func(t *testing.T) (fanInBarrierConformanceStore, *sql.DB) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			return storetest.AdmitPostgresRuntimeStore(t, db), db
		}},
	} {
		t.Run(setup.name, func(t *testing.T) {
			backend, db := setup.open(t)
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(t.Context(), conformanceSourceArtifactFact(t, source)), runID)
			seedFanInBarrierRun(t, ctx, backend, db, source, runID)
			runtime := newFanInBarrierRuntime(t, backend, source)
			if err := runtime.manager.ActivateFlowInstance(runtimeeffects.WithExecutionMode(ctx, executionmode.Live), runtimepipeline.FlowInstanceActivationRequest{
				ContractBundle: source,
				Instance:       runtimeflowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, runID, ""),
				OccurredAt:     time.Now().UTC(),
			}); err != nil {
				t.Fatalf("construct fan-in stream root and keyless ingress: %v", err)
			}
			load := func(period string) runtimepipeline.WorkflowInstance {
				instances, err := runtime.pipeline.ListWorkflowInstances(ctx, runID)
				var matches []runtimepipeline.WorkflowInstance
				for _, instance := range instances {
					if instance.WorkflowName == "portfolio" && instance.Fields["period_id"] == period {
						matches = append(matches, instance)
					}
				}
				if err != nil || len(matches) != 1 {
					dumpFanInBarrierEvents(t, ctx, backend, db)
					t.Logf("stream runtime diagnostics: %#v", runtime.diagnostics.snapshot())
					t.Fatalf("period %s: matches=%#v err=%v", period, matches, err)
				}
				return matches[0]
			}
			request := func(period string, revenue int) string {
				id := uuid.NewString()
				publishFanInBarrierEvent(t, ctx, runtime.bus, source, id, "ingress", "operating.report.requested",
					map[string]any{"period_id": period, "revenue": revenue})
				return id
			}
			first := request("2026-Q1", 100)
			q1 := load("2026-Q1")
			if q1.Fields["last_revenue"] != int64(100) || q1.Fields["period_id"] != "2026-Q1" {
				t.Fatalf("first period fields: %#v", q1.Fields)
			}
			assertKeyedPortfolioReport(t, q1, first, 100)
			routes, err := backend.ListEventDeliveryRoutes(ctx, first)
			if err != nil || len(routes) != 1 || routes[0].PayloadProjection.Fields()["operating_id"] != first {
				t.Fatalf("create event.id projection: routes=%#v err=%v", routes, err)
			}
			prepared, found, err := backend.LoadPreparedPublishEvent(ctx, first)
			if err != nil || !found {
				t.Fatalf("source event: found=%v err=%v", found, err)
			}
			original := prepared.Event.Event()
			var raw map[string]any
			if err := json.Unmarshal(original.Payload(), &raw); err != nil {
				t.Fatal(err)
			}
			if _, invented := raw["operating_id"]; invented {
				t.Fatalf("connection projection mutated source business payload: %#v", raw)
			}
			second := request("2026-Q2", 300)
			q2 := load("2026-Q2")
			assertKeyedPortfolioReport(t, q2, second, 300)
			if q2.Fields["last_revenue"] != int64(300) || q2.Fields["period_id"] != "2026-Q2" || q1.EntityID == q2.EntityID || q1.InstanceID == q2.InstanceID {
				t.Fatalf("period ownership collapsed: q1=%#v q2=%#v", q1, q2)
			}
			if after := load("2026-Q1"); !reflect.DeepEqual(after, q1) {
				t.Fatal("second period mutated the first period")
			}
			if err := runtime.manager.Shutdown(); err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.workOwner.RetireAndWait(ctx); err != nil {
				t.Fatal(err)
			}
			if err := runtime.grant.Retire(ctx); err != nil {
				t.Fatal(err)
			}
			runtime = newFanInBarrierRuntime(t, backend, source, 2)
			if err := runtime.bus.PublishAcknowledged(ctx, original); err != nil {
				t.Fatalf("reconstructed exact publication: %v", err)
			}
			if after := load("2026-Q1"); !reflect.DeepEqual(after, q1) {
				t.Fatal("restart duplicate changed the original stream period")
			}
			if after := load("2026-Q2"); !reflect.DeepEqual(after, q2) {
				t.Fatal("restart duplicate changed the sibling stream period")
			}
			for _, id := range []string{first, second} {
				view, err := backend.LoadOperatorEvent(ctx, id)
				if err != nil || len(view.Deliveries) != 1 || len(view.DeadLetters) != 0 || view.NoDelivery != nil {
					t.Fatalf("public source projection %s: view=%#v err=%v", id, view, err)
				}
			}
		})
	}
}

func assertKeyedPortfolioReport(t *testing.T, instance runtimepipeline.WorkflowInstance, operatingID string, revenue int64) {
	t.Helper()
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Fields, instance.Bookkeeping, instance.Gates, instance.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	reports, ok := carrier.Fields["reports"].(map[string]any)
	if !ok || len(reports) != 1 {
		t.Fatalf("period reports: %#v", carrier.Fields["reports"])
	}
	row, ok := reports[operatingID].(map[string]any)
	if !ok || row["revenue"] != revenue {
		t.Fatalf("reports[%s]=%#v, want revenue=%d", operatingID, reports[operatingID], revenue)
	}
}
