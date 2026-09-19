package conformance

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestFanOutSharedServingAcrossRealBundleGrantsOnBothBackends(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected notifyAllChildrenStore
			var db *sql.DB
			if backend == "postgres" {
				_, database, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				db = database
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
			} else {
				sqlite := storetest.StartSQLiteRuntimeStore(t)
				selected, db = sqlite, storetest.DatabaseForTest(sqlite)
			}
			sources := []semanticview.Source{
				notifyallchildren.LoadSource(t, notifyallchildren.Options{NumericRegistrationRows: true, NumericReporterSink: true}),
				notifyallchildren.LoadSource(t, notifyallchildren.Options{NumericRegistrationRows: true, NumericReporterSink: true, RegistrationUUIDField: true}),
			}
			for _, source := range sources {
				bundle, ok := semanticview.Bundle(source)
				if !ok || bundle == nil {
					t.Fatal("shared serving requires actual bundle sources")
				}
				storetest.RequireBundleDataCatalog(t, testAuthorActivityContextForBundle(context.Background(), conformanceSourceArtifactFact(t, source)), selected, bundle)
			}
			topology := newNotifyAllChildrenProcessTopology(t, testAuthorActivityContext(context.Background()), selected, sources...)
			runtimes := make([]notifyAllChildrenRuntime, len(sources))
			for i, source := range sources {
				runtimes[i] = newNotifyAllChildrenRuntime(t, selected, db, source, time.Now, notifyAllChildrenRuntimeOptions{processTopology: topology})
			}
			plan, exists, err := topology.capability.CurrentSourceSet(context.Background())
			if err != nil || !exists || len(plan.Sources) != 2 || plan.Sources[0].BundleHash == plan.Sources[1].BundleHash {
				t.Fatalf("complete selected process source set: plan=%+v exists=%t err=%v", plan, exists, err)
			}
			runIDs := []string{uuid.NewString(), uuid.NewString()}
			for i, runtime := range runtimes {
				ctx := correlation.WithRunID(testAuthorActivityContextForBundle(context.Background(), runtime.sourceArtifactFact), runIDs[i])
				if err := runtime.manager.Run(managedConformanceExecutionContextForBundle(t, ctx, "shared-fan-out", runtime.sourceArtifactFact)); err != nil {
					t.Fatalf("run bundle %d manager: %v", i, err)
				}
				publishNotifyAllChildrenRunCreatingEvent(t, ctx, runtime, sources[i], runIDs[i], "portfolio.opened", map[string]any{"portfolio_id": "shared", "threshold": 75})
				row := map[string]any{"account_id": "shared-account", "eng_roles": i + 1, "gem_score": 8.25}
				if i == 1 {
					row["external_id"] = uuid.NewString()
				}
				publishNotifyAllChildrenEventAsync(t, ctx, runtime, sources[i], runIDs[i], "portfolio.accounts.register.requested", map[string]any{
					"portfolio_id": "shared", "account_ids": []map[string]any{row},
				})
			}
			for i, runtime := range runtimes {
				waitNotifyAllChildrenRuntimeWithin(t, runtime, runIDs[i], 10*time.Second)
				ctx := correlation.WithRunID(testAuthorActivityContextForBundle(context.Background(), runtime.sourceArtifactFact), runIDs[i])
				summary, err := selected.FanOutRunSummary(ctx, runIDs[i], time.Now().UTC())
				if err != nil || summary.Intents != 1 || summary.Cursor != 1 || summary.Committed != 1 || summary.SemanticRejected != 0 || summary.Owed != 0 {
					t.Fatalf("bundle %d shared serving summary=%+v err=%v", i, summary, err)
				}
				registrations := loadNotifyAllChildrenNumericRegistrations(t, ctx, selected, db, runIDs[i])
				if len(registrations) != 1 || registrations["shared-account"].EngRoles != i+1 {
					t.Fatalf("bundle %d exact production publication=%+v", i, registrations)
				}
			}
		})
	}
}
