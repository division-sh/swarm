package conformance

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// Real construction and normal coordinator settlement; no provider or public
// launcher qualification is inferred from this bounded regression.
func TestEmptyReporterSettlesWithoutConstructedHeaderMutationBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) (notifyAllChildrenStore, *sql.DB)
	}{
		{"sqlite", func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
			selected := storetest.StartSQLiteRuntimeStore(t)
			return selected, storetest.DatabaseForTest(selected)
		}},
		{"postgres", func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			return storetest.AdmitPostgresRuntimeStore(t, db), db
		}},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected, db := backend.open(t)
			source := notifyallchildren.LoadSource(t, notifyallchildren.Options{
				NumericRegistrationRows: true, NumericReporterSink: true, RegistrationUUIDField: true,
			})
			runtime := newNotifyAllChildrenRuntime(t, selected, db, source, time.Now)
			runID := uuid.NewString()
			ctx := correlation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			if err := runtime.manager.Run(managedConformanceExecutionContextForBundle(t, ctx, "empty-reporter-header", runtime.sourceArtifactFact)); err != nil {
				t.Fatal(err)
			}
			publishNotifyAllChildrenRunCreatingEvent(t, ctx, runtime, source, runID, "portfolio.opened", map[string]any{
				"portfolio_id": "empty-reporter", "threshold": 75,
			})
			reader := pipeline.NewWorkflowPersistence(selected)
			owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(".", runID, runID))
			if err != nil {
				t.Fatal(err)
			}
			load := func() pipeline.WorkflowInstance {
				t.Helper()
				instance, found, err := reader.LoadWorkflowInstance(ctx, owner)
				if err != nil || !found || instance.EntityType != "" || len(instance.Fields) != 0 {
					t.Fatalf("constructed fieldless root: found=%v instance=%#v err=%v", found, instance, err)
				}
				return instance
			}
			before := load()
			rows := []map[string]any{
				{"account_id": "numeric-000", "eng_roles": 0, "gem_score": 0.25, "external_id": uuid.NewString()},
				{"account_id": "numeric-001", "eng_roles": 1, "gem_score": 1.25, "external_id": uuid.NewString()},
			}
			publishNotifyAllChildrenEvent(t, ctx, runtime, source, runID, "portfolio.accounts.register.requested", map[string]any{
				"portfolio_id": "empty-reporter", "account_ids": rows,
			})
			if after := load(); !reflect.DeepEqual(before, after) {
				t.Fatalf("explicit empty reporter mutated its constructed header: before=%#v after=%#v", before, after)
			}
			registrations := loadNotifyAllChildrenNumericRegistrations(t, ctx, selected, db, runID)
			if len(registrations) != len(rows) {
				t.Fatalf("registrations=%d, want %d", len(registrations), len(rows))
			}
			operator := selected.(interface {
				LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
			})
			for _, registration := range registrations {
				view, err := operator.LoadOperatorEvent(ctx, registration.ID)
				if err != nil {
					t.Fatal(err)
				}
				assertNumericRegistrationDelivery(t, view)
			}
			summary, err := selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
			if err != nil || summary.Committed != len(rows) || summary.Settled != len(rows) || summary.Unsettled != 0 || summary.Owed != 0 {
				t.Fatalf("empty reporter did not settle its exact work: summary=%#v err=%v", summary, err)
			}
		})
	}
}
