package storetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

// Use the existing native domain fixture family, not a supplied pool or an
// invented inspection context. Every query is admitted by the actual store.
func TestSelectedServedReadRoleAbsenceCancellationAndDomainsBothStores(t *testing.T) {
	for _, backend := range runFixtureProofBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			reader, ok := selected.(interface {
				InspectSnapshot(context.Context, func(context.Context) error) error
			})
			if !ok {
				t.Fatal("native selected fixture has no inspection lifetime")
			}
			sourceartifactfixture.Require(t, context.Background(), selected)
			runID := uuid.NewString()
			RequireRun(t, context.Background(), selected, RunFixture{RunID: runID, Origin: ScenarioSetupOrigin()})
			requireSelectedReadRoleAbsenceAndCancellation(t, reader, selected, runID)
			err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
				requireSelectedReadRoleDomainFacts(t, ctx, selected, runID)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func requireSelectedReadRoleAbsenceAndCancellation(t *testing.T, reader interface {
	InspectSnapshot(context.Context, func(context.Context) error) error
}, selected any, runID string) {
	t.Helper()
	for _, cancelled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		if cancelled {
			cancel()
		}
		for _, query := range []struct {
			name string
			read func(context.Context) error
		}{
			{"entity_accumulator", func(ctx context.Context) error {
				_, err := ReadEntityAccumulatorTextStorage(ctx, selected, runID, uuid.NewString())
				return err
			}},
			{"template_accumulator", func(ctx context.Context) error {
				_, err := ReadTemplateEntityAccumulatorTextStorage(ctx, selected, runID, uuid.NewString(), "missing")
				return err
			}},
			{"instance_template", func(ctx context.Context) error {
				_, err := ReadInstanceTemplateStorage(ctx, selected, runID, "missing")
				return err
			}},
			{"timer_due", func(ctx context.Context) error {
				_, err := ReadWorkflowTimerDueValueStorage(ctx, selected, runID, uuid.NewString())
				return err
			}},
			{"recovery_identity", func(ctx context.Context) error {
				_, err := ReadDeliveryRecoveryIdentityStorage(ctx, selected, uuid.NewString())
				return err
			}},
		} {
			err := reader.InspectSnapshot(ctx, query.read)
			if cancelled && !errors.Is(err, context.Canceled) {
				t.Fatalf("%s lost cancellation: %v", query.name, err)
			}
			if !cancelled && !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("%s absence became success: %v", query.name, err)
			}
		}
		cancel()
	}
}

func requireSelectedReadRoleDomainFacts(t *testing.T, ctx context.Context, selected any, runID string) {
	t.Helper()
	if count, err := CountGlobalRunStorage(ctx, selected); err != nil || count != 1 {
		t.Fatalf("native global run count=%d err=%v", count, err)
	}
	if routes, err := ReadServedDeliveryRouteEvidenceStorage(ctx, selected, runID); err != nil || len(routes) != 0 {
		t.Fatalf("empty run acquired routes: %v %v", routes, err)
	}
	if attempts, err := ReadDeliveryRecoveryAttemptsStorage(ctx, selected, uuid.NewString()); err != nil || len(attempts) != 0 {
		t.Fatalf("absent delivery acquired attempts: %v %v", attempts, err)
	}
	if events, err := CountGlobalBusinessEventsStorage(ctx, selected); err != nil || events != 0 {
		t.Fatalf("source admission invented business events: %d %v", events, err)
	}
	if notices, err := CountSourceNoticeStorage(ctx, selected, uuid.NewString()); err != nil || notices != 0 {
		t.Fatalf("foreign source acquired a notice: %d %v", notices, err)
	}
	if count, err := CountSourceRunDeliveryStorage(ctx, selected, runID, uuid.NewString()); err != nil || count != 0 {
		t.Fatalf("foreign source acquired a delivery: %d %v", count, err)
	}
	if out, err := ReadReporterIntentSnapshotStorage(ctx, selected, runID); err != nil || len(out.Columns) != 14 || len(out.Rows) != 0 || out.ColumnsError != nil || out.ScanError != nil || out.IterationError != nil {
		t.Fatalf("empty intent physical projection changed: %+v %v", out, err)
	}
}

func TestSelectedServedReadRolesRefuseForeignOwners(t *testing.T) {
	for _, selected := range []any{nil, struct{}{}} {
		if _, err := ReadEntityAccumulatorTextStorage(context.Background(), selected, uuid.NewString(), uuid.NewString()); err == nil {
			t.Fatal("foreign owner accepted")
		}
		if _, err := ReadServedDeliveryRouteEvidenceStorage(context.Background(), selected, uuid.NewString()); err == nil {
			t.Fatal("foreign delivery owner accepted")
		}
		if _, err := ReadServedForkRuntimeDiagnosticsStorage(context.Background(), selected); err == nil {
			t.Fatal("foreign diagnostic owner accepted")
		}
	}
}
