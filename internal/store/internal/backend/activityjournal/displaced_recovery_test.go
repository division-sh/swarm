package activityjournal

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	storestartup "github.com/division-sh/swarm/internal/store/internal/startupownership"
)

type displacedRecoveryPort interface {
	ReconcileDisplacedHTTPActivitiesTx(context.Context, *mutationprotocol.Attempt, storestartup.DisplacedHTTPActivityCut) (runtimepipeline.ActivityRecoverySummary, error)
}

var _ displacedRecoveryPort = (*ActivityPostgresOwner)(nil)
var _ displacedRecoveryPort = (*ActivitySQLiteOwner)(nil)

func TestDisplacedActivityRecoveryRefusesAbsentCutBeforeDomainMutation(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		for _, legacy := range []bool{false, true} {
			dialect := DialectPostgres
			if sqlite {
				dialect = DialectSQLite
			}
			name := dialectName(dialect) + "/bound"
			if legacy {
				name = dialectName(dialect) + "/legacy"
			}
			t.Run(name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				owner := activityGrantBinderForTest(t, db, sqlite).(displacedRecoveryPort)
				original := executionIdentityRecord(t)
				if legacy {
					original.ExecutionBinding = processbinding.ActivityExecutionBinding{}
				}
				mock.ExpectBegin()
				if sqlite {
					mock.ExpectExec("INSERT OR IGNORE INTO author_activity_order").WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectExec("UPDATE author_activity_order SET last_sequence = last_sequence").WillReturnResult(sqlmock.NewResult(0, 1))
				}
				mock.ExpectQuery("SELECT last_sequence FROM author_activity_order").WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(5))
				mock.ExpectQuery("SELECT request_event_id FROM activity_attempts WHERE execution_mode = 'live'").WillReturnRows(sqlmock.NewRows([]string{"request_event_id"}).AddRow(original.RequestEventID))
				mock.ExpectQuery("SELECT .* FROM activity_attempts WHERE request_event_id").WithArgs(original.RequestEventID).WillReturnRows(executionIdentityRows(t, original))
				mock.ExpectRollback()
				write := func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimepipeline.ActivityRecoverySummary, error) {
					return owner.ReconcileDisplacedHTTPActivitiesTx(ctx, attempt, storestartup.DisplacedHTTPActivityCut{})
				}
				var result mutationprotocol.Result[runtimepipeline.ActivityRecoverySummary]
				if sqlite {
					result = mutationprotocol.RunSQLite(context.Background(), owner.(*ActivitySQLiteOwner).backend, "missing activity recovery cut", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
				} else {
					result = mutationprotocol.RunPostgres(context.Background(), owner.(*ActivityPostgresOwner).backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
				}
				if result.Err() == nil || result.Acknowledged() {
					t.Fatalf("absent displacement proof reached a mutation: %v", result.Err())
				}
				if !legacy && !strings.Contains(result.Err().Error(), "exact original mutation") {
					t.Fatalf("unexpected refusal before domain mutation: %v", result.Err())
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestDisplacedActivityRecoveryCannotUseOriginalExecutionRole(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		owner, err := activityGrantBinderForTest(t, db, sqlite).BindActivityGrant(activityInstalledGrantForTest(false))
		if err != nil {
			t.Fatal(err)
		}
		if result, err := owner.(displacedRecoveryPort).ReconcileDisplacedHTTPActivitiesTx(context.Background(), nil, storestartup.DisplacedHTTPActivityCut{}); err == nil || len(result.Results) != 0 {
			t.Fatalf("original role became a successor: %+v %v", result, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}

func TestDisplacedActivityRecoveryPreservesCancellationBeforeNativeEntry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, sqlite := range []bool{false, true} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		owner := activityGrantBinderForTest(t, db, sqlite).(displacedRecoveryPort)
		result, err := owner.ReconcileDisplacedHTTPActivitiesTx(ctx, nil, storestartup.DisplacedHTTPActivityCut{})
		if !errors.Is(err, context.Canceled) || len(result.Results) != 0 {
			t.Fatalf("cancelled recovery entered native work: %+v %v", result, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}
