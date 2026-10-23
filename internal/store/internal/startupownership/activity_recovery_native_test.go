package startupownership

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

func TestDisplacedActivityRecoveryCannotUseOriginalExecutionRole(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		grant := runtimeownership.GrantEvidence{
			GrantID: uuid.NewString(), ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "installed-owner", ProcessBootID: uuid.NewString(),
			BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), RuntimeInstanceID: uuid.NewString(),
			RuntimeGeneration: 1, StateVersion: 3, State: runtimeownership.GrantAdmitted, SourceSetRevision: "source-set-revision",
		}
		var validate func() error
		if sqlite {
			backend, err := sqlitebackend.New(db)
			if err != nil {
				t.Fatal(err)
			}
			base, err := activityjournal.NewSQLite(backend, func() error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			owner, err := base.BindActivityGrant(grant)
			if err != nil {
				t.Fatal(err)
			}
			validate = owner.(*activityjournal.ActivitySQLiteOwner).ValidateDisplacedRecovery
		} else {
			backend, err := postgresbackend.New(db)
			if err != nil {
				t.Fatal(err)
			}
			base, err := activityjournal.NewPostgres(backend, func() error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			owner, err := base.BindActivityGrant(grant)
			if err != nil {
				t.Fatal(err)
			}
			validate = owner.(*activityjournal.ActivityPostgresOwner).ValidateDisplacedRecovery
		}
		if err := validate(); err == nil {
			t.Fatal("original role became a successor")
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
		dialect := activityjournal.DialectPostgres
		if sqlite {
			dialect = activityjournal.DialectSQLite
		}
		result, err := reconcileDisplacedHTTPActivities(ctx, nil, displacedHTTPActivityCut{}, dialect)
		if !errors.Is(err, context.Canceled) || len(result.Results) != 0 {
			t.Fatalf("cancelled recovery entered native work: %+v %v", result, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}
