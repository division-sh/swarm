package startupownership

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

func TestDisplacedActivityRecoveryRefusesAbsentCutBeforeDomainMutation(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		for _, legacy := range []bool{false, true} {
			dialect := activityjournal.DialectPostgres
			name := "postgres/bound"
			if sqlite {
				dialect = activityjournal.DialectSQLite
				name = "sqlite/bound"
			}
			if legacy {
				name = strings.TrimSuffix(name, "bound") + "legacy"
			}
			t.Run(name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				id := uuid.NewString()
				original := processbinding.LeaseStamp{Generation: 1, LeaseIdentity: processbinding.LeaseIdentity{
					LeaseID: uuid.NewString(), OwnerID: "recovery-fixture", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(), Backend: processbinding.PostgresLeaseBackend,
				}}
				if sqlite {
					original.Backend = processbinding.SQLiteLeaseBackend
				}
				ctx, err := generationauthority.MutationContext(context.Background(), original, false)
				if err != nil {
					t.Fatal(err)
				}
				binding := processbinding.ActivityExecutionBinding{Version: processbinding.ActivityExecutionBindingVersion, GrantID: uuid.NewString(), StateVersion: 3}
				if legacy {
					binding = processbinding.ActivityExecutionBinding{}
				}
				var rawBinding any
				if !binding.IsZero() {
					encoded, err := canonicaljson.Bytes(binding)
					if err != nil {
						t.Fatal(err)
					}
					rawBinding = string(encoded)
				}
				mock.ExpectBegin()
				if !sqlite {
					mock.ExpectExec("SELECT pg_advisory_xact_lock_shared").WillReturnResult(sqlmock.NewResult(0, 1))
				}
				mock.ExpectQuery("SELECT generation, CAST\\(lease_id AS TEXT\\)").WillReturnRows(sqlmock.NewRows([]string{"generation", "lease_id", "owner_id", "boot_id", "runtime_instance_id", "backend"}).
					AddRow(1, original.LeaseID, original.OwnerID, original.BootID, original.RuntimeInstanceID, original.Backend))
				mock.ExpectQuery("SELECT request_event_id FROM activity_attempts WHERE execution_mode = 'live'").WillReturnRows(sqlmock.NewRows([]string{"request_event_id"}).AddRow(id))
				columns := strings.Fields(`request_event_id run_id source_event_id parent_event_id entity_id flow_instance
					node_id handler_event_key activity_id tool attempt status success_event failure_event
					result_event_id result_event_type result_payload failure input_hash loop_generation loop_stage reply_context_id execution_mode
					started_at completed_at updated_at execution_binding live_parents_match`)
				now := time.Unix(100, 0).UTC()
				rows := sqlmock.NewRows(columns).AddRow(id, uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), "review",
					"review/check", "work.received", "check:1", "provider.check", 1, "started", "check.done", "check.failed",
					nil, nil, nil, nil, "exact-input", "{}", "", "reply-original", "live", now, nil, now, rawBinding, true)
				mock.ExpectQuery("SELECT .* FROM activity_attempts WHERE request_event_id").WithArgs(id).WillReturnRows(rows)
				mock.ExpectRollback()
				write := func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimepipeline.ActivityRecoverySummary, error) {
					return reconcileDisplacedHTTPActivities(ctx, attempt, displacedHTTPActivityCut{}, dialect)
				}
				var result mutationprotocol.Result[runtimepipeline.ActivityRecoverySummary]
				if sqlite {
					backend, err := sqlitebackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					result = mutationprotocol.RunSQLite(ctx, backend, "missing activity recovery cut", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
				} else {
					backend, err := postgresbackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					result = mutationprotocol.RunPostgres(ctx, backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
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
