package agentpersistence

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

func TestSelectedGrantProofLocksRunBeforeExecutionBothDialects(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		backend := "postgres"
		if sqlite {
			backend = "sqlite"
		}
		for _, mismatch := range []string{"none", "binding", "missing_binding", "artifact"} {
			t.Run(backend+"/"+mismatch, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				fingerprint := "sha256:" + strings.Repeat("1", 64)
				binding := startupownership.SelectedForkGrantBinding{BindingID: uuid.NewString(), ForkRunID: uuid.NewString(), ExecutionID: uuid.NewString(),
					ExecutionGeneration: 1, FenceGeneration: 1, ExecutionOwner: "selected-proof", AdmissionFingerprint: fingerprint,
					ContainerPlanFingerprint: fingerprint, ActorCensusFingerprint: fingerprint, EffectiveConfigFingerprint: fingerprint,
					DeclarationPlanFingerprint: fingerprint, PreparationFingerprint: fingerprint}
				grant := startupownership.GrantEvidence{GrantID: uuid.NewString(), ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "selected-proof",
					ProcessBootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(), RuntimeGeneration: 1, StateVersion: 1,
					BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), State: startupownership.GrantAdmitted, SelectedFork: &binding}
				if err := grant.Validate(); err != nil {
					t.Fatal(err)
				}
				mock.ExpectBegin()
				if sqlite {
					mock.ExpectExec("INSERT OR IGNORE INTO author_activity_order").WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectExec("UPDATE author_activity_order SET last_sequence = last_sequence").WillReturnResult(sqlmock.NewResult(0, 1))
				}
				mock.ExpectQuery("SELECT last_sequence FROM author_activity_order").WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(0))
				hash, id := grant.BundleHash, any(binding.BindingID)
				switch mismatch {
				case "binding":
					id = uuid.NewString()
				case "missing_binding":
					id = nil
				case "artifact":
					hash = "bundle-v2:sha256:" + strings.Repeat("b", 64)
				}
				query := `SELECT run.bundle_hash, binding.binding_id.*FROM runs AS run.*LEFT JOIN run_fork_selected_contract_bindings.*WHERE run.run_id=\$1`
				if !sqlite {
					query += ` FOR UPDATE OF run`
				}
				mock.ExpectQuery(query).WithArgs(binding.ForkRunID).WillReturnRows(sqlmock.NewRows([]string{"bundle_hash", "binding_id"}).AddRow(hash, id))
				if mismatch == "none" {
					// Stop at the joined execution read. Its order after the run
					// lock, not a fabricated successful grant, is this control.
					execution := mock.ExpectQuery("SELECT execution.binding_id, execution.fork_run_id")
					if sqlite {
						execution.WithArgs(binding.ExecutionID, sqlmock.AnyArg())
					} else {
						execution.WithArgs(binding.ExecutionID)
					}
					execution.WillReturnError(sql.ErrNoRows)
				}
				mock.ExpectRollback()
				prove := func(ctx context.Context, tx *sql.Tx) error {
					return ProveSelectedForkGenerationGrantTx(ctx, tx, grant, sqlite)
				}
				if sqlite {
					backend, backendErr := sqlitebackend.New(db)
					if backendErr != nil {
						t.Fatal(backendErr)
					}
					err = backend.RunTransaction(context.Background(), "selected grant run-first proof", prove)
				} else {
					backend, backendErr := postgresbackend.New(db)
					if backendErr != nil {
						t.Fatal(backendErr)
					}
					err = backend.RunTransaction(context.Background(), prove)
				}
				if mismatch == "none" && (err == nil || !strings.Contains(err.Error(), "selected-fork grant execution is absent, expired, terminal or inconsistent")) {
					t.Fatalf("execution read did not follow the run lock: %v", err)
				}
				if mismatch != "none" && (err == nil || !strings.Contains(err.Error(), "run binding changed")) {
					t.Fatalf("mismatched run reached execution proof: %v", err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
