package activityjournal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

// Component DTO only. Native permission still needs the real historical row
// and all process/grant/source/run/selected fences in proveOriginalTx.
func activityInstalledGrantForTest(selected bool) runtimeownership.GrantEvidence {
	evidence := runtimeownership.GrantEvidence{
		GrantID: uuid.NewString(), ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "installed-owner",
		ProcessBootID: uuid.NewString(), BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), RuntimeInstanceID: uuid.NewString(),
		RuntimeGeneration: 1, StateVersion: 3, State: runtimeownership.GrantAdmitted,
		SourceSetRevision: "source-set-revision", ProbeSurfaceIDs: []string{"original-probe"},
	}
	if selected {
		evidence.SourceSetRevision = ""
		fingerprint := "sha256:" + strings.Repeat("b", 64)
		evidence.SelectedFork = &runtimeownership.SelectedForkGrantBinding{
			BindingID: uuid.NewString(), ForkRunID: uuid.NewString(), ExecutionID: uuid.NewString(),
			ExecutionGeneration: 1, FenceGeneration: 1, ExecutionOwner: "selected-owner",
			AdmissionFingerprint: fingerprint, ContainerPlanFingerprint: fingerprint, ActorCensusFingerprint: fingerprint,
			EffectiveConfigFingerprint: fingerprint, DeclarationPlanFingerprint: fingerprint, PreparationFingerprint: fingerprint,
		}
	}
	return evidence
}

func TestActivityInstalledGrantCaptureIsImmutable(t *testing.T) {
	for _, selected := range []bool{false, true} {
		evidence := activityInstalledGrantForTest(selected)
		grant, err := captureActivityExecutionGrant(evidence)
		if err != nil {
			t.Fatal(err)
		}
		evidence.ProbeSurfaceIDs[0] = "changed-probe"
		if selected {
			evidence.SelectedFork.ExecutionOwner = "replacement-owner"
			if grant.evidence.SelectedFork.ExecutionOwner != "selected-owner" {
				t.Fatal("caller changed the captured selected role")
			}
		}
		evidence.StateVersion++
		if grant.reference.StateVersion != 3 || grant.evidence.ProbeSurfaceIDs[0] != "original-probe" {
			t.Fatal("caller changed the original captured grant")
		}
		for _, state := range []runtimeownership.GrantState{runtimeownership.GrantPrepared, runtimeownership.GrantProbeSettled, runtimeownership.GrantRetired} {
			invalid := activityInstalledGrantForTest(selected)
			invalid.State = state
			if role, err := captureActivityExecutionGrant(invalid); err == nil || role != nil {
				t.Fatalf("nonadmitted role %s was captured", state)
			}
		}
	}
}

func TestActivityOriginalGrantRefusalPrecedesNativeWork(t *testing.T) {
	grant, err := captureActivityExecutionGrant(activityInstalledGrantForTest(false))
	if err != nil {
		t.Fatal(err)
	}
	for _, cell := range []string{"legacy", "foreign_grant", "other_version", "invalid_mode", "cancelled", "missing_role"} {
		t.Run(cell, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			record := runtimepipeline.ActivityAttemptRecord{ExecutionMode: "live", ExecutionBinding: grant.reference}
			ctx := context.Background()
			role := grant
			switch cell {
			case "legacy":
				record.ExecutionBinding = processbinding.ActivityExecutionBinding{}
			case "foreign_grant":
				record.ExecutionBinding.GrantID = uuid.NewString()
			case "other_version":
				record.ExecutionBinding.StateVersion++
			case "invalid_mode":
				record.ExecutionMode = ""
			case "cancelled":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			case "missing_role":
				role = nil
			}
			if err := role.proveOriginalTx(ctx, tx, record, false); err == nil {
				t.Fatal("inexact original role reached native work")
			} else if cell == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("caller cancellation was lost: %v", err)
			}
			mock.ExpectRollback()
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestActivityOriginalGrantTakesCanonicalFenceFirst(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectBegin()
		grant, err := captureActivityExecutionGrant(activityInstalledGrantForTest(false))
		if err != nil {
			t.Fatal(err)
		}
		fenceErr := errors.New("original mutation order held")
		if sqlite {
			mock.ExpectExec("INSERT OR IGNORE INTO author_activity_order").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("UPDATE author_activity_order SET last_sequence = last_sequence").WillReturnError(fenceErr)
		} else {
			mock.ExpectQuery("SELECT last_sequence FROM author_activity_order .* FOR UPDATE").WillReturnError(fenceErr)
		}
		mock.ExpectRollback()
		write := func(ctx context.Context, tx *sql.Tx) error {
			return grant.proveOriginalTx(ctx, tx, runtimepipeline.ActivityAttemptRecord{ExecutionMode: "live", ExecutionBinding: grant.reference}, sqlite)
		}
		var committed bool
		if sqlite {
			backend, constructionErr := sqlitebackend.New(db)
			if constructionErr != nil {
				t.Fatal(constructionErr)
			}
			committed, err = backend.RunTransactionOutcome(context.Background(), "activity canonical fence component", write)
		} else {
			backend, constructionErr := postgresbackend.New(db)
			if constructionErr != nil {
				t.Fatal(constructionErr)
			}
			committed, err = backend.RunTransactionOutcome(context.Background(), write)
		}
		if committed || !errors.Is(err, fenceErr) {
			t.Fatalf("native work preceded the canonical fence: committed=%t err=%v", committed, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestActivityOriginalWritersRequireInstalledGrantBeforeNativeEntry(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		for _, operation := range []string{"start", "claim", "complete", "uncertain"} {
			t.Run(fmt.Sprintf("sqlite=%t/%s", sqlite, operation), func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				var owner runtimepipeline.ActivityAttemptJournal
				if sqlite {
					backend, err := sqlitebackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					native, err := NewSQLite(backend, func() error { return nil })
					if err != nil {
						t.Fatal(err)
					}
					owner = native
				} else {
					backend, err := postgresbackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					native, err := NewPostgres(backend, func() error { return nil })
					if err != nil {
						t.Fatal(err)
					}
					owner = native
				}
				if err != nil {
					t.Fatal(err)
				}
				record := runtimepipeline.ActivityAttemptRecord{ExecutionMode: "live", Attempt: 1}
				_, changed, err := invokeActivityWriterForTest(context.Background(), owner, operation, record)
				if changed || err == nil || !strings.Contains(err.Error(), "installed execution role") {
					t.Fatalf("unbound %s reached native work: changed=%t err=%v", operation, changed, err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func invokeActivityWriterForTest(ctx context.Context, owner runtimepipeline.ActivityAttemptJournal, operation string, record runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	switch operation {
	case "start":
		return owner.StartActivityAttempt(ctx, record)
	case "claim":
		return owner.ClaimActivityAttemptForLoopGeneration(ctx, record)
	case "complete":
		return owner.CompleteActivityAttempt(ctx, record)
	default:
		return owner.MarkActivityAttemptUncertain(ctx, record)
	}
}

func TestActivityGrantBinderKeepsOriginalRoleAndCannotRebind(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		for _, selected := range []bool{false, true} {
			t.Run(fmt.Sprintf("sqlite=%t/selected=%t", sqlite, selected), func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				owner := activityGrantBinderForTest(t, db, sqlite)
				signal := &render.ReconcileSignal{}
				composition := owner.(interface {
					BindChannelReconciliation(*render.ReconcileSignal) error
				})
				if err := composition.BindChannelReconciliation(signal); err != nil {
					t.Fatal(err)
				}
				evidence := activityInstalledGrantForTest(selected)
				bound, err := owner.BindActivityGrant(evidence)
				if err != nil {
					t.Fatal(err)
				}
				var grant *activityExecutionGrant
				if sqlite {
					base := owner.(*ActivitySQLiteOwner)
					fixed := bound.(*ActivitySQLiteOwner)
					if base.executionGrant != nil || base.authorActivityCatalog() != fixed.authorActivityCatalog() || fixed.channelChanges != signal {
						t.Fatal("grant binding changed the unbound owner or lost existing composition")
					}
					grant = fixed.executionGrant
					_, err = fixed.BindActivityGrant(evidence)
				} else {
					base := owner.(*ActivityPostgresOwner)
					fixed := bound.(*ActivityPostgresOwner)
					if base.executionGrant != nil || base.authorActivityCatalog() != fixed.authorActivityCatalog() || fixed.channelChanges != signal {
						t.Fatal("grant binding changed the unbound owner or lost existing composition")
					}
					grant = fixed.executionGrant
					_, err = fixed.BindActivityGrant(evidence)
				}
				if err == nil {
					t.Fatal("an in-flight journal role could be rebound")
				}
				evidence.StateVersion++
				evidence.ProbeSurfaceIDs[0] = "replacement-probe"
				original := runtimepipeline.ActivityAttemptRecord{ExecutionMode: "live", Attempt: 1}
				prepared, err := grant.prepareRecord(context.Background(), original, true)
				if err != nil || prepared.ExecutionBinding != grant.reference || !original.ExecutionBinding.IsZero() || grant.reference.StateVersion != 3 || grant.evidence.ProbeSurfaceIDs[0] != "original-probe" {
					t.Fatalf("fresh reference did not preserve the captured role: record=%+v err=%v", prepared, err)
				}
				if _, err := grant.prepareRecord(context.Background(), original, false); err == nil {
					t.Fatal("completion filled a missing original reference")
				}
				foreign := prepared
				foreign.ExecutionBinding.GrantID = uuid.NewString()
				for _, starting := range []bool{false, true} {
					if _, err := grant.prepareRecord(context.Background(), foreign, starting); err == nil {
						t.Fatal("grant binding replaced a foreign original reference")
					}
				}
				if got, err := grant.prepareRecord(context.Background(), prepared, false); err != nil || got.ExecutionBinding != prepared.ExecutionBinding {
					t.Fatalf("exact completion reference changed: record=%+v err=%v", got, err)
				}
				cancelled, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err := grant.prepareRecord(cancelled, prepared, false); !errors.Is(err, context.Canceled) {
					t.Fatalf("caller cancellation was lost before native entry: %v", err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

type activityGrantBinderForTestPort interface {
	BindActivityGrant(runtimeownership.GrantEvidence) (runtimepipeline.ActivityAttemptJournal, error)
}

func activityGrantBinderForTest(t *testing.T, db *sql.DB, sqlite bool) activityGrantBinderForTestPort {
	t.Helper()
	if sqlite {
		backend, err := sqlitebackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		owner, err := NewSQLite(backend, func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		return owner
	}
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewPostgres(backend, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func TestActivityOriginalWritersResolveExactGrantBeforeDomainWork(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		for _, operation := range []string{"start", "claim", "complete", "uncertain"} {
			t.Run(fmt.Sprintf("sqlite=%t/%s", sqlite, operation), func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				evidence := activityInstalledGrantForTest(false)
				owner, err := activityGrantBinderForTest(t, db, sqlite).BindActivityGrant(evidence)
				if err != nil {
					t.Fatal(err)
				}
				mock.ExpectBegin()
				if sqlite {
					mock.ExpectExec("INSERT OR IGNORE INTO author_activity_order").WillReturnResult(sqlmock.NewResult(0, 0))
					mock.ExpectExec("UPDATE author_activity_order SET last_sequence = last_sequence").WillReturnResult(sqlmock.NewResult(0, 1))
				}
				mock.ExpectQuery("SELECT last_sequence FROM author_activity_order").WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(5))
				historyErr := errors.New("exact original activity grant unavailable")
				mock.ExpectQuery("SELECT CAST\\(grant_id AS TEXT\\).*FROM runtime_generation_grants WHERE grant_id=\\$1 AND state_version=\\$2").WithArgs(evidence.GrantID, evidence.StateVersion).WillReturnError(historyErr)
				mock.ExpectRollback()
				record := runtimepipeline.ActivityAttemptRecord{ExecutionMode: "live", Attempt: 1, ExecutionBinding: processbinding.ActivityExecutionBinding{
					Version: processbinding.ActivityExecutionBindingVersion, GrantID: evidence.GrantID, StateVersion: evidence.StateVersion,
				}}
				if operation == "start" || operation == "claim" {
					record.ExecutionBinding = processbinding.ActivityExecutionBinding{}
				}
				_, changed, err := invokeActivityWriterForTest(context.Background(), owner, operation, record)
				if changed || !errors.Is(err, historyErr) {
					t.Fatalf("original %s did not resolve exact attribution before domain work: changed=%t err=%v", operation, changed, err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
