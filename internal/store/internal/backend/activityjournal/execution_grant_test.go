package activityjournal

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
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
