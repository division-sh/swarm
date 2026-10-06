package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/lib/pq"
)

const raiseSerializationConflictSQL = `DO $$ BEGIN RAISE EXCEPTION 'native serialization regression' USING ERRCODE='40001'; END $$`

func TestPostgresSerializationConflictRequiresCleanNativeSettlement(t *testing.T) {
	for _, retained := range []bool{false, true} {
		for _, scenario := range []string{"clean", "wrapped", "joined", "cancel", "rollback_failure", "already_settled", "read_only", "commit_uncertain", "session_cleanup", "session_fenced"} {
			if !retained && (scenario == "session_cleanup" || scenario == "session_fenced") {
				continue
			}
			t.Run(fmt.Sprintf("retained=%t/%s", retained, scenario), func(t *testing.T) {
				b, p := newExitProbe(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cleanup := errors.New("independent settlement failure")
				var session *SessionAuthority
				if retained {
					conn, err := b.Conn(ctx)
					if err != nil {
						t.Fatal(err)
					}
					session = newSessionAuthority(conn)
					defer session.release()
				}
				opts := &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: scenario == "read_only"}
				if scenario == "rollback_failure" {
					p.rollbackFailure = cleanup
				}
				if scenario == "commit_uncertain" {
					p.commitFailure = &pq.Error{Code: "40001"}
				}
				if scenario == "session_cleanup" {
					session.testEndTxError = func() error { return cleanup }
				}
				calls := 0
				operation := func(sqlCtx context.Context, tx *sql.Tx) error {
					calls++
					if scenario == "commit_uncertain" {
						return nil
					}
					_, conflict := tx.ExecContext(sqlCtx, raiseSerializationConflictSQL)
					var pgErr *pq.Error
					if !errors.As(conflict, &pgErr) || pgErr.Code != "40001" {
						t.Fatalf("not a real native serialization conflict: %v", conflict)
					}
					switch scenario {
					case "wrapped":
						return fmt.Errorf("fence: %w", conflict)
					case "joined":
						return errors.Join(conflict, cleanup)
					case "cancel":
						cancel()
					case "already_settled":
						if err := tx.Rollback(); err != nil {
							t.Fatal(err)
						}
					case "session_fenced":
						session.fenceLocal()
					}
					return conflict
				}
				var ack bool
				var err error
				if retained {
					ack, err = RunAuthorityTransactionOutcomeWithOptions(ctx, session, opts, operation)
				} else {
					ack, err = b.RunTransactionWithOptionsOutcome(ctx, opts, operation)
				}
				wantProof := scenario == "clean" || scenario == "wrapped"
				if ack || err == nil || IsRolledBackSerializationConflict(err) != wantProof || calls != 1 {
					t.Fatalf("settlement ack=%v proof=%v want=%v calls=%d err=%v", ack, IsRolledBackSerializationConflict(err), wantProof, calls, err)
				}
				if wantProof {
					if IsRolledBackSerializationConflict(errors.Join(err, cleanup)) || IsRolledBackSerializationConflict(fmt.Errorf("other owner: %w", err)) {
						t.Fatal("unsealed error tree admitted retry")
					}
				}
				if scenario == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("lost cancellation: %v", err)
				}
				if (scenario == "joined" || scenario == "rollback_failure" || scenario == "session_cleanup") && !errors.Is(err, cleanup) {
					t.Fatalf("lost settlement failure: %v", err)
				}
			})
		}
	}
}

func TestPostgresSerializationConflictClassificationIsExact(t *testing.T) {
	for _, err := range []error{nil, errors.New("40001"), &pq.Error{Code: "40P01"}, &pq.Error{Code: "23505"}, errors.Join(&pq.Error{Code: "40001"}, errors.New("independent"))} {
		if exactSerializationConflict(err) || IsRolledBackSerializationConflict(err) {
			t.Fatalf("unproven error admitted serialization retry: %v", err)
		}
	}
	conflict := &pq.Error{Code: "40001"}
	if !exactSerializationConflict(fmt.Errorf("lock order: %w", conflict)) || IsRolledBackSerializationConflict(conflict) {
		t.Fatal("SQLSTATE identification was confused with rollback proof")
	}
}
