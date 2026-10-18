package startupownership

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/google/uuid"
)

func TestRuntimeLogoutWithdrawalRefusesInvalidTemporalEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var original processbinding.LeaseStamp
			var run func(context.Context, func(context.Context, *sql.Tx) error) error
			sqlite := backend == "sqlite"
			if sqlite {
				session, _ := newSQLiteProofSession(t, 4)
				original = session.authority.LeaseStamp()
				run = func(ctx context.Context, f func(context.Context, *sql.Tx) error) error {
					return session.owner.backend.RunTransaction(ctx, "logout withdrawal negative proof", f)
				}
			} else {
				session, _, _ := newPostgresGenerationMonitor(t, "")
				original = session.authority.LeaseStamp()
				run = session.owner.backend.RunTransaction
			}
			for _, cell := range []struct {
				name string
				edit func(*processbinding.LeaseStamp)
			}{
				{"equal_original", func(*processbinding.LeaseStamp) {}},
				{"equal_different_lease", func(s *processbinding.LeaseStamp) { s.LeaseID = uuid.NewString() }},
				{"current_older", func(s *processbinding.LeaseStamp) { s.Generation++ }},
				{"zero_original", func(s *processbinding.LeaseStamp) { *s = processbinding.LeaseStamp{} }},
				{"malformed_original", func(s *processbinding.LeaseStamp) { s.LeaseID = "invalid" }},
				{"wrong_original_backend", func(s *processbinding.LeaseStamp) {
					if sqlite {
						s.Backend = processbinding.PostgresLeaseBackend
					} else {
						s.Backend = processbinding.SQLiteLeaseBackend
					}
				}},
			} {
				t.Run(cell.name, func(t *testing.T) {
					claimed := original
					cell.edit(&claimed)
					if err := run(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
						withdrawn, err := RuntimeAuthorityWithdrawnTx(ctx, tx, claimed, sqlite)
						if withdrawn {
							t.Error("invalid original gained successor proof")
						}
						if claimed.Validate() == nil && claimed.Backend == original.Backend {
							return err
						}
						if err == nil {
							t.Error("malformed original lost precise refusal")
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				})
			}
			for _, cell := range []string{"higher_same_lease", "equal_different_lease", "zero_current", "wrong_current_backend", "missing_current"} {
				t.Run(cell, func(t *testing.T) {
					// Corruption is deliberately rolled back; it is never native
					// successor admission and must not leave fixture authority changed.
					sentinel := &logoutWithdrawalRollback{}
					err := run(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
						if err := corruptLogoutGenerationForTest(ctx, tx, original, cell); err != nil {
							return err
						}
						withdrawn, err := RuntimeAuthorityWithdrawnTx(ctx, tx, original, sqlite)
						if withdrawn {
							t.Error("invalid current observation gained withdrawal authority")
						}
						if err != nil && (cell == "higher_same_lease" || cell == "equal_different_lease" || cell == "zero_current") {
							t.Error(err)
						}
						if err == nil && (cell == "wrong_current_backend" || cell == "missing_current") {
							t.Error("missing/malformed current lost refusal")
						}
						return sentinel
					})
					if !errors.Is(err, sentinel) {
						t.Fatal("negative observation did not retain rollback", err)
					}
				})
			}
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			if err := run(canceled, func(ctx context.Context, tx *sql.Tx) error {
				withdrawn, err := RuntimeAuthorityWithdrawnTx(ctx, tx, original, sqlite)
				if withdrawn {
					t.Error("cancellation supplied successor evidence")
				}
				return err
			}); err == nil {
				t.Fatal("canceled observation admitted")
			}
		})
	}
}

type logoutWithdrawalRollback struct{}

func (*logoutWithdrawalRollback) Error() string { return "roll back adversarial logout observation" }

func corruptLogoutGenerationForTest(ctx context.Context, tx *sql.Tx, original processbinding.LeaseStamp, cell string) error {
	query := `UPDATE runtime_process_generation SET generation=$1,lease_id=$2 WHERE singleton_id=1`
	args := []any{original.Generation + 1, original.LeaseID}
	switch cell {
	case "equal_different_lease":
		args = []any{original.Generation, uuid.NewString()}
	case "zero_current":
		query, args = `UPDATE runtime_process_generation SET generation=0,lease_id=NULL,owner_id=NULL,boot_id=NULL,runtime_instance_id=NULL,backend=NULL WHERE singleton_id=1`, nil
	case "wrong_current_backend":
		opposite := processbinding.PostgresLeaseBackend
		if original.Backend == opposite {
			opposite = processbinding.SQLiteLeaseBackend
		}
		query, args = `UPDATE runtime_process_generation SET backend=$1 WHERE singleton_id=1`, []any{opposite}
	case "missing_current":
		query, args = `DELETE FROM runtime_process_generation WHERE singleton_id=1`, nil
	}
	_, err := tx.ExecContext(ctx, query, args...)
	return err
}
