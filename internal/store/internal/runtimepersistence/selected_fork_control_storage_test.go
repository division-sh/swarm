package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestSelectedForkControlStorageDiscardsPartialEvidenceBothStores(t *testing.T) {
	ctx := context.Background()
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		got, err := ReadSelectedForkControlStorageForTest(ctx, owner, uuid.NewString(), "loaded", "agent")
		if err == nil || got != (SelectedForkControlStorage{}) {
			t.Fatalf("foreign owner returned evidence: %+v %v", got, err)
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"invalid-run", "missing-run", "missing-binding", "empty-bundle", "empty-agent", "cancelled", "closed", "late-read-error"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
				if cut == "closed" {
					if err := selected.(interface{ Close() error }).Close(); err != nil {
						t.Fatal(err)
					}
					got, err := ReadSelectedForkControlStorageForTest(ctx, selected, uuid.NewString(), "loaded", "selected-agent")
					if err == nil || got != (SelectedForkControlStorage{}) {
						t.Fatalf("closed owner returned evidence: %+v %v", got, err)
					}
					return
				}
				f := newSelectedCompletionFixture(t, selected, db, sqlite)
				runID, hash, agent, readCtx := f.forkRun, "loaded", "selected-agent", ctx
				switch cut {
				case "invalid-run":
					runID = "not-canonical"
				case "missing-run":
					runID = uuid.NewString()
				case "missing-binding":
					runID = f.sourceRun
				case "empty-bundle":
					hash = " "
				case "empty-agent":
					agent = " "
				case "cancelled":
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					readCtx = cancelled
				case "late-read-error":
					if _, err := db.ExecContext(ctx, `ALTER TABLE agents RENAME TO unavailable_control_agents`); err != nil {
						t.Fatal(err)
					}
				}
				got, err := ReadSelectedForkControlStorageForTest(readCtx, selected, runID, hash, agent)
				if err == nil || got != (SelectedForkControlStorage{}) || (cut == "cancelled" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("failed observation retained partial evidence: %+v %v", got, err)
				}
			})
		}
	}
}
