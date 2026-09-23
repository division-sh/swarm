package effectpersistence

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	_ "modernc.org/sqlite"
)

func TestSQLiteEffectWritesRejectStaleSchemaBeforeMutation(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "effects.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	stale := errors.New("selected schema is stale")
	owner := &EffectSQLiteOwner{backend: backend, requireCurrent: func() error { return stale }}
	ctx := context.Background()
	checks := []struct {
		name string
		call func() error
	}{
		{"settle completion", func() error {
			_, err := owner.SettleCompletion(ctx, runtimeeffects.Attempt{}, runtimeeffects.CompletionSettlement{})
			return err
		}},
		{"settle external attempt", func() error { return owner.SettleExternalAttempt(ctx, runtimeeffects.Settlement{}) }},
		{"launch external attempt", func() error { return owner.MarkExternalAttemptLaunched(ctx, runtimeeffects.Attempt{}, time.Now()) }},
		{"recover continuation", func() error {
			_, _, err := owner.RecoverCompletionContinuation(ctx, runtimeeffects.CompletionContinuationRequest{})
			return err
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.call(); !errors.Is(err, stale) {
				t.Fatalf("effect mutation did not reject stale schema before SQL: %v", err)
			}
		})
	}
}

func TestEffectWritesDoNotReassembleMutationProtocol(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	forbidden := []string{
		"runPrivateAuthorActivityMutation",
		"runRuntimeMutation",
		"WithCandidateHandoffOutcome",
		"ReserveCandidateHandoff",
		".RunTransaction(",
		".RunTransactionOutcome(",
		".FinalizePostgres(",
		".FinalizeSQLite(",
		"privateauthoractivity.Begin(",
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(".", entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, spelling := range forbidden {
			if strings.Contains(string(body), spelling) {
				t.Errorf("%s reassembles the selected-store mutation protocol through %q", entry.Name(), spelling)
			}
		}
	}
}
