package storetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestRecoveryRunFaultUsesExactNativeWriterAndRetainsPayloadAndSiblingBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) RunFixtureStore
	}{
		{"sqlite", func(t *testing.T) RunFixtureStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) RunFixtureStore { return StartPostgresRuntimeStore(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			ctx, _, target, sibling := seedRecoveryRunFaultEvents(t, selected)
			before, err := ReadTemplateEventPayloadText(ctx, selected, target)
			if err != nil {
				t.Fatal(err)
			}
			original, err := ReadPublicationEventIdentityStorage(ctx, selected, sibling)
			if err != nil {
				t.Fatal(err)
			}
			probe := CollectTransactions(t, selected, TransactionProbeOptions{})
			if err := ClearEventRunForRecovery(ctx, selected, target); err != nil {
				t.Fatal(err)
			}
			if receipt := probe.Snapshot(); receipt.Total.WriteCommits != 1 || receipt.Total.ReadCommits != 0 || receipt.Active != 0 {
				t.Fatalf("fault bypassed the original selected writer or changed its atomic cut: %+v", receipt)
			}
			if value, err := ReadPublicationEventIdentityStorage(ctx, selected, target); value != (PublicationEventIdentityStorage{}) || err == nil || errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("NULL run was not exact surviving physical corruption: %+v err=%v", value, err)
			}
			if after, err := ReadTemplateEventPayloadText(ctx, selected, target); after != before || err != nil {
				t.Fatalf("fault changed or deleted the diagnostic payload: %q err=%v", after, err)
			}
			if after, err := ReadPublicationEventIdentityStorage(ctx, selected, sibling); after != original || err != nil {
				t.Fatalf("fault changed unrelated event identity: %+v err=%v", after, err)
			}
			if count, err := CountEventNameStorage(ctx, selected, "platform.runtime_log"); count != 2 || err != nil {
				t.Fatalf("fault deleted diagnostic history or added an event: %d err=%v", count, err)
			}
		})
	}
}

func seedRecoveryRunFaultEvents(t *testing.T, selected RunFixtureStore) (context.Context, string, string, string) {
	t.Helper()
	ctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())
	run, target, sibling := uuid.NewString(), uuid.NewString(), uuid.NewString()
	RequireRun(t, ctx, selected, RunFixture{Origin: ScenarioSetupOrigin(), RunID: run})
	at := time.Date(2026, 10, 12, 4, 0, 0, 0, time.UTC)
	for i, id := range []string{target, sibling} {
		SeedRunDiagnosticDeadLetterHistory(t, ctx, selected, run, id, []byte(`{"message":"recovery fault control","log_level":"error"}`), at.Add(time.Duration(i)*time.Second))
	}
	return ctx, run, target, sibling
}

func TestRecoveryRunFaultRefusesUnavailableIdentityAndOwnersBothStores(t *testing.T) {
	for _, open := range []func(*testing.T) RunFixtureStore{
		func(t *testing.T) RunFixtureStore { return StartSQLiteRuntimeStore(t) },
		func(t *testing.T) RunFixtureStore { return StartPostgresRuntimeStore(t) },
	} {
		selected := open(t)
		ctx, _, target, _ := seedRecoveryRunFaultEvents(t, selected)
		before, err := ReadPublicationEventIdentityStorage(ctx, selected, target)
		if err != nil {
			t.Fatal(err)
		}
		var nilPostgres *store.PostgresStore
		var nilSQLite *store.SQLiteRuntimeStore
		for _, owner := range []any{nil, nilPostgres, nilSQLite, &sql.DB{}, &sql.Tx{}, struct{}{}} {
			if err := ClearEventRunForRecovery(ctx, owner, target); err == nil {
				t.Fatalf("foreign %T installed the fault", owner)
			}
		}
		for _, id := range []string{"", "invalid-event", uuid.NewString()} {
			if err := ClearEventRunForRecovery(ctx, selected, id); err == nil {
				t.Fatalf("invalid/absent event %q reported a successful fault", id)
			}
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if err := ClearEventRunForRecovery(cancelled, selected, target); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled fault did not refuse: %v", err)
		}
		if after, err := ReadPublicationEventIdentityStorage(ctx, selected, target); after != before || err != nil {
			t.Fatalf("refused fault mutated its target: %+v err=%v", after, err)
		}
		if err := selected.(interface{ Close() error }).Close(); err != nil {
			t.Fatal(err)
		}
		if err := ClearEventRunForRecovery(ctx, selected, target); err == nil {
			t.Fatal("closed owner installed the fault")
		}
	}
}
