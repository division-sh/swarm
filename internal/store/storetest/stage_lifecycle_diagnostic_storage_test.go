package storetest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestStageLifecycleDiagnosticStoragePreservesGlobalTwentyPhysicalPayloadsBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) RunFixtureStore
	}{
		{"sqlite", func(t *testing.T) RunFixtureStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) RunFixtureStore { return StartPostgresRuntimeStore(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			ctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())
			at := time.Date(2026, 10, 12, 1, 0, 0, 0, time.UTC)
			runs := []string{uuid.NewString(), uuid.NewString()}
			for _, run := range runs {
				RequireRunningRun(t, ctx, selected, run, at)
			}
			var physical []string
			for i := 0; i < 24; i++ {
				id := uuid.NewString()
				payload := []byte(fmt.Sprintf(`{"message":"diagnostic-%02d", "log_level":"error", "details":{"b":2,"a":1}}`, i))
				created := at.Add(time.Duration(i) * time.Second)
				if i >= 22 {
					SeedRunDiagnosticDeadLetterHistory(t, ctx, selected, runs[i-22], id, payload, created)
				} else {
					InsertDiagnosticDirectEventRecord(t, ctx, selected, id, payload, created)
				}
				text, err := ReadTemplateEventPayloadText(ctx, selected, id)
				if err != nil {
					t.Fatal(err)
				}
				physical = append(physical, text)
			}
			// A newer non-log must not displace a diagnostic; both run-scoped logs
			// must remain visible alongside the standalone diagnostic records.
			SeedPostForkSourceEvent(t, ctx, selected, runs[0], uuid.NewString(), uuid.NewString(), at.Add(time.Hour))
			var want []string
			for i := len(physical) - 1; i >= len(physical)-20; i-- {
				want = append(want, physical[i])
			}
			got, err := ReadStageLifecycleRuntimeLogPayloads(ctx, selected)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("global bounded physical log text/order changed: got=%q want=%q err=%v", got, want, err)
			}
			got[0] = "caller mutation"
			again, err := ReadStageLifecycleRuntimeLogPayloads(ctx, selected)
			if err != nil || !reflect.DeepEqual(again, want) {
				t.Fatalf("returned payloads alias owner state: got=%q want=%q err=%v", again, want, err)
			}
		})
	}
}

func TestStageLifecycleDiagnosticStorageRefusesForeignCancelledAndClosedOwnersBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) RunFixtureStore
	}{
		{"sqlite", func(t *testing.T) RunFixtureStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) RunFixtureStore { return StartPostgresRuntimeStore(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			if rows, err := ReadStageLifecycleRuntimeLogPayloads(context.Background(), selected); rows != nil || err != nil {
				t.Fatalf("empty physical diagnostic changed: rows=%q err=%v", rows, err)
			}
			var nilPostgres *store.PostgresStore
			var nilSQLite *store.SQLiteRuntimeStore
			for _, owner := range []any{nil, nilPostgres, nilSQLite, &sql.DB{}, &sql.Tx{}, struct{}{}} {
				if rows, err := ReadStageLifecycleRuntimeLogPayloads(context.Background(), owner); rows != nil || err == nil {
					t.Fatalf("foreign %T supplied successful/provisional diagnostic: rows=%q err=%v", owner, rows, err)
				}
			}
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			if rows, err := ReadStageLifecycleRuntimeLogPayloads(cancelled, selected); rows != nil || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled read supplied diagnostic: rows=%q err=%v", rows, err)
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if rows, err := ReadStageLifecycleRuntimeLogPayloads(context.Background(), selected); rows != nil || err == nil {
				t.Fatalf("closed owner supplied successful/provisional diagnostic: rows=%q err=%v", rows, err)
			}
		})
	}
}
