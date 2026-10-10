package storetest

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestRunNamedEventIdentityUsesOriginalReadAndExactScopeBothStores(t *testing.T) {
	for _, invalid := range []any{nil, &sql.DB{}, &sql.Tx{}, struct{}{}} {
		if id, err := ReadRunNamedEventIdentityStorage(context.Background(), invalid, uuid.NewString(), "fixture.ready"); err == nil || id != "" {
			t.Fatalf("foreign owner %T supplied event identity: %q/%v", invalid, id, err)
		}
	}
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
			run, sibling := uuid.NewString(), uuid.NewString()
			for _, id := range []string{run, sibling} {
				RequireRun(t, ctx, selected, RunFixture{RunID: id, Origin: ScenarioSetupOrigin()})
			}
			original := uuid.NewString()
			insert := func(run, id, name string) {
				t.Helper()
				CommitSemanticEvent(t, ctx, selected, eventtest.ExistingRunRootIngressWithRoutingSource(id, events.EventType(name), "fixture", "", []byte(`{}`), 0, run,
					events.EventEnvelope{}, eventtest.RootRoutingSource(run), time.Now().UTC()))
			}
			insert(run, original, "fixture.ready")
			insert(sibling, uuid.NewString(), "fixture.ready")
			insert(run, uuid.NewString(), "fixture.other")
			probe := CollectTransactions(t, selected, TransactionProbeOptions{})
			id, err := ReadRunNamedEventIdentityStorage(ctx, selected, run, "fixture.ready")
			if err != nil || id != original {
				t.Fatalf("exact run/name selected sibling or other event: %q/%v", id, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("identity escaped original read coordinator: %+v", counts)
			}
			for _, pair := range [][2]string{{uuid.NewString(), "fixture.ready"}, {run, "fixture.missing"}} {
				if id, err := ReadRunNamedEventIdentityStorage(ctx, selected, pair[0], pair[1]); !errors.Is(err, sql.ErrNoRows) || id != "" {
					t.Fatalf("missing run/name retained identity: %q/%v", id, err)
				}
			}
			for _, pair := range [][2]string{{"", "fixture.ready"}, {"bad", "fixture.ready"}, {uuid.Nil.String(), "fixture.ready"}, {run, ""}, {run, " fixture.ready"}} {
				if id, err := ReadRunNamedEventIdentityStorage(ctx, selected, pair[0], pair[1]); err == nil || id != "" {
					t.Fatalf("invalid run/name retained identity: %q/%v", id, err)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if id, err := ReadRunNamedEventIdentityStorage(cancelled, selected, run, "fixture.ready"); !errors.Is(err, context.Canceled) || id != "" {
				t.Fatalf("cancelled read retained identity: %q/%v", id, err)
			}
			insert(run, uuid.NewString(), "fixture.ready")
			if id, err := ReadRunNamedEventIdentityStorage(ctx, selected, run, "fixture.ready"); err == nil || !strings.Contains(err.Error(), "ambiguous physical event identity") || id != "" {
				t.Fatalf("ambiguous run/name guessed identity: %q/%v", id, err)
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if id, err := ReadRunNamedEventIdentityStorage(ctx, selected, sibling, "fixture.ready"); err == nil || id != "" {
				t.Fatalf("closed owner retained identity: %q/%v", id, err)
			}
		})
	}
}
