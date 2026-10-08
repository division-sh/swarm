package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	authoractivityadapter "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity/readadapter"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestAuthorActivityRollbackReusesSequenceOnBothStores(t *testing.T) {
	for _, fixture := range authorActivityAdapterFixtures(t) {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
			commitAuthorActivityDrafts(t, fixture, authorActivityInboundDraft("first", now, runtimeauthoractivity.BundleScope(fixture.runtimeID, "bundle-a")))

			rollback := errors.New("roll back after successful story finalization")
			err := runAuthorActivityFixture(t, fixture, func(ctx context.Context, story *privateauthoractivity.Mutation) error {
				if err := story.Record(ctx, authorActivityInboundDraft("rolled-back", now.Add(time.Second), runtimeauthoractivity.BundleScope(fixture.runtimeID, "bundle-a"))); err != nil {
					return err
				}
				if err := story.Finalize(ctx); err != nil {
					return err
				}
				return rollback
			})
			if !errors.Is(err, rollback) {
				t.Fatalf("native rollback did not preserve post-finalization failure: %v", err)
			}

			commitAuthorActivityDrafts(t, fixture, authorActivityInboundDraft("second", now.Add(2*time.Second), runtimeauthoractivity.BundleScope(fixture.runtimeID, "bundle-a")))
			page, err := authoractivityadapter.List(context.Background(), fixture.db, fixture.readDialect, runtimeauthoractivity.ListOptions{Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Occurrences) != 2 || page.Occurrences[0].Sequence != 1 || page.Occurrences[1].Sequence != 2 {
				t.Fatalf("occurrences after rollback = %#v, want contiguous sequences 1,2", page.Occurrences)
			}
		})
	}
}

func TestAuthorActivityExactRuntimeBundleScopeFilterParity(t *testing.T) {
	for _, fixture := range authorActivityAdapterFixtures(t) {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			now := time.Date(2026, 8, 2, 13, 0, 0, 0, time.UTC)
			runtimeB := uuid.NewString()
			commitAuthorActivityDrafts(t, fixture,
				authorActivityInboundDraft("runtime-a-bundle-a", now, runtimeauthoractivity.BundleScope(fixture.runtimeID, "bundle-a")),
				authorActivityInboundDraft("runtime-a-bundle-b", now.Add(time.Second), runtimeauthoractivity.BundleScope(fixture.runtimeID, "bundle-b")),
				authorActivityRuntimeDraft("runtime-a", now.Add(2*time.Second), runtimeauthoractivity.RuntimeScope(fixture.runtimeID)),
				authorActivityInboundDraft("runtime-b-bundle-a", now.Add(3*time.Second), runtimeauthoractivity.BundleScope(runtimeB, "bundle-a")),
			)

			assertAuthorActivitySequences(t, fixture, runtimeauthoractivity.ListOptions{
				RuntimeInstanceID:   fixture.runtimeID,
				BundleHashes:        []string{"bundle-a", "bundle-b"},
				IncludeRuntimeScope: true,
				Limit:               10,
			}, []int64{1, 2, 3})
			assertAuthorActivitySequences(t, fixture, runtimeauthoractivity.ListOptions{
				RuntimeInstanceID: fixture.runtimeID,
				BundleHashes:      []string{"bundle-a"},
				Limit:             10,
			}, []int64{1})
			assertAuthorActivitySequences(t, fixture, runtimeauthoractivity.ListOptions{
				RuntimeInstanceID:   runtimeB,
				BundleHashes:        []string{"bundle-a"},
				IncludeRuntimeScope: true,
				Limit:               10,
			}, []int64{4})
		})
	}
}

type authorActivityAdapterFixture struct {
	name        string
	db          *sql.DB
	dialect     authoractivityfixture.Dialect
	readDialect authoractivityadapter.Dialect
	runtimeID   string
}

func authorActivityAdapterFixtures(t *testing.T) []authorActivityAdapterFixture {
	t.Helper()
	sqlite := openAuthorActivityAdapterDB(t)
	_, postgres, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	return []authorActivityAdapterFixture{
		{name: "sqlite", db: sqlite, dialect: authoractivityfixture.DialectSQLite, readDialect: authoractivityadapter.DialectSQLite, runtimeID: uuid.NewString()},
		{name: "postgres", db: postgres, dialect: authoractivityfixture.DialectPostgres, readDialect: authoractivityadapter.DialectPostgres, runtimeID: uuid.NewString()},
	}
}

func commitAuthorActivityDrafts(t *testing.T, fixture authorActivityAdapterFixture, drafts ...runtimeauthoractivity.Draft) {
	t.Helper()
	err := runAuthorActivityFixture(t, fixture, func(ctx context.Context, story *privateauthoractivity.Mutation) error {
		for _, draft := range drafts {
			if err := story.Record(ctx, draft); err != nil {
				return err
			}
		}
		return story.Finalize(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func runAuthorActivityFixture(t *testing.T, fixture authorActivityAdapterFixture, write func(context.Context, *privateauthoractivity.Mutation) error) error {
	t.Helper()
	operation := func(ctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(ctx, tx, privateauthoractivity.Dialect(fixture.dialect))
		if err != nil {
			return err
		}
		return write(ctx, story)
	}
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		backend, err := postgresbackend.New(fixture.db)
		if err != nil {
			t.Fatal(err)
		}
		return backend.RunTransaction(context.Background(), operation)
	}
	backend, err := sqlitebackend.New(fixture.db)
	if err != nil {
		t.Fatal(err)
	}
	return backend.RunTransaction(context.Background(), "author activity fixture", operation)
}

func authorActivityInboundDraft(identity string, at time.Time, scope runtimeauthoractivity.Scope) runtimeauthoractivity.Draft {
	return runtimeauthoractivity.Draft{
		OccurrenceID: uuid.NewString(), Kind: runtimeauthoractivity.KindInboundReceived,
		Version: runtimeauthoractivity.Version, Transition: "received", SourceOwner: "events",
		SourceIdentity: identity, DedupKey: identity, OccurredAt: at, Scope: scope,
		Projection: runtimeauthoractivity.Projection{SubjectType: "entity", SubjectID: identity},
	}
}

func authorActivityRuntimeDraft(identity string, at time.Time, scope runtimeauthoractivity.Scope) runtimeauthoractivity.Draft {
	return runtimeauthoractivity.Draft{
		OccurrenceID: uuid.NewString(), Kind: runtimeauthoractivity.KindPlatformSignal,
		Version: runtimeauthoractivity.Version, Transition: "runtime_reset", SourceOwner: "events",
		SourceIdentity: identity, DedupKey: identity, OccurredAt: at, Scope: scope,
		Projection: runtimeauthoractivity.Projection{SubjectType: "platform", SubjectID: identity},
	}
}

func assertAuthorActivitySequences(t *testing.T, fixture authorActivityAdapterFixture, options runtimeauthoractivity.ListOptions, want []int64) {
	t.Helper()
	page, err := authoractivityadapter.List(context.Background(), fixture.db, fixture.readDialect, options)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]int64, 0, len(page.Occurrences))
	for _, occurrence := range page.Occurrences {
		got = append(got, occurrence.Sequence)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scope sequences = %v, want %v", got, want)
	}
	head, err := authoractivityadapter.Head(context.Background(), fixture.db)
	if err != nil || head != 4 {
		t.Fatalf("head = %d, %v, want 4", head, err)
	}
}
