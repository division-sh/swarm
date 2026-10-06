package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestPinnedAuthoredMutationStoragePreservesExactSourceAndPhysicalBytesBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, runID, otherRun := testAuthorActivityContext(), uuid.NewString(), uuid.NewString()
			for _, run := range []string{runID, otherRun} {
				seedAuthorActivityReceiptRun(t, fixture, ctx, run)
			}
			source := fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEntityField, RunID: runID, EntityID: uuid.NewString(), MutationID: uuid.NewString(), Field: "items"}
			otherMutation, nullMutation, privateMutation := uuid.NewString(), uuid.NewString(), uuid.NewString()
			const stored = `{ " a ": [1.0], "a": [2] }`
			wantStored := stored
			if backend.name == "postgres" {
				// JSONB owns its physical serialization; the observer must return
				// those exact bytes, not reconstruct the inserted text.
				wantStored = `{"a": [2], " a ": [1.0]}`
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				for _, row := range []struct {
					id, run, entity, domain string
					value                   any
				}{
					{source.MutationID, runID, source.EntityID, "authored_field", stored},
					{otherMutation, otherRun, source.EntityID, "authored_field", `"foreign-run"`},
					{nullMutation, runID, source.EntityID, "authored_field", nil},
					{privateMutation, runID, source.EntityID, "bookkeeping", `"not-authored"`},
				} {
					if _, err := tx.ExecContext(ctx, `INSERT INTO entity_mutations
						(mutation_id, run_id, entity_id, domain, path, new_value, writer_type, writer_id, created_at)
						VALUES ($1, $2, $3, $4, 'items', $5, 'platform', 'pinned-source-storage-control', $6)`,
						row.id, row.run, row.entity, row.domain, row.value, time.Now().UTC()); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			got, err := ReadPinnedAuthoredMutationStorageForTest(ctx, fixture.store, source)
			if err != nil || string(got) != wantStored {
				t.Fatalf("exact stored bytes changed: %s %v", got, err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("pinned source bypassed original snapshot: %+v", counts)
			}
			for _, wrong := range []fanoutobligation.SourceRef{
				{Kind: source.Kind, RunID: otherRun, EntityID: source.EntityID, MutationID: source.MutationID, Field: source.Field},
				{Kind: source.Kind, RunID: runID, EntityID: uuid.NewString(), MutationID: source.MutationID, Field: source.Field},
				{Kind: source.Kind, RunID: runID, EntityID: source.EntityID, MutationID: otherMutation, Field: source.Field},
				{Kind: source.Kind, RunID: runID, EntityID: source.EntityID, MutationID: source.MutationID, Field: "other"},
				{Kind: source.Kind, RunID: runID, EntityID: source.EntityID, MutationID: privateMutation, Field: source.Field},
				{Kind: source.Kind, RunID: runID, EntityID: source.EntityID, MutationID: uuid.NewString(), Field: source.Field},
			} {
				if got, err := ReadPinnedAuthoredMutationStorageForTest(ctx, fixture.store, wrong); !errors.Is(err, sql.ErrNoRows) || got != nil {
					t.Fatalf("contradictory source returned physical evidence: source=%+v got=%s err=%v", wrong, got, err)
				}
			}
			nullSource := source
			nullSource.MutationID = nullMutation
			if got, err := ReadPinnedAuthoredMutationStorageForTest(ctx, fixture.store, nullSource); err != nil || got != nil {
				t.Fatalf("SQL NULL was reinterpreted as a business value: %s %v", got, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadPinnedAuthoredMutationStorageForTest(cancelled, fixture.store, source); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled source read returned evidence: %s %v", got, err)
			}
			if got, err := ReadPinnedAuthoredMutationStorageForTest(ctx, fixture.store, source); err != nil || string(got) != wantStored || probe.Snapshot().Total.WriteCommits != 0 {
				t.Fatalf("observation changed pinned source: %s %v", got, err)
			}
		})
	}
}

func TestPinnedAuthoredMutationStorageRejectsInvalidAuthorityAndUnavailableEvidenceBothStores(t *testing.T) {
	ctx := context.Background()
	source := fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEntityField, RunID: uuid.NewString(), EntityID: uuid.NewString(), MutationID: uuid.NewString(), Field: "items"}
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadPinnedAuthoredMutationStorageForTest(ctx, owner, source); err == nil || got != nil {
			t.Errorf("invalid owner %T returned pinned bytes: %s %v", owner, got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, wrong := range []fanoutobligation.SourceRef{
				{},
				{Kind: fanoutobligation.SourceEventPayloadField, EventID: uuid.NewString(), Field: "items"},
				{Kind: source.Kind, RunID: "bad", EntityID: source.EntityID, MutationID: source.MutationID, Field: source.Field},
				{Kind: source.Kind, RunID: source.RunID, EntityID: uuid.Nil.String(), MutationID: source.MutationID, Field: source.Field},
				{Kind: source.Kind, RunID: source.RunID, EntityID: source.EntityID, MutationID: "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA", Field: source.Field},
				{Kind: source.Kind, RunID: source.RunID, EntityID: source.EntityID, MutationID: source.MutationID, Field: " "},
			} {
				if got, err := ReadPinnedAuthoredMutationStorageForTest(ctx, fixture.store, wrong); err == nil || got != nil {
					t.Errorf("invalid source returned pinned bytes: %+v %s %v", wrong, got, err)
				}
			}
			rename := func(from, to string) {
				t.Helper()
				if err := runUnrevisionedEventFixtureTransactionForTest(testAuthorActivityContext(), fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, "ALTER TABLE "+from+" RENAME TO "+to)
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			rename("entity_mutations", "unavailable_pinned_source_mutations")
			restored := false
			defer func() {
				if !restored {
					rename("unavailable_pinned_source_mutations", "entity_mutations")
				}
			}()
			if got, err := ReadPinnedAuthoredMutationStorageForTest(ctx, fixture.store, source); err == nil || got != nil {
				t.Fatalf("unavailable source storage returned bytes: %s %v", got, err)
			}
			rename("unavailable_pinned_source_mutations", "entity_mutations")
			restored = true
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadPinnedAuthoredMutationStorageForTest(ctx, fixture.store, source); err == nil || got != nil {
				t.Fatalf("closed source owner returned bytes: %s %v", got, err)
			}
		})
	}
}
