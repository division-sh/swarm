package runforkpersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func Test2293FanOutSourceUUIDOrderCannotChangeHistoricalState(t *testing.T) {
	const entity = "11111111-1111-4111-8111-111111111111"
	for _, remove := range []bool{false, true} {
		for _, sourceFirst := range []bool{false, true} {
			at := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
			small, middle, large := "00000000-0000-0000-0000-000000000001", "88888888-8888-8888-8888-888888888888", "ffffffff-ffff-ffff-ffff-ffffffffffff"
			if sourceFirst {
				small, large = large, small
			}
			value := json.RawMessage(`["final"]`)
			want := map[string]any{"items": []any{"final"}}
			if remove {
				value, want = json.RawMessage("null"), map[string]any{}
			}
			snapshot := &runForkRevisionSnapshot{EntityMutations: []runForkRevisionEntityMutation{
				{MutationID: small, EntityID: entity, Domain: string(mutationlog.DomainFanOutSource), Path: "items", NewValue: json.RawMessage(`["first source"]`), CreatedAt: at.Add(time.Hour)},
				{MutationID: middle, EntityID: entity, Domain: string(mutationlog.DomainAuthoredField), Path: "items", NewValue: value, CreatedAt: at},
				{MutationID: large, EntityID: entity, Domain: string(mutationlog.DomainFanOutSource), Path: "other", NewValue: json.RawMessage(`{"second source":[]}`), CreatedAt: at.Add(2 * time.Hour)},
			}}
			for i := range snapshot.EntityMutations {
				snapshot.EntityMutations[i].FirstRevision = 1
			}
			snapshot.sort()
			got, err := loadRunForkEntityStates(snapshot)
			if err != nil || len(got) != 1 || !reflect.DeepEqual(got[0].Fields, want) || got[0].EnteredStateAt != nil {
				t.Fatalf("source UUID order changed history: sourceFirst=%v remove=%v state=%+v err=%v", sourceFirst, remove, got, err)
			}
		}
	}
}

func Test2293FanOutSourcePhysicalHistoryAndConversationFoldBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, run, entity := runDriftDatabase(t, backend)
			before, err := inspectRunDriftTest(t, db, run)
			if err != nil || len(before.Rows) != 0 || before.EntitiesChecked != 1 {
				t.Fatalf("baseline drift: %+v err=%v", before, err)
			}
			at := time.Now().UTC().Add(time.Hour)
			for _, row := range []runForkRevisionEntityMutation{
				{MutationID: "00000000-0000-0000-0000-000000000001", EntityID: entity, Domain: string(mutationlog.DomainFanOutSource), Path: "items", NewValue: json.RawMessage(`["first"]`), CreatedAt: at},
				{MutationID: "ffffffff-ffff-ffff-ffff-ffffffffffff", EntityID: entity, Domain: string(mutationlog.DomainFanOutSource), Path: "items", NewValue: json.RawMessage(`["second"]`), CreatedAt: at.Add(time.Second)},
				{MutationID: uuid.NewString(), EntityID: uuid.NewString(), Domain: string(mutationlog.DomainFanOutSource), Path: "items", NewValue: json.RawMessage(`[]`), CreatedAt: at},
			} {
				insertRunDriftMutation(t, db, run, row, 1)
			}
			after, err := inspectRunDriftTest(t, db, run)
			if err != nil || len(after.Rows) != 0 || after.EntitiesChecked != 1 {
				t.Fatalf("source-only evidence changed physical drift: %+v err=%v", after, err)
			}
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			owner := conversationForkStore{dialect: conversationForkSQLite}
			if backend == "postgres" {
				owner.dialect = conversationForkPostgres
			}
			fork := runfork.OperatorConversationForkSession{SourceRunID: run,
				ForkPoint: runfork.ConversationForkPointDescriptor{SelectedAt: at.Add(time.Hour)}}
			got, err := loadConversationForkEntitySnapshot(context.Background(), owner, tx, fork)
			if err != nil || len(got) != 1 || got[0].EntityID != entity || got[0].CurrentState != "queued" || !reflect.DeepEqual(got[0].Fields, map[string]any{"value": "original"}) {
				t.Fatalf("conversation fold applied source evidence or created an entity: %+v err=%v", got, err)
			}
			if _, err := tx.Exec(`UPDATE entity_mutations SET new_value='null' WHERE mutation_id=$1`, "ffffffff-ffff-ffff-ffff-ffffffffffff"); err != nil {
				t.Fatal(err)
			}
			if _, err := loadConversationForkEntitySnapshot(context.Background(), owner, tx, fork); err == nil {
				t.Fatal("conversation fold accepted invalid source-only evidence")
			}
		})
	}
}

func Test2293FanOutSourceAloneCannotConstructHistoricalEntity(t *testing.T) {
	snapshot := &runForkRevisionSnapshot{EntityMutations: []runForkRevisionEntityMutation{{
		EntityID: "11111111-1111-4111-8111-111111111111", Domain: string(mutationlog.DomainFanOutSource), Path: "items", NewValue: json.RawMessage(`[]`),
	}}}
	got, err := loadRunForkEntityStates(snapshot)
	if err != nil || len(got) != 0 {
		t.Fatalf("source-only observation created a historical entity: %+v err=%v", got, err)
	}
	for _, raw := range []string{`null`, `"invalid"`, `false`} {
		snapshot.EntityMutations[0].NewValue = json.RawMessage(raw)
		if _, err := loadRunForkEntityStates(snapshot); err == nil {
			t.Fatalf("invalid source-only history accepted: %s", raw)
		}
	}
}
