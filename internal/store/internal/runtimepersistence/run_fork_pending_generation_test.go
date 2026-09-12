package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/google/uuid"
)

// This is a selected-store adapter proof. The card uses its real writer; the
// loop snapshot/child row are explicit fixtures, not a served source journey.
func TestForkPendingGenerationCorrespondenceBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			store := fixture.store.(runForkGateSelectedStore)
			for _, geometry := range []string{"current", "historical_closed", "foreign_first", "foreign_last", "missing_child", "wrong_activation", "wrong_cap", "wrong_run", "wrong_entity", "unknown_source", "duplicate_child", "missing_correspondence", "zero_correspondence", "wrong_destination"} {
				t.Run(geometry, func(t *testing.T) {
					ctx := testAuthorActivityContext()
					now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
					sourceRun, childRun := uuid.NewString(), uuid.NewString()
					requireRunningRunForTest(t, ctx, store, sourceRun, now)
					requireRunningRunForTest(t, ctx, store, childRun, now)
					card, effect := newProposedEffectTestCard(t, sourceRun, now, attemptgeneration.Generation{})
					source, err := loopruntime.New(sourceRun, effect.EntityID, "", "review", "revision", "start", "draft", 4, now)
					if err != nil {
						t.Fatal(err)
					}
					effect.Generation = source.Generation()
					if geometry == "historical_closed" {
						if _, err := source.Repeat("draft", "repeat", now.Add(time.Second)); err != nil {
							t.Fatal(err)
						}
						if err := source.Close("done", "close", now.Add(2*time.Second)); err != nil {
							t.Fatal(err)
						}
					}
					value, err := effect.EffectValue()
					if err != nil {
						t.Fatal(err)
					}
					effect.EffectContentHash, err = canonicaljson.HashValue(value)
					if err != nil {
						t.Fatal(err)
					}
					card.EffectContentHash = effect.EffectContentHash
					card, err = decisioncard.New(card)
					if err != nil {
						t.Fatal(err)
					}
					if err := store.CreateProposedEffectCard(ctx, card, effect); err != nil {
						t.Fatal(err)
					}
					projection, err := projectRunForkEntityOwnership(sourceRun, childRun, effect.EntityID, effect.FlowInstance)
					if err != nil {
						t.Fatal(err)
					}
					c, err := loopruntime.NewForkCorrespondence([]loopruntime.Activation{source}, childRun, projection.Fork.EntityID)
					if err != nil {
						t.Fatal(err)
					}
					actual := c.ProjectedActivations()
					foreign, err := loopruntime.New(sourceRun, effect.EntityID, "foreign", "review", "revision", "start", "draft", 4, now)
					if err != nil {
						t.Fatal(err)
					}
					foreign, err = loopruntime.Fork(foreign, childRun, effect.EntityID)
					if err != nil {
						t.Fatal(err)
					}
					wantFailure := false
					switch geometry {
					case "foreign_first":
						actual = append([]loopruntime.Activation{foreign}, actual...)
					case "foreign_last":
						actual = append(actual, foreign)
					case "missing_child":
						actual = nil
						wantFailure = true
					case "wrong_activation":
						other, err := loopruntime.New(sourceRun, effect.EntityID, "", "review", "revision", "other-start", "draft", 4, now)
						if err != nil {
							t.Fatal(err)
						}
						actual[0], err = loopruntime.Fork(other, childRun, effect.EntityID)
						if err != nil {
							t.Fatal(err)
						}
						wantFailure = true
					case "wrong_cap":
						actual[0].MaxAttempts++
						wantFailure = true
					case "wrong_run", "wrong_entity":
						runID, entityID := childRun, effect.EntityID
						if geometry == "wrong_run" {
							runID = uuid.NewString()
						} else {
							entityID = uuid.NewString()
						}
						actual[0], err = loopruntime.Fork(source, runID, entityID)
						if err != nil {
							t.Fatal(err)
						}
						wantFailure = true
					case "unknown_source":
						c, err = loopruntime.NewForkCorrespondence(nil, childRun, effect.EntityID)
						if err != nil {
							t.Fatal(err)
						}
						wantFailure = true
					case "duplicate_child":
						wantFailure = true
					case "missing_correspondence":
						c = nil
						wantFailure = true
					case "zero_correspondence":
						c = &loopruntime.ForkCorrespondence{}
						wantFailure = true
					case "wrong_destination":
						c, err = loopruntime.NewForkCorrespondence([]loopruntime.Activation{source}, uuid.NewString(), effect.EntityID)
						if err != nil {
							t.Fatal(err)
						}
						wantFailure = true
					}
					buckets := map[string]map[string]any{}
					for _, activation := range actual {
						if err := loopruntime.Store(buckets, activation); err != nil {
							t.Fatal(err)
						}
					}
					if geometry == "duplicate_child" {
						buckets[loopruntime.BucketKey]["duplicate"] = buckets[loopruntime.BucketKey][actual[0].Key()]
					}
					raw, err := json.Marshal(runtimeengine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets())
					if err != nil {
						t.Fatal(err)
					}
					if _, err := fixture.db.ExecContext(ctx, `INSERT INTO entity_state
				(run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, accumulator, entered_state_at, created_at, updated_at)
				VALUES ($1,$2,$3,'default','operating','{}','{}',$4,$5,$5,$5)`, childRun, effect.EntityID, effect.FlowInstance, string(raw), now); err != nil {
						t.Fatal(err)
					}
					before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
					point := runfork.RunForkPoint{EventID: uuid.NewString(), Timestamp: now.Add(time.Minute)}
					apply := func() error {
						switch s := fixture.store.(type) {
						case *PostgresStore:
							return s.runPrivateAuthorActivityMutation(ctx, func(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
								return s.runForkPostgresOwner.MaterializeRunForkProposedEffectCardsTx(ctx, tx, story, sourceRun, childRun, projection, point, c, now.Add(2*time.Minute))
							})
						case *SQLiteRuntimeStore:
							return s.runPrivateAuthorActivityMutation(ctx, "test exact pending generation", func(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
								return s.runForkSQLiteOwner.MaterializeRunForkProposedEffectCardsTx(ctx, tx, story, sourceRun, childRun, projection, point, c, now.Add(2*time.Minute))
							})
						default:
							t.Fatalf("unsupported store %T", fixture.store)
							return nil
						}
					}
					err = apply()
					if wantFailure {
						if err == nil {
							t.Fatal("accepted contradictory pending-effect generation")
						}
						if after := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres"); !reflect.DeepEqual(before, after) {
							t.Fatal("rejected correspondence mutated durable tables")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					items, _, err := store.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: childRun, Limit: 10})
					if err != nil || len(items) != 1 {
						t.Fatalf("child cards: %v %v", items, err)
					}
					got, err := store.LoadProposedEffectContinuation(ctx, items[0].CardID)
					if err != nil {
						t.Fatal(err)
					}
					want, err := loopruntime.ForkGeneration(effect.Generation, childRun, effect.EntityID)
					if err != nil {
						t.Fatal(err)
					}
					if got.Generation != want || got.RunID != childRun || got.SourceRunID != childRun || got.ReplyContextID != "" || got.State != decisioncard.ProposedEffectPending || !got.Input.Equal(effect.Input) || got.RequestEventID == effect.RequestEventID || got.EffectContentHash == effect.EffectContentHash {
						t.Fatalf("pending effect lost exact fresh authority: %#v", got)
					}
					original, err := store.LoadProposedEffectContinuation(ctx, effect.CardID)
					if err != nil || !reflect.DeepEqual(original, effect) {
						t.Fatalf("source effect changed: %v", err)
					}
					beforeRepeat := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
					if err := apply(); err != nil {
						t.Fatalf("repeat pending materialization: %v", err)
					}
					if after := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres"); !reflect.DeepEqual(beforeRepeat, after) {
						t.Fatal("repeated pending materialization changed durable evidence")
					}
				})
			}
		})
	}
}
