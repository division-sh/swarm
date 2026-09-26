package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func TestRunForkExactFactsEntityIDSpellingsBothStores(t *testing.T) {
	source := exactFactEntitySource(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, spelling := range []string{"canonical", "upper", "compact"} {
				t.Run(spelling, func(t *testing.T) {
					selected, db, ctx, runID := openStateOnlyAcquisitionStoreWithSource(t, backend, source)
					owner := selected.(interface {
						CreateEntity(context.Context, tools.EntityCreateRecord) (tools.EntityCreateResult, error)
					})
					canonical := uuid.NewString()
					input := canonical
					if spelling == "upper" {
						input = strings.ToUpper(canonical)
					} else if spelling == "compact" {
						input = strings.ReplaceAll(canonical, "-", "")
					}
					created, err := owner.CreateEntity(ctx, tools.EntityCreateRecord{Source: source, RunID: runID, EntityID: input, FlowInstance: "exact-fact/receiver", EntityType: "review_item", CurrentState: "active", FieldsJSON: json.RawMessage(`{"account_id":"preserved"}`), CreatedAt: time.Now().UTC(), Writer: tools.EntityMutationWriter{Type: "agent", ID: "generated-writer-proof", HandlerStep: "create_entity"}})
					if err != nil {
						t.Fatalf("actual CreateEntity(%s, %q): %v", spelling, input, err)
					}
					var actualRun, actualEntity string
					if err := db.QueryRowContext(ctx, `SELECT CAST(run_id AS TEXT),CAST(entity_id AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, input).Scan(&actualRun, &actualEntity); err != nil {
						t.Fatal(err)
					}
					wantEntity := input
					if backend == "postgres" {
						wantEntity = canonical
					}
					if !created.Acknowledged || created.EntityID != wantEntity {
						t.Fatalf("created coordinate = %+v, want acknowledged %q", created, wantEntity)
					}
					if actualRun != runID || actualEntity != wantEntity {
						t.Fatalf("stored coordinate=(%s,%s), want=(%s,%s)", actualRun, actualEntity, runID, wantEntity)
					}
					s := exactFactStore{db: db, postgres: backend == "postgres"}
					if len(assertActualMutationLedger(t, ctx, s, actualRun, actualEntity)) == 0 {
						t.Fatal("actual entity writer minted no mutation IDs")
					}
					exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
						matched := 0
						for _, row := range exactLedger(t, ctx, tx, actualRun) {
							if row.Family != string(runforkrevision.FamilyEntityMetadata) {
								continue
							}
							body, ok := row.Body.(map[string]any)
							if !ok || !row.Present || row.Key != actualEntity || body["entity_id"] != actualEntity {
								t.Fatalf("stored metadata coordinate disagrees with exact ledger: %#v", row)
							}
							matched++
						}
						if matched != 1 {
							t.Fatalf("metadata facts=%d want=1", matched)
						}
					})
				})
			}
			for _, producer := range []string{"scenario_setup", "engine_route_state"} {
				t.Run(producer, func(t *testing.T) {
					for _, spelling := range []string{"canonical", "upper", "compact"} {
						t.Run(spelling, func(t *testing.T) {
							proveExactEntityProducerSpelling(t, backend, producer, spelling)
						})
					}
				})
			}
		})
	}
}

func proveExactEntityProducerSpelling(t *testing.T, backend, producer, spelling string) {
	t.Helper()
	selected, db, ctx, runID := openStateOnlyAcquisitionStore(t, backend)
	owner := selected.(interface {
		pipeline.WorkflowEngineMutationOwner
		SetupScenarioEntities(context.Context, pipeline.ScenarioSetupRequest) (pipeline.ScenarioSetupResult, error)
	})
	canonical := uuid.NewString()
	input := canonical
	if spelling == "upper" {
		input = strings.ToUpper(canonical)
	} else if spelling == "compact" {
		input = strings.ReplaceAll(canonical, "-", "")
	}
	wantID := input
	if backend == "postgres" {
		wantID = canonical
	}
	s := exactFactStore{db: db, postgres: backend == "postgres"}
	flow := "exact-" + producer + "-" + uuid.NewString()
	instance := flow + "/receiver"
	at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	check := func(wantMetadata int) {
		t.Helper()
		var actualRun, actualID string
		if err := db.QueryRowContext(ctx, `SELECT CAST(run_id AS TEXT),CAST(entity_id AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, input).Scan(&actualRun, &actualID); err != nil {
			t.Fatal(err)
		}
		if actualRun != runID || actualID != wantID {
			t.Fatalf("stored coordinates=(%s,%s) want=(%s,%s)", actualRun, actualID, runID, wantID)
		}
		if len(assertActualMutationLedger(t, ctx, s, actualRun, actualID)) == 0 {
			t.Fatal("actual producer minted no mutation IDs")
		}
		exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
			matched := 0
			for _, row := range exactLedger(t, ctx, tx, actualRun) {
				if row.Family != string(runforkrevision.FamilyEntityMetadata) {
					continue
				}
				body, ok := row.Body.(map[string]any)
				if !ok || !row.Present || row.Key != actualID || body["entity_id"] != actualID {
					t.Fatalf("producer metadata differs from stored coordinates: %#v", row)
				}
				matched++
			}
			if matched != wantMetadata {
				t.Fatalf("metadata facts=%d want=%d", matched, wantMetadata)
			}
		})
	}
	if producer == "scenario_setup" {
		req := pipeline.ScenarioSetupRequest{RunID: runID, CreatedAt: at, Entities: []pipeline.ScenarioSetupEntityRequest{{Alias: "receiver", EntityID: input, FlowInstance: instance, EntityType: "review_item", CurrentState: "active", Fields: map[string]any{"account_id": "preserved"}}}}
		if _, err := owner.SetupScenarioEntities(ctx, req); err != nil {
			t.Fatalf("actual scenario insertion (%s): %v", spelling, err)
		}
		check(1)
		var before []exactLedgerRow
		exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) { before = exactLedger(t, ctx, tx, runID) })
		if _, err := owner.SetupScenarioEntities(ctx, req); err != nil {
			t.Fatalf("actual scenario duplicate (%s): %v", spelling, err)
		}
		exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
			if !reflect.DeepEqual(before, exactLedger(t, ctx, tx, runID)) {
				t.Fatal("scenario duplicate rewrote history")
			}
		})
		check(1)
		assertWorkflowTargetTransitionRows(t, backend, db, runID, input, instance, "", "active", 1, 0)
		return
	}
	record := stateOnlyWorkflowEngineMutationRecord(t, runID, flow, instance, input, "", 0, at)
	record.Transition = pipeline.WorkflowEngineStateTransitionCreateStateAndCompanion
	if _, err := owner.CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: record}); err != nil {
		t.Fatalf("actual engine create (%s): %v", spelling, err)
	}
	check(1)
	assertWorkflowTargetTransitionRows(t, backend, db, runID, input, instance, flow, "done", 1, 1)
	record.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
	record.ExpectedState, record.ExpectedRevision = "done", 1
	record.CurrentState, record.Name = "reviewed", "Updated exact metadata"
	record.UpdatedAt = record.UpdatedAt.Add(time.Second)
	if _, err := owner.CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: record}); err != nil {
		t.Fatalf("actual engine update (%s): %v", spelling, err)
	}
	check(2)
	assertWorkflowTargetTransitionRows(t, backend, db, runID, input, instance, flow, "reviewed", 2, 1)
}

// Only production operations contribute generated receipt/mutation IDs. The
// reused receipt fixture's raw trigger seed is captured before the tested call.
func TestRunForkExactFactsGeneratedWriterIDsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			t.Run("settlement_receipt_ids", func(t *testing.T) {
				f := newB10GroupFaultFixture(t, backend)
				s := exactFactStore{db: f.db, postgres: backend == "postgres"}
				exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
					for _, member := range f.members {
						var count int
						if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1`, member.Claim.EventID()).Scan(&count); err != nil || count != 0 {
							t.Fatalf("fixture already minted tested receipt: count=%d err=%v", count, err)
						}
					}
					// seedFanOutOwnerFixtureWithArtifact inserts these two trigger
					// facts without a revision boundary; member facts are production-owned.
					effects := exactEffects(t, f.seed.runID,
						exactFactRef(t, runforkrevision.FamilyEvents, f.seed.eventID),
						exactFactRef(t, runforkrevision.FamilyEventDeliveries, f.seed.deliveryID))
					if _, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, effects); err != nil {
						t.Fatal(err)
					}
					if err := validateRunForkRevisionMatrix(ctx, tx, s.postgres, f.seed.runID); err != nil {
						t.Fatalf("pre-settlement fixture history: %v", err)
					}
				})
				var before []exactLedgerRow
				exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) { before = exactLedger(t, ctx, tx, f.seed.runID) })
				out, err := f.group.Settle(f.ctx, f.members)
				requireB10Acknowledged(t, out, err, nil)
				actual := make(map[string]string)
				for _, member := range f.members {
					var receiptID, outcome string
					if err := f.db.QueryRowContext(f.ctx, `SELECT CAST(receipt_id AS TEXT),outcome FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, member.Claim.EventID()).Scan(&receiptID, &outcome); err != nil {
						t.Fatal(err)
					}
					if _, err := uuid.Parse(receiptID); err != nil || receiptID == member.Claim.EventID() || outcome != "success" {
						t.Fatalf("actual minted receipt=%s event=%s outcome=%s err=%v", receiptID, member.Claim.EventID(), outcome, err)
					}
					if _, duplicate := actual[receiptID]; duplicate {
						t.Fatal("settlement reused a generated receipt ID")
					}
					actual[receiptID] = member.Claim.EventID()
				}
				exactRollbackTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
					after := exactLedger(t, ctx, tx, f.seed.runID)
					if len(after) <= len(before) || !reflect.DeepEqual(after[:len(before)], before) {
						t.Fatal("settlement rewrote prior history")
					}
					matched := 0
					for _, row := range after[len(before):] {
						if row.Family != string(runforkrevision.FamilyEventReceipts) {
							continue
						}
						body, ok := row.Body.(map[string]any)
						event, requested := actual[row.Key]
						if !ok || !requested || !row.Present || body["receipt_id"] != row.Key || body["event_id"] != event || body["outcome"] != "success" {
							t.Fatalf("receipt writer lost generated coordinate or rewrote sibling: %#v", row)
						}
						matched++
					}
					if matched != len(actual) {
						t.Fatalf("minted receipt ledger matches=%d want=%d", matched, len(actual))
					}
					if err := validateRunForkRevisionMatrix(ctx, tx, s.postgres, f.seed.runID); err != nil {
						t.Fatalf("settlement complete history: %v", err)
					}
				})
			})
			t.Run("entity_and_workflow_mutation_ids", func(t *testing.T) {
				source := exactFactEntitySource(t)
				selected, db, ctx, runID := openStateOnlyAcquisitionStoreWithSource(t, backend, source)
				owner := selected.(interface {
					pipeline.WorkflowEngineMutationOwner
					CreateEntity(context.Context, tools.EntityCreateRecord) (tools.EntityCreateResult, error)
				})
				s := exactFactStore{db: db, postgres: backend == "postgres"}
				entityID, flowID := uuid.NewString(), "exact-fact"
				instance := flowID + "/" + uuid.NewString()
				at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				if _, err := owner.CreateEntity(ctx, tools.EntityCreateRecord{Source: source, RunID: runID, EntityID: entityID, FlowInstance: instance, EntityType: "review_item", CurrentState: "active", FieldsJSON: json.RawMessage(`{"account_id":"preserved"}`), CreatedAt: at, Writer: tools.EntityMutationWriter{Type: "agent", ID: "generated-writer-proof", HandlerStep: "create_entity"}}); err != nil {
					t.Fatal(err)
				}
				initial := assertActualMutationLedger(t, ctx, s, runID, entityID)
				if len(initial) == 0 {
					t.Fatal("entity owner did not generate mutation IDs")
				}
				record := stateOnlyWorkflowEngineMutationRecord(t, runID, flowID, instance, entityID, "active", 1, at)
				if _, err := owner.CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: record}); err != nil {
					t.Fatal(err)
				}
				all := assertActualMutationLedger(t, ctx, s, runID, entityID)
				if len(all) != len(initial)+2 {
					t.Fatalf("workflow state/handled mutations=%d, want two generated IDs beyond %d", len(all), len(initial))
				}
				for id := range initial {
					if !all[id] {
						t.Fatalf("workflow write lost earlier mutation %s", id)
					}
				}
				assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, instance, flowID, "done", 2, 1)
			})
		})
	}
}

func exactFactEntitySource(t *testing.T) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: exact-fact-source\ninitial_state: active\nstates: [active, done]\nterminal_states: [done]\n")
	writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, "exact-fact/schema.yaml"), "name: exact-fact\nmode: template\ninitial_state: active\nstates: [active, done]\nterminal_states: [done]\n")
	writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, "exact-fact/entities.yaml"), "review_item:\n  account_id: {type: text, initial: preserved}\n  handled: {type: boolean, initial: false}\n")
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(pipeline.WorkflowRepoRoot(), root, contracts.DefaultPlatformSpecFile(pipeline.WorkflowRepoRoot()))
	if err != nil {
		t.Fatalf("load exact-fact entity source: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func assertActualMutationLedger(t *testing.T, ctx context.Context, s exactFactStore, runID, entityID string) map[string]bool {
	t.Helper()
	actual := make(map[string]bool)
	exactRollbackTransaction(t, s, func(_ context.Context, tx *sql.Tx) {
		rows, err := tx.QueryContext(ctx, `SELECT CAST(mutation_id AS TEXT),domain,path,new_value FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 ORDER BY mutation_id`, runID, entityID)
		if err != nil {
			t.Fatal(err)
		}
		type mutation struct {
			id, domain, path string
			value            any
		}
		var mutations []mutation
		for rows.Next() {
			var m mutation
			var raw []byte
			if err := rows.Scan(&m.id, &m.domain, &m.path, &raw); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &m.value); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			if _, err := uuid.Parse(m.id); err != nil || m.id == entityID || actual[m.id] {
				rows.Close()
				t.Fatalf("mutation writer did not mint distinct actual ID: %s err=%v", m.id, err)
			}
			actual[m.id] = true
			mutations = append(mutations, m)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
		ledger := exactLedger(t, ctx, tx, runID)
		for _, m := range mutations {
			matches := 0
			for _, row := range ledger {
				if row.Family != string(runforkrevision.FamilyEntityMutations) || row.Key != m.id {
					continue
				}
				body, ok := row.Body.(map[string]any)
				if !ok || !row.Present || body["mutation_id"] != m.id || body["entity_id"] != entityID || body["domain"] != m.domain || body["path"] != m.path || !reflect.DeepEqual(body["new_value"], m.value) {
					t.Fatalf("actual generated mutation disagrees with ledger: %#v SQL=%#v", row, m)
				}
				matches++
			}
			if matches != 1 {
				t.Fatalf("generated mutation %s ledger rows=%d want=1", m.id, matches)
			}
		}
		for _, row := range ledger {
			if row.Family == string(runforkrevision.FamilyEntityMutations) && !actual[row.Key] {
				t.Fatalf("phantom generated mutation key=%s", row.Key)
			}
		}
		if err := validateRunForkRevisionMatrix(ctx, tx, s.postgres, runID); err != nil {
			t.Fatalf("actual mutation writer complete history: %v", err)
		}
	})
	return actual
}
