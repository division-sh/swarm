package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

func TestIssue2564EntityOperationsReapplyAfterRealCASBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, scenario := range []string{"append_distinct", "append_equal", "map_distinct_key", "map_same_key", "update_distinct_index", "update_same_index", "index_fresh_replacement", "index_out_of_range_after_replacement", "m20_named_record_leaf"} {
			t.Run(backend+"/"+scenario, func(t *testing.T) {
				f := newIssue2564OperationFixture(t, backend)
				initial := issue2564LoadOperationInstance(t, f)
				original, rival := issue2564OperationPair(scenario)
				captured := issue2564OperationJSON(t, original)
				losses := f.claim.Snapshot.MaxRetries + 2
				refused := scenario == "index_out_of_range_after_replacement"
				if refused {
					losses = 1
				}
				wrapped := &issue2564OperationRaceStore{
					workflowTestSelectedStore: f.selected, t: t, fixture: f,
					remaining: losses, rival: rival,
				}
				writer := issue2564OperationCoordinator(f, wrapped)
				result, err := writer.ApplyEntityFieldMutation(f.ctx, f.command(original))
				if refused {
					if err == nil || result.Acknowledged || result.Revision != 0 || failures.IsStateContention(err) {
						t.Fatalf("fresh out-of-range index was not a non-contention refusal: %+v %v", result, err)
					}
				} else if err != nil || !result.Acknowledged {
					t.Fatalf("original operation failed after real CAS contention: %+v %v", result, err)
				}
				if issue2564OperationJSON(t, original) != captured {
					t.Fatal("retry mutated the captured original operation/value/key/index")
				}
				wantCalls, ownCommits := losses+1, 1
				if refused {
					wantCalls, ownCommits = losses, 0
				}
				if wrapped.calls != wantCalls || wrapped.conflicts != losses || wrapped.rivalCommits != losses || wrapped.ownCommits != ownCommits {
					t.Fatalf("real CAS attempts=%d conflicts=%d rival=%d own=%d, want %d/%d/%d/%d", wrapped.calls, wrapped.conflicts, wrapped.rivalCommits, wrapped.ownCommits, wantCalls, losses, losses, ownCommits)
				}
				if !refused && wrapped.conflicts <= f.claim.Snapshot.MaxRetries {
					t.Fatal("proof did not exceed the unchanged finite delivery retry budget")
				}
				current := issue2564LoadOperationInstance(t, f)
				want := issue2564OperationExpected(scenario, losses)
				field, _, _ := strings.Cut(strings.TrimPrefix(original.Target, "entity."), ".")
				if !reflect.DeepEqual(current.Fields[field], want) {
					t.Fatalf("%s reapplication used a stale collection or re-resolved the index: got %#v want %#v", scenario, current.Fields[field], want)
				}
				wantRevision := initial.Revision + int64(losses+ownCommits)
				if current.Revision != wantRevision || !refused && int64(result.Revision) != wantRevision {
					t.Fatalf("canonical revision=%d receipt=%d, want %d", current.Revision, result.Revision, wantRevision)
				}
				issue2564AssertOperationReceipts(t, f, field, ownCommits, losses)
				issue2564AssertOperationDeliveryUnchanged(t, f)
				if refused {
					// The sole persisted effect belongs to the competing replacement.
					if !reflect.DeepEqual(wrapped.afterRival, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
						t.Fatal("out-of-range retry changed state/history after the competitor committed")
					}
				} else {
					settled, err := f.selected.SettleSuccess(f.ctx, f.claim.Claim, []string{"handler_completed"}, time.Millisecond, deliverylifecycle.NotApplicableHandlerRuleSelection())
					if err != nil || settled.Status != deliverylifecycle.StatusDelivered || settled.RetryCount != 0 || settled.ClaimVersion != f.claim.Claim.Version() {
						t.Fatalf("single final settlement after contention: %+v %v", settled, err)
					}
				}
				issue2564AssertOperationConstruction(t, f, initial)
			})
		}
	}
}

func TestIssue2564EntityOperationsAtomicRefusalBothStores(t *testing.T) {
	cases := []struct {
		name string
		op   entityruntime.Mutation
	}{
		{"negative_index", entityruntime.Mutation{Operation: "update", Target: "entity.positions", Index: -1, HasIndex: true, Value: "literal"}},
		{"fractional_index", entityruntime.Mutation{Operation: "update", Target: "entity.positions", Index: 0.5, HasIndex: true, Value: "literal"}},
		{"out_of_range_index", entityruntime.Mutation{Operation: "update", Target: "entity.positions", Index: 2, HasIndex: true, Value: "literal"}},
		{"missing_index", entityruntime.Mutation{Operation: "update", Target: "entity.positions", Value: "literal"}},
		{"wrong_append_type", entityruntime.Mutation{Operation: "append", Target: "entity.items", Value: 7}},
		{"invalid_map_key", entityruntime.Mutation{Operation: "set", Target: "entity.labels", HasKey: true, Key: "", Value: "literal"}},
		{"keyed_list_forbidden", entityruntime.Mutation{Operation: "set", Target: "entity.items", HasKey: true, Key: "business-id", Value: "literal"}},
		{"source_context_mismatch", entityruntime.Mutation{Operation: "append", Target: "entity.items", Value: "literal"}},
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, tc := range cases {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				f := newIssue2564OperationFixture(t, backend)
				initial := issue2564LoadOperationInstance(t, f)
				before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
				wrapped := &issue2564OperationRaceStore{workflowTestSelectedStore: f.selected, t: t, fixture: f}
				ctx := f.ctx
				if tc.name == "source_context_mismatch" {
					ctx = correlation.WithSourceArtifactFact(ctx, mustStoreTestSourceArtifactFact("bundle-v2:sha256:"+strings.Repeat("f", 64)))
				}
				result, err := issue2564OperationCoordinator(f, wrapped).ApplyEntityFieldMutation(ctx, f.command(tc.op))
				if err == nil || result.Acknowledged || result.Revision != 0 || failures.IsStateContention(err) || wrapped.calls != 0 {
					t.Fatalf("invalid operation reached commit/retry: result=%+v err=%v calls=%d", result, err, wrapped.calls)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
					t.Fatal("atomic refusal changed application state, constructor/source history or delivery accounting")
				}
				issue2564AssertOperationDeliveryUnchanged(t, f)
				issue2564AssertOperationConstruction(t, f, initial)
			})
		}
	}
}

func TestIssue2564EntityAppendAcknowledgedCleanupDoesNotRepeatBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newIssue2564OperationFixture(t, backend)
			initial := issue2564LoadOperationInstance(t, f)
			wrapped := &issue2564OperationRaceStore{workflowTestSelectedStore: f.selected, t: t, fixture: f, requestHandoff: true}
			// This is a postcommit cleanup error, not a fabricated precommit CAS
			// loss: acknowledgement must dominate even a contention-shaped cause.
			cleanup := failures.New(failures.ClassLifecycleConflict, "workflow_engine_state_revision_conflict", "issue2564-cleanup", "handoff_after_commit", nil)
			submits := 0
			sink := &completionHandoffEvidenceProbeSink{submit: func(candidate runlifecycle.Candidate) error {
				submits++
				if candidate.RunID != f.state.Identity.RunID {
					t.Fatalf("cleanup handoff changed run: %+v", candidate)
				}
				// The real finalizer invokes this sink after durable commit.
				current := issue2564LoadOperationInstance(t, f)
				if current.Revision != initial.Revision+1 || !reflect.DeepEqual(current.Fields["items"], []any{"equal", "equal"}) {
					t.Fatalf("cleanup callback preceded exact append COMMIT: %+v", current)
				}
				return cleanup
			}}
			registration, err := f.selected.(runlifecycle.CandidateRegistrar).RegisterCompletionCandidateSink(f.ctx, runlifecycle.CandidateScope{BundleHash: f.bundle.SourceArtifact.BundleHash()}, sink)
			if err != nil {
				t.Fatal(err)
			}
			defer registration.Release()
			result, err := issue2564OperationCoordinator(f, wrapped).ApplyEntityFieldMutation(f.ctx, f.command(entityruntime.Mutation{Operation: "append", Target: "entity.items", Value: "equal"}))
			if !result.Acknowledged || !errors.Is(err, cleanup) || !failures.IsStateContention(err) || int64(result.Revision) != initial.Revision+1 || wrapped.calls != 1 || wrapped.ownCommits != 1 || submits != 1 {
				t.Fatalf("acknowledged cleanup reissued append or lost receipt: result=%+v err=%v calls=%d commits=%d handoffs=%d", result, err, wrapped.calls, wrapped.ownCommits, submits)
			}
			current := issue2564LoadOperationInstance(t, f)
			if !reflect.DeepEqual(current.Fields["items"], []any{"equal", "equal"}) {
				t.Fatalf("acknowledged append repeated: %#v", current.Fields["items"])
			}
			issue2564AssertOperationReceipts(t, f, "items", 1, 0)
			issue2564AssertOperationDeliveryUnchanged(t, f)
			issue2564AssertOperationConstruction(t, f, initial)
		})
	}
}

func TestIssue2564EntityUnacknowledgedCommitHasNoRevisionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newIssue2564OperationFixture(t, backend)
			initial := issue2564LoadOperationInstance(t, f)
			before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			wrapped := &issue2564OperationRaceStore{workflowTestSelectedStore: f.selected, t: t, fixture: f, cancelBeforeCommit: cancel}
			result, err := issue2564OperationCoordinator(f, wrapped).ApplyEntityFieldMutation(ctx, f.command(entityruntime.Mutation{Operation: "append", Target: "entity.items", Value: "uncommitted"}))
			if !errors.Is(err, context.Canceled) || result.Acknowledged || result.Revision != 0 || wrapped.calls != 1 || wrapped.ownCommits != 0 {
				t.Fatalf("unacknowledged SQL commit returned a fabricated revision: result=%+v err=%v calls=%d commits=%d", result, err, wrapped.calls, wrapped.ownCommits)
			}
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
				t.Fatal("unacknowledged commit changed canonical state or constructor/source history")
			}
			issue2564AssertOperationDeliveryUnchanged(t, f)
			issue2564AssertOperationConstruction(t, f, initial)
		})
	}
}

func TestIssue2564CanonicalWriterEvidenceNoopRollbackForkBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newIssue2564OperationFixture(t, backend)
			initial := issue2564LoadOperationInstance(t, f)
			wrapped := &issue2564OperationRaceStore{workflowTestSelectedStore: f.selected, t: t, fixture: f}
			writer := issue2564OperationCoordinator(f, wrapped)
			beforeNoop := issue2564OperationEvidenceCounts(t, f)
			noop, err := writer.ApplyEntityFieldMutation(f.ctx, f.command(entityruntime.Mutation{Target: "entity.profile.leaf", Value: "old-leaf"}))
			if err != nil || !noop.Acknowledged {
				t.Fatalf("canonical no-op set: %+v %v", noop, err)
			}
			// Preserve the retired diff test's exact contribution boundary. Header
			// revision bookkeeping is not an authored mutation/story occurrence.
			if after := issue2564OperationEvidenceCounts(t, f); beforeNoop != after {
				t.Fatalf("no-op minted mutation facts/story: before=%+v after=%+v", beforeNoop, after)
			}
			if current := issue2564LoadOperationInstance(t, f); !reflect.DeepEqual(current.Fields, initial.Fields) {
				t.Fatal("no-op set changed constructed fields")
			}
			commits := wrapped.ownCommits
			written, err := writer.ApplyEntityFieldMutation(f.ctx, f.command(entityruntime.Mutation{Operation: "append", Target: "entity.items", Value: "fork-retained"}))
			if err != nil || !written.Acknowledged || wrapped.ownCommits != commits+1 {
				t.Fatalf("retained history was not written through actual SQL COMMIT: %+v %v", written, err)
			}
			retained := issue2564LoadOperationInstance(t, f)
			if !reflect.DeepEqual(retained.Fields["items"], []any{"equal", "fork-retained"}) {
				t.Fatalf("successful append not retained: %#v", retained.Fields)
			}
			counts := issue2564OperationEvidenceCounts(t, f)
			if counts.mutations != beforeNoop.mutations+1 || counts.mutationFacts != beforeNoop.mutationFacts+1 {
				t.Fatalf("actual SQL commit did not contribute one exact mutation fact: before=%+v after=%+v", beforeNoop, counts)
			}
			ledger := exactFactStore{db: f.db, postgres: backend == "postgres"}
			assertActualMutationLedger(t, f.ctx, ledger, f.state.Identity.RunID, f.state.EntityID)
			issue2564AssertOperationReceipts(t, f, "items", 1, 0)
			if _, err := f.selected.SettleSuccess(f.ctx, f.claim.Claim, []string{"handler_completed"}, time.Millisecond, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
			cut := eventtest.ExistingRunRootIngress(uuid.NewString(), "operations.snapshot", "fixture", "", []byte(`{}`), 0, f.state.Identity.RunID, events.EventEnvelope{}, time.Now().UTC().Truncate(time.Microsecond))
			if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, cut); err != nil {
				t.Fatal(err)
			}
			planner := f.store.(interface {
				PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
			})
			request := runfork.RunForkPlanRequest{SourceRunID: f.state.Identity.RunID, At: cut.ID()}
			plan, err := planner.PlanRunFork(f.ctx, request)
			if err != nil || plan.ForkPoint.Revision <= 0 {
				t.Fatalf("canonical historical fork readback: %+v %v", plan.ForkPoint, err)
			}
			issue2564AssertRetainedFork(t, f, retained, plan)
			issue2564InstallOperationRollback(t, f, retained.Revision)
			beforeRollback := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
			beforeCounts := issue2564OperationEvidenceCounts(t, f)
			failed, err := writer.ApplyEntityFieldMutation(f.ctx, f.command(entityruntime.Mutation{Operation: "append", Target: "entity.items", Value: "must-rollback"}))
			if err == nil || !strings.Contains(err.Error(), "issue2564_late_rollback") || failed.Acknowledged || failed.Revision != 0 {
				t.Fatalf("late SQL rollback was not an unacknowledged refusal: %+v %v", failed, err)
			}
			current := issue2564LoadOperationInstance(t, f)
			if current.Revision != retained.Revision || !reflect.DeepEqual(current.Fields, retained.Fields) || issue2564OperationEvidenceCounts(t, f) != beforeCounts || !reflect.DeepEqual(beforeRollback, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
				t.Fatal("late precommit rollback changed field/revision/mutation/story/source history")
			}
			again, err := planner.PlanRunFork(f.ctx, request)
			if err != nil || again.ForkPoint != plan.ForkPoint || !reflect.DeepEqual(again.Entities, plan.Entities) {
				t.Fatalf("rollback changed exact retained historical fork cut: %+v %v", again.ForkPoint, err)
			}
			issue2564AssertRetainedFork(t, f, retained, again)
			assertActualMutationLedger(t, f.ctx, ledger, f.state.Identity.RunID, f.state.EntityID)
			issue2564AssertOperationConstruction(t, f, initial)
		})
	}
}

func TestIssue2564EntityExplicitTerminationPreservesAcknowledgedOperationsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, order := range []string{"save_then_terminate", "terminate_then_save"} {
			t.Run(backend+"/"+order, func(t *testing.T) {
				f := newIssue2564OperationFixture(t, backend)
				wrapped := &issue2564OperationRaceStore{workflowTestSelectedStore: f.selected, t: t, fixture: f}
				writer := issue2564OperationCoordinator(f, wrapped)
				want := []any{"equal"}
				own := 0
				var acknowledged pipeline.EntityFieldMutationResult
				if order == "save_then_terminate" {
					var err error
					acknowledged, err = writer.ApplyEntityFieldMutation(f.ctx, f.command(entityruntime.Mutation{Operation: "append", Target: "entity.items", Value: "retained"}))
					if err != nil || !acknowledged.Acknowledged {
						t.Fatalf("earlier operation not acknowledged: %+v %v", acknowledged, err)
					}
					want, own = append(want, "retained"), 1
				}
				if err := f.workflows.MarkTerminated(f.ctx, f.state.Identity, identity.NormalizeEntityID(f.state.EntityID), time.Now().UTC().Truncate(time.Microsecond)); err != nil {
					t.Fatal(err)
				}
				terminated := issue2564LoadOperationInstance(t, f)
				if terminated.Status != "terminated" || terminated.CurrentState != "active" || !reflect.DeepEqual(terminated.Fields["items"], want) || own == 1 && acknowledged.Revision >= int(terminated.Revision) {
					t.Fatalf("explicit termination lost acknowledged fields or substituted a stage check: %+v receipt=%+v", terminated, acknowledged)
				}
				before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
				calls := wrapped.calls
				result, err := writer.ApplyEntityFieldMutation(f.ctx, f.command(entityruntime.Mutation{Operation: "append", Target: "entity.items", Value: "must-not-write"}))
				failure := failures.Normalize(err, "issue2564", "terminated_operation")
				if err == nil || result.Acknowledged || result.Revision != 0 || failure.Class != failures.ClassAuthorizationDenied || failure.Detail.Code != "entity_target_not_active" || failures.IsStateContention(err) || wrapped.calls != calls {
					t.Fatalf("explicitly retired target accepted/retried an operation: result=%+v failure=%+v calls=%d/%d", result, failure, wrapped.calls, calls)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
					t.Fatal("refused operation resurrected fields/evidence/retired authority")
				}
				issue2564AssertOperationReceipts(t, f, "items", own, 0)
			})
		}
	}
}

func TestIssue2564EntityRunRevokedDuringCASReevaluationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newIssue2564OperationFixture(t, backend)
			_, rival := issue2564OperationPair("append_distinct")
			wrapped := &issue2564OperationRaceStore{workflowTestSelectedStore: f.selected, t: t, fixture: f, remaining: 1, rival: rival}
			var revoked map[string][]string
			wrapped.afterLoss = func() {
				stopped, _, err := f.selected.MarkTerminalRun(f.ctx, runlifecycle.TerminalRequest{RunID: f.state.Identity.RunID, State: runlifecycle.StateCancelled, EndedAt: time.Now().UTC().Truncate(time.Microsecond)})
				if err != nil || stopped.State != runlifecycle.StateCancelled {
					t.Fatalf("actual semantic run-stop owner: %+v %v", stopped, err)
				}
				revoked = snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
			}
			result, err := issue2564OperationCoordinator(f, wrapped).ApplyEntityFieldMutation(f.ctx, f.command(entityruntime.Mutation{Operation: "append", Target: "entity.items", Value: "must-not-write"}))
			// One stale CAS is reevaluated once. The next real SQL attempt is
			// refused by run authority, not committed or treated as contention.
			if !errors.Is(err, runlifecycle.ErrRunNotActive) || result.Acknowledged || result.Revision != 0 || failures.IsStateContention(err) || wrapped.calls != 2 || wrapped.conflicts != 1 || wrapped.ownCommits != 0 || wrapped.rivalCommits != 1 || revoked == nil {
				t.Fatalf("revoked run repeated/refused the wrong commit: result=%+v err=%v calls=%d conflicts=%d own=%d rival=%d", result, err, wrapped.calls, wrapped.conflicts, wrapped.ownCommits, wrapped.rivalCommits)
			}
			if !reflect.DeepEqual(revoked, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
				t.Fatal("CAS re-evaluation changed revoked run fields/evidence/authority")
			}
			if current := issue2564LoadOperationInstance(t, f); !reflect.DeepEqual(current.Fields["items"], []any{"equal", "rival-1"}) {
				t.Fatalf("original operation escaped revocation: %#v", current.Fields)
			}
			issue2564AssertOperationReceipts(t, f, "items", 0, 1)
		})
	}
}

func issue2564InstallOperationRollback(t *testing.T, f issue2564OperationFixture, revision int64) {
	t.Helper()
	name := "issue2564_rollback_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	run, entity := f.state.Identity.RunID, f.state.EntityID
	prefix := fmt.Sprintf("(SELECT revision FROM flow_instances WHERE run_id='%s' AND entity_id='%s')=%d AND EXISTS (SELECT 1 FROM entity_state WHERE run_id='%s' AND entity_id='%s' AND CAST(fields AS TEXT) LIKE '%%must-rollback%%') AND EXISTS (SELECT 1 FROM entity_mutations WHERE run_id='%s' AND entity_id='%s' AND writer_id='operation-agent' AND CAST(new_value AS TEXT) LIKE '%%must-rollback%%')", run, entity, revision+1, run, entity, run, entity)
	query := fmt.Sprintf("CREATE TRIGGER %s BEFORE INSERT ON run_fork_revisions WHEN NEW.run_id='%s' BEGIN SELECT CASE WHEN %s THEN RAISE(ABORT,'issue2564_late_rollback') ELSE RAISE(ABORT,'issue2564_missing_rollback_prefix') END; END", name, run, prefix)
	if f.selectedBackend() == "postgres" {
		function := fmt.Sprintf("CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.run_id='%s' THEN IF %s THEN RAISE EXCEPTION 'issue2564_late_rollback'; ELSE RAISE EXCEPTION 'issue2564_missing_rollback_prefix'; END IF; END IF; RETURN NEW; END $$", name, run, prefix)
		issue2564ExecFixtureSQL(t, f.db, function)
		t.Cleanup(func() {
			if _, err := f.db.Exec("DROP FUNCTION IF EXISTS " + name + "()"); err != nil {
				t.Error(err)
			}
		})
		query = fmt.Sprintf("CREATE TRIGGER %s BEFORE INSERT ON run_fork_revisions FOR EACH ROW EXECUTE FUNCTION %s()", name, name)
	}
	issue2564ExecFixtureSQL(t, f.db, query)
	t.Cleanup(func() {
		query := "DROP TRIGGER IF EXISTS " + name
		if f.selectedBackend() == "postgres" {
			query += " ON run_fork_revisions"
		}
		if _, err := f.db.Exec(query); err != nil {
			t.Error(err)
		}
	})
}

func issue2564ExecFixtureSQL(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}
}

type issue2564OperationEvidence struct{ mutations, mutationFacts, activity int }

func issue2564OperationEvidenceCounts(t *testing.T, f issue2564OperationFixture) issue2564OperationEvidence {
	t.Helper()
	var counts issue2564OperationEvidence
	for query, target := range map[string]*int{
		`SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1`:                                      &counts.mutations,
		`SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='entity_mutations'`: &counts.mutationFacts,
		`SELECT COUNT(*) FROM author_activity_occurrences WHERE run_id=$1`:                           &counts.activity,
	} {
		if err := f.db.QueryRowContext(f.ctx, query, f.state.Identity.RunID).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	return counts
}

func issue2564AssertRetainedFork(t *testing.T, f issue2564OperationFixture, retained pipeline.WorkflowInstance, plan runfork.RunForkPlan) {
	t.Helper()
	matches := 0
	for _, entity := range plan.Entities {
		if entity.EntityID != f.state.EntityID {
			continue
		}
		matches++
		if entity.CurrentState != retained.CurrentState || !reflect.DeepEqual(entity.Fields, retained.Fields) || !reflect.DeepEqual(entity.Bookkeeping, retained.Bookkeeping) {
			t.Fatalf("fork history lost canonical writer/constructor values: %+v", entity)
		}
	}
	if matches != 1 {
		t.Fatalf("historical fork did not retain exactly the constructed writer target: matches=%d entities=%+v", matches, plan.Entities)
	}
}

type issue2564OperationFixture struct {
	receiverConfigActivationFixture
	selected workflowTestSelectedStore
	state    pipeline.WorkflowEngineStateRecord
	claim    deliverylifecycle.ClaimedObligation
	event    events.Event
	contract entityruntime.Contract
	source   semanticview.Source
	artifact sourceartifact.Persisted
}

func newIssue2564OperationFixture(t *testing.T, backend string) issue2564OperationFixture {
	t.Helper()
	const flow = "operations"
	f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
		"schema.yaml":            "name: issue2564-operation-proof\n",
		"events.yaml":            "operations.requested:\noperations.snapshot:\n",
		"types.yaml":             "types:\n  OperationProfile:\n    leaf: text\n    sibling: text\n",
		"operations/schema.yaml": "name: operations\ninstance: receiver_key\nstages:\n  active: {initial: true}\n  done: {terminal: true}\npins:\n  inputs: [construct.requested]\n",
		"operations/entities.yaml": `operation_state:
  receiver_key: text
  items: {type: list<text>, initial: [equal]}
  labels: {type: 'map[text]text', initial: {kept: initial}}
  positions: {type: list<text>, initial: [old-0, old-1]}
  profile: {type: OperationProfile, initial: {leaf: old-leaf, sibling: initial}}
`,
		"operations/events.yaml": "construct.requested:\n",
	}, nil)
	runID := correlation.RunIDFromContext(f.ctx)
	req := sqliteFlowActivationRequest(f.bundle, flow, "receiver", "", flow+"/receiver")
	req.OccurredAt = time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	req.ConstructorInput, req.ResolvedKey = "construct.requested", "receiver"
	req.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "construct.requested", "constructor-fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, req.OccurredAt)
	activation, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, activation)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("canonical operation constructor: %+v %v", committed, err)
	}
	persisted, err := activation.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(f.bundle)
	contract, found := entityruntime.ResolveForFlow(source, flow)
	if !found {
		t.Fatal("constructed operation source lacks its exact typed entity contract")
	}
	selected := f.store.(workflowTestSelectedStore)
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceNode(flow, "operation-writer")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: flow, FlowInstance: persisted.State.Identity.Route.InstancePath, EntityID: persisted.State.EntityID})}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "operations.requested", "fixture", "", []byte(`{}`), 0, runID, events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, persisted.State.EntityID), persisted.State.Identity.Route.InstancePath), time.Now().UTC().Truncate(time.Microsecond))
	if err := commitSemanticEventFixtureWithRoutes(f.ctx, f.store.(stateOnlyAcquisitionStore), event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claim, err := claimDeliveryFixture(f.ctx, selected, event, route)
	if err != nil {
		t.Fatal(err)
	}
	f.ctx = deliverylifecycle.WithClaim(correlation.WithInboundEvent(f.ctx, event), claim.Claim)
	artifact, err := f.store.(selectedSourceArtifactStore).GetSourceArtifact(f.ctx, f.bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	fixture := issue2564OperationFixture{receiverConfigActivationFixture: f, selected: selected, state: persisted.State, claim: claim, event: event, contract: contract, source: source, artifact: artifact}
	instance := issue2564LoadOperationInstance(t, fixture)
	if instance.Revision != 1 || instance.CurrentState != "active" || !reflect.DeepEqual(instance.Fields["items"], []any{"equal"}) || !reflect.DeepEqual(instance.Fields["positions"], []any{"old-0", "old-1"}) || !reflect.DeepEqual(instance.Fields["labels"], map[string]any{"kept": "initial"}) || !reflect.DeepEqual(instance.Fields["profile"], map[string]any{"leaf": "old-leaf", "sibling": "initial"}) {
		t.Fatalf("canonical constructor did not materialize original typed defaults: %+v", instance)
	}
	return fixture
}

func (f issue2564OperationFixture) command(operation entityruntime.Mutation) pipeline.EntityFieldMutation {
	return pipeline.EntityFieldMutation{RunID: f.state.Identity.RunID, EntityID: f.state.EntityID, Owner: f.state.Identity, FlowID: "operations", Source: f.source, Mutation: operation, Writer: mutationlog.Writer{Type: "agent", ID: "operation-agent", HandlerStep: "save_entity_field"}}
}

func issue2564OperationCoordinator(f issue2564OperationFixture, selected workflowTestSelectedStore) *pipeline.PipelineCoordinator {
	opts := completeWorkflowTestCoordinatorOptions(pipeline.NewWorkflowPersistence(selected), selected)
	opts.Module = runForkGateWorkflowModule{source: f.source}
	opts.SourceArtifactFact = mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash())
	return pipeline.NewPipelineCoordinatorWithOptions(workflowTestBus{}, opts)
}

type issue2564OperationRaceStore struct {
	workflowTestSelectedStore
	t                  *testing.T
	fixture            issue2564OperationFixture
	requestHandoff     bool
	cancelBeforeCommit context.CancelFunc
	afterLoss          func()
	remaining          int
	rival              func(int) entityruntime.Mutation
	calls              int
	conflicts          int
	rivalCommits       int
	ownCommits         int
	afterRival         map[string][]string
}

func (s *issue2564OperationRaceStore) CommitWorkflowEngineMutation(ctx context.Context, command pipeline.WorkflowEngineMutationCommand) (pipeline.CommittedWorkflowEngineMutation, error) {
	s.calls++
	if command.Writer == nil || command.Writer.ID != "operation-agent" || command.State.Identity.Normalize() != s.fixture.state.Identity.Normalize() || command.State.EntityID != s.fixture.state.EntityID || command.WriterSource.BundleHash() != s.fixture.bundle.SourceArtifact.BundleHash() {
		s.t.Fatalf("retry changed original owner/source/attribution: %+v", command)
	}
	before := issue2564LoadOperationInstance(s.t, s.fixture)
	if before.Revision != command.State.ExpectedRevision || before.CurrentState != command.State.ExpectedState {
		s.t.Fatalf("attempt did not retain its actual evaluated canonical revision: state=%+v expected=%d/%s", before, command.State.ExpectedRevision, command.State.ExpectedState)
	}
	raced := s.remaining > 0
	if raced {
		// Model an independently committing writer after the agent's R1, without
		// reacquiring its process lock or synthesizing a conflict return value.
		fields, err := entityruntime.ApplyMutations(s.fixture.contract, before.Fields, []entityruntime.Mutation{s.rival(s.rivalCommits + 1)})
		if err != nil {
			s.t.Fatal(err)
		}
		winner := command
		winner.State.Fields, err = canonicaljson.MarshalPreservingNumberKinds(fields)
		if err != nil {
			s.t.Fatal(err)
		}
		winner.State.UpdatedAt = time.Now().UTC().Truncate(time.Microsecond)
		winner.Writer = &mutationlog.Writer{Type: "agent", ID: "competing-agent", HandlerStep: "save_entity_field"}
		committed, err := s.workflowTestSelectedStore.CommitWorkflowEngineMutation(ctx, winner)
		if err != nil || !committed.Committed {
			s.t.Fatalf("real competing canonical commit failed: %+v %v", committed, err)
		}
		s.rivalCommits++
		s.remaining--
		s.afterRival = snapshotForkHistoricalExecutionTables(s.t, s.fixture.db, s.fixture.selectedBackend() == "postgres")
	}
	if s.requestHandoff {
		// Plain field operations do not request a candidate. This test-only
		// decoration exercises the real SQL owner's post-COMMIT handoff error.
		command.Lifecycle.RequestCompletionCandidate = true
	}
	if s.cancelBeforeCommit != nil {
		// Cancel only after the operation has been evaluated, then let the real
		// selected SQL owner refuse it without fabricating an acknowledgement.
		s.cancelBeforeCommit()
	}
	result, err := s.workflowTestSelectedStore.CommitWorkflowEngineMutation(ctx, command)
	if raced {
		if result.Committed || !failures.IsStateContention(err) {
			s.t.Fatalf("real stale SQL CAS did not return recognized contention: %+v %v", result, err)
		}
		s.conflicts++
		if !reflect.DeepEqual(s.afterRival, snapshotForkHistoricalExecutionTables(s.t, s.fixture.db, s.fixture.selectedBackend() == "postgres")) {
			s.t.Fatal("losing SQL CAS changed state/history or consumed delivery failure accounting")
		}
		if s.afterLoss != nil {
			s.afterLoss()
		}
	} else if result.Committed {
		s.ownCommits++
	}
	return result, err
}

func (f issue2564OperationFixture) selectedBackend() string {
	if _, sqlite := f.store.(*SQLiteRuntimeStore); sqlite {
		return "sqlite"
	}
	return "postgres"
}

func issue2564LoadOperationInstance(t *testing.T, f issue2564OperationFixture) pipeline.WorkflowInstance {
	t.Helper()
	instance, found, err := f.workflows.Load(f.ctx, f.state.Identity)
	if err != nil || !found {
		t.Fatalf("load exact constructed operation target: found=%t err=%v", found, err)
	}
	var headerRevision int64
	if err := f.db.QueryRowContext(f.ctx, `SELECT revision FROM flow_instances WHERE run_id=$1 AND entity_id=$2 AND instance_path=$3`, f.state.Identity.RunID, f.state.EntityID, f.state.Identity.Route.InstancePath).Scan(&headerRevision); err != nil {
		t.Fatal(err)
	}
	if instance.Revision != headerRevision {
		t.Fatalf("evaluation revision=%d differs from canonical header=%d", instance.Revision, headerRevision)
	}
	return instance
}

func issue2564OperationJSON(t *testing.T, operation entityruntime.Mutation) string {
	t.Helper()
	raw, err := json.Marshal(operation)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func issue2564OperationPair(scenario string) (entityruntime.Mutation, func(int) entityruntime.Mutation) {
	op := entityruntime.Mutation{Value: "literal"}
	var rival func(int) entityruntime.Mutation
	switch scenario {
	case "append_distinct", "append_equal":
		op.Operation, op.Target = "append", "entity.items"
		if scenario == "append_equal" {
			op.Value = "equal"
		}
		rival = func(n int) entityruntime.Mutation {
			value := fmt.Sprintf("rival-%d", n)
			if scenario == "append_equal" {
				value = "equal"
			}
			return entityruntime.Mutation{Operation: "append", Target: op.Target, Value: value}
		}
	case "map_distinct_key", "map_same_key":
		op.Operation, op.Target, op.HasKey, op.Key = "set", "entity.labels", true, "captured-key"
		rival = func(n int) entityruntime.Mutation {
			key := "other-key"
			if scenario == "map_same_key" {
				key = "captured-key"
			}
			return entityruntime.Mutation{Operation: "set", Target: op.Target, HasKey: true, Key: key, Value: fmt.Sprintf("rival-%d", n)}
		}
	case "update_distinct_index", "update_same_index", "index_fresh_replacement", "index_out_of_range_after_replacement":
		op.Operation, op.Target, op.HasIndex, op.Index = "update", "entity.positions", true, 1
		rival = func(n int) entityruntime.Mutation {
			if scenario == "index_fresh_replacement" {
				return entityruntime.Mutation{Target: op.Target, Value: []any{fmt.Sprintf("replacement-%d", n), "old-0", "old-1"}}
			}
			if scenario == "index_out_of_range_after_replacement" {
				return entityruntime.Mutation{Target: op.Target, Value: []any{"short"}}
			}
			index := 0
			if scenario == "update_same_index" {
				index = 1
			}
			return entityruntime.Mutation{Operation: "update", Target: op.Target, HasIndex: true, Index: index, Value: fmt.Sprintf("rival-%d", n)}
		}
	case "m20_named_record_leaf":
		op.Target = "entity.profile.leaf"
		rival = func(n int) entityruntime.Mutation {
			return entityruntime.Mutation{Target: "entity.profile.sibling", Value: fmt.Sprintf("rival-%d", n)}
		}
	}
	return op, rival
}

func issue2564OperationExpected(scenario string, losses int) any {
	switch scenario {
	case "append_distinct", "append_equal":
		want := []any{"equal"}
		for n := 1; n <= losses; n++ {
			value := fmt.Sprintf("rival-%d", n)
			if scenario == "append_equal" {
				value = "equal"
			}
			want = append(want, value)
		}
		if scenario == "append_equal" {
			return append(want, "equal")
		}
		return append(want, "literal")
	case "map_distinct_key":
		return map[string]any{"kept": "initial", "other-key": fmt.Sprintf("rival-%d", losses), "captured-key": "literal"}
	case "map_same_key":
		return map[string]any{"kept": "initial", "captured-key": "literal"}
	case "update_distinct_index":
		return []any{fmt.Sprintf("rival-%d", losses), "literal"}
	case "update_same_index":
		return []any{"old-0", "literal"}
	case "index_fresh_replacement":
		return []any{fmt.Sprintf("replacement-%d", losses), "literal", "old-1"}
	case "index_out_of_range_after_replacement":
		return []any{"short"}
	case "m20_named_record_leaf":
		return map[string]any{"leaf": "literal", "sibling": fmt.Sprintf("rival-%d", losses)}
	}
	return nil
}

func issue2564AssertOperationDeliveryUnchanged(t *testing.T, f issue2564OperationFixture) {
	t.Helper()
	snapshot, err := f.selected.Snapshot(f.ctx, f.claim.Claim.DeliveryID())
	if err != nil || !reflect.DeepEqual(snapshot, f.claim.Snapshot) {
		t.Fatalf("local contention/refusal changed exact delivery claim: before=%+v after=%+v err=%v", f.claim.Snapshot, snapshot, err)
	}
	var retries, dead, settlements int
	if err := f.db.QueryRowContext(f.ctx, `SELECT COALESCE(SUM(retry_count),0),SUM(CASE WHEN status='dead_letter' THEN 1 ELSE 0 END) FROM event_deliveries WHERE run_id=$1`, f.state.Identity.RunID).Scan(&retries, &dead); err != nil {
		t.Fatal(err)
	}
	if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id=$1 AND closure_kind='settled'`, f.claim.Claim.DeliveryID()).Scan(&settlements); err != nil {
		t.Fatal(err)
	}
	if retries != 0 || dead != 0 || settlements != 0 {
		t.Fatalf("local operation charged delivery failure budget: retries=%d dead=%d settlements=%d", retries, dead, settlements)
	}
}

func issue2564AssertOperationReceipts(t *testing.T, f issue2564OperationFixture, field string, own, rivals int) {
	t.Helper()
	rows, err := f.db.QueryContext(f.ctx, `SELECT writer_type,writer_id,handler_step,CAST(caused_by_event AS TEXT),domain,path FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND writer_type='agent'`, f.state.Identity.RunID, f.state.EntityID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var kind, id, step, cause, domain, path string
		if err := rows.Scan(&kind, &id, &step, &cause, &domain, &path); err != nil {
			t.Fatal(err)
		}
		if kind != "agent" || step != "save_entity_field" || cause != f.event.ID() || domain != "authored_field" || path != field {
			t.Fatalf("operation lost original writer/cause/path: %s %s %s %s %s %s", kind, id, step, cause, domain, path)
		}
		counts[id]++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if counts["operation-agent"] != own || counts["competing-agent"] != rivals || len(counts) > 2 {
		t.Fatalf("acknowledged operation receipts=%v, want original=%d rivals=%d", counts, own, rivals)
	}
}

func issue2564AssertOperationConstruction(t *testing.T, f issue2564OperationFixture, initial pipeline.WorkflowInstance) {
	t.Helper()
	current := issue2564LoadOperationInstance(t, f)
	if current.StorageRef != initial.StorageRef || current.InstanceID != initial.InstanceID || current.EntityID != initial.EntityID || current.EntityType != initial.EntityType || current.InstanceKind != initial.InstanceKind || current.TemplateVersion != initial.TemplateVersion || current.ParentFlowID != initial.ParentFlowID || current.ParentFlowInstance != initial.ParentFlowInstance || current.ParentEntityID != initial.ParentEntityID || current.WorkflowName != initial.WorkflowName || current.WorkflowVersion != initial.WorkflowVersion || current.Mode != initial.Mode || current.Status != initial.Status || !current.TerminatedAt.Equal(initial.TerminatedAt) || !current.CreatedAt.Equal(initial.CreatedAt) || current.CurrentState != initial.CurrentState || !reflect.DeepEqual(current.InitialFieldValues, initial.InitialFieldValues) || !reflect.DeepEqual(current.Bookkeeping, initial.Bookkeeping) || !reflect.DeepEqual(current.Gates, initial.Gates) || !reflect.DeepEqual(current.StateBuckets, initial.StateBuckets) || !reflect.DeepEqual(current.TransitionHistory, initial.TransitionHistory) {
		t.Fatal("field operation rewrote constructor identity/initial fields or lifecycle history")
	}
	artifact, err := f.store.(selectedSourceArtifactStore).GetSourceArtifact(f.ctx, f.bundle.SourceArtifact.BundleHash())
	if err != nil || !reflect.DeepEqual(artifact, f.artifact) {
		t.Fatalf("operation rewrote admitted persisted source history: %v", err)
	}
	if !reflect.DeepEqual(f.bundle.SourceArtifact.LogicalBlob(), artifact.SourceBlob) {
		t.Fatal("operation mutated the admitted source after construction")
	}
}
