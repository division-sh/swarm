package pipelinepersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

func TestA2FirstPublicationStageEntryFenceOnBothStores(t *testing.T) {
	for _, scenario := range []struct {
		backend string
		fields  bool
	}{{"sqlite", false}, {"sqlite", true}, {"postgres", false}, {"postgres", true}} {
		backend := scenario.backend
		t.Run(fmt.Sprintf("%s/fields=%t", backend, scenario.fields), func(t *testing.T) {
			db := fanOutReadbackTestDB(t, backend)
			idType := "TEXT"
			if backend == "postgres" {
				idType = "UUID"
			}
			if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE entity_state (run_id %s, entity_id %s, flow_instance TEXT, entity_type TEXT, fields TEXT, PRIMARY KEY (run_id, entity_id))`, idType, idType)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE flow_instances (run_id %s, entity_id %s, instance_path TEXT, entity_type TEXT, flow_template TEXT, current_state TEXT, revision BIGINT, gates TEXT, bookkeeping TEXT, accumulator TEXT, PRIMARY KEY (run_id, instance_path))`, idType, idType)); err != nil {
				t.Fatal(err)
			}
			run, entity := uuid.NewString(), uuid.NewString()
			owner, err := flowidentity.NewRunScopedFlowInstance(run, flowidentity.StoredRoute("collector", "one", "collector/one"))
			if err != nil {
				t.Fatal(err)
			}
			declaration, err := timeridentity.NewJoinRef(identitytest.FlowNode(t, "collector", "gather"), "item.reported", "awaiting", "awaiting")
			if err != nil {
				t.Fatal(err)
			}
			entry := timeridentity.StageEntryRef{RunID: run, EntityID: entity, FlowScope: owner.Route.ScopeKey, InstanceID: owner.Route.InstanceID, InstancePath: owner.Route.InstancePath, Stage: "awaiting", Cause: "construction"}
			ref, err := declaration.BindStageEntry(entry, attemptgeneration.Generation{})
			if err != nil {
				t.Fatal(err)
			}
			at := time.Now().UTC()
			activation, err := joinruntime.NewActivation(ref, []string{"alpha", "beta"}, nil, at, at.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			bookkeeping, buckets := map[string]any{}, map[string]map[string]any{}
			encode := func() (string, string) {
				t.Helper()
				if err := workflowlifecycle.StoreStageEntry(bookkeeping, entry); err != nil {
					t.Fatal(err)
				}
				if err := joinruntime.Store(buckets, activation); err != nil {
					t.Fatal(err)
				}
				book, err := json.Marshal(bookkeeping)
				if err != nil {
					t.Fatal(err)
				}
				arms, err := json.Marshal(buckets)
				if err != nil {
					t.Fatal(err)
				}
				return string(book), string(arms)
			}
			fence := pipeline.WorkflowJoinAdmissionFence{Owner: owner, EntityID: entity, Arms: []pipeline.WorkflowJoinAdmissionArm{{Receipt: events.JoinAdmissionReceipt{Ref: declaration, Disposition: events.JoinAdmissionEarly}}}}
			check := func(valid bool) {
				t.Helper()
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				err = requireWorkflowJoinAdmissionTx(context.Background(), tx, []pipeline.WorkflowJoinAdmissionFence{fence}, backend == "postgres")
				if valid {
					if err != nil {
						t.Fatalf("exact admission rejected: %v", err)
					}
				} else {
					envelope, typed := failures.As(err)
					if !typed || envelope.Failure.Class != failures.ClassLifecycleConflict || envelope.Failure.Detail.Code != "join_publication_entry_changed" || engine.FailureDispositionFor(err) != engine.FailureDispositionRetry {
						t.Fatalf("changed entry was not a typed precommit retry: %v", err)
					}
				}
			}
			check(true)
			book, arms := encode()
			var entityType any
			if scenario.fields {
				entityType = "default"
				if _, err := db.Exec(`INSERT INTO entity_state VALUES ($1,$2,$3,'default','{}')`, run, entity, owner.Route.InstancePath); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.Exec(`INSERT INTO flow_instances VALUES ($1,$2,$3,$4,'collector',$5,1,'{}',$6,$7)`, run, entity, owner.Route.InstancePath, entityType, entry.Stage, book, arms); err != nil {
				t.Fatal(err)
			}
			check(false)
			fence.Entry, fence.Arms = entry, []pipeline.WorkflowJoinAdmissionArm{{Receipt: events.JoinAdmissionReceipt{Ref: ref, Disposition: events.JoinAdmissionBound}, Status: joinruntime.StatusOpen}}
			check(true)
			// A business revision and sibling arrival leave the exact arm unchanged.
			if _, err := activation.Add("alpha", "red"); err != nil {
				t.Fatal(err)
			}
			book, arms = encode()
			if _, err := db.Exec(`UPDATE flow_instances SET revision=2,bookkeeping=$3,accumulator=$4 WHERE run_id=$1 AND entity_id=$2`, run, entity, book, arms); err != nil {
				t.Fatal(err)
			}
			check(true)
			// A close winner changes admission even without a new stage entry.
			activation.CloseForStageExit()
			book, arms = encode()
			if _, err := db.Exec(`UPDATE flow_instances SET revision=3,bookkeeping=$3,accumulator=$4 WHERE run_id=$1 AND entity_id=$2`, run, entity, book, arms); err != nil {
				t.Fatal(err)
			}
			check(false)
			fence.Arms[0].Status = joinruntime.StatusClosed
			check(true)
			// Same-named non-loop re-entry is distinct even with identical clocks.
			entry.Cause, entry.EventID, entry.OccurrenceID, entry.TransitionID = "delivery", uuid.NewString(), uuid.NewString(), "transition:reentry"
			ref, err = declaration.BindStageEntry(entry, attemptgeneration.Generation{})
			if err != nil {
				t.Fatal(err)
			}
			activation, err = joinruntime.NewActivation(ref, []string{"alpha", "beta"}, nil, at, at.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			book, arms = encode()
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.Exec(`UPDATE flow_instances SET revision=3,bookkeeping=$3,accumulator=$4 WHERE run_id=$1 AND entity_id=$2`, run, entity, book, arms); err != nil {
				t.Fatal(err)
			}
			if err := requireWorkflowJoinAdmissionTx(context.Background(), tx, []pipeline.WorkflowJoinAdmissionFence{fence}, backend == "postgres"); err == nil {
				t.Fatal("first-publication binding accepted a superseded entry")
			}
			fence.Entry, fence.Arms = entry, []pipeline.WorkflowJoinAdmissionArm{{Receipt: events.JoinAdmissionReceipt{Ref: ref, Disposition: events.JoinAdmissionBound}, Status: joinruntime.StatusOpen}}
			if err := requireWorkflowJoinAdmissionTx(context.Background(), tx, []pipeline.WorkflowJoinAdmissionFence{fence}, backend == "postgres"); err != nil {
				t.Fatalf("same-commit entry rejected: %v", err)
			}
			if _, err := tx.Exec(`UPDATE flow_instances SET accumulator='{}' WHERE run_id=$1 AND entity_id=$2`, run, entity); err != nil {
				t.Fatal(err)
			}
			if err := requireWorkflowJoinAdmissionTx(context.Background(), tx, []pipeline.WorkflowJoinAdmissionFence{fence}, backend == "postgres"); err == nil {
				t.Fatal("missing immutable arm accepted")
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			check(false)
		})
	}
}
