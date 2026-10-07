package pipeline_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

type observedProspectiveBus struct {
	*runtimebus.EventBus
	calls int
	err   error
	plans []runtimeengine.DurablePublicationPlan
}

type observedProspectivePersistence struct {
	runtimepipeline.WorkflowPersistenceOwner
	mode       string
	calls      int
	err        error
	db         *sql.DB
	raced      bool
	armRace    bool
	conflict   error
	revisions  []int64
	onConflict func(context.Context, runtimepipeline.WorkflowEngineMutationCommand)
}

func (p *observedProspectivePersistence) CommitWorkflowEngineMutation(ctx context.Context, command runtimepipeline.WorkflowEngineMutationCommand) (runtimepipeline.CommittedWorkflowEngineMutation, error) {
	p.calls++
	p.revisions = append(p.revisions, command.State.ExpectedRevision)
	switch p.mode {
	case "changed_state":
		command.State.Fields = json.RawMessage(`{"case_id":"foreign"}`)
	case "changed_lifecycle":
		command.Lifecycle.RequestCompletionCandidate = !command.Lifecycle.RequestCompletionCandidate
	case "competing_revision":
		if p.armRace && !p.raced {
			// Advance persisted state after the real planner has read it, without
			// altering the prepared command or its prospective evidence.
			tx, err := p.db.BeginTx(ctx, nil)
			if err != nil {
				return runtimepipeline.CommittedWorkflowEngineMutation{}, err
			}
			defer tx.Rollback()
			result, err := tx.ExecContext(ctx, `UPDATE flow_instances SET revision=revision+1 WHERE run_id=$1 AND instance_path=$2 AND entity_id=$3 AND revision=$4`, command.State.Identity.RunID, command.State.Identity.Route.InstancePath, command.State.EntityID, command.State.ExpectedRevision)
			if err != nil {
				return runtimepipeline.CommittedWorkflowEngineMutation{}, err
			}
			if count, err := result.RowsAffected(); err != nil || count != 1 {
				return runtimepipeline.CommittedWorkflowEngineMutation{}, fmt.Errorf("competing revision cut rows=%d err=%v", count, err)
			}
			result, err = tx.ExecContext(ctx, `UPDATE entity_state SET revision=revision+1 WHERE run_id=$1 AND flow_instance=$2 AND entity_id=$3 AND revision=$4`, command.State.Identity.RunID, command.State.Identity.Route.InstancePath, command.State.EntityID, command.State.ExpectedRevision)
			if err != nil {
				return runtimepipeline.CommittedWorkflowEngineMutation{}, err
			}
			if count, err := result.RowsAffected(); err != nil || count != 1 {
				return runtimepipeline.CommittedWorkflowEngineMutation{}, fmt.Errorf("competing field revision cut rows=%d err=%v", count, err)
			}
			if err := tx.Commit(); err != nil {
				return runtimepipeline.CommittedWorkflowEngineMutation{}, err
			}
			p.raced = true
		}
	}
	result, err := p.WorkflowPersistenceOwner.CommitWorkflowEngineMutation(ctx, command)
	if runtimefailures.IsStateContention(err) {
		p.conflict = err
		if p.onConflict != nil {
			p.onConflict(ctx, command)
		}
	}
	p.err = err
	return result, err
}

func (b *observedProspectiveBus) PrepareEngineMutationPublications(ctx context.Context, intents []runtimeengine.EmitIntent, state runtimepipeline.PreparedWorkflowPublicationState) ([]runtimeengine.DurablePublicationPlan, error) {
	b.calls++
	b.plans, b.err = b.EventBus.PrepareEngineMutationPublications(ctx, intents, state)
	return b.plans, b.err
}

func TestProspectivePublicationRealPlannerTerminalAndOrdinaryWriterFenceBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore},
	} {
		for _, name := range []string{"active", "terminal", "changed_state", "changed_lifecycle", "publication_failure", "competing_revision"} {
			terminal := name == "terminal"
			t.Run(backend.name+"/"+name, func(t *testing.T) {
				selected := backend.open(t)
				runID := uuid.NewString()
				insertGateRecoveryRun(t, selected, runID)
				ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
				root := canonicalrouting.CopyMaterializingSenderExistingReceiver(t, true)
				if terminal {
					root = canonicalrouting.CopyProspectiveTerminalSender(t)
				}
				repo := canonicalrouting.RepoRoot(t)
				bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
				if err != nil {
					t.Fatal(err)
				}
				source := semanticview.Wrap(bundle)
				var nodes []runtimepipeline.WorkflowNode
				for _, declaration := range []struct{ id, event string }{{"intake", "start"}, {"receiver", "work.ready"}} {
					nodes = append(nodes, runtimepipeline.WorkflowNode{Node: externalPipelineSourceNode(t, source, "", declaration.id), Subscriptions: []events.EventType{events.EventType(declaration.event)}, ExecutionType: runtimecontracts.SystemNodeExecutionType})
				}
				canonical, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source})
				if err != nil {
					t.Fatal(err)
				}
				observed := &observedProspectiveBus{EventBus: canonical}
				persistence := &observedProspectivePersistence{WorkflowPersistenceOwner: selected.events.(runtimepipeline.WorkflowPersistenceOwner), mode: name, db: selected.db}
				selected.persistence = runtimepipeline.NewWorkflowPersistence(persistence)
				coordinator := newGateRecoveryCoordinator(observed, selected, runtimepipeline.PipelineCoordinatorOptions{Module: proposedEffectProofModule{source: source, nodes: nodes}, SourceArtifactFact: authorActivityTestSourceArtifactFact})
				commitKeylessConstructorComponent(t, ctx, selected, coordinator, source)
				var initialFields string
				var initialRevision int64
				if err := selected.db.QueryRow(`SELECT CAST(fields AS TEXT), revision FROM entity_state WHERE run_id=$1`, runID).Scan(&initialFields, &initialRevision); err != nil {
					t.Fatal(err)
				}
				assertUnchangedConstruction := func() {
					t.Helper()
					var fields string
					var fieldRevision, headerRevision int64
					if err := selected.db.QueryRow(`SELECT CAST(fields AS TEXT), revision FROM entity_state WHERE run_id=$1`, runID).Scan(&fields, &fieldRevision); err != nil {
						t.Fatal(err)
					}
					if err := selected.db.QueryRow(`SELECT revision FROM flow_instances WHERE run_id=$1`, runID).Scan(&headerRevision); err != nil {
						t.Fatal(err)
					}
					if fields != initialFields || fieldRevision != initialRevision || headerRevision != initialRevision {
						t.Fatalf("failed handler changed construction: fields=%s field revision=%d header revision=%d; want %s at %d", fields, fieldRevision, headerRevision, initialFields, initialRevision)
					}
				}
				if name == "publication_failure" {
					fault := `CREATE TRIGGER prospective_publication_failure BEFORE INSERT ON events WHEN NEW.event_name='work.ready' BEGIN SELECT RAISE(ABORT,'prospective_publication_fault_cut'); END`
					if selected.postgres {
						if _, err := selected.db.Exec(`CREATE FUNCTION prospective_publication_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_name='work.ready' THEN RAISE EXCEPTION 'prospective_publication_fault_cut'; END IF; RETURN NEW; END $$`); err != nil {
							t.Fatal(err)
						}
						fault = `CREATE TRIGGER prospective_publication_failure BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION prospective_publication_fail()`
					}
					if _, err := selected.db.Exec(fault); err != nil {
						t.Fatal(err)
					}
				}
				if name == "competing_revision" {
					initial := eventtest.ExistingRunRootIngress(uuid.NewString(), "start", "operator", "", []byte(`{"case_id":"first"}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
					if err := canonical.Publish(ctx, initial); err != nil {
						t.Fatal(err)
					}
					prepared, found, err := selected.events.LoadPreparedPublishEvent(ctx, initial.ID())
					if err != nil || !found || len(prepared.DeliveryRoutes) != 1 {
						t.Fatalf("initial routes=%d found=%t err=%v", len(prepared.DeliveryRoutes), found, err)
					}
					delivery, err := events.NewDeliveryEvent(prepared.Event.Event(), prepared.DeliveryRoutes[0])
					if err != nil {
						t.Fatal(err)
					}
					_, _, _, _ = coordinator.InterceptDeliveryRoute(ctx, delivery, prepared.DeliveryRoutes[0])
					if persistence.err != nil || persistence.calls != 1 || observed.err != nil || persistence.raced {
						t.Fatalf("initial creation failed before race: commits=%d err=%v planner=%v", persistence.calls, persistence.err, observed.err)
					}
					observed.calls, persistence.calls = 0, 0
					persistence.revisions = nil
					persistence.armRace = true
					persistence.onConflict = func(ctx context.Context, command runtimepipeline.WorkflowEngineMutationCommand) {
						current, err := persistence.LoadWorkflowTargetPersistence(ctx, command.State.Identity, identity.NormalizeEntityID(command.State.EntityID))
						var fields map[string]any
						if err != nil || json.Unmarshal(current.State.Fields, &fields) != nil || fields["case_id"] != "first" || int64(current.State.Revision) != command.State.ExpectedRevision+1 {
							t.Fatalf("stale attempt changed rival state: %+v err=%v", current, err)
						}
						for _, publication := range command.Publications {
							if _, found, err := selected.events.LoadPreparedPublishEvent(ctx, publication.DurablePublicationEventID()); err != nil || found {
								t.Fatalf("stale attempt leaked publication: found=%t err=%v", found, err)
							}
						}
					}
				}
				seed := eventtest.ExistingRunRootIngress(uuid.NewString(), "start", "operator", "", []byte(`{"case_id":"exact"}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
				if err := canonical.Publish(ctx, seed); err != nil {
					t.Fatal(err)
				}
				prepared, found, err := selected.events.LoadPreparedPublishEvent(ctx, seed.ID())
				if err != nil || !found || len(prepared.DeliveryRoutes) != 1 {
					t.Fatalf("seed routes=%#v found=%v err=%v", prepared.DeliveryRoutes, found, err)
				}
				delivery, err := events.NewDeliveryEvent(prepared.Event.Event(), prepared.DeliveryRoutes[0])
				if err != nil {
					t.Fatal(err)
				}
				_, _, execution, dispatchErr := coordinator.InterceptDeliveryRoute(ctx, delivery, prepared.DeliveryRoutes[0])
				wantPlans := 1
				if name == "competing_revision" {
					wantPlans = 2
				}
				if observed.calls != wantPlans {
					t.Fatalf("actual prepared mutation reached planner %d times", observed.calls)
				}
				var states, published int
				if err := selected.db.QueryRow(`SELECT count(*) FROM entity_state WHERE run_id=$1`, runID).Scan(&states); err != nil {
					t.Fatal(err)
				}
				if err := selected.db.QueryRow(`SELECT count(*) FROM events WHERE run_id=$1 AND event_name='work.ready'`, runID).Scan(&published); err != nil {
					t.Fatal(err)
				}
				if terminal {
					var refusal *runtimepipeline.TerminalReceiverError
					if !errors.As(observed.err, &refusal) || refusal.Stage != "done" || refusal.FlowID != "." {
						t.Fatalf("exact planning refusal=%v", observed.err)
					}
					if states != 1 || published != 0 {
						t.Fatalf("terminal mutation leaked: state=%d publications=%d", states, published)
					}
					assertUnchangedConstruction()
					return
				}
				if name == "competing_revision" {
					failure, typed := runtimefailures.EnvelopeFromError(persistence.conflict)
					if !persistence.raced || !typed || failure.Detail.Code != "workflow_engine_state_revision_conflict" || persistence.err != nil || observed.err != nil || len(observed.plans) != 1 || persistence.calls != 2 || len(persistence.revisions) != 2 || persistence.revisions[1] != persistence.revisions[0]+1 {
						t.Fatalf("wrong revision fence: raced=%t commits=%d commit=%v planning=%v execution=%+v dispatch=%v", persistence.raced, persistence.calls, persistence.err, observed.err, execution, dispatchErr)
					}
					var fields string
					if err := selected.db.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1`, runID).Scan(&fields); err != nil {
						t.Fatal(err)
					}
					var value map[string]any
					if err := json.Unmarshal([]byte(fields), &value); err != nil || value["case_id"] != "exact" || states != 1 || published != 2 {
						t.Fatalf("fresh retry did not publish exactly once: fields=%s states=%d published=%d err=%v", fields, states, published, err)
					}
					return
				}
				if name != "active" {
					if observed.err != nil || len(observed.plans) != 1 || persistence.calls != 1 || persistence.err == nil {
						t.Fatalf("did not reach real commit fault: planner=%v plans=%d commits=%d err=%v", observed.err, len(observed.plans), persistence.calls, persistence.err)
					}
					want := "publication prospective receiver state disagrees with committing mutation"
					if name == "publication_failure" {
						want = "prospective_publication_fault_cut"
					}
					if !strings.Contains(persistence.err.Error(), want) {
						t.Fatalf("wrong commit fault: %v", persistence.err)
					}
					var companions int
					if err := selected.db.QueryRow(`SELECT count(*) FROM flow_instances WHERE run_id=$1`, runID).Scan(&companions); err != nil {
						t.Fatal(err)
					}
					if states != 1 || published != 0 || companions != 1 {
						t.Fatalf("failed transaction leaked: states=%d publications=%d companions=%d", states, published, companions)
					}
					assertUnchangedConstruction()
					return
				}
				if observed.err != nil || states != 1 || published != 1 || len(observed.plans) != 1 {
					t.Fatalf("positive control err=%v states=%d publications=%d plans=%d", observed.err, states, published, len(observed.plans))
				}
				plan, ok := observed.plans[0].(runtimebus.EnginePublicationPlan)
				if !ok {
					t.Fatalf("unexpected publication %T", observed.plans[0])
				}
				if err := plan.PublicationCommand().Validate(); err == nil {
					t.Fatal("ordinary writer accepted undischarged prospective plan")
				}
				if err := plan.PublicationCommand().ValidateFanOut(); err == nil {
					t.Fatal("fan-out writer accepted undischarged prospective plan")
				}
			})
		}
	}
}
