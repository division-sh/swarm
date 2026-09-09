package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimecore "github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

type forkWorkflowOwnershipStore interface {
	PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	MaterializeRunForkForSelectedContractExecution(context.Context, runforkreadiness.MaterializeRequest) (runfork.RunForkMaterialization, error)
}

func TestSelectedForkChangedDeclarationStateCorrespondenceBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, change := range []string{"same_owner", "repeated_associations", "renamed_flow", "receiver_rehoming", "entity_type", "route", "scope", "mode", "version", "omitted_state", "omitted_association"} {
				t.Run(change, func(t *testing.T) {
					ctx, request := forkWorkflowOwnershipRequestWithSeedDelivery(t, fixture, backend.name == "postgres", change == "repeated_associations")
					projection, err := request.Readiness.Projection()
					if err != nil {
						t.Fatal(err)
					}
					original, err := request.Readiness.Projection()
					if err != nil {
						t.Fatal(err)
					}
					if len(projection.States) != 1 {
						t.Fatalf("expected one admitted owner: %+v", projection.States)
					}
					state := &projection.States[0]
					switch change {
					case "renamed_flow":
						state.FlowID = "renamed"
					case "receiver_rehoming":
						state.EntityID = uuid.NewString()
						state.FlowID = "sink"
					case "entity_type":
						state.EntityType = "receipt"
					case "route", "scope":
						state.Route = flowidentity.StoredRoute("other", "other", "other")
					case "mode":
						state.Mode = "template"
					case "version":
						state.WorkflowVersion += "-other"
					case "omitted_state":
						projection.States = nil
					case "omitted_association":
						state.SourceEvents = nil
					}
					after, err := request.Readiness.Projection()
					if err != nil || !reflect.DeepEqual(original, after) {
						t.Fatalf("inspection copy changed admitted authority: %v", err)
					}
					if change == "repeated_associations" && len(after.States[0].SourceEvents) != 2 {
						t.Fatal("admission dropped a legitimate event association")
					}
					result, err := fixture.store.(forkWorkflowOwnershipStore).MaterializeRunForkForSelectedContractExecution(ctx, request)
					if err != nil {
						t.Fatalf("canonical immutable state did not materialize: %v", err)
					}
					requireForkWorkflowOwnershipPreserved(t, fixture.db, request.SourceRunID, result.ForkRunID)
				})
			}
		})
	}
}

func TestSelectedForkOwnershipEvidenceContradictionsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, change := range []string{"zero_admission", "foreign_admission", "missing_event", "foreign_event", "duplicate_event", "missing_planning", "foreign_frontier", "wrong_effective_identity"} {
				t.Run("store_boundary/"+change, func(t *testing.T) {
					ctx, request := forkWorkflowOwnershipRequest(t, fixture, backend.name == "postgres")
					switch change {
					case "zero_admission":
						request.Readiness = runforkreadiness.Admission{}
					case "foreign_admission":
						_, other := forkWorkflowOwnershipRequest(t, fixture, backend.name == "postgres")
						request.Readiness = other.Readiness
					case "missing_event":
						request.RecipientPlanning.RecipientPlanEvents[0].SourceEventID = ""
					case "foreign_event":
						request.RecipientPlanning.RecipientPlanEvents[0].SourceEventID = uuid.NewString()
					case "duplicate_event":
						request.RecipientPlanning.RecipientPlanEvents = append(request.RecipientPlanning.RecipientPlanEvents, request.RecipientPlanning.RecipientPlanEvents[0])
					case "missing_planning":
						request.RecipientPlanning.RecipientPlanEvents = nil
					case "foreign_frontier":
						request.FrontierAdmission.FrontierEvents[0].SourceEventID = uuid.NewString()
					case "wrong_effective_identity":
						request.EffectiveSourceIdentity = runforkreadiness.Binding{}.EffectiveSourceIdentity
					}
					before := forkWorkflowOwnershipSnapshot(t, fixture.db)
					if _, err := fixture.store.(forkWorkflowOwnershipStore).MaterializeRunForkForSelectedContractExecution(ctx, request); err == nil {
						t.Fatal("forged admission binding materialized")
					}
					if after := forkWorkflowOwnershipSnapshot(t, fixture.db); !reflect.DeepEqual(before, after) {
						t.Fatal("forged admission changed rows")
					}
				})
			}
		})
	}
}

func TestSelectedForkReceiverCompanionPresenceBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, change := range []string{"exact_reuse", "missing_companion", "missing_state", "foreign_route", "wrong_type", "wrong_workflow", "wrong_mode", "wrong_config", "inactive", "terminated"} {
				t.Run(change, func(t *testing.T) {
					ctx, request := forkWorkflowOwnershipRequest(t, fixture, backend.name == "postgres")
					selected := fixture.store.(forkWorkflowOwnershipStore)
					result, err := selected.MaterializeRunForkForSelectedContractExecution(ctx, request)
					if err != nil {
						t.Fatalf("fresh state-only child companion: %v", err)
					}
					requireForkWorkflowOwnershipPreserved(t, fixture.db, request.SourceRunID, result.ForkRunID)
					var statement string
					switch change {
					case "missing_companion":
						statement = `DELETE FROM flow_instances WHERE run_id = $1`
					case "missing_state":
						statement = `DELETE FROM entity_state WHERE run_id = $1`
					case "foreign_route":
						statement = `UPDATE entity_state SET flow_instance = 'sink' WHERE run_id = $1`
					case "wrong_type":
						statement = `UPDATE entity_state SET entity_type = 'receipt' WHERE run_id = $1`
					case "wrong_workflow":
						statement = `UPDATE flow_instances SET flow_template = 'sink' WHERE run_id = $1`
					case "wrong_mode":
						statement = `UPDATE flow_instances SET mode = 'template' WHERE run_id = $1`
					case "wrong_config":
						statement = `UPDATE flow_instances SET config = '{}' WHERE run_id = $1`
					case "inactive":
						statement = `UPDATE flow_instances SET status = 'draining' WHERE run_id = $1`
					case "terminated":
						statement = `UPDATE flow_instances SET status = 'terminated', terminated_at = created_at WHERE run_id = $1`
					}
					if statement != "" {
						if _, err := fixture.db.ExecContext(ctx, statement, result.ForkRunID); err != nil {
							t.Fatal(err)
						}
					}
					before := forkWorkflowOwnershipSnapshot(t, fixture.db)
					_, err = selected.MaterializeRunForkForSelectedContractExecution(ctx, request)
					if (err == nil) != (change == "exact_reuse") {
						t.Fatalf("existing child validation = %v", err)
					}
					if after := forkWorkflowOwnershipSnapshot(t, fixture.db); !reflect.DeepEqual(before, after) {
						t.Fatalf("replay repaired/revived/mutated existing child: %v", err)
					}
				})
			}
		})
	}
}

func TestSelectedForkProjectionFailurePhasesBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, table := range []string{"flow_instances", "run_fork_selected_contract_bindings"} {
				t.Run("inside_materialization/"+table, func(t *testing.T) {
					ctx, request := forkWorkflowOwnershipRequest(t, fixture, backend.name == "postgres")
					if backend.name == "postgres" {
						if _, err := fixture.db.Exec(`CREATE OR REPLACE FUNCTION reject_fork_workflow_ownership() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fork ownership injected rollback'; END $$`); err != nil {
							t.Fatal(err)
						}
						if _, err := fixture.db.Exec(`CREATE TRIGGER reject_fork_workflow_ownership BEFORE INSERT ON ` + table + ` FOR EACH ROW EXECUTE FUNCTION reject_fork_workflow_ownership()`); err != nil {
							t.Fatal(err)
						}
						defer fixture.db.Exec(`DROP TRIGGER reject_fork_workflow_ownership ON ` + table)
					} else {
						if _, err := fixture.db.Exec(`CREATE TRIGGER reject_fork_workflow_ownership BEFORE INSERT ON ` + table + ` BEGIN SELECT RAISE(ABORT, 'fork ownership injected rollback'); END`); err != nil {
							t.Fatal(err)
						}
						defer fixture.db.Exec(`DROP TRIGGER reject_fork_workflow_ownership`)
					}
					before := forkWorkflowOwnershipSnapshot(t, fixture.db)
					_, err := fixture.store.(forkWorkflowOwnershipStore).MaterializeRunForkForSelectedContractExecution(ctx, request)
					if err == nil || !strings.Contains(err.Error(), "fork ownership injected rollback") {
						t.Fatalf("did not reach actual transaction failure: %v", err)
					}
					if after := forkWorkflowOwnershipSnapshot(t, fixture.db); !reflect.DeepEqual(before, after) {
						t.Fatal("failed materialization left committed state/companion/binding/revision/activity rows")
					}
				})
			}
		})
	}
}

func TestSelectedForkWorkflowProjectionOmissionBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, change := range []string{"complete", "fresh_missing_state", "fresh_missing_association", "replay_missing_state", "replay_missing_association"} {
				t.Run(change, func(t *testing.T) {
					ctx, request := forkWorkflowOwnershipRequestWithSeedDelivery(t, fixture, backend.name == "postgres", true)
					selected := fixture.store.(forkWorkflowOwnershipStore)
					projection, err := request.Readiness.Projection()
					if err != nil {
						t.Fatal(err)
					}
					if len(projection.States) != 1 || len(projection.States[0].SourceEvents) != 2 {
						t.Fatalf("canonical complete control missing: %+v", projection.States)
					}
					if strings.HasPrefix(change, "replay_") {
						if _, err := selected.MaterializeRunForkForSelectedContractExecution(ctx, request); err != nil {
							t.Fatal(err)
						}
					}
					if strings.HasSuffix(change, "missing_state") {
						request.Readiness = runforkreadiness.Admission{}
					}
					if strings.HasSuffix(change, "missing_association") {
						request.RecipientPlanning.RecipientPlanEvents = request.RecipientPlanning.RecipientPlanEvents[:1]
					}
					before := forkWorkflowOwnershipSnapshot(t, fixture.db)
					result, err := selected.MaterializeRunForkForSelectedContractExecution(ctx, request)
					if change == "complete" {
						if err != nil {
							t.Fatal(err)
						}
						requireForkWorkflowOwnershipPreserved(t, fixture.db, request.SourceRunID, result.ForkRunID)
						return
					}
					if err == nil {
						t.Fatal("incomplete admitted relation was materialized")
					}
					if after := forkWorkflowOwnershipSnapshot(t, fixture.db); !reflect.DeepEqual(before, after) {
						t.Fatal("omission refusal changed persistent rows")
					}
				})
			}
		})
	}
}

func forkWorkflowOwnershipRequest(t *testing.T, fixture authorActivityReceiptFixture, postgres bool) (context.Context, runforkreadiness.MaterializeRequest) {
	t.Helper()
	return forkWorkflowOwnershipRequestWithSeedDelivery(t, fixture, postgres, false)
}

func forkWorkflowOwnershipRequestWithSeedDelivery(t *testing.T, fixture authorActivityReceiptFixture, postgres, seedDelivery bool) (context.Context, runforkreadiness.MaterializeRequest) {
	t.Helper()
	ctx := managedExecutionStoreTestContext(t, testAuthorActivityContext())
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(repo, canonicalrouting.CopyForkReceiverOwnership(t, []canonicalrouting.ForkReceiver{{Path: "sink", Policy: canonicalrouting.ForkReceiverOptionalAbsent}}, false), runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	runID, eventID, seedEventID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	hash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatal(err)
	}
	sourceFact, err := correlation.NewSourceArtifactFact(hash)
	if err != nil {
		t.Fatal(err)
	}
	effective, err := runtimecore.AdmitEffectiveSourceProjection(runtimecore.EffectiveSourceProjectionRequest{Source: source, SourceArtifactFact: sourceFact})
	if err != nil {
		t.Fatal(err)
	}
	source = effective.Source()
	ctx = correlation.WithSourceArtifactFact(ctx, sourceFact)
	requireRunFixtureForTest(t, ctx, fixture.store, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, BundleHash: hash, Artifact: bundle.SourceArtifact, StartedAt: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)})
	at := time.Date(2026, 7, 14, 12, 1, 0, 0, time.UTC)
	event := eventtest.ExistingRunRootIngress(eventID, "start.requested", "test", "", []byte(`{"token":"ownership"}`), 0, runID, events.EventEnvelope{}, at)
	node := mustPersistenceRootNode("controller")
	if _, ok := source.ExecutableNode(node); !ok {
		t.Fatal("fixture controller is not declared")
	}
	target, err := events.NewExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: runID})
	if err != nil {
		t.Fatal(err)
	}
	seed := eventtest.ExistingRunRootIngress(seedEventID, "start.seeded", "test", "", []byte(`{"token":"ownership"}`), 0, runID, events.EventEnvelope{}, at.Add(-time.Second))
	var seedRoutes []events.DeliveryRoute
	if seedDelivery {
		seedRoutes = []events.DeliveryRoute{{Recipient: events.MustNodeDeliveryRecipient(node), Target: target}}
	}
	if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, seed, seedRoutes); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ExecContext(ctx, `INSERT INTO entity_mutations
		(run_id, entity_id, domain, path, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at)
		VALUES ($1, $2, 'lifecycle_state', '', 'null', '"waiting"', $3, 'platform', 'ownership-fixture', 'seed', $4),
		($5, $6, 'authored_field', 'marker', 'null', '"source-owned"', $7, 'platform', 'ownership-fixture', 'seed', $8)`, runID, runID, seedEventID, at, runID, runID, seedEventID, at); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ExecContext(ctx, `INSERT INTO entity_state
		(run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, bookkeeping, accumulator, revision, entered_state_at, created_at, updated_at)
		VALUES ($1, $2, $3, 'root', 'waiting', '{}', '{"marker":"source-owned"}', '{}', '{}', 7, $4, $5, $6)`, runID, runID, runID, at, at, at); err != nil {
		t.Fatal(err)
	}
	captureFanOutBarrierForkRevision(t, ctx, fixture.db, runID, postgres)
	if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{{Recipient: events.MustNodeDeliveryRecipient(node), Target: target}}); err != nil {
		t.Fatal(err)
	}
	plan, err := fixture.store.(forkWorkflowOwnershipStore).PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Entities) != 1 || plan.Entities[0].MaterializationMetadata == nil || plan.Entities[0].MaterializationMetadata.FlowInstance != runID || plan.Entities[0].MaterializationMetadata.EntityType != "root" {
		t.Fatalf("fixture failed to publish exact fixed-revision metadata: %+v", plan.Entities)
	}
	fixture.advance()
	return ctx, prepareSelectedStoreMaterializationForTest(t, ctx, fixture.store, runID, eventID, runfork.RunForkContractSelection{Mode: "selected_contracts"})
}

func requireForkWorkflowOwnershipPreserved(t *testing.T, db *sql.DB, sourceRun, forkRun string) {
	t.Helper()
	for _, runID := range []string{sourceRun, forkRun} {
		var entity, route, entityType, state, fields string
		var revision int
		if err := db.QueryRow(`SELECT CAST(entity_id AS TEXT), flow_instance, entity_type, current_state, fields, revision FROM entity_state WHERE run_id = $1`, runID).Scan(&entity, &route, &entityType, &state, &fields, &revision); err != nil {
			t.Fatal(err)
		}
		wantRevision := 7
		if runID == forkRun {
			wantRevision = 1
		}
		if entity != runID || route != runID || entityType != "root" || state != "waiting" || revision != wantRevision || !strings.Contains(fields, "source-owned") {
			t.Fatalf("source/child ownership or state changed: %s/%s/%s/%s/%s/%d", entity, route, entityType, state, fields, revision)
		}
	}
	var sourceCompanions, childCompanions int
	if err := db.QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE run_id = $1`, sourceRun).Scan(&sourceCompanions); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE run_id = $1 AND instance_path = $2 AND flow_template = '.' AND status = 'active'`, forkRun, forkRun).Scan(&childCompanions); err != nil {
		t.Fatal(err)
	}
	if sourceCompanions != 0 || childCompanions != 1 {
		t.Fatalf("fresh child construction copied or changed source companion: source=%d child=%d", sourceCompanions, childCompanions)
	}
}

func forkWorkflowOwnershipSnapshot(t *testing.T, db *sql.DB) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, table := range []string{"runs", "entity_state", "entity_mutations", "flow_instances", "flow_instance_runtime_readiness", "run_fork_selected_contract_bindings", "run_fork_selected_contract_route_recoveries", "run_fork_revision_heads", "run_fork_revisions", "author_activity_occurrences"} {
		rows, err := db.Query(`SELECT * FROM ` + table)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		for rows.Next() {
			values, pointers := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			for i, value := range values {
				if raw, ok := value.([]byte); ok {
					values[i] = string(raw)
				}
			}
			out[table] = append(out[table], fmt.Sprintf("%#v", values))
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
		sort.Strings(out[table])
	}
	return out
}
